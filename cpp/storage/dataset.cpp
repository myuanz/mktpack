#include <mktpack/storage.hpp>
#include "result.hpp"
#include <algorithm>
#include <atomic>
#include <map>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>

namespace mktpack {
using namespace storage;

Json describe(const std::filesystem::path& root, const std::string& group, const std::string& symbol) {
    Database database(root);
    if (group.empty()) {
        Statement query(database,"SELECT name,kind,storage FROM __group__ ORDER BY name");
        Json result=Json::array();
        while (query.next()) result.push_back({{"name",query.text(0)},{"kind",query.text(1)},{"storage",query.text(2)}});
        return result;
    }
    const auto spec=group_spec(database,group);
    const auto& schema=spec.schema;
    if (spec.storage=="sqlite" && spec.kind=="timeseries") {
        SqlReader reader(database,spec);
        int64_t rows=0,first=INT64_MAX,last=INT64_MIN;
        if (symbol.empty()) {
            for (const auto& part:reader.partitions()) {
                rows+=part.rows; first=std::min(first,part.first); last=std::max(last,part.last);
            }
        } else if (!spec.symbol.empty()) {
            Statement lookup(database,"SELECT id FROM __symbols__ WHERE name=?");
            lookup.text(1,symbol);
            if (lookup.next()) {
                const auto symbol_id=lookup.integer(0);
                const auto sql="SELECT count(*),min("+identifier(spec.time)+"),max("+identifier(spec.time)+") FROM "+
                               identifier(spec.table)+" WHERE "+identifier(spec.symbol)+"=?";
                for (const auto& part:reader.partitions()) {
                    Statement q(reader.database(part.volume),sql);
                    q.integer(1,symbol_id);
                    if (q.next() && q.integer(0)>0) {
                        rows+=q.integer(0);
                        first=std::min(first,q.integer(1));
                        last=std::max(last,q.integer(2));
                    }
                }
            }
        }
        Json result={{"name",group},{"schema",schema},{"rows",rows},{"time_column",spec.time},
                     {"symbol_column",spec.symbol},
                     {"first_time",rows?Json(first):Json(nullptr)},
                     {"last_time",rows?Json(last):Json(nullptr)}};
        if (!symbol.empty()) result["symbol"]=symbol;
        return result;
    }
    int64_t rows=0;
    Json first=nullptr,last=nullptr;
    if (symbol.empty()) {
        Statement stats(database,"SELECT coalesce(sum(rows),0),min(first_time),max(last_time) FROM __timefile__ WHERE group_id=? AND rows>0");
        stats.integer(1,spec.id);
        stats.next();
        rows=sqlite3_column_int64(stats.statement,0);
        if (rows>0) {
            first=sqlite3_column_int64(stats.statement,1);
            last=sqlite3_column_int64(stats.statement,2);
        }
    } else {
        Statement stats(database,"SELECT coalesce(sum(f.rows),0),min(f.first_time),max(f.last_time) "
                                 "FROM __timefile__ f JOIN __symbols__ s ON s.id=f.symbol_id "
                                 "WHERE f.group_id=? AND s.name=? AND f.rows>0");
        stats.integer(1,spec.id);
        stats.text(2,symbol);
        stats.next();
        rows=sqlite3_column_int64(stats.statement,0);
        if (rows>0) {
            first=sqlite3_column_int64(stats.statement,1);
            last=sqlite3_column_int64(stats.statement,2);
        }
    }
    Json result={{"name",group},{"schema",schema},{"rows",rows},{"first_time",first},{"last_time",last}};
    if (!symbol.empty()) result["symbol"]=symbol;
    return result;
}

std::vector<std::string> groups(const std::filesystem::path& root) {
    Database database(root);
    Statement query(database,"SELECT name FROM __group__ ORDER BY name");
    std::vector<std::string> names;
    while(query.next())names.push_back(query.text(0));
    return names;
}

std::optional<std::string> group_kind(const std::filesystem::path& root, const std::string& group) {
    Database database(root);
    Statement query(database,"SELECT kind FROM __group__ WHERE name=?");
    query.text(1,group);
    if(!query.next())return std::nullopt;
    return query.text(0);
}

std::vector<std::string> symbols(const std::filesystem::path& root, const std::string& group,
                                 int64_t start, int64_t end) {
    Database database(root);
    const auto spec=group_spec(database,group);
    if(start>=end)return {};
    if(spec.storage=="sqlite") {
        if(spec.kind!="timeseries" || spec.symbol.empty())return {};
        SqlReader reader(database,spec);
        std::set<int64_t> ids;
        const auto sql="SELECT DISTINCT "+identifier(spec.symbol)+" FROM "+identifier(spec.table)+
                       " WHERE "+identifier(spec.time)+">=? AND "+identifier(spec.time)+"<? AND "+
                       identifier(spec.symbol)+" IS NOT NULL";
        for(const auto& part:reader.partitions(start,end)) {
            Statement q(reader.database(part.volume),sql);
            q.integer(1,std::max(start,part.start));
            q.integer(2,std::min(end,part.end));
            while(q.next())ids.insert(q.integer(0));
        }
        if(ids.empty())return {};
        std::string lookup="SELECT name FROM __symbols__ WHERE id IN (";
        for(size_t i=0;i<ids.size();++i)lookup+=(i?",?":"?");
        lookup+=") ORDER BY name";
        Statement names(database,lookup);
        int index=1;
        for(const auto id:ids)names.integer(index++,id);
        std::vector<std::string> result;
        while(names.next())result.push_back(names.text(0));
        return result;
    }
    Statement files(database,
        "SELECT s.name,g.name||'/'||s.name||'/'||f.period||'.mkp',f.id,coalesce(v.path,''),f.first_time,f.last_time "
        "FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id JOIN __symbols__ s ON s.id=f.symbol_id "
        "LEFT JOIN __volumes__ v ON v.id=f.volume_id "
        "WHERE g.name=? AND f.rows>0 AND f.first_time<? AND f.last_time>=? "
        "ORDER BY s.name,f.first_time,f.period,f.id");
    files.text(1,group);
    files.integer(2,end);
    files.integer(3,start);
    struct Candidate { std::string symbol,path,volume; int64_t id; };
    std::set<std::string> confirmed;
    std::vector<Candidate> uncertain;
    while(files.next()) {
        auto symbol=files.text(0);
        if(confirmed.contains(symbol))continue;
        const auto first=files.integer(4),last=files.integer(5);
        if(first>=start || last<end)confirmed.insert(std::move(symbol));
        else uncertain.push_back({std::move(symbol),files.text(1),files.text(3),files.integer(2)});
    }
    if(!uncertain.empty()) {
        const std::vector<std::string> time_only{"time"};
        std::map<std::string,std::unique_ptr<Database>> volumes;
        for(const auto& entry:uncertain) {
            if(confirmed.contains(entry.symbol))continue;
            std::vector<uint8_t> bytes;
            if(entry.volume.empty())bytes=read_file_bytes(database.root/entry.path);
            else {
                auto& volume=volumes[entry.volume];
                if(!volume)volume=std::make_unique<Database>(database.root/std::filesystem::path(entry.volume),
                                                             SQLITE_OPEN_READONLY|SQLITE_OPEN_NOMUTEX);
                Statement blob(*volume,"SELECT bytes FROM blob WHERE id=?");
                bytes=read_blob(blob,entry.id,entry.path);
            }
            const auto table=decode(bytes,time_only);
            const auto& times=table.columns.at("time");
            const auto it=std::lower_bound(times.begin(),times.end(),start);
            if(it!=times.end() && *it<end)confirmed.insert(entry.symbol);
        }
    }
    return {confirmed.begin(),confirmed.end()};
}

Json ordinary(const std::filesystem::path& root, const std::string& group) {
    Database index(root);
    const auto spec=group_spec(index,group);
    if(spec.kind!="ordinary") throw std::runtime_error("普通表不存在");
    SqlReader reader(index,spec);
    Statement query(reader.database(),"SELECT * FROM "+identifier(spec.table));
    Json rows=Json::array();
    while(query.next()) {
        Json row=Json::object();
        for(int i=0;i<sqlite3_column_count(query.statement);++i) {
            auto value=sql_value(query.statement,i);
            if(value.is_string()) {
                auto parsed=Json::parse(value.get_ref<const std::string&>(),nullptr,false);
                if(!parsed.is_discarded() && parsed.is_structured()) value=std::move(parsed);
            }
            row[sqlite3_column_name(query.statement,i)]=std::move(value);
        }
        rows.push_back(std::move(row));
    }
    return rows.empty()?Json::object():rows.size()==1?rows[0]:rows;
}

namespace {
std::vector<std::string> output_names(const Json& schema, bool symbol, const Projection& requested) {
    if (requested) {
        for (const auto& name : *requested)
            if (!schema.contains(name) && !(symbol && name=="symbol"))
                throw std::out_of_range("未知列："+name);
        return *requested;
    }
    std::vector<std::string> names;
    for (const auto& [name,field] : schema.items())
        if (!symbol || name!="symbol") names.push_back(name);
    if (symbol) names.emplace_back("symbol");
    return names;
}

struct Entry {
    std::string symbol, path, volume;
    int64_t id, first_time, last_time;
    size_t rows;
    bool boundary;
};

std::vector<Entry> select_entries(Database& database, const std::string& group,
                                  const std::vector<std::string>& symbols, int64_t start, int64_t end) {
    if (start >= end) return {};
    std::string sql = "SELECT s.name,g.name||'/'||s.name||'/'||f.period||'.mkp',f.id,coalesce(v.path,''),"
                      "f.first_time,f.last_time,f.rows "
                      "FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id JOIN __symbols__ s ON s.id=f.symbol_id "
                      "LEFT JOIN __volumes__ v ON v.id=f.volume_id "
                      "WHERE g.name=? AND f.rows>0 AND f.first_time<? AND f.last_time>=?";
    if (!symbols.empty()) {
        sql += " AND s.name IN (";
        for (size_t i = 0; i < symbols.size(); ++i) sql += (i ? ",?" : "?");
        sql += ")";
    }
    Statement files(database, sql + " ORDER BY s.name,f.first_time,f.period,f.id");
    files.text(1, group);
    files.integer(2, end);
    files.integer(3, start);
    for (size_t i = 0; i < symbols.size(); ++i) files.text(int(i + 4), symbols[i]);

    std::vector<Entry> entries;
    while (files.next()) {
        auto volume = files.text(3);
        const bool loose = volume.empty();
        const int64_t first = files.integer(4), last = files.integer(5);
        entries.push_back({files.text(0), loose ? files.text(1) : std::string{}, std::move(volume),
                           files.integer(2), first, last, static_cast<size_t>(files.integer(6)),
                           first < start || last >= end});
    }
    return entries;
}

// 按自适应小批次并行读取松散文件或分卷 BLOB 零拷贝视图，并调用 visit(entry_idx, span)。
template<class Visit>
void for_each_payload(const std::filesystem::path& root, const std::vector<Entry>& entries,
                      std::span<const size_t> selected, Visit&& visit) {
    if (selected.empty()) return;
    std::map<std::string, std::vector<size_t>> volume_groups;
    std::vector<size_t> loose_indices;
    for (size_t idx : selected) {
        if (entries[idx].volume.empty()) loose_indices.push_back(idx);
        else volume_groups[entries[idx].volume].push_back(idx);
    }

    const size_t batch_size = std::clamp<size_t>(selected.size() / 64, 1, 16);
    struct Batch {
        const std::string* volume = nullptr;
        std::vector<size_t> indices;
    };
    std::vector<Batch> batches;
    batches.reserve((selected.size() + batch_size - 1) / batch_size + loose_indices.size());
    for (const auto& [vpath, indices] : volume_groups) {
        for (size_t b = 0; b < indices.size(); b += batch_size) {
            const size_t e = std::min(indices.size(), b + batch_size);
            batches.push_back({&vpath, std::vector<size_t>(indices.begin() + b, indices.begin() + e)});
        }
    }
    for (size_t idx : loose_indices) batches.push_back({nullptr, {idx}});

    std::atomic_size_t next_batch{0};
    std::atomic_bool cancelled{false};
    std::mutex err_mutex;
    std::exception_ptr first_err;

    auto worker_fn = [&] {
        try {
            std::unordered_map<std::string, std::unique_ptr<Database>> open_volumes;
            std::unordered_map<int64_t, size_t> id_to_entry;
            id_to_entry.reserve(batch_size * 2);

            for (size_t b_idx = next_batch.fetch_add(1, std::memory_order_relaxed);
                 b_idx < batches.size() && !cancelled.load(std::memory_order_acquire);
                 b_idx = next_batch.fetch_add(1, std::memory_order_relaxed)) {
                const auto& batch = batches[b_idx];
                if (!batch.volume) {
                    const size_t idx = batch.indices[0];
                    const auto bytes = read_file_bytes(root / entries[idx].path);
                    visit(idx, std::span<const uint8_t>(bytes));
                    continue;
                }
                auto& vol = open_volumes[*batch.volume];
                if (!vol) {
                    vol = std::make_unique<Database>(root / std::filesystem::path(*batch.volume),
                                                     SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX);
                }
                const auto& pos = batch.indices;
                id_to_entry.clear();
                std::string sql = "SELECT id,bytes FROM blob WHERE id IN (";
                for (size_t k = 0; k < pos.size(); ++k) {
                    if (k) sql += ',';
                    sql += '?';
                    id_to_entry.emplace(entries[pos[k]].id, pos[k]);
                }
                sql += ')';
                Statement q(*vol, sql);
                for (size_t k = 0; k < pos.size(); ++k) q.integer(int(k + 1), entries[pos[k]].id);

                size_t seen = 0;
                while (!cancelled.load(std::memory_order_acquire) && q.next()) {
                    const size_t idx = id_to_entry.at(q.integer(0));
                    const auto* ptr = static_cast<const uint8_t*>(sqlite3_column_blob(q.statement, 1));
                    const auto sz = static_cast<size_t>(sqlite3_column_bytes(q.statement, 1));
                    visit(idx, std::span<const uint8_t>(ptr, sz));
                    ++seen;
                }
                if (!cancelled.load(std::memory_order_acquire) && seen != pos.size())
                    throw std::runtime_error("分卷缺少载荷：" + vol->path.string());
            }
        } catch (...) {
            {
                std::lock_guard lock(err_mutex);
                if (!first_err) first_err = std::current_exception();
            }
            cancelled.store(true, std::memory_order_release);
        }
    };

    const size_t hw = std::max(1u, std::thread::hardware_concurrency());
    const size_t n_workers = batches.size() <= 1 ? batches.size() : std::min({batches.size(), hw, size_t(64)});
    std::vector<std::thread> workers;
    workers.reserve(n_workers > 0 ? n_workers - 1 : 0);
    for (size_t w = 1; w < n_workers; ++w) workers.emplace_back(worker_fn);
    worker_fn();
    for (auto& w : workers) w.join();
    if (first_err) std::rethrow_exception(first_err);
}
}

QueryResult file_result(const std::filesystem::path& path) {
    const auto header=info(path);
    Json schema=Json::object();
    for (const auto& column : header.at("columns"))
        schema[column.at("name").get<std::string>()]=column;
    auto names=output_names(schema,!header.value("symbol","").empty(),{});
    return single_file_result(read(path),schema,std::move(names));
}

QueryResult query(const std::filesystem::path& root, const std::string& group,
                  const std::vector<std::string>& symbols, int64_t start, int64_t end,
                  const Projection& requested) {
    Database database(root);
    const auto spec = group_spec(database, group);
    auto names = output_names(spec.schema, spec.storage == "mkp", requested);
    if (spec.storage == "sqlite") {
        SqlReader reader(database, spec);
        return sql_result(reader, database, symbols, start, end, std::move(names));
    }

    const auto entries = select_entries(database, group, symbols, start, end);
    if (entries.empty()) return init_mkp_output(spec.schema, std::move(names), 0).result;

    std::set<std::string> field_set;
    for (const auto& name : names) if (name != "symbol") field_set.insert(name);
    const std::vector<std::string> data_fields(field_set.begin(), field_set.end());
    field_set.insert("time");
    const std::vector<std::string> boundary_fields(field_set.begin(), field_set.end());

    struct Slice { Table table; size_t first = 0, count = 0; };
    std::vector<size_t> boundary_indices, middle_indices;
    for (size_t i = 0; i < entries.size(); ++i)
        (entries[i].boundary ? boundary_indices : middle_indices).push_back(i);

    std::vector<Slice> boundary_slices(entries.size());
    for_each_payload(database.root, entries, boundary_indices, [&](size_t idx, std::span<const uint8_t> bytes) {
        auto table = decode(bytes, boundary_fields);
        const auto& time = table.columns.at("time");
        const size_t first = static_cast<size_t>(std::lower_bound(time.begin(), time.end(), start) - time.begin());
        const size_t last = static_cast<size_t>(std::lower_bound(time.begin() + first, time.end(), end) - time.begin());
        boundary_slices[idx] = {std::move(table), first, last - first};
    });

    std::vector<size_t> row_offsets(entries.size(), 0);
    std::vector<std::pair<std::string, size_t>> symbol_runs;
    size_t total_rows = 0;
    for (size_t i = 0; i < entries.size(); ++i) {
        const size_t n = entries[i].boundary ? boundary_slices[i].count : entries[i].rows;
        row_offsets[i] = total_rows;
        total_rows += n;
        if (n) {
            if (symbol_runs.empty() || symbol_runs.back().first != entries[i].symbol)
                symbol_runs.emplace_back(entries[i].symbol, n);
            else
                symbol_runs.back().second += n;
        }
    }

    auto out = init_mkp_output(spec.schema, std::move(names), total_rows);
    out.set_symbols(symbol_runs);
    for (size_t idx : boundary_indices)
        out.write_slice(boundary_slices[idx].table, boundary_slices[idx].first, boundary_slices[idx].count, row_offsets[idx]);
    boundary_slices.clear();

    if (!data_fields.empty()) {
        for_each_payload(database.root, entries, middle_indices, [&](size_t idx, std::span<const uint8_t> bytes) {
            const auto table = decode(bytes, data_fields);
            if (table.columns.begin()->second.size() != entries[idx].rows)
                throw std::runtime_error("索引行数与载荷不一致");
            out.write_slice(table, 0, entries[idx].rows, row_offsets[idx]);
        });
    }
    return std::move(out.result);
}

Json check_dataset(const std::filesystem::path& root) {
    Database database(root);
    Json errors=Json::array();
    {
        Statement missing(database,"SELECT 'timefile',id FROM __timefile__ f WHERE volume_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM __volumes__ v WHERE v.id=f.volume_id) "
                                   "UNION ALL SELECT 'sqlite',group_id FROM __sqlite_partitions__ p WHERE volume_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM __volumes__ v WHERE v.id=p.volume_id)");
        while(missing.next())errors.push_back(missing.text(0)+" 卷映射不存在："+std::to_string(missing.integer(1)));
    }
    Statement files(database,"SELECT f.id,g.name||'/'||s.name||'/'||f.period||'.mkp',first_time,last_time,rows,coalesce(v.path,'') "
                             "FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id JOIN __symbols__ s ON s.id=f.symbol_id "
                             "LEFT JOIN __volumes__ v ON v.id=f.volume_id ORDER BY v.path,f.id");
    std::optional<Database> volume;
    std::optional<Statement> blob;
    size_t count=0;
    while (files.next()) {
        const auto path=files.text(1);
        try {
            const auto volume_path=files.text(5);
            std::vector<uint8_t> bytes;
            if(volume_path.empty())bytes=read_file_bytes(database.root/path);
            else {
                const auto location=database.root/std::filesystem::path(volume_path);
                if(!volume || volume->path!=location) {
                    blob.reset();
                    volume.emplace(location);
                }
                if(!blob)blob.emplace(*volume,"SELECT bytes FROM blob WHERE id=?");
                bytes=read_blob(*blob,files.integer(0),path);
                if(bytes.empty())throw std::runtime_error("分卷载荷为空："+path);
            }
            const auto table=decode(bytes);
            const auto& time=table.columns.at("time");
            const bool valid=!time.empty() && time.size()==size_t(files.integer(4)) && time.front()==files.integer(2) &&
                             time.back()==files.integer(3) && std::is_sorted(time.begin(),time.end());
            if(!valid) errors.push_back({{"path",path},{"error","索引不一致"}});
        } catch (const std::exception& error) {
            errors.push_back(path+": "+error.what());
        }
        ++count;
    }
    blob.reset();
    volume.reset();
    int64_t sql_rows=0;
    Statement groups(database,"SELECT name FROM __group__ WHERE storage='sqlite'");
    while(groups.next()) {
        const auto spec=group_spec(database,groups.text(0));
        try {
            SqlReader reader(database,spec);
            if(spec.kind=="ordinary") {
                Statement check(reader.database(),"SELECT * FROM "+identifier(spec.table)+" LIMIT 0");
                check.next();
                continue;
            }
            std::set<std::string> checked;
            for(const auto& part:reader.partitions()) {
                if(checked.insert(part.volume).second) {
                    Statement integrity(reader.database(part.volume),"PRAGMA quick_check("+identifier(spec.table)+")");
                    while(integrity.next()) if(integrity.text(0)!="ok") errors.push_back(integrity.text(0));
                }
                const auto rows=reader.read_partition(part,part.start,part.end);
                const auto pos=std::find(rows.columns.begin(),rows.columns.end(),spec.time)-rows.columns.begin();
                if(rows.rows.size()!=size_t(part.rows) || rows.rows.empty() || rows.rows.front()[pos]!=part.first || rows.rows.back()[pos]!=part.last)
                    errors.push_back(spec.name+"：分区行数或时间范围不一致");
                sql_rows+=rows.rows.size();
            }
        } catch(const std::exception& error) { errors.push_back(spec.name+"："+error.what()); }
    }
    return {{"files",count},{"sqlite_rows",sql_rows},{"errors",errors}};
}
}

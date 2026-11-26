#include <mktpack/storage.hpp>
#include "result.hpp"
#include <algorithm>
#include <atomic>
#include <future>
#include <map>
#include <set>

namespace mktpack {
using namespace storage;

Json describe(const std::filesystem::path& root, const std::string& group) {
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
        for (const auto& part:reader.partitions()) {
            rows+=part.rows; first=std::min(first,part.first); last=std::max(last,part.last);
        }
        return {{"name",group},{"schema",schema},{"rows",rows},{"time_column",spec.time},
                {"symbol_column",spec.symbol},{"first_time",rows?first:0},{"last_time",rows?last:0}};
    }
    Statement stats(database,"SELECT sum(rows),min(first_time),max(last_time) FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id JOIN __symbols__ s ON s.id=f.symbol_id WHERE g.name=?");
    stats.text(1,group);
    stats.next();
    return {{"name",group},{"schema",schema},
            {"rows",sqlite3_column_int64(stats.statement,0)},
            {"first_time",sqlite3_column_int64(stats.statement,1)},
            {"last_time",sqlite3_column_int64(stats.statement,2)}};
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
}

QueryResult file_result(const std::filesystem::path& path) {
    const auto header=info(path);
    Json schema=Json::object();
    for (const auto& column : header.at("columns"))
        schema[column.at("name").get<std::string>()]=column;
    auto physical=output_names(schema,false,{});
    auto names=output_names(schema,!header.value("symbol","").empty(),{});
    return mkp_result({read(path)},schema,std::move(names));
}

QueryResult query(const std::filesystem::path& root, const std::string& group,
                  const std::vector<std::string>& symbols, int64_t start, int64_t end,
                  const Projection& requested) {
    Database database(root);
    const auto spec=group_spec(database,group);
    auto names=output_names(spec.schema,spec.storage=="mkp",requested);
    if(spec.storage=="sqlite") {
        SqlReader reader(database,spec);
        return sql_result(reader,database,symbols,start,end,std::move(names));
    }
    std::set<std::string> fields(names.begin(),names.end());
    fields.erase("symbol");
    fields.insert("time");
    const std::vector<std::string> physical(fields.begin(),fields.end());
    struct Entry {std::string symbol,path;int64_t id;};
    std::vector<Entry> entries;
    std::map<std::string,std::vector<size_t>> grouped;
    std::string sql="SELECT s.name,g.name||'/'||s.name||'/'||f.period||'.mkp',f.id,coalesce(v.path,'') "
                    "FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id JOIN __symbols__ s ON s.id=f.symbol_id "
                    "LEFT JOIN __volumes__ v ON v.id=f.volume_id WHERE g.name=? AND first_time<? AND last_time>=?";
    if(!symbols.empty()) {
        sql+=" AND s.name IN (";
        for(size_t i=0;i<symbols.size();++i)sql+=(i?",?":"?");
        sql+=")";
    }
    Statement files(database,sql+" ORDER BY s.name,first_time,f.period,f.id");
    files.text(1,group); files.integer(2,end); files.integer(3,start);
    for(size_t i=0;i<symbols.size();++i)files.text(int(i+4),symbols[i]);
    while (start<end && files.next()) {
        auto volume=files.text(3);
        const bool loose=volume.empty();
        if(!loose)grouped[std::move(volume)].push_back(entries.size());
        entries.push_back({files.text(0),loose?files.text(1):std::string{},files.integer(2)});
    }
    std::vector<std::vector<uint8_t>> payloads(entries.size());
    std::vector<int64_t> ids;
    ids.reserve(volume_batch_size);
    for(const auto& [path,positions]:grouped) {
        Database volume(database.root/std::filesystem::path(path));
        for(size_t begin=0;begin<positions.size();begin+=volume_batch_size) {
            const auto end=std::min(positions.size(),begin+volume_batch_size);
            ids.clear();
            for(size_t i=begin;i<end;++i)ids.push_back(entries[positions[i]].id);
            auto batch=read_volume(volume,ids);
            for(size_t i=begin;i<end;++i)payloads[positions[i]]=std::move(batch.at(entries[positions[i]].id));
        }
    }
    for(size_t i=0;i<entries.size();++i) {
        if(!entries[i].path.empty())payloads[i]=read_file_bytes(database.root/entries[i].path);
    }
    std::vector<Table> decoded(entries.size());
    std::atomic_size_t next{0};
    auto decode_next=[&] {
        for(size_t i=next++;i<entries.size();i=next++) {
            const auto bytes=std::move(payloads[i]);
            auto table=decode(bytes,physical);
            const auto& time=table.columns.at("time");
            if(!time.empty() && (time.front()<start || time.back()>=end))slice(table,start,end);
            if(!table.columns.at("time").empty())table.metadata["symbol"]=entries[i].symbol;
            decoded[i]=std::move(table);
        }
    };
    std::vector<std::future<void>> workers;
    for(size_t i=1;i<std::min<size_t>(4,entries.size());++i)
        workers.push_back(std::async(std::launch::async,decode_next));
    decode_next();
    for(auto& worker:workers)worker.get();
    std::vector<Table> tables;
    for(auto& table:decoded) {
        auto found=table.columns.find("time");
        if(found!=table.columns.end() && !found->second.empty())tables.push_back(std::move(table));
    }
    return mkp_result(std::move(tables),spec.schema,std::move(names));
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

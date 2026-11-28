#include <mktpack/storage.hpp>
#include "sqlite.hpp"
#include "store.hpp"
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <span>
#include <thread>

namespace mktpack {
namespace {
using namespace storage;
namespace fs=std::filesystem;
using namespace std::chrono;

struct File {
    int64_t id,group,symbol,period,first,last,rows;
    std::optional<int64_t> volume;
};

struct PackageFiles {
    std::vector<File> entries;
    std::map<int64_t,std::string> groups,symbols,volumes;

    std::string path(const File& file) const {
        return groups.at(file.group)+"/"+symbols.at(file.symbol)+"/"+std::to_string(file.period)+".mkp";
    }
};

class PackagePayloads {
    struct Batch {
        std::vector<std::vector<uint8_t>> payloads;
        size_t bytes=sizeof(payloads);
    };
    static constexpr size_t read_budget=64*1024*1024;
    static constexpr uintmax_t prefetch_limit=32*1024*1024;

    fs::path root;
    const PackageFiles& plan;
    bool prefetch_volumes;
    std::mutex mutex;
    std::condition_variable ready;
    std::condition_variable_any space;
    std::deque<Batch> pending;
    size_t pending_bytes=0;
    std::exception_ptr error;
    Batch current;
    size_t next=0;
    std::jthread reader;

    void read(std::stop_token stop) {
        try {
            std::optional<Database> volume;
            std::optional<int64_t> open_volume;
            std::vector<char> buffer;
            std::vector<int64_t> ids;
            ids.reserve(volume_batch_size);
            const auto& files=plan.entries;
            for(size_t i=0;i<files.size() && !stop.stop_requested();) {
                Batch batch;
                const auto& entry=files[i];
                if(!entry.volume) {
                    batch.payloads.push_back(read_file_bytes(root/plan.path(entry)));
                    ++i;
                } else {
                    if(open_volume!=entry.volume) {
                        volume.emplace(root/fs::path(plan.volumes.at(*entry.volume)));
                        open_volume=entry.volume;
                        if(prefetch_volumes) {
                            std::error_code stat_error;
                            if(fs::file_size(volume->path,stat_error)<=prefetch_limit) {
                                if(buffer.empty())buffer.resize(1024*1024);
                                std::ifstream stream(volume->path,std::ios::binary);
                                while(stream.read(buffer.data(),buffer.size())) {}
                                volume->exec("PRAGMA mmap_size="+std::to_string(prefetch_limit));
                            }
                        }
                    }
                    ids.clear();
                    for(;i<files.size() && ids.size()<volume_batch_size && files[i].volume==open_volume;++i)
                        ids.push_back(files[i].id);
                    auto payloads=read_volume(*volume,ids);
                    batch.payloads.reserve(ids.size());
                    for(auto id:ids)batch.payloads.push_back(std::move(payloads.at(id)));
                }
                for(const auto& bytes:batch.payloads)batch.bytes+=bytes.size()+sizeof(bytes);
                std::unique_lock lock(mutex);
                space.wait(lock,stop,[&] {
                    return pending.empty() ||
                        (pending_bytes<=read_budget && batch.bytes<=read_budget-pending_bytes);
                });
                if(stop.stop_requested())return;
                pending_bytes+=batch.bytes;
                pending.push_back(std::move(batch));
                lock.unlock();
                ready.notify_one();
            }
        } catch(...) {
            {
                std::lock_guard lock(mutex);
                error=std::current_exception();
            }
            ready.notify_one();
        }
    }
public:
    PackagePayloads(const fs::path& source,const PackageFiles& files,bool prefetch)
        :root(source),plan(files),prefetch_volumes(prefetch) {
        if(!plan.entries.empty())reader=std::jthread([this](std::stop_token stop){read(stop);});
    }
    std::vector<uint8_t> take() {
        if(next==current.payloads.size()) {
            std::unique_lock lock(mutex);
            ready.wait(lock,[&]{return !pending.empty() || error;});
            if(pending.empty())std::rethrow_exception(error);
            current=std::move(pending.front());
            pending.pop_front();
            pending_bytes-=current.bytes;
            next=0;
            lock.unlock();
            space.notify_one();
        }
        return std::move(current.payloads[next++]);
    }
};

class Volumes {
    fs::path root;
    std::string prefix;
    uint64_t limit;
    std::unique_ptr<Database> current;
    std::optional<Statement> blob_insert;
    size_t entries=0;
public:
    size_t count=0;
    uint64_t bytes=0;
    Volumes(fs::path directory,uint64_t maximum,std::string name_prefix="mkt.data.")
        :root(std::move(directory)),prefix(std::move(name_prefix)),limit(maximum) {}
    std::string path(int64_t id) const { return prefix+std::to_string(id); }
    void close() {
        if (!current) return;
        blob_insert.reset();
        current->exec("COMMIT");
        bytes+=sqlite_file_size(*current);
        current.reset();
    }
    int64_t append_entry(const std::string& label,const std::function<void(Database&)>& write) {
        for (;;) {
            if (!current) {
                ++count;
                current=std::make_unique<Database>(root/path(count),SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
                current->exec("PRAGMA journal_mode=MEMORY;BEGIN");
                entries=0;
            }
            current->exec("SAVEPOINT entry");
            write(*current);
            const auto size=sqlite_file_size(*current);
            if (size<=limit) {
                current->exec("RELEASE entry");
                ++entries;
                return count;
            }
            current->exec("ROLLBACK TO entry;RELEASE entry");
            if (!entries)
                throw std::runtime_error(label+"需要至少 "+std::to_string(size)+
                                         " 字节分卷，超过 --max-file-size="+std::to_string(limit));
            close();
        }
    }
    int64_t append(int64_t id,const std::vector<uint8_t>& payload,const std::string& label) {
        return append_entry(label,[&](Database& db) {
            if(!blob_insert) {
                db.exec("CREATE TABLE blob(id INTEGER PRIMARY KEY,bytes BLOB NOT NULL)");
                blob_insert.emplace(db,"INSERT INTO blob(id,bytes) VALUES(?,?)");
            }
            insert_blob(*blob_insert,id,payload);
        });
    }
};

int64_t register_volume(Database& db,const Volumes& volumes,int64_t id) {
    return ensure_volume(db,volumes.path(id));
}

struct MkpStats {
    size_t copied=0,reencoded=0;
    int64_t rows=0;
    size_t files() const {return copied+reencoded;}
};

void report_mkp(const PackageOptions& options,const std::string& path,const Volumes& volumes,int64_t volume,
                bool copy,size_t done,size_t total) {
    if(options.progress) options.progress(path+" "+(copy?"复制":"裁剪编码")+
        " → "+volumes.path(volume)+" ("+std::to_string(done)+"/"+std::to_string(total)+")");
}

void relocate_volume(Database& output,int64_t volume,std::span<const int64_t> ids) {
    constexpr size_t batch=500;
    while(!ids.empty()) {
        const auto chunk=ids.first(std::min(batch,ids.size()));
        std::string sql="UPDATE __timefile__ SET volume_id=? WHERE id IN (?";
        for(size_t i=1;i<chunk.size();++i)sql+=",?";
        Statement update(output,sql+")");
        update.integer(1,volume);
        for(size_t i=0;i<chunk.size();++i)update.integer(int(i+2),chunk[i]);
        update.next();
        ids=ids.subspan(chunk.size());
    }
}

MkpStats relocate_mkp(Database& output,Volumes& volumes,Volumes& hot_volumes,int64_t hot_from,
                      PackagePayloads& payloads,const PackageFiles& plan,const PackageOptions& options) {
    MkpStats stats;
    std::map<int64_t,std::vector<int64_t>> by_volume;
    for(const auto& file:plan.entries) {
        auto bytes=payloads.take();
        if(bytes.empty()) {
            Statement remove(output,"DELETE FROM __timefile__ WHERE id=?");
            remove.integer(1,file.id);remove.next();
            continue;
        }
        const auto path=plan.path(file);
        auto& target=file.last>=hot_from?hot_volumes:volumes;
        const auto local_id=target.append(file.id,bytes,path);
        const auto volume=register_volume(output,target,local_id);
        by_volume[volume].push_back(file.id);
        stats.rows+=file.rows;
        ++stats.copied;
        report_mkp(options,path,target,local_id,true,stats.files(),plan.entries.size());
    }
    for(const auto& [volume,ids]:by_volume) relocate_volume(output,volume,ids);
    return stats;
}

MkpStats pack_selected_mkp(Database& input,Database& output,Volumes& volumes,Volumes& hot_volumes,int64_t hot_from,
                           PackagePayloads& payloads,const PackageFiles& plan,const PackageOptions& options) {
    MkpStats stats;
    const auto& files=plan.entries;
    struct GroupSchema {Json original,output;};
    std::map<int64_t,GroupSchema> schemas;
    Statement insert(output,"INSERT INTO __timefile__(id,group_id,symbol_id,period,first_time,last_time,rows,volume_id) VALUES(?,?,?,?,?,?,?,?)");
    struct Pending {
        size_t index;
        bool copy;
        std::vector<uint8_t> bytes;
        std::future<std::vector<uint8_t>> work;
    };
    std::deque<Pending> pending;
    size_t next_file=0;
    while(next_file<files.size() || !pending.empty()) {
        while(next_file<files.size() && pending.size()<options.jobs) {
            const auto index=next_file++;
            const auto& file=files[index];
            auto bytes=payloads.take();
            const bool copy=file.first>=options.start && file.last<options.end;
            if(copy) pending.push_back({index,true,std::move(bytes),{}});
            else {
                auto task=[bytes=std::move(bytes),start=options.start,end=options.end]() mutable {
                    auto table=decode(bytes);
                    slice(table,start,end);
                    if(table.columns.at("time").empty()) return std::vector<uint8_t>{};
                    return encode(table);
                };
                pending.push_back({index,false,{},std::async(options.jobs==1?std::launch::deferred:std::launch::async,std::move(task))});
            }
        }
        auto item=std::move(pending.front());
        pending.pop_front();
        const auto& file=files[item.index];
        auto bytes=item.copy?std::move(item.bytes):item.work.get();
        if(bytes.empty()) continue;
        const auto header=info(bytes);
        const auto path=plan.path(file);
        const int64_t last_time=header.at("last_time");
        auto& target=last_time>=hot_from?hot_volumes:volumes;
        const auto local_id=target.append(file.id,bytes,path);
        const auto volume=register_volume(output,target,local_id);
        insert.integer(1,file.id);insert.integer(2,file.group);insert.integer(3,file.symbol);
        insert.integer(4,file.period);insert.integer(5,header.at("first_time"));insert.integer(6,last_time);
        insert.integer(7,header.at("rows"));insert.integer(8,volume);insert.next();
        sqlite3_reset(insert.statement);
        auto [schema_position,new_group]=schemas.try_emplace(file.group);
        auto& [original,group_schema]=schema_position->second;
        if(new_group) {
            Statement query(input,"SELECT schema FROM __group__ WHERE id=?");
            query.integer(1,file.group);query.next();
            original=Json::parse(query.text(0));
        }
        for(const auto& descriptor:header.at("columns")) {
            const auto name=descriptor.at("name").get<std::string>();
            auto& schema=group_schema[name];
            if(schema.is_null()) {
                schema=original.value(name,Json::object());
                schema["scale"]=descriptor.value("scale",0);
            } else schema["scale"]=std::max(schema.value("scale",0),descriptor.value("scale",0));
            schema["type"]=descriptor.at("type");
        }
        stats.rows+=header.at("rows").get<int64_t>();
        if(item.copy)++stats.copied;
        else ++stats.reencoded;
        report_mkp(options,path,target,local_id,item.copy,stats.files(),files.size());
    }
    for(const auto& [group,schema]:schemas) {
        Statement update(output,"UPDATE __group__ SET schema=? WHERE id=?");
        update.text(1,schema.output.dump());update.integer(2,group);update.next();
    }

    return stats;
}
}

Json package(const fs::path& source,const fs::path& destination,const PackageOptions& options) {
    if (options.start>=options.end) throw std::runtime_error("打包时间范围必须满足 start < end");
    if (!options.max_file_size) throw std::runtime_error("分卷上限必须大于零");
    if (!options.jobs) throw std::runtime_error("并行任务数必须大于零");
    if (fs::exists(destination) && (!fs::is_directory(destination) || !fs::is_empty(destination)))
        throw std::runtime_error("打包目的地必须为空："+destination.string());

    Database input(source);
    input.exec("BEGIN");
    std::set<std::string> selected,excluded;
    {
        std::set<std::string> known;
        Statement groups(input,"SELECT name,kind FROM __group__ ORDER BY name");
        while(groups.next()) {
            const auto name=groups.text(0);
            known.insert(name);
            if(groups.text(1)!="timeseries") continue;
            if(options.groups.empty() || std::find(options.groups.begin(),options.groups.end(),name)!=options.groups.end()) selected.insert(name);
            else excluded.insert(name);
        }
        for(const auto& name:options.groups) if(!known.contains(name)) throw std::runtime_error("未知数据组："+name);
    }

    const bool full_range=options.start==INT64_MIN && options.end==INT64_MAX &&
        options.symbols.empty() && options.groups.empty();
    PackageFiles plan;
    {
        Statement q(input,"SELECT f.id,f.group_id,f.symbol_id,f.period,f.first_time,f.last_time,f.rows,g.name,s.name,v.id,v.path "
                          "FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id "
                          "JOIN __symbols__ s ON s.id=f.symbol_id LEFT JOIN __volumes__ v ON v.id=f.volume_id "
                          "WHERE f.first_time<? AND f.last_time>=? ORDER BY g.name,s.name,f.period,f.id");
        q.integer(1,options.end);q.integer(2,options.start);
        while(q.next()) {
            const auto group=q.integer(1),symbol=q.integer(2);
            if(!plan.groups.contains(group)) {
                auto name=q.text(7);
                if(!selected.contains(name)) continue;
                plan.groups.emplace(group,std::move(name));
            }
            if(!plan.symbols.contains(symbol)) {
                auto name=q.text(8);
                if(!options.symbols.empty() && std::find(options.symbols.begin(),options.symbols.end(),name)==options.symbols.end()) continue;
                plan.symbols.emplace(symbol,std::move(name));
            }
            std::optional<int64_t> volume;
            if(sqlite3_column_type(q.statement,9)!=SQLITE_NULL) {
                const auto id=q.integer(9);
                auto [path,new_path]=plan.volumes.try_emplace(id);
                if(new_path)path->second=q.text(10);
                if(!path->second.empty())volume=id;
            }
            plan.entries.push_back({q.integer(0),group,symbol,q.integer(3),q.integer(4),q.integer(5),q.integer(6),volume});
        }
    }
    bool relocate=false;
    if(full_range) {
        Statement count(input,"SELECT count(*) FROM __timefile__");
        count.next();
        relocate=count.integer(0)==int64_t(plan.entries.size());
    }
    if(options.progress) options.progress("发现 "+std::to_string(plan.entries.size())+" 个逻辑载荷");

    fs::create_directories(destination);
    const auto work=destination/".mkt-package";
    fs::create_directory(work);
    Json result={{"files",0},{"rows",0},{"copied",0},{"reencoded",0},{"warnings",Json::array()}};
    try {
        PackagePayloads payloads(input.root,plan,full_range);
        std::optional<Database> output_db(std::in_place,work/"mkt.sqlite",SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
        auto& output=*output_db;
        copy_sqlite_file(input,output);
        if(!relocate)output.exec("DELETE FROM __timefile__");
        output.exec("DELETE FROM __volumes__;DELETE FROM __sqlite_partitions__;PRAGMA application_id=0x4d4b5450");
        for(const auto& group:excluded) {
            Statement remove(output,"DELETE FROM __group__ WHERE name=?");remove.text(1,group);remove.next();
        }

        uint64_t ordinary_bytes=0;
        Statement ordinary(input,"SELECT DISTINCT location FROM __group__ WHERE kind='ordinary' AND location<>''");
        while(ordinary.next()) {
            const auto location=ordinary.text(0);
            fs::create_directories((work/location).parent_path());
            Database src(input.root/location),dst(work/location,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
            copy_sqlite_file(src,dst);ordinary_bytes+=sqlite_file_size(dst);
        }

        const auto port=meta_get(input,"port").value_or("");
        const int64_t hot_from=port=="binance"?current_year_start_ms():INT64_MAX;
        meta_set(output,"max_file_size",std::to_string(options.max_file_size));
        ensure_default_version(output);

        Volumes volumes(work,options.max_file_size,"mkt.data.");
        Volumes hot_volumes(work,options.max_file_size,"hot.data.");
        int64_t sqlite_rows=0;
        output.exec("BEGIN");
        const auto mkp=relocate?relocate_mkp(output,volumes,hot_volumes,hot_from,payloads,plan,options)
                               :pack_selected_mkp(input,output,volumes,hot_volumes,hot_from,payloads,plan,options);

        for(const auto& name:selected) {
            const auto spec=group_spec(input,name);
            if(spec.storage!="sqlite") continue;
            SqlReader reader(input,spec);
            const auto parts=reader.partitions(options.start,options.end);
            auto destination_spec=spec;
            destination_spec.table="data_"+std::to_string(spec.id);
            Statement update(output,"UPDATE __group__ SET location='',table_name=? WHERE id=?");
            update.text(1,destination_spec.table);update.integer(2,spec.id);update.next();
            if(parts.empty()) continue;
            const auto first=std::max(options.start,parts.front().first),last=std::min(options.end-1,parts.back().last);
            auto date=year_month_day{floor<days>(sys_time<milliseconds>{milliseconds{first}})};
            auto month=date.year()/date.month();
            const auto create=reader.create_sql(parts.front().volume,destination_spec.table);
            for(;;month+=months{1}) {
                const auto start=duration_cast<milliseconds>(sys_days{month/1}.time_since_epoch()).count();
                if(start>last) break;
                const auto end=duration_cast<milliseconds>(sys_days{(month+months{1})/1}.time_since_epoch()).count();
                const auto records=reader.read(std::max(start,options.start),std::min(end,options.end),spec.symbol.empty()?std::vector<std::string>{}:options.symbols);
                if(records.rows.empty()) continue;
                char label[8];snprintf(label,sizeof label,"%04d-%02u",int(month.year()),unsigned(month.month()));
                const auto local_id=volumes.append_entry(name+" "+label+" 的整月数据",[&](Database& db){copy_rows(db,destination_spec,create,records);});
                const auto volume=register_volume(output,volumes,local_id);
                const auto column=std::find(records.columns.begin(),records.columns.end(),spec.time)-records.columns.begin();
                int64_t minimum=INT64_MAX,maximum=INT64_MIN;
                for(const auto& row:records.rows){minimum=std::min(minimum,row[column].get<int64_t>());maximum=std::max(maximum,row[column].get<int64_t>());}
                Statement part(output,"INSERT INTO __sqlite_partitions__ VALUES(?,?,?,?,?,?,?)");
                part.integer(1,spec.id);part.integer(2,start);part.integer(3,end);part.integer(4,volume);
                part.integer(5,records.rows.size());part.integer(6,minimum);part.integer(7,maximum);part.next();
                sqlite_rows+=records.rows.size();
            }
        }
        output.exec("COMMIT;VACUUM");
        volumes.close();
        hot_volumes.close();
        const auto index_size=sqlite_file_size(output);
        if(index_size>options.max_file_size)result["warnings"].push_back("mkt.sqlite 为 "+std::to_string(index_size)+" 字节，超过分卷上限 "+std::to_string(options.max_file_size));
        result["files"]=mkp.files();result["rows"]=mkp.rows+sqlite_rows;result["sqlite_rows"]=sqlite_rows;
        result["copied"]=mkp.copied;result["reencoded"]=mkp.reencoded;
        result["volumes"]=volumes.count;result["hot_volumes"]=hot_volumes.count;
        result["index_bytes"]=index_size;result["bytes"]=index_size+volumes.bytes+hot_volumes.bytes+ordinary_bytes;
        output_db.reset();
        for(const auto& entry:fs::directory_iterator(work))fs::rename(entry.path(),destination/entry.path().filename());
        fs::remove(work);
    } catch(...) {
        fs::remove_all(work);
        throw;
    }
    return result;
}
}

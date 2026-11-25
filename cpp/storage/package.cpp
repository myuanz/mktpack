#include <mktpack/storage.hpp>
#include "sqlite.hpp"
#include <chrono>
#include <cstdio>
#include <deque>
#include <future>
#include <set>

namespace mktpack {
namespace {
using namespace storage;
namespace fs=std::filesystem;
using namespace std::chrono;

struct File {
    int64_t id,period,first,last;
    std::string group,symbol,path,volume;
};

uint64_t database_size(Database& db) {
    Statement pages(db,"PRAGMA page_count"), size(db,"PRAGMA page_size");
    pages.next(); size.next();
    return uint64_t(pages.integer(0))*uint64_t(size.integer(0));
}

class Volumes {
    fs::path root;
    uint64_t limit;
    std::unique_ptr<Database> current;
    size_t entries=0;
public:
    size_t count=0;
    uint64_t bytes=0;
    Volumes(fs::path directory,uint64_t maximum):root(std::move(directory)),limit(maximum) {}
    std::string path(int64_t id) const { return "mkt.data."+std::to_string(id); }
    void close() {
        if (!current) return;
        current->exec("COMMIT");
        bytes+=database_size(*current);
        current.reset();
    }
    int64_t append_entry(const std::string& label,const std::function<void(Database&)>& write) {
        for (;;) {
            if (!current) {
                ++count;
                current=std::make_unique<Database>(root/path(count),SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
                current->exec("PRAGMA journal_mode=DELETE;BEGIN");
                entries=0;
            }
            current->exec("SAVEPOINT entry");
            write(*current);
            const auto size=database_size(*current);
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
            db.exec("CREATE TABLE IF NOT EXISTS blob(id INTEGER PRIMARY KEY,bytes BLOB NOT NULL)");
            Statement insert(db,"INSERT INTO blob(id,bytes) VALUES(?,?)");
            insert.integer(1,id);
            if (sqlite3_bind_blob64(insert.statement,2,payload.data(),payload.size(),SQLITE_TRANSIENT)!=SQLITE_OK)
                throw std::runtime_error(sqlite3_errmsg(db.db));
            insert.next();
        });
    }
};

void copy_index(Database& source,Database& destination) {
    auto* backup=sqlite3_backup_init(destination.db,"main",source.db,"main");
    if (!backup) throw std::runtime_error(sqlite3_errmsg(destination.db));
    const auto status=sqlite3_backup_step(backup,-1);
    const auto finish=sqlite3_backup_finish(backup);
    if (status!=SQLITE_DONE || finish!=SQLITE_OK)
        throw std::runtime_error("索引复制失败："+std::string(sqlite3_errmsg(destination.db)));
    destination.exec("PRAGMA journal_mode=DELETE");
}

int64_t register_volume(Database& db,const Volumes& volumes,int64_t id) {
    Statement insert(db,"INSERT OR IGNORE INTO __volumes__(id,path) VALUES(?,?)");
    insert.integer(1,id);insert.text(2,volumes.path(id));insert.next();
    return id;
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
    PayloadReader payloads(input);
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

    std::vector<File> files;
    {
        Statement q(input,"SELECT f.id,f.period,f.first_time,f.last_time,g.name,s.name,"
                          "g.name||'/'||s.name||'/'||f.period||'.mkp',coalesce(v.path,'') "
                          "FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id "
                          "JOIN __symbols__ s ON s.id=f.symbol_id LEFT JOIN __volumes__ v ON v.id=f.volume_id "
                          "WHERE f.first_time<? AND f.last_time>=? ORDER BY g.name,s.name,f.period,f.id");
        q.integer(1,options.end);q.integer(2,options.start);
        while(q.next()) {
            const auto group=q.text(4),symbol=q.text(5);
            if(!selected.contains(group)) continue;
            if(!options.symbols.empty() && std::find(options.symbols.begin(),options.symbols.end(),symbol)==options.symbols.end()) continue;
            files.push_back({q.integer(0),q.integer(1),q.integer(2),q.integer(3),group,symbol,q.text(6),q.text(7)});
        }
    }
    if(options.progress) options.progress("发现 "+std::to_string(files.size())+" 个逻辑载荷");

    fs::create_directories(destination);
    const auto work=destination/".mkt-package";
    fs::create_directory(work);
    Json result={{"files",0},{"rows",0},{"copied",0},{"reencoded",0},{"warnings",Json::array()}};
    try {
        Database output(work/"mkt.sqlite",SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
        copy_index(input,output);
        output.exec("DELETE FROM __timefile__;DELETE FROM __volumes__;DELETE FROM __sqlite_partitions__;PRAGMA application_id=0x4d4b5450");
        for(const auto& group:excluded) {
            Statement remove(output,"DELETE FROM __group__ WHERE name=?");remove.text(1,group);remove.next();
        }

        uint64_t ordinary_bytes=0;
        Statement ordinary(input,"SELECT DISTINCT location FROM __group__ WHERE kind='ordinary' AND location<>''");
        while(ordinary.next()) {
            const auto location=ordinary.text(0);
            fs::create_directories((work/location).parent_path());
            Database src(input.root/location),dst(work/location,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
            copy_index(src,dst);ordinary_bytes+=database_size(dst);
        }

        Volumes volumes(work,options.max_file_size);
        std::map<std::pair<std::string,std::string>,Json> schemas;
        int64_t rows=0,sqlite_rows=0;
        size_t processed=0,file_count=0;
        output.exec("BEGIN");
        std::string cached_volume;
        std::map<int64_t,std::vector<uint8_t>> cached_payloads;
        auto load_payload=[&](size_t file_index) {
            const auto& file=files[file_index];
            std::vector<uint8_t> bytes;
            if(file.volume.empty()) bytes=payloads.read(file.id,file.path,{});
            else {
                if(file.volume!=cached_volume || !cached_payloads.contains(file.id)) {
                    cached_volume=file.volume;
                    std::vector<int64_t> ids;
                    for(size_t i=file_index;i<files.size() && ids.size()<500;++i) {
                        if(files[i].volume==cached_volume)ids.push_back(files[i].id);
                    }
                    cached_payloads=payloads.read_volume(cached_volume,ids);
                }
                bytes=std::move(cached_payloads.at(file.id));
                cached_payloads.erase(file.id);
            }
            return bytes;
        };
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
                auto bytes=load_payload(index);
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
            const auto volume=volumes.append(file.id,bytes,file.group+"/"+file.symbol+"/"+std::to_string(file.period)+".mkp");
            register_volume(output,volumes,volume);
            Statement insert(output,"INSERT INTO __timefile__(id,group_id,symbol_id,period,first_time,last_time,rows,volume_id) VALUES(?,?,?,?,?,?,?,?)");
            insert.integer(1,file.id);insert.integer(2,group_id(output,file.group));insert.integer(3,symbol_id(output,file.symbol));
            insert.integer(4,file.period);insert.integer(5,header.at("first_time"));insert.integer(6,header.at("last_time"));
            insert.integer(7,header.at("rows"));insert.integer(8,volume);insert.next();
            ++file_count;
            for(const auto& descriptor:header.at("columns")) {
                const auto name=descriptor.at("name").get<std::string>();
                auto& schema=schemas[{file.group,name}];
                if(schema.is_null()) {
                    Statement original(input,"SELECT schema FROM __group__ WHERE name=?");original.text(1,file.group);
                    schema=original.next()?Json::parse(original.text(0)).value(name,Json::object()):Json::object();
                    schema["scale"]=descriptor.value("scale",0);
                } else schema["scale"]=std::max(schema.value("scale",0),descriptor.value("scale",0));
                schema["type"]=descriptor.at("type");
            }
            rows+=header.at("rows").get<int64_t>();
            const auto kind=item.copy?"copied":"reencoded";
            result[kind]=result[kind].get<size_t>()+1;
            if(options.progress) options.progress(file.group+"/"+file.symbol+"/"+std::to_string(file.period)+".mkp "+
                (item.copy?"复制":"裁剪编码")+" → "+volumes.path(volume)+" ("+std::to_string(++processed)+"/"+std::to_string(files.size())+")");
        }
        std::map<std::string,Json> group_schemas;
        for(const auto& [key,schema]:schemas)group_schemas[key.first][key.second]=schema;
        for(const auto& [group,schema]:group_schemas) {
            Statement update(output,"UPDATE __group__ SET schema=? WHERE name=?");
            update.text(1,schema.dump());update.text(2,group);update.next();
        }

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
                const auto volume=volumes.append_entry(name+" "+label+" 的整月数据",[&](Database& db){copy_rows(db,destination_spec,create,records);});
                register_volume(output,volumes,volume);
                const auto column=std::find(records.columns.begin(),records.columns.end(),spec.time)-records.columns.begin();
                int64_t minimum=INT64_MAX,maximum=INT64_MIN;
                for(const auto& row:records.rows){minimum=std::min(minimum,row[column].get<int64_t>());maximum=std::max(maximum,row[column].get<int64_t>());}
                Statement part(output,"INSERT INTO __sqlite_partitions__ VALUES(?,?,?,?,?,?,?)");
                part.integer(1,spec.id);part.integer(2,start);part.integer(3,end);part.integer(4,volume);
                part.integer(5,records.rows.size());part.integer(6,minimum);part.integer(7,maximum);part.next();
                sqlite_rows+=records.rows.size();
            }
        }
        rows+=sqlite_rows;
        output.exec("COMMIT;VACUUM");
        volumes.close();
        const auto index_size=database_size(output);
        if(index_size>options.max_file_size)result["warnings"].push_back("mkt.sqlite 为 "+std::to_string(index_size)+" 字节，超过分卷上限 "+std::to_string(options.max_file_size));
        result["files"]=file_count;result["rows"]=rows;result["sqlite_rows"]=sqlite_rows;
        result["volumes"]=volumes.count;result["index_bytes"]=index_size;result["bytes"]=index_size+volumes.bytes+ordinary_bytes;
        for(const auto& entry:fs::directory_iterator(work))fs::rename(entry.path(),destination/entry.path().filename());
        fs::remove(work);
    } catch(...) {
        fs::remove_all(work);
        throw;
    }
    return result;
}
}

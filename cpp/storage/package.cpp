#include <mktpack/storage.hpp>
#include "sqlite.hpp"
#include <chrono>
#include <cstdio>
#include <deque>
#include <future>
#include <set>
#include <tuple>

namespace mktpack {
namespace {
using namespace storage;
namespace fs=std::filesystem;
using namespace std::chrono;
struct File {
    int64_t id, first, last, volume;
    std::string path;
};
struct Bucket {
    int64_t start, end;
    std::vector<File> files;
    bool copied() const { return files.size()==1 && files[0].first>=start && files[0].last<end; }
};
std::vector<uint8_t> encode_bucket(const fs::path& source,const Bucket& bucket) {
    Database input(source);
    PayloadReader reader(input);
    std::vector<Table> tables;
    for (const auto& file:bucket.files) {
        auto table=decode(reader.read(file.id,file.path,file.volume));
        slice(table,bucket.start,bucket.end);
        if (!table.columns.at("time").empty()) tables.push_back(std::move(table));
    }
    if (tables.empty()) return {};
    auto combined=merge(tables);
    // 混合无损和有损来源时保存已读出的值，避免再量化无损部分
    for (auto& [name,schema]:combined.schema.items()) {
        if (!schema.contains("encoding")) continue;
        for (const auto& table:tables) {
            if (table.schema.at(name).value("encoding","")!=schema.value("encoding","")) {
                schema.erase("encoding");
                schema.erase("reference");
                break;
            }
        }
    }
    return encode(combined);
}
int year_of(int64_t time) {
    return int(year_month_day{floor<days>(sys_time<milliseconds>{milliseconds{time}})}.year());
}
int64_t year_start(int value) {
    return duration_cast<milliseconds>(sys_days{year{value}/1/1}.time_since_epoch()).count();
}
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
    void close() {
        if (!current) return;
        bytes+=database_size(*current);
        current.reset();
    }
    int64_t append_entry(const std::string& label,const std::function<void(Database&)>& write) {
        for (;;) {
            if (!current) {
                ++count;
                current=std::make_unique<Database>(root/("mkt.data."+std::to_string(count)),SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
                current->exec("PRAGMA journal_mode=DELETE");
                entries=0;
            }
            current->exec("BEGIN");
            write(*current);
            const auto size=database_size(*current);
            if (size<=limit) {
                current->exec("COMMIT");
                ++entries;
                return count;
            }
            current->exec("ROLLBACK");
            if (!entries)
                throw std::runtime_error(label+"需要至少 "+std::to_string(size)+
                                         " 字节分卷，超过 --max-file-size="+std::to_string(limit));
            close();
        }
    }
    int64_t append(int64_t id,const std::vector<uint8_t>& payload,const std::string& label) {
        return append_entry(label+" 的年度载荷",[&](Database& db) {
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
}

Json package(const fs::path& source,const fs::path& destination,const PackageOptions& options) {
    if (options.start>=options.end) throw std::runtime_error("打包时间范围必须满足 start < end");
    if (!options.max_file_size) throw std::runtime_error("分卷上限必须大于零");
    if (!options.jobs) throw std::runtime_error("并行任务数必须大于零");
    if (fs::exists(destination) && (!fs::is_directory(destination) || !fs::is_empty(destination)))
        throw std::runtime_error("打包目的地必须为空："+destination.string());
    Database input(source);
    input.exec("BEGIN");
    PayloadReader reader(input);
    std::set<std::string> selected, excluded;
    {
        std::set<std::string> known;
        Statement groups(input,"SELECT name,kind FROM __group__ ORDER BY name");
        while (groups.next()) {
            const auto name=groups.text(0);
            known.insert(name);
            if (groups.text(1)!="timeseries") continue;
            if (options.groups.empty() || std::find(options.groups.begin(),options.groups.end(),name)!=options.groups.end())
                selected.insert(name);
            else excluded.insert(name);
        }
        for (const auto& name:options.groups)
            if (!known.contains(name)) throw std::runtime_error("未知数据组："+name);
    }
    std::map<std::tuple<std::string,std::string,int>,Bucket> buckets;
    {
        Statement files(input,"SELECT f.id,g.name,s.name,g.name||'/'||s.name||'/'||f.name,first_time,last_time,volume "
                              "FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id JOIN __symbols__ s ON s.id=f.symbol_id "
                              "WHERE first_time<? AND last_time>=? ORDER BY g.name,s.name,first_time,f.name,f.id");
        files.integer(1,options.end); files.integer(2,options.start);
        while (files.next()) {
            const auto group=files.text(1), symbol=files.text(2);
            if (!selected.contains(group)) continue;
            if (!options.symbols.empty() && std::find(options.symbols.begin(),options.symbols.end(),symbol)==options.symbols.end()) continue;
            const File file{files.integer(0),files.integer(4),files.integer(5),files.integer(6),files.text(3)};
            const auto first=std::max(file.first,options.start), last=std::min(file.last,options.end-1);
            for (int y=year_of(first);y<=year_of(last);++y) {
                auto& bucket=buckets[{group,symbol,y}];
                bucket.start=std::max(options.start,year_start(y));
                bucket.end=std::min(options.end,year_start(y+1));
                bucket.files.push_back(file);
            }
        }
    }
    if (options.progress) options.progress("发现 "+std::to_string(buckets.size())+" 个组/合约/年度分区");
    fs::create_directories(destination);
    const auto work=destination/".mkt-package";
    fs::create_directory(work);
    Json result={{"files",0},{"rows",0},{"copied",0},{"reencoded",0},{"warnings",Json::array()}};
    try {
        {
            Database output(work/"mkt.sqlite",SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
            copy_index(input,output);
            output.exec("DELETE FROM __timefile__; PRAGMA application_id=0x4d4b5450");
            for (const auto& group:excluded) {
                Statement groups(output,"DELETE FROM __group__ WHERE name=?");
                groups.text(1,group); groups.next();
            }
            output.exec("DELETE FROM __sqlite_partitions__");
            // 外置普通数据库完整携带，与容量分卷独立
            uint64_t ordinary_bytes=0;
            Statement ordinary(input,"SELECT DISTINCT location FROM __group__ WHERE kind='ordinary' AND location<>''");
            while(ordinary.next()) {
                const auto location=ordinary.text(0);
                fs::create_directories((work/location).parent_path());
                Database src(input.root/location),dst(work/location,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
                copy_index(src,dst);
                ordinary_bytes+=database_size(dst);
            }
            int64_t sqlite_rows=0;
            Volumes volumes(work,options.max_file_size);
            std::map<std::pair<std::string,std::string>,Json> schemas;
            int64_t id=0, rows=0;
            size_t processed=0;
            output.exec("BEGIN");
            std::deque<std::future<std::vector<uint8_t>>> pending;
            auto next=buckets.begin();
            const auto submit=[&] {
                while (next!=buckets.end() && next->second.copied()) ++next;
                if (next==buckets.end()) return false;
                const auto* bucket=&(next++)->second;
                pending.push_back(std::async(options.jobs==1?std::launch::deferred:std::launch::async,
                    [&,bucket] { return encode_bucket(source,*bucket); }));
                return true;
            };
            for (size_t i=0;i<options.jobs && submit();++i) {}
            for (const auto& entry:buckets) {
                const auto& [group,symbol,y]=entry.first;
                const auto& bucket=entry.second;
                const auto label=group+"/"+symbol+"/"+std::to_string(y)+".mkp";
                const bool copy=bucket.copied();
                std::vector<uint8_t> payload;
                if (copy) {
                    const auto& file=bucket.files[0];
                    payload=reader.read(file.id,file.path,file.volume);
                } else {
                    payload=pending.front().get();
                    pending.pop_front();
                    submit();
                }
                if (payload.empty()) continue;
                const auto header=info(payload);
                const auto volume=volumes.append(++id,payload,label);
                Statement file(output,"INSERT INTO __timefile__(id,group_id,symbol_id,name,first_time,last_time,rows,volume) VALUES(?,?,?,?,?,?,?,?)");
                file.integer(1,id); file.integer(2,group_id(output,group)); file.integer(3,symbol_id(output,symbol)); file.text(4,std::to_string(y)+".mkp");
                file.integer(5,header.at("first_time")); file.integer(6,header.at("last_time")); file.integer(7,header.at("rows"));
                file.integer(8,volume);
                file.next();
                for (const auto& descriptor:header.at("columns")) {
                    const auto name=descriptor.at("name").get<std::string>();
                    auto& schema=schemas[{group,name}];
                    if (schema.is_null()) {
                        Statement original(input,"SELECT schema FROM __group__ WHERE name=?");
                        original.text(1,group);
                        schema=original.next()?Json::parse(original.text(0)).value(name,Json::object()):Json::object();
                        schema["scale"]=descriptor.value("scale",0);
                    } else schema["scale"]=std::max(schema.value("scale",0),descriptor.value("scale",0));
                    schema["type"]=descriptor.at("type");
                }
                rows+=header.at("rows").get<int64_t>();
                const std::string kind=copy?"copied":"reencoded";
                result[kind]=result[kind].get<size_t>()+1;
                if (options.progress) options.progress(label+" "+(copy?"复制":"合并编码")+" → mkt.data."+std::to_string(volume)+
                    " ("+std::to_string(++processed)+"/"+std::to_string(buckets.size())+")");
            }
            std::map<std::string,Json> group_schemas;
            for (const auto& [key,schema]:schemas) group_schemas[key.first][key.second]=schema;
            for (const auto& [group,schema]:group_schemas) {
                Statement update(output,"UPDATE __group__ SET schema=? WHERE name=?");
                update.text(1,schema.dump()); update.text(2,group); update.next();
            }
            for(const auto& name:selected) {
                const auto spec=group_spec(input,name);
                if(spec.storage!="sqlite") continue;
                SqlReader reader(input,spec);
                const auto parts=reader.partitions(options.start,options.end);
                auto destination_spec=spec;
                destination_spec.table="data_"+std::to_string(spec.id);
                Statement update(output,"UPDATE __group__ SET location='',table_name=? WHERE id=?");
                update.text(1,destination_spec.table); update.integer(2,spec.id); update.next();
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
                    char label[8]; snprintf(label,sizeof label,"%04d-%02u",int(month.year()),unsigned(month.month()));
                    const auto volume=volumes.append_entry(name+" "+label+" 的整月数据",[&](Database& db) { copy_rows(db,destination_spec,create,records); });
                    const auto column=std::find(records.columns.begin(),records.columns.end(),spec.time)-records.columns.begin();
                    int64_t minimum=INT64_MAX,maximum=INT64_MIN;
                    for(const auto& row:records.rows) { minimum=std::min(minimum,row[column].get<int64_t>());maximum=std::max(maximum,row[column].get<int64_t>()); }
                    Statement part(output,"INSERT INTO __sqlite_partitions__ VALUES(?,?,?,?,?,?,?)");
                    part.integer(1,spec.id); part.integer(2,start); part.integer(3,end); part.integer(4,volume);
                    part.integer(5,records.rows.size()); part.integer(6,minimum); part.integer(7,maximum); part.next();
                    sqlite_rows+=records.rows.size();
                    if(options.progress) options.progress(name+" "+label+" → mkt.data."+std::to_string(volume));
                }
            }
            rows+=sqlite_rows;
            output.exec("COMMIT; VACUUM");
            volumes.close();
            const auto index_size=database_size(output);
            if (index_size>options.max_file_size)
                result["warnings"].push_back("mkt.sqlite 为 "+std::to_string(index_size)+" 字节，超过分卷上限 "+std::to_string(options.max_file_size));
            result["files"]=id; result["rows"]=rows; result["sqlite_rows"]=sqlite_rows;
            result["volumes"]=volumes.count;
            result["index_bytes"]=index_size;
            result["bytes"]=index_size+volumes.bytes+ordinary_bytes;
        }
        for (const auto& entry:fs::directory_iterator(work)) fs::rename(entry.path(),destination/entry.path().filename());
        fs::remove(work);
    } catch (...) {
        fs::remove_all(work);
        throw;
    }
    return result;
}
}

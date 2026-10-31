#include <mktpack/storage.hpp>
#include "database.hpp"
#include <chrono>
#include <set>
#include <tuple>

namespace mktpack {
namespace {
using namespace storage;
namespace fs=std::filesystem;
using namespace std::chrono;
struct File {
    int64_t id, first, last;
    std::string path;
};
struct Bucket {
    int64_t start, end;
    std::vector<File> files;
};
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
    std::string append(int64_t id,const std::vector<uint8_t>& payload,const std::string& label) {
        for (;;) {
            if (!current) {
                ++count;
                current=std::make_unique<Database>(root/("mkt.data."+std::to_string(count)),SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
                current->exec("PRAGMA journal_mode=DELETE; CREATE TABLE blob(id INTEGER PRIMARY KEY,bytes BLOB NOT NULL)");
                entries=0;
            }
            current->exec("BEGIN");
            {
                Statement insert(*current,"INSERT INTO blob(id,bytes) VALUES(?,?)");
                insert.integer(1,id);
                if (sqlite3_bind_blob64(insert.statement,2,payload.data(),payload.size(),SQLITE_TRANSIENT)!=SQLITE_OK)
                    throw std::runtime_error(sqlite3_errmsg(current->db));
                insert.next();
            }
            const auto size=database_size(*current);
            if (size<=limit) {
                current->exec("COMMIT");
                ++entries;
                return current->path.filename().string();
            }
            current->exec("ROLLBACK");
            if (!entries)
                throw std::runtime_error(label+" 的年度载荷需要至少 "+std::to_string(size)+
                                         " 字节分卷，超过 --max-file-size="+std::to_string(limit));
            close();
        }
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
        Statement files(input,"SELECT rowid,group_name,symbol,path,first_time,last_time FROM __timefile__ "
                              "WHERE first_time<? AND last_time>=? ORDER BY group_name,symbol,first_time,path,rowid");
        files.integer(1,options.end); files.integer(2,options.start);
        while (files.next()) {
            const auto group=files.text(1), symbol=files.text(2);
            if (!selected.contains(group)) continue;
            if (!options.symbols.empty() && std::find(options.symbols.begin(),options.symbols.end(),symbol)==options.symbols.end()) continue;
            const File file{files.integer(0),files.integer(4),files.integer(5),files.text(3)};
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
            output.exec("DELETE FROM __timefile__; DROP TABLE IF EXISTS __blob__; "
                        "CREATE TABLE __blob__(blob_id INTEGER PRIMARY KEY,volume TEXT NOT NULL)");
            for (const auto& group:excluded) {
                Statement columns(output,"DELETE FROM __col__ WHERE group_name=?");
                columns.text(1,group); columns.next();
                Statement groups(output,"DELETE FROM __group__ WHERE name=?");
                groups.text(1,group); groups.next();
            }
            Volumes volumes(work,options.max_file_size);
            std::map<std::pair<std::string,std::string>,Json> schemas;
            int64_t id=0, rows=0;
            size_t processed=0;
            output.exec("BEGIN");
            for (const auto& [key,bucket]:buckets) {
                const auto& [group,symbol,y]=key;
                const auto label=group+"/"+symbol+"/"+std::to_string(y)+".mkp";
                const bool copy=bucket.files.size()==1 && bucket.files[0].first>=bucket.start && bucket.files[0].last<bucket.end;
                std::vector<uint8_t> payload;
                if (copy) {
                    payload=reader.read(bucket.files[0].id,bucket.files[0].path);
                } else {
                    std::vector<Table> tables;
                    for (const auto& file:bucket.files) {
                        auto table=decode(reader.read(file.id,file.path));
                        slice(table,bucket.start,bucket.end);
                        if (!table.columns.at("time").empty()) tables.push_back(std::move(table));
                    }
                    if (tables.empty()) continue;
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
                    payload=encode(combined);
                }
                const auto header=info(payload);
                const auto volume=volumes.append(++id,payload,label);
                Statement file(output,"INSERT INTO __timefile__(rowid,group_name,symbol,path,first_time,last_time,rows) VALUES(?,?,?,?,?,?,?)");
                file.integer(1,id); file.text(2,group); file.text(3,symbol); file.text(4,label);
                file.integer(5,header.at("first_time")); file.integer(6,header.at("last_time")); file.integer(7,header.at("rows"));
                file.next();
                Statement mapping(output,"INSERT INTO __blob__(blob_id,volume) VALUES(?,?)");
                mapping.integer(1,id); mapping.text(2,volume); mapping.next();
                for (const auto& descriptor:header.at("columns")) {
                    const auto name=descriptor.at("name").get<std::string>();
                    auto& schema=schemas[{group,name}];
                    if (schema.is_null()) {
                        Statement original(input,"SELECT schema FROM __col__ WHERE group_name=? AND name=?");
                        original.text(1,group); original.text(2,name);
                        schema=original.next()?Json::parse(original.text(0)):Json::object();
                        schema["scale"]=descriptor.value("scale",0);
                    } else schema["scale"]=std::max(schema.value("scale",0),descriptor.value("scale",0));
                    schema["type"]=descriptor.at("type");
                }
                rows+=header.at("rows").get<int64_t>();
                const std::string kind=copy?"copied":"reencoded";
                result[kind]=result[kind].get<size_t>()+1;
                if (options.progress) options.progress(label+" "+(copy?"复制":"合并编码")+" → "+volume+
                    " ("+std::to_string(++processed)+"/"+std::to_string(buckets.size())+")");
            }
            for (const auto& [key,schema]:schemas) {
                Statement update(output,"UPDATE __col__ SET schema=? WHERE group_name=? AND name=?");
                update.text(1,schema.dump()); update.text(2,key.first); update.text(3,key.second); update.next();
            }
            output.exec("COMMIT; VACUUM");
            volumes.close();
            const auto index_size=database_size(output);
            if (index_size>options.max_file_size)
                result["warnings"].push_back("mkt.sqlite 为 "+std::to_string(index_size)+" 字节，超过分卷上限 "+std::to_string(options.max_file_size));
            result["files"]=id; result["rows"]=rows;
            result["volumes"]=volumes.count;
            result["index_bytes"]=index_size;
            result["bytes"]=index_size+volumes.bytes;
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

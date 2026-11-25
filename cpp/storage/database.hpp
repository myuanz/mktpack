#pragma once
#include <mktpack/core.hpp>
#include <sqlite3.h>
#include <algorithm>
#include <fstream>
#include <memory>

namespace mktpack::storage {
struct Database {
    sqlite3* db=nullptr;
    std::filesystem::path root, path;
    explicit Database(std::filesystem::path location, int flags=SQLITE_OPEN_READONLY) {
        path=std::filesystem::is_directory(location)?location/"mkt.sqlite":location;
        root=path.parent_path();
        if (sqlite3_open_v2(path.c_str(),&db,flags,nullptr)!=SQLITE_OK) {
            const std::string error=sqlite3_errmsg(db);
            sqlite3_close(db);
            throw std::runtime_error("无法打开索引："+path.string()+"："+error);
        }
        sqlite3_busy_timeout(db,60000);
    }
    Database(const Database&)=delete;
    ~Database() { sqlite3_close(db); }
    void exec(const std::string& sql) {
        char* error=nullptr;
        if (sqlite3_exec(db,sql.c_str(),nullptr,nullptr,&error)!=SQLITE_OK) {
            const std::string message=error;
            sqlite3_free(error);
            throw std::runtime_error(message);
        }
    }
};

struct Statement {
    sqlite3_stmt* statement=nullptr;
    Statement(Database& database, const std::string& sql) {
        if (sqlite3_prepare_v2(database.db,sql.c_str(),-1,&statement,nullptr)!=SQLITE_OK)
            throw std::runtime_error(sqlite3_errmsg(database.db));
    }
    Statement(const Statement&)=delete;
    ~Statement() { sqlite3_finalize(statement); }
    void text(int index, const std::string& value) {
        sqlite3_bind_text(statement,index,value.c_str(),-1,SQLITE_TRANSIENT);
    }
    void integer(int index, int64_t value) { sqlite3_bind_int64(statement,index,value); }
    bool next() {
        const int status=sqlite3_step(statement);
        if (status!=SQLITE_ROW && status!=SQLITE_DONE)
            throw std::runtime_error(sqlite3_errmsg(sqlite3_db_handle(statement)));
        return status==SQLITE_ROW;
    }
    std::string text(int index) const {
        const auto value=sqlite3_column_text(statement,index);
        return value?reinterpret_cast<const char*>(value):"";
    }
    int64_t integer(int index) const { return sqlite3_column_int64(statement,index); }
};

inline std::string identifier(const std::string& name) {
    std::string result="\"";
    for (char c:name) { result+=c; if (c=='"') result+=c; }
    return result+'"';
}

inline void create_index(Database& db) {
    db.exec("CREATE TABLE IF NOT EXISTS __group__(id INTEGER PRIMARY KEY,name TEXT UNIQUE,kind TEXT,schema TEXT NOT NULL DEFAULT '{}',"
            "storage TEXT NOT NULL DEFAULT 'mkp',location TEXT NOT NULL DEFAULT '',table_name TEXT NOT NULL DEFAULT '',"
            "time_column TEXT NOT NULL DEFAULT 'time',symbol_column TEXT NOT NULL DEFAULT 'symbol_id');"
            "CREATE TABLE IF NOT EXISTS __symbols__(id INTEGER PRIMARY KEY,name TEXT UNIQUE);"
            "CREATE TABLE IF NOT EXISTS __volumes__(id INTEGER PRIMARY KEY,path TEXT NOT NULL UNIQUE);"
            "CREATE TABLE IF NOT EXISTS __timefile__(id INTEGER PRIMARY KEY,group_id INTEGER NOT NULL,symbol_id INTEGER NOT NULL,period INTEGER NOT NULL,first_time INTEGER NOT NULL,last_time INTEGER NOT NULL,rows INTEGER NOT NULL,volume_id INTEGER,UNIQUE(group_id,symbol_id,period));"
            "CREATE TABLE IF NOT EXISTS __sqlite_partitions__(group_id INTEGER,start INTEGER,end INTEGER,volume_id INTEGER,rows INTEGER,first_time INTEGER,last_time INTEGER,PRIMARY KEY(group_id,start)) WITHOUT ROWID;");
}

inline int64_t symbol_id(Database& db,const std::string& symbol) {
    Statement insert(db,"INSERT OR IGNORE INTO __symbols__(name) VALUES(?)");
    insert.text(1,symbol); insert.next();
    Statement query(db,"SELECT id FROM __symbols__ WHERE name=?");
    query.text(1,symbol); query.next();
    return query.integer(0);
}

inline int64_t group_id(Database& db,const std::string& group) {
    Statement insert(db,"INSERT OR IGNORE INTO __group__(name,kind) VALUES(?,'timeseries')");
    insert.text(1,group); insert.next();
    Statement query(db,"SELECT id FROM __group__ WHERE name=?");
    query.text(1,group); query.next();
    return query.integer(0);
}


class PayloadReader {
    Database& index;
    std::map<std::string,std::unique_ptr<Database>> volumes;
    Database& volume(const std::string& path) {
        auto& result=volumes[path];
        if(!result) result=std::make_unique<Database>(index.root/std::filesystem::path(path));
        return *result;
    }
public:
    explicit PayloadReader(Database& database):index(database) {}
    std::vector<uint8_t> read(int64_t id,const std::string& path,const std::string& volume_path) {
        if (volume_path.empty()) {
            std::ifstream input(index.root/path,std::ios::binary|std::ios::ate);
            if (!input) throw std::runtime_error("无法读取："+(index.root/path).string());
            std::vector<uint8_t> bytes(input.tellg());
            input.seekg(0);
            input.read(reinterpret_cast<char*>(bytes.data()),bytes.size());
            if (!input) throw std::runtime_error("载荷读取失败："+path);
            return bytes;
        }
        Statement query(volume(volume_path),"SELECT bytes FROM blob WHERE id=?");
        query.integer(1,id);
        if (!query.next()) throw std::runtime_error("分卷缺少载荷："+path);
        const auto* data=static_cast<const uint8_t*>(sqlite3_column_blob(query.statement,0));
        const auto size=sqlite3_column_bytes(query.statement,0);
        if (!size) throw std::runtime_error("分卷载荷为空："+path);
        return {data,data+size};
    }
    std::map<int64_t,std::vector<uint8_t>> read_volume(const std::string& path,const std::vector<int64_t>& ids) {
        std::map<int64_t,std::vector<uint8_t>> result;
        constexpr size_t batch=500;
        for(size_t begin=0;begin<ids.size();begin+=batch) {
            const auto end=std::min(ids.size(),begin+batch);
            std::string sql="SELECT id,bytes FROM blob WHERE id IN (";
            for(size_t i=begin;i<end;++i) sql+=(i==begin?"?":",?");
            Statement query(volume(path),sql+")");
            for(size_t i=begin;i<end;++i)query.integer(int(i-begin+1),ids[i]);
            while(query.next()) {
                const auto* data=static_cast<const uint8_t*>(sqlite3_column_blob(query.statement,1));
                const auto size=sqlite3_column_bytes(query.statement,1);
                result.emplace(query.integer(0),std::vector<uint8_t>(data,data+size));
            }
        }
        if(result.size()!=ids.size()) throw std::runtime_error("分卷缺少载荷："+path);
        return result;
    }
};

inline void slice(Table& table, int64_t start, int64_t end) {
    const auto& time=table.columns.at("time");
    const auto first=std::lower_bound(time.begin(),time.end(),start)-time.begin();
    const auto last=std::lower_bound(time.begin(),time.end(),end)-time.begin();
    for (auto& [name,values]:table.columns)
        values=std::vector<int64_t>(values.begin()+first,values.begin()+last);
}
}

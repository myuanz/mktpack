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

inline bool has_table(Database& db, const std::string& name) {
    Statement query(db,"SELECT 1 FROM sqlite_master WHERE type='table' AND name=?");
    query.text(1,name);
    return query.next();
}

class PayloadReader {
    Database& index;
    bool packed;
    std::map<std::string,std::unique_ptr<Database>> volumes;
public:
    explicit PayloadReader(Database& database):index(database),packed(has_table(database,"__blob__")) {}
    std::vector<uint8_t> read(int64_t id, const std::string& path) {
        if (!packed) {
            std::ifstream input(index.root/path,std::ios::binary|std::ios::ate);
            if (!input) throw std::runtime_error("无法读取："+(index.root/path).string());
            std::vector<uint8_t> bytes(input.tellg());
            input.seekg(0);
            input.read(reinterpret_cast<char*>(bytes.data()),bytes.size());
            if (!input) throw std::runtime_error("载荷读取失败："+path);
            return bytes;
        }
        Statement location(index,"SELECT volume FROM __blob__ WHERE blob_id=?");
        location.integer(1,id);
        if (!location.next()) throw std::runtime_error("缺少分卷映射："+path);
        const auto name=location.text(0);
        auto& volume=volumes[name];
        if (!volume) volume=std::make_unique<Database>(index.root/name);
        Statement query(*volume,"SELECT bytes FROM blob WHERE id=?");
        query.integer(1,id);
        if (!query.next()) throw std::runtime_error("分卷缺少载荷："+path);
        const auto* data=static_cast<const uint8_t*>(sqlite3_column_blob(query.statement,0));
        const auto size=sqlite3_column_bytes(query.statement,0);
        if (!size) throw std::runtime_error("分卷载荷为空："+path);
        return {data,data+size};
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

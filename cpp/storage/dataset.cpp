#include <mktpack/storage.hpp>
#include <sqlite3.h>
#include <algorithm>

namespace mktpack {
namespace {
struct Database {
    sqlite3* db=nullptr;
    std::filesystem::path root;
    explicit Database(std::filesystem::path path) {
        if (std::filesystem::is_directory(path)) {
            root=path;
            path/="mkt.sqlite";
        } else {
            root=path.parent_path();
        }
        if (sqlite3_open_v2(path.c_str(),&db,SQLITE_OPEN_READONLY,nullptr)!=SQLITE_OK) {
            throw std::runtime_error("无法读取索引");
        }
    }
    ~Database() { sqlite3_close(db); }
};

struct Statement {
    sqlite3_stmt* statement=nullptr;
    Statement(Database& database, const char* sql) {
        if (sqlite3_prepare_v2(database.db,sql,-1,&statement,nullptr)!=SQLITE_OK) {
            throw std::runtime_error(sqlite3_errmsg(database.db));
        }
    }
    ~Statement() { sqlite3_finalize(statement); }
    void text(int index, const std::string& value) {
        sqlite3_bind_text(statement,index,value.c_str(),-1,SQLITE_TRANSIENT);
    }
    bool next() { return sqlite3_step(statement)==SQLITE_ROW; }
    std::string text(int index) {
        const auto value=sqlite3_column_text(statement,index);
        return value?(const char*)value:"";
    }
};
}

Json describe(const std::filesystem::path& root, const std::string& group) {
    Database database(root);
    if (group.empty()) {
        Statement query(database,"SELECT name,kind FROM __group__ ORDER BY name");
        Json result=Json::array();
        while (query.next()) result.push_back({{"name",query.text(0)},{"kind",query.text(1)}});
        return result;
    }
    Statement columns(database,"SELECT name,schema FROM __col__ WHERE group_name=? ORDER BY name");
    columns.text(1,group);
    Json schema=Json::object();
    while (columns.next()) schema[columns.text(0)]=Json::parse(columns.text(1));
    Statement stats(database,"SELECT sum(rows),min(first_time),max(last_time) FROM __timefile__ WHERE group_name=?");
    stats.text(1,group);
    stats.next();
    return {{"name",group},{"schema",schema},
            {"rows",sqlite3_column_int64(stats.statement,0)},
            {"first_time",sqlite3_column_int64(stats.statement,1)},
            {"last_time",sqlite3_column_int64(stats.statement,2)}};
}

Json ordinary(const std::filesystem::path& root, const std::string& group) {
    Database database(root);
    Statement exists(database,"SELECT kind FROM __group__ WHERE name=?");
    exists.text(1,group);
    if (!exists.next() || exists.text(0)!="ordinary") throw std::runtime_error("普通表不存在");
    std::string quoted="\"";
    for (char c:group) { quoted+=c; if(c=='"') quoted+=c; }
    quoted+='"';
    Statement query(database,("SELECT * FROM "+quoted).c_str());
    Json result=Json::object();
    if (!query.next()) return result;
    for (int i=0;i<sqlite3_column_count(query.statement);++i) {
        const std::string key=sqlite3_column_name(query.statement,i);
        const int type=sqlite3_column_type(query.statement,i);
        if (type==SQLITE_INTEGER) result[key]=sqlite3_column_int64(query.statement,i);
        else if (type==SQLITE_FLOAT) result[key]=sqlite3_column_double(query.statement,i);
        else if (type==SQLITE_NULL) result[key]=nullptr;
        else {
            const auto text=query.text(i);
            auto parsed=Json::parse(text,nullptr,false);
            result[key]=!parsed.is_discarded() && parsed.is_structured()?parsed:Json(text);
        }
    }
    return result;
}

std::vector<Table> query(const std::filesystem::path& root, const std::string& group,
                         const std::vector<std::string>& symbols, int64_t start, int64_t end) {
    Database database(root);
    Statement files(database,"SELECT symbol,path FROM __timefile__ WHERE group_name=? "
                             "AND first_time<? AND last_time>=? ORDER BY symbol,first_time,path");
    files.text(1,group);
    sqlite3_bind_int64(files.statement,2,end);
    sqlite3_bind_int64(files.statement,3,start);
    std::map<std::string,std::vector<Table>> grouped;
    while (files.next()) {
        const auto symbol=files.text(0);
        if (!symbols.empty() && std::find(symbols.begin(),symbols.end(),symbol)==symbols.end()) continue;
        auto table=read(database.root/files.text(1));
        const auto& time=table.columns.at("time");
        const auto first=std::lower_bound(time.begin(),time.end(),start)-time.begin();
        const auto last=std::lower_bound(time.begin(),time.end(),end)-time.begin();
        if (first==last) continue;
        for (auto& [name,values]:table.columns) {
            values=std::vector<int64_t>(values.begin()+first,values.begin()+last);
        }
        grouped[symbol].push_back(std::move(table));
    }
    std::vector<Table> result;
    for (const auto& [symbol,tables]:grouped) result.push_back(merge(tables));
    return result;
}
}

#include <mktpack/storage.hpp>
#include "database.hpp"
#include <algorithm>

namespace mktpack {
using namespace storage;

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
    PayloadReader reader(database);
    Statement files(database,"SELECT symbol,path,rowid FROM __timefile__ WHERE group_name=? "
                             "AND first_time<? AND last_time>=? ORDER BY symbol,first_time,path");
    files.text(1,group);
    sqlite3_bind_int64(files.statement,2,end);
    sqlite3_bind_int64(files.statement,3,start);
    std::map<std::string,std::vector<Table>> grouped;
    while (files.next()) {
        const auto symbol=files.text(0);
        if (!symbols.empty() && std::find(symbols.begin(),symbols.end(),symbol)==symbols.end()) continue;
        auto table=decode(reader.read(files.integer(2),files.text(1)));
        slice(table,start,end);
        if (table.columns.at("time").empty()) continue;
        grouped[symbol].push_back(std::move(table));
    }
    std::vector<Table> result;
    for (const auto& [symbol,tables]:grouped) result.push_back(merge(tables));
    return result;
}

Json check_dataset(const std::filesystem::path& root) {
    Database database(root);
    PayloadReader reader(database);
    Statement files(database,"SELECT rowid,path,first_time,last_time,rows FROM __timefile__ ORDER BY rowid");
    Json errors=Json::array();
    size_t count=0;
    while (files.next()) {
        const auto path=files.text(1);
        try {
            const auto table=decode(reader.read(files.integer(0),path));
            const auto& time=table.columns.at("time");
            if (time.empty() || time.front()!=files.integer(2) || time.back()!=files.integer(3)
                || time.size()!=size_t(files.integer(4))) errors.push_back(path);
        } catch (const std::exception& error) {
            errors.push_back(path+": "+error.what());
        }
        ++count;
    }
    return {{"files",count},{"errors",errors}};
}
}

#include <mktpack/storage.hpp>
#include "sqlite.hpp"
#include <algorithm>
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

std::vector<ReadTable> query(const std::filesystem::path& root, const std::string& group,
                         const std::vector<std::string>& symbols, int64_t start, int64_t end) {
    Database database(root);
    const auto spec=group_spec(database,group);
    if(spec.storage=="sqlite") {
        SqlReader reader(database,spec);
        return {reader.query(start,end,symbols)};
    }
    PayloadReader reader(database);
    Statement files(database,"SELECT s.name,g.name||'/'||s.name||'/'||f.name,f.id,f.volume FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id JOIN __symbols__ s ON s.id=f.symbol_id WHERE g.name=? "
                             "AND first_time<? AND last_time>=? ORDER BY s.name,first_time,f.name");
    files.text(1,group);
    sqlite3_bind_int64(files.statement,2,end);
    sqlite3_bind_int64(files.statement,3,start);
    std::map<std::string,std::vector<Table>> grouped;
    while (files.next()) {
        const auto symbol=files.text(0);
        if (!symbols.empty() && std::find(symbols.begin(),symbols.end(),symbol)==symbols.end()) continue;
        auto table=decode(reader.read(files.integer(2),files.text(1),files.integer(3)));
        slice(table,start,end);
        if (table.columns.at("time").empty()) continue;
        grouped[symbol].push_back(std::move(table));
    }
    std::vector<ReadTable> result;
    for (const auto& [symbol,tables]:grouped) {
        auto table=merge(tables);
        result.push_back({table.columns,table.schema,table.metadata});
    }
    return result;
}

Json check_dataset(const std::filesystem::path& root) {
    Database database(root);
    PayloadReader reader(database);
    Statement files(database,"SELECT f.id,g.name||'/'||s.name||'/'||f.name,first_time,last_time,rows,volume FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id JOIN __symbols__ s ON s.id=f.symbol_id ORDER BY f.id");
    Json errors=Json::array();
    size_t count=0;
    while (files.next()) {
        const auto path=files.text(1);
        try {
            const auto table=decode(reader.read(files.integer(0),path,files.integer(5)));
            const auto& time=table.columns.at("time");
            if (time.empty() || time.front()!=files.integer(2) || time.back()!=files.integer(3)
                || time.size()!=size_t(files.integer(4))) errors.push_back(path);
        } catch (const std::exception& error) {
            errors.push_back(path+": "+error.what());
        }
        ++count;
    }
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
            std::set<int64_t> checked;
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

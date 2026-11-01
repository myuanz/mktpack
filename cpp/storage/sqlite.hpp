#pragma once
#include "database.hpp"
#include <mktpack/storage.hpp>

namespace mktpack::storage {
struct GroupSpec {
    int64_t id;
    std::string name,kind,storage,location,table,time,symbol;
    Json schema;
};
inline GroupSpec group_spec(Database& db,const std::string& name) {
    Statement q(db,"SELECT id,name,kind,storage,location,table_name,time_column,symbol_column,schema FROM __group__ WHERE name=?");
    q.text(1,name);
    if (!q.next()) throw std::runtime_error("未知数据组："+name);
    return {q.integer(0),q.text(1),q.text(2),q.text(3),q.text(4),q.text(5).empty()?name:q.text(5),q.text(6),q.text(7),Json::parse(q.text(8))};
}
inline Json sql_value(sqlite3_stmt* statement,int column) {
    switch (sqlite3_column_type(statement,column)) {
    case SQLITE_NULL: return nullptr;
    case SQLITE_INTEGER: return sqlite3_column_int64(statement,column);
    case SQLITE_FLOAT: return sqlite3_column_double(statement,column);
    case SQLITE_BLOB: {
        const auto* data=static_cast<const uint8_t*>(sqlite3_column_blob(statement,column));
        const auto size=sqlite3_column_bytes(statement,column);
        return Json::binary(size?std::vector<uint8_t>(data,data+size):std::vector<uint8_t>{});
    }
    default: return std::string(reinterpret_cast<const char*>(sqlite3_column_text(statement,column)),sqlite3_column_bytes(statement,column));
    }
}
inline void bind_value(Statement& statement,int column,const Json& value) {
    auto* s=statement.statement;
    if (value.is_null()) sqlite3_bind_null(s,column);
    else if (value.is_number_integer()) sqlite3_bind_int64(s,column,value.get<int64_t>());
    else if (value.is_number_float()) sqlite3_bind_double(s,column,value.get<double>());
    else if (value.is_binary()) {
        const auto& bytes=value.get_binary();
        sqlite3_bind_blob64(s,column,bytes.empty()?static_cast<const void*>(""):bytes.data(),bytes.size(),SQLITE_TRANSIENT);
    } else {
        const auto& text=value.get_ref<const std::string&>();
        sqlite3_bind_text64(s,column,text.data(),text.size(),SQLITE_TRANSIENT,SQLITE_UTF8);
    }
}
struct SqlPartition { int64_t start,end,volume,rows,first,last; };
struct SqlRows { std::vector<std::string> columns; std::vector<std::vector<Json>> rows; };

class SqlReader {
    Database& index;
    std::map<int64_t,std::unique_ptr<Database>> databases;
public:
    GroupSpec group;
    SqlReader(Database& db,GroupSpec spec):index(db),group(std::move(spec)) {}
    Database& database(int64_t volume=0) {
        if (!volume && group.location.empty()) return index;
        auto& db=databases[volume];
        if (!db) db=std::make_unique<Database>(index.root/(volume?"mkt.data."+std::to_string(volume):group.location));
        return *db;
    }
    std::vector<SqlPartition> partitions(int64_t start=INT64_MIN,int64_t end=INT64_MAX) {
        std::vector<SqlPartition> result;
        if (!group.location.empty()) {
            Statement q(database(),"SELECT count(*),min("+identifier(group.time)+"),max("+identifier(group.time)+") FROM "+identifier(group.table));
            q.next();
            if (q.integer(0)) result.push_back({INT64_MIN,INT64_MAX,0,q.integer(0),q.integer(1),q.integer(2)});
        } else {
            Statement q(index,"SELECT start,end,volume,rows,first_time,last_time FROM __sqlite_partitions__ WHERE group_id=? AND start<? AND end>? ORDER BY start");
            q.integer(1,group.id); q.integer(2,end); q.integer(3,start);
            while(q.next()) result.push_back({q.integer(0),q.integer(1),q.integer(2),q.integer(3),q.integer(4),q.integer(5)});
        }
        return result;
    }
    SqlRows read_partition(const SqlPartition& part,int64_t start,int64_t end,const std::vector<std::string>& symbols={}) {
        auto sql="SELECT * FROM "+identifier(group.table)+" WHERE "+identifier(group.time)+">=? AND "+identifier(group.time)+"<?";
        std::vector<int64_t> ids;
        if (!symbols.empty()) {
            Statement names(index,"SELECT id,name FROM __symbols__");
            while(names.next()) if (std::find(symbols.begin(),symbols.end(),names.text(1))!=symbols.end()) ids.push_back(names.integer(0));
            sql+=" AND "+identifier(group.symbol)+" IN (";
            for(size_t i=0;i<ids.size();++i) sql+=(i?",?":"?");
            sql+=")";
        }
        Statement q(database(part.volume),sql+" ORDER BY "+identifier(group.time));
        q.integer(1,std::max(start,part.start)); q.integer(2,std::min(end,part.end));
        for(size_t i=0;i<ids.size();++i) q.integer(i+3,ids[i]);
        SqlRows result;
        for(int i=0;i<sqlite3_column_count(q.statement);++i) result.columns.emplace_back(sqlite3_column_name(q.statement,i));
        while(q.next()) {
            std::vector<Json> row;
            for(size_t i=0;i<result.columns.size();++i) row.push_back(sql_value(q.statement,i));
            result.rows.push_back(std::move(row));
        }
        return result;
    }
    SqlRows read(int64_t start,int64_t end,const std::vector<std::string>& symbols={}) {
        if(!symbols.empty() && group.symbol.empty()) throw std::runtime_error("该时序表没有品种列："+group.name);
        SqlRows result;
        for(const auto& part:partitions(start,end)) {
            auto batch=read_partition(part,start,end,symbols);
            result.columns=std::move(batch.columns);
            for(auto& row:batch.rows) result.rows.push_back(std::move(row));
        }
        return result;
    }
    std::string create_sql(int64_t volume,const std::string& table) {
        Statement q(database(volume),"SELECT sql FROM sqlite_master WHERE type='table' AND name=?");
        q.text(1,group.table);
        if(!q.next()) throw std::runtime_error("缺少 SQLite 表："+group.table);
        const auto sql=q.text(0);
        char quote=0;
        size_t offset=0;
        for(;offset<sql.size();++offset) {
            const auto c=sql[offset];
            if(quote) { if(c==quote) quote=0; }
            else if(c=='"' || c=='\'' || c=='`') quote=c;
            else if(c=='[') quote=']';
            else if(c=='(') break;
        }
        return "CREATE TABLE "+identifier(table)+sql.substr(offset);
    }
    ReadTable query(int64_t start,int64_t end,const std::vector<std::string>& symbols) {
        const auto batch=read(start,end,symbols);
        std::map<std::string,size_t> positions;
        for(size_t i=0;i<batch.columns.size();++i) positions[batch.columns[i]]=i;
        ReadTable result{Json::object(),group.schema,{{"time_column",group.time}}};
        for(const auto& [name,spec]:group.schema.items())
            if(spec.value("column",name)==group.time) result.metadata["time_column"]=name;
        std::map<int64_t,std::string> names;
        if(!group.symbol.empty()) {
            Statement q(index,"SELECT id,name FROM __symbols__");
            while(q.next()) names.emplace(q.integer(0),q.text(1));
        }
        for(auto& [name,spec]:result.schema.items()) {
            auto& values=result.columns[name]=Json::array();
            if (name=="symbol" && !group.symbol.empty()) {
                for(const auto& row:batch.rows) values.push_back(names.at(row.at(positions.at(group.symbol)).get<int64_t>()));
                continue;
            }
            if(batch.rows.empty()) continue;
            const auto column=positions.at(spec.value("column",name));
            int scale=spec.value("scale",0);
            const auto scale_column=spec.value("scale_column",std::string{});
            if(!scale_column.empty()) {
                int highest=-1;
                for(const auto& row:batch.rows) if(!row[column].is_null())
                    highest=std::max(highest,row.at(positions.at(scale_column)).get<int>());
                if(highest>=0) scale=highest;
                spec["scale"]=scale;
            }
            for(const auto& row:batch.rows) {
                const auto& value=row[column];
                if (!value.is_null() && !scale_column.empty()) values.push_back(rescale(value.get<int64_t>(),row.at(positions.at(scale_column)).get<int>(),scale));
                else values.push_back(value);
            }
        }
        return result;
    }
};

inline void copy_rows(Database& target,const GroupSpec& group,const std::string& create,const SqlRows& rows) {
    Statement exists(target,"SELECT 1 FROM sqlite_master WHERE type='table' AND name=?");
    exists.text(1,group.table);
    if (!exists.next()) target.exec(create);
    std::string columns,parameters;
    for(const auto& name:rows.columns) { if(!columns.empty()) {columns+=",";parameters+=",";} columns+=identifier(name);parameters+="?"; }
    Statement insert(target,"INSERT INTO "+identifier(group.table)+"("+columns+") VALUES("+parameters+")");
    for(const auto& row:rows.rows) {
        for(size_t i=0;i<row.size();++i) bind_value(insert,i+1,row[i]);
        insert.next(); sqlite3_reset(insert.statement);
    }
}
}

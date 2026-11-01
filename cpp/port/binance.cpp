#include <mktpack/binance.hpp>
#include "../storage/sqlite.hpp"
#include <mktpack/decimal.hpp>
#include <sqlite3.h>
#include <miniz.h>
#include <pugixml.hpp>
#include <chrono>
#include <fstream>
#include <sstream>
#include <iostream>
#include <mutex>
#include <thread>
#include <atomic>
#include <set>
namespace mktpack::binance {
namespace fs=std::filesystem;
using namespace std::chrono;
namespace {
const std::string host="https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/";
std::mutex output_mutex;
std::string quote(const std::string& s){std::string q="'";for(char c:s){q+=c;if(c=='\'')q+=c;}return q+"'";}
Json funding_schema(int scale) {
    return {{"time",{{"type","timestamp"},{"scale",0}}},
            {"symbol",{{"type","string"}}},
            {"funding_rate",{{"type","decimal"},{"scale",scale},{"scale_column","scale"}}},
            {"interval_hours",{{"type","integer"},{"scale",0}}}};
}

void create_funding(storage::Database& db) {
    db.exec("CREATE TABLE IF NOT EXISTS funding_data.funding_rate_settlement("
            "symbol_id INTEGER,time INTEGER,funding_rate INTEGER,scale INTEGER,interval_hours INTEGER,"
            "PRIMARY KEY(symbol_id,time)) WITHOUT ROWID");
}

// 调用方将分区替换和下载状态更新放在同一事务中
void write_funding(storage::Database& db,const Table& table,int64_t start,int64_t end) {
    const auto symbol=storage::symbol_id(db,table.metadata.at("symbol"));
    storage::Statement remove(db,"DELETE FROM funding_data.funding_rate_settlement WHERE symbol_id=? AND time>=? AND time<?");
    remove.integer(1,symbol); remove.integer(2,start); remove.integer(3,end); remove.next();
    const int scale=table.schema.at("funding_rate").value("scale",0);
    storage::Statement insert(db,"INSERT INTO funding_data.funding_rate_settlement VALUES(?,?,?,?,?)");
    const auto& time=table.columns.at("time");
    for (size_t i=0;i<time.size();++i) {
        insert.integer(1,symbol); insert.integer(2,time[i]);
        insert.integer(3,table.columns.at("funding_rate")[i]); insert.integer(4,scale);
        insert.integer(5,table.columns.at("interval_hours")[i]); insert.next();
        sqlite3_reset(insert.statement);
    }
    int highest=scale;
    {
        storage::Statement previous(db,"SELECT schema FROM __group__ WHERE name='funding_rate_settlement'");
        if (previous.next()) {
            const auto schema=Json::parse(previous.text(0));
            if (schema.contains("funding_rate")) highest=std::max(highest,schema.at("funding_rate").value("scale",0));
        }
    }
    storage::Statement group(db,"INSERT INTO __group__(name,kind,storage,location,schema) VALUES('funding_rate_settlement','timeseries','sqlite','funding.sqlite',?) "
                       "ON CONFLICT(name) DO UPDATE SET schema=excluded.schema,storage=excluded.storage,location=excluded.location");
    group.text(1,funding_schema(highest).dump()); group.next();
}


struct Source {
    int64_t id;
    int period;
    bool rest=false;
    bool funding=false;
    std::string table() const { return rest?"__binance_rest_funding__":"__binance_datavision_files__"; }
    std::string key() const { return rest?"symbol_id":"template_id"; }
    std::string condition() const {
        return key()+"="+std::to_string(id)+" AND period="+std::to_string(period);
    }
};
int period_number(std::string period) {
    period.erase(std::remove(period.begin(),period.end(),'-'),period.end());
    return std::stoi(period);
}
int64_t path_id(storage::Database& db,const std::string& path) {
    int64_t parent=0;
    size_t start=0;
    do {
        const auto end=path.find('/',start);
        const auto name=path.substr(start,end==std::string::npos?end:end-start);
        storage::Statement insert(db,"INSERT OR IGNORE INTO __binance_datavision_paths__(parent_id,name) VALUES(?,?)");
        insert.integer(1,parent); insert.text(2,name); insert.next();
        storage::Statement query(db,"SELECT id FROM __binance_datavision_paths__ WHERE parent_id=? AND name=?");
        query.integer(1,parent); query.text(2,name); query.next();
        parent=query.integer(0);
        if (end==std::string::npos) break;
        start=end+1;
    } while (true);
    return parent;
}

struct DB:storage::Database {
    explicit DB(const fs::path& path):Database(path,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE) {
        if(fs::exists(root/"funding.sqlite")) attach_funding();
    }
    void attach_funding() { exec("ATTACH DATABASE "+quote((root/"funding.sqlite").string())+" AS funding_data"); }
    int64_t file_id(const std::string& group,const std::string& symbol,const std::string& name) {
        storage::Statement query(*this,"SELECT f.id FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id JOIN __symbols__ s ON s.id=f.symbol_id WHERE g.name=? AND s.name=? AND f.name=?");
        query.text(1,group); query.text(2,symbol); query.text(3,name); query.next();
        return query.integer(0);
    }
    Source source(const std::string& key,const std::string& group,const std::string& symbol,const std::string& period) {
        auto pattern=key;
        pattern.replace(pattern.rfind(period),period.size(),"{period}");
        for (size_t pos=0;(pos=pattern.find(symbol,pos))!=std::string::npos;pos+=8)
            pattern.replace(pos,symbol.size(),"{symbol}");
        const auto path=path_id(*this,pattern);
        const auto group_key=storage::group_id(*this,group), symbol_key=storage::symbol_id(*this,symbol);
        storage::Statement insert(*this,"INSERT OR IGNORE INTO __binance_datavision_templates__(path_id,group_id,symbol_id) VALUES(?,?,?)");
        insert.integer(1,path); insert.integer(2,group_key); insert.integer(3,symbol_key); insert.next();
        Source result{};
        {
            storage::Statement query(*this,"SELECT id FROM __binance_datavision_templates__ WHERE path_id=? AND group_id=? AND symbol_id=?");
            query.integer(1,path); query.integer(2,group_key); query.integer(3,symbol_key); query.next();
            result={query.integer(0),period_number(period),false,group=="funding_rate_settlement"};
        }
        exec("INSERT OR IGNORE INTO __binance_datavision_files__(template_id,period) VALUES("+
             std::to_string(result.id)+","+std::to_string(result.period)+")");
        return result;
    }
    Source rest_source(const std::string& symbol,const std::string& period) {
        Source result{storage::symbol_id(*this,symbol),period_number(period),true,true};
        exec("INSERT OR IGNORE INTO __binance_rest_funding__(symbol_id,period) VALUES("+
             std::to_string(result.id)+","+std::to_string(result.period)+")");
        return result;
    }
    void remap_file(int64_t from,int64_t to) {
        exec("UPDATE __binance_datavision_files__ SET file_id="+std::to_string(to)+" WHERE file_id="+std::to_string(from));
    }
    bool completed(const Source& source, bool verify_presence, int64_t required_end=0) {
        if (source.funding) {
            storage::Statement state(*this,"SELECT rows,error,"+std::string(source.rest?"covered_until":"0")+
                " FROM "+source.table()+" WHERE "+source.condition());
            if (!state.next() || sqlite3_column_type(state.statement,0)==SQLITE_NULL ||
                sqlite3_column_type(state.statement,1)!=SQLITE_NULL || state.integer(2)<required_end) return false;
            if (!verify_presence) return true;
            int64_t symbol=source.id;
            if (!source.rest) {
                storage::Statement query(*this,"SELECT symbol_id FROM __binance_datavision_templates__ WHERE id=?");
                query.integer(1,source.id); query.next(); symbol=query.integer(0);
            }
            auto period=std::to_string(source.period);
            period.insert(4,"-");
            if (period.size()==9) period.insert(7,"-");
            const auto begin=timestamp(period.size()==7?period+"-01":period);
            const auto date=year_month_day{floor<days>(sys_time<milliseconds>{milliseconds{begin}})};
            auto end=duration_cast<milliseconds>((period.size()==7?sys_days{date+months{1}}:sys_days{date}+days{1}).time_since_epoch()).count();
            if (source.rest) end=std::min(end,state.integer(2));
            storage::Statement count(*this,"SELECT count(*) FROM funding_rate_settlement WHERE symbol_id=? AND time>=? AND time<?");
            count.integer(1,symbol); count.integer(2,begin); count.integer(3,end); count.next();
            return count.integer(0)==state.integer(0);
        }
        sqlite3_stmt* statement;
        const auto sql="SELECT s.file_id,s.error,g.name||'/'||t.name||'/'||f.name,"+
            std::string(source.rest?"s.covered_until":"0")+" FROM "+source.table()+" s "
            "LEFT JOIN __timefile__ f ON f.id=s.file_id LEFT JOIN __group__ g ON g.id=f.group_id "
            "LEFT JOIN __symbols__ t ON t.id=f.symbol_id WHERE s."+source.key()+"=? AND s.period=?";
        sqlite3_prepare_v2(db,sql.c_str(),-1,&statement,nullptr);
        sqlite3_bind_int64(statement,1,source.id);
        sqlite3_bind_int(statement,2,source.period);
        bool ready=sqlite3_step(statement)==SQLITE_ROW && sqlite3_column_type(statement,0)!=SQLITE_NULL
                   && sqlite3_column_type(statement,1)==SQLITE_NULL;
        const bool covered=sqlite3_column_int64(statement,3)>=required_end;
        int64_t missing=0;
        if (ready && verify_presence) {
            const auto path=sqlite3_column_text(statement,2);
            ready=path && fs::exists(root/(const char*)path);
            if (!ready) missing=sqlite3_column_int64(statement,0);
        }
        sqlite3_finalize(statement);
        if (missing) {
            exec("UPDATE __binance_datavision_files__ SET error='载荷缺失' WHERE file_id="+std::to_string(missing));
        }
        return ready && covered;
    }
};
std::vector<std::string> list(const std::string& prefix) {
    std::vector<std::string> result;
    std::string token;
    do {
        std::string url=host+"?list-type=2&prefix="+prefix;
        if (!token.empty()) {
            url+="&continuation-token=";
            for (unsigned char c:token) {
                if (isalnum(c) || c=='-' || c=='_' || c=='.' || c=='~') url+=c;
                else {
                    char escaped[4];
                    snprintf(escaped,sizeof escaped,"%%%02X",c);
                    url+=escaped;
                }
            }
        }
        const auto xml=http_get(url);
        pugi::xml_document document;
        if (!document.load_buffer(xml.data(),xml.size())) throw std::runtime_error("清单 XML 错误");
        const auto root=document.document_element();
        for (auto child:root.children("Contents")) {
            const std::string key=child.child_value("Key");
            if (key.ends_with(".zip")) result.push_back(key);
        }
        token=root.child_value("NextContinuationToken");
    } while (!token.empty());
    return result;
}

std::string unzip(const std::string& bytes) {
    mz_zip_archive archive{};
    if (!mz_zip_reader_init_mem(&archive,bytes.data(),bytes.size(),0)) throw std::runtime_error("ZIP 错误");
    std::string result;
    for (mz_uint i=0;i<mz_zip_reader_get_num_files(&archive);++i) {
        mz_zip_archive_file_stat stat{};
        mz_zip_reader_file_stat(&archive,i,&stat);
        if (!std::string(stat.m_filename).ends_with(".csv")) continue;
        size_t size=0;
        void* data=mz_zip_reader_extract_to_heap(&archive,i,&size,0);
        if (!data) {
            mz_zip_reader_end(&archive);
            throw std::runtime_error("ZIP 解压失败");
        }
        result.assign((char*)data,size);
        mz_free(data);
        break;
    }
    mz_zip_reader_end(&archive);
    return result;
}

std::string source(const fs::path& cache, const std::string& key) {
    const auto path=cache/key;
    if (!cache.empty() && fs::exists(path)) {
        std::ifstream input(path,std::ios::binary);
        return {std::istreambuf_iterator<char>(input),{}};
    }
    const auto bytes=http_get(host+key);
    if (!cache.empty()) {
        fs::create_directories(path.parent_path());
        std::ofstream output(path,std::ios::binary);
        output.write(bytes.data(),bytes.size());
    }
    return bytes;
}

std::vector<std::string> split(const std::string& line) {
    std::vector<std::string> row;
    std::stringstream input(line);
    std::string field;
    while (std::getline(input,field,',')) {
        if (!field.empty() && field.back()=='\r') field.pop_back();
        row.push_back(field);
    }
    return row;
}

Table csv(const std::string& text, const std::string& group, const std::string& symbol,
          int64_t start, int64_t end) {
    Table table;
    table.metadata={{"symbol",symbol},{"group",group}};
    std::vector<std::pair<std::string,size_t>> fields;
    if (group=="kline") {
        fields={{"open",1},{"high",2},{"low",3},{"close",4},{"volume",5},{"taker_buy_volume",9}};
    } else if (group=="oi") {
        fields={{"sum_open_interest",2},{"sum_open_interest_value",3}};
    } else {
        fields={{"funding_rate",2},{"interval_hours",1}};
    }
    std::map<std::string,std::vector<DecimalInteger>> values;
    std::istringstream input(text);
    std::string line;
    while (std::getline(input,line)) {
        const auto row=split(line);
        if (row.empty() || row.front().empty() || !std::isdigit(row.front().front())) continue;
        int64_t time=row.front().find('-')!=std::string::npos?timestamp(row.front()):std::stoll(row.front());
        if (time>=100000000000000LL) time/=1000;
        if (time<start || time>=end) continue;
        table.columns["time"].push_back(time);
        for (const auto& [name,index]:fields) values[name].push_back(decimal_integer(row.at(index)));
    }
    table.schema["time"]={{"type","timestamp"},{"scale",0}};
    table.columns["time"];
    for (const auto& [name,index]:fields) {
        int scale=0;
        for (const auto value:values[name]) scale=std::max(scale,value.decimals);
        table.schema[name]={{"type",name=="interval_hours"?"integer":"decimal"},{"scale",scale}};
        auto& column=table.columns[name];
        for (const auto value:values[name]) column.push_back(rescale(value.value,value.decimals,scale));
    }
    return table;
}

void register_file(DB& db, const fs::path& path, const Table& table) {
    const auto& time=table.columns.at("time");
    const auto symbol=table.metadata.at("symbol").get<std::string>();
    const auto group=table.metadata.at("group").get<std::string>();
    const auto stored=info(path);
    Json schema=Json::object();
    {
        storage::Statement previous(db,"SELECT schema FROM __group__ WHERE name=?");
        previous.text(1,group);
        if (previous.next()) schema=Json::parse(previous.text(0));
    }
    for (const auto& descriptor:stored.at("columns")) {
        const auto name=descriptor.at("name").get<std::string>();
        const int old=schema.contains(name)?schema.at(name).value("scale",0):0;
        schema[name]={{"type",descriptor.at("type")},{"scale",std::max(old,descriptor.value("scale",0))}};
    }
    db.exec("INSERT INTO __group__(name,kind,schema) VALUES("+quote(group)+",'timeseries',"+quote(schema.dump())+") "
            "ON CONFLICT(name) DO UPDATE SET schema=excluded.schema");
    const auto group_key=storage::group_id(db,group), symbol_key=storage::symbol_id(db,symbol);
    db.exec("INSERT INTO __timefile__(group_id,symbol_id,name,first_time,last_time,rows) VALUES("+
            std::to_string(group_key)+","+std::to_string(symbol_key)+","+quote(path.filename().string())+","+std::to_string(time.front())+","+
            std::to_string(time.back())+","+std::to_string(time.size())+") "
            "ON CONFLICT(group_id,symbol_id,name) DO UPDATE SET first_time=excluded.first_time,last_time=excluded.last_time,rows=excluded.rows");
}

void set_taker_encoding(Table& table, const Options& options) {
    if (!table.columns.contains("taker_buy_volume")) return;
    auto& schema=table.schema["taker_buy_volume"];
    if (options.lossy_taker) {
        schema["encoding"]="ratio_1000";
        schema["reference"]="volume";
    } else {
        schema.erase("encoding");
        schema.erase("reference");
    }
}

void consolidate(DB& db, const Options& options, const std::string& group, const std::string& symbol) {
    const auto directory=options.root/group/symbol;
    if (!fs::exists(directory)) return;
    const auto today=floor<days>(system_clock::now());
    std::map<std::string,std::vector<fs::path>> buckets;
    for (const auto& entry:fs::directory_iterator(directory)) {
        if (entry.path().extension()!=".mkp") continue;
        const auto stem=entry.path().stem().string();
        const int year=std::stoi(stem.substr(0,4));
        if (sys_days{std::chrono::year{year+1}/1/1}<=today) {
            buckets[stem.substr(0,4)].push_back(entry.path());
        } else if (stem.size()==10) {
            const auto month=year_month_day{floor<days>(sys_time<milliseconds>{milliseconds{timestamp(stem.substr(0,7)+"-01")}})};
            if (sys_days{month+months{1}}<=today) buckets[stem.substr(0,7)].push_back(entry.path());
        }
    }
    for (auto& [period,paths]:buckets) {
        const auto destination=directory/(period+".mkp");
        if (paths.size()==1 && paths.front()==destination) continue;
        std::vector<Table> tables;
        for (const auto& path:paths) tables.push_back(read(path));
        auto table=merge(tables);
        set_taker_encoding(table,options);
        write(destination,table);
        db.exec("BEGIN IMMEDIATE");
        register_file(db,destination,table);
        const auto target=db.file_id(group,symbol,destination.filename().string());
        for (const auto& path:paths) {
            if (path==destination) continue;
            const auto old=db.file_id(group,symbol,path.filename().string());
            db.remap_file(old,target);
            db.exec("DELETE FROM __timefile__ WHERE id="+std::to_string(old));
        }
        db.exec("COMMIT");
        for (const auto& path:paths) if (path!=destination) fs::remove(path);
    }
}

void store_source(DB& db, const Options& options, const std::string& group,
                  const std::string& symbol, const std::string& period,
                  const Source& source_id, Table table, int64_t covered_until=0) {
    const auto directory=options.root/group/symbol;
    auto destination=directory/(period+".mkp");
    const auto begin=timestamp(period.size()==7?period+"-01":period);
    const auto date=year_month_day{floor<days>(sys_time<milliseconds>{milliseconds{begin}})};
    const auto end=duration_cast<milliseconds>((period.size()==7?sys_days{date+months{1}}:sys_days{date}+days{1}).time_since_epoch()).count();
    if (source_id.funding) {
        db.exec("BEGIN IMMEDIATE");
        write_funding(db,table,begin,end);
        db.exec("UPDATE "+source_id.table()+" SET rows="+std::to_string(table.columns.at("time").size())+
            ",error=NULL"+(source_id.rest?",covered_until="+std::to_string(covered_until):",file_id=NULL")+
            " WHERE "+source_id.condition());
        db.exec("COMMIT");
        return;
    }
    std::vector<fs::path> replaced;
    std::vector<Table> tables{table};
    if (fs::exists(directory)) {
        for (const auto& entry:fs::directory_iterator(directory)) {
            const auto path=entry.path();
            if (path.extension()!=".mkp") continue;
            const auto stem=path.stem().string();
            if (stem==period || stem.starts_with(period+"-")) {
                replaced.push_back(path);
            } else if (period.starts_with(stem+"-")) {
                auto previous=read(path);
                const auto& times=previous.columns.at("time");
                const auto a=std::lower_bound(times.begin(),times.end(),begin)-times.begin();
                const auto b=std::lower_bound(times.begin(),times.end(),end)-times.begin();
                for (auto& [name,values]:previous.columns) values.erase(values.begin()+a,values.begin()+b);
                tables.push_back(std::move(previous));
                destination=path;
            }
        }
    }
    auto combined=merge(tables);
    set_taker_encoding(combined,options);
    write(destination,combined);
    db.exec("BEGIN IMMEDIATE");
    register_file(db,destination,combined);
    const auto target=db.file_id(group,symbol,destination.filename().string());
    for (const auto& path:replaced) {
        if(path==destination) continue;
        const auto old=db.file_id(group,symbol,path.filename().string());
        db.remap_file(old,target);
        db.exec("DELETE FROM __timefile__ WHERE id="+std::to_string(old));
    }
    db.exec("UPDATE "+source_id.table()+" SET file_id="+std::to_string(target)+",error=NULL"+
            (source_id.rest?",covered_until="+std::to_string(covered_until):"")+" WHERE "+source_id.condition());
    db.exec("COMMIT");
    for(const auto& path:replaced) if(path!=destination) fs::remove(path);
}
void funding_rest(DB& db, const Options& options, const std::string& symbol, int64_t begin) {
    if (begin>=options.end) return;
    begin=duration_cast<milliseconds>(floor<days>(sys_time<milliseconds>{milliseconds{begin}}).time_since_epoch()).count();
    const auto key="rest/fundingRate/"+symbol+"/"+std::to_string(begin)+"-"+std::to_string(options.end)+".json";
    Json rows=Json::array();
    auto cache=options.cache/key;
    if (!options.cache.empty() && fs::exists(cache)) {
        std::ifstream input(cache);
        input>>rows;
    } else {
        auto cursor=begin-86400000;
        while(cursor<options.end) {
            auto page=Json::parse(http_get("https://fapi.binance.com/fapi/v1/fundingRate?symbol="+symbol+"&startTime="+std::to_string(cursor)+"&endTime="+std::to_string(options.end-1)+"&limit=1000"));
            if(!page.is_array()) throw std::runtime_error("资金费率REST响应错误："+page.dump());
            if(page.empty()) break;
            for(const auto& row:page) rows.push_back(row);
            cursor=page.back().at("fundingTime").get<int64_t>()+1;
            if(page.size()<1000) break;
        }
        if(!options.cache.empty()) {
            fs::create_directories(cache.parent_path());
            std::ofstream output(cache);
            output<<rows.dump();
        }
    }
    std::map<std::string,std::ostringstream> days;
    int64_t previous=0;
    for(const auto& row:rows) {
        const auto time=row.at("fundingTime").get<int64_t>();
        if(previous && time>=begin && time<options.end) {
            auto date=year_month_day{floor<std::chrono::days>(sys_time<milliseconds>{milliseconds{time}})};
            char period[11];
            snprintf(period,sizeof period,"%04d-%02u-%02u",int(date.year()),unsigned(date.month()),unsigned(date.day()));
            auto hours=(time-previous+1800000)/3600000;
            days[period]<<time<<","<<hours<<","<<row.at("fundingRate").get<std::string>()<<"\n";
        }
        previous=time;
    }
    for(auto& [period,csv_text]:days) {
        auto table=csv(csv_text.str(),"funding_rate_settlement",symbol,INT64_MIN,INT64_MAX);
        table.metadata["interval_hours_source"]="相邻实际结算时间差四舍五入到小时";
        const auto covered_until=std::min(options.end,timestamp(period)+86400000);
        const auto source_id=db.rest_source(symbol,period);
        if(db.completed(source_id,options.full,covered_until)) continue;
        store_source(db,options,"funding_rate_settlement",symbol,period,source_id,std::move(table),covered_until);
    }
}
void one(const Options& options, const std::string& group, const std::string& symbol) {
    DB db(options.root/"mkt.sqlite");
    std::map<std::string,std::string> remote;
    for (auto branch:{"monthly","daily"}) {
        if (group=="oi" && std::string(branch)=="monthly") continue;
        if (group=="funding_rate_settlement" && std::string(branch)=="daily") continue;
        const auto kind=group=="kline"?"klines":group=="oi"?"metrics":"fundingRate";
        const std::string prefix="data/futures/um/"+std::string(branch)+"/"+kind+"/"+symbol+"/"+(group=="kline"?"1m/":"");
        for (const auto& key:list(prefix)) {
            const auto name=fs::path(key).stem().string();
            const auto period=name.substr(name.size()-(std::string(branch)=="monthly"?7:10));
            const auto begin=timestamp(period.size()==7?period+"-01":period);
            const auto date=year_month_day{floor<days>(sys_time<milliseconds>{milliseconds{begin}})};
            const auto finish=duration_cast<milliseconds>((period.size()==7?sys_days{date+months{1}}:sys_days{date}+days{1}).time_since_epoch()).count();
            if (begin>=options.end || finish<=options.start) continue;
            if (period.size()==10 && remote.contains(period.substr(0,7))) continue;
            remote[period]=key;
        }
    }
    for (const auto& [period,key]:remote) {
        const auto source_id=db.source(key,group,symbol,period);
        if (db.completed(source_id,options.full)) continue;
        try {
            auto table=csv(unzip(source(options.cache,key)),group,symbol,INT64_MIN,INT64_MAX);
            if (table.columns.at("time").empty()) continue;
            store_source(db,options,group,symbol,period,source_id,table);
            std::lock_guard lock(output_mutex);
            std::cerr<<group<<" "<<symbol<<" "<<period<<" "<<table.columns.at("time").size()<<" 行\n";
        } catch (const std::exception& error) {
            sqlite3_exec(db.db,"ROLLBACK",nullptr,nullptr,nullptr);
            db.exec("UPDATE __binance_datavision_files__ SET file_id=NULL,error="+quote(error.what())+" WHERE "+source_id.condition());
            throw;
        }
    }
    if (group=="funding_rate_settlement") {
        int64_t begin=options.start;
        for (const auto& [period,key]:remote) {
            const auto date=year_month_day{floor<days>(sys_time<milliseconds>{milliseconds{timestamp(period+"-01")}})};
            begin=std::max(begin,duration_cast<milliseconds>(sys_days{date+months{1}}.time_since_epoch()).count());
        }
        funding_rest(db,options,symbol,begin);
    }
    if (group!="funding_rate_settlement") consolidate(db,options,group,symbol);
}
}
int64_t timestamp(std::string text) {
    if (text.size()==8 && text.find('-')==std::string::npos) {
        text=text.substr(0,4)+"-"+text.substr(4,2)+"-"+text.substr(6,2);
    }
    if (text.size()<10) throw std::runtime_error("日期需要 YYYY-MM-DD");
    const year_month_day date{year{std::stoi(text.substr(0,4))},
                              month{unsigned(std::stoi(text.substr(5,2)))},
                              day{unsigned(std::stoi(text.substr(8,2)))}};
    if (!date.ok()) throw std::runtime_error("日期无效");
    int64_t result=duration_cast<milliseconds>(sys_days{date}.time_since_epoch()).count();
    if (text.size()>=19) {
        result+=(std::stoll(text.substr(11,2))*3600+std::stoll(text.substr(14,2))*60+std::stoll(text.substr(17,2)))*1000;
    }
    return result;
}

void download(const Options& options) {
    fs::create_directories(options.root);
    {
        DB db(options.root/"mkt.sqlite");
        sqlite3_stmt* packed=nullptr;
        sqlite3_prepare_v2(db.db,"PRAGMA application_id",-1,&packed,nullptr);
        const bool is_packed=sqlite3_step(packed)==SQLITE_ROW && sqlite3_column_int(packed,0)==0x4d4b5450;
        sqlite3_finalize(packed);
        if (is_packed) throw std::runtime_error("打包数据集不支持下载写入，请使用分散目录");
        storage::create_index(db);
        db.exec("PRAGMA journal_mode=DELETE;"
                "CREATE TABLE IF NOT EXISTS __binance_datavision_paths__(id INTEGER PRIMARY KEY,parent_id INTEGER,name TEXT,UNIQUE(parent_id,name));CREATE TABLE IF NOT EXISTS __binance_datavision_templates__(id INTEGER PRIMARY KEY,path_id INTEGER,group_id INTEGER,symbol_id INTEGER,UNIQUE(path_id,group_id,symbol_id));"
                "CREATE TABLE IF NOT EXISTS __binance_datavision_files__(template_id INTEGER,period INTEGER,file_id INTEGER,error TEXT,rows INTEGER,PRIMARY KEY(template_id,period)) WITHOUT ROWID;"
                "CREATE TABLE IF NOT EXISTS __binance_rest_funding__(symbol_id INTEGER,period INTEGER,rows INTEGER,error TEXT,covered_until INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(symbol_id,period)) WITHOUT ROWID;");
        if(std::find(options.groups.begin(),options.groups.end(),"funding_rate_settlement")!=options.groups.end()) {
            if(!fs::exists(options.root/"funding.sqlite")) db.attach_funding();
            create_funding(db);
        }
    }
    std::vector<std::pair<std::string,std::string>> jobs;
    for (const auto& group:options.groups) {
        if (group=="exchangeinfo") continue;
        if (group!="kline" && group!="oi" && group!="funding_rate_settlement") throw std::runtime_error("未知下载组："+group);
        for (const auto& symbol:options.symbols) jobs.emplace_back(group,symbol);
    }
    std::atomic<size_t> next{0};
    std::mutex mutex;
    std::vector<std::string> errors;
    std::vector<std::thread> threads;
    for (size_t i=0;i<std::min(options.jobs,jobs.size());++i) {
        threads.emplace_back([&] {
            while (true) {
                const auto j=next++;
                if (j>=jobs.size()) break;
                try {
                    one(options,jobs[j].first,jobs[j].second);
                } catch (const std::exception& error) {
                    std::lock_guard lock(mutex);
                    errors.push_back(jobs[j].first+" "+jobs[j].second+": "+error.what());
                }
            }
        });
    }
    for (auto& thread:threads) thread.join();
    if (std::find(options.groups.begin(),options.groups.end(),"exchangeinfo")!=options.groups.end()) {
        const auto text=http_get("https://fapi.binance.com/fapi/v1/exchangeInfo");
        if (!options.cache.empty()) {
            fs::create_directories(options.cache/"api");
            std::ofstream cached(options.cache/"api/exchangeInfo.json");
            cached<<text;
        }
        const auto json=Json::parse(text);
        std::string columns,values;
        for (const auto& [name,value]:json.items()) {
            if (!columns.empty()) { columns+=","; values+=","; }
            columns+='"'+name+'"';
            values+=value.is_structured()?quote(value.dump()):value.is_string()?quote(value.get<std::string>()):value.dump();
        }
        DB db(options.root/"mkt.sqlite");
        db.exec("BEGIN IMMEDIATE");
        db.exec("DROP TABLE IF EXISTS exchangeinfo");
        db.exec("CREATE TABLE exchangeinfo("+columns+")");
        db.exec("INSERT INTO exchangeinfo VALUES("+values+")");
        db.exec("INSERT INTO __group__(name,kind,storage) VALUES('exchangeinfo','ordinary','sqlite') ON CONFLICT(name) DO UPDATE SET kind=excluded.kind,storage=excluded.storage");
        db.exec("COMMIT");
    }
    if (!errors.empty()) {
        std::string message;
        for (const auto& error:errors) message+=error+"\n";
        throw std::runtime_error(message);
    }
}

}

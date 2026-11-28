#include <mktpack/binance.hpp>
#include "../storage/sqlite.hpp"
#include "../storage/store.hpp"
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
using Reporter=std::function<void(const std::string&,const std::string&)>;
std::string quote(const std::string& s){std::string q="'";for(char c:s){q+=c;if(c=='\'')q+=c;}return q+"'";}
std::string url_encode(const std::string& text,bool slash=false) {
    std::string result;
    for(unsigned char c:text) {
        if((c>='a' && c<='z') || (c>='A' && c<='Z') || (c>='0' && c<='9') || c=='-' || c=='_' || c=='.' || c=='~' || (slash && c=='/')) result+=c;
        else { char escaped[4];snprintf(escaped,sizeof escaped,"%%%02X",c);result+=escaped; }
    }
    return result;
}
Json funding_schema(int scale) {
    return {{"time",{{"type","timestamp"},{"scale",0}}},
            {"symbol",{{"type","string"}}},
            {"funding_rate",{{"type","real"},{"scale",scale},{"scale_column","scale"}}},
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
    storage::PayloadStore store;
    explicit DB(const fs::path& path):Database(path,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE),store(*this) {
        if(!store.packed() && fs::exists(root/"funding.sqlite")) attach_funding();
    }
    void attach_funding() { exec("ATTACH DATABASE "+quote((root/"funding.sqlite").string())+" AS funding_data"); }
    int64_t file_id(const std::string& group,const std::string& symbol,int period) {
        storage::Statement query(*this,"SELECT f.id FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id JOIN __symbols__ s ON s.id=f.symbol_id WHERE g.name=? AND s.name=? AND f.period=?");
        query.text(1,group); query.text(2,symbol); query.integer(3,period); query.next();
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
        const auto sql="SELECT s.file_id,s.error,g.name||'/'||t.name||'/'||f.period||'.mkp',"+
            std::string(source.rest?"s.covered_until":"0")+",s.rows FROM "+source.table()+" s "
            "LEFT JOIN __timefile__ f ON f.id=s.file_id LEFT JOIN __group__ g ON g.id=f.group_id "
            "LEFT JOIN __symbols__ t ON t.id=f.symbol_id WHERE s."+source.key()+"=? AND s.period=?";
        sqlite3_prepare_v2(db,sql.c_str(),-1,&statement,nullptr);
        sqlite3_bind_int64(statement,1,source.id);
        sqlite3_bind_int(statement,2,source.period);
        bool ready=sqlite3_step(statement)==SQLITE_ROW && sqlite3_column_type(statement,0)!=SQLITE_NULL
                   && sqlite3_column_type(statement,1)==SQLITE_NULL;
        const bool empty=sqlite3_column_type(statement,4)!=SQLITE_NULL && sqlite3_column_int64(statement,4)==0 && sqlite3_column_type(statement,1)==SQLITE_NULL;
        const bool covered=sqlite3_column_int64(statement,3)>=required_end;
        int64_t missing=0;
        if (ready && verify_presence) {
            const auto fid=sqlite3_column_int64(statement,0);
            ready=store.exists(fid);
            if (!ready) missing=fid;
        }
        sqlite3_finalize(statement);
        if (missing) {
            exec("UPDATE __binance_datavision_files__ SET error='载荷缺失' WHERE file_id="+std::to_string(missing));
        }
        return (ready || empty) && covered;
    }
};
std::vector<std::string> list(const std::string& prefix,bool directories=false) {
    std::vector<std::string> result;
    std::string token;
    do {
        std::string url=host+"?list-type=2&prefix="+url_encode(prefix,true);
        if(directories) url+="&delimiter=/";
        if (!token.empty()) {
            url+="&continuation-token="+url_encode(token);
        }
        const auto xml=http_get(url);
        pugi::xml_document document;
        if (!document.load_buffer(xml.data(),xml.size())) throw std::runtime_error("清单 XML 错误");
        const auto root=document.document_element();
        for (auto child:root.children("Contents")) {
            const std::string key=child.child_value("Key");
            if (key.ends_with(".zip")) result.push_back(key);
        }
        if(directories) for(auto child:root.children("CommonPrefixes")) result.emplace_back(child.child_value("Prefix"));
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
    const auto bytes=http_get(host+url_encode(key,true));
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
        table.schema[name]={{"type",name=="interval_hours"?"integer":"real"},{"scale",scale}};
        auto& column=table.columns[name];
        for (const auto value:values[name]) column.push_back(rescale(value.value,value.decimals,scale));
    }
    return table;
}

void update_group_schema(DB& db, const std::string& group, const std::vector<uint8_t>& bytes) {
    const auto stored=info(bytes);
    Json incoming=Json::object();
    for (const auto& descriptor:stored.at("columns")) {
        incoming[descriptor.at("name").get<std::string>()]={
            {"type",descriptor.at("type")},{"scale",descriptor.value("scale",0)}
        };
    }
    storage::merge_group_schema(db,group,incoming);
}

void consolidate(DB& db, const Options& options, const std::string& group, const std::string& symbol,const Reporter& progress={}) {
    const auto group_key=storage::group_id(db,group), symbol_key=storage::symbol_id(db,symbol);
    const auto entries=db.store.list(group_key,symbol_key);
    if (entries.empty()) return;
    const auto today=floor<days>(system_clock::now());
    std::map<int,std::vector<storage::TimefileEntry>> buckets;
    for (const auto& entry:entries) {
        const auto stem=std::to_string(entry.period);
        if (stem.size()<4) continue;
        const int year=std::stoi(stem.substr(0,4));
        if (sys_days{std::chrono::year{year+1}/1/1}<=today) {
            buckets[year].push_back(entry);
        } else if (stem.size()==8) {
            const auto month=year_month_day{floor<days>(sys_time<milliseconds>{milliseconds{timestamp(stem.substr(0,6)+"01")}})};
            if (sys_days{month+months{1}}<=today) buckets[std::stoi(stem.substr(0,6))].push_back(entry);
        }
    }
    for (auto& [target_period,items]:buckets) {
        if (items.size()==1 && items.front().period==target_period) continue;
        if(progress) progress("合并",std::to_string(target_period));
        std::vector<Table> tables;
        tables.reserve(items.size());
        for (const auto& item:items) tables.push_back(decode(db.store.read(item,group,symbol)));
        auto table=merge(tables);
        if (table.columns.contains("taker_buy_volume")) set_taker_encoding(table.schema,options.lossy_taker);
        const auto& time=table.columns.at("time");
        const auto bytes=encode(table);
        std::vector<fs::path> deferred_unlink;
        db.exec("BEGIN IMMEDIATE");
        update_group_schema(db,group,bytes);
        const auto target=db.store.write_blob(group_key,group,symbol_key,symbol,target_period,
                                              time.front(),time.back(),static_cast<int64_t>(time.size()),bytes);
        for (const auto& item:items) {
            if (item.id==target) continue;
            db.remap_file(item.id,target);
            db.store.erase(item,group,symbol,&deferred_unlink);
        }
        db.exec("COMMIT");
        for (const auto& path:deferred_unlink) {
            std::error_code ec;
            fs::remove(path,ec);
        }
    }
}

void store_source(DB& db, const Options& options, const std::string& group,
                  const std::string& symbol, const std::string& period,
                  const Source& source_id, Table table, int64_t covered_until=0) {
    auto local_period=period;
    local_period.erase(std::remove(local_period.begin(),local_period.end(),'-'),local_period.end());
    int target_period=std::stoi(local_period);
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
    const auto group_key=storage::group_id(db,group), symbol_key=storage::symbol_id(db,symbol);
    std::vector<storage::TimefileEntry> replaced;
    std::vector<Table> tables{std::move(table)};
    for (const auto& entry:db.store.list(group_key,symbol_key)) {
        const auto stem=std::to_string(entry.period);
        if (stem==local_period || (stem.size()>local_period.size() && stem.starts_with(local_period))) {
            replaced.push_back(entry);
        } else if (local_period.size()>stem.size() && local_period.starts_with(stem)) {
            if (!db.store.exists(entry.id)) {
                replaced.push_back(entry);
                continue;
            }
            auto previous=decode(db.store.read(entry,group,symbol));
            const auto& times=previous.columns.at("time");
            const auto a=std::lower_bound(times.begin(),times.end(),begin)-times.begin();
            const auto b=std::lower_bound(times.begin(),times.end(),end)-times.begin();
            for (auto& [name,values]:previous.columns) values.erase(values.begin()+a,values.begin()+b);
            tables.push_back(std::move(previous));
            target_period=entry.period;
        }
    }
    auto combined=merge(tables);
    if (combined.columns.contains("taker_buy_volume")) set_taker_encoding(combined.schema,options.lossy_taker);
    const auto& time=combined.columns.at("time");
    const auto bytes=encode(combined);
    std::vector<fs::path> deferred_unlink;
    db.exec("BEGIN IMMEDIATE");
    update_group_schema(db,group,bytes);
    const auto target=db.store.write_blob(group_key,group,symbol_key,symbol,target_period,
                                          time.front(),time.back(),static_cast<int64_t>(time.size()),bytes);
    for (const auto& entry:replaced) {
        if (entry.id==target) continue;
        db.remap_file(entry.id,target);
        db.store.erase(entry,group,symbol,&deferred_unlink);
    }
    db.exec("UPDATE "+source_id.table()+" SET file_id="+std::to_string(target)+",error=NULL"+
            (source_id.rest?",covered_until="+std::to_string(covered_until):"")+" WHERE "+source_id.condition());
    db.exec("COMMIT");
    for (const auto& path:deferred_unlink) {
        std::error_code ec;
        fs::remove(path,ec);
    }
}
void funding_rest(DB& db, const Options& options, const std::string& symbol, int64_t begin,const Reporter& progress={}) {
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
            if(progress) progress("下载","REST 资金费率 "+std::to_string(cursor));
            auto page=Json::parse(http_get("https://fapi.binance.com/fapi/v1/fundingRate?symbol="+url_encode(symbol)+"&startTime="+std::to_string(cursor)+"&endTime="+std::to_string(options.end-1)+"&limit=1000"));
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
        if(progress) progress("写入","REST "+period);
        store_source(db,options,"funding_rate_settlement",symbol,period,source_id,std::move(table),covered_until);
    }
}
std::string period_text(int64_t value) {
    auto text=std::to_string(value);
    text.insert(4,"-");
    if(text.size()>7) text.insert(7,"-");
    return text;
}
std::pair<int64_t,int64_t> period_range(const std::string& period) {
    const auto start=timestamp(period.size()==7?period+"-01":period);
    const auto date=year_month_day{floor<days>(sys_time<milliseconds>{milliseconds{start}})};
    const auto end=duration_cast<milliseconds>((period.size()==7?sys_days{date+months{1}}:sys_days{date}+days{1}).time_since_epoch()).count();
    return {start,end};
}
std::string source_directory(const std::string& group,const std::string& symbol,bool monthly) {
    return "data/futures/um/"+std::string(monthly?"monthly/":"daily/")+
        (group=="kline"?"klines/":group=="oi"?"metrics/":"fundingRate/")+symbol+"/"+(group=="kline"?"1m/":"");
}
std::string source_filename(const std::string& group,const std::string& symbol) {
    return symbol+"-"+(group=="kline"?"1m-":group=="oi"?"metrics-":"fundingRate-");
}

std::map<std::string,std::string> discover(DB& db,const Options& options,const std::string& group,
                                          const std::string& symbol,const Reporter& progress) {
    std::map<std::string,std::string> remote;
    const auto group_key=storage::group_id(db,group),symbol_key=storage::symbol_id(db,symbol);
    // 来源清单保留待下载和失败记录；扫描范围只用于减少远端目录请求
    {
        storage::Statement known(db,"SELECT f.period FROM __binance_datavision_files__ f JOIN __binance_datavision_templates__ t ON t.id=f.template_id WHERE t.group_id=? AND t.symbol_id=?");
        known.integer(1,group_key); known.integer(2,symbol_key);
        while(known.next()) {
            const auto period=period_text(known.integer(0));
            const auto [first,last]=period_range(period);
            if(first<options.end && last>options.start)
                remote[period]=source_directory(group,symbol,period.size()==7)+source_filename(group,symbol)+period+".zip";
        }
    }
    const auto first_date=year_month_day{floor<days>(sys_time<milliseconds>{milliseconds{options.start}})};
    const auto last_date=year_month_day{floor<days>(sys_time<milliseconds>{milliseconds{options.end-1}})};
    for(bool monthly:{true,false}) {
        if((group=="oi" && monthly) || (group=="funding_rate_settlement" && !monthly)) continue;
        int64_t covered_start=INT64_MAX,covered_end=INT64_MIN;
        {
            storage::Statement scan(db,"SELECT start,end FROM __binance_datavision_scans__ WHERE group_id=? AND symbol_id=? AND monthly=?");
            scan.integer(1,group_key); scan.integer(2,symbol_key); scan.integer(3,monthly);
            if(scan.next()) {covered_start=scan.integer(0);covered_end=scan.integer(1);}
        }
        auto cursor=monthly?last_date.year()/January:last_date.year()/last_date.month();
        const auto oldest=monthly?first_date.year()/January:first_date.year()/first_date.month();
        const auto step=months{monthly?12:1};
        const auto latest=cursor;
        for(;cursor>=oldest;cursor-=step) {
            const auto begin=duration_cast<milliseconds>(sys_days{cursor/1}.time_since_epoch()).count();
            const auto end=duration_cast<milliseconds>(sys_days{(cursor+step)/1}.time_since_epoch()).count();
            char label[8]; snprintf(label,sizeof label,"%04d-%02u",int(cursor.year()),unsigned(cursor.month()));
            const auto period=std::string(label);
            if(!monthly && remote.contains(period)) continue;
            const bool covered=covered_start<=std::max(begin,options.start) && covered_end>=std::min(end,options.end);
            if(!options.full && covered && cursor!=latest) continue;
            const auto prefix=source_directory(group,symbol,monthly)+source_filename(group,symbol)+(monthly?period.substr(0,4):period);
            progress("发现",(monthly?"月归档 ":"日归档 ")+(monthly?period.substr(0,4):period));
            const auto keys=list(prefix);
            db.exec("BEGIN IMMEDIATE");
            for(const auto& key:keys) {
                const auto name=fs::path(key).stem().string();
                const auto date=name.substr(name.size()-(monthly?7:10));
                const auto [first,last]=period_range(date);
                if(first>=options.end || last<=options.start) continue;
                db.source(key,group,symbol,date);
                remote[date]=key;
            }
            db.exec("COMMIT");
        }
        // 不相交的查询范围只记本次范围，避免将未扫描的中间区域标为已完成
        if(covered_start<=options.end && covered_end>=options.start) {
            covered_start=std::min(covered_start,options.start); covered_end=std::max(covered_end,options.end);
        } else { covered_start=options.start; covered_end=options.end; }
        storage::Statement save(db,"INSERT INTO __binance_datavision_scans__ VALUES(?,?,?,?,?) ON CONFLICT(group_id,symbol_id,monthly) DO UPDATE SET start=excluded.start,end=excluded.end");
        save.integer(1,group_key); save.integer(2,symbol_key); save.integer(3,monthly); save.integer(4,covered_start); save.integer(5,covered_end); save.next();
    }
    for(auto it=remote.begin();it!=remote.end();) {
        if(it->first.size()==10 && remote.contains(it->first.substr(0,7))) it=remote.erase(it);
        else ++it;
    }
    return remote;
}

std::vector<std::string> discover_symbols(const Options& options) {
    std::set<std::string> symbols;
    for(const auto& group:options.groups) {
        if(group=="exchangeinfo") continue;
        for(bool monthly:{true,false}) {
            if((group=="oi" && monthly) || (group=="funding_rate_settlement" && !monthly)) continue;
            const auto prefix="data/futures/um/"+std::string(monthly?"monthly/":"daily/")+
                (group=="kline"?"klines/":group=="oi"?"metrics/":"fundingRate/");
            for(auto path:list(prefix,true)) {
                path.pop_back();
                const auto symbol=fs::path(path).filename().string();
                if(symbol.ends_with("USDT")) symbols.insert(symbol);
            }
        }
    }
    return {symbols.begin(),symbols.end()};
}

void one(const Options& options,const std::string& group,const std::string& symbol,size_t slot) {
    DB db(options.root/"mkt.sqlite");
    Progress state{slot,group,symbol,"发现","",0,0};
    const Reporter report=[&](const auto& stage,const auto& detail) {
        state.stage=stage;state.detail=detail;
        if(options.progress) options.progress(state);
    };
    const auto remote=discover(db,options,group,symbol,report);
    state.total=remote.size();
    size_t failures=0;
    for(auto it=remote.rbegin();it!=remote.rend();++it) {
        const auto& [period,key]=*it;
        const auto source_id=db.source(key,group,symbol,period);
        if(db.completed(source_id,options.full)) {++state.completed;continue;}
        try {
            report("下载",period);
            auto table=csv(unzip(source(options.cache,key)),group,symbol,INT64_MIN,INT64_MAX);
            if(table.columns.at("time").empty()) {
                db.exec("UPDATE __binance_datavision_files__ SET rows=0,error=NULL,file_id=NULL WHERE "+source_id.condition());
            } else {
                report("写入",period+" "+std::to_string(table.columns.at("time").size())+" 行");
                store_source(db,options,group,symbol,period,source_id,std::move(table));
            }
        } catch(const std::exception& error) {
            sqlite3_exec(db.db,"ROLLBACK",nullptr,nullptr,nullptr);
            db.exec("UPDATE __binance_datavision_files__ SET file_id=NULL,error="+quote(error.what())+" WHERE "+source_id.condition());
            ++failures;
            report("错误",period+" "+error.what());
        }
        ++state.completed;
    }
    if(group=="funding_rate_settlement") {
        int64_t begin=options.start;
        for(const auto& [period,key]:remote) begin=std::max(begin,period_range(period).second);
        funding_rest(db,options,symbol,begin,report);
    }
    if(failures) throw std::runtime_error(std::to_string(failures)+" 个来源下载失败");
    if(group!="funding_rate_settlement") consolidate(db,options,group,symbol,report);
    report("完成",std::to_string(state.completed)+" 个来源已处理");
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
        const bool is_packed=db.store.packed();
        if (is_packed) {
            if (std::find(options.groups.begin(),options.groups.end(),"funding_rate_settlement")!=options.groups.end()) {
                // TODO: 支持向打包数据集写入 SQLite 时序组 funding_rate_settlement
                throw std::runtime_error("暂不支持向打包数据集写入 SQLite 时序组 funding_rate_settlement");
            }
        }
        storage::create_index(db);
        storage::meta_set(db,"port","binance");
        storage::ensure_default_version(db);
        db.exec("PRAGMA journal_mode=DELETE;"
                "CREATE TABLE IF NOT EXISTS __binance_datavision_paths__(id INTEGER PRIMARY KEY,parent_id INTEGER,name TEXT,UNIQUE(parent_id,name));CREATE TABLE IF NOT EXISTS __binance_datavision_templates__(id INTEGER PRIMARY KEY,path_id INTEGER,group_id INTEGER,symbol_id INTEGER,UNIQUE(path_id,group_id,symbol_id));"
                "CREATE TABLE IF NOT EXISTS __binance_datavision_files__(template_id INTEGER,period INTEGER,file_id INTEGER,error TEXT,rows INTEGER,PRIMARY KEY(template_id,period)) WITHOUT ROWID;"
                "CREATE INDEX IF NOT EXISTS __binance_datavision_files_file_id__ ON __binance_datavision_files__(file_id) WHERE file_id IS NOT NULL;"
                "CREATE TABLE IF NOT EXISTS __binance_datavision_scans__(group_id INTEGER,symbol_id INTEGER,monthly INTEGER,start INTEGER,end INTEGER,PRIMARY KEY(group_id,symbol_id,monthly)) WITHOUT ROWID;"
                "CREATE TABLE IF NOT EXISTS __binance_rest_funding__(symbol_id INTEGER,period INTEGER,rows INTEGER,error TEXT,covered_until INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(symbol_id,period)) WITHOUT ROWID;");
        if(!is_packed && std::find(options.groups.begin(),options.groups.end(),"funding_rate_settlement")!=options.groups.end()) {
            if(!fs::exists(options.root/"funding.sqlite")) db.attach_funding();
            create_funding(db);
        }
    }
    auto symbols=options.symbols;
    if(symbols.empty()) {
        if(options.progress) options.progress({0,"","","发现","全品种目录"});
        symbols=discover_symbols(options);
        if(options.progress) options.progress({0,"","","发现",std::to_string(symbols.size())+" 个 USDT 品种"});
    }
    std::vector<std::pair<std::string,std::string>> jobs;
    for (const auto& group:options.groups) {
        if (group=="exchangeinfo") continue;
        if (group!="kline" && group!="oi" && group!="funding_rate_settlement") throw std::runtime_error("未知下载组："+group);
        for (const auto& symbol:symbols) jobs.emplace_back(group,symbol);
    }
    if(options.progress) options.progress({0,"","","总进度","",0,jobs.size()});
    std::atomic<size_t> next{0},finished{0};
    std::mutex mutex;
    std::vector<std::string> errors;
    std::vector<std::thread> threads;
    for (size_t i=0;i<std::min(options.jobs,jobs.size());++i) {
        threads.emplace_back([&,slot=i+1] {
            while (true) {
                const auto j=next++;
                if (j>=jobs.size()) break;
                try {
                    one(options,jobs[j].first,jobs[j].second,slot);
                } catch (const std::exception& error) {
                    if(options.progress) options.progress({slot,jobs[j].first,jobs[j].second,"错误",error.what()});
                    std::lock_guard lock(mutex);
                    errors.push_back(jobs[j].first+" "+jobs[j].second+": "+error.what());
                }
                const auto done=++finished;
                if(options.progress) options.progress({0,"","","总进度","",done,jobs.size()});
            }
        });
    }
    for (auto& thread:threads) thread.join();
    if (std::find(options.groups.begin(),options.groups.end(),"exchangeinfo")!=options.groups.end()) {
        if(options.progress) options.progress({0,"exchangeinfo","","下载","交易所信息"});
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
    if(options.progress) options.progress({0,"","",errors.empty()?"完成":"错误",std::to_string(errors.size())+" 个任务失败",jobs.size(),jobs.size()});
    if (!errors.empty()) {
        std::string message;
        for (const auto& error:errors) message+=error+"\n";
        throw std::runtime_error(message);
    }
}

}

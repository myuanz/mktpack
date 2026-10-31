#include <mktpack/binance.hpp>
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
struct DB {
    sqlite3* db=nullptr;
    fs::path root;
    explicit DB(const fs::path& path):root(path.parent_path()) {
        if (sqlite3_open(path.c_str(),&db)!=SQLITE_OK) throw std::runtime_error("无法打开索引");
        sqlite3_busy_timeout(db,60000);
    }
    ~DB() { sqlite3_close(db); }
    void exec(const std::string& sql) {
        char* error=nullptr;
        if (sqlite3_exec(db,sql.c_str(),nullptr,nullptr,&error)!=SQLITE_OK) {
            const std::string message=error;
            sqlite3_free(error);
            throw std::runtime_error(message);
        }
    }
    bool completed(const std::string& key, bool verify_presence) {
        sqlite3_stmt* statement;
        sqlite3_prepare_v2(db,"SELECT s.success,m.path FROM __binance_download_status__ s "
                             "LEFT JOIN __binance_file_mapping__ m ON m.key=s.key WHERE s.key=?",
                           -1,&statement,nullptr);
        sqlite3_bind_text(statement,1,key.c_str(),-1,SQLITE_TRANSIENT);
        bool ready=sqlite3_step(statement)==SQLITE_ROW && sqlite3_column_int(statement,0);
        std::string missing;
        if (ready && verify_presence) {
            const auto path=sqlite3_column_text(statement,1);
            ready=path && fs::exists(root/(const char*)path);
            if (path && !ready) missing=(const char*)path;
        }
        sqlite3_finalize(statement);
        if (!missing.empty()) {
            exec("UPDATE __binance_download_status__ SET success=0,error='载荷缺失' WHERE key IN "
                 "(SELECT key FROM __binance_file_mapping__ WHERE path="+quote(missing)+")");
        }
        return ready;
    }
    bool has(const std::string& key) { return completed(key,false); }
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

void register_file(DB& db, const fs::path& root, const fs::path& path, const Table& table) {
    const auto& time=table.columns.at("time");
    const auto relative=fs::relative(path,root).generic_string();
    const auto symbol=table.metadata.at("symbol").get<std::string>();
    const auto group=table.metadata.at("group").get<std::string>();
    db.exec("INSERT OR REPLACE INTO __group__ VALUES("+quote(group)+",'timeseries')");
    const auto stored=info(path);
    for (const auto& descriptor:stored.at("columns")) {
        const auto name=descriptor.at("name").get<std::string>();
        const Json schema={{"type",descriptor.at("type")},{"scale",descriptor.value("scale",0)}};
        db.exec("INSERT INTO __col__ VALUES("+quote(group)+","+quote(name)+","+quote(schema.dump())+") "
                "ON CONFLICT(group_name,name) DO UPDATE SET schema=json_set(excluded.schema,'$.scale',"
                "max(json_extract(__col__.schema,'$.scale'),json_extract(excluded.schema,'$.scale')))");
    }
    db.exec("INSERT INTO __timefile__(group_name,symbol,path,first_time,last_time,rows) VALUES("
            +quote(group)+","+quote(symbol)+","+quote(relative)+","+std::to_string(time.front())+","
            +std::to_string(time.back())+","+std::to_string(time.size())+") "
            "ON CONFLICT(path) DO UPDATE SET first_time=excluded.first_time,last_time=excluded.last_time,rows=excluded.rows");
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
        register_file(db,options.root,destination,table);
        const auto target=quote(fs::relative(destination,options.root).generic_string());
        for (const auto& path:paths) {
            if (path==destination) continue;
            const auto old=quote(fs::relative(path,options.root).generic_string());
            db.exec("UPDATE __binance_file_mapping__ SET path="+target+" WHERE path="+old);
            db.exec("DELETE FROM __timefile__ WHERE path="+old);
        }
        db.exec("COMMIT");
        for (const auto& path:paths) if (path!=destination) fs::remove(path);
    }
}

void store_source(DB& db, const Options& options, const std::string& group,
                  const std::string& symbol, const std::string& period,
                  const std::string& key, Table table) {
    const auto directory=options.root/group/symbol;
    auto destination=directory/(period+".mkp");
    const auto begin=timestamp(period.size()==7?period+"-01":period);
    const auto date=year_month_day{floor<days>(sys_time<milliseconds>{milliseconds{begin}})};
    const auto end=duration_cast<milliseconds>((period.size()==7?sys_days{date+months{1}}:sys_days{date}+days{1}).time_since_epoch()).count();
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
    register_file(db,options.root,destination,combined);
    const auto target=quote(fs::relative(destination,options.root).generic_string());
    for (const auto& path:replaced) {
        if(path==destination) continue;
        const auto old=quote(fs::relative(path,options.root).generic_string());
        db.exec("UPDATE __binance_file_mapping__ SET path="+target+" WHERE path="+old);
        db.exec("DELETE FROM __timefile__ WHERE path="+old);
    }
    db.exec("INSERT OR REPLACE INTO __binance_file_mapping__ VALUES("+quote(key)+","+target+")");
    db.exec("INSERT OR REPLACE INTO __binance_download_status__ VALUES("+quote(key)+",1,'')");
    db.exec("COMMIT");
    for(const auto& path:replaced) if(path!=destination) fs::remove(path);
}
void funding_rest(DB& db, const Options& options, const std::string& symbol, int64_t begin) {
    if (begin>=options.end) return;
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
        auto day_key=key+"#"+period;
        if(db.completed(day_key,options.full)) continue;
        db.exec("INSERT OR IGNORE INTO __binance_datavision_files__ VALUES("+quote(day_key)+",'funding_rate_settlement',"+quote(symbol)+","+quote(period)+")");
        store_source(db,options,"funding_rate_settlement",symbol,period,day_key,std::move(table));
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
        db.exec("INSERT OR IGNORE INTO __binance_datavision_files__ VALUES("+quote(key)+","+quote(group)+","+quote(symbol)+","+quote(period)+")");
        if (db.completed(key,options.full)) continue;
        try {
            auto table=csv(unzip(source(options.cache,key)),group,symbol,INT64_MIN,INT64_MAX);
            if (table.columns.at("time").empty()) continue;
            store_source(db,options,group,symbol,period,key,table);
            std::lock_guard lock(output_mutex);
            std::cerr<<group<<" "<<symbol<<" "<<period<<" "<<table.columns.at("time").size()<<" 行\n";
        } catch (const std::exception& error) {
            sqlite3_exec(db.db,"ROLLBACK",nullptr,nullptr,nullptr);
            db.exec("INSERT OR REPLACE INTO __binance_download_status__ VALUES("+quote(key)+",0,"+quote(error.what())+")");
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
    consolidate(db,options,group,symbol);
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
        db.exec("PRAGMA journal_mode=WAL;"
                "CREATE TABLE IF NOT EXISTS __group__(name TEXT PRIMARY KEY,kind TEXT);"
                "CREATE TABLE IF NOT EXISTS __col__(group_name TEXT,name TEXT,schema TEXT,PRIMARY KEY(group_name,name));"
                "CREATE TABLE IF NOT EXISTS __timefile__(id INTEGER PRIMARY KEY,group_name TEXT,symbol TEXT,path TEXT UNIQUE,first_time INTEGER,last_time INTEGER,rows INTEGER);"
                "CREATE TABLE IF NOT EXISTS __binance_datavision_files__(key TEXT PRIMARY KEY,group_name TEXT,symbol TEXT,period TEXT);"
                "CREATE TABLE IF NOT EXISTS __binance_download_status__(key TEXT PRIMARY KEY,success INTEGER,error TEXT);"
                "CREATE TABLE IF NOT EXISTS __binance_file_mapping__(key TEXT PRIMARY KEY,path TEXT);");
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
        db.exec("INSERT OR REPLACE INTO __group__ VALUES('exchangeinfo','ordinary')");
        db.exec("COMMIT");
    }
    if (!errors.empty()) {
        std::string message;
        for (const auto& error:errors) message+=error+"\n";
        throw std::runtime_error(message);
    }
}

Json check(const fs::path& root) {
    DB db(root/"mkt.sqlite");
    sqlite3_stmt* statement;
    sqlite3_prepare_v2(db.db,"SELECT path,first_time,last_time,rows FROM __timefile__",-1,&statement,nullptr);
    Json errors=Json::array();
    size_t count=0;
    while (sqlite3_step(statement)==SQLITE_ROW) {
        const std::string path=(const char*)sqlite3_column_text(statement,0);
        try {
            const auto table=read(root/path);
            const auto& time=table.columns.at("time");
            if (time.empty() || time.front()!=sqlite3_column_int64(statement,1)
                || time.back()!=sqlite3_column_int64(statement,2)
                || time.size()!=size_t(sqlite3_column_int64(statement,3))) errors.push_back(path);
        } catch (const std::exception& error) {
            errors.push_back(path+": "+error.what());
        }
        ++count;
    }
    sqlite3_finalize(statement);
    return {{"files",count},{"errors",errors}};
}
}

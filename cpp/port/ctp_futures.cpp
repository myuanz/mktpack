#include <mktpack/ctp_futures.hpp>
#include <mktpack/decimal.hpp>
#include "../storage/database.hpp"
#include <miniz.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <chrono>
#include <cmath>
#include <climits>
#include <cctype>
#include <functional>
#include <fstream>
#include <future>
#include <deque>
#include <numeric>
#include <set>

namespace mktpack::ctp_futures {
namespace {
namespace fs=std::filesystem;
using storage::Database;
using storage::Statement;
using View=std::string_view;
using Row=std::vector<View>;

enum class Stage:size_t {
    discover,catalog,index_open,source_total,source_index,source_read,csv_parse_build,
    block_finish,mkp_encode,mkp_write,encode_wait,publish_index,source_commit,
    archive_scan,month_total,month_read,month_insert,month_commit,
    year_total,year_read,year_insert,year_commit,count
};

constexpr std::array stage_names={
    "discover","catalog","index_open","source_total","source_index","source_read","csv_parse_build",
    "block_finish","mkp_encode","mkp_write","encode_wait","publish_index","source_commit",
    "archive_scan","month_total","month_read","month_insert","month_commit",
    "year_total","year_read","year_insert","year_commit"
};

enum class Counter:size_t {
    source_files,csv_rows,csv_compressed_bytes,csv_bytes,imported_sources,skipped_sources,
    blocks,rows,mkp_bytes,month_days,month_read_bytes,month_write_bytes,
    year_days,year_read_bytes,year_write_bytes,count
};

constexpr std::array counter_names={
    "source_files","csv_rows","csv_compressed_bytes","csv_bytes","imported_sources","skipped_sources",
    "blocks","rows","mkp_bytes","month_days","month_read_bytes","month_write_bytes",
    "year_days","year_read_bytes","year_write_bytes"
};

class Profile {
    using Clock=std::chrono::steady_clock;
    struct Metric {
        std::atomic<uint64_t> calls{0},total{0},minimum{UINT64_MAX},maximum{0};
        std::array<std::atomic<uint64_t>,64> histogram{};
    };
    fs::path path;
    Json configuration;
    Clock::time_point started=Clock::now();
    std::array<Metric,static_cast<size_t>(Stage::count)> metrics;
    std::array<std::atomic<uint64_t>,static_cast<size_t>(Counter::count)> counters{};

    static void lower(std::atomic<uint64_t>& target,uint64_t value) {
        auto old=target.load(std::memory_order_relaxed);
        while(value<old && !target.compare_exchange_weak(old,value,std::memory_order_relaxed));
    }
    static void raise(std::atomic<uint64_t>& target,uint64_t value) {
        auto old=target.load(std::memory_order_relaxed);
        while(value>old && !target.compare_exchange_weak(old,value,std::memory_order_relaxed));
    }
    static double milliseconds(uint64_t nanoseconds) {return double(nanoseconds)/1'000'000;}
    static double percentile(const Metric& metric,uint64_t calls,unsigned percent) {
        const auto target=(calls*percent+99)/100;
        uint64_t seen=0;
        for(size_t i=0;i<metric.histogram.size();++i) {
            seen+=metric.histogram[i].load(std::memory_order_relaxed);
            if(seen>=target) return i==63?double(UINT64_MAX)/1'000:double(uint64_t(1)<<i)/1'000;
        }
        return 0;
    }
public:
    class Scope {
        Profile* profile=nullptr;
        Stage stage{};
        Clock::time_point start;
    public:
        Scope(Profile* owner,Stage value):profile(owner),stage(value) {if(profile)start=Clock::now();}
        Scope(const Scope&)=delete;
        Scope(Scope&& other) noexcept:profile(other.profile),stage(other.stage),start(other.start) {other.profile=nullptr;}
        ~Scope() {if(profile)profile->add(stage,Clock::now()-start);}
    };

    Profile(fs::path output,size_t jobs):path(std::move(output)),configuration{{"jobs",jobs},
        {"sqlite_journal","PERSIST"}} {}
    bool enabled() const {return !path.empty();}
    Scope scope(Stage stage) {return Scope(enabled()?this:nullptr,stage);}
    void add(Stage stage,Clock::duration duration) {
        if(!enabled())return;
        const auto ns=uint64_t(std::max<int64_t>(0,std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count()));
        auto& metric=metrics[static_cast<size_t>(stage)];
        metric.calls.fetch_add(1,std::memory_order_relaxed);
        metric.total.fetch_add(ns,std::memory_order_relaxed);
        lower(metric.minimum,ns);raise(metric.maximum,ns);
        const auto us=ns/1'000;
        const auto bin=std::min<size_t>(63,std::bit_width(us));
        metric.histogram[bin].fetch_add(1,std::memory_order_relaxed);
    }
    void count(Counter counter,uint64_t value=1) {
        if(enabled())counters[static_cast<size_t>(counter)].fetch_add(value,std::memory_order_relaxed);
    }
    void finish(const std::string& status,const std::string& error="") {
        if(!enabled())return;
        Json result={{"version",1},{"status",status},
            {"elapsed_ms",milliseconds(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now()-started).count())},
            {"configuration",configuration},
            {"stages",Json::object()},{"counters",Json::object()}};
        if(!error.empty())result["error"]=error;
        for(size_t i=0;i<metrics.size();++i) {
            const auto calls=metrics[i].calls.load(std::memory_order_relaxed);
            if(!calls)continue;
            const auto total=metrics[i].total.load(std::memory_order_relaxed);
            result["stages"][stage_names[i]]={{"calls",calls},{"total_ms",milliseconds(total)},
                {"mean_ms",milliseconds(total)/calls},
                {"min_ms",milliseconds(metrics[i].minimum.load(std::memory_order_relaxed))},
                {"max_ms",milliseconds(metrics[i].maximum.load(std::memory_order_relaxed))},
                {"p50_upper_ms",percentile(metrics[i],calls,50)},
                {"p95_upper_ms",percentile(metrics[i],calls,95)},
                {"p99_upper_ms",percentile(metrics[i],calls,99)}};
        }
        for(size_t i=0;i<counters.size();++i)
            result["counters"][counter_names[i]]=counters[i].load(std::memory_order_relaxed);
        if(!path.parent_path().empty())fs::create_directories(path.parent_path());
        const auto temporary=fs::path(path.string()+".tmp");
        {std::ofstream output(temporary);output<<result.dump(2)<<'\n';if(!output)throw std::runtime_error("无法写入 profile："+path.string());}
        fs::remove(path);fs::rename(temporary,path);
        path.clear();
    }
};

std::string upper(View value) {
    std::string result(value);
    for (char& c:result) c=static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return result;
}

int64_t integer(View text) {
    int64_t n;
    const auto [end,error]=std::from_chars(text.data(),text.data()+text.size(),n);
    if (error==std::errc{} && end==text.data()+text.size()) return n;
    auto decimal=decimal_integer(text);
    if (decimal.decimals) throw std::runtime_error("需要整数："+std::string(text));
    return decimal.value;
}

int date_number(std::string text) {
    if (text.size()==10 && text[4]=='-' && text[7]=='-') {
        text.erase(7,1);
        text.erase(4,1);
    }
    if (text.size()!=8 || text.find_first_not_of("0123456789")!=text.npos)
        throw std::runtime_error("日期格式错误："+text);
    auto n=static_cast<int>(integer(text));
    if (!(std::chrono::year(n/10000)/((n/100)%100)/(n%100)).ok())
        throw std::runtime_error("无效日期："+text);
    return n;
}

// 镜像 CSV 的标识和数值均为 ASCII；中文名称无需转码。
void split_row(View line,Row& fields) {
    fields.clear();
    while (true) {
        size_t end=0;
        if (!line.empty() && line.front()=='"') {
            end=1;
            while (true) {
                end=line.find('"',end);
                if (end==line.npos) throw std::runtime_error("CSV 引号未闭合");
                ++end;
                if (end==line.size() || line[end]!='"') break;
                ++end;
            }
            fields.push_back(line.substr(1,end-2));
            if (end<line.size() && line[end]!=',') throw std::runtime_error("CSV 引号后缺少分隔符");
        } else {
            end=line.find(',');
            fields.push_back(line.substr(0,end));
        }
        if (end==line.npos || end==line.size()) return;
        line.remove_prefix(end+1);
    }
}

class CsvInput {
public:
    struct Stats {uint64_t lines=0,compressed=0,uncompressed=0,parse_ns=0;};
private:
    std::string pending;
    std::function<void(const Row&)> visit;
    std::exception_ptr error;
    Row fields;
    bool measure=false;
    Stats stats;
    void line(View text) {
        if (!text.empty() && text.back()=='\r') text.remove_suffix(1);
        if (text.empty()) return;
        split_row(text,fields);
        visit(fields);
        ++stats.lines;
    }
    static size_t chunk(void* opaque,mz_uint64,const void* bytes,size_t size) {
        auto& csv=*static_cast<CsvInput*>(opaque);
        const auto started=csv.measure?std::chrono::steady_clock::now():std::chrono::steady_clock::time_point{};
        try {
            View input(static_cast<const char*>(bytes),size);
            size_t end;
            while ((end=input.find('\n'))!=input.npos) {
                auto part=input.substr(0,end);
                if (csv.pending.empty()) csv.line(part);
                else {csv.pending.append(part); csv.line(csv.pending); csv.pending.clear();}
                input.remove_prefix(end+1);
            }
            csv.pending.append(input);
            if(csv.measure)csv.stats.parse_ns+=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now()-started).count();
            return size;
        } catch (...) {
            if(csv.measure)csv.stats.parse_ns+=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now()-started).count();
            csv.error=std::current_exception();return 0;
        }
    }
public:
    explicit CsvInput(std::function<void(const Row&)> visitor,bool timed=false):visit(std::move(visitor)),measure(timed) {}
    Stats read(const fs::path& path) {
        mz_zip_archive zip{};
        if (!mz_zip_reader_init_file(&zip,path.string().c_str(),0)) throw std::runtime_error("无法打开 ZIP："+path.string());
        try {
            const auto member=path.stem().string();
            const auto i=mz_zip_reader_locate_file(&zip,member.c_str(),nullptr,MZ_ZIP_FLAG_IGNORE_PATH);
            if (i<0) throw std::runtime_error("ZIP 缺少 "+member);
            mz_zip_archive_file_stat stat{};
            if(!mz_zip_reader_file_stat(&zip,i,&stat))throw std::runtime_error("ZIP 无法读取成员信息");
            stats.compressed=stat.m_comp_size;stats.uncompressed=stat.m_uncomp_size;
            const auto ok=mz_zip_reader_extract_to_callback(&zip,i,chunk,this,0);
            if (error) std::rethrow_exception(error);
            if (!ok) throw std::runtime_error("ZIP 解压失败");
            const auto started=measure?std::chrono::steady_clock::now():std::chrono::steady_clock::time_point{};
            line(pending);
            if(measure)stats.parse_ns+=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now()-started).count();
        } catch (const std::exception& ex) {
            mz_zip_reader_end(&zip);
            throw std::runtime_error(path.string()+"："+ex.what());
        }
        mz_zip_reader_end(&zip);
        return stats;
    }
};

struct Header {
    std::map<std::string,size_t,std::less<>> positions;
    void set(const Row& row) {
        for (size_t i=0;i<row.size();++i) positions.emplace(row[i],i);
    }
    size_t at(const std::string& key) const {
        auto it=positions.find(key);
        if (it==positions.end()) throw std::runtime_error("CSV 缺少字段："+key);
        return it->second;
    }
};

struct Contract {std::string name;int64_t multiplier=0;};
struct Listing {Contract contract;int start,end;};

class Catalog {
    std::map<std::string,std::vector<Listing>> listed;
    std::map<std::string,int> expirations;
    void load(const fs::path& path) {
        Header h;
        size_t exchange=0,instrument=0,name=0,open=0,expiry=0,multiple=0;
        CsvInput csv([&](const Row& r){
            if (h.positions.empty()) {
                h.set(r);
                const bool old=h.positions.contains("instrumentid_key");
                exchange=h.at(old?"exchangeid":"exchange_id");
                instrument=h.at(old?"instrumentid":"instrument_id");
                name=h.at(old?"instrumentid_key":"mk_instrument_id");
                open=h.at(old?"opendate":"open_date");
                expiry=h.at(old?"expiredate":"expire_date");
                multiple=h.at(old?"volumemultiple":"volume_multiple");
                return;
            }
            if (r.at(name).empty() || r.at(open).empty() || r.at(expiry).empty()) return;
            const auto key=upper(r.at(exchange))+"/"+upper(r.at(instrument));
            auto mult=r.at(multiple);
            const auto canonical=upper(r.at(name));
            const auto end=date_number(std::string(r.at(expiry)));
            listed[key].push_back({{canonical,mult.empty()?0:integer(mult)},
                    date_number(std::string(r.at(open))),end});
            expirations[canonical]=std::max(expirations[canonical],end);
        });
        csv.read(path);
    }
public:
    explicit Catalog(const std::vector<fs::path>& roots) {
        // 合约快照包含历年已退市合约。每个镜像取最新快照，以存续日期区分同名代码。
        for (const auto& root:roots) {
            auto dir=root/"basicinfo"/"future_instrument";
            if (!fs::is_directory(dir)) continue;
            fs::path latest;
            for (const auto& e:fs::directory_iterator(dir)) {
                auto name=e.path().filename().string();
                if (name.starts_with("future_instrument") && name.ends_with(".csv.zip") && e.path()>latest) latest=e.path();
            }
            if (!latest.empty()) load(latest);
        }
    }
    Contract resolve(View exchange,View id,View unique,int day) const {
        const auto raw=upper(id);
        std::string canonical;
        if (!unique.empty()) {
            auto kind=unique.find("|F|"),last=unique.rfind('|');
            if (kind==unique.npos || last<=kind+2) throw std::runtime_error("无效期货标识："+std::string(unique));
            canonical=upper(unique.substr(kind+3,last-kind-3))+upper(unique.substr(last+1));
        }
        auto records=listed.find(upper(exchange)+"/"+raw);
        if (records!=listed.end()) for (const auto& r:records->second)
            if (r.start<=day && day<=r.end && (canonical.empty() || r.contract.name==canonical)) return r.contract;
        if (canonical.empty()) {
            canonical=raw;
            auto start=canonical.find_first_of("0123456789");
            if (upper(exchange)=="CZCE" && start!=canonical.npos && canonical.size()-start==3) {
                int current=day/10000;
                int year=current/10*10+canonical[start]-'0';
                if (year<current) year+=10;
                canonical.insert(start,1,static_cast<char>('0'+year/10%10));
            }
        }
        return {canonical,0};
    }
    bool expired(const std::string& symbol,int day) const {
        auto it=expirations.find(symbol);
        return it!=expirations.end() && it->second<day;
    }
};

struct Field {std::string name;bool price=false,decimal=false;};

std::vector<Field> fields() {
    std::vector<Field> out={{"time"},{"trading_day"},{"volume"},{"turnover",false,true},{"open_interest",false,true},{"pre_open_interest",false,true}};
    for (auto name:{"last_price","open_price","highest_price","lowest_price","close_price","settlement_price","upper_limit_price","lower_limit_price","pre_settlement_price","pre_close_price"})
        out.push_back({name,true,true});
    for (std::string side:{"bid_","ask_"}) for (int i=1;i<=5;++i) {
        out.push_back({side+"price"+std::to_string(i),true,true});
        out.push_back({side+"volume"+std::to_string(i)});
    }
    return out;
}

Json schema(const std::vector<Field>& fields,int64_t multiplier) {
    Json result=Json::object();
    for (const auto& field:fields) {
        auto& spec=result[field.name];
        spec={{"type",field.name=="time"?"timestamp":field.decimal?"decimal":"integer"},{"scale",0}};
        if (field.price) spec["scale_group"]="price";
    }
    auto& prediction=result["turnover"]["prediction"];
    prediction={{"kind","trade_value"},{"price","last_price"},{"volume","volume"}};
    if (multiplier>0) prediction["multiplier"]=multiplier;
    for (std::string side:{"bid_","ask_"}) {
        std::vector<std::string> prices,quantities;
        for (int i=1;i<=5;++i) {
            prices.push_back(side+"price"+std::to_string(i));
            quantities.push_back(side+"volume"+std::to_string(i));
        }
        for (int i=0;i<5;++i) {
            const auto ref=i?prices[i-1]:side=="bid_"?"last_price":"bid_price1";
            result[prices[i]]["prediction"]={{"kind","reference"},{"column",ref}};
            result[quantities[i]]["prediction"]={{"kind","level_quantity"},{"price",prices[i]},{"prices",prices},{"quantities",quantities}};
        }
    }
    return result;
}

DecimalInteger decimal(View text,bool price) {
    if (price && text.find_first_of(".eE")!=text.npos) {
        double number;
        auto [end,error]=std::from_chars(text.data(),text.data()+text.size(),number);
        if (end!=text.data()+text.size() || error!=std::errc{} || !std::isfinite(number))
            throw std::runtime_error("无效价格："+std::string(text));
        char buf[64];
        auto result=std::to_chars(buf,buf+sizeof buf,number,std::chars_format::general);
        if (result.ec!=std::errc{}) throw std::runtime_error("无法转换价格");
        return decimal_integer(View(buf,result.ptr));
    }
    return decimal_integer(text);
}

struct Values {std::vector<int64_t> data;int scale=0;};
struct DayBlock {
    Contract contract;
    std::vector<Values> columns;
    void append(const Row& row,const std::vector<Field>& fields,const std::vector<size_t>& positions,int day) {
        for (size_t i=0;i<columns.size();++i) {
            auto& col=columns[i];
            if (!fields[i].decimal) {
                col.data.push_back(i==1?day:integer(row.at(positions[i])));
                continue;
            }
            const auto n=decimal(row.at(positions[i]),fields[i].price);
            if (n.decimals>col.scale) {
                for (auto& val:col.data) val=rescale(val,col.scale,n.decimals);
                col.scale=n.decimals;
            }
            col.data.push_back(rescale(n.value,n.decimals,col.scale));
        }
    }
    Table finish(const std::vector<Field>& fields,int day) {
        Table t;
        t.metadata={{"group","tick"},{"symbol",contract.name}};
        t.schema=schema(fields,contract.multiplier);
        const auto& times=columns[0].data;
        std::vector<size_t> order;
        if (!std::is_sorted(times.begin(),times.end())) {
            order.resize(times.size());std::iota(order.begin(),order.end(),0);
            std::stable_sort(order.begin(),order.end(),[&](auto a,auto b){return times[a]<times[b];});
        }
        for (size_t i=0;i<fields.size();++i) {
            const auto& name=fields[i].name;
            t.schema[name]["scale"]=columns[i].scale;
            auto& dst=t.columns[name];
            if (order.empty()) dst=std::move(columns[i].data);
            else {dst.reserve(order.size());for (auto k:order) dst.push_back(columns[i].data[k]);}
        }
        return t;
    }
};

struct Source {fs::path path;std::string series;int day;};
struct Stats {int64_t imported=0,skipped=0,blocks=0,rows=0;};

class Sources {
    Database& db;
    Profile& profile;
    int64_t group;
    size_t jobs;
    const std::set<std::string>& wanted;
    const std::vector<Field> names=fields();
    Json group_schema;
    std::map<std::string,int64_t> symbol_keys;

    int64_t series_id(const std::string& series) {
        Statement insert(db,"INSERT OR IGNORE INTO __ctp_futures_series__(name) VALUES(?)");
        insert.text(1,series);insert.next();
        Statement find(db,"SELECT id FROM __ctp_futures_series__ WHERE name=?");
        find.text(1,series);find.next();return find.integer(0);
    }
    bool needed(int64_t series,int day) {
        Statement q(db,"SELECT 1 FROM __ctp_futures_files__ WHERE series_id=? AND period=?");
        q.integer(1,series);q.integer(2,day);
        return !q.next();
    }
    std::set<std::string> loaded(int day) {
        std::set<std::string> result;
        Statement q(db,"SELECT s.name FROM __timefile__ f JOIN __symbols__ s ON s.id=f.symbol_id "
                "WHERE f.group_id=? AND f.period=?");
        q.integer(1,group);q.integer(2,day);
        while(q.next())result.insert(q.text(0));
        return result;
    }
    int64_t symbol_id(const std::string& symbol) {
        auto found=symbol_keys.find(symbol);
        if(found!=symbol_keys.end())return found->second;
        const auto id=storage::symbol_id(db,symbol);
        symbol_keys.emplace(symbol,id);
        return id;
    }
    void publish(const Table& t,int64_t symbol,int period) {
        const auto& time=t.columns.at("time");
        Statement target(db,"INSERT INTO __timefile__(group_id,symbol_id,period,first_time,last_time,rows) VALUES(?,?,?,?,?,?)");
        target.integer(1,group);target.integer(2,symbol);target.integer(3,period);
        target.integer(4,time.front());target.integer(5,time.back());target.integer(6,time.size());target.next();
    }
    void merge_schema(Json& logical,const Table& table) {
        for(const auto& [name,column]:table.schema.items()) {
            auto& spec=logical[name];
            const int scale=spec.is_null()?0:spec.value("scale",0);
            spec={{"type",column.at("type")},{"scale",std::max(scale,column.value("scale",0))}};
        }
    }
public:
    Sources(Database& database,Profile& profiler,const std::set<std::string>& requested,size_t concurrency):db(database),profile(profiler),group(storage::group_id(db,"tick")),jobs(concurrency),wanted(requested) {
        Json logical={{"symbol",{{"type","string"}}}};
        const auto specs=schema(names,0);
        for (const auto& [name,spec]:specs.items()) {
            logical[name]={{"type",spec.at("type")},{"scale",0}};
        }
        Statement fill(db,"UPDATE __group__ SET schema=? WHERE id=? AND schema='{}'");
        fill.text(1,logical.dump());fill.integer(2,group);fill.next();
        Statement current(db,"SELECT schema FROM __group__ WHERE id=?");
        current.integer(1,group);current.next();group_schema=Json::parse(current.text(0));
    }
    void import(const Source& source,const Catalog& catalog,Stats& stats) {
        auto source_timer=profile.scope(Stage::source_total);
        db.exec("BEGIN IMMEDIATE");
        std::vector<fs::path> publishing;
        try {
            auto commit=[&] {
                auto timer=profile.scope(Stage::source_commit);
                db.exec("COMMIT");
            };
            int64_t series;
            std::set<std::string> existing;
            bool pending_source;
            {
                auto timer=profile.scope(Stage::source_index);
                series=series_id(source.series);
                pending_source=needed(series,source.day);
                if(pending_source)existing=loaded(source.day);
            }
            if(!pending_source) {commit();++stats.skipped;profile.count(Counter::skipped_sources);return;}

            Header header;std::vector<size_t> positions;
            size_t exchange=0,instrument=0,uid=0;
            std::map<std::string,DayBlock,std::less<>> blocks;
            // 输入的行缓冲复用，缓存合约标识时持有字符串。
            std::map<std::pair<std::string,std::string>,DayBlock*> by_id;
            auto alias=by_id.end();
            CsvInput csv([&](const Row& row){
                if (header.positions.empty()) {
                    header.set(row);
                    exchange=header.at("exchange_id"); instrument=header.at("instrument_id");
                    uid=header.positions.contains("unique_instrument_id")?header.at("unique_instrument_id"):row.size();
                    for (const auto& field:names)
                        positions.push_back(field.name=="trading_day"?0:header.at(field.name=="time"?"trade_timestamp":field.name));
                    return;
                }
                const auto exchange_name=row.at(exchange),instrument_name=row.at(instrument);
                if(alias==by_id.end() || alias->first.first!=exchange_name || alias->first.second!=instrument_name) {
                    auto key=std::make_pair(std::string(exchange_name),std::string(instrument_name));
                    alias=by_id.find(key);
                    if (alias==by_id.end()) {
                        auto item=catalog.resolve(key.first,key.second,uid<row.size()?row[uid]:View{},source.day);
                        if (item.name.empty() || item.name.find_first_not_of("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")!=std::string::npos)
                            throw std::runtime_error("无效合约代码："+item.name);
                        auto [it,fresh]=blocks.try_emplace(item.name);
                        if (fresh) {
                            it->second.contract=item;
                            if ((wanted.empty() || wanted.contains(item.name)) &&
                                !existing.contains(item.name))
                                it->second.columns.resize(names.size());
                        }
                        alias=by_id.emplace(std::move(key),&it->second).first;
                    }
                }
                if (!alias->second->columns.empty()) alias->second->append(row,names,positions,source.day);
            },profile.enabled());
            CsvInput::Stats csv_stats;
            {auto timer=profile.scope(Stage::source_read);csv_stats=csv.read(source.path);}
            profile.add(Stage::csv_parse_build,std::chrono::nanoseconds(csv_stats.parse_ns));
            profile.count(Counter::csv_rows,csv_stats.lines?csv_stats.lines-1:0);
            profile.count(Counter::csv_compressed_bytes,csv_stats.compressed);
            profile.count(Counter::csv_bytes,csv_stats.uncompressed);
            if (header.positions.empty()) throw std::runtime_error("缺少 CSV 表头："+source.path.string());
            const auto ymd=std::to_string(source.day);
            bool changed=false;
            struct Prepared {std::string symbol;Table table;};
            std::deque<std::future<Prepared>> pending;
            auto next_schema=group_schema;
            auto collect=[&] {
                auto prepared=[&] {
                    auto timer=profile.scope(Stage::encode_wait);
                    return pending.front().get();
                }();
                pending.pop_front();
                auto timer=profile.scope(Stage::publish_index);
                const auto sid=symbol_id(prepared.symbol);
                publish(prepared.table,sid,source.day);
                merge_schema(next_schema,prepared.table);
                changed=true;stats.rows+=prepared.table.columns.at("time").size();++stats.blocks;
                profile.count(Counter::rows,prepared.table.columns.at("time").size());
                profile.count(Counter::blocks);
            };
            for (auto& [symbol,block]:blocks) {
                if (!block.columns.empty()) {
                    auto path=db.root/"tick"/symbol/(ymd+".mkp");
                    publishing.push_back(path);
                    pending.push_back(std::async(jobs==1?std::launch::deferred:std::launch::async,
                        [this,symbol,path,day=source.day,block=std::move(block)]() mutable {
                            auto table=[&] {
                                auto timer=profile.scope(Stage::block_finish);
                                return block.finish(names,day);
                            }();
                            auto bytes=[&] {
                                auto timer=profile.scope(Stage::mkp_encode);
                                return encode(table);
                            }();
                            profile.count(Counter::mkp_bytes,bytes.size());
                            {auto timer=profile.scope(Stage::mkp_write);write(path,bytes);}
                            return Prepared{symbol,std::move(table)};
                        }));
                    if(pending.size()>=jobs)collect();
                }
            }
            while(!pending.empty())collect();
            if(next_schema!=group_schema) {
                Statement update(db,"UPDATE __group__ SET schema=? WHERE id=?");
                update.text(1,next_schema.dump());update.integer(2,group);update.next();
            }
            if(wanted.empty()) {
                Statement file(db,"INSERT INTO __ctp_futures_files__(series_id,period) VALUES(?,?)");
                file.integer(1,series);file.integer(2,source.day);file.next();
            }
            commit();
            group_schema=std::move(next_schema);
            if(changed) {++stats.imported;profile.count(Counter::imported_sources);}
            else {++stats.skipped;profile.count(Counter::skipped_sources);}
        } catch (...) {
            for (const auto& path:publishing) {
                std::error_code ec;fs::remove(path,ec);fs::remove(path.string()+".tmp",ec);
            }
            db.exec("ROLLBACK");symbol_keys.clear();throw;
        }
    }
};

struct StoredDay {int64_t id,volume;std::string symbol,path;int period;};

fs::path volume_file(const Database& db,const std::string& path) {
    return db.root/fs::path(path);
}

std::vector<uint8_t> file_bytes(const fs::path& path) {
    std::ifstream input(path,std::ios::binary|std::ios::ate);
    if(!input)throw std::runtime_error("无法读取："+path.string());
    std::vector<uint8_t> bytes(size_t(input.tellg()));
    input.seekg(0);
    input.read(reinterpret_cast<char*>(bytes.data()),bytes.size());
    if(!input)throw std::runtime_error("载荷读取失败："+path.string());
    return bytes;
}

int64_t volume_id(Database& db,const std::string& path) {
    Statement insert(db,"INSERT OR IGNORE INTO __volumes__(path) VALUES(?)");
    insert.text(1,path);insert.next();
    Statement find(db,"SELECT id FROM __volumes__ WHERE path=?");
    find.text(1,path);find.next();return find.integer(0);
}

void insert_blob(Statement& insert,int64_t id,const std::vector<uint8_t>& bytes) {
    sqlite3_reset(insert.statement);
    sqlite3_clear_bindings(insert.statement);
    insert.integer(1,id);
    if(sqlite3_bind_blob64(insert.statement,2,bytes.data(),bytes.size(),SQLITE_TRANSIENT)!=SQLITE_OK)
        throw std::runtime_error(sqlite3_errmsg(sqlite3_db_handle(insert.statement)));
    insert.next();
}

void archive_month(Database& db,Profile& profile,const std::string& path,const std::vector<StoredDay>& days) {
    auto total=profile.scope(Stage::month_total);
    profile.count(Counter::month_days,days.size());
    const auto file=volume_file(db,path);
    fs::create_directories(file.parent_path());
    {
        Statement attach(db,"ATTACH DATABASE ? AS ctp_volume");attach.text(1,file.string());attach.next();
    }
    std::vector<fs::path> loose;
    try {
        db.exec("BEGIN IMMEDIATE;CREATE TABLE IF NOT EXISTS ctp_volume.blob(id INTEGER PRIMARY KEY,bytes BLOB NOT NULL)");
        Statement insert(db,"INSERT INTO ctp_volume.blob(id,bytes) VALUES(?,?)");
        const auto volume=volume_id(db,path);
        Statement update(db,"UPDATE __timefile__ SET volume_id=? WHERE id=? AND volume_id IS NULL");
        for(const auto& day:days) {
            const auto source=db.root/"tick"/day.symbol/(std::to_string(day.period)+".mkp");
            std::vector<uint8_t> bytes;
            {auto timer=profile.scope(Stage::month_read);bytes=file_bytes(source);}
            profile.count(Counter::month_read_bytes,bytes.size());
            {auto timer=profile.scope(Stage::month_insert);insert_blob(insert,day.id,bytes);}
            profile.count(Counter::month_write_bytes,bytes.size());
            update.integer(1,volume);update.integer(2,day.id);update.next();sqlite3_reset(update.statement);
            loose.push_back(source);
        }
        {auto timer=profile.scope(Stage::month_commit);db.exec("COMMIT");}
    } catch(...) {
        db.exec("ROLLBACK");
        db.exec("DETACH DATABASE ctp_volume");
        throw;
    }
    db.exec("DETACH DATABASE ctp_volume");
    for(const auto& path:loose)fs::remove(path);
}

class VolumeBytes {
    const Database& index;
    std::map<std::string,std::unique_ptr<Database>> databases;
public:
    explicit VolumeBytes(const Database& db):index(db) {}
    std::vector<uint8_t> read(const std::string& path,int64_t id) {
        auto& db=databases[path];
        if(!db)db=std::make_unique<Database>(volume_file(index,path));
        Statement q(*db,"SELECT bytes FROM blob WHERE id=?");q.integer(1,id);
        if(!q.next())throw std::runtime_error("卷缺少载荷："+path);
        const auto size=sqlite3_column_bytes(q.statement,0);
        const auto* data=static_cast<const uint8_t*>(sqlite3_column_blob(q.statement,0));
        return {data,data+size};
    }
};

void archive_year(Database& index,Profile& profile,const std::string& path,const std::vector<StoredDay>& days) {
    auto total=profile.scope(Stage::year_total);
    profile.count(Counter::year_days,days.size());
    const auto destination=volume_file(index,path),temporary=fs::path(destination.string()+".tmp");
    fs::create_directories(destination.parent_path());
    fs::remove(temporary);
    {
        Database output(temporary,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
        output.exec("PRAGMA journal_mode=DELETE;BEGIN;CREATE TABLE blob(id INTEGER PRIMARY KEY,bytes BLOB NOT NULL)");
        Statement insert(output,"INSERT INTO blob(id,bytes) VALUES(?,?)");
        VolumeBytes source(index);
        for(const auto& day:days) {
            std::vector<uint8_t> bytes;
            {auto timer=profile.scope(Stage::year_read);bytes=source.read(day.path,day.id);}
            profile.count(Counter::year_read_bytes,bytes.size());
            {auto timer=profile.scope(Stage::year_insert);insert_blob(insert,day.id,bytes);}
            profile.count(Counter::year_write_bytes,bytes.size());
        }
        {auto timer=profile.scope(Stage::year_commit);output.exec("COMMIT");}
    }
    fs::remove(destination);
    fs::rename(temporary,destination);

    std::map<int64_t,std::string> old;
    for(const auto& day:days)if(day.path!=path)old.emplace(day.volume,day.path);
    index.exec("BEGIN IMMEDIATE");
    try {
        const auto volume=volume_id(index,path);
        Statement update(index,"UPDATE __timefile__ SET volume_id=? WHERE id=?");
        for(const auto& day:days) {
            update.integer(1,volume);update.integer(2,day.id);update.next();sqlite3_reset(update.statement);
        }
        for(const auto& [id,source]:old) {
            Statement used(index,"SELECT 1 FROM __timefile__ WHERE volume_id=? LIMIT 1");used.integer(1,id);
            if(used.next())continue;
            Statement erase(index,"DELETE FROM __volumes__ WHERE id=?");erase.integer(1,id);erase.next();
        }
        index.exec("COMMIT");
    } catch(...) {index.exec("ROLLBACK");throw;}
    for(const auto& [id,source]:old) {
        Statement used(index,"SELECT 1 FROM __volumes__ WHERE id=?");used.integer(1,id);
        if(!used.next())fs::remove(volume_file(index,source));
    }
}

void compact_volumes(Database& db,Profile& profile,const Catalog& catalog,int today) {
    const auto group=storage::group_id(db,"tick");
    std::map<std::pair<std::string,int>,std::vector<StoredDay>> months;
    {
        auto timer=profile.scope(Stage::archive_scan);
        Statement q(db,"SELECT f.id,s.name,f.period FROM __timefile__ f JOIN __symbols__ s ON s.id=f.symbol_id "
                "WHERE f.group_id=? AND f.volume_id IS NULL ORDER BY s.name,f.period");q.integer(1,group);
        while(q.next()) {
            StoredDay day{q.integer(0),0,q.text(1),"",int(q.integer(2))};
            if(day.period/100<today/100 || catalog.expired(day.symbol,today))
                months[{day.symbol,day.period/100}].push_back(std::move(day));
        }
    }
    for(const auto& [key,days]:months) {
        const auto& [symbol,month]=key;
        archive_month(db,profile,(fs::path("tick")/symbol/(std::to_string(month)+".data")).string(),days);
    }

    std::map<std::pair<std::string,int>,std::vector<StoredDay>> years;
    {
        auto timer=profile.scope(Stage::archive_scan);
        Statement q(db,"SELECT f.id,f.volume_id,s.name,f.period,v.path FROM __timefile__ f "
                "JOIN __symbols__ s ON s.id=f.symbol_id JOIN __volumes__ v ON v.id=f.volume_id "
                "WHERE f.group_id=? ORDER BY s.name,f.period");q.integer(1,group);
        while(q.next()) {
            StoredDay day{q.integer(0),q.integer(1),q.text(2),q.text(4),int(q.integer(3))};
            if(day.period/10000<today/10000 || catalog.expired(day.symbol,today))
                years[{day.symbol,day.period/10000}].push_back(std::move(day));
        }
    }
    for(const auto& [key,days]:years) {
        const auto& [symbol,year]=key;
        const auto path=(fs::path("tick")/symbol/(std::to_string(year)+".data")).string();
        if(std::all_of(days.begin(),days.end(),[&](const auto& day){return day.path==path;}))continue;
        archive_year(db,profile,path,days);
    }
}

std::vector<Source> discover(const std::vector<fs::path>& roots,int start,int end,const std::set<std::string>& products) {
    std::vector<Source> files;
    for (const auto& root:roots) {
        const auto dir=root/"tick"/"future";
        auto accept=[&](const fs::directory_entry& entry) {
            auto name=entry.path().filename().string();
            if (!name.starts_with("FUTURE_TICK_") || !name.ends_with(".csv.zip")) return;
            auto pos=name.rfind('_');
            constexpr View prefix="FUTURE_TICK_";
            auto key=View(name).substr(prefix.size(),pos-prefix.size());
            if(key.ends_with("_F"))return;
            if (!products.empty()) {
                // 旧文件以品种开头，新文件先写交易所；行内再按完整合约名筛选。
                auto split=key.find('_');
                auto first=key.substr(0,split);
                auto rest=split==key.npos?View{}:key.substr(split+1);
                if (!products.contains(std::string(first)) &&
                    !products.contains(std::string(rest.substr(0,rest.find('_'))))) return;
            }
            const auto period=date_number(name.substr(pos+1,name.size()-pos-9));
            if (start<=period && period<end)
                files.push_back({entry.path(),root.filename().string()+"/"+name.substr(0,pos),period});
        };
        if(start && end!=INT_MAX) {
            const auto first=std::chrono::sys_days(std::chrono::year(start/10000)/((start/100)%100)/(start%100));
            const auto last=std::chrono::sys_days(std::chrono::year(end/10000)/((end/100)%100)/(end%100));
            for(auto date=first;date<last;date+=std::chrono::days(1)) {
                const std::chrono::year_month_day ymd(date);
                auto part=[](unsigned value) {
                    std::string text(2,'0');
                    text[0]=static_cast<char>('0'+value/10);
                    text[1]=static_cast<char>('0'+value%10);
                    return text;
                };
                const auto daily=dir/std::to_string(int(ymd.year()))/part(unsigned(ymd.month()))/part(unsigned(ymd.day()));
                if(fs::is_directory(daily))for(const auto& entry:fs::directory_iterator(daily))accept(entry);
            }
        } else {
            for (const auto& entry:fs::recursive_directory_iterator(dir)) accept(entry);
        }
    }
    std::sort(files.begin(),files.end(),[](const Source& a,const Source& b){return std::tie(a.day,a.series)<std::tie(b.day,b.series);});
    return files;
}
}

static Json import_ticks_impl(const Options& options,Profile& profile) {
    if(!options.jobs)throw std::runtime_error("并行任务数必须大于零");
    const int start=options.start.empty()?0:date_number(options.start);
    const int end=options.end.empty()?INT_MAX:date_number(options.end);
    if (start>=end) throw std::runtime_error("需要 start < end");
    std::vector<fs::path> roots;
    for (const auto& entry:fs::directory_iterator(options.mirror)) {
        const auto& root=entry.path();
        if (root.filename().string().starts_with("nfs_rq") && fs::is_directory(root/"tick"/"future"))
            roots.push_back(root);
    }
    std::sort(roots.rbegin(),roots.rend());
    if (roots.empty()) throw std::runtime_error("镜像中找不到 nfs_rq*/tick/future");
    std::set<std::string> symbols;
    for (const auto& symbol:options.symbols) symbols.insert(upper(symbol));
    std::set<std::string> products;
    for (const auto& symbol:symbols) products.insert(symbol.substr(0,symbol.find_first_of("0123456789")));
    std::vector<Source> files;
    {auto timer=profile.scope(Stage::discover);files=discover(roots,start,end,products);}
    std::unique_ptr<Catalog> catalog;
    {auto timer=profile.scope(Stage::catalog);catalog=std::make_unique<Catalog>(roots);}
    fs::create_directories(options.root);
    std::unique_ptr<Database> database;
    {auto timer=profile.scope(Stage::index_open);database=std::make_unique<Database>(options.root,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);}
    auto& db=*database;
    {
        Statement check(db,"PRAGMA application_id");check.next();
        if (check.integer(0)==0x4d4b5450) throw std::runtime_error("不能导入到已打包数据集");
    }
    db.exec("PRAGMA journal_mode=PERSIST;BEGIN IMMEDIATE");
    storage::create_index(db);
    db.exec("CREATE TABLE IF NOT EXISTS __ctp_futures_series__(id INTEGER PRIMARY KEY,name TEXT UNIQUE NOT NULL);"
            "CREATE TABLE IF NOT EXISTS __ctp_futures_files__(series_id INTEGER,period INTEGER,PRIMARY KEY(series_id,period)) WITHOUT ROWID;");
    Sources sources(db,profile,symbols,options.jobs);
    db.exec("COMMIT");
    Stats stats;
    profile.count(Counter::source_files,files.size());
    for (const auto& file:files) sources.import(file,*catalog,stats);
    auto today=std::chrono::year_month_day{std::chrono::floor<std::chrono::days>(std::chrono::system_clock::now()+std::chrono::hours(8))};
    compact_volumes(db,profile,*catalog,int(today.year())*10000+int(unsigned(today.month()))*100+int(unsigned(today.day())));
    db.exec("PRAGMA journal_mode=DELETE");
    return {{"imported",stats.imported},{"skipped",stats.skipped},{"blocks",stats.blocks},{"rows",stats.rows}};
}

Json import_ticks(const Options& options) {
    Profile profile(options.profile,options.jobs);
    try {
        auto result=import_ticks_impl(options,profile);
        profile.finish("ok");
        return result;
    } catch(const std::exception& error) {
        profile.finish("error",error.what());
        throw;
    }
}
}

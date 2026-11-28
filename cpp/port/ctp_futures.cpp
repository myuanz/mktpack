#include <mktpack/ctp_futures.hpp>
#include <mktpack/decimal.hpp>
#include "../storage/database.hpp"
#include "../storage/store.hpp"
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
#include <condition_variable>
#include <functional>
#include <fstream>
#include <future>
#include <deque>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <queue>
#include <set>
#include <span>
#include <thread>

namespace mktpack::ctp_futures {
namespace {
namespace fs=std::filesystem;
using storage::Database;
using storage::Statement;
using storage::insert_blob;
using storage::read_file_bytes;
using View=std::string_view;
using Row=std::vector<View>;

enum class Stage:size_t {
    discover,catalog,index_open,source_prepare,source_index,zip_read,zip_decode,csv_parse_build,
    read_wait,zip_queue_wait,decode_wait,
    block_finish,mkp_encode,mkp_write,source_wait,publish_index,source_commit,
    archive_scan,month_total,month_read,month_insert,month_commit,
    year_total,year_read,year_insert,year_commit,bulk_total,bulk_commit,count
};

constexpr std::array stage_names={
    "discover","catalog","index_open","source_prepare","source_index","zip_read","zip_decode","csv_parse_build",
    "read_wait","zip_queue_wait","decode_wait",
    "block_finish","mkp_encode","mkp_write","source_wait","publish_index","source_commit",
    "archive_scan","month_total","month_read","month_insert","month_commit",
    "year_total","year_read","year_insert","year_commit","bulk_total","bulk_commit"
};

enum class Counter:size_t {
    source_files,zip_bytes,csv_rows,csv_compressed_bytes,csv_bytes,imported_sources,skipped_sources,
    blocks,rows,mkp_bytes,month_days,month_read_bytes,month_write_bytes,
    year_days,year_read_bytes,year_write_bytes,bulk_days,bulk_bytes,count
};

constexpr std::array counter_names={
    "source_files","zip_bytes","csv_rows","csv_compressed_bytes","csv_bytes","imported_sources","skipped_sources",
    "blocks","rows","mkp_bytes","month_days","month_read_bytes","month_write_bytes",
    "year_days","year_read_bytes","year_write_bytes","bulk_days","bulk_bytes"
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

    Profile(fs::path output,size_t jobs,size_t readers):path(std::move(output)),configuration{{"jobs",jobs},{"readers",readers},
        {"sqlite_journal","MEMORY"},{"sqlite_synchronous","OFF"},{"sqlite_locking","EXCLUSIVE"}} {}
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
        auto temporary=path;
        temporary+=".tmp";
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
    Stats read(const fs::path& path,std::vector<uint8_t> bytes) {
        mz_zip_archive zip{};
        if (!mz_zip_reader_init_mem(&zip,bytes.data(),bytes.size(),0)) throw std::runtime_error("无法打开 ZIP："+path.string());
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
        csv.read(path,read_file_bytes(path));
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
        spec={{"type",field.name=="time"?"timestamp":field.decimal?"real":"integer"},{"scale",0}};
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

DecimalInteger decimal(View text) {
    if (text.find_first_of(".eE")!=text.npos) {
        double number;
        auto [end,error]=std::from_chars(text.data(),text.data()+text.size(),number);
        if (end!=text.data()+text.size() || error!=std::errc{} || !std::isfinite(number))
            throw std::runtime_error("无效数值："+std::string(text));
        char buf[64];
        auto result=std::to_chars(buf,buf+sizeof buf,number,std::chars_format::general);
        if (result.ec!=std::errc{}) throw std::runtime_error("无法转换数值");
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
            const auto n=decimal(row.at(positions[i]));
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

std::string tick_volume(const std::string& symbol,int period) {
    return (fs::path("tick")/symbol/(std::to_string(period)+".data")).generic_string();
}

int64_t volume_id(Database& db,const std::string& path);

template<class Write>
void with_volume(Database& db,const std::string& path,Write write_rows) {
    const auto file=db.root/fs::path(path);
    fs::create_directories(file.parent_path());
    {
        Statement attach(db,"ATTACH DATABASE ? AS ctp_volume");
        attach.text(1,file.string());attach.next();
    }
    try {
        db.exec("PRAGMA ctp_volume.journal_mode=MEMORY;PRAGMA ctp_volume.synchronous=OFF;PRAGMA ctp_volume.locking_mode=EXCLUSIVE");
        write_rows();
    } catch(...) {
        db.exec("DETACH DATABASE ctp_volume");
        throw;
    }
    db.exec("DETACH DATABASE ctp_volume");
}

using Symbols=std::set<std::string>;

struct EncodedBlock {
    std::string symbol;
    std::vector<uint8_t> scales;
    int64_t first,last;
    size_t rows;
    std::vector<uint8_t> bytes;
};

struct PreparedSource {
    const Source& source;
    std::vector<EncodedBlock> blocks;
};

struct ImportPlan {
    bool backfill;
    std::vector<const Source*> sources;
    std::map<int,Symbols> existing;
};

class SourceResults {
    struct Task {
        const Source* source;
        std::promise<PreparedSource> result;
    };
    struct Loaded {
        Task task;
        std::vector<uint8_t> bytes;
    };
    static constexpr size_t zip_budget=128*1024*1024;

    const Catalog& catalog;
    const std::vector<Field>& names;
    const Symbols& wanted;
    Profile& profile;
    const std::map<int,Symbols>& existing;
    std::span<const Source* const> remaining;
    std::mutex mutex;
    std::condition_variable_any read_ready,decode_ready,zip_space;
    std::queue<Task> tasks;
    std::queue<Loaded> loaded;
    size_t queued_bytes=0;
    std::deque<std::future<PreparedSource>> pending;
    int write_day=0;
    Symbols written;
    std::vector<std::jthread> workers,readers; // 先结束线程，再销毁任务队列。

    PreparedSource prepare(const Source& source,std::vector<uint8_t> bytes) {
        auto source_timer=profile.scope(Stage::source_prepare);
        const auto day=existing.find(source.day);
        const auto* known=day==existing.end()?nullptr:&day->second;
        Header header;std::vector<size_t> positions;
        size_t exchange=0,instrument=0,uid=0;
        std::map<std::string,DayBlock,std::less<>> blocks;
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
                            (!known || !known->contains(item.name)))
                            it->second.columns.resize(names.size());
                    }
                    alias=by_id.emplace(std::move(key),&it->second).first;
                }
            }
            if (!alias->second->columns.empty()) alias->second->append(row,names,positions,source.day);
        },profile.enabled());
        CsvInput::Stats csv_stats;
        {
            auto timer=profile.scope(Stage::zip_decode);
            csv_stats=csv.read(source.path,std::move(bytes));
        }
        profile.add(Stage::csv_parse_build,std::chrono::nanoseconds(csv_stats.parse_ns));
        profile.count(Counter::csv_rows,csv_stats.lines?csv_stats.lines-1:0);
        profile.count(Counter::csv_compressed_bytes,csv_stats.compressed);
        profile.count(Counter::csv_bytes,csv_stats.uncompressed);
        if (header.positions.empty()) throw std::runtime_error("缺少 CSV 表头："+source.path.string());

        PreparedSource prepared{source,{}};
        for (auto& [symbol,block]:blocks) if (!block.columns.empty()) {
            Table table;
            {
                auto timer=profile.scope(Stage::block_finish);
                table=block.finish(names,source.day);
            }
            std::vector<uint8_t> scales;
            scales.reserve(names.size());
            for(const auto& field:names)scales.push_back(table.schema.at(field.name).at("scale").get<int>());
            const auto& time=table.columns.at("time");
            const auto first=time.front(),last=time.back();
            const auto rows=time.size();
            std::vector<uint8_t> bytes;
            {
                auto timer=profile.scope(Stage::mkp_encode);
                bytes=encode(std::move(table));
            }
            profile.count(Counter::mkp_bytes,bytes.size());
            prepared.blocks.push_back({symbol,std::move(scales),first,last,rows,std::move(bytes)});
        }
        return prepared;
    }

    void read(std::stop_token stop) {
        while(!stop.stop_requested()) {
            std::unique_lock lock(mutex);
            if(tasks.empty()) {
                auto timer=profile.scope(Stage::read_wait);
                read_ready.wait(lock,stop,[&] {return !tasks.empty();});
            }
            if(stop.stop_requested())return;
            auto task=std::move(tasks.front());
            tasks.pop();
            lock.unlock();

            std::vector<uint8_t> bytes;
            try {
                auto timer=profile.scope(Stage::zip_read);
                bytes=read_file_bytes(task.source->path);
            } catch(...) {
                task.result.set_exception(std::current_exception());
                continue;
            }
            profile.count(Counter::zip_bytes,bytes.size());

            lock.lock();
            const auto fits=[&] {
                return loaded.empty() || (queued_bytes<=zip_budget && bytes.size()<=zip_budget-queued_bytes);
            };
            if(!fits()) {
                auto timer=profile.scope(Stage::zip_queue_wait);
                if(!zip_space.wait(lock,stop,fits))return;
            }
            if(stop.stop_requested())return;
            queued_bytes+=bytes.size();
            loaded.push({std::move(task),std::move(bytes)});
            lock.unlock();
            decode_ready.notify_one();
        }
    }
    void decode(std::stop_token stop) {
        while(!stop.stop_requested()) {
            std::unique_lock lock(mutex);
            if(loaded.empty()) {
                auto timer=profile.scope(Stage::decode_wait);
                if(!decode_ready.wait(lock,stop,[&] {return !loaded.empty();}))return;
            }
            if(stop.stop_requested())return;
            auto input=std::move(loaded.front());
            loaded.pop();
            queued_bytes-=input.bytes.size();
            lock.unlock();
            zip_space.notify_all();

            try {
                input.task.result.set_value(prepare(*input.task.source,std::move(input.bytes)));
            } catch(...) {
                input.task.result.set_exception(std::current_exception());
            }
        }
    }
    void schedule() {
        if(remaining.empty())return;
        Task task{remaining.front(),{}};
        remaining=remaining.subspan(1);
        pending.push_back(task.result.get_future());
        {
            std::lock_guard lock(mutex);
            tasks.push(std::move(task));
        }
        read_ready.notify_one();
    }
public:
    SourceResults(const ImportPlan& plan,const Catalog& contracts,const std::vector<Field>& fields,
                  const Symbols& requested,Profile& profiler,size_t jobs,size_t read_jobs)
        :catalog(contracts),names(fields),wanted(requested),profile(profiler),
         existing(plan.existing),remaining(plan.sources) {
        const auto count=std::min(jobs,remaining.size());
        const auto reader_count=std::min(read_jobs,remaining.size());
        // 有序写入等待慢包时，后面的包仍可读取和处理。
        const auto window=std::min(remaining.size(),4*std::max(count,reader_count));
        workers.reserve(count);
        readers.reserve(reader_count);
        for(size_t i=0;i<count;++i)workers.emplace_back([this](std::stop_token stop) {decode(stop);});
        for(size_t i=0;i<reader_count;++i)readers.emplace_back([this](std::stop_token stop) {read(stop);});
        for(size_t i=0;i<window;++i)schedule();
    }
    PreparedSource take() {
        auto prepared=[&] {
            auto timer=profile.scope(Stage::source_wait);
            return pending.front().get();
        }();
        pending.pop_front();
        schedule();
        if(prepared.source.day!=write_day) {
            write_day=prepared.source.day;
            written.clear();
        }
        // 同日多个源包可能在前一包提交前完成编码。
        std::erase_if(prepared.blocks,[&](const auto& block) {return !written.insert(block.symbol).second;});
        return prepared;
    }
};

class TickWriter {
    struct Day {int period;const EncodedBlock* block;};
    Database& db;
    storage::PayloadStore& store;
    Profile& profile;
    Stats& stats;
    const std::vector<Field>& names;
    int64_t group;
    bool complete_sources;
    Json group_schema;
    std::map<std::string,int64_t> symbol_keys;

    int64_t series_id(const std::string& series) {
        Statement insert(db,"INSERT OR IGNORE INTO __ctp_futures_series__(name) VALUES(?)");
        insert.text(1,series);insert.next();
        Statement find(db,"SELECT id FROM __ctp_futures_series__ WHERE name=?");
        find.text(1,series);find.next();return find.integer(0);
    }
    int64_t symbol_id(const std::string& symbol) {
        auto found=symbol_keys.find(symbol);
        if(found!=symbol_keys.end())return found->second;
        const auto id=storage::symbol_id(db,symbol);
        symbol_keys.emplace(symbol,id);
        return id;
    }
    int64_t publish(const EncodedBlock& block,int day,std::optional<int64_t> volume,Json& logical) {
        auto timer=profile.scope(Stage::publish_index);
        Statement target(db,"INSERT INTO __timefile__(group_id,symbol_id,period,first_time,last_time,rows,volume_id) "
                            "VALUES(?,?,?,?,?,?,?)");
        target.integer(1,group);target.integer(2,symbol_id(block.symbol));target.integer(3,day);
        target.integer(4,block.first);target.integer(5,block.last);target.integer(6,block.rows);
        if(volume)target.integer(7,*volume);
        target.next();
        for(size_t i=0;i<names.size();++i) {
            auto& scale=logical.at(names[i].name).at("scale");
            if(block.scales[i]>scale.get<int>())scale=block.scales[i];
        }
        stats.rows+=block.rows;++stats.blocks;
        profile.count(Counter::rows,block.rows);profile.count(Counter::blocks);
        return sqlite3_last_insert_rowid(db.db);
    }

    template<class Write>
    void transaction(Stage stage,Write write_rows) {
        auto next_schema=group_schema;
        db.exec("BEGIN IMMEDIATE");
        try {
            write_rows(next_schema);
            if(next_schema!=group_schema) {
                Statement update(db,"UPDATE __group__ SET schema=? WHERE id=?");
                update.text(1,next_schema.dump());update.integer(2,group);update.next();
            }
            {auto timer=profile.scope(stage);db.exec("COMMIT");}
        } catch (...) {
            db.exec("ROLLBACK");
            throw;
        }
        group_schema=std::move(next_schema);
    }
    void finish_source(const PreparedSource& prepared) {
        if(complete_sources) {
            Statement file(db,"INSERT INTO __ctp_futures_files__(series_id,period) VALUES(?,?)");
            file.integer(1,series_id(prepared.source.series));file.integer(2,prepared.source.day);file.next();
        }
        if(!prepared.blocks.empty()) {++stats.imported;profile.count(Counter::imported_sources);}
        else {++stats.skipped;profile.count(Counter::skipped_sources);}
    }

    void write_loose(std::span<const Day> days,std::span<const PreparedSource> sources,Stage stage) {
        std::vector<fs::path> publishing;
        publishing.reserve(days.size());
        try {
            transaction(stage,[&](Json& logical) {
                for(const auto& [day,block]:days) {
                    publish(*block,day,std::nullopt,logical);
                    const auto& path=publishing.emplace_back(db.root/"tick"/block->symbol/(std::to_string(day)+".mkp"));
                    auto timer=profile.scope(Stage::mkp_write);
                    write(path,block->bytes);
                }
                for(const auto& source:sources)finish_source(source);
            });
        } catch(...) {
            for(const auto& path:publishing) {
                std::error_code ec;
                fs::remove(path,ec);
                auto temporary=path;
                temporary+=".tmp";
                fs::remove(temporary,ec);
            }
            throw;
        }
    }
    void write_volume(const std::string& path,const std::vector<Day>& days) {
        with_volume(db,path,[&] {
            transaction(Stage::bulk_commit,[&](Json& logical) {
                db.exec("CREATE TABLE IF NOT EXISTS ctp_volume.blob(id INTEGER PRIMARY KEY,bytes BLOB NOT NULL)");
                const auto volume=volume_id(db,path);
                Statement insert(db,"INSERT INTO ctp_volume.blob(id,bytes) VALUES(?,?)");
                for(const auto& [day,block]:days) {
                    const auto id=publish(*block,day,volume,logical);
                    insert_blob(insert,id,block->bytes);
                    profile.count(Counter::bulk_days);
                    profile.count(Counter::bulk_bytes,block->bytes.size());
                }
            });
        });
    }
    void write_hot(std::span<const Day> days,std::span<const PreparedSource> sources,Stage stage) {
        store.begin_hot_batch();
        try {
            transaction(stage,[&](Json& logical) {
                for(const auto& [day,block]:days) {
                    {
                        auto timer=profile.scope(Stage::mkp_write);
                        store.write_blob(group,"tick",symbol_id(block->symbol),block->symbol,
                                         day,block->first,block->last,block->rows,block->bytes);
                    }
                    for(size_t i=0;i<names.size();++i) {
                        auto& scale=logical.at(names[i].name).at("scale");
                        if(block->scales[i]>scale.get<int>())scale=block->scales[i];
                    }
                    stats.rows+=block->rows;++stats.blocks;
                    profile.count(Counter::rows,block->rows);profile.count(Counter::blocks);
                }
                for(const auto& source:sources)finish_source(source);
                store.commit_hot_batch();
            });
        } catch(...) {
            store.rollback_hot_batch();
            throw;
        }
    }
public:
    TickWriter(Database& database,storage::PayloadStore& payload_store,Profile& profiler,Stats& result,const std::vector<Field>& fields,bool complete)
        :db(database),store(payload_store),profile(profiler),stats(result),names(fields),group(storage::group_id(db,"tick")),complete_sources(complete) {
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

    ImportPlan plan(const std::vector<Source>& files) {
        auto timer=profile.scope(Stage::source_index);
        Statement present(db,"SELECT 1 FROM __timefile__ WHERE group_id=? LIMIT 1");
        present.integer(1,group);
        ImportPlan plan{!present.next(),{},{}};
        Statement needed(db,"SELECT 1 FROM __ctp_futures_files__ f JOIN __ctp_futures_series__ s ON s.id=f.series_id "
                            "WHERE s.name=? AND f.period=?");
        for(const auto& source:files) {
            needed.text(1,source.series);needed.integer(2,source.day);
            if(!needed.next())plan.sources.push_back(&source);
            else {++stats.skipped;profile.count(Counter::skipped_sources);}
            sqlite3_reset(needed.statement);
        }
        if(plan.backfill || plan.sources.empty())return plan;

        for(const auto* source:plan.sources)plan.existing.try_emplace(source->day);
        Statement loaded(db,"SELECT f.period,s.name FROM __timefile__ f JOIN __symbols__ s ON s.id=f.symbol_id "
                            "WHERE f.group_id=? AND f.period BETWEEN ? AND ?");
        loaded.integer(1,group);loaded.integer(2,plan.sources.front()->day);loaded.integer(3,plan.sources.back()->day);
        while(loaded.next()) {
            const auto day=plan.existing.find(int(loaded.integer(0)));
            if(day!=plan.existing.end())day->second.insert(loaded.text(1));
        }
        return plan;
    }
    void write_source(const PreparedSource& source) {
        std::vector<Day> days;
        days.reserve(source.blocks.size());
        for(const auto& block:source.blocks)days.push_back({source.source.day,&block});
        if(store.packed())write_hot(days,std::span(&source,1),Stage::source_commit);
        else write_loose(days,std::span(&source,1),Stage::source_commit);
    }
    void write_year(std::span<const PreparedSource> sources,const Catalog& catalog,int today) {
        if(sources.empty())return;
        auto timer=profile.scope(Stage::bulk_total);
        if(store.packed()) {
            std::vector<Day> all_days;
            for(const auto& source:sources) {
                for(const auto& block:source.blocks)all_days.push_back({source.source.day,&block});
            }
            write_hot(all_days,sources,Stage::bulk_commit);
            return;
        }
        std::map<std::string,std::vector<Day>> volumes;
        std::vector<Day> loose;
        for(const auto& source:sources) {
            const int day=source.source.day;
            for(const auto& block:source.blocks) {
                if(day/10000<today/10000 || catalog.expired(block.symbol,today))
                    volumes[tick_volume(block.symbol,day/10000)].push_back({day,&block});
                else if(day/100<today/100)
                    volumes[tick_volume(block.symbol,day/100)].push_back({day,&block});
                else
                    loose.push_back({day,&block});
            }
        }
        for(const auto& [path,days]:volumes)write_volume(path,days);
        write_loose(loose,sources,Stage::bulk_commit);
    }
};

void write_import(TickWriter& writer,const ImportPlan& plan,SourceResults& results,const Catalog& catalog,int today) {
    if(!plan.backfill) {
        for(size_t i=0;i<plan.sources.size();++i)writer.write_source(results.take());
        return;
    }
    std::vector<PreparedSource> year;
    for(const auto* source:plan.sources) {
        if(!year.empty() && year.front().source.day/10000!=source->day/10000) {
            writer.write_year(year,catalog,today);
            year.clear();
        }
        try {
            year.push_back(results.take());
        } catch(...) {
            writer.write_year(year,catalog,today);
            throw;
        }
    }
    writer.write_year(year,catalog,today);
}

struct StoredDay {int64_t id,volume;std::string symbol,path;int period;};

fs::path volume_file(const Database& db,const std::string& path) {
    return db.root/fs::path(path);
}

int64_t volume_id(Database& db,const std::string& path) {
    Statement find(db,"SELECT id FROM __volumes__ WHERE path IN (?,?)");
    find.text(1,path);find.text(2,fs::path(path).make_preferred().string());
    if(find.next())return find.integer(0);
    Statement insert(db,"INSERT INTO __volumes__(path) VALUES(?) RETURNING id");
    insert.text(1,path);insert.next();
    return insert.integer(0);
}

void archive_month(Database& db,Profile& profile,const std::string& path,const std::vector<StoredDay>& days) {
    auto total=profile.scope(Stage::month_total);
    profile.count(Counter::month_days,days.size());
    std::vector<fs::path> loose;
    loose.reserve(days.size());
    with_volume(db,path,[&] {
        db.exec("BEGIN IMMEDIATE");
        try {
            db.exec("CREATE TABLE IF NOT EXISTS ctp_volume.blob(id INTEGER PRIMARY KEY,bytes BLOB NOT NULL)");
            Statement insert(db,"INSERT INTO ctp_volume.blob(id,bytes) VALUES(?,?)");
            const auto volume=volume_id(db,path);
            Statement update(db,"UPDATE __timefile__ SET volume_id=? WHERE id=? AND volume_id IS NULL");
            for(const auto& day:days) {
                const auto& source=loose.emplace_back(db.root/"tick"/day.symbol/(std::to_string(day.period)+".mkp"));
                auto bytes=[&] {
                    auto timer=profile.scope(Stage::month_read);
                    return read_file_bytes(source);
                }();
                profile.count(Counter::month_read_bytes,bytes.size());
                {auto timer=profile.scope(Stage::month_insert);insert_blob(insert,day.id,bytes);}
                profile.count(Counter::month_write_bytes,bytes.size());
                update.integer(1,volume);update.integer(2,day.id);update.next();sqlite3_reset(update.statement);
            }
            auto timer=profile.scope(Stage::month_commit);
            db.exec("COMMIT");
        } catch(...) {
            db.exec("ROLLBACK");
            throw;
        }
    });
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
        Statement q(*db,"SELECT bytes FROM blob WHERE id=?");
        return storage::read_blob(q,id,path);
    }
};

using VolumeRefs=std::map<int64_t,int64_t>;

VolumeRefs volume_refs(Database& db) {
    VolumeRefs result;
    Statement q(db,"SELECT volume_id,count(*) FROM __timefile__ WHERE volume_id IS NOT NULL GROUP BY volume_id "
                   "UNION ALL SELECT volume_id,count(*) FROM __sqlite_partitions__ WHERE volume_id IS NOT NULL GROUP BY volume_id");
    while(q.next())result[q.integer(0)]+=q.integer(1);
    return result;
}

void archive_year(Database& index,Profile& profile,const std::string& path,
                  const std::vector<StoredDay>& days,VolumeRefs& refs) {
    auto total=profile.scope(Stage::year_total);
    profile.count(Counter::year_days,days.size());
    const auto destination=volume_file(index,path);
    auto temporary=destination;
    temporary+=".tmp";
    fs::create_directories(destination.parent_path());
    fs::remove(temporary);
    {
        Database output(temporary,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
        output.exec("PRAGMA journal_mode=MEMORY;PRAGMA synchronous=OFF;PRAGMA locking_mode=EXCLUSIVE;BEGIN;CREATE TABLE blob(id INTEGER PRIMARY KEY,bytes BLOB NOT NULL)");
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

    std::vector<std::string> retired;
    index.exec("BEGIN IMMEDIATE");
    try {
        const auto volume=volume_id(index,path);
        Statement update(index,"UPDATE __timefile__ SET volume_id=? WHERE id=?");
        Statement erase(index,"DELETE FROM __volumes__ WHERE id=?");
        for(const auto& day:days) {
            if(day.volume==volume)continue;
            update.integer(1,volume);update.integer(2,day.id);update.next();sqlite3_reset(update.statement);
            ++refs[volume];
            if(--refs.at(day.volume)==0) {
                erase.integer(1,day.volume);erase.next();sqlite3_reset(erase.statement);
                retired.push_back(day.path);
            }
        }
        index.exec("COMMIT");
    } catch(...) {index.exec("ROLLBACK");throw;}
    for(const auto& source:retired)fs::remove(volume_file(index,source));
}

void compact_volumes(Database& db,Profile& profile,const Catalog& catalog,int today) {
    const auto group=storage::group_id(db,"tick");
    const int month_start=today/100*100+1;
    const int year=today/10000;
    std::map<std::pair<std::string,int>,std::vector<StoredDay>> months;
    {
        auto timer=profile.scope(Stage::archive_scan);
        const std::string loose="SELECT f.id,s.name,f.period FROM __timefile__ f JOIN __symbols__ s ON s.id=f.symbol_id "
                "WHERE f.group_id=? AND f.volume_id IS NULL AND f.period";
        auto collect=[&](const std::string& sql,bool require_expired) {
            Statement q(db,sql);
            q.integer(1,group);q.integer(2,month_start);
            while(q.next()) {
                StoredDay day{q.integer(0),0,q.text(1),"",int(q.integer(2))};
                if(!require_expired || catalog.expired(day.symbol,today))
                    months[{day.symbol,day.period/100}].push_back(std::move(day));
            }
        };
        collect(loose+"<? ORDER BY f.period",false);
        collect(loose+">=? ORDER BY f.period",true);
    }
    for(const auto& [key,days]:months) {
        const auto& [symbol,month]=key;
        archive_month(db,profile,tick_volume(symbol,month),days);
    }

    std::set<std::pair<std::string,int>> years;
    {
        auto timer=profile.scope(Stage::archive_scan);
        Statement q(db,"SELECT path FROM __volumes__ "
                       "WHERE substr(path,-11) GLOB '[0-9][0-9][0-9][0-9][0-9][0-9].data'");
        while(q.next()) {
            const fs::path path=q.text(0);
            const auto period=path.stem().string();
            if(period.size()!=6)continue;
            const int month=int(integer(period));
            const auto symbol=path.parent_path().filename().string();
            if(month/100<year || catalog.expired(symbol,today))years.emplace(symbol,month/100);
        }
    }
    if(years.empty())return;

    Statement year_days(db,"SELECT f.id,f.volume_id,f.period,v.path FROM __timefile__ f "
            "JOIN __volumes__ v ON v.id=f.volume_id WHERE f.group_id=? "
            "AND f.symbol_id=(SELECT id FROM __symbols__ WHERE name=?) "
            "AND f.period>=? AND f.period<? ORDER BY f.period");
    std::optional<VolumeRefs> refs;
    for(const auto& [symbol,period]:years) {
        const auto path=tick_volume(symbol,period);
        std::vector<StoredDay> days;
        {
            auto timer=profile.scope(Stage::archive_scan);
            year_days.integer(1,group);year_days.text(2,symbol);
            year_days.integer(3,period*10000);year_days.integer(4,(period+1)*10000);
            while(year_days.next())days.push_back({year_days.integer(0),year_days.integer(1),symbol,
                                                  year_days.text(3),int(year_days.integer(2))});
            sqlite3_reset(year_days.statement);
        }
        if(std::all_of(days.begin(),days.end(),[&](const auto& day){return day.path==path;}))continue;
        if(!refs) {
            auto timer=profile.scope(Stage::archive_scan);
            refs=volume_refs(db);
        }
        archive_year(db,profile,path,days,*refs);
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
    if(!options.readers)throw std::runtime_error("读取线程数必须大于零");
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
    storage::PayloadStore store(db);
    db.exec("PRAGMA journal_mode=MEMORY;PRAGMA synchronous=OFF;PRAGMA locking_mode=EXCLUSIVE;BEGIN EXCLUSIVE");
    storage::create_index(db);
    storage::meta_set(db,"port","ctp-futures");
    storage::ensure_default_version(db);
    db.exec("CREATE TABLE IF NOT EXISTS __ctp_futures_series__(id INTEGER PRIMARY KEY,name TEXT UNIQUE NOT NULL);"
            "CREATE TABLE IF NOT EXISTS __ctp_futures_files__(series_id INTEGER,period INTEGER,PRIMARY KEY(series_id,period)) WITHOUT ROWID;"
            "CREATE INDEX IF NOT EXISTS __ctp_futures_loose__ ON __timefile__(group_id,period,symbol_id) WHERE volume_id IS NULL;");
    Stats stats;
    const auto names=fields();
    TickWriter writer(db,store,profile,stats,names,symbols.empty());
    db.exec("COMMIT");
    const auto date=std::chrono::year_month_day{std::chrono::floor<std::chrono::days>(std::chrono::system_clock::now()+std::chrono::hours(8))};
    const int today=int(date.year())*10000+int(unsigned(date.month()))*100+int(unsigned(date.day()));
    profile.count(Counter::source_files,files.size());
    const auto plan=writer.plan(files);
    {
        SourceResults results(plan,*catalog,names,symbols,profile,options.jobs,options.readers);
        write_import(writer,plan,results,*catalog,today);
    }
    if(!store.packed() && !plan.backfill)compact_volumes(db,profile,*catalog,today);
    return {{"imported",stats.imported},{"skipped",stats.skipped},{"blocks",stats.blocks},{"rows",stats.rows}};
}

Json import_ticks(const Options& options) {
    Profile profile(options.profile,options.jobs,options.readers);
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

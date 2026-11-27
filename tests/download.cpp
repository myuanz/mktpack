#include <cstring>
#include "../cpp/port/binance.cpp"
#include <mktpack/storage.hpp>

namespace {
std::map<std::string,std::string> responses;
int requests=0;
std::vector<std::string> requested_urls;
void expect(bool ok, const char* message) { if(!ok) throw std::runtime_error(message); }
std::string zipped(const std::string& text) {
    mz_zip_archive archive{};
    mz_zip_writer_init_heap(&archive,0,0);
    mz_zip_writer_add_mem(&archive,"data.csv",text.data(),text.size(),MZ_BEST_SPEED);
    void* p=nullptr; size_t size=0;
    mz_zip_writer_finalize_heap_archive(&archive,&p,&size);
    std::string result((char*)p,size);
    mz_free(p);
    mz_zip_writer_end(&archive);
    return result;
}
std::string listing(const std::vector<std::string>& keys) {
    std::string xml="<ListBucketResult>";
    for(auto& key:keys)xml+="<Contents><Key>"+key+"</Key></Contents>";
    return xml+"</ListBucketResult>";
}
}
namespace mktpack::binance {
std::string http_get(const std::string& url) {
    ++requests;
    requested_urls.push_back(url);
    if(responses.contains(url)) return responses.at(url);
    const auto marker=url.find("?list-type=2&prefix=");
    if(marker!=std::string::npos) {
        const auto begin=marker+20;
        const auto prefix=url.substr(begin,url.find('&',begin)-begin);
        const bool directories=url.find("&delimiter=/")!=std::string::npos;
        std::set<std::string> keys,folders;
        for(const auto& [address,body]:responses) {
            if(address.find("?list-type=2&prefix=")==std::string::npos) continue;
            pugi::xml_document doc;
            if(!doc.load_buffer(body.data(),body.size())) continue;
            for(auto item:doc.document_element().children("Contents")) {
                const std::string key=item.child_value("Key");
                if(!key.starts_with(prefix)) continue;
                if(directories) {
                    const auto slash=key.find('/',prefix.size());
                    if(slash!=std::string::npos) folders.insert(key.substr(0,slash+1));
                } else keys.insert(key);
            }
        }
        if(directories) {
            std::string xml="<ListBucketResult>";
            for(const auto& folder:folders) xml+="<CommonPrefixes><Prefix>"+folder+"</Prefix></CommonPrefixes>";
            return xml+"</ListBucketResult>";
        }
        return listing({keys.begin(),keys.end()});
    }
    throw std::runtime_error("模拟下载失败："+url);
}
}
int main() {
    using namespace mktpack;
    using namespace mktpack::binance;
    const auto root=fs::temp_directory_path()/("mktpack-test-"+std::to_string(system_clock::now().time_since_epoch().count()));
    Options options; options.root=root; options.groups={"kline"}; options.symbols={"BTCUSDT"};
    options.start=timestamp("2024-01-01"); options.end=timestamp("2024-02-01"); options.jobs=1;
    const auto prefix="data/futures/um/";
    const std::string daily=std::string(prefix)+"daily/klines/BTCUSDT/1m/";
    const std::string monthly=std::string(prefix)+"monthly/klines/BTCUSDT/1m/";
    const auto day=daily+"BTCUSDT-1m-2024-01-01.zip";
    const auto month=monthly+"BTCUSDT-1m-2024-01.zip";
    responses[host+"?list-type=2&prefix="+daily]=listing({day});
    responses[host+"?list-type=2&prefix="+monthly]=listing({});
    responses[host+day]=zipped("1704067200000,1,3,1,2,10,0,0,0,4,0,0\n");
    download(options);
    auto annual=root/"kline/BTCUSDT/2024.mkp";
    expect(read(annual).columns.at("close")==std::vector<int64_t>{2},"日包合年失败");
    { DB db(root/"mkt.sqlite"); db.exec("DROP INDEX __binance_datavision_files_file_id__"); }
    responses[host+"?list-type=2&prefix="+monthly]=listing({month});
    responses[host+month]=zipped("1704067200000,1.1,3.1,1.1,2.1,10.1,0,0,0,4.1,0,0\n1704153600000,2.1,4.1,2.1,3.1,12.1,0,0,0,5.1,0,0\n");
    download(options);
    expect(read(annual).columns.at("close")==std::vector<int64_t>({21,31}),"月包替换年内日数据失败");
    auto before=requests;
    download(options);
    expect(requests-before==1,"已有月归档不应重复列举日目录或下载载荷");
    fs::remove(annual);
    options.full=true;
    download(options);
    expect(fs::exists(annual)&&read(annual).columns.at("time").size()==2,"全量恢复失败");
    responses.erase(host+month);
    fs::remove(annual);
    bool failed=false;
    try { download(options); } catch(const std::exception&) { failed=true; }
    expect(failed,"模拟失败未返回错误");
    { DB db(root/"mkt.sqlite"); expect(!db.completed(db.source(month,"kline","BTCUSDT","2024-01"),false),"失败状态未记录"); }
    responses[host+month]=zipped("1704067200000,1,3,1,2,10,0,0,0,4,0,0\n");
    download(options);
    { DB db(root/"mkt.sqlite"); expect(db.completed(db.source(month,"kline","BTCUSDT","2024-01"),false),"失败重试未成功"); }
    auto description=mktpack::describe(root,"kline");
    expect(description["schema"]["close"]["scale"]==1,"组精度发生降低");
    const auto february=monthly+"BTCUSDT-1m-2024-02.zip";
    responses[host+"?list-type=2&prefix="+monthly]=listing({month,february});
    responses[host+february]=zipped("1706745600000,2,4,2,3,11,0,0,0,5,0,0\n");
    options.end=timestamp("2024-03-01");
    download(options);
    expect(read(annual).columns.at("time").size()==2,"两月合年失败");
    fs::remove(annual);
    options.end=timestamp("2024-02-01");
    download(options);
    { DB db(root/"mkt.sqlite"); expect(!db.completed(db.source(february,"kline","BTCUSDT","2024-02"),false),"缺失年文件其他来源未失效"); }
    options.full=false;
    options.start=timestamp("2024-02-01");
    options.end=timestamp("2024-03-01");
    download(options);
    expect(read(annual).columns.at("time").size()==2,"局部恢复后增量补另一月失败");
    expect(!info(annual)["metadata"].contains("lossy_columns"),"默认下载未保持无损编码");
    expect(read(annual).columns.at("taker_buy_volume")==std::vector<int64_t>({4,5}),"默认无损下载数值改变");
    {
        DB db(root/"mkt.sqlite");
        sqlite3_stmt* query=nullptr;
        sqlite3_prepare_v2(db.db,"SELECT count(*) FROM __binance_datavision_files__ s LEFT JOIN __timefile__ f ON f.id=s.file_id "
                                "WHERE s.file_id IS NOT NULL AND f.id IS NULL",-1,&query,nullptr);
        expect(sqlite3_step(query)==SQLITE_ROW && sqlite3_column_int(query,0)==0,"来源引用了已删除载荷");
        sqlite3_finalize(query);
        {
            storage::Statement plan(db,"EXPLAIN QUERY PLAN UPDATE __binance_datavision_files__ SET file_id=1 WHERE file_id=2");
            expect(plan.next() && plan.text(3).find("SEARCH")!=std::string::npos,"合并来源引用更新退化为全表扫描");
            storage::Statement indexes(db,"SELECT count(*) FROM sqlite_master WHERE type='index' AND tbl_name='__binance_datavision_files__'");
            expect(indexes.next() && indexes.integer(0)==1,"来源主键被重复索引");
        }
        const auto january=db.source(month,"kline","BTCUSDT","2024-01");
        const auto feb=db.source(february,"kline","BTCUSDT","2024-02");
        const auto daily_source=db.source(day,"kline","BTCUSDT","2024-01-01");
        expect(january.id==feb.id && january.id!=daily_source.id,"日月来源字典混淆");
        expect(january.period==202401 && daily_source.period==20240101,"月日整数期间编码错误");
        const auto a=db.rest_source("BTCUSDT","2024-01-01");
        const auto b=db.rest_source("BTCUSDT","2024-01-02");
        const auto other=db.rest_source("BTCUSDT","2024-01-01");
        expect(a.id==b.id && a.id==other.id,"REST 同日来源未复用");
        auto restore=[&](const Source& source) {
            storage::Statement query(db,"WITH RECURSIVE path(id,parent_id,value) AS ("
                "SELECT p.id,p.parent_id,p.name FROM __binance_datavision_paths__ p JOIN __binance_datavision_templates__ d ON d.path_id=p.id WHERE d.id=? "
                "UNION ALL SELECT p.id,p.parent_id,p.name||'/'||path.value FROM __binance_datavision_paths__ p JOIN path ON p.id=path.parent_id) "
                "SELECT replace(replace(path.value,'{symbol}',s.name),'{period}',?),d.group_id,d.symbol_id "
                "FROM path JOIN __binance_datavision_templates__ d ON d.id=? JOIN __symbols__ s ON s.id=d.symbol_id WHERE path.parent_id=0");
            auto date=std::to_string(source.period);
            date.insert(4,"-");
            if (date.size()==9) date.insert(7,"-");
            query.integer(1,source.id); query.text(2,date); query.integer(3,source.id);
            expect(query.next(),"路径还原失败");
            return query.text(0);
        };
        expect(restore(january)==month && restore(daily_source)==day,"来源 key 恢复错误");
        db.source("data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip",
                  "funding_rate_settlement","BTCUSDT","2024-01");
        storage::Statement count_before(db,"SELECT count(*) FROM __binance_datavision_paths__");
        count_before.next();
        const auto nodes=count_before.integer(0);
        db.exec("BEGIN");
        for (int i=0;i<1000;++i) {
            const auto symbol="COIN"+std::to_string(i)+"USDT";
            const auto key="data/futures/um/monthly/klines/"+symbol+"/1m/"+symbol+"-1m-2024-01.zip";
            expect(restore(db.source(key,"kline",symbol,"2024-01"))==key,"多品种路径恢复错误");
            const auto funding_key="data/futures/um/monthly/fundingRate/"+symbol+"/"+symbol+"-fundingRate-2024-01.zip";
            expect(restore(db.source(funding_key,"funding_rate_settlement",symbol,"2024-01"))==funding_key,"跨组路径恢复错误");
        }
        db.exec("COMMIT");
        storage::Statement counts(db,"SELECT (SELECT count(*) FROM __symbols__), (SELECT count(*) FROM __group__), (SELECT count(*) FROM __binance_datavision_paths__)");
        counts.next();
        expect(counts.integer(0)==1001 && counts.integer(1)==2,"组或品种字典重复");
        expect(counts.integer(2)==nodes,"新增品种重复存储了路径模板");

    }
    auto lossy_options=options;
    lossy_options.root=root/"lossy";
    lossy_options.lossy_taker=true;
    download(lossy_options);
    const auto lossy=lossy_options.root/"kline/BTCUSDT/2024.mkp";
    expect(info(lossy)["metadata"].contains("lossy_columns"),"开启有损编码未生效");
    auto rest_options=options;
    rest_options.root=root/"rest";
    rest_options.groups={};
    rest_options.full=false;
    download(rest_options);
    {
        DB db(rest_options.root/"mkt.sqlite");
        db.attach_funding();
        create_funding(db);
        const auto begin=timestamp("2024-01-01");
        auto fetch=[&](int64_t end,int64_t start) {
            rest_options.end=end;
            Json rows=Json::array();
            for (int hour=-8;hour<48;hour+=8) {
                const auto time=begin+hour*3600000LL;
                if(time<end) rows.push_back({{"fundingTime",time},{"fundingRate","0.0001"}});
            }
            const auto url="https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&startTime="+
                std::to_string(begin-86400000)+"&endTime="+std::to_string(end-1)+"&limit=1000";
            responses[url]=rows.dump();
            funding_rest(db,rest_options,"BTCUSDT",start);
        };
        auto count=[&](const std::string& table) {
            storage::Statement query(db,"SELECT count(*) FROM "+table);
            query.next(); return query.integer(0);
        };
        fetch(begin+4*3600000,begin);
        const auto nodes=count("__binance_datavision_paths__");
        auto day_rows=[&] { return mktpack::query(rest_options.root,"funding_rate_settlement",{"BTCUSDT"},begin,begin+86400000).rows; };
        expect(day_rows()==1,"REST 首次局部下载失败");
        fetch(begin+24*3600000,begin+8*3600000);
        expect(day_rows()==3,"REST 延长终点未补齐同一天或丢失早期记录");
        fetch(begin+4*3600000,begin);
        expect(day_rows()==3,"REST 重复小范围请求覆盖了完整日数据");
        expect(count("__binance_rest_funding__")==1,"重复下载新增了同日来源");
        fetch(begin+28*3600000,begin);
        expect(count("__binance_rest_funding__")==2,"延长范围应只增加一个新日期");
        expect(count("__binance_datavision_paths__")==nodes && count("__binance_datavision_templates__")==0 && count("__binance_datavision_files__")==0,"请求范围污染了路径字典");
        db.exec("DELETE FROM funding_rate_settlement WHERE time<"+std::to_string(begin+86400000));
        rest_options.full=true;
        fetch(begin+24*3600000,begin);
        expect(day_rows()==3,"REST 全量恢复失败");
        expect(!fs::exists(rest_options.root/"funding_rate_settlement"),"资金费率仍写出了 mkp 文件");
        expect(count("__timefile__")==0,"资金费率进入了文件索引");
        const auto funding_month=std::string(prefix)+"monthly/fundingRate/BTCUSDT/";
        responses[host+"?list-type=2&prefix="+funding_month]=listing({funding_month+"BTCUSDT-fundingRate-2024-01.zip"});
        responses[host+funding_month+"BTCUSDT-fundingRate-2024-01.zip"]=zipped(
            "1704067200000,8,0.00012345\n1704096000000,8,-0.0002\n");
        rest_options.groups={"funding_rate_settlement"};
        rest_options.start=begin; rest_options.end=timestamp("2024-02-01");
        download(rest_options);
        auto funding=mktpack::query(rest_options.root,"funding_rate_settlement",{"BTCUSDT"},begin,rest_options.end,std::vector<std::string>{"funding_rate"});
        __int128 coefficients[2];
        expect(funding.rows==2 && funding.columns.at(0).buffers.at(1).size()==sizeof(coefficients),"资金费率条数错误");
        std::memcpy(coefficients,funding.columns[0].buffers[1].data(),sizeof(coefficients));
        expect(coefficients[0]==12345 && coefficients[1]==-20000,"月归档未精确替换 REST 数据");
        auto before=requests;
        download(rest_options);
        expect(requests-before==1,"资金费率增量重复下载");
        db.exec("DELETE FROM funding_rate_settlement WHERE time=1704067200000");
        download(rest_options);
        expect(count("funding_rate_settlement")==2,"资金费率全量检查未补齐缺行");

    }
    {
        Options scan; scan.root=root/"scans"; scan.groups={"oi"}; scan.symbols={"XUSDT"}; scan.jobs=1;
        scan.start=timestamp("2024-01-01"); scan.end=timestamp("2026-03-01");
        const auto directory=source_directory("oi","XUSDT",false);
        const auto key=[&](const std::string& date) {return directory+"XUSDT-metrics-"+date+".zip";};
        auto sample=[](const std::string& date) {return zipped(date+" 00:00:00,XUSDT,1.25,2.5\n");};
        responses[host+"?list-type=2&prefix="+directory]=listing({key("2024-01-01"),key("2025-01-01"),key("2026-02-01")});
        responses[host+key("2025-01-01")]=sample("2025-01-01");
        responses[host+key("2026-02-01")]=sample("2026-02-01");
        std::vector<Progress> events;
        scan.progress=[&](const auto& event) {events.push_back(event);};
        const auto initial=requested_urls.size();
        bool failed=false;
        try {download(scan);} catch(const std::exception&) {failed=true;}
        expect(failed,"旧日期失败没有报告");
        expect(std::find_if(events.begin(),events.end(),[](const auto& e) {return e.stage=="错误" && e.detail.find("2024-01-01")!=std::string::npos;})!=events.end(),"来源错误未及时报告");
        expect(requested_urls[initial].ends_with("XUSDT-metrics-2026-02"),"发现未从最近月份开始");
        const auto first_payload=std::find_if(requested_urls.begin()+initial,requested_urls.end(),[](const auto& url) {return url.ends_with(".zip");});
        expect(first_payload!=requested_urls.end() && first_payload->ends_with("2026-02-01.zip"),"下载未从最近日期开始");
        responses[host+key("2024-01-01")]=sample("2024-01-01");
        const auto retry=requested_urls.size();
        download(scan);
        expect(requested_urls.size()-retry==2,"增量应只刷新末月目录并补齐已登记的旧失败项");
        expect(requested_urls.back()==host+key("2024-01-01"),"旧失败项被扫描范围跳过");
        const auto unchanged=requests;
        download(scan);
        expect(requests-unchanged==1,"增量重复扫描了历史月份");
        for(const auto& stage:{"发现","下载","写入","合并","完成"})
            expect(std::find_if(events.begin(),events.end(),[&](const auto& e) {return e.slot==1 && e.stage==stage;})!=events.end(),"工作槽缺少阶段事件");
        fs::remove(scan.root/"oi/XUSDT/2024.mkp");
        scan.full=true;
        const auto full=requested_urls.size();
        download(scan);
        expect(fs::exists(scan.root/"oi/XUSDT/2024.mkp"),"全量检查未恢复历史载荷");
        expect(std::find(requested_urls.begin()+full,requested_urls.end(),host+"?list-type=2&prefix="+directory+"XUSDT-metrics-2024-01")!=requested_urls.end(),"全量检查未扫描历史目录");
        scan.full=false; scan.start=timestamp("2023-01-01");
        responses[host+"?list-type=2&prefix="+directory]=listing({key("2023-01-01"),key("2024-01-01"),key("2025-01-01"),key("2026-02-01")});
        responses[host+key("2023-01-01")]=sample("2023-01-01");
        download(scan);
        expect(fs::exists(scan.root/"oi/XUSDT/2023.mkp"),"扩大历史范围未发现新增区间");

        // 中途发现失败，重跑必须继续尚未扫描的旧月份
        auto interrupted=scan; interrupted.root=root/"interrupted";
        interrupted.start=timestamp("2024-01-01"); interrupted.end=timestamp("2024-03-01");
        const auto broken=host+"?list-type=2&prefix="+directory+"XUSDT-metrics-2024-01";
        responses[broken]="broken xml";
        failed=false;
        try {download(interrupted);} catch(const std::exception&) {failed=true;}
        expect(failed,"发现错误未报告");
        responses.erase(broken);
        download(interrupted);
        expect(fs::exists(interrupted.root/"oi/XUSDT/2024.mkp"),"中断后遗漏未扫描月份");

        // 空归档作为成功来源记录，不重复下载
        const auto empty=key("2026-02-02");
        responses[host+"?list-type=2&prefix="+directory]=listing({empty});
        responses[host+empty]=zipped("create_time,symbol,sum_open_interest,sum_open_interest_value\n");
        download(scan);
        const auto empty_before=requests;
        download(scan);
        expect(requests-empty_before==1,"空归档被反复下载");
    }
    {
        Options all; all.root=root/"all-symbols"; all.groups={"oi"}; all.jobs=1;
        all.start=timestamp("2024-01-01"); all.end=timestamp("2024-02-01");
        const auto add_symbol=[&](const std::string& symbol) {
            const auto directory=source_directory("oi",symbol,false);
            const auto key=directory+source_filename("oi",symbol)+"2024-01-01.zip";
            responses[host+"?list-type=2&prefix="+directory]=listing({key});
            responses[host+key]=zipped("2024-01-01 00:00:00,"+symbol+",1,2\n");
        };
        add_symbol("NEWUSDT");
        download(all);
        expect(fs::exists(all.root/"oi/NEWUSDT/2024.mkp"),"全品种发现遗漏品种");
        add_symbol("LATERUSDT");
        download(all);
        expect(fs::exists(all.root/"oi/LATERUSDT/2024.mkp"),"增量未发现新合约");

        const auto prefix=std::string("test/pagination/");
        responses[host+"?list-type=2&prefix="+prefix+"&delimiter=/"]="<ListBucketResult><CommonPrefixes><Prefix>test/pagination/AUSDT/</Prefix></CommonPrefixes><NextContinuationToken>a+b=</NextContinuationToken></ListBucketResult>";
        responses[host+"?list-type=2&prefix="+prefix+"&delimiter=/&continuation-token=a%2Bb%3D"]="<ListBucketResult><CommonPrefixes><Prefix>test/pagination/BUSDT/</Prefix></CommonPrefixes></ListBucketResult>";
        expect(list(prefix,true)==std::vector<std::string>({prefix+"AUSDT/",prefix+"BUSDT/"}),"品种目录分页错误");
        expect(url_encode("币/USDT",true)=="%E5%B8%81/USDT","非ASCII品种路径未转义");
    }
    fs::remove_all(root);
}

#include "../cpp/port/binance.cpp"
#include <mktpack/storage.hpp>

namespace {
std::map<std::string,std::string> responses;
int requests=0;
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
    if(!responses.contains(url)) throw std::runtime_error("模拟下载失败");
    return responses.at(url);
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
    responses[host+"?list-type=2&prefix="+monthly]=listing({month});
    responses[host+month]=zipped("1704067200000,1.1,3.1,1.1,2.1,10.1,0,0,0,4.1,0,0\n1704153600000,2.1,4.1,2.1,3.1,12.1,0,0,0,5.1,0,0\n");
    download(options);
    expect(read(annual).columns.at("close")==std::vector<int64_t>({21,31}),"月包替换年内日数据失败");
    auto before=requests;
    download(options);
    expect(requests-before==2,"增量不应下载已有载荷");
    fs::remove(annual);
    options.full=true;
    download(options);
    expect(fs::exists(annual)&&read(annual).columns.at("time").size()==2,"全量恢复失败");
    responses.erase(host+month);
    fs::remove(annual);
    bool failed=false;
    try { download(options); } catch(const std::exception&) { failed=true; }
    expect(failed,"模拟失败未返回错误");
    { DB db(root/"mkt.sqlite"); expect(!db.completed(month,false),"失败状态未记录"); }
    responses[host+month]=zipped("1704067200000,1,3,1,2,10,0,0,0,4,0,0\n");
    download(options);
    { DB db(root/"mkt.sqlite"); expect(db.completed(month,false),"失败重试未成功"); }
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
    { DB db(root/"mkt.sqlite"); expect(!db.completed(february,false),"缺失年文件其他来源未失效"); }
    options.full=false;
    options.start=timestamp("2024-02-01");
    options.end=timestamp("2024-03-01");
    download(options);
    expect(read(annual).columns.at("time").size()==2,"局部恢复后增量补另一月失败");
    expect(info(annual)["metadata"].contains("lossy_columns"),"默认下载未启用有损编码");
    auto lossless_options=options;
    lossless_options.root=root/"exact";
    lossless_options.lossy_taker=false;
    download(lossless_options);
    const auto exact=lossless_options.root/"kline/BTCUSDT/2024.mkp";
    expect(!info(exact)["metadata"].contains("lossy_columns"),"关闭有损编码未生效");
    expect(read(exact).columns.at("taker_buy_volume")==std::vector<int64_t>{5},"无损下载数值改变");
    fs::remove_all(root);
}

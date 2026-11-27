#include <mktpack/binance.hpp>
#include <mktpack/ctp_futures.hpp>
#include <mktpack/storage.hpp>
#include <CLI/CLI.hpp>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include "progress.hpp"

namespace {
std::filesystem::path utf8_path(const std::string& text) {
    return std::filesystem::path(std::u8string(reinterpret_cast<const char8_t*>(text.data()),text.size()));
}

std::string format_utc_ms(int64_t ms) {
    using namespace std::chrono;
    const auto tp=sys_time<milliseconds>{milliseconds{ms}};
    const auto dp=floor<days>(tp);
    const year_month_day ymd{dp};
    const hh_mm_ss hms{tp-dp};
    char buf[32];
    std::snprintf(buf,sizeof(buf),"%04d-%02u-%02uT%02lld:%02lld:%02lld.%03lldZ",
                  int(ymd.year()),unsigned(ymd.month()),unsigned(ymd.day()),
                  static_cast<long long>(hms.hours().count()),
                  static_cast<long long>(hms.minutes().count()),
                  static_cast<long long>(hms.seconds().count()),
                  static_cast<long long>(hms.subseconds().count()));
    return buf;
}

uint64_t file_size(const std::string& text) {
    if (text.empty() || text.front()<'0' || text.front()>'9') throw std::runtime_error("分卷大小格式错误");
    size_t used=0;
    const auto value=std::stoull(text,&used);
    const auto unit=text.substr(used);
    const std::map<std::string,uint64_t> factors={{"",1},{"B",1},{"KiB",1024},{"MiB",1024*1024},
        {"GiB",1024ULL*1024*1024},{"KB",1000},{"MB",1000000},{"GB",1000000000}};
    if (!factors.contains(unit) || !value || value>UINT64_MAX/factors.at(unit))
        throw std::runtime_error("分卷大小应为正整数，可带 B、KiB、MiB、GiB、KB、MB、GB");
    return value*factors.at(unit);
}
}

int main(int argc, char** argv) {
    CLI::App app{"MktPack"};
    std::string root=".", file, start="2024-01-01", end, cache;
    mktpack::binance::Options options;
    app.add_option("--root", root, "数据集目录或 mkt.sqlite 索引路径（默认当前目录）");
    auto info=app.add_subcommand("info","查看单个 .mkp 文件头元数据");
    info->add_option("file", file, "单个 .mkp 文件路径")->required();
    auto describe_cmd=app.add_subcommand("describe","查看数据集或指定数据组的概览信息");
    std::string describe_group, describe_symbol;
    describe_cmd->add_option("group", describe_group, "数据组名称（省略时列出所有数据组）");
    describe_cmd->add_option("--symbol", describe_symbol, "按单个合约查询行数与时间范围");
    auto symbols_cmd=app.add_subcommand("symbols","列出数据组在可选时间区间内有记录的合约");
    std::string symbols_group, symbols_start, symbols_end;
    symbols_cmd->add_option("group", symbols_group, "数据组名称")->required();
    symbols_cmd->add_option("--start", symbols_start, "起始时间（含，UTC）");
    symbols_cmd->add_option("--end", symbols_end, "结束时间（不含，UTC）");
    auto binance=app.add_subcommand("binance","币安 USDT UM 永续数据下载与检查");
    auto download=binance->add_subcommand("download","下载币安历史与增量行情数据");
    download->add_option("--symbols", options.symbols,"品种列表，省略时自动发现全部 USDT 品种")->expected(-1);
    download->add_option("--groups", options.groups,"数据组列表")->expected(-1);
    download->add_option("--start", start,"起始时间（含，UTC）");
    download->add_option("--end", end,"结束时间（不含，UTC）");
    download->add_option("--jobs", options.jobs,"下载并发数（默认 4）")->check(CLI::PositiveNumber);
    download->add_option("--source-cache", cache,"原始 ZIP 缓存目录");
    download->add_flag("--full", options.full,"全量核对远端清单并补齐本地缺失数据");
    download->add_flag("--lossy-taker,!--no-lossy-taker", options.lossy_taker,
                       "主动买量按0.1个百分点量化（默认关闭）");
    auto check=binance->add_subcommand("check","检查数据集索引与载荷完整性");
    mktpack::ctp_futures::Options ctp_options;
    auto ctp=app.add_subcommand("ctp-futures","国内期货时序数据");
    auto check_ctp=ctp->add_subcommand("check","检查数据集索引与载荷");
    auto import_ticks=ctp->add_subcommand("import","导入镜像中的完整期货 tick ZIP");
    import_ticks->add_option("mirror",ctp_options.mirror,"包含 nfs_rq*/tick/future 的镜像目录")->required();
    import_ticks->add_option("--start",ctp_options.start,"起始交易日（含）");
    import_ticks->add_option("--end",ctp_options.end,"结束交易日（不含）");
    import_ticks->add_option("--symbols",ctp_options.symbols,"标准合约代码")->expected(-1);
    import_ticks->add_option("--jobs",ctp_options.jobs,"解压与编码线程数（默认 16）")->check(CLI::PositiveNumber);
    import_ticks->add_option("--readers",ctp_options.readers,"ZIP 读取线程数（默认 1）")->check(CLI::PositiveNumber);
    import_ticks->add_option("--profile",ctp_options.profile,"写入分阶段性能记录 JSON");
    auto packing=app.add_subcommand("package","打包为 SQLite 载荷分卷");
    std::string destination, maximum="128MiB", package_start, package_end;
    mktpack::PackageOptions package_options;
    packing->add_option("destination",destination)->required();
    packing->add_option("--max-file-size",maximum,"每个分卷的实际文件大小上限");
    packing->add_option("--jobs",package_options.jobs,"并行裁剪重编码的任务数（默认 4）")->check(CLI::PositiveNumber);
    packing->add_option("--groups",package_options.groups)->expected(-1);
    packing->add_option("--symbols",package_options.symbols)->expected(-1);
    packing->add_option("--start",package_start);
    packing->add_option("--end",package_end);
    try {
        app.parse(argc,app.ensure_utf8(argv));
        if (*info) {
            const auto target=utf8_path(file);
            if (std::filesystem::is_directory(target))
                throw std::runtime_error("mkt info 仅用于单个 .mkp 文件；查看数据集请使用 mkt --root <目录> describe");
            std::cout << mktpack::info(target).dump(2) << "\n";
        } else if (*describe_cmd) {
            if (!describe_symbol.empty() && describe_group.empty())
                throw std::runtime_error("指定 --symbol 时必须同时提供数据组名称");
            auto result=mktpack::describe(utf8_path(root),describe_group,describe_symbol);
            if (result.is_object()) {
                for (const auto* key:{"first_time","last_time"})
                    if (result.contains(key) && result[key].is_number_integer())
                        result[key]=format_utc_ms(result[key].get<int64_t>());
            }
            std::cout << result.dump(2) << "\n";
        } else if (*symbols_cmd) {
            const auto first=symbols_start.empty()?std::numeric_limits<int64_t>::min():mktpack::binance::timestamp(symbols_start);
            const auto last=symbols_end.empty()?std::numeric_limits<int64_t>::max():mktpack::binance::timestamp(symbols_end);
            for (const auto& name:mktpack::symbols(utf8_path(root),symbols_group,first,last))
                std::cout << name << "\n";
        } else if (*download) {
            options.root=root;
            options.cache=cache;
            options.start=mktpack::binance::timestamp(start);
            options.end=end.empty()
                ? std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::floor<std::chrono::days>(std::chrono::system_clock::now()).time_since_epoch()).count()
                : mktpack::binance::timestamp(end);
            if (options.start>=options.end) throw std::runtime_error("需要 start < end");
            mktpack::cli::DownloadProgress progress(options.jobs);
            options.progress=[&](const auto& event) { progress.update(event); };
            mktpack::binance::download(options);
        } else if (*import_ticks) {
            ctp_options.root=root;
            std::cout<<mktpack::ctp_futures::import_ticks(ctp_options).dump(2)<<"\n";
        } else if (*packing) {
            package_options.max_file_size=file_size(maximum);
            if (!package_start.empty()) package_options.start=mktpack::binance::timestamp(package_start);
            if (!package_end.empty()) package_options.end=mktpack::binance::timestamp(package_end);
            package_options.progress=[](const auto& message) { std::cerr<<message<<"\n"; };
            const auto result=mktpack::package(root,destination,package_options);
            for (const auto& warning:result.at("warnings")) std::cerr<<"警告："<<warning.get<std::string>()<<"\n";
            std::cout<<result.dump(2)<<"\n";
        } else if (*check || *check_ctp) {
            auto result=mktpack::check_dataset(root);
            std::cout << result.dump(2) << "\n";
            return result["errors"].empty()?0:1;
        } else {
            std::cout << app.help();
        }
        return 0;
    } catch (const CLI::ParseError& e) {
        return app.exit(e);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return 1;
    }
}

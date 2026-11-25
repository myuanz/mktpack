#include <mktpack/binance.hpp>
#include <mktpack/ctp_futures.hpp>
#include <mktpack/storage.hpp>
#include <CLI/CLI.hpp>
#include <chrono>
#include <iostream>
#include "progress.hpp"

namespace {
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
    app.add_option("--root", root);
    auto info=app.add_subcommand("info");
    info->add_option("file", file)->required();
    auto binance=app.add_subcommand("binance");
    auto download=binance->add_subcommand("download");
    download->add_option("--symbols", options.symbols,"品种列表，省略时自动发现全部 USDT 品种")->expected(-1);
    download->add_option("--groups", options.groups)->expected(-1);
    download->add_option("--start", start);
    download->add_option("--end", end);
    download->add_option("--jobs", options.jobs)->check(CLI::PositiveNumber);
    download->add_option("--source-cache", cache);
    download->add_flag("--full", options.full);
    download->add_flag("--lossy-taker,!--no-lossy-taker", options.lossy_taker,
                       "主动买量按0.1个百分点量化（默认启用）");
    auto check=binance->add_subcommand("check");
    mktpack::ctp_futures::Options ctp_options;
    auto ctp=app.add_subcommand("ctp-futures","国内期货时序数据");
    auto check_ctp=ctp->add_subcommand("check","检查数据集索引与载荷");
    auto import_ticks=ctp->add_subcommand("import","导入镜像中的完整期货 tick ZIP");
    import_ticks->add_option("mirror",ctp_options.mirror,"包含 nfs_rq*/tick/future 的镜像目录")->required();
    import_ticks->add_option("--start",ctp_options.start,"起始交易日（含）");
    import_ticks->add_option("--end",ctp_options.end,"结束交易日（不含）");
    import_ticks->add_option("--symbols",ctp_options.symbols,"标准合约代码")->expected(-1);
    auto packing=app.add_subcommand("package","打包为 SQLite 载荷分卷");
    std::string destination, maximum="128MiB", package_start, package_end;
    mktpack::PackageOptions package_options;
    packing->add_option("destination",destination)->required();
    packing->add_option("--max-file-size",maximum,"每个分卷的实际文件大小上限");
    packing->add_option("--jobs",package_options.jobs,"并行处理年度分区的任务数（默认 4）")->check(CLI::PositiveNumber);
    packing->add_option("--groups",package_options.groups)->expected(-1);
    packing->add_option("--symbols",package_options.symbols)->expected(-1);
    packing->add_option("--start",package_start);
    packing->add_option("--end",package_end);
    try {
        app.parse(argc, argv);
        if (*info) {
            std::cout << mktpack::info(file).dump(2) << "\n";
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

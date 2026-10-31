#include <mktpack/binance.hpp>
#include <CLI/CLI.hpp>
#include <chrono>
#include <iostream>

int main(int argc, char** argv) {
    CLI::App app{"MTKPack"};
    std::string root=".", file, start="2024-01-01", end, cache;
    mktpack::binance::Options options;
    app.add_option("--root", root);
    auto info=app.add_subcommand("info");
    info->add_option("file", file)->required();
    auto binance=app.add_subcommand("binance");
    auto download=binance->add_subcommand("download");
    download->add_option("--symbols", options.symbols)->required()->expected(-1);
    download->add_option("--groups", options.groups)->expected(-1);
    download->add_option("--start", start);
    download->add_option("--end", end);
    download->add_option("--jobs", options.jobs)->check(CLI::PositiveNumber);
    download->add_option("--source-cache", cache);
    download->add_flag("--full", options.full);
    download->add_flag("--lossy-taker,!--no-lossy-taker", options.lossy_taker,
                       "主动买量按0.1个百分点量化（默认启用）");
    auto check=binance->add_subcommand("check");
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
            if (options.start>options.end) throw std::runtime_error("start 晚于 end");
            mktpack::binance::download(options);
        } else if (*check) {
            auto result=mktpack::binance::check(root);
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

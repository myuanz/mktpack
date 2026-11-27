#pragma once
#include <mktpack/core.hpp>
#include <functional>
namespace mktpack::binance {
std::string http_get(const std::string& url);
int64_t timestamp(std::string text);
struct Progress {
    size_t slot=0;
    std::string group,symbol,stage,detail;
    size_t completed=0,total=0;
};
struct Options {
    std::filesystem::path root,cache;
    std::vector<std::string> symbols,groups{"kline","oi","funding_rate_settlement","exchangeinfo"};
    int64_t start,end;
    bool full=false,lossy_taker=false;
    size_t jobs=4;
    std::function<void(const Progress&)> progress;
};
void download(const Options& options);
}

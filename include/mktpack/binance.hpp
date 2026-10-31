#pragma once
#include <mktpack/core.hpp>
namespace mktpack::binance {
std::string http_get(const std::string& url);
int64_t timestamp(std::string text);
struct Options {std::filesystem::path root,cache; std::vector<std::string> symbols,groups{"kline","oi","funding_rate_settlement","exchangeinfo"}; int64_t start,end; bool full=false; bool lossy_taker=true; size_t jobs=4;};
void download(const Options& options);
Json check(const std::filesystem::path& root);
}

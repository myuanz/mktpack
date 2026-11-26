#pragma once
#include <mktpack/core.hpp>

namespace mktpack::ctp_futures {
struct Options {
    std::filesystem::path root, mirror;
    std::filesystem::path profile;
    std::vector<std::string> symbols;
    std::string start, end;
    size_t jobs=16;
    size_t readers=1;
};

Json import_ticks(const Options& options);
}

#pragma once
#include <mktpack/core.hpp>

namespace mktpack::ctp_futures {
struct Options {
    std::filesystem::path root, mirror;
    std::filesystem::path profile;
    std::vector<std::string> symbols;
    std::string start, end;
    size_t jobs=4;
};

Json import_ticks(const Options& options);
}

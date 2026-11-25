#pragma once
#include <mktpack/core.hpp>

namespace mktpack::ctp_futures {
struct Options {
    std::filesystem::path root, mirror;
    std::vector<std::string> symbols;
    std::string start, end;
};

Json import_ticks(const Options& options);
}

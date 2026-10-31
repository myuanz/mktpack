#pragma once
#include <mktpack/core.hpp>
#include <functional>
#include <limits>
namespace mktpack {
Json describe(const std::filesystem::path& root,const std::string& group="");
Json ordinary(const std::filesystem::path& root,const std::string& group);
std::vector<Table> query(const std::filesystem::path& root,const std::string& group,const std::vector<std::string>& symbols,int64_t start,int64_t end);
Json check_dataset(const std::filesystem::path& root);
struct PackageOptions {
    std::vector<std::string> groups, symbols;
    int64_t start=std::numeric_limits<int64_t>::min(), end=std::numeric_limits<int64_t>::max();
    uint64_t max_file_size=128ULL*1024*1024;
    std::function<void(const std::string&)> progress;
};
Json package(const std::filesystem::path& source,const std::filesystem::path& destination,const PackageOptions& options={});
}

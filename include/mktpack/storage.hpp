#pragma once
#include <mktpack/core.hpp>
#include <functional>
#include <limits>
#include <optional>
namespace mktpack {
Json describe(const std::filesystem::path& root,const std::string& group="");
std::vector<std::string> groups(const std::filesystem::path& root);
std::optional<std::string> group_kind(const std::filesystem::path& root,const std::string& group);
std::vector<std::string> symbols(const std::filesystem::path& root,const std::string& group,
                                 int64_t start=std::numeric_limits<int64_t>::min(),
                                 int64_t end=std::numeric_limits<int64_t>::max());
Json ordinary(const std::filesystem::path& root,const std::string& group);
// Arrow 数组的列类型、有效性位图、数据缓冲区及嵌套子列。
struct ResultColumn {
    std::string type;
    int scale = 0;
    size_t rows = 0;
    std::vector<std::vector<uint8_t>> buffers;
    std::vector<ResultColumn> children;
};
struct QueryResult {
    size_t rows = 0;
    std::vector<std::string> names;
    std::vector<ResultColumn> columns;
};
using Projection = std::optional<std::vector<std::string>>;
QueryResult query(const std::filesystem::path& root, const std::string& group,
                  const std::vector<std::string>& symbols, int64_t start, int64_t end,
                  const Projection& requested = {});
QueryResult file_result(const std::filesystem::path& path);
Json check_dataset(const std::filesystem::path& root);
struct PackageOptions {
    std::vector<std::string> groups, symbols;
    int64_t start=std::numeric_limits<int64_t>::min(), end=std::numeric_limits<int64_t>::max();
    uint64_t max_file_size=128ULL*1024*1024;
    size_t jobs=4;
    std::function<void(const std::string&)> progress;
};
Json package(const std::filesystem::path& source,const std::filesystem::path& destination,const PackageOptions& options={});
}

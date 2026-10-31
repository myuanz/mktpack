#pragma once
#include <mktpack/core.hpp>
namespace mktpack {
Json describe(const std::filesystem::path& root,const std::string& group="");
Json ordinary(const std::filesystem::path& root,const std::string& group);
std::vector<Table> query(const std::filesystem::path& root,const std::string& group,const std::vector<std::string>& symbols,int64_t start,int64_t end);
}

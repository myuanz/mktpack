#pragma once
#include <nlohmann/json.hpp>
#include <cstdint>
#include <filesystem>
#include <map>
#include <vector>
namespace mktpack {
using Json=nlohmann::json;
using Columns=std::map<std::string,std::vector<int64_t>>;
struct Table { Columns columns; Json schema=Json::object(); Json metadata=Json::object(); };
std::vector<uint8_t> encode(const Table& table);
Table decode(const std::vector<uint8_t>& bytes);
Json info(const std::filesystem::path& path);
Table read(const std::filesystem::path& path);
void write(const std::filesystem::path& path,const Table& table);
Table merge(const std::vector<Table>& tables);
int64_t rescale(int64_t value,int from,int to);
}

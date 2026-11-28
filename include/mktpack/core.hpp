#pragma once
#include <nlohmann/json.hpp>
#include <cstdint>
#include <filesystem>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <vector>
namespace mktpack {
inline std::string utf8_string(const std::filesystem::path& path) {
    const auto u8=path.u8string();
    return std::string(reinterpret_cast<const char*>(u8.data()),u8.size());
}
inline std::filesystem::path utf8_path(std::string_view text) {
    return std::filesystem::path(std::u8string(reinterpret_cast<const char8_t*>(text.data()),text.size()));
}
using Json=nlohmann::json;
using Columns=std::map<std::string,std::vector<int64_t>>;
struct Table { Columns columns; Json schema=Json::object(); Json metadata=Json::object(); };
std::vector<uint8_t> encode(const Table& table);
std::vector<uint8_t> encode(Table&& table);
Table decode(std::span<const uint8_t> bytes);
Table decode(std::span<const uint8_t> bytes,const std::vector<std::string>& requested);
Table decode(const std::vector<uint8_t>& bytes);
Table decode(const std::vector<uint8_t>& bytes,const std::vector<std::string>& requested);
Json info(const std::filesystem::path& path);
Json info(const std::vector<uint8_t>& bytes);
Table read(const std::filesystem::path& path);
void write(const std::filesystem::path& path,const Table& table);
void write(const std::filesystem::path& path,const std::vector<uint8_t>& bytes);
Table merge(const std::vector<Table>& tables);
void merge_files(const std::vector<std::filesystem::path>& paths,const std::filesystem::path& destination);
int64_t rescale(__int128 value,int from,int to);
void set_taker_encoding(Json& schema,bool enabled);
}

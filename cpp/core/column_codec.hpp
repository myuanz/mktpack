#pragma once
#include <mktpack/core.hpp>

namespace mktpack::detail {
using Bytes=std::vector<uint8_t>;
using U=std::vector<uint64_t>;

void check(bool condition,const char* message);
int64_t checked(__int128 value);
uint64_t zz(int64_t value);
int64_t unzz(uint64_t value);
std::pair<Bytes,Json> pack(const U& values);
U unpack(const Bytes& bytes,size_t offset,const Json& descriptor,size_t rows);
void encode_block(const Table& table,Json& header,std::vector<Bytes>& payload);
Table decode_block(const Bytes& bytes,const Json& header,size_t offset,const std::vector<std::string>* requested);
}

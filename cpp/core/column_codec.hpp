#pragma once
#include <mktpack/core.hpp>
#include <span>

namespace mktpack::detail {
using Bytes=std::vector<uint8_t>;
using ByteSpan=std::span<const uint8_t>;
using U=std::vector<uint64_t>;

void check(bool condition,const char* message);
int64_t checked(__int128 value);
uint64_t zz(int64_t value);
int64_t unzz(uint64_t value);
std::pair<Bytes,Json> pack(const U& values);
const uint64_t* unpack_stream(ByteSpan bytes,size_t offset,const Json& descriptor,size_t rows);
void encode_block(Columns columns,Json schema,Json& header,std::vector<Bytes>& payload);
Table decode_block(ByteSpan bytes,const Json& header,size_t offset,const std::vector<std::string>* requested);
}

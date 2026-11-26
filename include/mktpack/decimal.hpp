#pragma once
#include <cstdint>
#include <string_view>

namespace mktpack {
// 十进制整数及去除尾零后的小数位数，供输入适配器使用。
struct DecimalInteger { int64_t value; int decimals; };
DecimalInteger decimal_integer(std::string_view text, int* literal_scale=nullptr);
}

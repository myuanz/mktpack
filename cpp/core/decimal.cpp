#include <mktpack/decimal.hpp>
#include <charconv>
#include <limits>
#include <stdexcept>

namespace mktpack {
DecimalInteger decimal_integer(std::string s, int* literal_scale) {
    bool negative=!s.empty() && s[0]=='-';
    if(!s.empty() && (s[0]=='-' || s[0]=='+')) s.erase(0,1);
    auto e=s.find_first_of("eE"); int exponent=0;
    if(e!=std::string::npos) {
        auto text=s.substr(e+1);
        if(!text.empty() && text[0]=='+') text.erase(0,1);
        auto parsed=std::from_chars(text.data(),text.data()+text.size(),exponent);
        if(parsed.ec!=std::errc{} || parsed.ptr!=text.data()+text.size()) throw std::runtime_error("十进制指数超出范围或格式错误");
        s.resize(e);
    }
    auto dot=s.find('.'); int64_t decimals=dot==std::string::npos?0:static_cast<int64_t>(s.size()-dot-1);
    if(dot!=std::string::npos) s.erase(dot,1);
    if(s.empty() || s.find_first_not_of("0123456789")!=std::string::npos) throw std::runtime_error("数值必须是有限十进制数");
    decimals-=exponent;
    if(literal_scale) {
        if(decimals>std::numeric_limits<int>::max()) throw std::runtime_error("小数位数超出范围");
        *literal_scale=decimals>0?static_cast<int>(decimals):0;
    }
    auto nonzero=s.find_first_not_of('0');
    if(nonzero==std::string::npos) return {0,0};
    s.erase(0,nonzero);
    while(s.back()=='0') { s.pop_back(); --decimals; }
    if(decimals>18 || decimals < -18) throw std::runtime_error("十进制数超出精度或 int64 范围");
    uint64_t unsigned_value=0; auto parsed=std::from_chars(s.data(),s.data()+s.size(),unsigned_value);
    if(parsed.ec!=std::errc{} || parsed.ptr!=s.data()+s.size()) throw std::runtime_error("十进制整数超出范围");
    __int128 value=unsigned_value;
    if(negative) value=-value;
    while(decimals<0) { value*=10; ++decimals; }
    if(value<INT64_MIN || value>INT64_MAX) throw std::runtime_error("十进制整数超出 int64");
    return {static_cast<int64_t>(value),static_cast<int>(decimals)};
}
}

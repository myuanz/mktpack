#include <mktpack/decimal.hpp>
#include <charconv>
#include <limits>
#include <stdexcept>

namespace mktpack {
DecimalInteger decimal_integer(std::string_view text, int* literal_scale) {
    const bool negative=!text.empty() && text.front()=='-';
    if(!text.empty() && (text.front()=='-' || text.front()=='+')) text.remove_prefix(1);
    const auto e=text.find_first_of("eE");
    auto mantissa=text.substr(0,e);
    int exponent=0;
    if(e!=text.npos) {
        auto suffix=text.substr(e+1);
        if(!suffix.empty() && suffix.front()=='+')suffix.remove_prefix(1);
        const auto parsed=std::from_chars(suffix.data(),suffix.data()+suffix.size(),exponent);
        if(parsed.ec!=std::errc{} || parsed.ptr!=suffix.data()+suffix.size()) throw std::runtime_error("十进制指数超出范围或格式错误");
    }
    const auto dot=mantissa.find('.');
    int64_t decimals=dot==mantissa.npos?0:static_cast<int64_t>(mantissa.size()-dot-1);
    bool digit=false;
    size_t first=mantissa.npos,last=mantissa.npos;
    for(size_t i=0;i<mantissa.size();++i) {
        const char c=mantissa[i];
        if(c=='.' && i==dot)continue;
        if(c<'0' || c>'9')throw std::runtime_error("数值必须是有限十进制数");
        digit=true;
        if(c!='0') {if(first==mantissa.npos)first=i;last=i;}
    }
    if(!digit)throw std::runtime_error("数值必须是有限十进制数");
    decimals-=exponent;
    if(literal_scale) {
        if(decimals>std::numeric_limits<int>::max()) throw std::runtime_error("小数位数超出范围");
        *literal_scale=decimals>0?static_cast<int>(decimals):0;
    }
    if(first==mantissa.npos) return {0,0};
    for(size_t i=last+1;i<mantissa.size();++i)if(mantissa[i]!='.')--decimals;
    if(decimals>18 || decimals < -18) throw std::runtime_error("十进制数超出精度或 int64 范围");
    uint64_t unsigned_value=0;
    for(size_t i=first;i<=last;++i)if(mantissa[i]!='.') {
        const auto digit=uint64_t(mantissa[i]-'0');
        if(unsigned_value>(UINT64_MAX-digit)/10)throw std::runtime_error("十进制整数超出范围");
        unsigned_value=unsigned_value*10+digit;
    }
    __int128 value=unsigned_value;
    if(negative) value=-value;
    while(decimals<0) { value*=10; ++decimals; }
    if(value<INT64_MIN || value>INT64_MAX) throw std::runtime_error("十进制整数超出 int64");
    return {static_cast<int64_t>(value),static_cast<int>(decimals)};
}
}

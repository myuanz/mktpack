#include "column_codec.hpp"
#include <zstd.h>
#include <algorithm>
#include <bit>
#include <cstring>
#include <fstream>
#include <memory>
#include <numeric>
#include <set>
#include <utility>
namespace mktpack {
namespace detail {
void check(bool b,const char* m) { if(!b) throw std::runtime_error(m); }
int64_t checked(__int128 v) { check(v>=INT64_MIN&&v<=INT64_MAX,"整数溢出"); return static_cast<int64_t>(v); }
uint64_t zz(int64_t x) { return (uint64_t(x)<<1)^(x<0?UINT64_MAX:0); }
int64_t unzz(uint64_t x) { return std::bit_cast<int64_t>((x>>1)^(uint64_t(0)-(x&1))); }
Bytes compress(const Bytes& input) {
    struct FreeContext {void operator()(ZSTD_CCtx* context) const {ZSTD_freeCCtx(context);}};
    thread_local std::unique_ptr<ZSTD_CCtx,FreeContext> context(ZSTD_createCCtx());
    check(bool(context),"无法创建压缩上下文");
    Bytes output(ZSTD_compressBound(input.size()));
    const auto size=ZSTD_compressCCtx(context.get(),output.data(),output.size(),input.data(),input.size(),9);
    check(!ZSTD_isError(size),"压缩失败");
    output.resize(size);
    return output;
}

// 8 行 × 8 位矩阵逐级交换宽度为 1、2、4 的子块，转置自身也是逆变换。
uint64_t transpose8(uint64_t x) {
    auto swap=[&](unsigned shift,uint64_t mask) {
        const uint64_t t=(x^(x>>shift))&mask;
        x^=t^(t<<shift);
    };
    swap(7,0x00aa00aa00aa00aaULL);
    swap(14,0x0000cccc0000ccccULL);
    swap(28,0x00000000f0f0f0f0ULL);
    return x;
}

uint64_t load_le(const uint8_t* data,size_t count) {
    uint64_t value=0;
    if constexpr (std::endian::native==std::endian::little) std::memcpy(&value,data,count);
    else for (size_t i=0;i<count;++i) value|=uint64_t(data[i])<<(8*i);
    return value;
}

std::pair<Bytes,Json> pack(const U& values) {
    uint64_t maximum=0;
    for (auto value:values) maximum|=value;
    const int bits=std::max(1,int(std::bit_width(maximum)));
    const size_t plane=(values.size()+7)/8;
    Bytes bit(plane*bits),byte(values.size()*8);
    for (int j=0;j<(bits+7)/8;++j) {
        auto base=size_t(j)*values.size();
        for (size_t i=0;i<values.size();++i) byte[base+i]=values[i]>>(8*j);
        for (size_t i=0;i<values.size();i+=8) {
            auto transposed=transpose8(load_le(byte.data()+base+i,std::min<size_t>(8,values.size()-i)));
            for (int k=0;k<std::min(8,bits-j*8);++k) bit[(8*j+k)*plane+i/8]=transposed>>(8*k);
        }
    }
    auto bit_frame=compress(bit),byte_frame=compress(byte);
    const bool use_bit=bit_frame.size()<=byte_frame.size();
    Bytes result=use_bit?std::move(bit_frame):std::move(byte_frame);
    return {result,{{"mode",use_bit?0:1},{"bits",bits},
                    {"raw_size",use_bit?bit.size():byte.size()},{"size",result.size()}}};
}

U unpack(const Bytes& bytes, size_t offset, const Json& descriptor, size_t rows) {
    const size_t size=descriptor.at("size"),raw_size=descriptor.at("raw_size");
    const int bits=descriptor.at("bits"),mode=descriptor.at("mode");
    check(offset<=bytes.size() && size<=bytes.size()-offset,"载荷截断");
    check(bits>=1 && bits<=64 && (mode==0 || mode==1),"排列参数错误");
    check(raw_size==(mode==0?((rows+7)/8)*bits:rows*8),"原始长度错误");
    Bytes raw(raw_size);
    const auto size_read=ZSTD_decompress(raw.data(),raw.size(),bytes.data()+offset,size);
    check(!ZSTD_isError(size_read) && size_read==raw.size(),"解压失败");
    U output(rows);
    if (mode==0) {
        const size_t plane=(rows+7)/8;
        for (int b=0;b<bits;b+=8) for (size_t i=0;i<rows;i+=8) {
            uint64_t matrix=0;
            for (int k=0;k<std::min(8,bits-b);++k) matrix|=uint64_t(raw[(b+k)*plane+i/8])<<(8*k);
            const auto transposed=transpose8(matrix);
            for (size_t j=0;j<std::min<size_t>(8,rows-i);++j) output[i+j]|=((transposed>>(8*j))&255)<<b;
        }
    } else {
        for (int b=0;b<8;++b) for (size_t i=0;i<rows;++i) output[i]|=uint64_t(raw[b*rows+i])<<(b*8);
    }
    return output;
}

Bytes load(const std::filesystem::path& path) {
    std::ifstream input(path,std::ios::binary|std::ios::ate);
    if (!input) throw std::runtime_error("无法读取："+path.string());
    const auto size=input.tellg();
    Bytes bytes(size);
    input.seekg(0);
    input.read((char*)bytes.data(),size);
    check(bool(input),"读取失败");
    return bytes;
}

const std::string magic("MKP\r\n\x1a\n\0",8);
std::pair<Json,size_t> header(const Bytes& bytes) {
    check(bytes.size()>=12 && std::equal(magic.begin(),magic.end(),bytes.begin()),"文件标识错误");
    uint32_t size=0;
    for (int i=0;i<4;++i) size|=uint32_t(bytes[8+i])<<(8*i);
    check(size<=bytes.size()-12,"头部截断");
    auto description=Json::parse(bytes.begin()+12,bytes.begin()+12+size);
    check(description.at("version")==1,"格式版本错误");
    return {description,12+size};
}
Bytes assemble(const Json& header,const std::vector<Bytes>& payload) {
    const std::string text=header.dump();
    Bytes output(magic.begin(),magic.end());
    const uint32_t length=text.size();
    for (int i=0;i<4;++i) output.push_back(length>>(i*8));
    output.insert(output.end(),text.begin(),text.end());
    for (const auto& bytes:payload) output.insert(output.end(),bytes.begin(),bytes.end());
    return output;
}
}
using namespace detail;
int64_t rescale(__int128 value,int from,int to) {
    if (!value) return 0;
    while (from>to) {
        if (value%10) throw std::invalid_argument("指定精度不足");
        value/=10;
        --from;
    }
    while (from<to) {
        value=__int128(checked(value))*10;
        ++from;
    }
    return checked(value);
}

void set_taker_encoding(Json& schema,bool enabled) {
    auto& column=schema.at("taker_buy_volume");
    if (enabled) {
        column["encoding"]="ratio_1000";
        column["reference"]="volume";
    } else {
        column.erase("encoding");
        column.erase("reference");
    }
}

template<class Input>
static std::vector<uint8_t> encode_impl(Input&& input) {
    const Table& table=input;
    check(table.columns.contains("time"),"缺少 time");
    const auto& time=table.columns.at("time");
    const size_t rows=time.size();
    check(std::is_sorted(time.begin(),time.end()),"时间未排序");
    for (const auto& [name,values]:table.columns) {
        check(values.size()==rows,"列长度不一致");
        const auto& spec=table.schema.at(name);
        const int scale=spec.value("scale",0);
        check(scale>=0 && scale<=18,"小数位数必须在0至18之间");
        const auto type=spec.at("type").get<std::string>();
        check(type=="timestamp" || type=="integer" || type=="decimal","不支持的数值列类型");
    }
    Json header={{"version",1},{"rows",rows},{"time_column","time"},
                 {"symbol",table.metadata.value("symbol","")},{"metadata",table.metadata},
                 {"columns",Json::array()},
                 {"first_time",rows?Json(time.front()):Json(nullptr)},
                 {"last_time",rows?Json(time.back()):Json(nullptr)}};
    std::vector<Bytes> payload;
    const bool ohlc=table.columns.contains("open") && table.columns.contains("high")
                    && table.columns.contains("low") && table.columns.contains("close");
    const bool ratio=std::any_of(table.schema.begin(),table.schema.end(),[](const auto& spec) {
        return spec.value("encoding","")=="ratio_1000";
    });
    if (!ohlc && !ratio) {
        encode_block(std::forward<Input>(input).columns,std::forward<Input>(input).schema,header,payload);
        return assemble(header,payload);
    }
    auto columns=table.columns;
    int price_scale=0;
    uint64_t price_factor=0;
    if (ohlc) {
        for (auto name:{"open","high","low","close"}) {
            price_scale=std::max(price_scale,table.schema.at(name).value("scale",0));
        }
        for (auto name:{"open","high","low","close"}) {
            for (auto& value:columns.at(name)) {
                value=rescale(value,table.schema.at(name).value("scale",0),price_scale);
                price_factor=std::gcd(price_factor,value<0?uint64_t(0)-uint64_t(value):uint64_t(value));
            }
        }
        if (!price_factor || price_factor>INT64_MAX) price_factor=1;
        for (auto name:{"open","high","low","close"}) {
            for (auto& value:columns.at(name)) value/=int64_t(price_factor);
        }
    }
    for (const auto& [name,original]:table.columns) {
        Json descriptor=table.schema.at(name);
        descriptor["name"]=name;
        auto values=columns.at(name);
        uint64_t factor=1;
        const bool price=ohlc && (name=="open" || name=="high" || name=="low" || name=="close");
        if (descriptor.value("encoding", "") == "ratio_1000") {
            const auto reference=descriptor.at("reference").get<std::string>();
            const auto& volume=table.columns.at(reference);
            const int volume_scale=table.schema.at(reference).value("scale",0);
            const int value_scale=descriptor.value("scale",0);
            const int common_scale=std::max(volume_scale,value_scale);
            U direct(rows),centered(rows);
            for (size_t i=0;i<rows;++i) {
                const auto v=rescale(volume[i],volume_scale,common_scale);
                const auto b=rescale(original[i],value_scale,common_scale);
                check(v>=0 && b>=0 && b<=v,"比例列必须位于0与参照列之间");
                const int64_t q=v?static_cast<int64_t>((__int128(b)*2000+v)/(__int128(v)*2)):0;
                direct[i]=q;
                centered[i]=zz(q-500);
            }
            auto [direct_bytes,direct_packing]=pack(direct);
            auto [centered_bytes,centered_packing]=pack(centered);
            const bool center=centered_bytes.size()<direct_bytes.size();
            descriptor["type"]="decimal";
            descriptor["scale"]=volume_scale+3;
            check(volume_scale+3<=18,"比例恢复精度超出18位");
            descriptor["factor"]=1;
            descriptor["first"]=0;
            descriptor["transform"]="ratio_1000";
            descriptor["centered"]=center;
            descriptor.update(center?centered_packing:direct_packing);
            header["columns"].push_back(descriptor);
            header["metadata"]["lossy_columns"][name]={{"reference",reference},{"ratio_step",0.001}};
            payload.push_back(center?std::move(centered_bytes):std::move(direct_bytes));
            continue;
        }
        if (price) {
            factor=price_factor;
            descriptor["scale"]=price_scale;
        } else if (name!="time") {
            factor=0;
            for (auto value:values) factor=std::gcd(factor,value<0?uint64_t(0)-uint64_t(value):uint64_t(value));
            if (!factor || factor>INT64_MAX) factor=1;
            for (auto& value:values) value/=int64_t(factor);
        }
        descriptor["factor"]=factor;
        descriptor["first"]=rows?values.front():0;
        U stream(rows);
        std::string transform="direct";
        if (name=="time" || (name=="close" && price)) {
            transform="delta";
            for (size_t i=1;i<rows;++i) stream[i]=zz(checked(__int128(values[i])-values[i-1]));
        } else if (price) {
            transform=name;
            const auto& open=columns.at("open");
            const auto& close=columns.at("close");
            for (size_t i=0;i<rows;++i) {
                const __int128 value=name=="open" ? __int128(values[i])-close[i?i-1:0]
                    : name=="high" ? __int128(values[i])-std::max(open[i],close[i])
                    : __int128(std::min(open[i],close[i]))-values[i];
                if (name!="open") check(value>=0,"OHLC 关系错误");
                stream[i]=zz(checked(value));
            }
        } else {
            for (size_t i=0;i<rows;++i) stream[i]=zz(values[i]);
        }
        descriptor["transform"]=transform;
        auto [bytes,packing]=pack(stream);
        descriptor.update(packing);
        header["columns"].push_back(descriptor);
        payload.push_back(std::move(bytes));
    }
    return assemble(header,payload);
}

std::vector<uint8_t> encode(const Table& table) {return encode_impl(table);}
std::vector<uint8_t> encode(Table&& table) {return encode_impl(std::move(table));}
static Table decode_selected(const Bytes& bytes,const std::vector<std::string>* requested) {
    auto [description,offset]=header(bytes);
    const bool predictive=std::any_of(description.at("columns").begin(),description.at("columns").end(),[](const auto& d) {
        return d.value("codec","")=="predictive";
    });
    if (predictive) return decode_block(bytes,description,offset,requested);
    const size_t rows=description.at("rows");
    Table table;
    table.metadata=description.at("metadata");
    std::map<std::string,Json> descriptors;
    std::set<std::string> selected;
    if (requested) {
        std::map<std::string,const Json*> available;
        for (const auto& d:description.at("columns")) available.emplace(d.at("name").get<std::string>(),&d);
        auto require=[&](auto&& self,const std::string& name)->void {
            if (!selected.insert(name).second) return;
            const auto& d=*available.at(name);
            const auto& transform=d.at("transform");
            if (transform=="open" || transform=="high" || transform=="low") self(self,"close");
            if (transform=="high" || transform=="low") self(self,"open");
            if (transform=="ratio_1000") self(self,d.at("reference"));
        };
        for (const auto& name:*requested) require(require,name);
    }
    for (const auto& descriptor:description.at("columns")) {
        const std::string name=descriptor.at("name");
        if (requested && !selected.contains(name)) {
            const size_t size=descriptor.at("size");
            check(offset<=bytes.size() && size<=bytes.size()-offset,"载荷截断");
            offset+=size;
            continue;
        }
        auto stream=unpack(bytes,offset,descriptor,rows);
        offset+=size_t(descriptor.at("size"));
        auto& values=table.columns[name];
        values.resize(rows);
        const bool ratio=descriptor.at("transform")=="ratio_1000";
        for (size_t i=0;i<rows;++i) {
            values[i]=ratio && !descriptor.at("centered").get<bool>()
                ? static_cast<int64_t>(stream[i]) : unzz(stream[i]);
            if (ratio && descriptor.at("centered").get<bool>()) values[i]+=500;
        }
        if (descriptor.at("transform")=="delta" && rows) {
            values.front()=descriptor.at("first");
            for (size_t i=1;i<rows;++i) values[i]=checked(__int128(values[i])+values[i-1]);
        }
        descriptors[name]=descriptor;
        table.schema[name]={{"type",descriptor.at("type")},{"scale",descriptor.value("scale",0)}};
        if (ratio) {
            table.schema[name]["encoding"]="ratio_1000";
            table.schema[name]["reference"]=descriptor.at("reference");
        }
    }
    check(offset==bytes.size(),"文件尾有多余字节");
    for (auto name:{"open","high","low"}) {
        if (!descriptors.contains(name) || descriptors.at(name).at("transform")!=name) continue;
        auto& values=table.columns.at(name);
        const auto& close=table.columns.at("close");
        const auto& open=table.columns.at("open");
        for (size_t i=0;i<rows;++i) {
            const __int128 value=name==std::string("open") ? __int128(values[i])+close[i?i-1:0]
                : name==std::string("high") ? __int128(values[i])+std::max(open[i],close[i])
                : __int128(std::min(open[i],close[i]))-values[i];
            values[i]=checked(value);
        }
    }
    for (auto& [name,values]:table.columns) {
        const int64_t factor=descriptors.at(name).at("factor");
        for (auto& value:values) value=checked(__int128(value)*factor);
    }
    for (const auto& [name,descriptor]:descriptors) {
        if (descriptor.at("transform")!="ratio_1000") continue;
        const auto& volume=table.columns.at(descriptor.at("reference").get<std::string>());
        auto& values=table.columns.at(name);
        for (size_t i=0;i<rows;++i) values[i]=checked(__int128(volume[i])*values[i]);
    }
    if (requested) for (const auto& name:selected) {
        if (std::find(requested->begin(),requested->end(),name)!=requested->end()) continue;
        table.columns.erase(name);
        table.schema.erase(name);
    }
    return table;
}
Table decode(const Bytes& bytes) { return decode_selected(bytes,nullptr); }
Table decode(const Bytes& bytes,const std::vector<std::string>& requested) {
    return decode_selected(bytes,&requested);
}
Json info(const std::vector<uint8_t>& bytes) { return header(bytes).first; }
Json info(const std::filesystem::path& path) {
    std::ifstream input(path,std::ios::binary);
    Bytes bytes(12);
    input.read((char*)bytes.data(),12);
    check(input.gcount()==12,"头部截断");
    uint32_t size=0;
    for (int i=0;i<4;++i) size|=uint32_t(bytes[8+i])<<(8*i);
    bytes.resize(12+size);
    input.read((char*)bytes.data()+12,size);
    check(size_t(input.gcount())==size,"头部截断");
    return header(bytes).first;
}

Table read(const std::filesystem::path& path) { return decode(load(path)); }

void write(const std::filesystem::path& path, const std::vector<uint8_t>& bytes) {
    if (!path.parent_path().empty()) std::filesystem::create_directories(path.parent_path());
    const auto temporary=path.string()+".tmp";
    {
        std::ofstream output(temporary,std::ios::binary);
        output.write((char*)bytes.data(),bytes.size());
        check(bool(output),"写入失败");
    }
    std::filesystem::rename(temporary,path);
}

void write(const std::filesystem::path& path, const Table& table) {
    write(path,encode(table));
}

Table merge(const std::vector<Table>& inputs) {
    Table output;
    if (inputs.empty()) return output;
    output.metadata=inputs.front().metadata;
    output.schema=inputs.front().schema;
    for (const auto& table:inputs) {
        check(table.schema.size()==output.schema.size(),"合并列不一致");
        if (table.metadata.contains("lossy_columns")) {
            if (!output.metadata.contains("lossy_columns")) output.metadata["lossy_columns"]=Json::object();
            output.metadata["lossy_columns"].update(table.metadata.at("lossy_columns"));
        }
        for (auto& [name,schema]:output.schema.items()) {
            schema["scale"]=std::max(schema.value("scale",0),table.schema.at(name).value("scale",0));
            if (table.schema.at(name).value("encoding","")=="ratio_1000") {
                schema["encoding"]="ratio_1000";
                schema["reference"]=table.schema.at(name).at("reference");
            }
        }
    }
    for (const auto& table:inputs) {
        for (const auto& [name,values]:table.columns) {
            auto& destination=output.columns[name];
            for (auto value:values) destination.push_back(rescale(value,table.schema.at(name).value("scale",0),output.schema.at(name).value("scale",0)));
        }
    }
    const auto& time=output.columns.at("time");
    std::vector<size_t> order(time.size());
    std::iota(order.begin(),order.end(),0);
    std::stable_sort(order.begin(),order.end(),[&](size_t a,size_t b) { return time[a]<time[b]; });
    for (auto& [name,values]:output.columns) {
        const auto copy=values;
        for (size_t i=0;i<values.size();++i) values[i]=copy[order[i]];
    }
    return output;
}

void merge_files(const std::vector<std::filesystem::path>& paths,const std::filesystem::path& destination) {
    check(!paths.empty(),"缺少合并输入文件");
    std::vector<Table> tables;
    tables.reserve(paths.size());
    for (const auto& path:paths) tables.push_back(read(path));
    write(destination,merge(tables));
}
}

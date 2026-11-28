#include <mktpack/core.hpp>
#include <mktpack/decimal.hpp>
#include <stdexcept>

namespace {
void expect(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}
}

int main() {
    using namespace mktpack;
    int literal_scale=0;
    expect(decimal_integer("001.2300e+2",&literal_scale).value==123 && literal_scale==2,"十进制指数解析错误");
    expect(decimal_integer("-9223372036854775808").value==INT64_MIN,"int64 下界解析错误");
    expect(decimal_integer(".000").value==0,"零值解析错误");
    Table table;
    table.columns={{"time",{1,1,3}},{"open",{12,14,17}},{"high",{15,18,19}},
                   {"low",{10,12,14}},{"close",{13,17,15}},{"taker_buy_volume",{5,0,17}}};
    for (const auto& [name,values]:table.columns) {
        table.schema[name]={{"type",name=="time"?"timestamp":"decimal"},{"scale",name=="time"?0:2}};
    }
    table.metadata={{"symbol","BTCUSDT"}};
    const auto encoded_table=encode(table);
    expect(info(encoded_table)["columns"][0]["type"]=="real","新编码应将旧 decimal 升级为 real");
    expect(decode(encoded_table).columns==table.columns,"往返失败");
    // 构造存量 decimal 标签字节流，验证向后兼容解析
    {
        auto h=info(encoded_table);
        for (auto& col:h["columns"]) if (col["type"]=="real") col["type"]="decimal";
        const auto h_text=h.dump();
        uint32_t old_len=0;
        for (int i=0;i<4;++i) old_len|=uint32_t(encoded_table[8+i])<<(8*i);
        std::vector<uint8_t> legacy;
        legacy.reserve(12+h_text.size()+(encoded_table.size()-12-old_len));
        legacy.insert(legacy.end(),encoded_table.begin(),encoded_table.begin()+8);
        const uint32_t new_len=static_cast<uint32_t>(h_text.size());
        for (int i=0;i<4;++i) legacy.push_back(uint8_t(new_len>>(8*i)));
        legacy.insert(legacy.end(),h_text.begin(),h_text.end());
        legacy.insert(legacy.end(),encoded_table.begin()+12+old_len,encoded_table.end());
        expect(info(legacy)["columns"][0]["type"]=="decimal","未构造出存量 decimal 头部");
        expect(decode(legacy).columns==table.columns,"存量 decimal 文件头解析失败");
    }
    auto combined=merge({table,table});
    expect(combined.columns.at("time")==std::vector<int64_t>{1,1,1,1,3,3},"合并排序失败");

    auto short_column=table;
    short_column.columns.at("open").pop_back();
    bool rejected=false;
    try { encode(short_column); } catch (const std::runtime_error&) { rejected=true; }
    expect(rejected,"短OHLC列未拒绝");

    auto mixed=table;
    mixed.schema["open"]["scale"]=3;
    for (auto& value:mixed.columns.at("open")) value*=10;
    auto decoded=decode(encode(mixed));
    for (auto name:{"open","high","low","close"}) {
        expect(decoded.schema[name]["scale"]==3,"OHLC精度未统一");
        for (size_t i=0;i<3;++i) expect(decoded.columns.at(name)[i]==table.columns.at(name)[i]*10,"OHLC精确缩放错误");
    }

    for (const auto& name:{"open","high","low","close"}) {
        auto projection=decode(encode(mixed),{name});
        expect(projection.columns.size()==1 && projection.columns.at(name)==decoded.columns.at(name),"OHLC列投影错误");
    }
    auto quantized=table;
    quantized.columns["volume"]={10,0,34};
    quantized.schema["volume"]={{"type","integer"},{"scale",0}};
    quantized.schema["taker_buy_volume"]={{"type","integer"},{"encoding","ratio_1000"},{"reference","volume"}};
    auto ratio=encode(quantized);
    expect(decode(ratio,{"taker_buy_volume"}).columns.at("taker_buy_volume")==decode(ratio).columns.at("taker_buy_volume"),
           "比例列投影缺少参照量");

    Table day;
    day.columns={{"time",{0,0,250,500}},{"period",{20261002,20261002,20261002,20261002}},
                 {"price",{100,110,110,0}},{"p1",{1050,950,1040,0}},{"p2",{95,90,95,0}},
                 {"q1",{8,9,10,0}},{"q2",{7,5,11,0}},{"volume",{5,6,9,9}},{"amount",{500,610,940,940}},
                 {"bounds",{INT64_MIN,INT64_MAX,0,0}}};
    day.metadata={{"symbol","X2610"}};
    for (const auto& [name,values]:day.columns) day.schema[name]={{"type",name=="time"?"timestamp":"integer"}};
    for (const auto& name:{"price","p1","p2"}) day.schema[name]={{"type","decimal"},{"scale_group","quotes"},{"scale",1}};
    day.schema["p1"]["scale"]=2;
    day.schema["p1"]["prediction"]={{"kind","reference"},{"column","price"}};
    day.schema["p2"]["prediction"]={{"kind","reference"},{"column","p1"}};
    for (const auto& name:{"q1","q2"})
        day.schema[name]["prediction"]={{"kind","level_quantity"},{"price",name==std::string("q1")?"p1":"p2"},
                                      {"prices",{"p1","p2"}},{"quantities",{"q1","q2"}}};
    day.schema["amount"]["prediction"]={{"kind","trade_value"},{"price","price"},{"volume","volume"}};
    auto block=encode(day);
    const auto block_info=info(block);
    expect(block_info["version"]==1,"预测编码改变了格式版本");
    Columns expected=day.columns;
    for (auto n:{"price","p2"}) for (auto& x:expected[n]) x*=10;
    expect(decode(block).columns==expected,"多列预测日块往返错误");
    expect(decode(encode(decode(block))).columns==expected,"日块重编码发生变化");
    for (const auto& [name,values]:expected) {
        expect(decode(block,{name}).columns==Columns{{name,values}},"单列预测依赖错误");
    }
    expect(decode(block,{}).columns.empty(),"空投影错误");
    size_t offset=12;
    for (unsigned i=0;i<4;++i) offset+=size_t(block[8+i])<<(8*i);
    expect(block_info.at("codec")=="predictive","文件头缺少预测编码标签");
    for (const auto& d:block_info.at("columns")) {
        expect(!d.contains("codec"),"列描述重复保存预测编码标签");
        if (d.at("name")=="period") expect(d.at("size")==0,"常量不应写入压缩帧");
        if (d.at("name")=="bounds") block[offset]=0;
        offset+=d.at("size").get<size_t>();
    }
    expect(decode(block,{"q1"}).columns.at("q1")==day.columns.at("q1"),"列投影不应解压无关帧");
    Table sparse;
    sparse.schema={{"time",{{"type","timestamp"}}},{"value",{{"type","integer"}}}};
    int64_t level=17;
    uint32_t state=1234;
    for (int i=0;i<30000;++i) {
        state=state*1664525u+1013904223u;
        if ((state>>24)<3) level+=int((state>>10)%12345)-6000;
        sparse.columns["time"].push_back(i*500);
        sparse.columns["value"].push_back(level);
    }
    const auto rare=encode(sparse);
    expect(info(rare)["columns"][1]["transform"]=="changes","稀疏变更未压缩");
    expect(decode(rare).columns==sparse.columns,"稀疏变更往返错误");

    auto empty=table;
    for (auto& [name,values]:empty.columns) values.clear();
    expect(decode(encode(empty)).columns==empty.columns,"空表往返失败");

    Table bounds;
    bounds.columns={{"time",{0,1}},{"value",{INT64_MIN,INT64_MAX}}};
    bounds.schema={{"time",{{"type","timestamp"}}},{"value",{{"type","integer"}}}};
    expect(decode(encode(bounds)).columns==bounds.columns,"int64边界往返失败");
    bounds.columns["time"]={INT64_MIN,INT64_MAX};
    expect(decode(encode(bounds)).columns==bounds.columns,"时间差分溢出未回退直接编码");
}

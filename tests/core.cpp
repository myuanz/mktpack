#include <mktpack/core.hpp>
#include <stdexcept>

namespace {
void expect(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}
}

int main() {
    using namespace mktpack;
    Table table;
    table.columns={{"time",{1,1,3}},{"open",{12,14,17}},{"high",{15,18,19}},
                   {"low",{10,12,14}},{"close",{13,17,15}},{"taker_buy_volume",{5,0,17}}};
    for (const auto& [name,values]:table.columns) {
        table.schema[name]={{"type",name=="time"?"timestamp":"decimal"},{"scale",name=="time"?0:2}};
    }
    table.metadata={{"symbol","BTCUSDT"}};
    expect(decode(encode(table)).columns==table.columns,"往返失败");
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

    auto empty=table;
    for (auto& [name,values]:empty.columns) values.clear();
    expect(decode(encode(empty)).columns==empty.columns,"空表往返失败");

    Table bounds;
    bounds.columns={{"time",{0,1}},{"value",{INT64_MIN,INT64_MAX}}};
    bounds.schema={{"time",{{"type","timestamp"}}},{"value",{{"type","integer"}}}};
    expect(decode(encode(bounds)).columns==bounds.columns,"int64边界往返失败");
    bounds.columns["time"]={INT64_MIN,INT64_MAX};
    rejected=false;
    try { encode(bounds); } catch (const std::runtime_error&) { rejected=true; }
    expect(rejected,"差分溢出未拒绝");
}

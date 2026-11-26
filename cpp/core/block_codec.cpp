#include "column_codec.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <set>

namespace mktpack::detail {
namespace {
using Values=std::vector<int64_t>;
using Wide=__int128;
using Names=std::vector<std::string>;

uint64_t magnitude(int64_t v) { return v<0?uint64_t(0)-uint64_t(v):uint64_t(v); }

Wide power10(int scale) {
    Wide value=1;
    while (scale-->0) value*=10;
    return value;
}

enum class Transform { constant, direct, changes, delta, reference, quantity, amount };

Transform transform(const std::string& kind) {
    if (kind=="constant") return Transform::constant;
    if (kind=="direct") return Transform::direct;
    if (kind=="changes") return Transform::changes;
    if (kind=="delta") return Transform::delta;
    if (kind=="reference") return Transform::reference;
    if (kind=="level_quantity") return Transform::quantity;
    if (kind=="trade_value") return Transform::amount;
    throw std::runtime_error("未知列变换："+kind);
}

struct Model {
    Transform kind;
    const Values* self;
    const Values* price=nullptr;
    const Values* volume=nullptr;
    std::vector<const Values*> prices,quantities;
    int64_t multiplier=1;
    Wide numerator=1,denominator=1;

    // 调用方单独保存首行，后续行共享同一预测公式
    bool predict(size_t row,Wide& value) const {
        switch (kind) {
        case Transform::delta: value=(*self)[row-1]; return true;
        case Transform::reference: value=(*price)[row]; return true;
        case Transform::quantity: {
            const auto current=(*price)[row];
            if (current) for (size_t j=0;j<prices.size();++j) {
                if ((*prices[j])[row-1]==current) {
                    value=(*quantities[j])[row-1];
                    return true;
                }
            }
            value=0;
            return true;
        }
        case Transform::amount: {
            const Wide diff=Wide((*volume)[row])-(*volume)[row-1];
            Wide amount;
            if (__builtin_mul_overflow(diff,Wide((*price)[row]),&amount) ||
                __builtin_mul_overflow(amount,Wide(multiplier),&amount) ||
                __builtin_mul_overflow(amount,numerator,&amount)) return false;
            return !__builtin_add_overflow(Wide((*self)[row-1]),amount/denominator,&value);
        }
        default: value=0; return true;
        }
    }
};

Model model_for(const std::string& name,const Json& descriptor,const Columns& columns,const Json& schema) {
    Model model{transform(descriptor.at("transform")), &columns.at(name)};
    if (!descriptor.contains("prediction")) return model;
    const auto& pred=descriptor.at("prediction");
    if (model.kind==Transform::reference) {
        const auto ref=pred.at("column").get<std::string>();
        check(schema.at(ref).value("scale",0)==schema.at(name).value("scale",0),"参照列小数尺度不一致");
        model.price=&columns.at(ref);
    } else if (model.kind==Transform::quantity) {
        model.price=&columns.at(pred.at("price").get<std::string>());
        for (const auto& s:pred.at("prices")) model.prices.push_back(&columns.at(s.get<std::string>()));
        for (const auto& s:pred.at("quantities")) model.quantities.push_back(&columns.at(s.get<std::string>()));
        check(model.prices.size()==model.quantities.size(),"预测档位数量不一致");
    } else if (model.kind==Transform::amount) {
        const auto price=pred.at("price").get<std::string>(),volume=pred.at("volume").get<std::string>();
        model.price=&columns.at(price);
        model.volume=&columns.at(volume);
        model.multiplier=pred.value("multiplier",int64_t(1));
        const int scale=schema.at(name).value("scale",0)-schema.at(price).value("scale",0)-schema.at(volume).value("scale",0);
        model.numerator=power10(std::max(0,scale));
        model.denominator=power10(std::max(0,-scale));
    }
    return model;
}

int64_t estimate_multiplier(const Model& model) {
    std::vector<int64_t> samples;
    for (size_t i=1;i<model.self->size() && samples.size()<1024;++i) {
        const auto dv=Wide((*model.volume)[i])-(*model.volume)[i-1];
        const auto da=Wide((*model.self)[i])-(*model.self)[i-1];
        const auto price=(*model.price)[i];
        if (dv<=0 || da<=0 || price<=0) continue;
        const long double ratio=static_cast<long double>(da)*model.denominator/
            (static_cast<long double>(dv)*price*model.numerator);
        const long double rounded=std::round(ratio);
        if (rounded>=1 && rounded<std::ldexp(1.L,63)) samples.push_back(static_cast<int64_t>(rounded));
    }
    if (samples.empty()) return 1;
    const auto mid=samples.begin()+samples.size()/2;
    std::nth_element(samples.begin(),mid,samples.end());
    return *mid;
}

struct ScaleGroup { int scale=0; uint64_t gcd=0; };

std::map<std::string,ScaleGroup> normalize_prices(Columns& columns,Json& schema) {
    std::map<std::string,ScaleGroup> groups;
    for (const auto& [name,spec]:schema.items()) {
        if (!spec.contains("scale_group")) continue;
        auto& g=groups[spec.at("scale_group").get<std::string>()];
        g.scale=std::max(g.scale,spec.value("scale",0));
    }
    for (auto& [name,values]:columns) {
        auto& spec=schema.at(name);
        if (!spec.contains("scale_group")) continue;
        auto& group=groups.at(spec.at("scale_group").get<std::string>());
        const int scale=spec.value("scale",0);
        for (auto& value:values) {
            value=rescale(value,scale,group.scale);
            group.gcd=std::gcd(group.gcd,magnitude(value));
        }
        spec["scale"]=group.scale;
    }
    return groups;
}

Names dependencies(const Json& d,bool include_previous) {
    const auto kind=transform(d.at("transform"));
    Names deps;
    if (kind==Transform::reference || kind==Transform::amount || kind==Transform::quantity) {
        const auto& pred=d.at("prediction");
        if (kind==Transform::reference) deps.push_back(pred.at("column"));
        if (kind==Transform::amount) deps={pred.at("price"),pred.at("volume")};
        if (kind==Transform::quantity) {
            deps.push_back(pred.at("price"));
            if (include_previous) {
                for (const auto& s:pred.at("prices")) deps.push_back(s.get<std::string>());
                for (const auto& s:pred.at("quantities")) deps.push_back(s.get<std::string>());
            }
        }
    }
    return deps;
}
}

void encode_block(Columns columns,Json schema,Json& header,std::vector<Bytes>& payload) {
    const auto groups=normalize_prices(columns,schema);
    for (const auto& [name,values]:columns) {
        auto d=schema.at(name);
        d["name"]=name;
        d["codec"]="predictive";
        d["first"]=values.empty()?0:values[0];
        if (std::adjacent_find(values.begin(),values.end(),std::not_equal_to<>())==values.end()) {
            d["transform"]="constant";
            d["size"]=0;
        } else {
            d["transform"]=d.contains("prediction")?d.at("prediction").at("kind").get<std::string>():"delta";
            auto model=model_for(name,d,columns,schema);
            if (model.kind==Transform::amount && !d.at("prediction").contains("multiplier")) {
                model.multiplier=estimate_multiplier(model);
                d["prediction"]["multiplier"]=model.multiplier;
            }
            Values residual(values.size());
            for (size_t i=1;i<values.size();++i) {
                Wide prediction,value;
                if (!model.predict(i,prediction) || __builtin_sub_overflow(Wide(values[i]),prediction,&value) ||
                    value<INT64_MIN || value>INT64_MAX) {
                    d["transform"]="direct";
                    std::copy(values.begin()+1,values.end(),residual.begin()+1);
                    break;
                }
                residual[i]=static_cast<int64_t>(value);
            }
            uint64_t gcd=d.contains("scale_group")?groups.at(d.at("scale_group").get<std::string>()).gcd:0;
            for (auto value:residual) {
                gcd=std::gcd(gcd,magnitude(value));
                if (gcd==1) break;
            }
            const int64_t factor=gcd && gcd<=INT64_MAX?int64_t(gcd):1;
            d["factor"]=factor;
            U stream(residual.size());
            for (size_t i=0;i<residual.size();++i) stream[i]=zz(residual[i]/factor);
            auto [bytes,layout]=pack(stream);
            if (d.at("transform")=="delta") {
                const size_t changes=std::count_if(stream.begin(),stream.end(),[](auto v){return v!=0;});
                if (changes && changes<=values.size()/32) {
                    U sparse;
                    sparse.reserve(changes*2);
                    size_t previous=0;
                    for (size_t i=1;i<stream.size();++i) if (stream[i]) {
                        sparse.push_back(i-previous);
                        sparse.push_back(stream[i]);
                        previous=i;
                    }
                    auto [short_bytes,short_layout]=pack(sparse);
                    const auto extra=Json{{"changes",changes}}.dump().size()+1;
                    if (short_bytes.size()+extra<bytes.size()) {
                        d["transform"]="changes";
                        d["changes"]=changes;
                        layout=std::move(short_layout);
                        bytes=std::move(short_bytes);
                    }
                }
            }
            d.update(layout);
            payload.push_back(std::move(bytes));
        }
        header["columns"].push_back(std::move(d));
    }
}

Table decode_block(const Bytes& bytes,const Json& header,size_t offset,const Names* requested) {
    const size_t rows=header.at("rows");
    struct Column {const Json& descriptor; size_t offset;};
    std::map<std::string,Column> available;
    for (const auto& descriptor:header.at("columns")) {
        const std::string name=descriptor.at("name");
        available.emplace(name,Column{descriptor,offset});
        const size_t size=descriptor.at("size");
        check(offset<=bytes.size() && size<=bytes.size()-offset,"载荷截断");
        offset+=size;
    }
    check(offset==bytes.size(),"文件尾有多余字节");
    std::set<std::string> needed;
    auto require=[&](auto&& self,const std::string& name)->void {
        if (!needed.insert(name).second) return;
        for (const auto& dep:dependencies(available.at(name).descriptor,true)) self(self,dep);
    };
    if (requested) { for (const auto& name:*requested) require(require,name); }
    else { for (const auto& [name,column]:available) needed.insert(name); }

    Table table;
    table.metadata=header.at("metadata");
    for (const auto& name:needed) {
        const auto& col=available.at(name);
        const auto& d=col.descriptor;
        auto& spec=table.schema[name];
        spec={{"type",d.at("type")},{"scale",d.value("scale",0)}};
        for (auto k:{"prediction","scale_group"}) if (d.contains(k)) spec[k]=d.at(k);
        auto& values=table.columns[name];
        const int64_t first=d.at("first");
        if (d.at("transform")=="constant") {
            values.assign(rows,first);
        } else if (d.at("transform")=="changes") {
            const size_t changes=d.at("changes");
            check(changes<rows && changes<=SIZE_MAX/2,"变更计数错误");
            const auto stream=unpack(bytes,col.offset,d,2*changes);
            const int64_t factor=d.at("factor");
            values.resize(rows);
            int64_t current=first;
            size_t previous=0;
            for (size_t i=0;i<stream.size();i+=2) {
                const auto gap=stream[i];
                check(gap && gap<rows-previous,"变更位置错误");
                std::fill_n(values.begin()+previous,gap,current);
                previous+=gap;
                current=checked(Wide(current)+Wide(unzz(stream[i+1]))*factor);
            }
            std::fill(values.begin()+previous,values.end(),current);
        } else {
            auto stream=unpack(bytes,col.offset,d,rows);
            const int64_t factor=d.at("factor");
            values.resize(rows);
            if (rows) values.front()=first;
            for (size_t i=1;i<rows;++i) values[i]=checked(Wide(unzz(stream[i]))*factor);
        }
    }
    struct Restore {Model model; Values& values;};
    std::vector<Restore> restorers;
    std::set<std::string> visiting,ordered;
    auto order=[&](auto&& self,const std::string& name)->void {
        if (ordered.contains(name)) return;
        check(visiting.insert(name).second,"列预测循环依赖");
        const auto& d=available.at(name).descriptor;
        for (const auto& dep:dependencies(d,false)) self(self,dep);
        visiting.erase(name);
        ordered.insert(name);
        auto m=model_for(name,d,table.columns,table.schema);
        if (m.kind!=Transform::constant && m.kind!=Transform::direct && m.kind!=Transform::changes)
            restorers.push_back({std::move(m),table.columns.at(name)});
    };
    for (const auto& name:needed) order(order,name);
    for (size_t i=1;i<rows;++i) for (auto& [m,values]:restorers) {
        Wide prediction,sum;
        check(m.predict(i,prediction) && !__builtin_add_overflow(prediction,Wide(values[i]),&sum),"预测值溢出");
        values[i]=checked(sum);
    }
    if (requested) for (const auto& name:needed) {
        if (std::find(requested->begin(),requested->end(),name)==requested->end()) {
            table.columns.erase(name);
            table.schema.erase(name);
        }
    }
    return table;
}
}

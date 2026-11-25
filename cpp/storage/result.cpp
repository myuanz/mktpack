#include "result.hpp"
#include <cstring>
#include <set>
#include <queue>

namespace mktpack::storage {
namespace {
using Buffer = std::vector<uint8_t>;

template<class T> void append_value(Buffer& buffer, T value) {
    const auto offset = buffer.size();
    buffer.resize(offset + sizeof(T));
    std::memcpy(buffer.data() + offset, &value, sizeof(T));
}

__int128 power10(int exponent) {
    if (exponent < 0 || exponent > 38) throw std::runtime_error("小数尺度超出 decimal128 范围");
    __int128 value = 1;
    for (int i = 0; i < exponent; ++i) value *= 10;
    return value;
}

ResultColumn column(const std::string& type, int scale, size_t rows) {
    ResultColumn out{type,scale,rows};
    auto& buffers = out.buffers;
    if (type == "null") {
        buffers.resize(1);
    } else if (type == "any") {
        buffers.resize(3);
        buffers[1].reserve(rows);
        buffers[2].reserve(rows * sizeof(int32_t));
        for (auto name : {"null","integer","real","string","blob"})
            out.children.push_back(column(name,0,0));
    } else if (type == "string" || type == "blob") {
        buffers.resize(3);
        buffers[1].reserve((rows+1) * sizeof(int32_t));
        append_value(buffers[1],int32_t(0));
    } else {
        buffers.resize(2);
        buffers[1].reserve(rows * (type == "decimal" ? 16 : 8));
    }
    return out;
}

void append_string(ResultColumn& out, const void* data, size_t length) {
    auto& bytes = out.buffers[2];
    if (length > INT32_MAX-bytes.size()) throw std::runtime_error("Arrow 字符串数据超过 2 GiB");
    if (length) {
        const auto* first = static_cast<const uint8_t*>(data);
        bytes.insert(bytes.end(), first, first+length);
    }
    append_value(out.buffers[1], static_cast<int32_t>(bytes.size()));
}

int union_child(const Json& value) {
    if (value.is_null()) return 0;
    if (value.is_number_integer()) return 1;
    if (value.is_number_float()) return 2;
    return value.is_binary() ? 4 : 3;
}

void append_sql_value(ResultColumn& out, size_t row, const Json& value, __int128 factor = 1) {
    if (out.type == "any") {
        const auto code = union_child(value);
        auto& child = out.children[code];
        if(child.rows > INT32_MAX) throw std::runtime_error("Arrow union 子列偏移溢出");
        out.buffers[1].push_back(static_cast<uint8_t>(code));
        append_value(out.buffers[2],static_cast<int32_t>(child.rows));
        const auto pos = child.rows++;
        append_sql_value(child,pos,value);
        return;
    }
    if (out.type == "null") return;
    auto& validity = out.buffers[0];
    if (value.is_null()) {
        if (validity.empty()) validity.resize((out.rows+7)/8,0xff);
        validity[row/8] &= uint8_t(~(1u << (row%8)));
    }
    if (out.type == "string") {
        if (value.is_null()) append_string(out,nullptr,0);
        else {
            const auto& text = value.get_ref<const std::string&>();
            append_string(out,text.data(),text.size());
        }
    } else if (out.type == "blob") {
        if (value.is_null()) append_string(out,nullptr,0);
        else {
            const auto& bytes = value.get_binary();
            append_string(out,bytes.data(),bytes.size());
        }
    } else if (out.type == "real") {
        append_value(out.buffers[1],value.is_null() ? 0.0 : value.get<double>());
    } else if (out.type == "decimal") {
        const int64_t coefficient=value.is_null()?0:value.get<int64_t>();
        __int128 number;
        static const auto limit=power10(38);
        if (__builtin_mul_overflow(factor,__int128(coefficient),&number) || number<=-limit || number>=limit)
            throw std::runtime_error("统一尺度后超出 decimal128 精度");
        append_value(out.buffers[1],number);
    } else {
        append_value(out.buffers[1],value.is_null() ? int64_t(0) : value.get<int64_t>());
    }
}

struct Position { size_t table, row; };

// 无重叠的块顺序复制。重叠时用同一份稳定行序生成所有输出列。
std::vector<Position> ordered_rows(const std::vector<Table>& tables, size_t total) {
    bool sorted = true;
    int64_t last = INT64_MIN;
    for (const auto& table : tables) {
        const auto& time = table.columns.at("time");
        if (time.empty()) continue;
        sorted &= time.front() >= last;
        last = time.back();
    }
    if (sorted) return {};
    std::vector<const std::vector<int64_t>*> clocks;
    for (const auto& table : tables) clocks.push_back(&table.columns.at("time"));
    const auto later=[&](Position a, Position b) {
        const auto ta=(*clocks[a.table])[a.row], tb=(*clocks[b.table])[b.row];
        return ta==tb ? a.table>b.table : ta>tb;
    };
    std::priority_queue<Position,std::vector<Position>,decltype(later)> next(later);
    for (size_t i=0; i<tables.size(); ++i)
        if(!clocks[i]->empty()) next.push({i,0});
    std::vector<Position> order;
    order.reserve(total);
    while(!next.empty()) {
        auto pos=next.top();
        next.pop();
        const auto length=clocks[pos.table]->size();
        do {
            order.push_back(pos);
            ++pos.row;
        } while(pos.row<length && (next.empty() || !later(pos,next.top())));
        if(pos.row<length) next.push(pos);
    }
    return order;
}
}

QueryResult mkp_result(std::vector<Table> tables, const Json& empty_schema,
                       std::vector<std::string> names) {
    QueryResult result{0,std::move(names),{}};
    for (const auto& table : tables) result.rows += table.columns.at("time").size();
    auto order = ordered_rows(tables,result.rows);
    for (const auto& name : result.names) {
        if (name == "symbol") {
            auto out = column("string",0,result.rows);
            std::vector<std::string> symbols;
            for (const auto& t : tables) symbols.push_back(t.metadata.value("symbol",""));
            auto emit=[&](size_t i){append_string(out,symbols[i].data(),symbols[i].size());};
            if (!order.empty()) {
                for(auto pos : order) emit(pos.table);
            } else {
                for(size_t i=0;i<tables.size();++i)
                    for(size_t n=tables[i].columns.at("time").size();n;--n) emit(i);
            }
            result.columns.push_back(std::move(out));
            continue;
        }
        const auto& spec = tables.empty() ? empty_schema.at(name) : tables.front().schema.at(name);
        int scale = spec.value("scale",0);
        for(const auto& t : tables) scale = std::max(scale,t.schema.at(name).value("scale",0));
        auto out = column(spec.at("type").get<std::string>(),scale,result.rows);
        auto& values = out.buffers[1];
        std::vector<__int128> factors;
        std::vector<const int64_t*> sources;
        for(const auto& t : tables) {
            factors.push_back(power10(scale-t.schema.at(name).value("scale",0)));
            sources.push_back(t.columns.at(name).data());
        }
        if(order.empty()) {
            for(size_t i=0; i<tables.size(); ++i) {
                const auto& v=tables[i].columns.at(name);
                if (out.type=="decimal") {
                    for(auto x:v) append_value(values,x*factors[i]);
                } else if(!v.empty()) {
                    auto pos=values.size();
                    values.resize(pos+v.size()*sizeof(int64_t));
                    std::memcpy(values.data()+pos,v.data(),v.size()*sizeof(int64_t));
                }
            }
        } else if(out.type=="decimal") {
            for(auto pos:order) append_value(values,sources[pos.table][pos.row]*factors[pos.table]);
        } else {
            for(auto pos:order) append_value(values,sources[pos.table][pos.row]);
        }
        result.columns.push_back(std::move(out));
    }
    return result;
}

QueryResult sql_result(SqlReader& reader, Database& index, const std::vector<std::string>& symbols,
                       int64_t start, int64_t end, std::vector<std::string> names) {
    const auto& group=reader.group;
    std::set<std::string> fields{group.time};
    const bool need_symbols=!group.symbol.empty() && std::find(names.begin(),names.end(),"symbol")!=names.end();
    std::map<int64_t,std::string> symbol_names;
    if(need_symbols) {
        fields.insert(group.symbol);
        Statement q(index,"SELECT id,name FROM __symbols__");
        while(q.next()) symbol_names.emplace(q.integer(0),q.text(1));
    }
    for(const auto& name:names) {
        if(name=="symbol" && need_symbols) continue;
        const auto& spec=group.schema.at(name);
        fields.insert(spec.value("column",name));
        const auto scale_column=spec.value("scale_column",std::string{});
        if(!scale_column.empty()) fields.insert(scale_column);
    }
    const auto batch=reader.read(start,end,symbols,{fields.begin(),fields.end()});
    std::map<std::string,size_t> positions;
    for(size_t i=0;i<batch.columns.size();++i) positions[batch.columns[i]]=i;
    QueryResult result{batch.rows.size(),std::move(names),{}};
    for(const auto& name:result.names) {
        const auto& spec=group.schema.at(name);
        auto scale=spec.value("scale",0);
        const bool symbol=name=="symbol" && need_symbols;
        size_t pos=0, scale_pos=0;
        const auto scale_column=spec.value("scale_column",std::string{});
        if (!batch.rows.empty()) {
            pos=positions.at(symbol?group.symbol:spec.value("column",name));
            if(!scale_column.empty()) {
                scale_pos=positions.at(scale_column);
                int highest=-1;
                for(const auto& row:batch.rows)
                    if(!row[pos].is_null()) highest=std::max(highest,row[scale_pos].get<int>());
                if(highest>=0) scale=highest;
            }
        }
        auto out=column(spec.at("type").get<std::string>(),scale,result.rows);
        for(size_t i=0;i<batch.rows.size();++i) {
            const auto& row=batch.rows[i];
            if(symbol) {
                const auto& s=symbol_names.at(row[pos].get<int64_t>());
                append_string(out,s.data(),s.size());
            } else {
                const auto factor=scale_column.empty()||row[pos].is_null()?1:power10(scale-row[scale_pos].get<int>());
                append_sql_value(out,i,row[pos],factor);
            }
        }
        result.columns.push_back(std::move(out));
    }
    return result;
}
}

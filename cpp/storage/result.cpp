#include "result.hpp"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <map>
#include <queue>
#include <set>
#include <unordered_map>

namespace mktpack::storage {
namespace {
constexpr double kPow10[23] = {
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11,
    1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22
};

template<class T> void append_value(EmptyBuffer& buffer, T value) {
    buffer.append(&value, sizeof(T));
}

std::string decimal_as_real(const std::string& schema_type) {
    return schema_type == "decimal" ? "real" : schema_type;
}

int union_child(const Json& value) {
    if (value.is_null()) return 0;
    if (value.is_number_integer() || value.is_number_unsigned()) return 1;
    if (value.is_number_float()) return 2;
    return value.is_binary() ? 4 : 3;
}

void append_sql_value(ResultColumn& out, size_t row, const Json& value, int scale = 0) {
    if (out.type == "any") {
        const auto code = union_child(value);
        auto& child = out.children[code];
        if (child.rows > INT32_MAX) throw std::runtime_error("Arrow union 子列偏移溢出");
        out.buffers[1].push_back(static_cast<uint8_t>(code));
        append_value(out.buffers[2], static_cast<int32_t>(child.rows));
        const auto pos = child.rows++;
        append_sql_value(child, pos, value);
        return;
    }
    if (out.type == "null") return;
    auto& validity = out.buffers[0];
    if (value.is_null()) {
        if (validity.empty()) validity.resize((out.rows + 7) / 8, 0xff);
        validity[row / 8] &= uint8_t(~(1u << (row % 8)));
    }
    if (out.type == "string") {
        if (value.is_null()) append_string(out, nullptr, 0);
        else {
            const auto& text = value.get_ref<const std::string&>();
            append_string(out, text.data(), text.size());
        }
    } else if (out.type == "blob") {
        if (value.is_null()) append_string(out, nullptr, 0);
        else {
            const auto& bytes = value.get_binary();
            append_string(out, bytes.data(), bytes.size());
        }
    } else if (out.type == "real") {
        double v = 0.0;
        if (!value.is_null()) {
            if (value.is_number_float()) v = value.get<double>();
            else {
                const auto raw = value.get<int64_t>();
                v = scale > 0 ? static_cast<double>(raw) / scale_divisor(scale)
                  : scale < 0 ? static_cast<double>(raw) * scale_divisor(-scale)
                  : static_cast<double>(raw);
            }
        }
        append_value(out.buffers[1], v);
    } else {
        append_value(out.buffers[1], value.is_null() ? int64_t(0) : value.get<int64_t>());
    }
}

struct Position { uint32_t table, row; };

// 仅在同一合约内部出现时间重叠块时按时间稳定归并。
std::vector<Position> ordered_span(const std::vector<Table>& tables, size_t begin, size_t end, size_t span_rows) {
    bool sorted = true;
    int64_t last = INT64_MIN;
    for (size_t i = begin; i < end; ++i) {
        const auto& time = tables[i].columns.at("time");
        if (time.empty()) continue;
        sorted &= time.front() >= last;
        last = time.back();
    }
    if (sorted) return {};
    std::vector<const std::vector<int64_t>*> clocks;
    clocks.reserve(end - begin);
    for (size_t i = begin; i < end; ++i) clocks.push_back(&tables[i].columns.at("time"));
    const auto later = [&](Position a, Position b) {
        const auto ta = (*clocks[a.table - begin])[a.row], tb = (*clocks[b.table - begin])[b.row];
        return ta == tb ? a.table > b.table : ta > tb;
    };
    std::priority_queue<Position, std::vector<Position>, decltype(later)> next(later);
    for (size_t i = begin; i < end; ++i)
        if (!clocks[i - begin]->empty()) next.push({static_cast<uint32_t>(i), 0});
    std::vector<Position> order;
    order.reserve(span_rows);
    while (!next.empty()) {
        auto pos = next.top();
        next.pop();
        const auto length = static_cast<uint32_t>(clocks[pos.table - begin]->size());
        do {
            order.push_back(pos);
            ++pos.row;
        } while (pos.row < length && (next.empty() || !later(pos, next.top())));
        if (pos.row < length) next.push(pos);
    }
    return order;
}
}

double scale_divisor(int scale) {
    if (scale >= 0 && scale <= 22) return kPow10[scale];
    return std::pow(10.0, scale);
}

ResultColumn make_column(const std::string& type, size_t rows, bool preallocate_fixed) {
    const auto kind = decimal_as_real(type);
    ResultColumn out{kind, 0, rows, {}, {}};
    auto& buffers = out.buffers;
    if (kind == "null") {
        buffers.resize(1);
    } else if (kind == "dictionary") {
        buffers.resize(2);
        if (preallocate_fixed) buffers[1].resize_for_overwrite(rows * sizeof(int32_t));
        else buffers[1].reserve(rows * sizeof(int32_t));
        out.children.push_back(make_column("string", 0, false));
    } else if (kind == "any") {
        buffers.resize(3);
        buffers[1].reserve(rows);
        buffers[2].reserve(rows * sizeof(int32_t));
        for (auto name : {"null", "integer", "real", "string", "blob"})
            out.children.push_back(make_column(name, 0, false));
    } else if (kind == "string" || kind == "blob") {
        buffers.resize(3);
        buffers[1].reserve((rows + 1) * sizeof(int32_t));
        append_value(buffers[1], int32_t(0));
    } else {
        buffers.resize(2);
        if (preallocate_fixed) buffers[1].resize_for_overwrite(rows * 8);
        else buffers[1].reserve(rows * 8);
    }
    return out;
}

void append_string(ResultColumn& out, const void* data, size_t length) {
    auto& bytes = out.buffers[2];
    if (length > size_t(INT32_MAX) - bytes.size()) throw std::runtime_error("Arrow 字符串数据超过 2 GiB");
    if (length) bytes.append(data, length);
    append_value(out.buffers[1], static_cast<int32_t>(bytes.size()));
}

QueryResult mkp_result(std::vector<Table> tables, const Json& empty_schema,
                       std::vector<std::string> names) {
    // 确保按 (symbol, time) 稳定分块排列
    std::stable_sort(tables.begin(), tables.end(), [](const Table& a, const Table& b) {
        return a.metadata.value("symbol", "") < b.metadata.value("symbol", "");
    });

    QueryResult result{0, std::move(names), {}};
    for (const auto& table : tables) result.rows += table.columns.at("time").size();

    struct Span {
        size_t begin, end, rows;
        std::string symbol;
        std::vector<Position> order;
    };
    std::vector<Span> spans;
    for (size_t i = 0; i < tables.size();) {
        const std::string sym = tables[i].metadata.value("symbol", "");
        size_t j = i;
        size_t span_rows = 0;
        for (; j < tables.size() && tables[j].metadata.value("symbol", "") == sym; ++j)
            span_rows += tables[j].columns.at("time").size();
        if (span_rows > 0) {
            auto order = ordered_span(tables, i, j, span_rows);
            spans.push_back({i, j, span_rows, sym, std::move(order)});
        }
        i = j;
    }

    for (const auto& name : result.names) {
        if (name == "symbol") {
            auto out = make_column("dictionary", result.rows, true);
            auto& dict = out.children[0];
            auto* indices = reinterpret_cast<int32_t*>(out.buffers[1].data());
            size_t offset = 0;
            for (const auto& span : spans) {
                const auto code = static_cast<int32_t>(dict.rows++);
                append_string(dict, span.symbol.data(), span.symbol.size());
                std::fill_n(indices + offset, span.rows, code);
                offset += span.rows;
            }
            result.columns.push_back(std::move(out));
            continue;
        }
        const auto& spec = tables.empty() ? empty_schema.at(name) : tables.front().schema.at(name);
        auto kind = decimal_as_real(spec.at("type").get<std::string>());
        for (const auto& t : tables) {
            if (decimal_as_real(t.schema.at(name).at("type").get<std::string>()) == "real") kind = "real";
        }
        auto out = make_column(kind, result.rows, true);
        uint8_t* raw_dst = out.buffers[1].data();
        std::vector<const int64_t*> sources(tables.size());
        std::vector<int> scales(tables.size(), 0);
        for (size_t i = 0; i < tables.size(); ++i) {
            sources[i] = tables[i].columns.at(name).data();
            scales[i] = tables[i].schema.at(name).value("scale", 0);
        }
        size_t offset = 0;
        for (const auto& span : spans) {
            if (span.order.empty()) {
                for (size_t i = span.begin; i < span.end; ++i) {
                    const size_t n = tables[i].columns.at(name).size();
                    if (!n) continue;
                    const int64_t* src = sources[i];
                    if (kind == "real") {
                        double* dst = reinterpret_cast<double*>(raw_dst) + offset;
                        const int sc = scales[i];
                        if (sc == 0) {
                            for (size_t r = 0; r < n; ++r) dst[r] = static_cast<double>(src[r]);
                        } else {
                            const double div = scale_divisor(sc);
                            for (size_t r = 0; r < n; ++r) dst[r] = static_cast<double>(src[r]) / div;
                        }
                    } else {
                        std::memcpy(raw_dst + offset * sizeof(int64_t), src, n * sizeof(int64_t));
                    }
                    offset += n;
                }
            } else if (kind == "real") {
                double* dst = reinterpret_cast<double*>(raw_dst) + offset;
                for (size_t r = 0; r < span.order.size(); ++r) {
                    const auto pos = span.order[r];
                    const int sc = scales[pos.table];
                    const auto v = sources[pos.table][pos.row];
                    dst[r] = sc == 0 ? static_cast<double>(v) : static_cast<double>(v) / scale_divisor(sc);
                }
                offset += span.order.size();
            } else {
                int64_t* dst = reinterpret_cast<int64_t*>(raw_dst) + offset;
                for (size_t r = 0; r < span.order.size(); ++r) {
                    const auto pos = span.order[r];
                    dst[r] = sources[pos.table][pos.row];
                }
                offset += span.order.size();
            }
        }
        result.columns.push_back(std::move(out));
    }
    return result;
}

QueryResult sql_result(SqlReader& reader, Database& index, const std::vector<std::string>& symbols,
                       int64_t start, int64_t end, std::vector<std::string> names) {
    const auto& group = reader.group;
    std::set<std::string> fields{group.time};
    const bool has_symbol_col = !group.symbol.empty();
    std::unordered_map<int64_t, std::string> symbol_names;
    if (has_symbol_col) {
        fields.insert(group.symbol);
        Statement q(index, "SELECT id,name FROM __symbols__");
        while (q.next()) symbol_names.emplace(q.integer(0), q.text(1));
    }
    for (const auto& name : names) {
        if (name == "symbol" && has_symbol_col) continue;
        const auto& spec = group.schema.at(name);
        fields.insert(spec.value("column", name));
        const auto scale_column = spec.value("scale_column", std::string{});
        if (!scale_column.empty()) fields.insert(scale_column);
    }
    auto batch = reader.read(start, end, symbols, {fields.begin(), fields.end()});
    std::map<std::string, size_t> positions;
    for (size_t i = 0; i < batch.columns.size(); ++i) positions[batch.columns[i]] = i;

    if (has_symbol_col && !batch.rows.empty()) {
        const size_t sym_pos = positions.at(group.symbol);
        const size_t time_pos = positions.at(group.time);
        std::stable_sort(batch.rows.begin(), batch.rows.end(),
                         [&](const std::vector<Json>& a, const std::vector<Json>& b) {
            const std::string empty;
            const auto& sa = a[sym_pos].is_null() ? empty : symbol_names.at(a[sym_pos].get<int64_t>());
            const auto& sb = b[sym_pos].is_null() ? empty : symbol_names.at(b[sym_pos].get<int64_t>());
            if (sa != sb) return sa < sb;
            return a[time_pos].get<int64_t>() < b[time_pos].get<int64_t>();
        });
    }

    QueryResult result{batch.rows.size(), std::move(names), {}};
    for (const auto& name : result.names) {
        const bool is_symbol = name == "symbol" && has_symbol_col;
        if (is_symbol) {
            auto out = make_column("dictionary", result.rows, true);
            auto& dict = out.children[0];
            auto& validity = out.buffers[0];
            auto* indices = reinterpret_cast<int32_t*>(out.buffers[1].data());
            const size_t pos = batch.rows.empty() ? 0 : positions.at(group.symbol);
            std::unordered_map<int64_t, int32_t> dict_codes;
            for (size_t i = 0; i < batch.rows.size(); ++i) {
                const auto& cell = batch.rows[i][pos];
                if (cell.is_null()) {
                    if (validity.empty()) validity.resize((result.rows + 7) / 8, 0xff);
                    validity[i / 8] &= uint8_t(~(1u << (i % 8)));
                    indices[i] = 0;
                    continue;
                }
                const int64_t sid = cell.get<int64_t>();
                auto [it, inserted] = dict_codes.try_emplace(sid, static_cast<int32_t>(dict.rows));
                if (inserted) {
                    const auto& text = symbol_names.at(sid);
                    ++dict.rows;
                    append_string(dict, text.data(), text.size());
                }
                indices[i] = it->second;
            }
            result.columns.push_back(std::move(out));
            continue;
        }
        const auto& spec = group.schema.at(name);
        const int fixed_scale = spec.value("scale", 0);
        const auto scale_column = spec.value("scale_column", std::string{});
        const size_t pos = batch.rows.empty() ? 0 : positions.at(spec.value("column", name));
        const size_t scale_pos = (batch.rows.empty() || scale_column.empty()) ? 0 : positions.at(scale_column);
        auto out = make_column(spec.at("type").get<std::string>(), result.rows, false);
        for (size_t i = 0; i < batch.rows.size(); ++i) {
            const auto& row = batch.rows[i];
            const int row_scale = (scale_column.empty() || row[pos].is_null()) ? fixed_scale : row[scale_pos].get<int>();
            append_sql_value(out, i, row[pos], row_scale);
        }
        result.columns.push_back(std::move(out));
    }
    return result;
}
}

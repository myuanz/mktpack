#include "result.hpp"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <map>
#include <set>
#include <unordered_map>

namespace mktpack::storage {
namespace {
constexpr double kPow10[23] = {
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11,
    1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22
};

double scale_divisor(int scale) {
    if (scale >= 0 && scale <= 22) return kPow10[scale];
    return std::pow(10.0, scale);
}

template<class T> void append_value(EmptyBuffer& buffer, T value) {
    buffer.append(&value, sizeof(T));
}

std::string decimal_as_real(const std::string& schema_type) {
    return schema_type == "decimal" ? "real" : schema_type;
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
}

MkpOutput init_mkp_output(const Json& schema, std::vector<std::string> names, size_t total_rows) {
    MkpOutput out{{total_rows, std::move(names), {}}, {}, {}};
    out.result.columns.reserve(out.result.names.size());
    std::map<std::string, std::vector<TargetSlot>> target_map;
    for (size_t c = 0; c < out.result.names.size(); ++c) {
        const auto& name = out.result.names[c];
        if (name == "symbol") {
            out.symbol_columns.push_back(c);
            out.result.columns.push_back(make_column("dictionary", total_rows, true));
            continue;
        }
        const auto kind = decimal_as_real(schema.at(name).at("type").get<std::string>());
        out.result.columns.push_back(make_column(kind, total_rows, true));
        target_map[name].push_back({c, kind == "real"});
    }
    out.targets.assign(target_map.begin(), target_map.end());
    return out;
}

void MkpOutput::set_symbols(std::span<const std::pair<std::string, size_t>> symbol_runs) {
    if (symbol_columns.empty() || !result.rows) return;
    auto& primary = result.columns[symbol_columns[0]];
    auto& dict = primary.children[0];
    auto* indices = reinterpret_cast<int32_t*>(primary.buffers[1].data());
    size_t offset = 0;
    for (const auto& [sym, count] : symbol_runs) {
        if (!count) continue;
        const auto code = static_cast<int32_t>(dict.rows++);
        append_string(dict, sym.data(), sym.size());
        std::fill_n(indices + offset, count, code);
        offset += count;
    }
    for (size_t k = 1; k < symbol_columns.size(); ++k) {
        auto& copy = result.columns[symbol_columns[k]];
        std::memcpy(copy.buffers[1].data(), primary.buffers[1].data(), primary.buffers[1].size());
        auto& dst_dict = copy.children[0];
        dst_dict.rows = dict.rows;
        dst_dict.buffers[1].resize_for_overwrite(dict.buffers[1].size());
        if (!dict.buffers[1].empty())
            std::memcpy(dst_dict.buffers[1].data(), dict.buffers[1].data(), dict.buffers[1].size());
        dst_dict.buffers[2].resize_for_overwrite(dict.buffers[2].size());
        if (!dict.buffers[2].empty())
            std::memcpy(dst_dict.buffers[2].data(), dict.buffers[2].data(), dict.buffers[2].size());
    }
}

void MkpOutput::write_slice(const Table& table, size_t slice_first, size_t slice_rows, size_t dst_offset) {
    if (!slice_rows) return;
    for (const auto& [name, slots] : targets) {
        const int64_t* src = table.columns.at(name).data() + slice_first;
        const int sc = table.schema.at(name).value("scale", 0);
        for (size_t s = 0; s < slots.size(); ++s) {
            const auto& slot = slots[s];
            uint8_t* raw_dst = result.columns[slot.column_index].buffers[1].data();
            if (s > 0 && slot.as_real == slots[0].as_real) {
                const uint8_t* first_dst = result.columns[slots[0].column_index].buffers[1].data();
                std::memcpy(raw_dst + dst_offset * 8, first_dst + dst_offset * 8, slice_rows * 8);
                continue;
            }
            if (slot.as_real) {
                double* dst = reinterpret_cast<double*>(raw_dst) + dst_offset;
                if (sc == 0) {
                    for (size_t r = 0; r < slice_rows; ++r) dst[r] = static_cast<double>(src[r]);
                } else {
                    const double div = scale_divisor(sc);
                    for (size_t r = 0; r < slice_rows; ++r) dst[r] = static_cast<double>(src[r]) / div;
                }
            } else if (sc == 0) {
                std::memcpy(raw_dst + dst_offset * sizeof(int64_t), src, slice_rows * sizeof(int64_t));
            } else {
                int64_t* dst = reinterpret_cast<int64_t*>(raw_dst) + dst_offset;
                for (size_t r = 0; r < slice_rows; ++r) dst[r] = rescale(src[r], sc, 0);
            }
        }
    }
}

QueryResult single_file_result(const Table& table, const Json& schema, std::vector<std::string> names) {
    const size_t rows = table.columns.at("time").size();
    auto out = init_mkp_output(schema, std::move(names), rows);
    if (rows) {
        const std::pair<std::string, size_t> run{table.metadata.value("symbol", ""), rows};
        out.set_symbols(std::span(&run, 1));
        out.write_slice(table, 0, rows, 0);
    }
    return std::move(out.result);
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

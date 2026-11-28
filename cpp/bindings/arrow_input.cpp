#include "arrow_input.hpp"
#include <mktpack/decimal.hpp>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstring>
#include <optional>

namespace mktpack::bindings {
namespace nb = nanobind;
namespace {

// Py_buffer 持有 Arrow 缓冲区。偏移以元素为单位，切片不必重新分配。
class Buffer {
    Py_buffer view_{};
public:
    explicit Buffer(nb::handle buffer) {
        if (PyObject_GetBuffer(buffer.ptr(), &view_, PyBUF_SIMPLE) < 0)
            throw nb::python_error();
    }
    Buffer(const Buffer&) = delete;
    ~Buffer() { PyBuffer_Release(&view_); }
    const char* data() const { return static_cast<const char*>(view_.buf); }

    template<class T> T get(size_t index) const {
        T value;
        std::memcpy(&value, data() + index * sizeof(T), sizeof(T));
        return value;
    }
};

using Values = std::vector<int64_t>;

template<class T>
void append_coefficients(Values& out, const Buffer& data, size_t offset, size_t length,
                         int from, int to) {
    if constexpr (std::is_same_v<T, int64_t>) {
        if (from == to) {
            const auto start = out.size();
            out.resize(start + length);
            std::memcpy(out.data() + start, data.data() + offset * sizeof(T), length * sizeof(T));
            return;
        }
    }
    for (size_t i = offset; i < offset + length; ++i)
        out.push_back(rescale(data.get<T>(i), from, to));
}

std::string float_text(double value) {
    char text[64];
    auto [end, status] = std::to_chars(text, text + sizeof(text), value, std::chars_format::general);
    if (status != std::errc{}) throw std::invalid_argument("浮点数转换失败");
    return {text, end};
}

void append_literal(Values& out, std::string text, std::optional<int> requested, int& scale,
                    int minimum_scale = 0) {
    int literal_scale;
    const auto value = decimal_integer(std::move(text), &literal_scale);
    const int next = requested.value_or(std::max({scale, literal_scale, minimum_scale}));
    if (next > 18 || next < 0) throw std::invalid_argument("小数位数必须在 0 至 18 之间");
    if (next != scale) {
        for (auto& previous : out) previous = rescale(previous, scale, next);
        scale = next;
    }
    out.push_back(rescale(value.value, value.decimals, scale));
}

Json import_column(const nb::module_& pa, const nb::object& types, nb::object column,
                   std::optional<int> requested, Values& out) {
    nb::object type = column.attr("type");
    auto is = [&](const char* test) { return nb::cast<bool>(types.attr(test)(type)); };
    const bool dictionary = is("is_dictionary");
    if (dictionary) {
        type = type.attr("value_type");
        column = column.attr("cast")(type);
    }
    if (nb::cast<int64_t>(column.attr("null_count")))
        throw std::invalid_argument("时序数值列不接受 null");

    std::string kind = "real";
    int bits = 64, from = 0;
    bool floating = false, text = false;
    if (is("is_timestamp")) {
        kind = "timestamp";
        column = column.attr("cast")(pa.attr("timestamp")("ms","UTC"));
    } else if (is("is_integer")) {
        kind = dictionary ? "real" : "integer";
        column = column.attr("cast")(pa.attr("int64")());
    } else if (is("is_decimal")) {
        bits = nb::cast<int>(type.attr("bit_width"));
        from = nb::cast<int>(type.attr("scale"));
        if (bits == 256) {
            // 先以 Arrow 的 256 位运算精确降尺度，再收窄到 MKP 系数。
            from = requested.value_or(std::max(0, from));
            column = column.attr("cast")(pa.attr("decimal128")(19, from));
            bits = 128;
        }
    } else if (is("is_floating")) {
        floating = true;
        column = column.attr("cast")(pa.attr("float64")());
    } else if (is("is_string") || is("is_large_string") || is("is_string_view") || is("is_null")) {
        text = true;
        column = column.attr("cast")(pa.attr("large_string")());
        column = nb::module_::import_("pyarrow.compute").attr("utf8_trim_whitespace")(column);
    } else {
        throw std::invalid_argument("不支持的时序列类型：" + nb::cast<std::string>(nb::str(type)));
    }

    int scale = requested.value_or(std::max(0, from));
    out.reserve(nb::len(column));
    for (nb::handle chunk : column.attr("chunks")) {
        const size_t length = nb::len(chunk);
        if (!length) continue;
        const size_t offset = nb::cast<size_t>(chunk.attr("offset"));
        nb::object buffers = chunk.attr("buffers")();
        Buffer data(buffers[1]);
        if (text) {
            Buffer chars(buffers[2]);
            nb::gil_scoped_release release;
            for (size_t i = offset; i < offset + length; ++i) {
                const auto start = data.get<int64_t>(i), end = data.get<int64_t>(i+1);
                std::string literal(chars.data()+start, end-start);
                std::erase(literal, '_');
                append_literal(out, std::move(literal), requested, scale);
            }
        } else if (floating) {
            nb::gil_scoped_release release;
            for (size_t i = offset; i < offset + length; ++i) {
                const double v = data.get<double>(i);
                // 浮点列沿用 binary64 最短十进制表示，普通整数浮点值保留一位小数。
                const int minimum = std::trunc(v)==v && std::abs(v)<1e16 ? 1 : 0;
                append_literal(out, float_text(v), requested, scale, minimum);
            }
        } else {
            nb::gil_scoped_release release;
            switch (bits) {
            case 32: append_coefficients<int32_t>(out,data,offset,length,from,scale); break;
            case 64: append_coefficients<int64_t>(out,data,offset,length,from,scale); break;
            case 128: append_coefficients<__int128>(out,data,offset,length,from,scale); break;
            }
        }
    }
    return {{"type",kind},{"scale",scale}};
}

void import_symbol(const nb::object& column, Json& metadata) {
    auto symbols = column.attr("unique")();
    if (nb::len(symbols) > 1) throw std::invalid_argument("单文件只能保存一个 symbol");
    if (nb::len(symbols) == 1) metadata["symbol"] = nb::cast<std::string>(symbols[0].attr("as_py")());
}
}

Table table_from_arrow(nb::handle table, Json schema, Json metadata) {
    const auto pa = nb::module_::import_("pyarrow");
    const auto types = pa.attr("types");
    const auto names = nb::cast<std::vector<std::string>>(table.attr("column_names"));
    Table result;
    result.metadata = std::move(metadata);
    for (size_t i = 0; i < names.size(); ++i) {
        const auto& name = names[i];
        nb::object column = table.attr("column")(i);
        if (name == "symbol") {
            import_symbol(column, result.metadata);
            continue;
        }
        if (result.columns.contains(name)) throw std::invalid_argument("重复列名：" + name);
        const std::optional<int> target = schema.empty() ? std::nullopt :
            std::optional<int>(schema.at(name).value("scale",0));
        auto inferred = import_column(pa, types, std::move(column), target, result.columns[name]);
        result.schema[name] = std::move(inferred);
    }
    if (!schema.empty()) {
        for (auto& [name, spec] : schema.items()) {
            if (!result.columns.contains(name)) throw nb::key_error(name.c_str());
            if (spec.contains("type") && spec["type"] == "decimal") spec["type"] = "real";
        }
        result.schema = std::move(schema);
    }
    return result;
}
}

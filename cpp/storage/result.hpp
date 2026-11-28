#pragma once
#include "sqlite.hpp"

namespace mktpack::storage {
struct TargetSlot {
    size_t column_index;
    bool as_real;
};

struct MkpOutput {
    QueryResult result;
    std::vector<size_t> symbol_columns;
    std::vector<std::pair<std::string, std::vector<TargetSlot>>> targets;

    void set_symbols(std::span<const std::pair<std::string, size_t>> symbol_runs);
    void write_slice(const Table& table, size_t slice_first, size_t slice_rows, size_t dst_offset);
};

MkpOutput init_mkp_output(const Json& schema, std::vector<std::string> names, size_t total_rows);
QueryResult single_file_result(const Table& table, const Json& schema, std::vector<std::string> names);
QueryResult sql_result(SqlReader& reader, Database& index, const std::vector<std::string>& symbols,
                       int64_t start, int64_t end, std::vector<std::string> names);
}

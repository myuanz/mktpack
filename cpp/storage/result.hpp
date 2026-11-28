#pragma once
#include "sqlite.hpp"

namespace mktpack::storage {
double scale_divisor(int scale);
ResultColumn make_column(const std::string& type, size_t rows, bool zero_init = false);
void append_string(ResultColumn& out, const void* data, size_t length);

QueryResult mkp_result(std::vector<Table> tables, const Json& empty_schema,
                       std::vector<std::string> names);
QueryResult sql_result(SqlReader& reader, Database& index, const std::vector<std::string>& symbols,
                       int64_t start, int64_t end, std::vector<std::string> names);
}

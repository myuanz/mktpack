#pragma once
#include "sqlite.hpp"

namespace mktpack::storage {
QueryResult mkp_result(std::vector<Table> tables, const Json& empty_schema,
                       std::vector<std::string> names);
QueryResult sql_result(SqlReader& reader, Database& index, const std::vector<std::string>& symbols,
                       int64_t start, int64_t end, std::vector<std::string> names);
}

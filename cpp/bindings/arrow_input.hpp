#pragma once
#include <mktpack/core.hpp>
#include <nanobind/nanobind.h>

namespace mktpack::bindings {
Table table_from_arrow(nanobind::handle table, Json schema, Json metadata);
}

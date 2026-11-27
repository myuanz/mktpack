#pragma once
#include <mktpack/storage.hpp>
#include <nanobind/nanobind.h>

namespace mktpack::bindings {
nanobind::object arrow_table(QueryResult result);
}

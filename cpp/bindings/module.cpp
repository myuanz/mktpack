#include <mktpack/core.hpp>
#include <mktpack/storage.hpp>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/map.h>

namespace nb=nanobind;
NB_MODULE(_native, module) {
    module.def("describe", [](const std::string& root, const std::string& group) {
        return mktpack::describe(root,group).dump();
    });
    module.def("ordinary", [](const std::string& root, const std::string& group) {
        return mktpack::ordinary(root,group).dump();
    });
    module.def("query", [](const std::string& root, const std::string& group,
                           const std::vector<std::string>& symbols, int64_t start, int64_t end) {
        std::vector<mktpack::Table> tables;
        {
            nb::gil_scoped_release release;
            tables=mktpack::query(root,group,symbols,start,end);
        }
        nb::list result;
        for (const auto& table:tables) {
            result.append(nb::make_tuple(table.columns,table.schema.dump(),table.metadata.dump()));
        }
        return result;
    });
    module.def("write", [](const std::string& path, const mktpack::Columns& columns,
                           const std::string& schema, const std::string& metadata) {
        mktpack::Table table{columns,mktpack::Json::parse(schema),mktpack::Json::parse(metadata)};
        nb::gil_scoped_release release;
        mktpack::write(path,table);
    });
    module.def("read", [](const std::string& path) {
        mktpack::Table table;
        {
            nb::gil_scoped_release release;
            table=mktpack::read(path);
        }
        return nb::make_tuple(table.columns,table.schema.dump(),table.metadata.dump());
    });
    module.def("info", [](const std::string& path) { return mktpack::info(path).dump(); });
    module.def("merge", [](const std::vector<std::string>& paths, const std::string& destination) {
        nb::gil_scoped_release release;
        std::vector<mktpack::Table> tables;
        for (const auto& path:paths) tables.push_back(mktpack::read(path));
        mktpack::write(destination,mktpack::merge(tables));
    });
}

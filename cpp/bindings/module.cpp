#include <mktpack/core.hpp>
#include <mktpack/storage.hpp>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/map.h>

namespace nb=nanobind;
nb::object python_value(const mktpack::Json& value) {
    if(value.is_null()) return nb::none();
    if(value.is_boolean()) return nb::bool_(value.get<bool>());
    if(value.is_number_integer()) return nb::int_(value.get<int64_t>());
    if(value.is_number_float()) return nb::float_(value.get<double>());
    if(value.is_string()) return nb::str(value.get_ref<const std::string&>().data(),value.get_ref<const std::string&>().size());
    if(value.is_binary()) {const auto& b=value.get_binary();return nb::bytes(reinterpret_cast<const char*>(b.data()),b.size());}
    if(value.is_array()) { nb::list result;for(const auto& item:value) result.append(python_value(item));return result; }
    nb::dict result;for(const auto& [key,item]:value.items()) result[nb::str(key.c_str())]=python_value(item);return result;
}
NB_MODULE(_native, module) {
    module.def("describe", [](const std::string& root, const std::string& group) {
        return mktpack::describe(root,group).dump();
    });
    module.def("ordinary", [](const std::string& root, const std::string& group) {
        return python_value(mktpack::ordinary(root,group));
    });
    module.def("query", [](const std::string& root, const std::string& group,
                           const std::vector<std::string>& symbols, int64_t start, int64_t end) {
        std::vector<mktpack::ReadTable> tables;
        {
            nb::gil_scoped_release release;
            tables=mktpack::query(root,group,symbols,start,end);
        }
        nb::list result;
        for (const auto& table:tables) {
            result.append(nb::make_tuple(python_value(table.columns),table.schema.dump(),table.metadata.dump()));
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

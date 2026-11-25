#include <mktpack/core.hpp>
#include <mktpack/storage.hpp>
#include "arrow_input.hpp"
#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

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
namespace {
nb::object buffer(std::vector<uint8_t> values) {
    auto data=std::make_unique<std::vector<uint8_t>>(std::move(values));
    auto* ptr=data.get();
    nb::capsule owner(ptr,[](void* p) noexcept { delete static_cast<std::vector<uint8_t>*>(p); });
    data.release();
    // 让零长数组也有可导出的地址。
    static const uint8_t empty=0;
    return nb::cast(nb::ndarray<nb::numpy,const uint8_t>(ptr->empty()?&empty:ptr->data(),
                                                          {ptr->size()}, owner));
}

nb::tuple arrow_column(mktpack::ResultColumn column) {
    nb::list buffers, children;
    for(size_t i=0;i<column.buffers.size();++i) {
        if(i==0 && column.buffers[i].empty()) buffers.append(nb::none());
        else buffers.append(buffer(std::move(column.buffers[i])));
    }
    for(auto& child:column.children) children.append(arrow_column(std::move(child)));
    return nb::make_tuple(column.type,column.scale,column.rows,buffers,children);
}

nb::tuple arrow_table(mktpack::QueryResult result) {
    nb::list columns;
    for(auto& c:result.columns) columns.append(arrow_column(std::move(c)));
    return nb::make_tuple(result.rows,result.names,columns);
}
}

NB_MODULE(_native, module) {
    module.def("describe", [](const std::string& root, const std::string& group) {
        return mktpack::describe(root,group).dump();
    });
    module.def("ordinary", [](const std::string& root, const std::string& group) {
        return python_value(mktpack::ordinary(root,group));
    });
    module.def("query", [](const std::string& root, const std::string& group,
                           const std::vector<std::string>& symbols, int64_t start, int64_t end,
                           const mktpack::Projection& columns) {
        mktpack::QueryResult result;
        try {
            nb::gil_scoped_release release;
            result=mktpack::query(root,group,symbols,start,end,columns);
        } catch(const std::out_of_range& e) {
            throw nb::key_error(e.what());
        }
        return arrow_table(std::move(result));
    });
    module.def("write", [](const std::string& path, nb::handle source, const std::string& schema,
                           const std::string& metadata, bool lossy_taker) {
        auto table=mktpack::bindings::table_from_arrow(source,mktpack::Json::parse(schema),mktpack::Json::parse(metadata));
        if (lossy_taker) {
            try {
                mktpack::set_taker_encoding(table.schema,true);
            } catch (const mktpack::Json::out_of_range& e) {
                throw nb::key_error(e.what());
            }
        }
        nb::gil_scoped_release release;
        mktpack::write(path,table);
    });
    module.def("read", [](const std::string& path) {
        mktpack::QueryResult result;
        {
            nb::gil_scoped_release release;
            result=mktpack::file_result(path);
        }
        return arrow_table(std::move(result));
    });
    module.def("info", [](const std::string& path) { return mktpack::info(path).dump(); });
    module.def("merge", [](const std::vector<std::string>& paths, const std::string& destination) {
        nb::gil_scoped_release release;
        mktpack::merge_files({paths.begin(),paths.end()}, destination);
    });
}

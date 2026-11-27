#include "arrow_output.hpp"
#include <nanobind/ndarray.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <memory>

namespace mktpack::bindings {
namespace nb=nanobind;
namespace {
nb::object arrow_buffer(const nb::module_& pa,std::vector<uint8_t> values) {
    auto data=std::make_unique<std::vector<uint8_t>>(std::move(values));
    auto* ptr=data.get();
    nb::capsule owner(ptr,[](void* p) noexcept {delete static_cast<std::vector<uint8_t>*>(p);});
    data.release();
    static const uint8_t empty=0;
    auto array=nb::cast(nb::ndarray<nb::numpy,const uint8_t>(ptr->empty()?&empty:ptr->data(),
                                                            {ptr->size()},owner));
    return pa.attr("py_buffer")(array);
}

nb::object arrow_array(const nb::module_& pa,const nb::dict& types,ResultColumn column) {
    auto type=column.type=="decimal"?pa.attr("decimal128")(38,column.scale)
                                    :nb::borrow<nb::object>(types[nb::str(column.type.c_str())]);
    nb::list buffers,children;
    for(size_t i=0;i<column.buffers.size();++i) {
        if(i==0 && column.buffers[i].empty())buffers.append(nb::none());
        else buffers.append(arrow_buffer(pa,std::move(column.buffers[i])));
    }
    for(auto& child:column.children)children.append(arrow_array(pa,types,std::move(child)));
    return pa.attr("Array").attr("from_buffers")(type,column.rows,buffers,nb::arg("children")=children);
}
}

nb::object arrow_table(QueryResult result) {
    const auto pa=nb::module_::import_("pyarrow");
    if(result.names.empty()) {
        nb::list fields;
        auto array=pa.attr("Array").attr("from_buffers")(pa.attr("struct")(fields),result.rows,
                                                       nb::make_tuple(nb::none()));
        auto batch=pa.attr("RecordBatch").attr("from_struct_array")(array);
        return pa.attr("Table").attr("from_batches")(nb::make_tuple(batch));
    }

    nb::dict types;
    types["timestamp"]=pa.attr("timestamp")("ms","UTC");
    types["integer"]=pa.attr("int64")();
    types["real"]=pa.attr("float64")();
    types["string"]=pa.attr("string")();
    types["blob"]=pa.attr("binary")();
    types["null"]=pa.attr("null")();
    nb::list fields;
    for(const auto* name:{"null","integer","real","string","blob"})
        fields.append(pa.attr("field")(name,types[name]));
    types["any"]=pa.attr("dense_union")(fields);

    nb::list columns;
    for(auto& column:result.columns)columns.append(arrow_array(pa,types,std::move(column)));
    return pa.attr("Table").attr("from_arrays")(columns,nb::arg("names")=result.names);
}
}

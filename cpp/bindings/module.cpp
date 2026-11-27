#include <mktpack/core.hpp>
#include <mktpack/storage.hpp>
#include "arrow_input.hpp"
#include "arrow_output.hpp"
#include <nanobind/nanobind.h>
#include <nanobind/stl/filesystem.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

namespace nb=nanobind;
namespace fs=std::filesystem;
namespace {
nb::object python_value(const mktpack::Json& value) {
    if(value.is_null()) return nb::none();
    if(value.is_boolean()) return nb::bool_(value.get<bool>());
    if(value.is_number_unsigned()) return nb::int_(value.get<uint64_t>());
    if(value.is_number_integer()) return nb::int_(value.get<int64_t>());
    if(value.is_number_float()) return nb::float_(value.get<double>());
    if(value.is_string()) return nb::str(value.get_ref<const std::string&>().data(),value.get_ref<const std::string&>().size());
    if(value.is_binary()) {const auto& b=value.get_binary();return nb::bytes(reinterpret_cast<const char*>(b.data()),b.size());}
    if(value.is_array()) { nb::list result;for(const auto& item:value) result.append(python_value(item));return result; }
    nb::dict result;for(const auto& [key,item]:value.items()) result[nb::str(key.data(),key.size())]=python_value(item);return result;
}

int64_t timestamp(nb::object value,int64_t fallback) {
    if(value.is_none())return fallback;
    const auto dates=nb::module_::import_("datetime");
    const auto datetime=dates.attr("datetime");
    if(nb::isinstance<nb::int_>(value)) {
        const auto raw=nb::cast<int64_t>(value);
        if(raw>=10000101 && raw<=99991231)value=nb::str(value);
        else return raw;
    }
    if(nb::isinstance<nb::str>(value)) {
        value=nb::len(value)==8?datetime.attr("strptime")(value,"%Y%m%d")
                               :datetime.attr("fromisoformat")(value);
    } else if(!nb::isinstance(value,datetime) && nb::isinstance(value,dates.attr("date"))) {
        value=datetime(value.attr("year"),value.attr("month"),value.attr("day"));
    }
    if(value.attr("tzinfo").is_none())
        value=value.attr("replace")(nb::arg("tzinfo")=dates.attr("timezone").attr("utc"));
    return static_cast<int64_t>(nb::cast<double>(value.attr("timestamp")())*1000);
}

nb::object utc_datetime(const mktpack::Json& value) {
    if(value.is_null())return nb::none();
    const auto dates=nb::module_::import_("datetime");
    const auto epoch=dates.attr("datetime")(1970,1,1,nb::arg("tzinfo")=dates.attr("timezone").attr("utc"));
    return epoch+dates.attr("timedelta")(nb::arg("milliseconds")=value.get<int64_t>());
}

struct Group {
    fs::path root;
    std::string name;
};

struct Dataset {
    fs::path root;
};

nb::object describe(const fs::path& root,const std::string& group={},const std::string& symbol={}) {
    mktpack::Json result;
    {
        nb::gil_scoped_release release;
        result=mktpack::describe(root,group,symbol);
    }
    auto obj=python_value(result);
    if(!group.empty() && result.is_object()) {
        auto dict=nb::cast<nb::dict>(obj);
        if(result.contains("first_time"))dict["first_time"]=utc_datetime(result["first_time"]);
        if(result.contains("last_time"))dict["last_time"]=utc_datetime(result["last_time"]);
    }
    return obj;
}

mktpack::Json json_argument(nb::handle value) {
    if(value.is_none())return mktpack::Json::object();
    const auto text=nb::module_::import_("json").attr("dumps")(value);
    return mktpack::Json::parse(nb::cast<std::string>(text));
}
}

NB_MODULE(_native, module) {
    nb::class_<Group>(module,"Group","数据集中的时序数据组。")
        .def(nb::init<fs::path,std::string>(),nb::arg("root"),nb::arg("name"))
        .def_prop_ro("root",[](const Group& group){return group.root.string();},"数据集根目录或索引路径。")
        .def_ro("name",&Group::name,"数据组名称。")
        .def("describe",[](const Group& group,const std::optional<std::string>& symbol){
            return describe(group.root,group.name,symbol.value_or(std::string{}));
        },nb::arg("symbol")=nb::none(),
          "返回数据组（或指定 symbol）的 schema、行数与 UTC datetime 首末时间（first_time/last_time）。")
        .def("symbols",[](const Group& group,nb::object start,nb::object end) {
            const auto first=timestamp(start,INT64_MIN),last=timestamp(end,INT64_MAX);
            nb::gil_scoped_release release;
            return mktpack::symbols(group.root,group.name,first,last);
        },nb::kw_only(),nb::arg("start")=nb::none(),nb::arg("end")=nb::none(),
          "返回在 UTC [start, end) 区间内实际有记录的品种列表（按名称排序）。start/end 支持 YYYYMMDD 整数、UTC 毫秒整数、ISO 字符串、date 或 datetime（无时区按 UTC 解释）。")
        .def("read",[](const Group& group,const std::optional<std::string>& symbol,
                       std::optional<std::vector<std::string>> symbols,nb::object start,nb::object end,
                       const mktpack::Projection& columns) {
            auto names=std::move(symbols).value_or(std::vector<std::string>{});
            if(names.empty() && symbol && !symbol->empty())names.push_back(*symbol);
            const auto first=timestamp(start,INT64_MIN),last=timestamp(end,INT64_MAX);
            mktpack::QueryResult result;
            try {
                nb::gil_scoped_release release;
                result=mktpack::query(group.root,group.name,names,first,last,columns);
            } catch(const std::out_of_range& e) {
                throw nb::key_error(e.what());
            }
            return mktpack::bindings::arrow_table(std::move(result));
        },nb::kw_only(),nb::arg("symbol")=nb::none(),nb::arg("symbols")=nb::none(),
          nb::arg("start")=nb::none(),nb::arg("end")=nb::none(),nb::arg("columns")=nb::none(),
          "按合约、UTC [start, end) 时间区间与列投影读取时序数据，返回 pyarrow.Table。start/end 支持 YYYYMMDD 整数、UTC 毫秒整数、ISO 字符串、date 或 datetime（无时区按 UTC 解释）。");

    nb::class_<Dataset>(module,"Dataset","MktPack 数据集入口，支持普通目录、打包目录和 mkt.sqlite 索引路径。")
        .def(nb::init<fs::path>(),nb::arg("root"))
        .def_prop_ro("root",[](const Dataset& dataset){return dataset.root.string();},"数据集根目录或索引路径。")
        .def("describe",[](const Dataset& dataset){return describe(dataset.root);},"列出数据集内所有数据组的名称、类型与存储载体。")
        .def("keys",[](const Dataset& dataset) {
            nb::gil_scoped_release release;
            return mktpack::groups(dataset.root);
        },"返回按名称排序的所有数据组名列表。")
        .def("__len__",[](const Dataset& dataset) {
            nb::gil_scoped_release release;
            return mktpack::groups(dataset.root).size();
        })
        .def("__iter__",[](const Dataset& dataset) {
            std::vector<std::string> names;
            {
                nb::gil_scoped_release release;
                names=mktpack::groups(dataset.root);
            }
            return nb::iter(nb::cast(std::move(names)));
        })
        .def("__contains__",[](const Dataset& dataset,const std::string& name) {
            nb::gil_scoped_release release;
            return mktpack::group_kind(dataset.root,name).has_value();
        },nb::arg("name"))
        .def("__getitem__",[](const Dataset& dataset,const std::string& name) {
            std::optional<std::string> kind;
            mktpack::Json result;
            {
                nb::gil_scoped_release release;
                kind=mktpack::group_kind(dataset.root,name);
                if(kind=="ordinary")result=mktpack::ordinary(dataset.root,name);
            }
            if(!kind)throw nb::key_error(name.c_str());
            if(kind=="ordinary")return python_value(result);
            return nb::cast(Group{dataset.root,name});
        },nb::arg("name"),"按名称获取时序数据组（Group）或普通表数据（dict / list[dict]）。");
    module.def("open",[](fs::path root){return Dataset{std::move(root)};},nb::arg("root"),
               "打开 MktPack 数据集目录或 mkt.sqlite 索引文件。");

    auto raw=module.def_submodule("raw","单个 .mkp 文件的底层读写与头部检查接口。");
    raw.def("write", [](const fs::path& path,nb::handle source,nb::handle schema,
                       nb::handle metadata,bool lossy_taker) {
        auto table=mktpack::bindings::table_from_arrow(source,json_argument(schema),json_argument(metadata));
        if (lossy_taker) {
            try {
                mktpack::set_taker_encoding(table.schema,true);
            } catch (const mktpack::Json::out_of_range& e) {
                throw nb::key_error(e.what());
            }
        }
        nb::gil_scoped_release release;
        mktpack::write(path,table);
    },nb::arg("path"),nb::arg("table"),nb::kw_only(),nb::arg("schema")=nb::none(),
      nb::arg("metadata")=nb::none(),nb::arg("lossy_taker")=false,
      "将 pyarrow.Table 写入单个 .mkp 文件。");
    raw.def("read", [](const fs::path& path) {
        mktpack::QueryResult result;
        {
            nb::gil_scoped_release release;
            result=mktpack::file_result(path);
        }
        return mktpack::bindings::arrow_table(std::move(result));
    },nb::arg("path"),"读取单个 .mkp 文件并返回 pyarrow.Table。");
    raw.def("info", [](const fs::path& path) {return python_value(mktpack::info(path));},nb::arg("path"),
            "仅读取单个 .mkp 文件头元数据并返回 dict。");
    module.def("merge", [](const std::vector<fs::path>& paths,const fs::path& destination) {
        nb::gil_scoped_release release;
        mktpack::merge_files({paths.begin(),paths.end()}, destination);
    });
}

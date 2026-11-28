#include <mktpack/update.hpp>
#include <mktpack/binance.hpp>
#include <miniz.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <map>
#include <optional>
#include <queue>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace mktpack::update {
namespace {
namespace fs = std::filesystem;

std::string to_lower_ascii(std::string text) {
    for (char& c : text) {
        if (c >= 'A' && c <= 'Z') c = char(c - 'A' + 'a');
    }
    return text;
}

std::string resolve_url(const std::string& base_url, const std::string& rel) {
    if (rel.starts_with("http://") || rel.starts_with("https://")) return rel;
    if (rel.starts_with('/')) {
        const auto scheme = base_url.find("://");
        if (scheme == std::string::npos) return rel;
        const auto slash = base_url.find('/', scheme + 3);
        const auto origin = slash == std::string::npos ? base_url : base_url.substr(0, slash);
        return origin + rel;
    }
    const auto slash = base_url.rfind('/');
    if (slash == std::string::npos) return rel;
    return base_url.substr(0, slash + 1) + rel;
}

Json read_zip_meta(const fs::path& zip_path) {
    mz_zip_archive archive{};
    const auto path_str = utf8_string(zip_path);
    if (!mz_zip_reader_init_file(&archive, path_str.c_str(), 0))
        throw std::runtime_error("无法打开 ZIP 更新包：" + zip_path.string());

    Json meta;
    bool found = false;
    const mz_uint count = mz_zip_reader_get_num_files(&archive);
    for (mz_uint i = 0; i < count; ++i) {
        mz_zip_archive_file_stat stat{};
        if (!mz_zip_reader_file_stat(&archive, i, &stat)) continue;
        std::string name = stat.m_filename;
        if (name == "meta.json" || name == "./meta.json") {
            size_t size = 0;
            void* data = mz_zip_reader_extract_to_heap(&archive, i, &size, 0);
            if (!data) {
                mz_zip_reader_end(&archive);
                throw std::runtime_error("解压 meta.json 失败：" + zip_path.string());
            }
            const std::string text(static_cast<const char*>(data), size);
            mz_free(data);
            meta = Json::parse(text);
            found = true;
            break;
        }
    }
    mz_zip_reader_end(&archive);
    if (!found) throw std::runtime_error("ZIP 更新包缺少根目录 meta.json：" + zip_path.string());
    if (!meta.is_object() || !meta.contains("to") || !meta.at("to").is_string() || meta.at("to").get<std::string>().empty())
        throw std::runtime_error("meta.json 缺少有效的 to 版本号：" + zip_path.string());
    return meta;
}

fs::path safe_join(const fs::path& base, const std::string& entry_name) {
    const fs::path rel = utf8_path(entry_name);
    if (rel.is_absolute()) throw std::runtime_error("ZIP 包含非法绝对路径：" + entry_name);
    fs::path clean;
    for (const auto& part : rel) {
        if (part == "..") throw std::runtime_error("ZIP 包含非法越界路径：" + entry_name);
        if (part == "." || part.empty()) continue;
        clean /= part;
    }
    return base / clean;
}

void extract_zip(const fs::path& zip_path, const fs::path& dest_dir) {
    mz_zip_archive archive{};
    const auto path_str = utf8_string(zip_path);
    if (!mz_zip_reader_init_file(&archive, path_str.c_str(), 0))
        throw std::runtime_error("无法打开 ZIP 更新包：" + zip_path.string());
    try {
        const mz_uint count = mz_zip_reader_get_num_files(&archive);
        for (mz_uint i = 0; i < count; ++i) {
            mz_zip_archive_file_stat stat{};
            if (!mz_zip_reader_file_stat(&archive, i, &stat))
                throw std::runtime_error("读取 ZIP 条目失败：" + zip_path.string());
            const auto out_path = safe_join(dest_dir, stat.m_filename);
            if (mz_zip_reader_is_file_a_directory(&archive, i)) {
                fs::create_directories(out_path);
                continue;
            }
            if (out_path.has_parent_path()) fs::create_directories(out_path.parent_path());
            const auto out_str = utf8_string(out_path);
            if (!mz_zip_reader_extract_to_file(&archive, i, out_str.c_str(), 0))
                throw std::runtime_error("解压文件失败：" + std::string(stat.m_filename));
        }
    } catch (...) {
        mz_zip_reader_end(&archive);
        throw;
    }
    mz_zip_reader_end(&archive);
}

bool matches_from(const Json& from_spec, const std::optional<std::string>& current) {
    if (from_spec.is_null()) return !current.has_value();
    if (from_spec.is_string()) {
        const auto& s = from_spec.get_ref<const std::string&>();
        if (s == "*") return true;
        return current.has_value() && *current == s;
    }
    if (from_spec.is_array()) {
        for (const auto& item : from_spec) {
            if (matches_from(item, current)) return true;
        }
        return false;
    }
    return false;
}

struct PackageEdge {
    std::string to;
    Json from = "*";
    uint64_t weight = 1;
    std::string url;
    std::string sha256;
    fs::path local_path;
    Json meta;
};

// 从当前版本规划到目标版本（或离线最长可达终点）的最优升级路径（按总字节数/步数最小）。
std::vector<size_t> plan_path(const std::optional<std::string>& current,
                              const std::vector<PackageEdge>& edges,
                              const std::string& target_version) {
    using Node = std::optional<std::string>;
    auto key_of = [](const Node& n) -> std::string {
        return n ? ("v:" + *n) : std::string("null");
    };

    if (!target_version.empty() && current && *current == target_version) return {};

    // Dijkstra 搜索：节点为版本号（含 null），边权为包大小（默认 1）。
    std::map<std::string, uint64_t> dist;
    std::map<std::string, std::pair<std::string, size_t>> prev; // node -> (prev_node, edge_idx)
    std::map<std::string, Node> nodes;

    using QItem = std::pair<uint64_t, std::string>;
    std::priority_queue<QItem, std::vector<QItem>, std::greater<QItem>> pq;

    const std::string start_key = key_of(current);
    dist[start_key] = 0;
    nodes[start_key] = current;
    pq.push({0, start_key});

    while (!pq.empty()) {
        const auto [d, u_key] = pq.top();
        pq.pop();
        if (d != dist[u_key]) continue;
        const auto& u_ver = nodes.at(u_key);

        for (size_t i = 0; i < edges.size(); ++i) {
            const auto& e = edges[i];
            if (u_ver && *u_ver == e.to) continue;
            if (!matches_from(e.from, u_ver)) continue;
            const Node v_ver = e.to;
            const std::string v_key = key_of(v_ver);
            const uint64_t nd = d + std::max<uint64_t>(1, e.weight);
            if (!dist.contains(v_key) || nd < dist[v_key]) {
                dist[v_key] = nd;
                nodes[v_key] = v_ver;
                prev[v_key] = {u_key, i};
                pq.push({nd, v_key});
            }
        }
    }

    std::string end_key;
    if (!target_version.empty()) {
        end_key = "v:" + target_version;
        if (!dist.contains(end_key)) {
            throw std::runtime_error("无法从当前版本 " + (current ? *current : std::string("<无版本>")) +
                                     " 规划到目标版本 " + target_version + " 的升级路径（可能缺少中间版本包）");
        }
    } else {
        // 离线未指定目标版本：选取可达且无法继续前进的终节点（多终节点时按版本字典序最大）。
        std::string best_ver;
        for (const auto& [k, _] : dist) {
            if (k == start_key) continue;
            const auto& ver = *nodes.at(k);
            bool has_out = false;
            for (const auto& e : edges) {
                if (e.to != ver && matches_from(e.from, nodes.at(k))) {
                    has_out = true;
                    break;
                }
            }
            if (!has_out && (end_key.empty() || ver > best_ver)) {
                end_key = k;
                best_ver = ver;
            }
        }
        if (end_key.empty()) {
            // 若所有包均不适用或当前已是终点，检查是否已处于提供的最高版本
            for (const auto& e : edges) {
                if (current && *current == e.to) return {};
            }
            throw std::runtime_error("没有可应用于当前版本 " + (current ? *current : std::string("<无版本>")) + " 的更新包");
        }
    }

    std::vector<size_t> path;
    for (std::string cur = end_key; cur != start_key;) {
        const auto [p_key, edge_idx] = prev.at(cur);
        path.push_back(edge_idx);
        cur = p_key;
    }
    std::reverse(path.begin(), path.end());
    return path;
}

std::string find_python() {
    if (const char* env = std::getenv("MKTPACK_PYTHON"); env && *env) return env;
    if (const char* venv = std::getenv("VIRTUAL_ENV"); venv && *venv) {
#ifdef _WIN32
        const auto candidate = fs::path(venv) / "Scripts" / "python.exe";
#else
        const auto candidate = fs::path(venv) / "bin" / "python";
#endif
        if (fs::exists(candidate)) return utf8_string(candidate);
    }
#ifdef _WIN32
    return "python";
#else
    return "python3";
#endif
}

void set_env_var(const char* name, const std::string& value) {
#ifdef _WIN32
    _putenv_s(name, value.c_str());
#else
    setenv(name, value.c_str(), 1);
#endif
}

std::string shell_quote(const std::string& arg) {
    std::string out = "\"";
    for (char c : arg) {
        if (c == '"' || c == '\\') out += '\\';
        out += c;
    }
    out += '"';
    return out;
}

void run_update_script(const fs::path& script_path, const fs::path& target,
                       const fs::path& extract_dir,
                       const std::optional<std::string>& from_ver,
                       const std::string& to_ver) {
    set_env_var("MKTPACK_TARGET", utf8_string(target));
    set_env_var("MKTPACK_PACKAGE_DIR", utf8_string(extract_dir));
    set_env_var("MKTPACK_FROM_VERSION", from_ver ? *from_ver : "");
    set_env_var("MKTPACK_TO_VERSION", to_ver);

    const std::string py = find_python();
    std::string cmd = shell_quote(py) + " " + shell_quote(utf8_string(script_path)) + " " +
                      shell_quote(utf8_string(target)) + " " + shell_quote(utf8_string(extract_dir));
#ifdef _WIN32
    cmd = "\"" + cmd + "\"";
#endif
    const int rc = std::system(cmd.c_str());
    if (rc != 0) throw std::runtime_error("更新脚本执行失败（退出码 " + std::to_string(rc) + "）：" + script_path.string());
}

struct TempDir {
    fs::path path;
    explicit TempDir(const std::string& prefix) {
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        path = fs::temp_directory_path() / (prefix + "-" + std::to_string(stamp));
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

Json apply_single_zip(const fs::path& target, const fs::path& zip_path,
                      const Json& meta, const std::optional<std::string>& current_ver,
                      const Options& options) {
    const std::string to_ver = meta.at("to").get<std::string>();
    const Json from_spec = meta.contains("from") ? meta.at("from") : Json("*");
    if (!matches_from(from_spec, current_ver)) {
        throw std::runtime_error("更新包 " + zip_path.filename().string() + " 不支持从当前版本 " +
                                 (current_ver ? *current_ver : std::string("<无版本>")) + " 升级");
    }

    TempDir tmp("mktpack-update");
    extract_zip(zip_path, tmp.path);

    Json step = {{"to", to_ver}, {"package", zip_path.filename().string()}};
    fs::path script_path;
    if (meta.contains("script") && meta.at("script").is_string() && !meta.at("script").get<std::string>().empty()) {
        script_path = safe_join(tmp.path, meta.at("script").get<std::string>());
        if (!fs::exists(script_path))
            throw std::runtime_error("meta.json 指定的更新脚本不存在：" + meta.at("script").get<std::string>());
    }

    if (!script_path.empty()) {
        if (options.progress) options.progress("执行脚本 " + script_path.filename().string() + " → " + to_ver);
        run_update_script(script_path, target, tmp.path, current_ver, to_ver);
        step["mode"] = "script";
    } else {
        fs::path data_dir;
        if (fs::exists(tmp.path / "data" / "mkt.sqlite")) data_dir = tmp.path / "data";
        else if (fs::exists(tmp.path / "mkt.sqlite")) data_dir = tmp.path;
        else throw std::runtime_error("更新包中未找到 data/mkt.sqlite：" + zip_path.string());

        if (options.progress) options.progress("合并数据 → " + to_ver);
        step["mode"] = "merge";
        auto merged = merge_dataset(target, data_dir, options.merge);
        const bool packed = merged.value("packed", false);
        step["merge"] = std::move(merged);
        if (packed && dataset_meta(target).value("port", std::string{}) == "binance") {
            if (options.progress) options.progress("归档热卷 → " + to_ver);
            step["compact"] = compact(target);
        }
    }

    set_dataset_meta(target, "version", to_ver);
    return step;
}

std::optional<std::string> current_version_of(const fs::path& target) {
    const auto meta = dataset_meta(target);
    if (meta.contains("version") && meta.at("version").is_string() && !meta.at("version").get<std::string>().empty())
        return meta.at("version").get<std::string>();
    return std::nullopt;
}
}

Json merge_zips(const fs::path& target,
                const std::vector<fs::path>& zip_paths,
                const Options& options) {
    if (zip_paths.empty()) throw std::runtime_error("至少需要提供一个 ZIP 更新包");

    std::vector<PackageEdge> edges;
    edges.reserve(zip_paths.size());
    for (const auto& p : zip_paths) {
        if (!fs::exists(p)) throw std::runtime_error("更新包不存在：" + p.string());
        auto meta = read_zip_meta(p);
        PackageEdge edge;
        edge.to = meta.at("to").get<std::string>();
        edge.from = meta.contains("from") ? meta.at("from") : Json("*");
        std::error_code ec;
        const auto sz = fs::file_size(p, ec);
        edge.weight = (!ec && sz > 0) ? sz : 1;
        edge.local_path = p;
        edge.meta = std::move(meta);
        edges.push_back(std::move(edge));
    }

    auto cur = current_version_of(target);
    const auto plan = plan_path(cur, edges, options.target_version);
    if (plan.empty()) {
        return {
            {"status", "up_to_date"},
            {"version", cur ? Json(*cur) : Json(nullptr)},
            {"applied", Json::array()},
            {"warnings", Json::array()}
        };
    }

    Json applied = Json::array();
    Json warnings = Json::array();
    for (size_t idx : plan) {
        const auto& edge = edges[idx];
        auto step = apply_single_zip(target, edge.local_path, edge.meta, cur, options);
        if (step.contains("merge") && step["merge"].contains("warnings")) {
            for (const auto& w : step["merge"]["warnings"]) warnings.push_back(w);
        }
        cur = edge.to;
        applied.push_back(std::move(step));
    }
    return {
        {"status", "updated"},
        {"version", cur ? Json(*cur) : Json(nullptr)},
        {"applied", applied},
        {"warnings", warnings}
    };
}

Json merge_remote(const fs::path& target,
                  const std::string& url,
                  const Options& options) {
    if (options.progress) options.progress("获取清单 " + url);
    const auto manifest = Json::parse(binance::http_get(url));
    if (!manifest.is_object() || !manifest.contains("packages") || !manifest.at("packages").is_array())
        throw std::runtime_error("manifest.json 格式无效：缺少 packages 数组");

    const std::string target_ver = !options.target_version.empty()
        ? options.target_version
        : manifest.value("latest", std::string{});
    if (target_ver.empty()) throw std::runtime_error("manifest.json 缺少 latest 版本号");

    std::vector<PackageEdge> edges;
    for (const auto& item : manifest.at("packages")) {
        if (!item.is_object() || !item.contains("to") || !item.contains("url")) continue;
        PackageEdge edge;
        edge.to = item.at("to").get<std::string>();
        edge.from = item.contains("from") ? item.at("from") : Json("*");
        edge.url = resolve_url(url, item.at("url").get<std::string>());
        edge.weight = item.value("size", uint64_t(1));
        edge.sha256 = to_lower_ascii(item.value("sha256", std::string{}));
        edges.push_back(std::move(edge));
    }

    auto cur = current_version_of(target);
    const auto plan = plan_path(cur, edges, target_ver);
    if (plan.empty()) {
        return {
            {"status", "up_to_date"},
            {"version", cur ? Json(*cur) : Json(nullptr)},
            {"applied", Json::array()},
            {"warnings", Json::array()}
        };
    }

    TempDir dl_dir("mktpack-download");
    Json applied = Json::array();
    Json warnings = Json::array();
    for (size_t step_i = 0; step_i < plan.size(); ++step_i) {
        const auto& edge = edges[plan[step_i]];
        const auto local_zip = dl_dir.path / ("pkg-" + std::to_string(step_i) + ".zip");
        if (options.progress)
            options.progress("下载 (" + std::to_string(step_i + 1) + "/" + std::to_string(plan.size()) + ") " + edge.url);
        const auto actual_sha = http_download(edge.url, local_zip);
        if (!edge.sha256.empty() && actual_sha != edge.sha256) {
            throw std::runtime_error("下载校验失败（SHA256 不匹配）：" + edge.url +
                                     " 期望 " + edge.sha256 + " 实际 " + actual_sha);
        }
        const auto meta = read_zip_meta(local_zip);
        if (meta.at("to").get<std::string>() != edge.to) {
            throw std::runtime_error("ZIP 内 meta.json 版本与 manifest 不一致：" + edge.url);
        }
        auto step = apply_single_zip(target, local_zip, meta, cur, options);
        if (step.contains("merge") && step["merge"].contains("warnings")) {
            for (const auto& w : step["merge"]["warnings"]) warnings.push_back(w);
        }
        cur = edge.to;
        applied.push_back(std::move(step));
        std::error_code ec;
        fs::remove(local_zip, ec);
    }

    return {
        {"status", "updated"},
        {"version", cur ? Json(*cur) : Json(nullptr)},
        {"applied", applied},
        {"warnings", warnings}
    };
}

}

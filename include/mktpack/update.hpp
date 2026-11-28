#pragma once
#include <mktpack/storage.hpp>
#include <filesystem>
#include <functional>
#include <string>
#include <vector>

namespace mktpack::update {

struct Options {
    MergeOptions merge;
    std::string target_version;
    std::function<void(const std::string&)> progress;
};

// 从一个或多个本地发布 zip（每个含 meta.json）按版本链升级目标数据集。
Json merge_zips(const std::filesystem::path& target,
                const std::vector<std::filesystem::path>& zip_paths,
                const Options& options = {});

// 从远端 HTTP(S) manifest.json 规划升级链、流式下载校验并应用更新。
Json merge_remote(const std::filesystem::path& target,
                  const std::string& url,
                  const Options& options = {});

// 流式下载 URL 到本地文件并返回小写十六进制 SHA256。
std::string http_download(const std::string& url,
                          const std::filesystem::path& destination,
                          const std::function<void(uint64_t, uint64_t)>& progress = {});

}

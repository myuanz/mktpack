#include <mktpack/binance.hpp>
#include <mktpack/update.hpp>
#include <httplib.h>
#include <mbedtls/error.h>
#include <mbedtls/sha256.h>
#include <cstdio>
#include <fstream>
#include <stdexcept>
#include <chrono>
#include <map>
#include <memory>
#include <thread>

namespace mktpack::binance {
std::string http_get(const std::string& url) {
    const auto scheme=url.find("://");
    if(scheme==std::string::npos) throw std::runtime_error("HTTP 地址缺少协议");
    const auto split=url.find('/',scheme+3);
    const auto origin=url.substr(0,split);
    const auto path=split==std::string::npos?std::string("/"):url.substr(split);
    thread_local std::map<std::string,std::unique_ptr<httplib::Client>> clients;
    auto& client=clients[origin];
    httplib::Result response;
    for(int attempt=0;attempt<5;++attempt) {
        if(!client) {
            client=std::make_unique<httplib::Client>(origin);
            client->set_path_encode(false);
            client->set_connection_timeout(30);
            client->set_read_timeout(60);
            client->set_follow_location(true);
            client->set_keep_alive(true);
        }
        response=client->Get(path);
        if(response && response->status==200) return response->body;
        const bool retry=!response || response->status==408 || response->status==429 || (response->status>=500 && response->status<600);
        client.reset();
        if(!retry || attempt==4) break;
        std::this_thread::sleep_for(std::chrono::seconds(1 << attempt));
    }
    if(!response) {
        std::string message="HTTP 请求失败："+httplib::to_string(response.error());
        if(response.ssl_backend_error()) {
            char detail[256]{};
            mbedtls_strerror(-static_cast<int>(response.ssl_backend_error()),detail,sizeof(detail));
            message+="（"+std::string(detail)+"）";
        }
        throw std::runtime_error(message+" "+url);
    }
    throw std::runtime_error("HTTP 返回状态 "+std::to_string(response->status)+" "+url);
}
}

namespace mktpack::update {
std::string http_download(const std::string& url,
                          const std::filesystem::path& destination,
                          const std::function<void(uint64_t, uint64_t)>& progress) {
    const auto scheme=url.find("://");
    if(scheme==std::string::npos) throw std::runtime_error("HTTP 地址缺少协议");
    const auto split=url.find('/',scheme+3);
    const auto origin=url.substr(0,split);
    const auto path=split==std::string::npos?std::string("/"):url.substr(split);

    httplib::Result response;
    std::string sha_hex;
    for(int attempt=0;attempt<5;++attempt) {
        httplib::Client client(origin);
        client.set_path_encode(false);
        client.set_connection_timeout(30);
        client.set_read_timeout(300);
        client.set_follow_location(true);
        client.set_keep_alive(true);

        if(destination.has_parent_path()) std::filesystem::create_directories(destination.parent_path());
        std::ofstream out(destination,std::ios::binary|std::ios::trunc);
        if(!out) throw std::runtime_error("无法创建下载文件："+destination.string());

        mbedtls_sha256_context sha;
        mbedtls_sha256_init(&sha);
        mbedtls_sha256_starts(&sha,0);
        uint64_t downloaded=0,total=0;
        bool write_error=false;

        response=client.Get(
            path,
            [&](const httplib::Response& res) {
                if(res.has_header("Content-Length")) {
                    try { total=std::stoull(res.get_header_value("Content-Length")); } catch(...) {}
                }
                return true;
            },
            [&](const char* data,size_t len) {
                if(!out.write(data,static_cast<std::streamsize>(len))) {
                    write_error=true;
                    return false;
                }
                mbedtls_sha256_update(&sha,reinterpret_cast<const unsigned char*>(data),len);
                downloaded+=len;
                if(progress) progress(downloaded,total);
                return true;
            });
        out.close();

        unsigned char digest[32]{};
        mbedtls_sha256_finish(&sha,digest);
        mbedtls_sha256_free(&sha);

        if(response && response->status==200 && !write_error) {
            char buf[65]{};
            for(int i=0;i<32;++i) std::snprintf(buf+i*2,3,"%02x",digest[i]);
            sha_hex.assign(buf,64);
            return sha_hex;
        }
        std::error_code ec;
        std::filesystem::remove(destination,ec);
        if(write_error) throw std::runtime_error("下载写入失败："+destination.string());
        const bool retry=!response || response->status==408 || response->status==429 ||
                         (response->status>=500 && response->status<600);
        if(!retry || attempt==4) break;
        std::this_thread::sleep_for(std::chrono::seconds(1 << attempt));
    }
    if(!response) {
        std::string message="HTTP 下载失败："+httplib::to_string(response.error());
        if(response.ssl_backend_error()) {
            char detail[256]{};
            mbedtls_strerror(-static_cast<int>(response.ssl_backend_error()),detail,sizeof(detail));
            message+="（"+std::string(detail)+"）";
        }
        throw std::runtime_error(message+" "+url);
    }
    throw std::runtime_error("HTTP 返回状态 "+std::to_string(response->status)+" "+url);
}
}

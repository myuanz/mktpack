#include <mktpack/binance.hpp>
#include <httplib.h>
#include <mbedtls/error.h>
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

#pragma once
#include <mktpack/binance.hpp>
#include <chrono>
#include <iostream>
#include <mutex>
#include <sstream>
#include <set>
#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#include <sys/ioctl.h>
#endif

namespace mktpack::cli {
inline bool stderr_is_terminal() {
#ifdef _WIN32
    return _isatty(_fileno(stderr));
#else
    return isatty(STDERR_FILENO);
#endif
}

class DownloadProgress {
    using Clock=std::chrono::steady_clock;
    std::ostream& output;
    bool terminal;
    std::mutex mutex;
    std::vector<binance::Progress> states;
    std::vector<Clock::time_point> logged;
    std::vector<std::set<std::string>> stages;
    Clock::time_point rendered{};
    size_t lines=0;

    static std::string text(const binance::Progress& state) {
        std::ostringstream out;
        if(state.slot) out<<"["<<state.slot<<"] ";
        if(!state.symbol.empty()) out<<state.symbol<<" ";
        if(!state.group.empty()) out<<state.group<<" ";
        out<<state.stage;
        if(state.total) out<<" "<<state.completed<<"/"<<state.total;
        if(!state.detail.empty()) out<<" · "<<state.detail;
        return out.str();
    }
    static std::string fit(const std::string& text,size_t width) {
        size_t offset=0,columns=0;
        while(offset<text.size()) {
            const auto c=static_cast<unsigned char>(text[offset]);
            const size_t length=c<0x80?1:c<0xe0?2:c<0xf0?3:4;
            const size_t used=length>=3?2:1;
            if(columns+used>=width) break;
            columns+=used; offset+=length;
        }
        return text.substr(0,offset);
    }
    void clear() {
        if(!lines) return;
        output<<"\033["<<lines<<"A\033[J";
        lines=0;
    }
    void render() {
        size_t width=100;
#ifndef _WIN32
        winsize size{};
        if(ioctl(STDERR_FILENO,TIOCGWINSZ,&size)==0 && size.ws_col) width=size.ws_col;
#endif
        clear();
        for(const auto& state:states) {
            output<<fit(state.stage.empty()?"等待任务":text(state),width)<<"\n";
            ++lines;
        }
        output.flush();
        rendered=Clock::now();
    }
public:
    explicit DownloadProgress(size_t slots,std::ostream& stream=std::cerr,bool tty=stderr_is_terminal())
        :output(stream),terminal(tty),states(slots+1),logged(slots+1),stages(slots+1) {}
    ~DownloadProgress() { if(terminal && lines) render(); }
    void update(const binance::Progress& event) {
        std::lock_guard lock(mutex);
        const auto now=Clock::now();
        auto& previous=states.at(event.slot);
        const bool new_job=event.group!=previous.group || event.symbol!=previous.symbol;
        if(new_job) stages[event.slot].clear();
        const bool first_stage=stages[event.slot].insert(event.stage).second;
        previous=event;
        const bool important=new_job || first_stage || event.stage=="错误" || event.stage=="完成" || event.slot==0 || event.stage=="合并";
        if(terminal) {
            if(event.stage=="错误") {clear();output<<text(event)<<"\n";}
            if(important || now-rendered>=std::chrono::milliseconds(100)) render();
        } else if(important || now-logged[event.slot]>=std::chrono::seconds(10)) {
            output<<text(event)<<"\n";
            output.flush();
            logged[event.slot]=now;
        }
    }
};
}

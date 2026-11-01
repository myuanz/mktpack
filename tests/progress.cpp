#include "../cpp/cli/progress.hpp"
#include <stdexcept>

int main() {
    using namespace mktpack;
    auto expect=[](bool value,const char* message) {if(!value) throw std::runtime_error(message);};
    std::ostringstream plain;
    {
        cli::DownloadProgress progress(2,plain,false);
        progress.update({1,"kline","BTCUSDT","发现","2024"});
        progress.update({1,"kline","BTCUSDT","下载","2024-01",0,12});
        progress.update({1,"kline","BTCUSDT","错误","模拟错误",0,12});
        expect(plain.str().find("模拟错误")!=std::string::npos,"错误没有及时输出");
        progress.update({2,"oi","ETHUSDT","合并","2024"});
        progress.update({1,"kline","BTCUSDT","完成","",12,12});
    }
    expect(plain.str().find("\033")==std::string::npos,"普通日志包含终端控制码");
    expect(plain.str().find("下载")!=std::string::npos,"短任务遗漏阶段摘要");
    expect(plain.str().find("12/12")!=std::string::npos,"完成进度缺失");
    std::ostringstream tty;
    {
        cli::DownloadProgress progress(2,tty,true);
        progress.update({1,"kline","BTCUSDT","发现","2024"});
        progress.update({2,"oi","ETHUSDT","合并","2024"});
        progress.update({1,"kline","BTCUSDT","错误","保留错误"});
    }
    expect(tty.str().find("\033[3A")!=std::string::npos,"没有固定工作槽");
    expect(tty.str().find("BTCUSDT")!=std::string::npos && tty.str().find("ETHUSDT")!=std::string::npos,"工作槽互相覆盖");
    expect(tty.str().find("保留错误")!=std::string::npos,"终端错误丢失");
}

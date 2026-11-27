# MktPack

高压缩、高性能的金融时序数据 C++20 读写库，提供 Python 接口和原生 `mkt` 命令。除了 `.mkp` 基础市场时序压缩格式，还提供两个适配: 

- 币安 USDT UM 永续 Kline、OI、结算资金费率和交易所信息下载
- 中国期货 tick 导入

## 安装

```sh
uv add mktpack
```

需要 Python 3.12+。wheel 包含原生 `mkt` 命令。

## Python 读取接口

```python
import mktpack

market = mktpack.open('data/binance')
groups = market.describe()
symbols = market['kline'].symbols(start=20260101, end=20260201)
bars = market['kline'].read(
    symbol='BTCUSDT', start=20260101, end=20260201,
    columns=['time', 'close', 'volume'],
)
info = market['exchangeinfo']

futures = mktpack.open('data/cn_futures')
ticks = futures['tick'].read(symbol='CU2610', columns=['time', 'last_price'])
```

时序查询返回 Arrow Table，时间按 UTC `[start,end)` 筛选

## 币安下载

```sh
uv run mkt --root data/binance binance download --symbols BTCUSDT --start 2026-01-01 --end 2026-02-01
```

省略 `--symbols` 自动发现全部 USDT 品种。

币安下载默认使用无损编码，添加 `--lossy-taker` 将主动买量占比按 0.1 个百分点量化。`--full` 检查来源对应的本地载荷并补齐缺失数据。

## 导入中国期货 tick

```sh
uv run mkt --root data/cn_futures ctp-futures import path/to/ctp-mirror --symbols CU2610 --start 2026-09-01 --end 2026-10-01
```

镜像根目录包含 `nfs_rq*/tick/future`，导入按源包文件名中的交易日筛选，不写 symbols、start、end 时默认全量

## 打包

工具提供将零散的数据文件打包到若干个大文件的功能: 

```sh
uv run mkt --root data/binance package packaged-data --max-file-size 8GiB
```

可用 `--groups`、`--symbols`、`--start`、`--end` 筛选. 

普通数据目录和打包目录使用相同的读取接口。

## 开发

```sh
uv sync
just test
```

## LLM 与 MktPack

我完成了需求和规范的整理，LLM 完成了主要代码，我负责了审查和编写部分核心代码. 

库已经在实际项目投入使用

---

[需求](https://github.com/myuanz/mktpack/blob/master/docs/%E9%9C%80%E6%B1%82.md) · [格式规范](https://github.com/myuanz/mktpack/blob/master/docs/%E8%A7%84%E8%8C%83.md)

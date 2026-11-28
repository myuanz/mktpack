---
name: mktpack
description: 使用 mktpack Python 库和 mkt CLI 读取、查询、下载、导入、校验与打包高压缩、高性能金融时序数据（.mkp 与 mkt.sqlite 数据集），涵盖币安 USDT UM 永续行情/OI/资金费率及中国期货 CTP tick 数据。
---

# MktPack 使用指南

MktPack 是高压缩、高性能的金融时序数据存储格式（`.mkp`）与读写库，包含 Python 包 `mktpack` 和随 wheel 安装的原生命令行工具 `mkt`

## 安装

```sh
uv add mktpack
```

## Python 读取接口

### 1. 发现数据集结构与可用合约

```python
import mktpack

ds = mktpack.open("data/binance")

ds.keys()                      # 所有数据组名称列表，支持 len(ds)、list(ds)、"kline" in ds
ds.describe()                  # 各组的 name / kind / storage 列表

kline = ds["kline"]            # 时序组返回 Group 对象
kline.describe()               # 字段 schema、总行数、first_time / last_time（UTC datetime，空组为 None）
kline.describe("BTCUSDT")      # 查询单个合约的行数与 UTC datetime 首末时间
kline.symbols()                # 该组全部有记录的品种列表（按名称排序）
kline.symbols(start=20260101, end=20260201)  # 仅返回区间内实际有记录的品种

info = ds["exchangeinfo"]      # 普通表（ordinary）直接返回 dict 或 list[dict]
```

### 2. 读取时序数据

```python
table = kline.read(
    symbol="BTCUSDT",          # 单品种；多品种用 symbols=["BTCUSDT", "ETHUSDT"]
    start=20260101,            # 可传 YYYYMMDD 整数、ISO 字符串、date 或 datetime
    end=20260201,              # 统一按 UTC [start, end) 半开区间筛选
    columns=["time", "close", "volume"],  # 建议显式投影，只解码请求列及预测依赖
)

# 国内期货 tick 读取示例（合约使用标准代码，不含交易所前缀）
futures = mktpack.open("data/cn_futures")
tick = futures["tick"]
contracts = tick.symbols(start=20250101, end=20250110)

ticks = tick.read(
    symbol="ZN2501",
    start="2025-01-01T21:00:00+08:00",    # 支持带时区 ISO 字符串或 datetime（自动转 UTC）
    end="2025-01-02T15:00:00+08:00",
    columns=[
        "time", "trading_day", "last_price", "volume", "open_interest",
        "bid_price1", "bid_price2", "bid_price3", "bid_price4", "bid_price5",
        "bid_volume1", "bid_volume2", "bid_volume3", "bid_volume4", "bid_volume5",
        "ask_price1", "ask_price2", "ask_price3", "ask_price4", "ask_price5",
        "ask_volume1", "ask_volume2", "ask_volume3", "ask_volume4", "ask_volume5",
    ],
)
```

注意事项：
- 返回 `pyarrow.Table`。时间戳为 `timestamp[ms, tz=UTC]`。
- `start`/`end` 支持 `YYYYMMDD` 整数、UTC 毫秒整数、ISO 字符串、`date` 或 `datetime`；**无时区的 `datetime` 统一按 UTC 解释**，查询国内期货建议显式带 `+08:00`。
- 结果按 `(symbol, time)` 稳定排序，空结果仍保留完整 schema。
- 省略 `symbol`/`symbols` 时读取全部品种；省略 `start`/`end` 时不限时间范围。
- 国内期货 `tick` 合约代码统一标准化为**全大写 + 4 位年月**（如 `RB2701`、`ZN2501`），不含交易所前缀且**严格区分大小写**（传小写如 `rb2701` 会返回空表）。
- 国内期货 `tick` 包含 37 列（含五档买卖价量），`time` 为 UTC 毫秒、`trading_day` 为 `YYYYMMDD` 整数交易日，查询时按需传入 `columns` 可显著减少解码开销。

### 3. 单文件调试与数据集维护接口

```python
import mktpack
from mktpack import raw

# 单文件读写与头部检查
file_info = raw.info("sample.mkp")  # 仅读取文件头（版本、行数、列编码与统计）
table = raw.read("sample.mkp")
raw.write("sample.mkp", table, schema=schema, metadata=metadata, lossy_taker=False)

# 数据集元数据、校验、打包、增量升级与热卷归档
mktpack.meta("data/binance")
mktpack.set_meta("data/binance", "version", "2026.11.28")
mktpack.check("data/binance")
mktpack.package("data/binance", "packaged-data", max_file_size=8 * 1024**3)
mktpack.merge_zips("packaged-data", ["2026.11.27_to_2026.11.28.zip"])
mktpack.merge_remote("packaged-data", "https://bucket.example.com/ctp-futures/manifest.json")
mktpack.compact("packaged-data", before="2026-01-01")

# 数据源下载与导入
mktpack.download_binance("data/binance", symbols=["BTCUSDT"], start="2026-01-01")
mktpack.import_ctp_ticks("data/cn_futures", "ctp-mirror", start="20260101", end="20260201")
```

## 原生 CLI（`mkt`）

全局参数 `--root <dir>` 指定数据集目录（默认当前目录 `.`）。

### 币安 USDT UM 永续

```sh
# 增量下载（省略 --symbols 自动发现全部 USDT 永续合约；默认无损编码）
mkt --root data/binance binance download --symbols BTCUSDT ETHUSDT --start 2026-01-01 --end 2026-02-01

# 常用选项：
#   --groups kline oi funding_rate_settlement exchangeinfo
#   --jobs 8                并发数（默认 4）
#   --full                  全量核对远端清单并补齐本地缺失数据
#   --lossy-taker           将主动买量占比按 0.1 个百分点量化（默认关闭）
#   --source-cache <dir>    缓存原始 ZIP

# 核查本地索引与载荷完整性（存在差异时返回非零退出码）
mkt --root data/binance binance check
```

### 数据集概览、分卷打包与单文件查看

```sh
# 查看数据集包含的所有组，或指定组 / 单合约的 schema、行数与时间范围
mkt --root data/binance describe
mkt --root data/binance describe kline
mkt --root data/binance describe kline --symbol BTCUSDT

# 列出指定组在可选时间区间内有记录的合约
mkt --root data/binance symbols kline --start 2026-01-01 --end 2026-02-01

# 将散文件或已有打包目录按容量重打包为 mkt.sqlite + mkt.data.N（目标目录必须为空）
mkt --root data/binance package packaged-data --max-file-size 8GiB --groups kline --symbols BTCUSDT --start 2026-01-01 --end 2026-02-01 --jobs 4

# 通过本地发布 zip 或远端 manifest.json 文件增量升级数据集
mkt --root packaged-data merge 2026.11.27_to_2026.11.28.zip
mkt --root packaged-data merge https://bucket.example.com/ctp-futures/manifest.json

# 归档热卷（--before 之前的数据封存进末尾冷卷，其余留在 hot.data.1..M；默认沿用 __meta__ 中的分卷上限）
mkt --root packaged-data compact --before 2026-01-01

# 查看单个 .mkp 文件头 JSON
mkt info data/binance/kline/BTCUSDT/2025.mkp
```

### Skill

```sh
# 输出或安装本 skill 文档
mkt skill
mkt skill --install ~/.agents/skills
```

## 内置数据组速查

| 数据源 | 数据组 | 类型 / 载体 | 主要字段 |
|---|---|---|---|
| 币安 | `kline` | 时序 / MKP | `time`, `open`, `high`, `low`, `close`, `volume`, `taker_buy_volume` |
| 币安 | `oi` | 时序 / MKP | `time`, `sum_open_interest`, `sum_open_interest_value` |
| 币安 | `funding_rate_settlement` | 时序 / SQLite | `time`, `funding_rate`, `interval_hours` |
| 币安 | `exchangeinfo` | 普通表 / SQLite | 交易所与合约元信息（嵌套字段自动解析为 JSON 结构） |
| 国内期货 | `tick` | 时序 / MKP（日文件/月年卷） | `time`, `trading_day`, `last_price`, OHLC, 涨跌停/结算参考价, `volume`, `turnover`, `open_interest`, 五档买卖价量 |

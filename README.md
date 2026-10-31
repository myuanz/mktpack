# MktPack

精确保存金融时序数据的 C++20 库，提供 Python Arrow 接口和原生 `mkt` 命令。当前支持基础 `.mkp` 格式、数据集查询，以及币安 USDT UM 永续 Kline、OI、结算资金费率和交易所信息下载。

## 开发

```sh
uv sync
just build
just test
```

## 下载与读取

```sh
uv run mkt --root data/binance binance download \
  --symbols BTCUSDT DOGEUSDT ETHUSDT BNBUSDT \
  --start 2024-01-01 --end 2026-10-22 \
  --source-cache validation/source/binance --jobs 4
uv run mkt --root data/binance binance check
```

```python
import mktpack

market = mktpack.open('data/binance')
table = market['kline'].read(
    symbol='BTCUSDT', start=20261021, end=20261022,
    columns=['time', 'close', 'volume', 'taker_buy_volume'],
)
info: dict = market['exchangeinfo']
```

时间范围为 UTC `[start,end)`，定点数返回 Arrow decimal。币安下载默认将主动买量占比按 0.1 个百分点量化，添加 `--no-lossy-taker` 使用无损编码。`--full` 检查来源对应的本地载荷并补齐缺失数据。

[需求](docs/需求.md) · [格式规范](docs/规范.md) · [真实数据验收](validation/验收.md)

from decimal import Decimal
import pyarrow as pa
import pytest
from mktpack import raw


def test_exact_roundtrip(tmp_path):
    table = pa.table({"time": pa.array([1, 1, 3], type=pa.timestamp("ms", tz="UTC")), "funding_rate": pa.array([Decimal("-.000001"), Decimal(".234567"), Decimal("0")]), "symbol": ["BTCUSDT"] * 3})
    path = tmp_path / "funding.mkp"
    raw.write(path, table)
    restored = raw.read(path)
    assert restored["time"].equals(table["time"])
    assert restored["funding_rate"].to_pylist() == table["funding_rate"].to_pylist()
    assert restored["symbol"].equals(table["symbol"])


def test_schema_rescale(tmp_path):
    path = tmp_path / "data.mkp"
    table = pa.table({"time": [1], "value": [Decimal("1.23")]})
    raw.write(path, table, schema={"time": {"type": "timestamp", "scale": 0}, "value": {"type": "decimal", "scale": 4}})
    assert raw.read(path)["value"][0].as_py() == Decimal("1.2300")
    with pytest.raises(ValueError):
        raw.write(path, table, schema={"time": {"type": "timestamp", "scale": 0}, "value": {"type": "decimal", "scale": 1}})


def test_empty_and_int64_bounds(tmp_path):
    path = tmp_path / "bounds.mkp"
    table = pa.table({"time": pa.array([0, 1], type=pa.timestamp("ms", tz="UTC")), "value": pa.array([-(2**63), 2**63-1], type=pa.int64())})
    raw.write(path, table)
    assert raw.read(path).equals(table)
    raw.write(path, table.slice(0, 0))
    assert raw.read(path).equals(table.slice(0, 0))


def test_lossy_ratio_and_repeated_merge(tmp_path):
    from mktpack import _native

    table = pa.table({
        "time": pa.array([0, 1, 2, 3, 4], type=pa.timestamp("ms", tz="UTC")),
        "volume": pa.array([Decimal(v) for v in ["0.000", "1.000", "0.001", "1.000", "1.000"]]),
        "taker_buy_volume": pa.array([Decimal(v) for v in ["0.0000000", "0.3334000", "0.0003334", "0.0005000", "1.0000000"]]),
    })
    lossless = tmp_path / "exact.mkp"
    path = tmp_path / "lossy.mkp"
    raw.write(lossless, table)
    assert raw.read(lossless)["taker_buy_volume"].to_pylist() == table["taker_buy_volume"].to_pylist()
    raw.write(path, table, lossy_taker=True)
    restored = raw.read(path)
    assert restored["taker_buy_volume"].to_pylist() == [Decimal(v) for v in ["0", ".333", ".000333", ".001", "1"]]
    assert restored["volume"].to_pylist() == table["volume"].to_pylist()
    assert raw.info(lossless)["version"] == 1
    assert raw.info(path)["version"] == 2
    assert raw.info(path)["metadata"]["lossy_columns"]["taker_buy_volume"]["ratio_step"] == .001
    for i in range(3):
        merged = tmp_path / f"merged-{i}.mkp"
        _native.merge([str(path)], str(merged))
        assert raw.read(merged).equals(restored)
        path = merged
    empty = tmp_path / "empty.mkp"
    raw.write(empty, table.slice(0, 0), lossy_taker=True)
    assert raw.read(empty).num_rows == 0


def test_lossy_merge_different_scales(tmp_path):
    from mktpack import _native

    paths = []
    for i, (volume, buy, lossy) in enumerate([
        ("1.00", ".3334", True),
        (".001", ".0003334", False),
    ]):
        path = tmp_path / f"part-{i}.mkp"
        raw.write(path, pa.table({"time": [i], "volume": [Decimal(volume)],
                                 "taker_buy_volume": [Decimal(buy)]}), lossy_taker=lossy)
        paths.append(str(path))
    for sources in [paths, paths[::-1]]:
        merged = tmp_path / "merged.mkp"
        _native.merge(sources, str(merged))
        restored = raw.read(merged)
        assert restored["volume"].to_pylist() == [Decimal("1"), Decimal(".001")]
        assert restored["taker_buy_volume"].to_pylist() == [Decimal(".333"), Decimal(".000333")]
        rewritten = tmp_path / "rewritten.mkp"
        _native.merge([str(merged)], str(rewritten))
        assert raw.read(rewritten).equals(restored)

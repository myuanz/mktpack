from decimal import Decimal, localcontext
import pyarrow as pa
import pytest
from mktpack import raw


def test_exact_roundtrip(tmp_path):
    table = pa.table({"time": pa.array([1, 1, 3], type=pa.timestamp("ms", tz="UTC")), "funding_rate": pa.array([Decimal("-.000001"), Decimal(".234567"), Decimal("0")]), "symbol": ["BTCUSDT"] * 3})
    path = tmp_path / "funding.mkp"
    raw.write(path, table)
    restored = raw.read(path)
    assert restored["time"].equals(table["time"])
    assert restored["funding_rate"].type == pa.float64()
    assert restored["funding_rate"].to_pylist() == [-0.000001, 0.234567, 0.0]
    assert restored["symbol"].type == pa.dictionary(pa.int32(), pa.string())
    assert restored["symbol"].to_pylist() == ["BTCUSDT"] * 3
    # 验证含 dictionary symbol 与 float64 列的表可直接再次写入
    rewritten = tmp_path / "funding2.mkp"
    raw.write(rewritten, restored)
    assert raw.read(rewritten).equals(restored)


def test_sliced_chunks_preserve_exact_values(tmp_path):
    clock = pa.array([-1000, 0, 2000, 5000], type=pa.timestamp("us", tz="Asia/Shanghai"))
    prices = pa.array([None, Decimal("-12345.67"), Decimal("0.01"), Decimal("98765.43")],
                      type=pa.decimal128(7, 2))
    contracts = pa.array(["OTHER", "CU2610", "CU2610", "CU2610"], type=pa.large_string())
    table = pa.table({
        "time": pa.chunked_array([clock.slice(1, 2), clock.slice(3)]),
        "price": pa.chunked_array([prices.slice(1, 1), prices.slice(2)]),
        "quantity": pa.array([0, 2**31, 2**32-1], type=pa.uint32()),
        "symbol": pa.chunked_array([contracts.slice(1, 2), contracts.slice(3)]),
    })
    path = tmp_path / "chunks.mkp"
    with localcontext() as context:
        context.prec = 3
        raw.write(path, table)
    restored = raw.read(path)
    assert restored["price"].to_pylist() == [-12345.67, 0.01, 98765.43]
    assert restored["time"].type == pa.timestamp("ms", tz="UTC")
    assert restored["time"].cast(pa.int64()).to_pylist() == [0, 2, 5]
    assert restored["quantity"].to_pylist() == [0, 2**31, 2**32-1]
    assert restored["symbol"].to_pylist() == ["CU2610"]*3


def test_symbol_must_be_constant_across_chunks(tmp_path):
    table = pa.table({"time": [0, 1], "symbol": pa.chunked_array([["CU2610"], ["CU2611"]])})
    with pytest.raises(ValueError):
        raw.write(tmp_path / "symbols.mkp", table)


def test_numeric_strings_and_floats(tmp_path):
    table = pa.table({
        "time": [1, 2],
        "text": pa.chunked_array([["1e2"], ["-0.000210"]]),
        "real": [0.1, -2.75],
    })
    path = tmp_path / "values.mkp"
    raw.write(path, table)
    cols = {c["name"]: c for c in raw.info(path)["columns"]}
    assert cols["text"]["scale"] == 6 and cols["text"]["type"] == "real"
    restored = raw.read(path)
    assert restored["text"].to_pylist() == [100.0, -0.00021]
    assert restored["real"].to_pylist() == [0.1, -2.75]


def test_timestamp_rejects_submillisecond_precision(tmp_path):
    table = pa.table({"time": pa.array([1001], type=pa.timestamp("us"))})
    with pytest.raises(ValueError):
        raw.write(tmp_path / "clock.mkp", table)


def test_schema_rescale(tmp_path):
    path = tmp_path / "data.mkp"
    table = pa.table({"time": [1], "value": [Decimal("1.23")]})
    raw.write(path, table, schema={"time": {"type": "timestamp", "scale": 0}, "value": {"type": "decimal", "scale": 4}})
    cols = {c["name"]: c for c in raw.info(path)["columns"]}
    assert cols["value"]["type"] == "real" and cols["value"]["scale"] == 4
    assert raw.read(path)["value"][0].as_py() == 1.23
    with pytest.raises(ValueError):
        raw.write(path, table, schema={"time": {"type": "timestamp", "scale": 0}, "value": {"type": "real", "scale": 1}})

    value = Decimal("12345678901234.120000000000000")
    table = pa.table({"time": [1], "value": pa.array([value], type=pa.decimal128(30, 15))})
    raw.write(path, table, schema={"time": {"type": "timestamp"}, "value": {"type": "real", "scale": 2}})
    assert raw.read(path)["value"][0].as_py() == 12345678901234.12


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
    assert raw.read(lossless)["taker_buy_volume"].to_pylist() == [0.0, 0.3334, 0.0003334, 0.0005, 1.0]
    raw.write(path, table, lossy_taker=True)
    restored = raw.read(path)
    assert restored["taker_buy_volume"].to_pylist() == [0.0, 0.333, 0.000333, 0.001, 1.0]
    assert restored["volume"].to_pylist() == [0.0, 1.0, 0.001, 1.0, 1.0]
    assert raw.info(lossless)["version"] == 1
    assert raw.info(path)["version"] == 1
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
        assert restored["volume"].to_pylist() == [1.0, 0.001]
        assert restored["taker_buy_volume"].to_pylist() == [0.333, 0.000333]
        rewritten = tmp_path / "rewritten.mkp"
        _native.merge([str(merged)], str(rewritten))
        assert raw.read(rewritten).equals(restored)

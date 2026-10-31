from decimal import Decimal
import json
import sqlite3
import pyarrow as pa
import mktpack
from mktpack import raw


def test_query_scales_and_empty_schema(tmp_path):
    db = sqlite3.connect(tmp_path / "mkt.sqlite")
    db.executescript("""
        CREATE TABLE __group__(name TEXT PRIMARY KEY, kind TEXT);
        CREATE TABLE __col__(group_name TEXT, name TEXT, schema TEXT);
        CREATE TABLE __timefile__(symbol TEXT, group_name TEXT, path TEXT, first_time INTEGER, last_time INTEGER, rows INTEGER);
        INSERT INTO __group__ VALUES ('oi', 'timeseries');
    """)
    schema = {"time": {"type": "timestamp", "scale": 0}, "value": {"type": "decimal", "scale": 3}}
    for name, spec in schema.items():
        db.execute("INSERT INTO __col__ VALUES (?, ?, ?)", ("oi", name, json.dumps(spec)))
    for i, value in enumerate([Decimal("1.2"), Decimal("2.345")]):
        table = pa.table({"time": pa.array([i], type=pa.timestamp("ms", tz="UTC")), "value": [value]})
        path = f"{i}.mkp"
        raw.write(tmp_path / path, table, metadata={"symbol": "BTCUSDT"})
        db.execute("INSERT INTO __timefile__ VALUES (?, ?, ?, ?, ?, ?)", ("BTCUSDT", "oi", path, i, i, 1))
    db.commit()
    group = mktpack.open(tmp_path)["oi"]
    result = group.read(symbol="BTCUSDT")
    assert result["value"].type == pa.decimal128(38, 3)
    assert result["value"].to_pylist() == [Decimal("1.200"), Decimal("2.345")]
    assert group.read(symbol="BTCUSDT", start="2024-01-01").schema == result.schema
    assert group.read(columns=["value"]).column_names == ["value"]

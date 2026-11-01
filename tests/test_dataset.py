from decimal import Decimal
import json
import sqlite3
import pyarrow as pa
import mktpack
from mktpack import raw


def test_query_scales_and_empty_schema(tmp_path):
    db = sqlite3.connect(tmp_path / "mkt.sqlite")
    db.executescript("""
        CREATE TABLE __group__(id INTEGER PRIMARY KEY, name TEXT UNIQUE, kind TEXT, schema TEXT,storage TEXT NOT NULL DEFAULT 'mkp',location TEXT NOT NULL DEFAULT '',table_name TEXT NOT NULL DEFAULT '',time_column TEXT NOT NULL DEFAULT 'time',symbol_column TEXT NOT NULL DEFAULT 'symbol_id');
        CREATE TABLE __sqlite_partitions__(group_id INTEGER,start INTEGER,end INTEGER,volume INTEGER,rows INTEGER,first_time INTEGER,last_time INTEGER,PRIMARY KEY(group_id,start)) WITHOUT ROWID;
        CREATE TABLE __symbols__(id INTEGER PRIMARY KEY, name TEXT UNIQUE);
        CREATE TABLE __timefile__(id INTEGER PRIMARY KEY, group_id INTEGER, symbol_id INTEGER, name TEXT, first_time INTEGER, last_time INTEGER, rows INTEGER, volume INTEGER DEFAULT 0, UNIQUE(group_id,symbol_id,name));
        INSERT INTO __symbols__ VALUES (1, 'BTCUSDT');
    """)
    schema = {"time": {"type": "timestamp", "scale": 0}, "value": {"type": "decimal", "scale": 3}}
    db.execute("INSERT INTO __group__(id,name,kind,schema) VALUES (1, ?, ?, ?)", ("oi", "timeseries", json.dumps(schema)))
    for i, value in enumerate([Decimal("1.2"), Decimal("2.345")]):
        table = pa.table({"time": pa.array([i], type=pa.timestamp("ms", tz="UTC")), "value": [value]})
        path = f"{i}.mkp"
        raw.write(tmp_path / 'oi' / 'BTCUSDT' / path, table, metadata={"symbol": "BTCUSDT"})
        db.execute("INSERT INTO __timefile__(group_id,symbol_id,name,first_time,last_time,rows) VALUES (1,1,?,?,?,1)", (path, i, i))
    db.commit()
    group = mktpack.open(tmp_path)["oi"]
    result = group.read(symbol="BTCUSDT")
    assert result["value"].type == pa.decimal128(38, 3)
    assert result["value"].to_pylist() == [Decimal("1.200"), Decimal("2.345")]
    assert group.read(symbol="BTCUSDT", start="2024-01-01").schema == result.schema
    assert group.read(columns=["value"]).column_names == ["value"]

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import sqlite3
import pyarrow as pa
import mktpack
import pytest
from mktpack import raw


def test_query_scales_and_empty_schema(tmp_path):
    tmp_path = tmp_path / "行情"
    tmp_path.mkdir()
    db = sqlite3.connect(tmp_path / "mkt.sqlite")
    db.executescript("""
        CREATE TABLE __group__(id INTEGER PRIMARY KEY, name TEXT UNIQUE, kind TEXT, schema TEXT,storage TEXT NOT NULL DEFAULT 'mkp',location TEXT NOT NULL DEFAULT '',table_name TEXT NOT NULL DEFAULT '',time_column TEXT NOT NULL DEFAULT 'time',symbol_column TEXT NOT NULL DEFAULT 'symbol_id');
        CREATE TABLE __sqlite_partitions__(group_id INTEGER,start INTEGER,end INTEGER,volume_id INTEGER,rows INTEGER,first_time INTEGER,last_time INTEGER,PRIMARY KEY(group_id,start)) WITHOUT ROWID;
        CREATE TABLE __symbols__(id INTEGER PRIMARY KEY, name TEXT UNIQUE);
        CREATE TABLE __volumes__(id INTEGER PRIMARY KEY,path TEXT NOT NULL UNIQUE);
        CREATE TABLE __timefile__(id INTEGER PRIMARY KEY, group_id INTEGER NOT NULL, symbol_id INTEGER NOT NULL, period INTEGER NOT NULL, first_time INTEGER NOT NULL, last_time INTEGER NOT NULL, rows INTEGER NOT NULL, volume_id INTEGER, UNIQUE(group_id,symbol_id,period));
        INSERT INTO __symbols__ VALUES (1, 'BTCUSDT');
    """)
    schema = {"time": {"type": "timestamp", "scale": 0}, "value": {"type": "decimal", "scale": 3}}
    db.execute("INSERT INTO __group__(id,name,kind,schema) VALUES (1, ?, ?, ?)", ("oi", "timeseries", json.dumps(schema)))
    for i, value in enumerate([Decimal("1.2"), Decimal("2.345")]):
        table = pa.table({"time": pa.array([i], type=pa.timestamp("ms", tz="UTC")), "value": [value]})
        path = f"{i}.mkp"
        raw.write(tmp_path / 'oi' / 'BTCUSDT' / path, table, metadata={"symbol": "BTCUSDT"})
        db.execute("INSERT INTO __timefile__(group_id,symbol_id,period,first_time,last_time,rows) VALUES (1,1,?,?,?,1)", (i, i, i))
    db.commit()
    dataset = mktpack.open(tmp_path)
    assert dataset.root == str(tmp_path)
    with pytest.raises(KeyError, match='missing'):
        dataset['missing']
    group = dataset["oi"]
    result = group.read(symbol="BTCUSDT")
    assert result["value"].type == pa.decimal128(38, 3)
    assert result["value"].to_pylist() == [Decimal("1.200"), Decimal("2.345")]
    assert group.read(symbol="BTCUSDT", start="2024-01-01").schema == result.schema
    assert group.read(columns=["value"]).column_names == ["value"]
    assert group.read(start=19700101, end=date(1970, 1, 2)).equals(result)
    assert group.read(symbols=["BTCUSDT"], start="1970-01-01T08:00:00.001+08:00",
                      end=datetime(1970, 1, 1, 8, 0, 0, 2000, tzinfo=timezone(timedelta(hours=8)))).equals(result.slice(1))

    identifier = db.execute("SELECT id FROM __timefile__ WHERE period=1").fetchone()[0]
    volume = tmp_path / "absolute.data"
    with sqlite3.connect(volume) as packed:
        packed.execute("CREATE TABLE blob(id INTEGER PRIMARY KEY,bytes BLOB NOT NULL)")
        packed.execute("INSERT INTO blob VALUES(?,?)", (identifier, (tmp_path / "oi/BTCUSDT/1.mkp").read_bytes()))
    db.execute("INSERT INTO __volumes__(path) VALUES(?)", (str(volume),))
    db.execute("UPDATE __timefile__ SET volume_id=(SELECT id FROM __volumes__ WHERE path=?) WHERE id=?", (str(volume), identifier))
    db.commit()
    (tmp_path / "oi/BTCUSDT/1.mkp").unlink()
    assert group.read(symbol="BTCUSDT")["value"].to_pylist() == [Decimal("1.200"), Decimal("2.345")]


def test_dataset_discovery_and_symbols(tmp_path):
    db = sqlite3.connect(tmp_path / "mkt.sqlite")
    db.executescript("""
        CREATE TABLE __group__(id INTEGER PRIMARY KEY, name TEXT UNIQUE, kind TEXT, schema TEXT,storage TEXT NOT NULL DEFAULT 'mkp',location TEXT NOT NULL DEFAULT '',table_name TEXT NOT NULL DEFAULT '',time_column TEXT NOT NULL DEFAULT 'time',symbol_column TEXT NOT NULL DEFAULT 'symbol_id');
        CREATE TABLE __sqlite_partitions__(group_id INTEGER,start INTEGER,end INTEGER,volume_id INTEGER,rows INTEGER,first_time INTEGER,last_time INTEGER,PRIMARY KEY(group_id,start)) WITHOUT ROWID;
        CREATE TABLE __symbols__(id INTEGER PRIMARY KEY, name TEXT UNIQUE);
        CREATE TABLE __volumes__(id INTEGER PRIMARY KEY,path TEXT NOT NULL UNIQUE);
        CREATE TABLE __timefile__(id INTEGER PRIMARY KEY, group_id INTEGER NOT NULL, symbol_id INTEGER NOT NULL, period INTEGER NOT NULL, first_time INTEGER NOT NULL, last_time INTEGER NOT NULL, rows INTEGER NOT NULL, volume_id INTEGER, UNIQUE(group_id,symbol_id,period));
        INSERT INTO __symbols__ VALUES (1, 'BTCUSDT'), (2, 'ETHUSDT'), (3, 'SOLUSDT');
    """)
    mkp_schema = {"time": {"type": "timestamp", "scale": 0}, "value": {"type": "integer", "scale": 0}}
    sql_schema = {"time": {"type": "timestamp", "scale": 0}, "rate": {"type": "integer", "scale": 0}}
    db.execute("INSERT INTO __group__(id,name,kind,schema) VALUES (1, 'kline', 'timeseries', ?)", (json.dumps(mkp_schema),))
    db.execute("INSERT INTO __group__(id,name,kind,schema,storage,location,table_name,time_column,symbol_column) "
               "VALUES (2, 'funding', 'timeseries', ?, 'sqlite', 'funding.sqlite', 'funding', 'time', 'symbol_id')",
               (json.dumps(sql_schema),))
    db.execute("INSERT INTO __group__(id,name,kind,schema,storage,table_name) VALUES (3, 'info', 'ordinary', '{}', 'sqlite', 'info')")
    db.execute("CREATE TABLE info(exchange TEXT)")
    db.execute("INSERT INTO info VALUES ('binance')")

    # BTCUSDT 在 1000 和 5000 有记录，[2000, 4000) 内实际无记录
    btc = pa.table({"time": pa.array([1000, 5000], type=pa.timestamp("ms", tz="UTC")), "value": [1, 2]})
    raw.write(tmp_path / "kline/BTCUSDT/2024.mkp", btc, metadata={"symbol": "BTCUSDT"})
    db.execute("INSERT INTO __timefile__(group_id,symbol_id,period,first_time,last_time,rows) VALUES (1,1,2024,1000,5000,2)")

    # ETHUSDT 在 3000 有记录
    eth = pa.table({"time": pa.array([3000], type=pa.timestamp("ms", tz="UTC")), "value": [3]})
    raw.write(tmp_path / "kline/ETHUSDT/2024.mkp", eth, metadata={"symbol": "ETHUSDT"})
    db.execute("INSERT INTO __timefile__(group_id,symbol_id,period,first_time,last_time,rows) VALUES (1,2,2024,3000,3000,1)")
    db.commit()

    funding_db = sqlite3.connect(tmp_path / "funding.sqlite")
    funding_db.executescript("""
        CREATE TABLE funding(symbol_id INTEGER, time INTEGER, rate INTEGER);
        INSERT INTO funding VALUES (1, 1000, 10), (3, 3000, 20);
    """)
    funding_db.commit()

    dataset = mktpack.open(tmp_path)
    assert dataset.keys() == ["funding", "info", "kline"]
    assert list(dataset) == ["funding", "info", "kline"]
    assert len(dataset) == 3
    assert "kline" in dataset and "missing" not in dataset
    assert [g["name"] for g in dataset.describe()] == ["funding", "info", "kline"]

    kline = dataset["kline"]
    assert kline.symbols() == ["BTCUSDT", "ETHUSDT"]
    assert kline.symbols(start="1970-01-01T00:00:02Z", end="1970-01-01T00:00:04Z") == ["ETHUSDT"]
    assert kline.symbols(start="1970-01-01T00:00:05Z", end="1970-01-01T00:00:06Z") == ["BTCUSDT"]
    assert kline.symbols(start="1970-01-01T00:00:06Z", end="1970-01-01T00:00:05Z") == []

    funding = dataset["funding"]
    assert funding.symbols() == ["BTCUSDT", "SOLUSDT"]
    assert funding.symbols(start="1970-01-01T00:00:02Z", end="1970-01-01T00:00:04Z") == ["SOLUSDT"]

    # describe() 返回 UTC datetime，支持单合约描述，且毫秒时间戳可直接传给 read/symbols
    kline_desc = kline.describe()
    assert kline_desc["first_time"] == datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    assert kline_desc["last_time"] == datetime(1970, 1, 1, 0, 0, 5, tzinfo=timezone.utc)
    eth_desc = kline.describe("ETHUSDT")
    assert eth_desc["rows"] == 1 and eth_desc["symbol"] == "ETHUSDT"
    assert eth_desc["first_time"] == datetime(1970, 1, 1, 0, 0, 3, tzinfo=timezone.utc)
    assert kline.describe("UNKNOWN")["first_time"] is None
    assert funding.describe("SOLUSDT")["rows"] == 1
    assert funding.describe("SOLUSDT")["first_time"] == datetime(1970, 1, 1, 0, 0, 3, tzinfo=timezone.utc)
    assert kline.symbols(start=2000, end=4000) == ["ETHUSDT"]
    assert kline.read(symbol="ETHUSDT", start=eth_desc["first_time"], end=4000).num_rows == 1

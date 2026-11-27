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

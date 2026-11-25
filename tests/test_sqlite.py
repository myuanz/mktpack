from decimal import Decimal
import json
import os
from pathlib import Path
import sqlite3
import shutil
import subprocess

import mktpack
import pytest
from test_package import source, run_package, check_package, milliseconds


def test_sqlite_query_and_package(source, tmp_path):
    root, db = source
    schema = {'time': {'type': 'timestamp', 'scale': 0},
              'rate': {'type': 'decimal', 'scale': 10, 'scale_column': 'scale'},
              'period': {'type': 'integer', 'scale': 0}, 'symbol': {'type': 'string'}}
    db.execute("ATTACH DATABASE ? AS rates", (str(root / 'rates.sqlite'),))
    db.execute('CREATE TABLE rates.settlements(symbol_id INTEGER,time INTEGER,rate INTEGER,scale INTEGER,period INTEGER,PRIMARY KEY(symbol_id,time)) WITHOUT ROWID')
    db.execute("INSERT INTO __group__(name,kind,storage,location,schema) VALUES (?,?,'sqlite','rates.sqlite',?)",
               ('settlements', 'timeseries', json.dumps(schema)))
    ids = dict(db.execute('SELECT name,id FROM __symbols__'))
    db.executemany('INSERT INTO settlements VALUES (?,?,?,?,?)', [
        (ids['BTCUSDT'], milliseconds('2024-01-01'), 1, 4, 8),
        (ids['BTCUSDT'], milliseconds('2025-01-01'), -123456789, 10, 4),
        (ids['DOGEUSDT'], milliseconds('2025-01-01'), 123456789, 10, 1),
    ])
    db.commit()
    group = mktpack.open(root)['settlements']
    assert group.describe()['rows'] == 3
    assert group.read(symbol='BTCUSDT')['rate'].to_pylist() == [Decimal('.0001'), Decimal('-.0123456789')]
    assert group.read(start=20250101, end=20260101).num_rows == 2
    assert group.read(end=20250101).num_rows == 1
    assert group.read(symbol='missing').schema == group.read().schema
    assert group.read(columns=['rate']).column_names == ['rate']
    packed = tmp_path / 'funding-only'
    result = run_package(root, packed, groups=['settlements'], symbols=['BTCUSDT'], start=20250101, end=20260101)
    assert result['files'] == 0 and result['volumes'] == 1
    assert result['rows'] == result['sqlite_rows'] == 1
    assert mktpack.open(packed)['settlements'].describe()['rows'] == 1
    with sqlite3.connect(packed / 'mkt.sqlite') as index:
        assert index.execute("SELECT name FROM sqlite_master WHERE name='settlements'").fetchall() == []
    actual = mktpack.open(packed)['settlements'].read()
    assert actual.equals(group.read(symbol='BTCUSDT', start=20250101, end=20260101))
    check_package(packed)
    repacked = tmp_path / 'repacked'
    run_package(packed, repacked)
    assert mktpack.open(repacked)['settlements'].read().equals(actual)
    all_groups = tmp_path / 'all'
    run_package(root, all_groups)
    assert mktpack.open(all_groups)['settlements'].read().equals(group.read())
    excluded = tmp_path / 'kline-only'
    run_package(root, excluded, groups=['kline'])
    assert 'settlements' not in {g['name'] for g in mktpack.open(excluded).describe()}
    with sqlite3.connect(excluded / 'mkt.sqlite') as copied:
        assert copied.execute('SELECT count(*) FROM __sqlite_partitions__').fetchone()[0] == 0


@pytest.fixture
def monthly_sqlite(source):
    root, db = source
    schema = {'time': {'type': 'timestamp', 'scale': 0},
              'rate': {'type': 'decimal', 'scale': 8, 'scale_column': 'scale'},
              'period': {'type': 'integer', 'scale': 0}, 'symbol': {'type': 'string'}}
    db.execute("ATTACH DATABASE ? AS rates", (str(root / 'rates.sqlite'),))
    db.execute('CREATE TABLE rates.settlements(symbol_id INTEGER,time INTEGER,rate INTEGER,scale INTEGER,period INTEGER,PRIMARY KEY(symbol_id,time)) WITHOUT ROWID')
    db.execute("INSERT INTO __group__(name,kind,storage,location,schema) VALUES (?,?,'sqlite','rates.sqlite',?)",
               ('settlements', 'timeseries', json.dumps(schema)))
    ids = [r[0] for r in db.execute('SELECT id FROM __symbols__')]
    for month in ['2024-01-01', '2024-02-01']:
        start = milliseconds(month)
        db.executemany('INSERT INTO settlements VALUES (?,?,?,?,?)',
                       ((symbol, start + i*3600000, 123456789+i, 8, 1) for symbol in ids for i in range(200)))
    db.commit()
    return root


def test_sqlite_whole_month_volumes(monthly_sqlite, tmp_path):
    root = monthly_sqlite
    options = dict(groups=['settlements'])
    january = tmp_path / 'january'
    run_package(root, january, end=20240201, **options)
    limit = (january / 'mkt.data.1').stat().st_size
    packed = tmp_path / 'packed'
    result = run_package(root, packed, max_file_size=limit, **options)
    assert result['volumes'] == 2
    assert all(p.stat().st_size <= limit for p in packed.glob('mkt.data.*'))
    with sqlite3.connect(packed / 'mkt.sqlite') as db:
        parts = db.execute('SELECT start,end,volume_id,rows FROM __sqlite_partitions__ ORDER BY start').fetchall()
    assert parts == [(milliseconds('2024-01-01'), milliseconds('2024-02-01'), 1, 400),
                     (milliseconds('2024-02-01'), milliseconds('2024-03-01'), 2, 400)]
    original = mktpack.open(root)['settlements']
    actual = mktpack.open(packed)['settlements']
    assert actual.read().equals(original.read())
    assert actual.describe() == original.describe()
    check_package(packed)
    repacked = tmp_path / 'repacked'
    run_package(packed, repacked, symbols=['BTCUSDT'], start=20240105, end=20240205)
    assert mktpack.open(repacked)['settlements'].read().equals(
        original.read(symbol='BTCUSDT', start=20240105, end=20240205))
    empty = tmp_path / 'empty'
    result = run_package(packed, empty, start=20300101)
    assert result['volumes'] == result['sqlite_rows'] == 0
    assert mktpack.open(empty)['settlements'].read().schema == original.read().schema
    assert mktpack.open(empty)['settlements'].describe()['rows'] == 0
    failed = tmp_path / 'failed'
    run_package(root, failed, max_file_size=limit-1, end=20240201, error='整月数据需要至少', **options)
    assert list(failed.iterdir()) == []
    # 时间筛选只打开相关卷
    (packed / 'mkt.data.1').unlink()
    assert actual.read(start=20240201).equals(original.read(start=20240201))


def test_sqlite_and_blob_share_volume(monthly_sqlite, tmp_path):
    packed = tmp_path / 'mixed'
    run_package(monthly_sqlite, packed)
    with sqlite3.connect(packed / 'mkt.sqlite') as db:
        funding_volume = db.execute('SELECT volume_id FROM __sqlite_partitions__ LIMIT 1').fetchone()[0]
        funding_path = db.execute('SELECT path FROM __volumes__ WHERE id=?', (funding_volume,)).fetchone()[0]
        table = db.execute("SELECT table_name FROM __group__ WHERE name='settlements'").fetchone()[0]
        assert db.execute('SELECT count(*) FROM __timefile__ WHERE volume_id=?', (funding_volume,)).fetchone()[0] > 0
    with sqlite3.connect(packed / funding_path) as volume:
        assert volume.execute('SELECT count(*) FROM blob').fetchone()[0] > 0
        assert volume.execute(f'SELECT count(*) FROM {table}').fetchone()[0] == 800
    check_package(packed)


def test_parallel_mixed_package_matches_serial(monthly_sqlite, tmp_path):
    serial = tmp_path / 'serial'
    parallel = tmp_path / 'parallel'
    expected = run_package(monthly_sqlite, serial, jobs=1, max_file_size=32768)
    assert run_package(monthly_sqlite, parallel, jobs=4, max_file_size=32768) == expected
    assert {p.name: p.read_bytes() for p in parallel.iterdir()} == {
        p.name: p.read_bytes() for p in serial.iterdir()
    }
    repacked = tmp_path / 'repacked'
    run_package(parallel, repacked, jobs=4, max_file_size=32768)
    for group in ['kline', 'oi', 'settlements']:
        assert mktpack.open(repacked)[group].read().equals(mktpack.open(monthly_sqlite)[group].read())


@pytest.mark.parametrize('damage', ['volume', 'rows', 'mapping'])
def test_check_sqlite_volume_damage(monthly_sqlite, tmp_path, damage):
    packed = tmp_path / 'damaged'
    run_package(monthly_sqlite, packed, groups=['settlements'])
    if damage == 'volume':
        (packed / 'mkt.data.1').unlink()
    elif damage == 'rows':
        with sqlite3.connect(packed / 'mkt.data.1') as volume:
            table = volume.execute("SELECT name FROM sqlite_master WHERE name LIKE 'data_%'").fetchone()[0]
            volume.execute(f'DELETE FROM {table}')
    else:
        with sqlite3.connect(packed / 'mkt.sqlite') as db:
            db.execute('UPDATE __sqlite_partitions__ SET volume_id=999')
    result = subprocess.run([shutil.which('mkt'), '--root', str(packed), 'binance', 'check'], capture_output=True, text=True)
    assert result.returncode == 1
    assert json.loads(result.stdout)['errors']


@pytest.mark.skipif(not os.environ.get('MKTPACK_REAL_DATA'), reason='设置 MKTPACK_REAL_DATA 运行真实数据回归')
def test_real_funding_volumes(tmp_path, real_dataset):
    root = real_dataset
    original = mktpack.open(root)['funding_rate_settlement'].read()
    packed = tmp_path / 'funding-volumes'
    result = run_package(root, packed, groups=['funding_rate_settlement'], max_file_size=32768)
    assert result['volumes'] > 1
    assert result['sqlite_rows'] == original.num_rows
    assert all(p.stat().st_size <= 32768 for p in packed.glob('mkt.data.*'))
    assert mktpack.open(packed)['funding_rate_settlement'].read().equals(original)
    with sqlite3.connect(packed / 'mkt.sqlite') as db:
        parts = db.execute('SELECT start,end,volume_id,rows FROM __sqlite_partitions__ ORDER BY start').fetchall()
        assert sum(p[3] for p in parts) == original.num_rows
        assert len(parts) == len(set(p[0] for p in parts))
    check_package(packed)
    repacked = tmp_path / 'funding-selected'
    run_package(packed, repacked, symbols=['BTCUSDT', 'DOGEUSDT'], start=20251215, end=20260201, max_file_size=32768)
    assert mktpack.open(repacked)['funding_rate_settlement'].read().equals(
        mktpack.open(root)['funding_rate_settlement'].read(symbols=['BTCUSDT', 'DOGEUSDT'], start=20251215, end=20260201))
    print(json.dumps(dict(result, months=len(parts)), ensure_ascii=False))




def test_sqlite_types_without_symbol_and_external_ordinary(source, tmp_path):
    root, db = source
    db.execute('ATTACH DATABASE ? AS extra', (str(root / 'observations.sqlite'),))
    db.executescript('''
        CREATE TABLE extra.measurements(at INTEGER, value REAL, label TEXT, payload BLOB, exact INTEGER, mixed);
        CREATE INDEX extra.by_time ON measurements(at);
    ''')
    schema = {'at': {'type': 'timestamp'}, 'value': {'type': 'real'},
              'label': {'type': 'string'}, 'payload': {'type': 'blob'},
              'exact': {'type': 'decimal', 'scale': 4}, 'mixed': {'type': 'any'}}
    db.execute("INSERT INTO __group__(name,kind,storage,location,table_name,time_column,symbol_column,schema) VALUES ('observations','timeseries','sqlite','observations.sqlite','measurements','at','',?)", (json.dumps(schema),))
    t = milliseconds('2024-01-01')
    rows = [(t, 1.25, '中文\x00文本', b'\x00\xff', 123, 12),
            (t, None, None, b'', None, 'text'),
            (milliseconds('2024-02-01'), -2.5, '', None, -3, b'bytes')]
    db.executemany('INSERT INTO extra.measurements VALUES (?,?,?,?,?,?)', rows)
    db.execute('ATTACH DATABASE ? AS normal', (str(root / 'info.sqlite'),))
    db.executescript('''
        CREATE TABLE normal.records(k TEXT, v BLOB);
        INSERT INTO normal.records VALUES ('one',x'00ff'),('two',x'');
        INSERT INTO __group__(name,kind,storage,location,table_name) VALUES ('details','ordinary','sqlite','info.sqlite','records');
    ''')
    db.commit()
    group = mktpack.open(root)['observations']
    original = group.read()
    assert 'symbol' not in original.column_names
    assert original['value'].to_pylist() == [1.25, None, -2.5]
    assert original['payload'].to_pylist() == [b'\x00\xff', b'', None]
    assert original['mixed'].to_pylist() == [12, 'text', b'bytes']
    assert original['label'].to_pylist() == ['中文\x00文本', None, '']
    assert original['exact'].to_pylist() == [Decimal('.0123'), None, Decimal('-.0003')]
    assert group.read(start=20300101).schema == original.schema
    with pytest.raises(RuntimeError, match='没有品种列'):
        group.read(symbol='BTCUSDT')
    packed = tmp_path / 'generic'
    report = run_package(root, packed, groups=['observations'], symbols=['BTCUSDT'])
    assert report['bytes'] == sum(p.stat().st_size for p in packed.rglob('*') if p.is_file())
    assert mktpack.open(packed)['observations'].read().equals(original)
    assert mktpack.open(packed)['details'] == mktpack.open(root)['details'] == [
        {'k': 'one', 'v': b'\x00\xff'}, {'k': 'two', 'v': b''}]
    assert (packed / 'info.sqlite').is_file()
    check_package(packed)
    repacked = tmp_path / 'generic-selected'
    run_package(packed, repacked, start=20240115)
    assert mktpack.open(repacked)['observations'].read().equals(group.read(start=20240115))
    assert mktpack.open(repacked)['details'] == mktpack.open(root)['details']


def test_two_sqlite_groups_same_table_name(source, tmp_path):
    root, db = source
    schema = json.dumps({'time': {'type': 'timestamp'}, 'v': {'type': 'integer'}})
    for i in [1, 2]:
        location = f'{i}.sqlite'
        with sqlite3.connect(root / location) as external:
            external.execute('CREATE TABLE data(time INTEGER, v INTEGER)')
            external.execute('INSERT INTO data VALUES (?,?)', (milliseconds('2024-01-01'), i))
        db.execute("INSERT INTO __group__(name,kind,storage,location,table_name,symbol_column,schema) VALUES (?,'timeseries','sqlite',?,'data','',?)", (f'group{i}', location, schema))
    db.commit()
    destination = tmp_path / 'groups'
    run_package(root, destination, groups=['group1', 'group2'])
    for i in [1, 2]:
        assert mktpack.open(destination)[f'group{i}'].read()['v'].to_pylist() == [i]
    check_package(destination)


def test_sqlite_column_mapping_and_nullable_scale(source, tmp_path):
    root, db = source
    with sqlite3.connect(root / 'mapped.sqlite') as external:
        external.execute('CREATE TABLE "table(quoted)"(stamp INTEGER, amount INTEGER, precision INTEGER)')
        external.executemany('INSERT INTO "table(quoted)" VALUES (?,?,?)', [(1, 12, 1), (2, None, None)])
    schema = {'time': {'type': 'timestamp', 'column': 'stamp'},
              'value': {'type': 'decimal', 'column': 'amount', 'scale_column': 'precision', 'scale': 1}}
    db.execute("INSERT INTO __group__(name,kind,storage,location,table_name,time_column,symbol_column,schema) VALUES ('mapped','timeseries','sqlite','mapped.sqlite','table(quoted)','stamp','',?)", (json.dumps(schema),))
    db.commit()
    original = mktpack.open(root)['mapped'].read()
    assert original['value'].to_pylist() == [Decimal('1.2'), None]
    destination = tmp_path / 'mapped'
    run_package(root, destination, groups=['mapped'])
    assert mktpack.open(destination)['mapped'].read().equals(original)
    check_package(destination)

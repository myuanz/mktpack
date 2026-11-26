from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess

import numpy as np
import pyarrow as pa
import pytest
import mktpack
from mktpack import raw


def run_package(root, destination, *, error=None, **options):
    command = [shutil.which('mkt'), '--root', str(root), 'package', str(destination)]
    for name, value in options.items():
        command.append('--' + name.replace('_', '-'))
        command.extend(map(str, value if isinstance(value, list) else [value]))
    result = subprocess.run(command, capture_output=True, text=True)
    if error is not None:
        assert result.returncode == 1, result.stderr
        assert error in result.stderr
        return
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def check_package(root):
    result = subprocess.run([shutil.which('mkt'), '--root', str(root), 'binance', 'check'], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['errors'] == []


def milliseconds(text):
    return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp() * 1000)


def add_file(root, db, group, symbol, period, times, values=None, *, lossy=False, quantities=None):
    columns = {'time': pa.array(times, type=pa.timestamp('ms', tz='UTC'))}
    if quantities is None:
        columns['value'] = [Decimal(v) for v in values]
    else:
        columns.update({key: [Decimal(v) for v in numbers] for key, numbers in quantities.items()})
    relative = f'{group}/{symbol}/{period}.mkp'
    raw.write(root / relative, pa.table(columns), metadata={'group': group, 'symbol': symbol}, lossy_taker=lossy)
    h = raw.info(root / relative)
    db.execute('INSERT OR IGNORE INTO __group__(name,kind) VALUES (?, ?)', (group, 'timeseries'))
    schema = json.loads(db.execute('SELECT schema FROM __group__ WHERE name=?', (group,)).fetchone()[0])
    for d in h['columns']:
        scale = max(schema.get(d['name'], {}).get('scale', 0), d['scale'])
        schema[d['name']] = {'type': d['type'], 'scale': scale}
    db.execute('UPDATE __group__ SET schema=? WHERE name=?', (json.dumps(schema), group))
    db.execute('INSERT OR IGNORE INTO __symbols__(name) VALUES (?)', (symbol,))
    symbol_id = db.execute('SELECT id FROM __symbols__ WHERE name=?', (symbol,)).fetchone()[0]
    group_id = db.execute('SELECT id FROM __group__ WHERE name=?', (group,)).fetchone()[0]
    db.execute('INSERT INTO __timefile__(group_id,symbol_id,period,first_time,last_time,rows) VALUES (?,?,?,?,?,?)',
               (group_id, symbol_id, int(period), h['first_time'], h['last_time'], h['rows']))
    db.commit()
    return root / relative


@pytest.fixture
def source(tmp_path):
    root = tmp_path / 'source'
    root.mkdir()
    db = sqlite3.connect(root / 'mkt.sqlite')
    db.executescript('''
        PRAGMA journal_mode=WAL;
        CREATE TABLE __group__(id INTEGER PRIMARY KEY,name TEXT UNIQUE,kind TEXT,schema TEXT NOT NULL DEFAULT '{}',storage TEXT NOT NULL DEFAULT 'mkp',location TEXT NOT NULL DEFAULT '',table_name TEXT NOT NULL DEFAULT '',time_column TEXT NOT NULL DEFAULT 'time',symbol_column TEXT NOT NULL DEFAULT 'symbol_id');
        CREATE TABLE __sqlite_partitions__(group_id INTEGER,start INTEGER,end INTEGER,volume_id INTEGER,rows INTEGER,first_time INTEGER,last_time INTEGER,PRIMARY KEY(group_id,start)) WITHOUT ROWID;
        CREATE TABLE __symbols__(id INTEGER PRIMARY KEY,name TEXT UNIQUE);
        CREATE TABLE __volumes__(id INTEGER PRIMARY KEY,path TEXT NOT NULL UNIQUE);
        CREATE TABLE __timefile__(id INTEGER PRIMARY KEY,group_id INTEGER NOT NULL,symbol_id INTEGER NOT NULL,period INTEGER NOT NULL,first_time INTEGER NOT NULL,last_time INTEGER NOT NULL,rows INTEGER NOT NULL,volume_id INTEGER,UNIQUE(group_id,symbol_id,period));
        CREATE TABLE exchangeinfo(timezone TEXT,symbols TEXT);
        INSERT INTO __group__(name,kind,storage) VALUES ('exchangeinfo','ordinary','sqlite');
        INSERT INTO exchangeinfo VALUES ('UTC','[{"symbol":"BTCUSDT"},{"symbol":"DOGEUSDT"}]');
        CREATE TABLE notes(key TEXT,value TEXT);
        INSERT INTO notes VALUES ('first','one'),('second','two');
        CREATE TABLE __binance_datavision_paths__(id INTEGER PRIMARY KEY,parent_id INTEGER,name TEXT,UNIQUE(parent_id,name));
        CREATE TABLE __binance_datavision_templates__(id INTEGER PRIMARY KEY,path_id INTEGER,group_id INTEGER,symbol_id INTEGER,UNIQUE(path_id,group_id,symbol_id));
        CREATE TABLE __binance_datavision_files__(template_id INTEGER,period INTEGER,file_id INTEGER,error TEXT,rows INTEGER,PRIMARY KEY(template_id,period)) WITHOUT ROWID;
        INSERT INTO __symbols__ VALUES (1,'BTCUSDT');
        INSERT INTO __group__(name,kind) VALUES ('kline','timeseries');
        INSERT INTO __binance_datavision_paths__ VALUES (1,0,'source-{period}');
        INSERT INTO __binance_datavision_templates__ VALUES (1,1,2,1);
        INSERT INTO __binance_datavision_files__ VALUES (1,202401,NULL,'模拟待重试',NULL);
        CREATE TABLE __binance_rest_funding__(symbol_id INTEGER,period INTEGER,rows INTEGER,error TEXT,covered_until INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(symbol_id,period)) WITHOUT ROWID;
        INSERT INTO __binance_rest_funding__ VALUES (1,20240101,NULL,'模拟待重试',0);
    ''')
    rng = np.random.default_rng(42)
    times = np.arange(6000) * 60000 + milliseconds('2024-01-01')
    for symbol in ['BTCUSDT', 'DOGEUSDT']:
        values = [str(Decimal(int(v)) / 1000) for v in rng.integers(0, 2**32, len(times))]
        add_file(root, db, 'kline', symbol, '2024', times, values)
    add_file(root, db, 'kline', 'BTCUSDT', 202501, [milliseconds('2025-01-01')], ['1.23'])
    duplicate = milliseconds('2025-02-01')
    add_file(root, db, 'kline', 'BTCUSDT', 202502, [duplicate, duplicate], ['2.345', '3.456'])
    add_file(root, db, 'kline', 'BTCUSDT', '2026', [milliseconds('2026-01-01')], ['4.56789'])
    add_file(root, db, 'oi', 'BTCUSDT', '2024', [milliseconds('2024-01-01')], ['12'])
    # 保持连接，验证 SQLite backup 能复制尚在 WAL 中的索引和普通表
    yield root, db
    db.close()


def payloads(root):
    with sqlite3.connect(root / 'mkt.sqlite') as db:
        rows = db.execute("SELECT g.name||'/'||s.name||'/'||f.period||'.mkp',v.path,f.id FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id JOIN __symbols__ s ON s.id=f.symbol_id JOIN __volumes__ v ON v.id=f.volume_id").fetchall()
    result = {}
    for path, volume, identifier in rows:
        with sqlite3.connect(root / volume) as db:
            result[path] = db.execute('SELECT bytes FROM blob WHERE id=?', (identifier,)).fetchone()[0]
    return result


@pytest.mark.parametrize('selection', [{}, {'start': 20240102, 'end': 20260201}])
@pytest.mark.parametrize('packed', [False, True])
def test_parallel_package_matches_serial(source, tmp_path, selection, packed):
    root, _ = source
    if packed:
        initial = tmp_path / 'input-package'
        run_package(root, initial, jobs=1, max_file_size=32768)
        root = initial
    serial = tmp_path / 'serial'
    expected = run_package(root, serial, jobs=1, max_file_size=32768, **selection)
    for jobs in [2, 4]:
        parallel = tmp_path / f'parallel-{jobs}'
        assert run_package(root, parallel, jobs=jobs, max_file_size=32768, **selection) == expected
        assert {p.name: p.read_bytes() for p in parallel.iterdir()} == {
            p.name: p.read_bytes() for p in serial.iterdir()
        }


def test_package_volumes_repack_and_index_path(source, tmp_path):
    root, db = source
    before = {p: p.read_bytes() for p in root.rglob('*.mkp')}
    destination = tmp_path / 'packed'
    result = run_package(root / 'mkt.sqlite', destination, max_file_size=32768)
    assert result['files'] == 6 and result['copied'] == 6 and result['reencoded'] == 0
    assert result['volumes'] >= 2
    assert len(list(destination.glob('mkt.data.*'))) == result['volumes']
    assert all(p.stat().st_size <= 32768 for p in destination.glob('mkt.data.*'))
    assert result['bytes'] == sum(p.stat().st_size for p in destination.iterdir())
    assert not list(destination.glob('*journal'))
    original = mktpack.open(root)
    for location in [destination, destination / 'mkt.sqlite']:
        packed = mktpack.open(location)
        assert packed.describe() == original.describe()
        for group in ['kline', 'oi']:
            assert packed[group].read().equals(original[group].read())
        assert packed['exchangeinfo'] == original['exchangeinfo']
    with sqlite3.connect(destination / 'mkt.sqlite') as copied:
        assert copied.execute('SELECT * FROM notes').fetchall() == db.execute('SELECT * FROM notes').fetchall()
        for name in ['__binance_datavision_files__', '__binance_datavision_templates__',
                     '__binance_datavision_paths__', '__binance_rest_funding__']:
            assert copied.execute(f'SELECT * FROM {name}').fetchall() == db.execute(f'SELECT * FROM {name}').fetchall()
        assert copied.execute('PRAGMA application_id').fetchone()[0] == 0x4d4b5450
    assert db.execute('SELECT count(*) FROM __binance_datavision_files__').fetchone()[0] == 1
    frames = payloads(destination)
    assert frames['kline/BTCUSDT/2024.mkp'] == (root / 'kline/BTCUSDT/2024.mkp').read_bytes()
    merged = mktpack.open(destination)['kline'].read(symbol='BTCUSDT', start='2025-01-01', end='2026-01-01')
    assert merged['value'].to_pylist() == [Decimal('1.23'), Decimal('2.345'), Decimal('3.456')]
    repacked = tmp_path / 'repacked'
    result2 = run_package(destination / 'mkt.sqlite', repacked, max_file_size=65536)
    assert result2['copied'] == 6 and result2['reencoded'] == 0
    assert payloads(repacked) == frames
    assert mktpack.open(repacked)['kline'].read().equals(original['kline'].read())
    check_package(repacked)
    assert all(p.read_bytes() == data for p, data in before.items())
    index_before = (destination / 'mkt.sqlite').read_bytes()
    download = subprocess.run([shutil.which('mkt'), '--root', str(destination), 'binance', 'download',
                               '--symbols', 'BTCUSDT', '--groups', 'kline'], capture_output=True, text=True)
    assert download.returncode == 1 and '不支持下载写入' in download.stderr
    assert (destination / 'mkt.sqlite').read_bytes() == index_before


def test_filters_partial_year_precision_and_ordinary(source, tmp_path):
    root, _ = source
    destination = tmp_path / 'selected'
    result = run_package(root, destination, groups=['kline'], symbols=['BTCUSDT'], start=20250101, end=20260201)
    assert result['files'] == 3
    selected = mktpack.open(destination)
    with sqlite3.connect(destination / 'mkt.sqlite') as db, sqlite3.connect(root / 'mkt.sqlite') as original:
        assert db.execute('SELECT * FROM __symbols__ ORDER BY id').fetchall() == original.execute('SELECT * FROM __symbols__ ORDER BY id').fetchall()
    assert {g['name'] for g in selected.describe()} == {'kline', 'exchangeinfo'}
    assert selected['exchangeinfo'] == mktpack.open(root)['exchangeinfo']
    assert selected['kline'].read().equals(mktpack.open(root)['kline'].read(symbol='BTCUSDT', start=20250101, end=20260201))
    narrow = tmp_path / 'narrow'
    run_package(destination, narrow, start='2025-02-01', end='2026-01-01')
    table = mktpack.open(narrow)['kline'].read()
    assert table['value'].to_pylist() == [Decimal('2.345'), Decimal('3.456')]
    assert table['value'].type.scale == 3
    assert mktpack.open(narrow)['kline'].describe()['schema']['value']['scale'] == 3
    assert mktpack.open(narrow)['kline'].read(start='2026-01-01').num_rows == 0


def test_package_reads_interleaved_volumes_with_wal(source, tmp_path):
    root, _ = source
    packed = tmp_path / 'packed'
    assert run_package(root, packed, max_file_size='1MiB')['volumes'] == 1
    expected = payloads(packed)
    wal_volume = packed / 'wal.data'
    with sqlite3.connect(packed / 'mkt.sqlite') as index:
        middle, = index.execute(
            'SELECT f.id FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id '
            'JOIN __symbols__ s ON s.id=f.symbol_id ORDER BY g.name,s.name,f.period LIMIT 1 OFFSET 2'
        ).fetchone()
        index.execute('INSERT INTO __volumes__ VALUES(0,?)', (str(wal_volume),))
        index.execute('UPDATE __timefile__ SET volume_id=0 WHERE id=?', (middle,))
    with sqlite3.connect(packed / 'mkt.data.1') as first:
        blob, = first.execute('SELECT bytes FROM blob WHERE id=?', (middle,)).fetchone()
    with sqlite3.connect(wal_volume) as second:
        second.execute('CREATE TABLE blob(id INTEGER PRIMARY KEY,bytes BLOB NOT NULL)')
        assert second.execute('PRAGMA journal_mode=WAL').fetchone() == ('wal',)
        second.execute('PRAGMA wal_autocheckpoint=0')
        second.execute('INSERT INTO blob VALUES(?,?)', (middle, blob))
        second.commit()
        assert mktpack.open(packed)['kline'].read().equals(mktpack.open(root)['kline'].read())
        check_package(packed)
        repacked = tmp_path / 'repacked-wal'
        run_package(packed, repacked)
        assert payloads(repacked) == expected


def test_full_package_preserves_index_metadata(source, tmp_path):
    root, db = source
    schema = json.loads(db.execute("SELECT schema FROM __group__ WHERE name='kline'").fetchone()[0])
    original_scale = schema['value']['scale']
    schema['value']['scale'] = 7
    db.execute("UPDATE __group__ SET schema=? WHERE name='kline'", (json.dumps(schema),))
    db.commit()
    columns = 'id,group_id,symbol_id,period,first_time,last_time,rows'
    expected = db.execute(f'SELECT {columns} FROM __timefile__ ORDER BY id').fetchall()
    destination = tmp_path / 'full'
    result = run_package(root, destination, max_file_size=32768)
    assert result['rows'] == sum(row[-1] for row in expected)
    with sqlite3.connect(destination / 'mkt.sqlite') as packed:
        assert packed.execute(f'SELECT {columns} FROM __timefile__ ORDER BY id').fetchall() == expected
        assert json.loads(packed.execute("SELECT schema FROM __group__ WHERE name='kline'").fetchone()[0]) == schema
    filtered = tmp_path / 'filtered'
    run_package(root, filtered, groups=['kline'])
    with sqlite3.connect(filtered / 'mkt.sqlite') as packed:
        assert packed.execute(f'SELECT {columns} FROM __timefile__ ORDER BY id').fetchall() == [
            row for row in expected if row[1] == 2
        ]
    assert mktpack.open(filtered)['kline'].describe()['schema']['value']['scale'] == original_scale


def test_size_boundary_rejection_and_index_warning(source, tmp_path):
    root, _ = source
    selection = dict(groups=['kline'], symbols=['BTCUSDT'], start=20240101, end=20250101)
    initial = tmp_path / 'initial'
    run_package(root, initial, **selection)
    limit = (initial / 'mkt.data.1').stat().st_size
    run_package(root, tmp_path / 'exact-limit', max_file_size=limit, **selection)
    failed = tmp_path / 'too-small'
    run_package(root, failed, max_file_size=limit-1, **selection, error='需要至少')
    assert list(failed.iterdir()) == []
    existing = tmp_path / 'existing'
    existing.mkdir()
    sentinel = existing / 'keep'
    sentinel.write_text('keep')
    run_package(root, existing, error='目的地必须为空')
    assert sentinel.read_text() == 'keep' and list(existing.iterdir()) == [sentinel]
    empty = tmp_path / 'empty'
    result = run_package(root, empty, start=20300101, max_file_size=8192)
    assert result['files'] == result['volumes'] == 0
    assert result['warnings'] and result['index_bytes'] > 8192
    assert mktpack.open(empty)['kline'].read().num_rows == 0
    assert mktpack.open(empty)['exchangeinfo'] == mktpack.open(root)['exchangeinfo']
    check_package(empty)
    download = subprocess.run([shutil.which('mkt'), '--root', str(empty), 'binance', 'download',
                               '--symbols', 'BTCUSDT', '--groups', 'kline'], capture_output=True, text=True)
    assert download.returncode == 1 and '不支持下载写入' in download.stderr


@pytest.mark.parametrize('jobs', [1, 4])
@pytest.mark.parametrize('missing', ['kline/DOGEUSDT/2024.mkp', 'kline/BTCUSDT/202502.mkp'])
def test_failed_pack_cleans_partial_output(source, tmp_path, jobs, missing):
    root, _ = source
    (root / missing).unlink()
    destination = tmp_path / 'failed'
    run_package(root, destination, jobs=jobs, max_file_size=32768, error='无法读取')
    assert list(destination.iterdir()) == []


def test_cross_year_split_and_gaps(source, tmp_path):
    root, db = source
    add_file(root, db, 'extra', 'X', 2024,
             [milliseconds('2023-12-31'), milliseconds('2025-01-01'), milliseconds('2025-01-01')], ['1', '2', '3'])
    destination = tmp_path / 'split'
    result = run_package(root, destination, groups=['extra'])
    assert result['files'] == 1
    assert set(payloads(destination)) == {'extra/X/2024.mkp'}
    assert mktpack.open(destination)['extra'].read().equals(mktpack.open(root)['extra'].read())


@pytest.mark.parametrize('mixed', [False, True])
def test_quantized_pack_preserves_values(source, tmp_path, mixed):
    root, db = source
    for i in range(2):
        add_file(root, db, 'ratio', 'BTCUSDT', 202401+i, [milliseconds(f'2024-0{i+1}-01')],
                 lossy=not mixed or i == 1,
                 quantities={'volume': ['1.000'], 'taker_buy_volume': ['.3334000']})
    expected = mktpack.open(root)['ratio'].read()
    destination = tmp_path / 'quantized'
    run_package(root, destination, groups=['ratio'])
    actual = mktpack.open(destination)['ratio'].read()
    assert actual.equals(expected)
    assert actual['taker_buy_volume'].to_pylist() == ([Decimal('.3334'), Decimal('.333')] if mixed else [Decimal('.333')]*2)
    rewritten = tmp_path / 'quantized-again'
    run_package(destination, rewritten)
    assert mktpack.open(rewritten)['ratio'].read().equals(expected)


@pytest.mark.parametrize('damage', ['volume', 'blob', 'mapping'])
def test_check_reports_missing_packed_payload(source, tmp_path, damage):
    root, _ = source
    destination = tmp_path / 'damaged'
    run_package(root, destination)
    if damage == 'volume':
        (destination / 'mkt.data.1').unlink()
    elif damage == 'blob':
        with sqlite3.connect(destination / 'mkt.data.1') as db:
            db.execute('DELETE FROM blob WHERE id=1')
    else:
        with sqlite3.connect(destination / 'mkt.sqlite') as db:
            db.execute('UPDATE __timefile__ SET volume_id=999 WHERE id=1')
    command = subprocess.run([shutil.which('mkt'), '--root', str(destination / 'mkt.sqlite'), 'binance', 'check'], capture_output=True, text=True)
    assert command.returncode == 1
    assert json.loads(command.stdout)['errors']


def test_cli_package_and_invalid_arguments(source, tmp_path):
    root, _ = source
    command = [shutil.which('mkt'), '--root', str(root / 'mkt.sqlite'), 'package']
    destination = tmp_path / 'cli'
    done = subprocess.run([*command, str(destination), '--max-file-size', '32KiB', '--groups', 'kline', '--symbols', 'BTCUSDT',
                           '--start', '2025-01-01', '--end', '2026-01-01'], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert json.loads(done.stdout)['files'] == 2
    assert '复制' in done.stderr
    assert mktpack.open(destination)['kline'].read().num_rows == 3
    for size in ['0', '-1', '1XB']:
        failed = subprocess.run([*command, str(tmp_path / 'invalid'), '--max-file-size', size], capture_output=True, text=True)
        assert failed.returncode != 0
    for jobs in ['0', '-1']:
        failed = subprocess.run([*command, str(tmp_path / 'invalid-jobs'), '--jobs', jobs], capture_output=True, text=True)
        assert failed.returncode != 0
    run_package(root, tmp_path / 'dates', start=20250101, end=20240101, error='start < end')
    run_package(root, tmp_path / 'unknown', groups=['missing'], error='未知数据组')


@pytest.mark.skipif(not os.environ.get('MKTPACK_REAL_DATA'), reason='设置 MKTPACK_REAL_DATA 运行真实数据回归')
def test_real_dataset_package(tmp_path, real_dataset):
    root = real_dataset
    original = mktpack.open(root)
    destination = tmp_path / 'real-package'
    result = run_package(root, destination, max_file_size=8*1024*1024)
    assert result['volumes'] > 1
    assert result['copied'] == result['files'] > 0 and result['reencoded'] == 0
    assert all(p.stat().st_size <= 8*1024*1024 for p in destination.glob('mkt.data.*'))
    packed = mktpack.open(destination / 'mkt.sqlite')
    for group in original.describe():
        name = group['name']
        if group['kind'] == 'ordinary':
            assert packed[name] == original[name]
        else:
            for symbol in ['BTCUSDT', 'DOGEUSDT', 'ETHUSDT', 'BNBUSDT']:
                assert packed[name].read(symbol=symbol).equals(original[name].read(symbol=symbol)), (name, symbol)
    for relative, data in payloads(destination).items():
        path = root / relative
        if path.is_file():
            assert data == path.read_bytes()
    check_package(destination)
    selected = tmp_path / 'real-selected'
    run_package(destination, selected, groups=['kline'], symbols=['BTCUSDT', 'DOGEUSDT'], start=20251215, end=20260201, max_file_size=8*1024*1024)
    assert mktpack.open(selected)['kline'].read().equals(original['kline'].read(
        symbols=['BTCUSDT', 'DOGEUSDT'], start=20251215, end=20260201))
    assert mktpack.open(selected)['exchangeinfo'] == original['exchangeinfo']
    print(json.dumps(result, ensure_ascii=False))

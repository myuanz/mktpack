import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import functools
import hashlib
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import threading
import zipfile

import numpy as np
import pyarrow as pa
import mktpack
from mktpack import raw


def archive(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = io.StringIO()
    writer = csv.DictWriter(data, columns)
    writer.writeheader()
    writer.writerows(rows)
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr(path.stem, data.getvalue())


def tick(market: str, symbol: str, time: int, price: str, volume: int) -> dict:
    row = {
        'exchange_id': market, 'instrument_id': symbol, 'trade_timestamp': time,
        'trading_day': '1999.01.01', 'volume': volume, 'turnover': '4.100000000000001E6',
        'open_interest': '12.5', 'pre_open_interest': 20,
    }
    for name in ('last', 'open', 'highest', 'lowest', 'close', 'settlement', 'upper_limit',
                 'lower_limit', 'pre_settlement', 'pre_close'):
        row[name + '_price'] = price
    for side in ('ask', 'bid'):
        for level in range(1, 6):
            row[f'{side}_price{level}'] = price
            row[f'{side}_volume{level}'] = volume + level
    return row


def cli(root: Path, *args, error: str | None = None, env: dict | None = None):
    command = [shutil.which('mkt'), '--root', str(root), *map(str, args)]
    result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', env=env)
    if error is not None:
        assert result.returncode == 1, result.stderr
        assert error in result.stderr
        return None
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def check_ok(root: Path):
    res = cli(root, 'binance', 'check')
    assert res['errors'] == []


def ms(text: str) -> int:
    return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp() * 1000)


def init_loose_dataset(root: Path) -> sqlite3.Connection:
    root.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(root / 'mkt.sqlite')
    db.executescript('''
        CREATE TABLE __meta__(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
        CREATE TABLE __group__(id INTEGER PRIMARY KEY,name TEXT UNIQUE,kind TEXT,schema TEXT NOT NULL DEFAULT '{}',storage TEXT NOT NULL DEFAULT 'mkp',location TEXT NOT NULL DEFAULT '',table_name TEXT NOT NULL DEFAULT '',time_column TEXT NOT NULL DEFAULT 'time',symbol_column TEXT NOT NULL DEFAULT 'symbol_id');
        CREATE TABLE __sqlite_partitions__(group_id INTEGER,start INTEGER,end INTEGER,volume_id INTEGER,rows INTEGER,first_time INTEGER,last_time INTEGER,PRIMARY KEY(group_id,start)) WITHOUT ROWID;
        CREATE TABLE __symbols__(id INTEGER PRIMARY KEY,name TEXT UNIQUE);
        CREATE TABLE __volumes__(id INTEGER PRIMARY KEY,path TEXT NOT NULL UNIQUE);
        CREATE TABLE __timefile__(id INTEGER PRIMARY KEY,group_id INTEGER NOT NULL,symbol_id INTEGER NOT NULL,period INTEGER NOT NULL,first_time INTEGER NOT NULL,last_time INTEGER NOT NULL,rows INTEGER NOT NULL,volume_id INTEGER,UNIQUE(group_id,symbol_id,period));
    ''')
    return db


def add_mkp(root: Path, db: sqlite3.Connection, group: str, symbol: str, period: int, times: list[int], values: list[str]):
    relative = f'{group}/{symbol}/{period}.mkp'
    table = pa.table({
        'time': pa.array(times, type=pa.timestamp('ms', tz='UTC')),
        'value': [Decimal(v) for v in values],
    })
    raw.write(root / relative, table, metadata={'group': group, 'symbol': symbol})
    h = raw.info(root / relative)
    db.execute('INSERT OR IGNORE INTO __group__(name,kind) VALUES (?, ?)', (group, 'timeseries'))
    schema = json.loads(db.execute('SELECT schema FROM __group__ WHERE name=?', (group,)).fetchone()[0])
    for d in h['columns']:
        scale = max(schema.get(d['name'], {}).get('scale', 0), d['scale'])
        schema[d['name']] = {'type': d['type'], 'scale': scale}
    db.execute('UPDATE __group__ SET schema=? WHERE name=?', (json.dumps(schema), group))
    db.execute('INSERT OR IGNORE INTO __symbols__(name) VALUES (?)', (symbol,))
    sid = db.execute('SELECT id FROM __symbols__ WHERE name=?', (symbol,)).fetchone()[0]
    gid = db.execute('SELECT id FROM __group__ WHERE name=?', (group,)).fetchone()[0]
    db.execute(
        'INSERT INTO __timefile__(group_id,symbol_id,period,first_time,last_time,rows) VALUES (?,?,?,?,?,?) '
        'ON CONFLICT(group_id,symbol_id,period) DO UPDATE SET first_time=excluded.first_time,last_time=excluded.last_time,rows=excluded.rows',
        (gid, sid, period, h['first_time'], h['last_time'], h['rows']),
    )
    db.commit()


def make_release_zip(zip_path: Path, meta: dict, data_dir: Path | None = None, script_text: str | None = None) -> tuple[int, str]:
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('meta.json', json.dumps(meta))
        if script_text is not None:
            z.writestr(meta.get('script', 'update.py'), script_text)
        if data_dir is not None:
            for p in sorted(data_dir.rglob('*')):
                if p.is_file():
                    z.write(p, f'data/{p.relative_to(data_dir).as_posix()}')
    raw_bytes = zip_path.read_bytes()
    return len(raw_bytes), hashlib.sha256(raw_bytes).hexdigest()


def test_meta_multi_hot_volumes_and_tail_cold_append(tmp_path):
    rng = np.random.default_rng(7)
    bulk_times = lambda day: [ms(day) + i * 60000 for i in range(4000)]
    vals = lambda: [str(Decimal(int(v)) / 1000) for v in rng.integers(0, 2**30, 4000)]

    full_src = tmp_path / 'full-src'
    with init_loose_dataset(full_src) as db:
        db.executescript('''
            INSERT INTO __meta__(key,value) VALUES ('port','binance');
            CREATE TABLE exchangeinfo(version TEXT);
            INSERT INTO __group__(name,kind,storage) VALUES ('exchangeinfo','ordinary','sqlite');
            INSERT INTO exchangeinfo VALUES ('v1');
        ''')
        add_mkp(full_src, db, 'kline', 'AAAUSDT', 20240101, bulk_times('2024-01-01'), vals())
        add_mkp(full_src, db, 'kline', 'BTCUSDT', 20240101, bulk_times('2024-01-01'), vals())
        add_mkp(full_src, db, 'kline', 'CCCUSDT', 20250101, bulk_times('2025-01-01'), vals())
        add_mkp(full_src, db, 'kline', 'DDDUSDT', 20250102, bulk_times('2025-01-02'), vals())
        add_mkp(full_src, db, 'kline', 'EEEUSDT', 20260101, [ms('2026-01-01')], ['30.456'])

    # 1. 初始基准包：32KiB 上限，2024 年 2 个大块切入 mkt.data.1 / mkt.data.2，__meta__ 记录 max_file_size
    base = tmp_path / 'base-pkg'
    cli(full_src, 'package', base, '--end', '2025-01-01', '--max-file-size', '32KiB')
    assert mktpack.meta(base)['max_file_size'] == str(32 * 1024)
    first_vol = base / 'mkt.data.1'
    first_vol_bytes = first_vol.read_bytes()
    assert (base / 'mkt.data.2').exists()

    # 2. 切出 2025~2026 增量包并合入：因 __meta__.max_file_size = 32KiB，热卷自动切分为 hot.data.1 / hot.data.2+
    with sqlite3.connect(full_src / 'mkt.sqlite') as db:
        db.execute("UPDATE exchangeinfo SET version='v2'")
        db.commit()
    delta = tmp_path / 'delta'
    cli(full_src, 'package', delta, '--start', '2025-01-01', '--end', '2027-01-01', '--max-file-size', '64KiB')

    merge_res = mktpack.merge_dataset(base, delta)
    assert merge_res['packed'] is True and merge_res['inserted'] == 3
    hot_vols = sorted(base.glob('hot.data.*'))
    assert len(hot_vols) >= 2
    assert all(p.stat().st_size <= 32 * 1024 for p in hot_vols)
    assert first_vol.read_bytes() == first_vol_bytes
    assert mktpack.open(base)['exchangeinfo'] == {'version': 'v2'}
    assert mktpack.open(base)['kline'].read().equals(mktpack.open(full_src)['kline'].read())
    check_ok(base)

    # 3. compact --before 2026-01-01（不传 max_file_size，自动沿用 __meta__）：2025 封存进冷卷，2026 留 hot.data.1，mkt.data.1 零改动
    c1 = mktpack.compact(base, before='2026-01-01')
    assert c1['cold_files'] == 2 and c1['hot_files'] == 1 and c1['hot_volumes'] == 1
    assert first_vol.read_bytes() == first_vol_bytes
    assert [p.name for p in sorted(base.glob('hot.data.*'))] == ['hot.data.1']
    assert mktpack.open(base)['kline'].read().equals(mktpack.open(full_src)['kline'].read())
    check_ok(base)

    # 4. 模拟跨年到 2027 收口：把 max_file_size 调大到 1MiB 后 compact，应优先向最后一个未满冷卷尾部追加而不建新卷
    mktpack.set_meta(base, 'max_file_size', str(1024 * 1024))
    cold_before = sorted(base.glob('mkt.data.*'))
    c2 = mktpack.compact(base, before='2027-01-01')
    assert c2['hot_files'] == 0 and c2['created_volumes'] == 0 and c2['appended_tail'] is True
    assert not list(base.glob('hot.data.*'))
    assert sorted(base.glob('mkt.data.*')) == cold_before
    assert first_vol.read_bytes() == first_vol_bytes
    assert mktpack.open(base)['kline'].read().equals(mktpack.open(full_src)['kline'].read())
    check_ok(base)


def test_binance_package_auto_splits_hot_and_cold(tmp_path):
    src = tmp_path / 'binance-src'
    with init_loose_dataset(src) as db:
        db.execute("INSERT INTO __meta__(key,value) VALUES('port','binance')")
        add_mkp(src, db, 'kline', 'BTCUSDT', 2024, [ms('2024-06-01')], ['100.0'])
        add_mkp(src, db, 'kline', 'BTCUSDT', 202611, [ms('2026-11-01')], ['200.0'])

    pkg = tmp_path / 'binance-pkg'
    res = cli(src, 'package', pkg, '--max-file-size', '1MiB')
    assert res['volumes'] == 1 and res['hot_volumes'] == 1
    assert (pkg / 'mkt.data.1').exists() and (pkg / 'hot.data.1').exists()
    assert mktpack.meta(pkg)['port'] == 'binance'
    assert mktpack.open(pkg)['kline'].read().equals(mktpack.open(src)['kline'].read())
    check_ok(pkg)


def test_merge_zips_scripts_and_remote_manifest(tmp_path):
    # 准备 3 个版本的增量数据
    base_loose = tmp_path / 'base-loose'
    with init_loose_dataset(base_loose) as db:
        add_mkp(base_loose, db, 'kline', 'BTCUSDT', 20261126, [ms('2026-11-26')], ['100.0'])
    target = tmp_path / 'client-pkg'
    mktpack.package(base_loose, target, max_file_size=1024 * 1024)
    # 新打包的数据集默认自动写入当天 UTC+8 日期 YYYY.MM.DD 作为版本号
    today_ver = datetime.now(timezone(timedelta(hours=8))).strftime('%Y.%m.%d')
    assert mktpack.meta(target)['version'] == today_ver
    assert mktpack.check(target)['errors'] == []
    # 手动移除 version 以模拟 1126 无版本老包
    with sqlite3.connect(target / 'mkt.sqlite') as db:
        db.execute("DELETE FROM __meta__ WHERE key='version'")
        db.commit()
    assert 'version' not in mktpack.meta(target)

    d1 = tmp_path / 'd1'
    with init_loose_dataset(d1) as db:
        add_mkp(d1, db, 'kline', 'BTCUSDT', 20261127, [ms('2026-11-27')], ['101.5'])
    d2 = tmp_path / 'd2'
    with init_loose_dataset(d2) as db:
        add_mkp(d2, db, 'kline', 'BTCUSDT', 20261128, [ms('2026-11-28')], ['102.5'])
    d12 = tmp_path / 'd12'
    with init_loose_dataset(d12) as db:
        add_mkp(d12, db, 'kline', 'BTCUSDT', 20261127, [ms('2026-11-27')], ['101.5'])
        add_mkp(d12, db, 'kline', 'BTCUSDT', 20261128, [ms('2026-11-28')], ['102.5'])

    dist_dir = tmp_path / 's3-dist'
    dist_dir.mkdir()
    # 包 1：允许从无版本(null)升级到 2026.11.27，纯数据包（无脚本）
    s1, h1 = make_release_zip(dist_dir / 'v1.zip', {'from': [None, '2026.11.26'], 'to': '2026.11.27'}, data_dir=d1)
    # 包 2：从 2026.11.27 升级到 2026.11.28，显式声明 script 执行自定义脚本
    script = (
        'import sys, pathlib, mktpack\n'
        'target, pkg = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])\n'
        'mktpack.merge_dataset(target, pkg / "data")\n'
        'mktpack.set_meta(target, "migrated_by", "update.py")\n'
    )
    s2, h2 = make_release_zip(dist_dir / 'v2.zip', {'from': '2026.11.27', 'to': '2026.11.28', 'script': 'update.py'}, data_dir=d2, script_text=script)
    # 包 3（快捷大包）：从 null 直接跳到 2026.11.28
    s12, h12 = make_release_zip(dist_dir / 'v12.zip', {'from': None, 'to': '2026.11.28'}, data_dir=d12)

    # 1. 本地离线 zip 测试：只传 v2.zip 应因缺少中间版本报错
    cli(target, 'merge', dist_dir / 'v2.zip', error='没有可应用于当前版本')

    # 2. 乱序传入 v2.zip 和 v1.zip：自动按版本链先跑 v1（纯 C++ merge + compact），再跑 v2（Python 脚本）
    env = dict(**os.environ, MKTPACK_PYTHON=sys.executable)
    res_local = cli(target, 'merge', dist_dir / 'v2.zip', dist_dir / 'v1.zip', env=env)
    assert res_local['status'] == 'updated' and res_local['version'] == '2026.11.28'
    assert [step['to'] for step in res_local['applied']] == ['2026.11.27', '2026.11.28']
    assert [step['mode'] for step in res_local['applied']] == ['merge', 'script']
    assert mktpack.meta(target)['migrated_by'] == 'update.py'
    assert mktpack.open(target)['kline'].read()['value'].to_pylist() == [100.0, 101.5, 102.5]
    check_ok(target)

    # 再次运行应返回 up_to_date
    res_again = cli(target, 'merge', dist_dir / 'v1.zip', dist_dir / 'v2.zip', env=env)
    assert res_again['status'] == 'up_to_date'

    # 3. 远端 HTTP manifest.json 测试（起本地 HTTP 服务器模拟 S3）
    remote_target = tmp_path / 'remote-client'
    mktpack.package(base_loose, remote_target, max_file_size=1024 * 1024)
    with sqlite3.connect(remote_target / 'mkt.sqlite') as db:
        db.execute("DELETE FROM __meta__ WHERE key='version'")
        db.commit()

    manifest = {
        'latest': '2026.11.28',
        'packages': [
            {'from': [None, '2026.11.26'], 'to': '2026.11.27', 'url': 'v1.zip', 'size': s1 + 10000, 'sha256': h1},
            {'from': '2026.11.27', 'to': '2026.11.28', 'url': 'v2.zip', 'size': s2 + 10000, 'sha256': h2},
            {'from': None, 'to': '2026.11.28', 'url': 'v12.zip', 'size': s12, 'sha256': h12},
        ],
    }
    (dist_dir / 'manifest.json').write_text(json.dumps(manifest), encoding='utf-8')

    handler = functools.partial(SimpleHTTPRequestHandler, directory=str(dist_dir))
    server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        manifest_url = f'http://127.0.0.1:{server.server_port}/manifest.json'
        # 应自动选总下载量最小的直达包 v12.zip
        res_http = cli(remote_target, 'merge', manifest_url, env=env)
        assert res_http['status'] == 'updated' and res_http['version'] == '2026.11.28'
        assert len(res_http['applied']) == 1
        assert mktpack.open(remote_target)['kline'].read()['value'].to_pylist() == [100.0, 101.5, 102.5]
        check_ok(remote_target)

        # 再次拉取应 up_to_date
        assert cli(remote_target, 'merge', manifest_url, env=env)['status'] == 'up_to_date'

        # 篡改 SHA256 应拒绝安装
        bad_target = tmp_path / 'bad-sha-client'
        mktpack.package(base_loose, bad_target, max_file_size=1024 * 1024)
        with sqlite3.connect(bad_target / 'mkt.sqlite') as db:
            db.execute("DELETE FROM __meta__ WHERE key='version'")
            db.commit()
        manifest['packages'][2]['sha256'] = '0' * 64
        (dist_dir / 'manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
        cli(bad_target, 'merge', manifest_url, error='SHA256 不匹配', env=env)
        assert 'version' not in mktpack.meta(bad_target)
    finally:
        server.shutdown()
        server.server_close()


def test_merge_into_loose_dataset_and_sqlite_todo(tmp_path):
    loose = tmp_path / 'loose'
    with init_loose_dataset(loose) as db:
        add_mkp(loose, db, 'kline', 'BTCUSDT', 20240101, [ms('2024-01-01')], ['1.0'])

    src = tmp_path / 'src'
    with init_loose_dataset(src) as db:
        add_mkp(src, db, 'kline', 'ETHUSDT', 20240102, [ms('2024-01-02')], ['2.5'])
        db.execute("INSERT INTO __group__(name,kind,storage) VALUES ('funding_rate_settlement','timeseries','sqlite')")
        db.commit()

    # CLI merge 拒绝无版本的本地目录
    cli(loose, 'merge', src, error='仅支持本地 .zip 更新包')

    # 底层 merge_dataset 支持合并进松散目录，SQLite 时序组显式指定报错、默认返回 warning
    import pytest
    with pytest.raises(RuntimeError, match='暂不支持合并 SQLite 时序组'):
        mktpack.merge_dataset(loose, src, groups=['funding_rate_settlement'])

    res = mktpack.merge_dataset(loose, src)
    assert res['packed'] is False and res['inserted'] == 1
    assert any('funding_rate_settlement' in w for w in res['warnings'])
    assert (loose / 'kline/ETHUSDT/20240102.mkp').exists()
    assert mktpack.open(loose)['kline'].symbols() == ['BTCUSDT', 'ETHUSDT']
    check_ok(loose)


def test_ctp_futures_direct_import_into_package(tmp_path):
    mirror = tmp_path / 'mirror'
    rows_d1 = [tick('CZCE', 'AP505', 1743469200000, '6241.8', 10)]
    rows_d2 = [tick('CZCE', 'AP505', 1743555600000, '6250.2', 20)]
    archive(mirror / 'nfs_rq/tick/future/2025/04/01/FUTURE_TICK_AP_20250401.csv.zip', list(rows_d1[0]), rows_d1)
    archive(mirror / 'nfs_rq/tick/future/2025/04/02/FUTURE_TICK_AP_20250402.csv.zip', list(rows_d2[0]), rows_d2)
    listing = {
        'exchangeid': 'CZCE', 'instrumentid': 'AP505', 'instrumentid_key': 'AP2505',
        'opendate': '20240520', 'expiredate': '20250519', 'volumemultiple': '10',
    }
    archive(mirror / 'nfs_rq/basicinfo/future_instrument/future_instrument20250321.csv.zip', list(listing), [listing])

    loose = tmp_path / 'loose'
    cli(loose, 'ctp-futures', 'import', mirror)

    day1_loose = tmp_path / 'day1-loose'
    cli(day1_loose, 'ctp-futures', 'import', mirror, '--end', '2025-04-02')
    packed = tmp_path / 'packed'
    cli(day1_loose, 'package', packed, '--max-file-size', '1MiB')
    assert mktpack.meta(packed)['port'] == 'ctp-futures'

    imp = mktpack.import_ctp_ticks(packed, mirror)
    assert imp['imported'] == 1 and imp['skipped'] == 1 and imp['blocks'] == 1
    assert not list(packed.glob('hot.data.*'))
    assert [p.name for p in packed.glob('mkt.data.*')] == ['mkt.data.1']
    assert not (packed / 'tick').exists()
    assert mktpack.open(packed)['tick'].read().equals(mktpack.open(loose)['tick'].read())
    assert mktpack.check(packed)['errors'] == []

    imp2 = mktpack.import_ctp_ticks(packed, mirror)
    assert imp2['imported'] == 0 and imp2['skipped'] == 2


def test_package_default_max_file_size_is_8gib(tmp_path):
    src = tmp_path / 'src'
    with init_loose_dataset(src) as db:
        add_mkp(src, db, 'kline', 'BTCUSDT', 20240101, [ms('2024-01-01')], ['1.0'])

    cli_packed = tmp_path / 'cli-packed'
    cli(src, 'package', cli_packed)
    assert mktpack.meta(cli_packed)['max_file_size'] == str(8 * 1024**3)

    py_packed = tmp_path / 'py-packed'
    mktpack.package(src, py_packed)
    assert mktpack.meta(py_packed)['max_file_size'] == str(8 * 1024**3)

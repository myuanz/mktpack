import csv
import io
import json
import shutil
import sqlite3
import subprocess
import zipfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import mktpack


def archive(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = io.StringIO()
    writer = csv.DictWriter(data, columns)
    writer.writeheader()
    writer.writerows(rows)
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr(path.stem, data.getvalue())


def tick(market: str, symbol: str, time: int, price: str, volume: int, unique: str | None = None) -> dict:
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
    if unique is not None:
        row['unique_instrument_id'] = unique
    return row


def cli(root: Path, *args: str) -> dict:
    command = [shutil.which('mkt'), '--root', str(root), *map(str, args)]
    return json.loads(subprocess.run(command, capture_output=True, check=True, text=True).stdout)


def test_subset_incremental_tick_import_and_archive(tmp_path):
    mirror, root = tmp_path/'mirror', tmp_path/'data'
    zip_path = mirror/'nfs_rq/tick/future/2025/04/01/FUTURE_TICK_AP_20250401.csv.zip'
    # 时间跨到前一个月，且相同时间戳的两行顺序有意义。
    times = [1743469200000, 1743426000000, 1743426000000]
    rows = [tick('CZCE', 'AP505', time, '6241.800000000000181', i)
            for i, time in enumerate(times)]
    rows.append(tick('CZCE', 'AP510', times[0], '6200', 3))
    archive(zip_path, list(rows[0]), rows)
    name = 'future_instrument20250321.csv.zip'
    listing = {
        'exchangeid': 'CZCE', 'instrumentid': 'AP505', 'instrumentid_key': 'AP2505',
        'opendate': '20240520', 'expiredate': '20250519', 'volumemultiple': '10',
    }
    archive(mirror/'nfs_rq/basicinfo/future_instrument'/name, list(listing), [listing])

    def run(*args):
        return cli(root, 'ctp-futures', 'import', mirror, *args)

    assert run('--end', '2025-04-01')['blocks'] == 0
    assert run('--symbols', 'ap2505')['rows'] == 3
    assert run('--symbols', 'AP2505')['skipped'] == 1
    assert run()['rows'] == 1
    assert run()['skipped'] == 1
    group = mktpack.open(root)['tick']
    columns = ['last_price', 'volume', 'bid_volume5', 'trading_day', 'turnover', 'open_interest']
    table = group.read(symbol='AP2505', columns=columns)
    assert table.column_names == columns
    assert table['volume'].to_pylist() == [1, 2, 0]
    assert table['bid_volume5'].to_pylist() == [6, 7, 5]
    assert table['trading_day'].to_pylist() == [20250401]*3
    assert table['last_price'].to_pylist() == [Decimal('6241.8')]*3
    assert table['turnover'].to_pylist() == [Decimal('4100000.000000001')]*3
    assert table['open_interest'].to_pylist() == [Decimal('12.5')]*3
    assert group.read(symbol='AP2510').num_rows == 1
    with sqlite3.connect(root/'mkt.sqlite') as db:
        assert db.execute("SELECT count(*) FROM sqlite_master WHERE name IN ('__ctp_futures_members__','__ctp_futures_loaded__')").fetchone()[0] == 0
        assert db.execute('SELECT period FROM __timefile__ ORDER BY period').fetchall() == [(20250401,), (20250401,)]
        assert db.execute('SELECT path FROM __volumes__').fetchall() == [('tick/AP2505/2025.data',), ('tick/AP2510/2025.data',)]
    assert not list((root/'tick').rglob('*-*.mkp'))
    packed = tmp_path/'packed'
    cli(root, 'package', packed)
    assert mktpack.open(packed)['tick'].read(symbol='AP2505', columns=columns).equals(table)
    with pytest.raises(subprocess.CalledProcessError, match='non-zero'):
        cli(packed, 'ctp-futures', 'import', mirror)


def test_retry_after_bad_source(tmp_path):
    mirror, root = tmp_path/'mirror', tmp_path/'data'
    path = mirror/'nfs_rq_new/tick/future/2026/11/02/FUTURE_TICK_DCE_L_20261102.csv.zip'
    row = tick('DCE', 'l2612F', 1793365140027, '9851', 23, 'DCE|F|L|2612F')
    bad = dict(row, bid_volume1='bad')
    archive(path, list(row), [row, bad])
    args = ['ctp-futures', 'import', mirror, '--symbols', 'L2612F']
    with pytest.raises(subprocess.CalledProcessError):
        cli(root, *args)
    with sqlite3.connect(root/'mkt.sqlite') as db:
        assert db.execute('SELECT count(*) FROM __timefile__').fetchone()[0] == 0
        assert db.execute('SELECT count(*) FROM __ctp_futures_files__').fetchone()[0] == 0
    archive(path, list(row), [row])
    runs = [cli(root, *args), cli(root, *args)]
    assert sorted(r['blocks'] for r in runs) == [0, 1]
    assert mktpack.open(root)['tick'].read(symbol='L2612F', columns=['last_price']).to_pydict() == {
        'last_price': [Decimal('9851')],
    }


def test_completed_source_remains_after_later_failure(tmp_path):
    mirror, root = tmp_path/'mirror', tmp_path/'data'
    directory = mirror/'nfs_rq_new/tick/future/2026/11/02'
    good_path = directory/'FUTURE_TICK_DCE_A_20261102.csv.zip'
    bad_path = directory/'FUTURE_TICK_DCE_B_20261102.csv.zip'
    good = tick('DCE', 'a2612', 1793581200000, '4000', 3, 'DCE|F|A|2612')
    bad = tick('DCE', 'b2612', 1793581200000, '3000', 4, 'DCE|F|B|2612')
    archive(good_path, list(good), [good])
    archive(bad_path, list(bad), [dict(bad, bid_volume1='bad')])
    with pytest.raises(subprocess.CalledProcessError):
        cli(root, 'ctp-futures', 'import', mirror)
    with sqlite3.connect(root/'mkt.sqlite') as db:
        assert db.execute('SELECT count(*) FROM __timefile__').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM __ctp_futures_files__').fetchone()[0] == 1
    archive(bad_path, list(bad), [bad])
    result = cli(root, 'ctp-futures', 'import', mirror)
    assert result['skipped'] == 1
    assert result['blocks'] == 1



def test_mirror_suffix_independent_of_filename_format(tmp_path):
    mirror, root = tmp_path/'mirror', tmp_path/'data'
    directory = mirror/'nfs_rq_archive/tick/future/2026/11/02'
    rows = [
        ('FUTURE_TICK_AP_20261102.csv.zip', tick('CZCE', 'AP701', 1793581200000, '6600', 3)),
        ('FUTURE_TICK_DCE_L_20261102.csv.zip', tick('DCE', 'l2612F', 1793581200000, '9851', 4, 'DCE|F|L|2612F')),
    ]
    for filename, row in rows:
        archive(directory/filename, list(row), [row])
    args = ['ctp-futures', 'import', mirror, '--symbols', 'AP2701', 'L2612F']
    assert cli(root, *args)['blocks'] == 2
    assert cli(root, *args)['skipped'] == 2
    assert sorted(mktpack.open(root)['tick'].read(columns=['symbol'])['symbol'].to_pylist()) == ['AP2701', 'L2612F']


def test_ignore_f_packages_and_keep_first_daily_block(tmp_path):
    mirror, root = tmp_path/'mirror', tmp_path/'data'
    relative = Path('tick/future/2025/10/29')
    row = tick('DCE', 'l2602F', 1761700000000, '9851', 4, 'DCE|F|L|2602F')
    filename = 'FUTURE_TICK_DCE_L_20251029.csv.zip'
    archive(mirror/'nfs_rq'/relative/filename, list(row), [row])
    archive(mirror/'nfs_rq_new'/relative/filename, list(row), [dict(row, volume=5)])
    for key in ('L', 'PP'):
        path = mirror/'nfs_rq_new'/relative/f'FUTURE_TICK_DCE_{key}_F_20251029.csv.zip'
        archive(path, list(row), [dict(row, bid_volume1='bad')])
    assert cli(root, 'ctp-futures', 'import', mirror, '--jobs', '4') == {
        'imported': 1, 'skipped': 1, 'blocks': 1, 'rows': 1,
    }
    assert mktpack.open(root)['tick'].read(symbol='L2602F', columns=['volume'])['volume'].to_pylist() == [4]


def test_bounded_discovery_only_visits_requested_date_directories(tmp_path):
    mirror, root = tmp_path/'mirror', tmp_path/'data'
    row = tick('CZCE', 'AP701', 1793581200000, '6600', 3)
    archive(mirror/'nfs_rq/tick/future/2026/11/02/FUTURE_TICK_AP_20261102.csv.zip', list(row), [row])
    archive(mirror/'nfs_rq/tick/future/2026/11/03/FUTURE_TICK_AP_invalid.csv.zip', list(row), [row])
    result = cli(root, 'ctp-futures', 'import', mirror, '--start', '2026-11-02', '--end', '2026-11-03')
    assert result['blocks'] == 1


def test_parallel_bulk_and_incremental_match_serial_import(tmp_path):
    mirror = tmp_path/'mirror'
    for day in range(2, 6):
        row = tick('CZCE', 'AP701', 1793581200000+day*1000, '6600', day)
        archive(mirror/f'nfs_rq/tick/future/2026/11/{day:02d}/FUTURE_TICK_AP_202611{day:02d}.csv.zip',
                list(row), [row])
    serial, parallel = tmp_path/'serial', tmp_path/'parallel'
    args = ('ctp-futures', 'import', mirror, '--start', '2026-11-02', '--end', '2026-11-06')
    assert cli(serial, *args, '--jobs', '1')['blocks'] == 4
    assert cli(parallel, *args[:-1], '2026-11-04', '--jobs', '4')['blocks'] == 2
    for day in (3, 4):
        duplicate = tick('CZCE', 'AP701', 1793581200000+day*1000, '6600', 100)
        archive(mirror/f'nfs_rq_new/tick/future/2026/11/{day:02d}/FUTURE_TICK_AP_202611{day:02d}.csv.zip',
                list(duplicate), [duplicate])
    assert cli(parallel, *args, '--jobs', '4')['blocks'] == 2
    assert mktpack.open(serial)['tick'].read().equals(mktpack.open(parallel)['tick'].read())


def test_completed_months_use_separate_volumes(tmp_path):
    mirror, root = tmp_path/'mirror', tmp_path/'data'
    for day in (20260901, 20261008):
        path = mirror/f'nfs_rq/tick/future/2026/FUTURE_TICK_AP_{day}.csv.zip'
        row = tick('CZCE', 'AP701', 1788195600000+(day%100)*1000, '6600', day%100)
        archive(path, list(row), [row])
    profile = tmp_path/'profile.json'
    assert cli(root, 'ctp-futures', 'import', mirror, '--profile', profile)['blocks'] == 2
    measured = json.loads(profile.read_text())
    assert measured['status'] == 'ok'
    assert measured['counters']['blocks'] == 2
    assert measured['stages']['mkp_encode']['calls'] == 2
    assert measured['counters']['bulk_days'] == 2
    assert 'mkp_write' not in measured['stages']
    assert 'month_total' not in measured['stages']
    with sqlite3.connect(root/'mkt.sqlite') as db:
        assert db.execute('SELECT period FROM __timefile__ ORDER BY period').fetchall() == [(20260901,), (20261008,)]
        assert db.execute('SELECT path FROM __volumes__ ORDER BY path').fetchall() == [
            ('tick/AP2701/202609.data',), ('tick/AP2701/202610.data',),
        ]
    assert mktpack.open(root)['tick'].read(symbol='AP2701').num_rows == 2


@pytest.mark.parametrize('shared_month', [False, True], ids=['retire_month', 'keep_shared_month'])
def test_historical_bulk_writes_directly_then_incremental_archives_month(tmp_path, shared_month):
    mirror, root = tmp_path/'mirror', tmp_path/'data'
    for day, stamp in ((20240902, 1725238800000), (20241008, 1728349200000)):
        row = tick('SHFE', 'cu2412', stamp, '70000', day%100)
        path = mirror/f'nfs_rq/tick/future/2024/{day//100%100:02d}/{day%100:02d}/FUTURE_TICK_CU_{day}.csv.zip'
        archive(path, list(row), [row])

    def run(start, end, name):
        profile = tmp_path/name
        result = cli(root, 'ctp-futures', 'import', mirror, '--start', start, '--end', end, '--profile', profile)
        assert result['blocks'] == 1
        return json.loads(profile.read_text())

    bulk = run('2024-09-01', '2024-10-01', 'bulk.json')
    assert bulk['counters']['bulk_days'] == 1
    assert bulk['counters']['month_days'] == bulk['counters']['year_days'] == 0
    assert 'mkp_write' not in bulk['stages']
    folder = root/'tick/CU2412'
    assert sorted(p.name for p in folder.iterdir()) == ['2024.data']
    month = folder/'202410.data'
    if shared_month:
        with sqlite3.connect(month) as db:
            db.execute('CREATE TABLE rates(time INTEGER, rate INTEGER)')
            db.execute('INSERT INTO rates VALUES(1728349200000, 3)')
        with sqlite3.connect(root/'mkt.sqlite') as db:
            group = db.execute("INSERT INTO __group__(name,kind,storage) VALUES('rates','timeseries','sqlite')").lastrowid
            volume = db.execute('INSERT INTO __volumes__(path) VALUES(?)', (str(month.relative_to(root)),)).lastrowid
            db.execute('INSERT INTO __sqlite_partitions__ VALUES(?,?,?,?,?,?,?)',
                       (group, 1727740800000, 1730419200000, volume, 1, 1728349200000, 1728349200000))

    incremental = run('2024-10-01', '2024-11-01', 'incremental.json')
    assert incremental['counters']['bulk_days'] == 0
    assert incremental['counters']['month_days'] == 1
    assert incremental['counters']['year_days'] == 2
    expected = [folder/'2024.data', month] if shared_month else [folder/'2024.data']
    assert sorted(folder.iterdir()) == expected
    with sqlite3.connect(root/'mkt.sqlite') as db:
        assert db.execute('SELECT path FROM __volumes__ ORDER BY path').fetchall() == [
            (str(path.relative_to(root)),) for path in expected
        ]
    if shared_month:
        with sqlite3.connect(month) as db:
            assert db.execute('SELECT rate FROM rates').fetchall() == [(3,)]
    assert mktpack.open(root)['tick'].read(symbol='CU2412', columns=['volume'])['volume'].to_pylist() == [2, 8]


def test_expired_contract_archives_without_new_sources(tmp_path):
    mirror, root = tmp_path/'mirror', tmp_path/'data'
    now = datetime.now(UTC)
    today = (now + timedelta(hours=8)).date()
    day = f'{today:%Y%m%d}'
    listing = {
        'exchangeid': 'SHFE', 'instrumentid': 'cu2612', 'instrumentid_key': 'CU2612',
        'opendate': '20200101', 'expiredate': f'{today + timedelta(days=30):%Y%m%d}',
        'volumemultiple': '5',
    }
    info = mirror/f'nfs_rq/basicinfo/future_instrument/future_instrument{day}.csv.zip'
    archive(info, list(listing), [listing])
    row = tick('SHFE', 'cu2612', int(now.timestamp()*1000), '70000', 1)
    archive(mirror/f'nfs_rq/tick/future/{today:%Y/%m/%d}/FUTURE_TICK_CU_{day}.csv.zip', list(row), [row])
    assert cli(root, 'ctp-futures', 'import', mirror)['blocks'] == 1
    folder = root/'tick/CU2612'
    assert [p.name for p in folder.iterdir()] == [f'{day}.mkp']

    archive(info, list(listing), [dict(listing, expiredate=f'{today - timedelta(days=1):%Y%m%d}')])
    profile = tmp_path/'archive.json'
    result = cli(root, 'ctp-futures', 'import', mirror, '--profile', profile)
    assert result == {'imported': 0, 'skipped': 1, 'blocks': 0, 'rows': 0}
    assert [p.name for p in folder.iterdir()] == [f'{today:%Y}.data']
    assert mktpack.open(root)['tick'].read(symbol='CU2612').num_rows == 1
    counters = json.loads(profile.read_text())['counters']
    assert counters['month_days'] == counters['year_days'] == 1

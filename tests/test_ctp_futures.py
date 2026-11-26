import csv
import io
import json
import shutil
import sqlite3
import subprocess
import zipfile
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
    with sqlite3.connect(root/'mkt.sqlite') as db:
        assert db.execute('SELECT period FROM __timefile__ ORDER BY period').fetchall() == [(20260901,), (20261008,)]
        assert db.execute('SELECT path FROM __volumes__ ORDER BY path').fetchall() == [
            ('tick/AP2701/202609.data',), ('tick/AP2701/202610.data',),
        ]
    assert mktpack.open(root)['tick'].read(symbol='AP2701').num_rows == 2

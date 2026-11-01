import json
import os
from pathlib import Path
import sqlite3

import pytest


@pytest.fixture(scope='session')
def real_dataset(tmp_path_factory):
    '''将只读历史样本装入当前格式，转换仅用于回归夹具。'''
    original = Path(os.environ['MKTPACK_REAL_DATA']).resolve()
    target = tmp_path_factory.mktemp('real-source')
    with sqlite3.connect(f'file:{original / "mkt.sqlite"}?mode=ro', uri=True) as source:
        with sqlite3.connect(target / 'mkt.sqlite') as db:
            source.backup(db)
            for definition in ["storage TEXT NOT NULL DEFAULT 'mkp'", "location TEXT NOT NULL DEFAULT ''",
                               "table_name TEXT NOT NULL DEFAULT ''", "time_column TEXT NOT NULL DEFAULT 'time'",
                               "symbol_column TEXT NOT NULL DEFAULT 'symbol_id'"]:
                db.execute(f'ALTER TABLE __group__ ADD COLUMN {definition}')
            db.execute('CREATE TABLE __sqlite_partitions__(group_id INTEGER,start INTEGER,end INTEGER,volume INTEGER,rows INTEGER,first_time INTEGER,last_time INTEGER,PRIMARY KEY(group_id,start)) WITHOUT ROWID')
            schema = json.loads(db.execute("SELECT schema FROM __group__ WHERE name='funding_rate_settlement'").fetchone()[0])
            schema['symbol'] = {'type': 'string'}
            schema['funding_rate']['scale_column'] = 'scale'
            db.execute("UPDATE __group__ SET storage='sqlite' WHERE kind='ordinary'")
            db.execute("UPDATE __group__ SET storage='sqlite',location='funding.sqlite',schema=? WHERE name='funding_rate_settlement'", (json.dumps(schema),))
            with sqlite3.connect(target / 'funding.sqlite') as funding:
                sql = db.execute("SELECT sql FROM sqlite_master WHERE name='funding_rate_settlement'").fetchone()[0]
                funding.execute(sql)
                funding.executemany('INSERT INTO funding_rate_settlement VALUES (?,?,?,?,?)', db.execute('SELECT * FROM funding_rate_settlement'))
            db.execute('DROP TABLE funding_rate_settlement')
    for path in original.rglob('*.mkp'):
        output = target / path.relative_to(original)
        output.parent.mkdir(parents=True, exist_ok=True)
        os.link(path, output)
    return target

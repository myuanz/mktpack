from decimal import Decimal
import json
import struct

import mktpack
import pyarrow as pa
import pytest
from test_package import source, add_file


def test_projection_keeps_order_precision_and_row_count(source):
    root, db = source
    add_file(root, db, 'quotes', 'B', 20240102, [1, 2, 2], ['1.23', '4.56', '7.89'])
    add_file(root, db, 'quotes', 'A', 20240101, [0, 2, 3], ['1', '9000000000000000000', '2'])
    quotes = mktpack.open(root)['quotes']
    table = quotes.read(columns=['value', 'symbol'])
    assert table.column_names == ['value', 'symbol']
    assert table['value'].type == pa.decimal128(38, 2)
    assert table['symbol'].to_pylist() == ['A', 'B', 'A', 'B', 'B', 'A']
    assert table['value'].to_pylist() == list(map(Decimal, ['1', '1.23', '9000000000000000000', '4.56', '7.89', '2']))
    assert quotes.read(columns=[]).num_rows == 6
    assert quotes.read(columns=['symbol', 'value', 'value']).num_columns == 3
    assert quotes.read(symbol='missing', columns=['value', 'symbol']).schema == table.schema
    with pytest.raises(KeyError):
        quotes.read(symbol='missing', columns=['unknown'])


def test_projection_skips_unrequested_zstd_frame(source):
    root, db = source
    path = add_file(root, db, 'volume', 'A', 20240101, [1, 2], quantities={
        'volume': ['10', '20'], 'taker_buy_volume': ['2', '5'], 'unused': ['123', '456']
    }, lossy=True)
    data = bytearray(path.read_bytes())
    length, = struct.unpack_from('<I', data, 8)
    header = json.loads(data[12:12+length])
    offset = 12+length
    for col in header['columns']:
        if col['name'] == 'unused':
            data[offset] ^= 0xff
            break
        offset += col['size']
    path.write_bytes(data)
    group = mktpack.open(root)['volume']
    assert group.read(columns=['taker_buy_volume']).column(0).to_pylist() == [Decimal(2), Decimal(5)]
    assert group.read(columns=['symbol']).column(0).to_pylist() == ['A', 'A']
    with pytest.raises(RuntimeError):
        group.read()

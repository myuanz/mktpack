import json
import struct

import mktpack
import pyarrow as pa
import pytest
from test_package import source, add_file


def test_projection_keeps_symbol_time_order_and_row_count(source):
    root, db = source
    add_file(root, db, 'quotes', 'B', 20240102, [1, 2, 2], ['1.23', '4.56', '7.89'])
    add_file(root, db, 'quotes', 'A', 20240101, [0, 2, 3], ['1', '9000000000000000000', '2'])
    quotes = mktpack.open(root)['quotes']
    table = quotes.read(columns=['value', 'symbol'])
    assert table.column_names == ['value', 'symbol']
    assert table['value'].type == pa.float64()
    assert table['symbol'].type == pa.dictionary(pa.int32(), pa.string())
    assert table['symbol'].to_pylist() == ['A', 'A', 'A', 'B', 'B', 'B']
    assert table['value'].to_pylist() == [1.0, 9e18, 2.0, 1.23, 4.56, 7.89]
    assert quotes.read(columns=[]).num_rows == 6
    dup = quotes.read(columns=['symbol', 'value', 'value', 'symbol'])
    assert dup.num_columns == 4
    assert dup.column(0).equals(dup.column(3))
    assert dup.column(1).equals(dup.column(2))
    assert quotes.read(symbol='missing', columns=['value', 'symbol']).schema == table.schema
    with pytest.raises(KeyError):
        quotes.read(symbol='missing', columns=['unknown'])

    # 同一合约内部块时间重叠时自动在合约内按时间稳定归并
    add_file(root, db, 'quotes', 'A', 20240102, [1, 2], ['10.5', '20.5'])
    overlapped = quotes.read(symbol='A', columns=['time', 'value', 'symbol'])
    assert overlapped['time'].cast(pa.int64()).to_pylist() == [0, 1, 2, 2, 3]
    assert overlapped['value'].to_pylist() == [1.0, 10.5, 9e18, 20.5, 2.0]
    assert overlapped['symbol'].to_pylist() == ['A'] * 5


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
    assert group.read(columns=['taker_buy_volume']).column(0).to_pylist() == [2.0, 5.0]
    assert group.read(columns=['symbol']).column(0).to_pylist() == ['A', 'A']
    with pytest.raises(RuntimeError):
        group.read()

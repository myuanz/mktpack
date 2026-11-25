import json
from pathlib import Path
import pyarrow as pa
from . import _native


def write(path: str | Path, table: pa.Table, *, schema: dict | None = None, metadata: dict | None = None, lossy_taker: bool = False) -> None:
    '''写入单个时序文件，可选主动买量比例量化。'''
    _native.write(str(path), table, json.dumps(schema or {}), json.dumps(metadata or {}), lossy_taker)


def read(path: str | Path) -> pa.Table:
    '''读取数值列并精确恢复 Arrow 类型。'''
    return _table(*_native.read(str(path)))


_types = {'timestamp': pa.timestamp('ms', tz='UTC'), 'integer': pa.int64(),
          'real': pa.float64(), 'string': pa.string(), 'blob': pa.binary(), 'null': pa.null()}
_types['any'] = pa.dense_union([pa.field(name, _types[name])
                               for name in ('null', 'integer', 'real', 'string', 'blob')])


def _array(column) -> pa.Array:
    kind, scale, rows, buffers, children = column
    dtype = pa.decimal128(38, scale) if kind == 'decimal' else _types[kind]
    return pa.Array.from_buffers(dtype, rows, [None if b is None else pa.py_buffer(b) for b in buffers],
                                 children=[_array(child) for child in children])


def _table(rows, names, columns) -> pa.Table:
    if not names:
        batch = pa.RecordBatch.from_struct_array(pa.Array.from_buffers(pa.struct([]), rows, [None]))
        return pa.Table.from_batches([batch])
    return pa.Table.from_arrays([_array(c) for c in columns], names=names)


def info(path: str | Path) -> dict:
    return json.loads(_native.info(str(path)))

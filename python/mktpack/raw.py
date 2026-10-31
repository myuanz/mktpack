import json
from decimal import Decimal
from pathlib import Path
import pyarrow as pa
from . import _native


def write(path: str | Path, table: pa.Table, *, schema: dict | None = None, metadata: dict | None = None, lossy_taker: bool = False) -> None:
    '''写入单个时序文件，可选主动买量比例量化。'''
    columns, inferred = {}, {}
    metadata = dict(metadata or {})
    for name in table.column_names:
        column = table[name]
        if name == "symbol":
            values = column.unique().to_pylist()
            if len(values) > 1:
                raise ValueError("单文件只能保存一个 symbol")
            if values:
                metadata["symbol"] = values[0]
            continue
        if column.null_count:
            raise ValueError("时序数值列不接受 null")
        if pa.types.is_timestamp(column.type):
            columns[name] = column.cast(pa.timestamp("ms", tz="UTC")).cast(pa.int64()).to_pylist()
            inferred[name] = {"type": "timestamp", "scale": 0}
        elif pa.types.is_integer(column.type):
            columns[name] = column.to_pylist()
            inferred[name] = {"type": "integer", "scale": 0}
        else:
            values = [v if isinstance(v, Decimal) else Decimal(str(v)) for v in column.to_pylist()]
            scale = column.type.scale if pa.types.is_decimal(column.type) else max((max(0, -v.as_tuple().exponent) for v in values), default=0)
            columns[name] = [int(v.scaleb(scale)) for v in values]
            inferred[name] = {"type": "decimal", "scale": scale}
    if schema:
        for name, spec in schema.items():
            old, new = inferred[name]["scale"], spec.get("scale", 0)
            if new >= old:
                columns[name] = [v * 10 ** (new - old) for v in columns[name]]
            else:
                divisor = 10 ** (old - new)
                if any(v % divisor for v in columns[name]):
                    raise ValueError(f"{name} 指定精度不足")
                columns[name] = [v // divisor for v in columns[name]]
    stored_schema = {name: dict(spec) for name, spec in (schema or inferred).items()}
    if lossy_taker:
        stored_schema["taker_buy_volume"].update(encoding="ratio_1000", reference="volume")
    _native.write(str(path), columns, json.dumps(stored_schema), json.dumps(metadata))


def read(path: str | Path) -> pa.Table:
    '''读取数值列并精确恢复 Arrow 类型。'''
    return _table(*_native.read(str(path)))


def _table(columns, schema, metadata):
    schema, metadata = json.loads(schema), json.loads(metadata)
    arrays = {}
    for name, values in columns.items():
        spec = schema[name]
        if spec["type"] == "timestamp":
            arrays[name] = pa.array(values, type=pa.timestamp("ms", tz="UTC"))
        elif spec["type"] == "decimal":
            scale = spec["scale"]
            arrays[name] = pa.array([Decimal(v).scaleb(-scale) for v in values], type=pa.decimal128(38, scale))
        else:
            arrays[name] = pa.array(values, type=pa.int64())
    if metadata.get("symbol"):
        arrays["symbol"] = pa.array([metadata["symbol"]] * len(next(iter(columns.values()))))
    return pa.table(arrays)


def info(path: str | Path) -> dict:
    return json.loads(_native.info(str(path)))

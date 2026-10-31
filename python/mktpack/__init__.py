import json
from datetime import date, datetime, timezone
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
from . import _native, raw


def _time(value, default):
    if value is None:
        return default
    if isinstance(value, int):
        value = str(value)
    if isinstance(value, str):
        value = datetime.strptime(value, "%Y%m%d") if len(value) == 8 else datetime.fromisoformat(value)
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime(value.year, value.month, value.day)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


class Group:
    def __init__(self, root: str, name: str):
        self.root, self.name = root, name

    def describe(self) -> dict:
        return json.loads(_native.describe(self.root, self.name))

    def read(self, *, symbol: str | None = None, symbols: list[str] | None = None, start=None, end=None, columns: list[str] | None = None) -> pa.Table:
        tables = [raw._table(*t) for t in _native.query(self.root, self.name, symbols or ([symbol] if symbol else []), _time(start, -(2**63)), _time(end, 2**63 - 1))]
        if tables:
            scales = {f.name: max(t.schema.field(f.name).type.scale for t in tables) for f in tables[0].schema if pa.types.is_decimal(f.type)}
            schema = pa.schema([pa.field(f.name, pa.decimal128(38, scales[f.name]) if f.name in scales else f.type) for f in tables[0].schema])
            result = pa.concat_tables([t.cast(schema) for t in tables])
            result = result.take(pc.sort_indices(result, sort_keys=[("time", "ascending")]))
        else:
            fields = self.describe()["schema"]
            result = pa.table({name: pa.array([], type=pa.timestamp("ms", tz="UTC") if s["type"] == "timestamp" else pa.decimal128(38, s.get("scale", 0)) if s["type"] == "decimal" else pa.int64()) for name, s in fields.items()})
            result = result.append_column("symbol", pa.array([], type=pa.string()))
        return result.select(columns) if columns is not None else result


class Dataset:
    def __init__(self, root: str | Path):
        self.root = str(root)

    def describe(self) -> list[dict]:
        return json.loads(_native.describe(self.root, ""))

    def __getitem__(self, name: str):
        groups = {g["name"]: g["kind"] for g in self.describe()}
        if name not in groups:
            raise KeyError(name)
        return json.loads(_native.ordinary(self.root, name)) if groups[name] == "ordinary" else Group(self.root, name)


def open(root: str | Path) -> Dataset:
    return Dataset(root)

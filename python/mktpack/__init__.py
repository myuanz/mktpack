import json
from datetime import date, datetime, timezone
from pathlib import Path
import pyarrow as pa
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
        data = _native.query(self.root, self.name, symbols or ([symbol] if symbol else []),
                             _time(start, -(2**63)), _time(end, 2**63 - 1), columns)
        return raw._table(*data)


class Dataset:
    def __init__(self, root: str | Path):
        self.root = str(root)

    def describe(self) -> list[dict]:
        return json.loads(_native.describe(self.root, ""))

    def __getitem__(self, name: str):
        groups = {g["name"]: g["kind"] for g in self.describe()}
        if name not in groups:
            raise KeyError(name)
        return _native.ordinary(self.root, name) if groups[name] == "ordinary" else Group(self.root, name)


def open(root: str | Path) -> Dataset:
    return Dataset(root)

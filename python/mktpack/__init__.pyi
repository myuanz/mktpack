from collections.abc import Iterator
from datetime import date, datetime
from os import PathLike
from typing import Any

from pyarrow import Table

from . import raw as raw

type _Path = str | PathLike[str]
type _Time = date | datetime | str | int

class Group:
    def __init__(self, root: _Path, name: str) -> None: ...
    @property
    def root(self) -> str: ...
    @property
    def name(self) -> str: ...
    def describe(self, symbol: str | None = None) -> dict[str, Any]: ...
    def symbols(
        self,
        *,
        start: _Time | None = None,
        end: _Time | None = None,
    ) -> list[str]: ...
    def read(
        self,
        *,
        symbol: str | None = None,
        symbols: list[str] | None = None,
        start: _Time | None = None,
        end: _Time | None = None,
        columns: list[str] | None = None,
    ) -> Table: ...

class Dataset:
    def __init__(self, root: _Path) -> None: ...
    @property
    def root(self) -> str: ...
    def describe(self) -> list[dict[str, Any]]: ...
    def keys(self) -> list[str]: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[str]: ...
    def __contains__(self, name: str) -> bool: ...
    def __getitem__(self, name: str) -> Group | dict[str, Any] | list[dict[str, Any]]: ...

def open(root: _Path) -> Dataset: ...
def meta(root: _Path) -> dict[str, str]: ...
def set_meta(root: _Path, key: str, value: str) -> None: ...
def merge_dataset(
    target: _Path,
    source: _Path,
    *,
    groups: list[str] | None = None,
    symbols: list[str] | None = None,
) -> dict[str, Any]: ...
def compact(
    root: _Path,
    *,
    max_file_size: int | None = None,
    before: _Time | None = None,
) -> dict[str, Any]: ...
def check(root: _Path) -> dict[str, Any]: ...
def package(
    source: _Path,
    destination: _Path,
    *,
    max_file_size: int = ...,
    groups: list[str] | None = None,
    symbols: list[str] | None = None,
    start: _Time | None = None,
    end: _Time | None = None,
    jobs: int = 4,
) -> dict[str, Any]: ...
def merge_zips(
    target: _Path,
    paths: list[_Path],
    *,
    groups: list[str] | None = None,
    symbols: list[str] | None = None,
    target_version: str = "",
) -> dict[str, Any]: ...
def merge_remote(
    target: _Path,
    url: str,
    *,
    groups: list[str] | None = None,
    symbols: list[str] | None = None,
    target_version: str = "",
) -> dict[str, Any]: ...
def import_ctp_ticks(
    root: _Path,
    mirror: _Path,
    *,
    start: str = "",
    end: str = "",
    symbols: list[str] | None = None,
    jobs: int = 16,
    readers: int = 1,
    profile: _Path | None = None,
) -> dict[str, Any]: ...
def download_binance(
    root: _Path,
    *,
    symbols: list[str] | None = None,
    groups: list[str] | None = None,
    start: _Time = "2024-01-01",
    end: _Time | None = None,
    jobs: int = 4,
    cache: _Path | None = None,
    full: bool = False,
    lossy_taker: bool = False,
) -> None: ...

from os import PathLike
from typing import Any

from pyarrow import Table

type _Path = str | PathLike[str]

def read(path: _Path) -> Table: ...
def write(
    path: _Path,
    table: Table,
    *,
    schema: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    lossy_taker: bool = False,
) -> None: ...
def info(path: _Path) -> dict[str, Any]: ...

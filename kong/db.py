from typing import TYPE_CHECKING, Any, List

if not TYPE_CHECKING:
    from playhouse.sqlite_ext import *
else:
    class SqliteExtDatabase:
        def __init__(self, *args: Any) -> None: ...
        def init(self, *args: Any) -> None: ...
        def connect(self) -> None: ...
        def create_tables(self, tables: List[Any]) -> None: ...

database = SqliteExtDatabase(None)

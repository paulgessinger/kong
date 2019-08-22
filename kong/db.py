from typing import TYPE_CHECKING, Any, List, ContextManager

if not TYPE_CHECKING:
    from playhouse.sqlite_ext import SqliteExtDatabase, AutoIncrementField
else:  # pragma: no cover

    class SqliteExtDatabase:
        def __init__(self, *args: Any) -> None:
            ...

        def init(self, *args: Any) -> None:
            ...

        def connect(self) -> None:
            ...

        def create_tables(self, tables: List[Any]) -> None:
            ...

        def atomic(self) -> ContextManager[None]:
            ...

    class AutoIncrementField:
        ...


database = SqliteExtDatabase(None)

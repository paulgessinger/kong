"""
Singleton database instance
"""
from typing import TYPE_CHECKING, Any, List, ContextManager

if not TYPE_CHECKING:
    from playhouse.sqlite_ext import SqliteExtDatabase, AutoIncrementField
else:  # pragma: no cover

    class SqliteExtDatabase:
        """
        Mypy stub for the not type-hinted SqliteExtDatabase class
        """

        def __init__(self, *args: Any) -> None:
            """
            Type stub
            :param args:
            """
            ...

        def init(self, *args: Any) -> None:
            """
            Type stub
            :param args:
            :return:
            """
            ...

        def connect(self) -> None:
            """
            Type stub
            :return:
            """
            ...

        def create_tables(self, tables: List[Any]) -> None:
            """
            Type stub
            :param tables:
            :return:
            """
            ...

        def atomic(self) -> ContextManager[None]:
            """
            Type stub
            :return:
            """
            ...

    class AutoIncrementField:
        """
        Type stub
        """

        ...


database = SqliteExtDatabase(None)

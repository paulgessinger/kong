"""
Polyfill for a JSON field in SQLite.
Newer versions of sqlite (>= 3.9.0) have a native JSON extension, which we use.
If the sqlite version is lower, we roll a less-optimal replacement.
"""
from typing import Dict, cast

import peewee as pw
from peewee import sqlite3
import json

if sqlite3.sqlite_version_info < (3, 9, 0):  # type: ignore

    class JSONField(pw.CharField):  # pragma: no cover
        """
        Polyfill class to provide a JSON field
        """

        def db_value(self, value: Dict) -> str:
            """
            Convert a value to a string for storage in a `CharField`
            :param value: The value to store
            :return: The JSON string
            """
            return json.dumps(value)

        def python_value(self, value: str) -> Dict:
            """
            Convert a string value from the database back to what it was.
            :param value: The string value
            :return: Parsed JSON value
            """
            return cast(Dict, json.loads(value))


else:  # pragma: no cover
    # flake8: noqa
    from playhouse.sqlite_ext import JSONField  # type: ignore

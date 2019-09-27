from typing import Dict, cast

import peewee as pw
import sqlite3
import json

if sqlite3.sqlite_version_info < (3, 9, 0):  # type: ignore

    class JSONField(pw.CharField):  # pragma: no cover
        def db_value(self, value: Dict) -> str:
            return json.dumps(value)

        def python_value(self, value: str) -> Dict:
            return cast(Dict, json.loads(value))


else:  # pragma: no cover
    # flake8: noqa
    from playhouse.sqlite_ext import JSONField  # type: ignore

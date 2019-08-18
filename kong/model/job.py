from typing import Any, List, Dict

import peewee as pw

from ..logger import logger
from . import BaseModel
from .. import drivers


class EnumField(pw.IntegerField):
    def __init__(self, choices: List, *args: Any, **kwargs: Any):
        self.to_db: Dict[str, int] = {v: k for k, v in enumerate(choices)}
        self.from_db: Dict[int, str] = {k: v for k, v in enumerate(choices)}
        super(pw.IntegerField, self).__init__(*args, **kwargs)

    def db_value(self, value: str) -> int:
        return self.to_db[value]

    def python_value(self, value: int) -> str:
        return self.from_db[value]


class Job(BaseModel):
    class Meta:
        pass
        # indexes = ((("parent", "name"), True),)

    job_id = pw.AutoField()
    batch_job_id = pw.CharField(index=True)
    driver = EnumField(choices=drivers.__all__)
    parent = pw.ForeignKeyField("self", null=True, backref="jobs")

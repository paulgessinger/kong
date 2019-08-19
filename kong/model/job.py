from typing import Any, List, Dict, Union, cast, TYPE_CHECKING

import peewee as pw
from playhouse.sqlite_ext import JSONField # type: ignore

from kong.model import Folder
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
        indexes = ((("batch_job_id", "driver"), True),) # batch job is is unique per driver

    if TYPE_CHECKING:
        job_id: int
        batch_job_id: str
        driver: drivers.Driver
        folder: Folder
        command: str
        data: Dict[str, Any]
    else:
        job_id = pw.AutoField()
        batch_job_id = pw.CharField(index=True, null=True) # can be null, some drivers only know after submission
        driver = EnumField(choices=drivers.__all__, null=False)
        folder = pw.ForeignKeyField(Folder, null=False, backref="jobs")
        command = pw.CharField(null=False) # should allow arbitrary length in sqlite
        data = JSONField(default={})

    def save(self, *args: Any, **kwargs: Any) -> None:
        assert self.driver in drivers.__all__, f"{self.driver} is not a valid driver"
        assert self.command is not None, "Need to specify a command"
        assert len(self.command) > 0, "Command must be longer than 0"
        super().save(*args, **kwargs)

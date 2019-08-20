from contextlib import contextmanager
from enum import IntFlag
from functools import wraps
from typing import (
    Any,
    List,
    Dict,
    Union,
    cast,
    TYPE_CHECKING,
    Optional,
    Callable,
    ContextManager,
    IO,
)

import peewee as pw
from playhouse.sqlite_ext import JSONField  # type: ignore

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


def with_driver(f: Any) -> Any:
    @wraps(f)
    def wrapper(self: "Job", *args: Any, **kwargs: Any) -> Any:
        return f(self, self._driver_instance, *args, **kwargs)

    return wrapper


class Job(BaseModel):
    class Status(IntFlag):
        CREATED = 0
        SUBMITTED = 1
        RUNNING = 2
        FAILED = 3
        COMPLETED = 4
        UNKOWN = 5

    class Meta:
        indexes = (
            (("batch_job_id", "driver"), True),
        )  # batch job is is unique per driver

    if TYPE_CHECKING:  # pragma: no cover
        job_id: int
        batch_job_id: str
        driver: drivers.Driver
        folder: Folder
        command: str
        data: Dict[str, Any]
        status: Status
    else:
        job_id = pw.AutoField()
        batch_job_id = pw.CharField(
            index=True, null=True
        )  # can be null, some drivers only know after submission
        driver = EnumField(choices=drivers.__all__, null=False)
        folder = pw.ForeignKeyField(Folder, null=False, backref="jobs")
        command = pw.CharField(null=False)  # should allow arbitrary length in sqlite
        data = JSONField(default={})
        status = EnumField(choices=Status, null=False, default=Status.CREATED)

    _driver_instance: Optional[drivers.Driver]

    def save(self, *args: Any, **kwargs: Any) -> None:
        assert self.driver in drivers.__all__, f"{self.driver} is not a valid driver"
        assert self.command is not None, "Need to specify a command"
        assert len(self.command) > 0, "Command must be longer than 0"
        super().save(*args, **kwargs)

    @with_driver
    def submit(self, driver: drivers.Driver) -> Any:
        driver.submit(self)

    @with_driver
    def wait(self, driver: drivers.Driver, timeout: Optional[int] = None) -> None:
        driver.wait(self, timeout=timeout)

    @with_driver
    def kill(self, driver: drivers.Driver) -> None:
        driver.kill(self)

    @with_driver
    def get_status(self, driver: drivers.Driver) -> Status:
        driver.sync_status(self)
        return self.status

    @with_driver  # type: ignore
    @contextmanager  # type: ignore
    def stdout(self, driver: drivers.Driver) -> ContextManager[None]:
        fh: IO[str]
        with driver.stdout(self) as fh:
            yield fh

    @with_driver  # type: ignore
    @contextmanager  # type: ignore
    def stderr(self, driver: drivers.Driver) -> ContextManager[None]:
        fh: IO[str]
        with driver.stderr(self) as fh:
            yield fh

    @property
    def is_done(self) -> bool:
        return self.status not in (Job.Status.RUNNING, Job.Status.SUBMITTED)

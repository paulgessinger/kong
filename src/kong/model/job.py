import datetime
from contextlib import contextmanager
from enum import IntFlag
from functools import wraps
from typing import Any, List, Dict, Union, cast, TYPE_CHECKING, Optional, Type, Iterator

import peewee as pw

from ..json_field import JSONField
from ..db import AutoIncrementField
from ..drivers import DriverMismatch
from ..drivers.driver_base import DriverBase
from ..config import Config
from ..model.folder import Folder
from . import BaseModel


class EnumField(pw.IntegerField):
    def __init__(self, choices: List, *args: Any, **kwargs: Any):
        self.from_db: Dict[int, str] = {int(k): k for k in choices}
        super(pw.IntegerField, self).__init__(*args, **kwargs)

    def db_value(self, value: "Job.Status") -> int:
        return int(value)

    def python_value(self, value: int) -> str:
        return self.from_db[value]


def with_driver(f: Any) -> Any:
    @wraps(f)
    def wrapper(self: "Job", *args: Any, **kwargs: Any) -> Any:
        assert (
            self._driver_instance is not None
        ), "Cannot call this method without a driver instance"
        return f(self, self._driver_instance, *args, **kwargs)

    return wrapper


class DriverField(pw.CharField):
    def __init__(self, *args: Any, **kwargs: Any):
        return super().__init__(*args, **kwargs)

    def db_value(self, value: Any) -> str:
        assert issubclass(value, DriverBase)
        class_name = ".".join([value.__module__, value.__name__])
        return class_name

    def python_value(selfself, value: str) -> Type[DriverBase]:
        import importlib

        components = value.split(".")
        module_name = ".".join(components[:-1])
        class_name = components[-1]

        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)
        return cast(Type[DriverBase], class_)


class Job(BaseModel):
    class Status(IntFlag):
        UNKNOWN = 5
        CREATED = 0
        SUBMITTED = 1
        RUNNING = 2
        FAILED = 3
        COMPLETED = 4

    class Meta:
        indexes = (
            (("batch_job_id", "driver"), True),
        )  # batch job is is unique per driver

    if TYPE_CHECKING:  # pragma: no cover
        job_id: int
        batch_job_id: str
        driver: Type[DriverBase]
        folder: "Folder"
        command: str
        data: Dict[str, Any]
        status: Status
        created_at: datetime.datetime
        updated_at: datetime.datetime
        cores: int
        memory: int
    else:
        job_id = AutoIncrementField(column_name="rowid")
        batch_job_id = pw.CharField(
            index=True, null=True
        )  # can be null, some drivers only know after submission
        # driver = EnumField(choices=drivers.__all__, null=False)
        driver = DriverField(null=False)
        folder = pw.ForeignKeyField(Folder, null=False, backref="jobs")
        command = pw.CharField(null=False)  # should allow arbitrary length in sqlite
        data = JSONField(default={})
        cores = pw.IntegerField(null=False, default=1)
        memory = pw.IntegerField(null=False, default=1000)  # memory in Megabytes
        status = EnumField(choices=Status, null=False, default=Status.CREATED)

        created_at = pw.DateTimeField(default=datetime.datetime.now)
        updated_at = pw.DateTimeField()

    _driver_instance: Optional[DriverBase] = None

    def ensure_driver_instance(self, arg: Union[DriverBase, Config]) -> None:
        if self._driver_instance is not None:
            return
        if isinstance(arg, Config):
            self._driver_instance = self.driver(arg)
        else:
            if not isinstance(arg, self.driver):
                raise DriverMismatch(
                    f"Given driver {arg} is not instance of {self.driver}"
                )
            self._driver_instance = arg

    @property
    def driver_instance(self) -> DriverBase:
        assert self._driver_instance is not None
        return self._driver_instance

    def save(self, *args: Any, **kwargs: Any) -> None:
        # assert self.driver in drivers.__all__, f"{self.driver} is not a valid driver"
        assert self.command is not None, "Need to specify a command"
        assert len(self.command) > 0, "Command must be longer than 0"
        self.updated_at = datetime.datetime.now()
        super().save(*args, **kwargs)

    @with_driver
    def remove(self, driver: DriverBase) -> None:
        driver.remove(self)

    @property
    def log_dir(self) -> str:
        return str(self.data["log_dir"])

    @property
    def output_dir(self) -> str:
        return str(self.data["output_dir"])

    @with_driver
    def submit(self, driver: DriverBase) -> Any:
        driver.submit(self)

    @with_driver
    def resubmit(self, driver: DriverBase) -> Any:
        driver.resubmit(self)

    @with_driver
    def wait(self, driver: DriverBase, timeout: Optional[int] = None) -> None:
        driver.wait(self, timeout=timeout)

    @with_driver
    def kill(self, driver: DriverBase) -> None:
        driver.kill(self)

    @with_driver
    def get_status(self, driver: DriverBase) -> Status:  # noqa: F821
        self.reload()
        driver.sync_status(self)
        return self.status

    @with_driver  # type: ignore
    @contextmanager  # type: ignore
    def stdout(self, driver: DriverBase) -> Iterator[None]:
        with driver.stdout(self) as fh:
            yield fh

    @with_driver  # type: ignore
    @contextmanager  # type: ignore
    def stderr(self, driver: DriverBase) -> Iterator[None]:
        with driver.stderr(self) as fh:
            yield fh

    def __str__(self) -> str:
        return f"Job<{self.job_id}, {self.batch_job_id}, {str(self.status)}>"


color_dict = {
    Job.Status.UNKNOWN: "red",
    Job.Status.CREATED: "white",
    Job.Status.SUBMITTED: "yellow",
    Job.Status.RUNNING: "blue",
    Job.Status.FAILED: "red",
    Job.Status.COMPLETED: "green",
}

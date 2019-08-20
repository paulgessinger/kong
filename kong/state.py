import os
from typing import List, Callable, Any, Union, Optional, Tuple

import peewee as pw

from kong.drivers import DriverBase, DriverMismatch
from . import config, drivers
from .db import database
from . import model
from .model import Folder, Job
from .logger import logger


class CannotCreateError(RuntimeError):
    pass


class CannotRemoveRoot(RuntimeError):
    pass


class DoesNotExist(RuntimeError):
    pass


JobSpec = Union[str, int, Job]


class State:
    def __init__(self, config: config.Config, cwd: Folder) -> None:
        self.config = config
        self.cwd = cwd
        self.default_driver: drivers.Driver = getattr(
            drivers, self.config.default_driver
        )(self.config)

    @classmethod
    def get_instance(cls) -> "State":
        cfg = config.Config()
        logger.debug("Initialized config: %s", cfg.data)

        logger.debug(
            "Initializing database '%s' at '%s'", config.APP_NAME, config.DB_FILE
        )
        database.init(config.DB_FILE)

        # ensure database is set up
        database.connect()
        database.create_tables([getattr(model, m) for m in model.__all__])

        cwd = Folder.get_root()

        return cls(cfg, cwd)

    def ls(
        self, path: str = ".", refresh: bool = False
    ) -> Tuple[List["Folder"], List["Job"]]:
        "List the current directory content"
        logger.debug("%s", list(self.cwd.children))
        folder = Folder.find_by_path(self.cwd, path)
        if folder is None:
            raise pw.DoesNotExist()

        jobs = folder.jobs

        if refresh == True and len(jobs) > 0:
            # try bulk refresh first
            jobs[0].ensure_driver_instance(self.config)
            driver: DriverBase = jobs[0].driver_instance

            try:
                logger.debug("Attempting bulk mode sync using %s", driver.__class__)
                driver.bulk_sync_status(jobs)
            except DriverMismatch:
                # fall back to slow mode
                logger.debug("Bulk mode sync failed, falling back to slow loop mode")
                for job in jobs:
                    job.ensure_driver_instance(self.config)
                    job.get_status()

        return folder.children, jobs

    def cd(self, name: str = ".") -> None:
        if name == "":
            folder = Folder.get_root()
        else:
            _folder = Folder.find_by_path(self.cwd, name)
            if _folder is None:
                raise pw.DoesNotExist()
            folder = _folder
        self.cwd = folder

    def mkdir(self, path: str) -> None:
        head, tail = os.path.split(path)

        location = Folder.find_by_path(self.cwd, head)
        if location is None:
            raise CannotCreateError(f"Cannot create folder at '{path}'")
        logger.debug("Attempt to create folder named '%s' in '%s'", tail, location.path)

        Folder.create(name=tail, parent=location)

    def rm(
        self, name: Union[str, Job, Folder], confirm: Callable[[], bool] = lambda: True
    ) -> bool:
        if isinstance(name, str):
            # string name, could be both
            if name == "/":
                raise CannotRemoveRoot()

            # try to find folder first
            folder = Folder.find_by_path(self.cwd, name)
            if folder is not None:
                if confirm():
                    folder.delete_instance(recursive=True, delete_nullable=True)
                    return True
                return False

            # is not a folder, let's look for a job
            if not name.isdigit():
                # not a job, done
                raise DoesNotExist(f"Object {name} in {self.cwd.path} does not exist")

            # should be unique, shouldn't matter where we are
            job = Job.get_or_none(job_id=int(name))
            if job is None:
                raise DoesNotExist(f"Object {name} in {self.cwd.path} does not exist")

            if confirm():
                # need driver instance
                job.driver_instance = self.config
                job.delete_instance()
                return True

            return False
        elif isinstance(name, Job):
            job = name

            if confirm():
                # need driver instance
                job.ensure_driver_instance(self.config)
                job.delete_instance()
                return True
            return False
        elif isinstance(name, Folder):
            folder = name
            if confirm():
                folder.delete_instance(recursive=True, delete_nullable=True)
                return True
            return False
        else:
            return False

    def create_job(self, *args: Any, **kwargs: Any) -> Job:
        assert (
            "folder" not in kwargs
        ), "To submit to explicit folder, use driver directly"
        assert "driver" not in kwargs, "To submit with explicit driver, use it directly"
        kwargs["folder"] = self.cwd
        return self.default_driver.create_job(*args, **kwargs)

    def _extract_job(self, name: JobSpec) -> Optional[Job]:
        if isinstance(name, int):
            job = Job.get_or_none(name)
        elif isinstance(name, str):
            # check if we have path component
            if name.isdigit():
                job = Job.get_or_none(int(name))
            else:
                head, tail = os.path.split(name)
                logger.debug("Getting job: head: %s, tail: %s", head, tail)
                assert tail.isdigit()
                jobid = int(tail)
                job = Job.get_or_none(jobid)
        elif isinstance(name, Job):
            job = name
        else:
            raise TypeError("Name is neither job id nor job instance")
        return job

    def submit_job(self, name: JobSpec) -> None:
        job = self._extract_job(name)
        if job is None:
            raise DoesNotExist(f"Job at {name} does not exist")
        job.ensure_driver_instance(self.config)
        job.submit()

    def kill_job(self, name: JobSpec) -> None:
        job = self._extract_job(name)
        if job is None:
            raise DoesNotExist(f"Job at {name} does not exist")
        job.ensure_driver_instance(self.config)
        job.kill()

    def resubmit_job(self, name: JobSpec) -> None:
        job = self._extract_job(name)
        if job is None:
            raise DoesNotExist(f"Job at {name} does not exist")
        job.ensure_driver_instance(self.config)
        job.resubmit()

    def get_job(self, name: JobSpec) -> Optional[Job]:
        job = self._extract_job(name)
        if job is not None:
            job.ensure_driver_instance(self.config)
        return job

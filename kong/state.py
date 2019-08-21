import os
from typing import List, Callable, Any, Union, Optional, Tuple, cast, Iterable

import peewee as pw

from .drivers import DriverMismatch
from .drivers.driver_base import DriverBase
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
        self.default_driver: DriverBase = getattr(drivers, self.config.default_driver)(
            self.config
        )

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

    def refresh_jobs(self, jobs: List[Job]) -> None:
        first_job: Job = jobs[0]
        # try bulk refresh first
        first_job.ensure_driver_instance(self.config)
        driver: DriverBase = first_job.driver_instance
        try:
            logger.debug("Attempting bulk mode sync using %s", driver.__class__)
            driver.bulk_sync_status(cast(Iterable[Job], jobs))
        except DriverMismatch:
            # fall back to slow mode
            logger.debug("Bulk mode sync failed, falling back to slow loop mode")
            for job in jobs:
                job.ensure_driver_instance(self.config)
                job.get_status()

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
            self.refresh_jobs(jobs)

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
                job.ensure_driver_instance(self.config)
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

    def _extract_jobs(self, name: JobSpec) -> List[Job]:
        jobs: List[Job] = []

        if isinstance(name, int):
            j = Job.get_or_none(name)
            if j is None:
                raise DoesNotExist(f"Did not find job with id {name}")
            jobs = [j]

        elif isinstance(name, str):
            # check if we have path component
            if name.isdigit():
                j = Job.get_or_none(int(name))
                if j is None:
                    raise DoesNotExist(f"Did not find job with path {name}")
                jobs = [j]
            else:
                head, tail = os.path.split(name)
                logger.debug("Getting job: head: %s, tail: %s", head, tail)
                if tail.isdigit():
                    # single job id, just get that
                    j = Job.get_or_none(int(tail))
                    if j is None:
                        raise DoesNotExist(f"Did not find job with path {name}")
                    jobs = [j]
                elif tail == "*":
                    # "glob" jobs: get folder, and select all jobs
                    # this is not recursive right now
                    folder = Folder.find_by_path(self.cwd, head)
                    assert folder is not None
                    jobs = list(folder.jobs)
                else:
                    raise RuntimeError(f"{name} jobspec is not understood")
        elif isinstance(name, Job):
            jobs = [name]
        else:
            raise TypeError("Name is neither job id, path to job(s) nor job instance")

        return jobs

    def submit_job(self, name: JobSpec) -> None:
        jobs = self._extract_jobs(name)
        for job in jobs:
            job.ensure_driver_instance(self.config)
            job.submit()

    def kill_job(self, name: JobSpec) -> None:
        jobs = self._extract_jobs(name)
        for job in jobs:
            job.ensure_driver_instance(self.config)
            job.kill()

    def resubmit_job(self, name: JobSpec) -> None:
        jobs = self._extract_jobs(name)
        for job in jobs:
            job.ensure_driver_instance(self.config)
            job.resubmit()

    def get_jobs(self, name: JobSpec) -> List[Job]:
        return self._extract_jobs(name)

import datetime
import os
from typing import (
    List,
    Callable,
    Any,
    Union,
    Optional,
    Tuple,
    cast,
    Iterable,
    ContextManager,
)

import peewee as pw
from contextlib import contextmanager

from .drivers import DriverMismatch, get_driver
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
        self.default_driver: DriverBase = get_driver(self.config.default_driver)(
            self.config
        )

    @contextmanager  # type: ignore
    def pushd(self, folder: Union["Folder", str]) -> ContextManager[None]:
        prev = self.cwd

        if isinstance(folder, Folder):
            self.cwd = folder
        elif isinstance(folder, str):
            _folder = Folder.find_by_path(self.cwd, folder)
            assert _folder is not None
            self.cwd = _folder
        else:
            raise TypeError("Argument is neither Folder nor str")

        try:
            yield
        finally:
            self.cwd = prev

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
        self, path: str = ".", refresh: bool = False, recursive: bool = False
    ) -> Tuple[List["Folder"], List["Job"]]:
        "List the current directory content"
        logger.debug("%s", list(self.cwd.children))
        folder = Folder.find_by_path(self.cwd, path)
        if folder is None:
            raise pw.DoesNotExist()

        jobs = folder.jobs

        if refresh == True and len(jobs) > 0:
            if recursive == True:
                self.refresh_jobs(folder.jobs_recursive())
            else:
                self.refresh_jobs(jobs)

        return list(folder.children), list(jobs)

    def cd(self, target: Union[str, Folder] = ".") -> None:
        if isinstance(target, str):
            if target == "":
                folder = Folder.get_root()
            else:
                _folder = Folder.find_by_path(self.cwd, target)
                if _folder is None:
                    raise pw.DoesNotExist()
                folder = _folder
            self.cwd = folder
        elif isinstance(target, Folder):
            self.cwd = target
        else:
            raise TypeError(f"{target} is not str or Folder")

    def _mv_folder(self, source: Folder, dest: Union[str, Folder]) -> None:
        if isinstance(dest, Folder):
            # requested action: move source INTO dest
            source.parent = dest
            source.save()
        elif isinstance(dest, str):
            dest_folder = Folder.find_by_path(self.cwd, dest)
            if dest_folder is not None:
                # dest exists!
                # requested action: move source INTO dest
                source.parent = dest_folder
                source.save()
            else:
                # dest does not exist
                # requested action: RENAME source to dest and potentially move
                # head is new parent folder, tail is new name
                head, tail = os.path.split(dest)
                source.name = tail
                dest_folder = Folder.find_by_path(self.cwd, head)
                if dest_folder is None:
                    raise ValueError(f"Target folder {head} does not exist")

                source.parent = dest_folder
                source.save()
        else:
            raise TypeError(f"{dest} is neither string nor Folder")

    def _mv_folders(self, folders: List[Folder], dest: Union[str, Folder]) -> None:
        dest_folder: Folder
        if isinstance(dest, Folder):
            dest_folder = dest
        elif isinstance(dest, str):
            folder = Folder.find_by_path(self.cwd, dest)
            assert folder is not None
            dest_folder = folder
        else:
            raise TypeError(f"{dest} is neither string nor Folder")

        with database.atomic():
            Folder.update(parent=dest_folder, updated_at=datetime.datetime.now()).where(
                Folder.folder_id  # type: ignore
                << [f.folder_id for f in folders if f != dest_folder]  # type: ignore
            ).execute()

    def _mv_jobs(self, jobs: List[Job], dest: Union[str, Folder]) -> None:
        dest_folder: Optional[Folder]
        if isinstance(dest, Folder):
            dest_folder = dest
        elif isinstance(dest, str):
            dest_folder = Folder.find_by_path(self.cwd, dest)
            if dest_folder is None:
                raise ValueError(f"{dest} does not exists, and jobs cannot be renamed")
        else:
            raise TypeError(f"{dest} is neither string nor Folder")

        assert dest_folder is not None

        with database.atomic():
            Job.update(folder=dest_folder, updated_at=datetime.datetime.now()).where(
                Job.job_id << [j.job_id for j in jobs]  # type: ignore
            ).execute()

    def mv(
        self, source: Union[str, Job, Folder], dest: Union[str, Folder]
    ) -> List[Union[Job, Folder]]:
        # source might be: a job or a folder
        if isinstance(source, Folder):
            self._mv_folder(source, dest)
            return [source]
        elif isinstance(source, str):
            source_folder = Folder.find_by_path(self.cwd, source)
            if source_folder is not None:
                self._mv_folder(source_folder, dest)
                return [source_folder]
            else:
                # either job(s) or possibly a list of folders (*)
                jobs: List[Job] = []
                folders: List[Folder] = []

                try:
                    jobs = self.get_jobs(source)
                except ValueError:
                    pass

                try:
                    folders = self.get_folders(source)
                except ValueError:
                    pass

                if len(folders) == 0 and len(jobs) == 0:
                    raise DoesNotExist(f"No such folder or job: {source}")

                self._mv_jobs(jobs, dest)
                self._mv_folders(folders, dest)

                return list(folders) + list(jobs)  # type: ignore

        elif isinstance(source, Job):
            # is a job
            self._mv_jobs([source], dest)
            return [source]
        else:
            raise TypeError(f"{source} is not a Folder or a Job")

    def mkdir(self, path: str) -> None:
        head, tail = os.path.split(path)

        location = Folder.find_by_path(self.cwd, head)
        if location is None:
            raise CannotCreateError(f"Cannot create folder at '{path}'")
        logger.debug("Attempt to create folder named '%s' in '%s'", tail, location.path)

        Folder.create(name=tail, parent=location)

    def rm(
        self,
        name: Union[str, Job, Folder],
        confirm: Callable[[str], bool] = lambda _: True,
    ) -> bool:
        if isinstance(name, str):
            # string name, could be both
            if name == "/":
                raise CannotRemoveRoot()

            folders: List[Folder] = []
            jobs: List[Job] = []

            try:
                folders = self.get_folders(name)
            except ValueError:
                pass
            try:
                jobs = self.get_jobs(name)
            except ValueError:
                pass

            if len(folders) == 0 and len(jobs) == 0:
                raise DoesNotExist(f"No such folder or job: {name}")

            if confirm(f"Remove {len(folders)} folders and {len(jobs)} jobs?"):
                for folder in folders:
                    folder.delete_instance(recursive=True, delete_nullable=True)
                for job in jobs:
                    job.ensure_driver_instance(self.config)
                    job.remove()
                return True

            return False
        elif isinstance(name, Job):
            job = name

            if confirm(f"Delete job {job}?"):
                # need driver instance
                job.ensure_driver_instance(self.config)
                job.driver_instance.remove(job)
                return True
            return False
        elif isinstance(name, Folder):
            folder = name
            if confirm(f"Delete folder {folder}?"):
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
                    raise ValueError(f"{name} jobspec is not understood")
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

    def get_folders(self, pattern: str) -> List[Folder]:
        head, tail = os.path.split(pattern)
        if tail == "*":
            folder: Optional[Folder] = None
            if head == "":
                folder = Folder.get_root()
            else:
                folder = Folder.find_by_path(self.cwd, head)

            if folder is None:
                raise ValueError(f"No folder {head} found")
            return folder.children
        else:
            folder = Folder.find_by_path(self.cwd, pattern)
            if folder is None:
                raise ValueError(f"No folder {pattern} found")
            return [folder]

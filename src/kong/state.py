import datetime
from fnmatch import fnmatch
import os
import re
from typing import (
    List,
    Callable,
    Any,
    Union,
    Optional,
    Tuple,
    Sequence,
    Iterable,
    Iterator,
    cast,
)

import peewee as pw
from contextlib import contextmanager

from click import style
from kong.model.job import color_dict

from .util import Progress, Spinner, exhaust
from .drivers import DriverMismatch, get_driver
from .drivers.driver_base import DriverBase
from . import config
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


class CannotRemoveIsFolder(RuntimeError):
    pass


JobSpec = Union[str, int, Job]

Confirmation = Callable[[str], bool]


def YES(_: str) -> bool:
    return True


class State:
    def __init__(self, config: config.Config, cwd: Folder) -> None:
        self.config = config
        self.cwd = cwd
        self.default_driver: DriverBase = get_driver(self.config.default_driver)(
            self.config
        )

    @contextmanager  # type: ignore
    def pushd(self, folder: Union["Folder", str]) -> Iterator[None]:
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

    def refresh_jobs(self, jobs: List[Job]) -> Sequence[Job]:
        logger.debug("Refresing %d jobs", len(jobs))
        first_job: Job = jobs[0]
        # try bulk refresh first
        first_job.ensure_driver_instance(self.config)
        driver: DriverBase = first_job.driver_instance
        try:
            logger.debug("Attempting bulk mode sync using %s", driver.__class__)
            jobs = list(driver.bulk_sync_status(jobs))
        except DriverMismatch:
            # fall back to slow mode
            logger.debug("Bulk mode sync failed, falling back to slow loop mode")
            for job in jobs:
                job.ensure_driver_instance(self.config)
                job.get_status()
        return jobs

    def ls(
        self, path: str = ".", refresh: bool = False, recursive: bool = False
    ) -> Tuple[List["Folder"], List["Job"]]:
        "List the current directory content"
        logger.debug("%s", list(self.cwd.children))
        folder = Folder.find_by_path(self.cwd, path)
        if folder is None:
            raise pw.DoesNotExist()

        jobs = folder.jobs

        if refresh and len(jobs) > 0:
            if recursive:
                jobs = list(self.refresh_jobs(folder.jobs_recursive()))
            else:
                jobs = list(self.refresh_jobs(jobs))

        return list(folder.children), jobs

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

    def mkdir(
        self, path: str, exist_ok: bool = False, create_parent: bool = False
    ) -> Optional[Folder]:
        logger.debug("mkdir %s", path)
        found_folder = Folder.find_by_path(self.cwd, path)
        if found_folder is not None:
            if not exist_ok:
                raise CannotCreateError(f"Cannot create folder at {path}")
            else:
                return found_folder

        location: Optional[Folder] = None

        head, tail = os.path.split(path)

        if create_parent and head != "":

            def create(p: str) -> Folder:
                head, tail = os.path.split(p)
                loc = Folder.find_by_path(self.cwd, head)
                if loc is None:
                    loc = create(head)
                subf = loc.subfolder(tail)
                if subf is not None:
                    return subf
                return Folder.create(name=tail, parent=loc)

            location = create(head)
        else:
            location = Folder.find_by_path(self.cwd, head)

        if location is None:
            raise CannotCreateError(f"Cannot create folder at '{path}'")

        logger.debug("Attempt to create folder named '%s' in '%s'", tail, location.path)

        return Folder.create(name=tail, parent=location)

    def rm(
        self,
        name: Union[str, Job, Folder],
        recursive: bool = False,
        confirm: Confirmation = lambda _: True,
    ) -> bool:
        jobs: List[Job] = []
        if isinstance(name, str):
            # string name, could be both
            if name == "/":
                raise CannotRemoveRoot()

            folders: List[Folder] = []

            try:
                folders = self.get_folders(name)
                if len(folders) > 0 and not recursive:
                    raise CannotRemoveIsFolder(
                        f"{name} matches {len(folders)} folder(s). Use recursive to delete"
                    )
                for folder in folders:
                    jobs += folder.jobs_recursive()
            except ValueError:
                pass
            try:
                jobs += self.get_jobs(name)
            except ValueError:
                pass

            if len(folders) == 0 and len(jobs) == 0:
                raise DoesNotExist(f"No such folder or job: {name}")

            if confirm(f"Remove {len(folders)} folders and {len(jobs)} jobs?"):

                if len(jobs) > 0:
                    first_job = jobs[0]
                    first_job.ensure_driver_instance(self.config)
                    driver = first_job.driver_instance

                    driver.bulk_remove(jobs)

                for folder in folders:
                    folder.delete_instance(recursive=True, delete_nullable=True)

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
            # jobs: List[Job] = []
            if not recursive:
                raise CannotRemoveIsFolder(f"Cannot remove {folder} non-recursively")
            jobs = folder.jobs_recursive()
            if confirm(f"Delete folder {folder.path} and {len(jobs)} jobs?"):
                if len(jobs) > 0:
                    first_job = jobs[0]
                    first_job.ensure_driver_instance(self.config)
                    driver = first_job.driver_instance

                    driver.bulk_remove(jobs)

                folder.delete_instance(recursive=True, delete_nullable=True)
                return True
            return False
        else:
            raise TypeError("Invalid rm target type given")

    def create_job(self, *args: Any, **kwargs: Any) -> Job:
        if "folder" in kwargs:
            raise ValueError("To submit to explicit folder, use driver directly")
        if "driver" in kwargs:
            raise ValueError("To submit with explicit driver, use it directly")
        kwargs["folder"] = self.cwd
        return self.default_driver.create_job(*args, **kwargs)

    def _extract_jobs(self, name: JobSpec, recursive: bool = False) -> List[Job]:
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

                r_m = re.match(r"(\d+)\.\.(\d+)", tail)

                if tail.isdigit():
                    # single job id, just get that
                    j = Job.get_or_none(int(tail))
                    if j is None:
                        raise DoesNotExist(f"Did not find job with path {name}")
                    jobs = [j]
                elif "*" in tail:
                    # "glob" jobs: get folder, and select all jobs
                    # this is half-recursive right now
                    # folder = Folder.find_by_path(self.cwd, head)
                    folders = self.get_folders(head)
                    jobs = sum([list(f.jobs) for f in folders], [])
                elif r_m is not None:
                    start = int(r_m.group(1))
                    end = int(r_m.group(2))

                    if start > end:
                        raise ValueError(f"Illegal job range: {tail}")

                    folder = Folder.find_by_path(self.cwd, head)
                    assert folder is not None

                    jobs = []
                    for job in folder.jobs:
                        if job.job_id < start or job.job_id > end:
                            continue
                        jobs.append(job)
                    return jobs
                else:
                    folder = Folder.find_by_path(self.cwd, name)
                    if not recursive or folder is None:
                        raise ValueError(f"{name} jobspec is not understood")

                    jobs = folder.jobs_recursive()
        elif isinstance(name, Job):
            jobs = [name]
        else:
            raise TypeError("Name is neither job id, path to job(s) nor job instance")

        return jobs

    def submit_job(
        self, name: JobSpec, confirm: Confirmation = YES, recursive: bool = False
    ) -> None:

        jobs: List[Job]
        if recursive and isinstance(name, str):
            # get folders, extract jobs from those
            folders = self.get_folders(name)
            logger.debug("Recursive, found %s folders", len(folders))
            jobs = sum([f.jobs_recursive() for f in folders], [])
        else:
            jobs = self._extract_jobs(name)

        if not confirm(f"Submit {len(jobs)} jobs?"):
            return

        assert len(jobs) > 0
        first_job = jobs[0]
        first_job.ensure_driver_instance(self.config)
        driver = first_job.driver_instance

        def job_iter() -> Iterable[Job]:
            job: Job
            for job in Progress(jobs, desc="Submitting jobs"):
                yield job

        driver.bulk_submit(job_iter())

    def kill_job(
        self, name: JobSpec, recursive: bool = False, confirm: Confirmation = YES
    ) -> None:
        jobs: List[Job]
        if recursive and isinstance(name, str):
            # get folders, extract jobs from thos
            folders = self.get_folders(name)
            logger.debug("Recursive, found %s folders", len(folders))
            jobs = sum([f.jobs_recursive() for f in folders], [])
        else:
            jobs = self._extract_jobs(name)

        if not confirm(f"Kill {len(jobs)}?"):
            return
        job: Job
        for job in Progress(jobs, desc="Killing jobs"):
            job.ensure_driver_instance(self.config)  # type: ignore
            job.kill()  # type: ignore

    def resubmit_job(
        self,
        name: JobSpec,
        confirm: Confirmation = YES,
        recursive: bool = False,
        failed_only: bool = False,
    ) -> None:
        jobs: List[Job]
        jobs = self._extract_jobs(name, recursive=recursive)

        if failed_only:
            jobs = [job for job in jobs if job.status == Job.Status.FAILED]

        if not confirm(f"Resubmit {len(jobs)} jobs?"):
            return

        assert len(jobs) > 0
        first_job = jobs[0]
        first_job.ensure_driver_instance(self.config)
        driver = first_job.driver_instance

        with Spinner(f"Preparing for resubmission for {len(jobs)} jobs"):
            jobs = list(driver.bulk_resubmit(jobs, do_submit=False))

        def job_iter() -> Iterable[Job]:
            job: Job
            for job in Progress(jobs, desc="Submitting jobs"):
                yield job

        driver.bulk_submit(job_iter())

    def get_jobs(self, name: JobSpec, recursive: bool = False) -> List[Job]:
        return self._extract_jobs(name, recursive)

    def get_folders(self, pattern: str) -> List[Folder]:
        head, tail = os.path.split(pattern)
        if "*" in tail:
            logger.debug("Pattern: %s, will glob", tail)
            folder: Optional[Folder] = None
            if head == "":
                folder = self.cwd
            else:
                folder = Folder.find_by_path(self.cwd, head)

            if folder is None:
                raise ValueError(f"No folder {head} found")

            if tail == "*":  # no need to match, just get all
                return folder.children
            else:
                folders = [f for f in folder.children if fnmatch(f.name, tail)]
                return folders
        else:
            folder = Folder.find_by_path(self.cwd, pattern)
            if folder is None:
                raise ValueError(f"No folder {pattern} found")
            return [folder]

    def wait(
        self, *args: Any, progress: bool = False, **kwargs: Any
    ) -> Optional[Iterable[List[Job]]]:
        it = self.wait_gen(*args, **kwargs)
        if progress:
            return it
        else:
            exhaust(it)
            return None

    def wait_gen(
        self,
        jobspec: JobSpec,
        recursive: bool = False,
        notify: bool = True,
        timeout: Optional[int] = None,
        poll_interval: Optional[int] = None,
    ) -> Iterable[List[Job]]:
        jobs: List[Job]
        jobs = self._extract_jobs(jobspec, recursive=recursive)
        assert len(jobs) > 0
        first_job = jobs[0]
        first_job.ensure_driver_instance(self.config)
        driver = first_job.driver_instance
        orig_jobs = jobs[:]

        try:
            with Spinner(text=f"Waiting for {len(jobs)} jobs") as s:
                for cur_jobs in cast(
                    Iterable[List[Job]],
                    driver.wait(
                        jobs,
                        timeout=timeout,
                        poll_interval=poll_interval,
                        progress=True,
                    ),
                ):
                    counts = {k: 0 for k in Job.Status}
                    for job in cur_jobs:
                        counts[job.status] += 1

                    out = [
                        style(f"{k.name[:1]}{v}", fg=color_dict[k])
                        for k, v in counts.items()
                    ]
                    s.text = f"Waiting for {len(jobs)} jobs: {', '.join(out)}"
                    yield cur_jobs

            driver.bulk_sync_status(orig_jobs)
            counts = {k: 0 for k in Job.Status}
            for job in orig_jobs:
                counts[job.status] += 1

            out = [f"{k.name[:1]}{v}" for k, v in counts.items()]

            if notify:
                self.config.notifications.notify(
                    title="kong: Job wait complete",
                    message=f"Successfully waited for {len(jobs)} job(s) to finish:\n{', '.join(out)}",
                )
        except TimeoutError:
            if notify:
                self.config.notifications.notify(
                    title="kong: Job wait timeout",
                    message=f"Timeout waiting for {len(jobs)} job(s) after {timeout}s",
                )

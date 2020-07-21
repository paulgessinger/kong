import datetime
from concurrent.futures import ThreadPoolExecutor

import humanfriendly
from datetime import timedelta
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

from .util import Progress, Spinner, exhaust, strip_colors
from .drivers import DriverMismatch, get_driver
from .drivers.driver_base import DriverBase
from . import config
from .db import database
from .model.folder import Folder
from .model.job import Job, color_dict
from .logger import logger


class CannotCreateError(RuntimeError):
    """
    Raised whenever something cannot be created.
    """

    pass


class CannotRemoveRoot(RuntimeError):
    """
    Raised when the root folder is attempted to be deleted.
    """

    pass


class DoesNotExist(RuntimeError):
    """
    Raised whenever a query does not resolve to an existing resource.
    """

    pass


class CannotRemoveIsFolder(RuntimeError):
    """
    Raised if non-recursive removal of a folder is requested.
    """

    pass


JobSpec = Union[str, int, Job]

Confirmation = Callable[[str], bool]


def YES(_: str) -> bool:
    return True


class State:
    """
    The state class provides a stateful interface to the kong database.
    This is modeled closely after the interactive pseudo-shell from
    :class:`kong.repl.Repl`, but is pure python. (Actually, :class:`kong.repl.Repl`
    is implemented entirely on top of :class:`kong.state.State`, with argument
    parsing and result printing)

    Many functions accept a `JobSpec` parameter. This can be one of:

    * A job id (i.e. 1234)
    * A job range in the form 1111..9999, which will select job ids within
      the range **inclusively**
    * A path to a folder (i.e. /a/b/c). If this is the case, most methods
      have a `recursive` argument to instruct collection of jobs from the
      folder and it's descendants.
    """

    def __init__(self, config: config.Config, cwd: Folder) -> None:
        """
        Initializer for the state class. Takes an instance of :class:`kong.config.Config`
        and a current working directory to start out in.

        :param config: The config to initialize with
        :param cwd: Current working directory to start in
        """
        self.config = config
        self.cwd = cwd
        self.default_driver: DriverBase = get_driver(self.config.default_driver)(
            self.config
        )

    @contextmanager  # type: ignore
    def pushd(self, folder: Union["Folder", str]) -> Iterator[None]:
        """
        Contextmanager to temporarily change the current working directory.

        :param folder: Folder instance or path string to change into
        """
        prev = self.cwd

        if isinstance(folder, Folder):
            self.cwd = folder
        elif isinstance(folder, str):
            _folder = Folder.find_by_path(folder, self.cwd)
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
        """
        Create an instance of :class:`kong.state.State`, by reading the default config, and preparing the database.
        The returned state object can be used for stateful work with the kong database.

        :return: state object
        """
        cfg = config.Config()
        logger.debug("Initialized config: %s", cfg.data)

        logger.debug(
            "Initializing database '%s' at '%s'", config.APP_NAME, config.DB_FILE
        )
        database.init(config.DB_FILE)

        # ensure database is set up
        database.connect()
        database.create_tables([Job, Folder])

        cwd = Folder.get_root()

        return cls(cfg, cwd)

    def refresh_jobs(self, jobs: List[Job]) -> Sequence[Job]:
        """
        Refresh a list of jobs and retrieve their current status.

        :param jobs: List of jobs to refresh
        :return: Updated job instances
        """

        logger.debug("Refreshing %d jobs", len(jobs))
        if len(jobs) == 0:
            return jobs

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
        """
        Lists the current directory content.

        :param path: The path to list the content for
        :param refresh: FLag to indicate whether job statuses should be refreshed
        :param recursive: Descend into the folder hierarchy to find all jobs to list
        :return: List of folders and list of jobs found
        """

        logger.debug("%s", list(self.cwd.children))
        folder = Folder.find_by_path(path, self.cwd)
        if folder is None:
            raise pw.DoesNotExist()

        jobs = folder.jobs

        if refresh and len(jobs) > 0:
            if recursive:
                jobs = list(self.refresh_jobs(list(folder.jobs_recursive())))
            else:
                jobs = list(self.refresh_jobs(jobs))

        return list(folder.children), jobs

    def cd(self, target: Union[str, Folder] = ".") -> None:
        """
        Change the current working directory.

        :param target: String path or folder instance to change into.
        """

        if isinstance(target, str):
            if target == "":
                folder = Folder.get_root()
            else:
                _folder = Folder.find_by_path(target, self.cwd)
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
            dest_folder = Folder.find_by_path(dest, self.cwd)
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
                dest_folder = Folder.find_by_path(head, self.cwd)
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
            folder = Folder.find_by_path(dest, self.cwd)
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
            dest_folder = Folder.find_by_path(dest, self.cwd)
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
        """
        :param source: The object to move, can be a path, job object or folder object
        :param dest: The object to move to. If `source` is a job, this can be a folder.
                     If `source` is a folder, and `dest` is a folder, `source` will be
                     moved *into* `dest`. If `dest` does not exist, `source` will be renamed to `dest`.
        :return: List of moved objects
        """
        # source might be: a job or a folder
        if isinstance(source, Folder):
            self._mv_folder(source, dest)
            return [source]
        elif isinstance(source, str):
            source_folder = Folder.find_by_path(source, self.cwd)
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
        """
        Make a directory at the given path

        :param path: The relative or absolute path to create
        :param exist_ok: If `True`, there will be no error if the folder already exists.
        :param create_parent: Create all parent directories of `path` if they don't exist
        :return: The folder if one was created
        """

        logger.debug("mkdir %s", path)
        found_folder = Folder.find_by_path(path, self.cwd)
        if found_folder is not None:
            if not exist_ok:
                raise CannotCreateError(f"Cannot create folder at {path}")
            else:
                return found_folder

        location: Optional[Folder] = None

        if path.startswith("/"):
            path = path[1:]

        head, tail = os.path.split(path)

        if create_parent and head != "":

            def create(p: str) -> Folder:
                head, tail = os.path.split(p)
                loc = Folder.find_by_path(head, self.cwd)
                if loc is None:
                    loc = create(head)
                subf = loc.subfolder(tail)
                if subf is not None:
                    return subf
                return Folder.create(name=tail, parent=loc)

            location = create(head)
        else:
            location = Folder.find_by_path(head, self.cwd)

        if location is None:
            raise CannotCreateError(f"Cannot create folder at '{path}'")

        logger.debug("Attempt to create folder named '%s' in '%s'", tail, location.path)

        return Folder.create(name=tail, parent=location)

    def rm(
        self,
        name: Union[str, Job, Folder],
        recursive: bool = False,
        confirm: Confirmation = lambda _: True,
        threads: Optional[int] = os.cpu_count(),
    ) -> bool:
        """
        Remove jobs or folders.

        :param name: A path, job or folder
        :param recursive: Recursively delete from `path`. Needed to remove a directory
        :param confirm: Callback to confirm. Defaults to `True`, i.e. will confirm automatically
        :return: Whether the object at `path` was removed or not.
        """

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
                    jobs += list(folder.jobs_recursive())
            except ValueError:
                pass

            try:
                jobs += list(self.get_jobs(name))
            except ValueError:
                pass

            jobs = list(set(jobs))

            if len(folders) == 0 and len(jobs) == 0:
                raise DoesNotExist(f"No such folder or job: {name}")

            if confirm(f"Remove {len(folders)} folders and {len(jobs)} jobs?"):

                if len(jobs) > 0:
                    first_job = jobs[0]
                    first_job.ensure_driver_instance(self.config)
                    driver = first_job.driver_instance

                    with ThreadPoolExecutor(threads) as ex:
                        for _ in Progress(
                            driver.bulk_cleanup(jobs, progress=True, ex=ex),
                            total=len(jobs),
                            desc="Cleaning up",
                        ):
                            pass

                    with Spinner(f"Removing {len(jobs)} jobs"):
                        driver.bulk_remove(jobs, do_cleanup=False)

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
            jobs = list(folder.jobs_recursive())
            if confirm(f"Delete folder {folder.path} and {len(jobs)} jobs?"):
                if len(jobs) > 0:
                    first_job = jobs[0]
                    first_job.ensure_driver_instance(self.config)
                    driver = first_job.driver_instance

                    with ThreadPoolExecutor(threads) as ex:
                        for _ in Progress(
                            driver.bulk_cleanup(jobs, progress=True, ex=ex),
                            total=len(jobs),
                            desc="Cleaning up",
                        ):
                            pass

                    with Spinner(f"Removing {len(jobs)} jobs"):
                        driver.bulk_remove(jobs, do_cleanup=False)

                folder.delete_instance(recursive=True, delete_nullable=True)
                return True
            return False
        else:
            raise TypeError("Invalid rm target type given")

    def create_job(self, *args: Any, **kwargs: Any) -> Job:
        """
        Create a job with the default driver. Passes through any arguments to the driver

        :param args: Positional arguments
        :param kwargs: Keyword arguments
        :return: The created Job
        """

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

                    folder = Folder.find_by_path(head, self.cwd)
                    assert folder is not None

                    jobs = []
                    for job in folder.jobs:
                        if job.job_id < start or job.job_id > end:
                            continue
                        jobs.append(job)
                    return jobs
                else:
                    folder = Folder.find_by_path(name, self.cwd)
                    if folder is None:
                        raise ValueError(f"{name} jobspec is not understood")
                    if recursive:
                        jobs = list(folder.jobs_recursive())
                    else:
                        jobs = folder.jobs
        elif isinstance(name, Job):
            jobs = [name]
        else:
            raise TypeError("Name is neither job id, path to job(s) nor job instance")

        return jobs

    def submit_job(
        self, name: JobSpec, confirm: Confirmation = YES, recursive: bool = False
    ) -> None:
        """
        Submit one or more jobs using the driver it was created with.
        This will cause it to execute.

        :param name: Path or job id
        :param confirm: Confirmation callback. Defaults to YES
        :param recursive: If `True`, will recursively select jobs for submission.
                          Required if `path` is a n actual path.
        """

        jobs: List[Job]
        if recursive and isinstance(name, str):
            # get folders, extract jobs from those
            folders = self.get_folders(name)
            logger.debug("Recursive, found %s folders", len(folders))
            jobs = sum([list(f.jobs_recursive()) for f in folders], [])
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
        """
        Terminate execution of one or more jobs.

        :param name: Path or job id
        :param confirm: Confirmation callback. Defaults to YES
        :param recursive: If `True`, will recursively select jobs for termination.
                          Required if `path` is a n actual path.
        """

        jobs: List[Job]

        if recursive and isinstance(name, str):
            # get folders, extract jobs from thos
            folders = self.get_folders(name)
            logger.debug("Recursive, found %s folders", len(folders))
            jobs = sum([list(f.jobs_recursive()) for f in folders], [])
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

        """
        Resubmit one or more jobs. This causes them to run with the same
        settings as before.

        :param name: Path or job id
        :param confirm: Confirmation callback. Defaults to YES
        :param recursive: If `True`, will recursively select jobs for resubmission.
                          Required if `path` is a n actual path.
        :param failed_only: If `True` only select jobs in the FAILED
                            state for resubmission.
        """

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
        """
        Helper method to select jobs from a path, range or instance.

        :param name: Identifies one or more jobs
        :param recursive: Select jobs recursively, i.e. follow folders and
                          collect all jobs on the way
        :return: A list with all collected jobs.
        """
        return self._extract_jobs(name, recursive)

    def get_folders(self, pattern: str) -> List[Folder]:
        """
        Helper method to select jobs from a pattern.

        :param pattern: Glob like pattern to select folders.
        :return: List of selected folders
        """

        head, tail = os.path.split(pattern)
        if "*" in tail:
            logger.debug("Pattern: %s, will glob", tail)
            folder: Optional[Folder] = None
            if head == "":
                folder = self.cwd
            else:
                folder = Folder.find_by_path(head, self.cwd)

            if folder is None:
                raise ValueError(f"No folder {head} found")

            if tail == "*":  # no need to match, just get all
                return folder.children
            else:
                folders = [f for f in folder.children if fnmatch(f.name, tail)]
                return folders
        else:
            folder = Folder.find_by_path(pattern, self.cwd)
            if folder is None:
                raise ValueError(f"No folder {pattern} found")
            return [folder]

    def wait(
        self,
        jobspecs: Sequence[JobSpec],
        recursive: bool = False,
        notify: bool = True,
        timeout: Optional[int] = None,
        poll_interval: Optional[int] = None,
        update_interval: Optional[timedelta] = None,
        progress: bool = False,
    ) -> Optional[Iterable[List[Job]]]:
        """
        Wait for completion of a number of job

        :param jobspec: Selector for jobs, string path, job instance or folder instance
        :param recursive: Select jobs recursively
        :param notify: Notify on completion of wait
        :param timeout: Error out after a certain time
        :param poll_interval: How often to poll the driver for updates
        :param update_interval: How often to send update notificationa
        :param progress: If `True`, return progress information as an iterable
        :return: An iterable if `progress` is `True`, else `None`.
        """
        it = self._wait_gen(
            jobspecs=jobspecs,
            recursive=recursive,
            notify=notify,
            timeout=timeout,
            poll_interval=poll_interval,
            update_interval=update_interval,
        )
        if progress:
            return it
        else:
            exhaust(it)
            return None

    def _wait_gen(
        self,
        jobspecs: Sequence[JobSpec],
        recursive: bool = False,
        notify: bool = True,
        timeout: Optional[int] = None,
        poll_interval: Optional[int] = None,
        update_interval: Optional[timedelta] = None,
    ) -> Iterable[List[Job]]:
        jobs: List[Job] = []
        for jobspec in jobspecs:
            jobs.extend(self._extract_jobs(jobspec, recursive=recursive))

        logger.debug("Jobs for waiting: %s", jobs)
        assert len(jobs) > 0
        first_job = jobs[0]
        first_job.ensure_driver_instance(self.config)
        driver = first_job.driver_instance
        orig_jobs = jobs[:]

        wait_start = datetime.datetime.now()
        last_update = datetime.datetime.now()

        if update_interval is not None and notify and self.config.notifications.enabled:
            print(f"Will notify every {humanfriendly.format_timespan(update_interval)}")

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

                    if notify and update_interval is not None:
                        now = datetime.datetime.now()
                        delta = now - last_update
                        if delta > update_interval:
                            prog = humanfriendly.format_timespan(now - wait_start)
                            last_update = now
                            self.config.notifications.notify(
                                title="kong: Job wait progress",
                                message=f"Progress after {prog}:\n{strip_colors(', '.join(out))}",
                            )

                    yield cur_jobs

            orig_jobs = list(driver.bulk_sync_status(orig_jobs))
            counts = {k: 0 for k in Job.Status}
            for job in orig_jobs:
                counts[job.status] += 1

            out = [f"{k.name[:1]}{v}" for k, v in counts.items()]

            print(f"Wait terminated: {out}")

            if notify:
                if counts[Job.Status.FAILED] > 0:
                    result = "FAILURE"
                else:
                    result = "COMPLETED"
                self.config.notifications.notify(
                    title=f"kong: Job wait {result}",
                    message=f"Successfully waited for {len(jobs)} job(s) to finish:\n{', '.join(out)}",
                )
        except TimeoutError:
            if notify:
                self.config.notifications.notify(
                    title="kong: Job wait TIMEOUT",
                    message=f"Timeout waiting for {len(jobs)} job(s) after {timeout}s",
                )

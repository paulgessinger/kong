import datetime
import tempfile
import os
from contextlib import contextmanager
from subprocess import Popen
from typing import (
    Any,
    Optional,
    IO,
    Union,
    List,
    Iterable,
    Dict,
    Collection,
    Iterator,
    cast,
)
import uuid

import psutil

from ..util import rmtree
from ..db import database
from ..model import Folder, Job
from ..logger import logger
from . import InvalidJobStatus
from .driver_base import DriverBase, checked_job

jobscript_tpl = """
#!/usr/bin/env bash

exit_status_file={exit_status_file}
stdout={stdout}
stderr={stderr}

sig_handler() {{
    exit_status=$?
    echo $exit_status > $exit_status_file
}}
trap sig_handler INT HUP TERM QUIT

export KONG_JOB_ID={internal_job_id}
export KONG_JOB_OUTPUT_DIR={output_dir}
export KONG_JOB_LOG_DIR={log_dir}
export KONG_JOB_NPROC={nproc}
export KONG_JOB_SCRATCHDIR={scratch_dir}

touch $stdout
touch $stderr

({command}) >> $stdout 2>> $stderr
echo $? > {exit_status_file}

""".strip()


class LocalDriver(DriverBase):
    def create_job(
        self, folder: Folder, command: str, cores: int = 1, *args: Any, **kwargs: Any
    ) -> Job:
        assert len(args) == 0 and len(kwargs) == 0, "No extra arguments allowed"
        batch_job_id = str(uuid.uuid1())

        job: Job = Job.create(
            folder=folder,
            batch_job_id=batch_job_id,
            command=command,
            driver=self.__class__,
            cores=cores,
        )

        # in job dir, create output dir
        output_dir = os.path.abspath(
            os.path.join(self.config.joboutputdir, f"{job.job_id:>06d}")
        )
        os.makedirs(output_dir, exist_ok=True)

        log_dir = os.path.abspath(
            os.path.join(self.config.jobdir, f"{job.job_id:>06d}")
        )
        os.makedirs(log_dir, exist_ok=True)

        stdout = os.path.abspath(os.path.join(log_dir, "stdout.txt"))
        stderr = os.path.abspath(os.path.join(log_dir, "stderr.txt"))
        exit_status_file = os.path.abspath(os.path.join(log_dir, "exit_status.txt"))

        scriptpath = os.path.join(log_dir, "jobscript.sh")

        scratch_dir = tempfile.mkdtemp(prefix=f"kong_job_{job.job_id}")

        job.data = dict(
            stdout=stdout,
            stderr=stderr,
            exit_status_file=exit_status_file,
            jobscript=scriptpath,
            output_dir=output_dir,
            log_dir=log_dir,
            scratch_dir=scratch_dir,
        )
        job.save()

        values = dict(
            command=command,
            stdout=stdout,
            stderr=stderr,
            internal_job_id=job.job_id,
            output_dir=output_dir,
            log_dir=log_dir,
            exit_status_file=exit_status_file,
            nproc=cores,
            scratch_dir=scratch_dir,
        )
        logger.debug("Creating job with values: %s", str(values))

        jobscript = jobscript_tpl.format(**values)

        with open(scriptpath, "w") as fh:
            fh.write(jobscript)

        job._driver_instance = self
        return job

    def bulk_create_jobs(self, jobs: Iterable[Dict[str, Any]]) -> List["Job"]:
        # right now, implemented as loop, potential to optimize
        return [self.create_job(**kwargs) for kwargs in jobs]

    def cleanup(self, job: Job) -> Job:
        if job.status not in (
            Job.Status.CREATED,
            Job.Status.FAILED,
            Job.Status.COMPLETED,
            Job.Status.UNKNOWN,
        ):
            raise InvalidJobStatus(
                f"Cannot clean up job {job} in {job.status}, please kill first"
            )

        logger.debug("Removing job output directory for job %s", job)

        for name in ["log_dir", "output_dir", "scratch_dir"]:
            path = job.data[name]
            if os.path.exists(path):
                rmtree(path)
        return job

    def bulk_cleanup(self, jobs: Collection["Job"]) -> Collection["Job"]:
        for job in jobs:
            self.cleanup(job)
        return jobs

    def remove(self, job: Job) -> None:
        logger.debug("Removing job %s", job)
        self.cleanup(job)
        job.delete_instance()

    def bulk_remove(self, jobs: Collection["Job"]) -> None:
        for job in jobs:
            self.remove(job)

    @checked_job
    def sync_status(self, job: Job, save: bool = True) -> Job:
        if job.status not in (Job.Status.RUNNING, Job.Status.SUBMITTED):
            logger.debug(
                "Job %s is neither RUNNING nor SUBMITTED (%s), so no status changes without intervention",
                job,
                job.status,
            )
            return job

        logger.debug("Job %s in status %s, checking for updates", job, job.status)
        exit_status_file = job.data["exit_status_file"]
        pid = job.data["pid"]

        def check_exit_code() -> None:
            if not os.path.exists(exit_status_file):
                logger.debug(
                    "Job %s appears to have exited, but exit status file is not present"
                )
                job.status = Job.Status.UNKNOWN
            else:
                with open(exit_status_file, "r") as fh:
                    exit_code = int(fh.read().strip())
                job.data["exit_code"] = exit_code
                if exit_code == 0:
                    logger.debug("Job %s succeeded", job)
                    job.status = Job.Status.COMPLETED
                else:
                    logger.debug("Job %s failed", job)
                    job.status = Job.Status.FAILED

        # check if it is still running
        try:
            proc = psutil.Process(pid)
            if proc.is_running():
                # is running, but is it zombie waiting to be reaped?
                if proc.status() == psutil.STATUS_ZOMBIE:
                    logger.debug(
                        "Job %s with pid %s is running but zombie, reaping", job, pid
                    )
                    proc.wait()  # reaping
                    logger.debug("Reaped pid %d", pid)
                    check_exit_code()
                else:
                    job.status = Job.Status.RUNNING
            else:
                logger.debug("Job %s is not running, exit code should be set", job)
                check_exit_code()
            if save:
                job.save()

        except psutil.NoSuchProcess:
            logger.debug("Job %s with pid %d doesn't exist, check exit code", job, pid)
            check_exit_code()
            if save:
                job.save()
        return job

    def bulk_sync_status(self, jobs: Collection[Job]) -> Iterable[Job]:
        # simply implemented as loop over single sync status for local driver
        now = datetime.datetime.now()

        def sync() -> Iterable[Job]:
            for job in jobs:
                self.sync_status(job, save=False)
                job.updated_at = now
                yield job

        with database.atomic():
            Job.bulk_update(
                sync(), fields=[Job.status, Job.updated_at], batch_size=self.batch_size
            )

        return jobs

    @checked_job
    def kill(self, job: Job, save: bool = True) -> None:
        self.sync_status(job)
        if job.status == Job.Status.CREATED:
            logger.debug("Job %s in %s, simply setting to failed", job, job.status)
            job.status = Job.Status.FAILED
        elif job.status in (Job.Status.RUNNING, Job.Status.SUBMITTED):
            logger.debug(
                "Job %s in %s, killing pid %d", job, job.status, job.data["pid"]
            )
            proc = psutil.Process(job.data["pid"])
            proc.kill()
            proc.wait()
            job.status = Job.Status.FAILED
        else:
            logger.debug("Job %s in %s, do nothing")
        if save:
            job.save()

    def bulk_kill(self, jobs: Collection["Job"]) -> Iterable[Job]:
        now = datetime.datetime.now()

        def k() -> Iterable[Job]:
            for job in jobs:
                self.kill(job, save=False)
                job.updated_at = now
                yield job

        with database.atomic():
            Job.bulk_update(
                k(), fields=[Job.status, Job.updated_at], batch_size=self.batch_size
            )

        return jobs

    def bulk_submit(self, jobs: Iterable["Job"]) -> None:
        now = datetime.datetime.now()

        def sub() -> Iterable[Job]:
            for job in jobs:
                self.submit(job, save=False)
                job.updated_at = now
                yield job

        with database.atomic():
            Job.bulk_update(
                sub(),
                fields=[Job.status, Job.data, Job.updated_at],
                batch_size=self.batch_size,
            )

    @checked_job
    def submit(self, job: Job, save: bool = True) -> None:
        self.sync_status(job)
        if job.status > Job.Status.CREATED:
            raise InvalidJobStatus(f"Cannot submit job in state {job.status}")

        cmd = ["/usr/bin/env", "bash", job.data["jobscript"]]
        logger.debug("About to submit job with command: %s", str(cmd))

        proc = Popen(cmd, stdin=None, stdout=None, stderr=None, close_fds=True)

        job.data["pid"] = proc.pid
        job.status = Job.Status.SUBMITTED
        if save:
            job.save()
        logger.debug("Submitted job as %s", job)

    @checked_job  # type: ignore
    @contextmanager  # type: ignore
    def stdout(self, job: Job) -> Iterator[IO[str]]:
        self.sync_status(job)
        if job.status not in (Job.Status.FAILED, Job.Status.COMPLETED):
            raise InvalidJobStatus("Cannot get stdout for job in status %s", job.status)

        with open(job.data["stdout"], "r") as fh:
            yield fh

    @checked_job  # type: ignore
    @contextmanager  # type: ignore
    def stderr(self, job: Job) -> Iterator[IO[str]]:
        self.sync_status(job)
        if job.status not in (Job.Status.FAILED, Job.Status.COMPLETED):
            raise InvalidJobStatus("Cannot get stdout for job in status %s", job.status)

        with open(job.data["stderr"], "r") as fh:
            yield fh

    @checked_job
    def _wait_single(self, job: Job, timeout: Optional[int] = None) -> Job:
        logger.debug("Wait for job %s requested", job)
        self.sync_status(job)
        if job.status not in (Job.Status.SUBMITTED, Job.Status.RUNNING):
            logger.info(
                "Job %s is in status %s, neither SUBMITTED nor RUNNING, wait will not complete, returning now",
                job,
                job.status,
            )
            return job

        proc = psutil.Process(pid=job.data["pid"])
        try:
            proc.wait(timeout=timeout)
        except psutil.TimeoutExpired as e:
            raise TimeoutError(str(e))
        return cast(Job, self.sync_status(job))

    def wait_gen(
        self,
        jobs: Union[Job, List[Job]],
        poll_interval: Optional[int] = None,
        timeout: Optional[int] = None,
    ) -> Iterable[List[Job]]:
        if not isinstance(jobs, list):
            jobs = [jobs]

        logger.debug("Waiting for %s jobs", len(jobs))
        for job in jobs:
            self._wait_single(job, timeout=timeout)

        yield list(self.bulk_sync_status(jobs))

    @checked_job
    def resubmit(self, job: Job) -> Job:
        self.sync_status(job)
        if job.status not in (
            Job.Status.COMPLETED,
            Job.Status.FAILED,
            Job.Status.UNKNOWN,
        ):
            logger.error("Will not resubmit job %s in status %s", job, job.status)
            raise InvalidJobStatus(
                f"Will not resubmit job {job} in status {job.status}"
            )

        self.kill(job)

        # need to make sure the output artifacts are gone, since we're reusing the same job dir
        for name in ["exit_status_file", "stdout", "stderr"]:
            path = job.data[name]
            if os.path.exists(path):
                logger.debug("Removing %s", path)
                os.remove(path)
            assert not os.path.exists(path)

        for d in ["scratch_dir", "output_dir"]:
            path = job.data[d]
            if os.path.exists(path):
                logger.debug("Removing %s", path)
                rmtree(path)
                os.makedirs(path)

        job.status = Job.Status.CREATED
        job.save()
        self.submit(job)
        return job

    def bulk_resubmit(
        self, jobs: Collection["Job"], do_submit: bool = True
    ) -> Iterable["Job"]:
        for job in jobs:
            self.sync_status(job)
            if job.status not in (
                Job.Status.COMPLETED,
                Job.Status.FAILED,
                Job.Status.UNKNOWN,
            ):
                logger.error("Will not resubmit job %s in status %s", job, job.status)
                raise InvalidJobStatus(
                    f"Will not resubmit job {job} in status {job.status}"
                )

            self.kill(job)

            # need to make sure the output artifacts are gone, since we're reusing the same job dir
            for name in ["exit_status_file", "stdout", "stderr"]:
                path = job.data[name]
                if os.path.exists(path):
                    logger.debug("Removing %s", path)
                    os.remove(path)
                assert not os.path.exists(path)

            for d in ["scratch_dir", "output_dir"]:
                path = job.data[d]
                if os.path.exists(path):
                    logger.debug("Removing %s", path)
                    rmtree(path)
                    os.makedirs(path)

            job.status = Job.Status.CREATED
            job.save()
            if do_submit:
                self.submit(job)
        return jobs

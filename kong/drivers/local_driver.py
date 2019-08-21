import functools
import shutil
import tempfile
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime, timedelta
import os
import time
from contextlib import contextmanager, _GeneratorContextManager
from shutil import rmtree
from subprocess import Popen
from typing import (
    Any,
    Optional,
    IO,
    Iterator,
    ContextManager,
    Union,
    List,
    Generator,
    Iterable,
)
import uuid

import psutil

from ..model import Folder, Job
from ..logger import logger
from ..config import Config
from . import DriverMismatch, InvalidJobStatus
from .driver_base import DriverBase

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


def checked_job(f: Any) -> Any:
    @functools.wraps(f)
    def wrapper(self: Any, job: Job, *args: Any, **kwargs: Any) -> Any:
        self._check_driver(job)
        return f(self, job, *args, **kwargs)

    return wrapper


class LocalDriver(DriverBase):
    config: Config

    def __init__(self, config: Optional[Config]) -> None:
        if config is None:
            logger.debug("Attempt to default-construct configuration object")
            self.config = Config()
        else:
            logger.debug("Taking explicit confit")
            self.config = config

        logger.debug("Opening jobdir filesystem at %s", self.config.jobdir)
        assert os.path.exists(self.config.jobdir)

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
            os.path.join(self.config.joboutputdir, f"{job.job_id}")
        )
        os.makedirs(output_dir)

        log_dir = os.path.abspath(os.path.join(self.config.jobdir, f"{job.job_id}"))
        os.makedirs(log_dir)

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

    def cleanup(self, job: Job) -> None:
        assert job.status in (
            Job.Status.CREATED,
            Job.Status.FAILED,
            Job.Status.COMPLETED,
            Job.Status.UNKOWN,
        ), f"Cannot clean up job {job} in {job.status}, please kill first"

        logger.debug("Removing job output directory for job %s", job)

        for name in ["log_dir", "output_dir", "scratch_dir"]:
            path = job.data[name]
            if os.path.exists(path):
                shutil.rmtree(path)

    def _check_driver(self, job: Job) -> None:
        # check if we're the right driver for this
        if not isinstance(self, job.driver):
            raise DriverMismatch(
                f"Job {job} is has driver {job.driver}, not {self.__class__}"
            )

    @checked_job
    def sync_status(self, job: Job) -> None:
        if job.status not in (Job.Status.RUNNING, Job.Status.SUBMITTED):
            logger.debug(
                "Job %s is neither RUNNING nor SUBMITTED (%s), so no status changes without intervention",
                job,
                job.status,
            )
            return

        logger.debug("Job %s in status %s, checking for updates", job, job.status)
        exit_status_file = job.data["exit_status_file"]
        pid = job.data["pid"]

        def check_exit_code() -> None:
            if not os.path.exists(exit_status_file):
                logger.debug(
                    "Job %s appears to have exited, but exit status file is not present"
                )
                job.status = Job.Status.UNKOWN
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
                job.save()
            else:
                logger.debug("Job %s is not running, exit code should be set", job)
                check_exit_code()
                job.save()

        except psutil.NoSuchProcess:
            logger.debug("Job %s with pid %d doesn't exist, check exit code", job, pid)
            check_exit_code()
            job.save()

    def bulk_sync_status(self, jobs: Iterable[Job]) -> None:
        # simply implemented as loop over single sync status for local driver
        for job in jobs:
            self.sync_status(job)

    @checked_job
    def kill(self, job: Job) -> None:
        self.sync_status(job)
        if job.status == Job.Status.CREATED:
            logger.debug("Job %s in %s, simply setting to failed", job, job.status)
            job.status = Job.Status.FAILED
            job.save()
        elif job.status in (Job.Status.RUNNING, Job.Status.SUBMITTED):
            logger.debug(
                "Job %s in %s, killing pid %d", job, job.status, job.data["pid"]
            )
            proc = psutil.Process(job.data["pid"])
            proc.kill()
            proc.wait()
            job.status = Job.Status.FAILED
            job.save()
        else:
            logger.debug("Job %s in %s, do nothing")

    @checked_job
    def submit(self, job: Job) -> None:
        self.sync_status(job)
        if job.status > Job.Status.CREATED:
            raise InvalidJobStatus(f"Cannot submit job in state {job.status}")

        # need to make sure the output artifacts are gone, since we're reusing the same job dir
        output_dir = job.data["output_dir"]
        stdout = job.data["stdout"]
        stderr = job.data["stderr"]
        exit_status_file = job.data["exit_status_file"]
        if os.path.exists(output_dir):
            logger.debug("Removing %s", output_dir)
            rmtree(output_dir)
        for path in [stdout, stderr, exit_status_file]:
            if os.path.exists(path):
                logger.debug("Removing %s", path)
                os.remove(path)

        cmd = ["/usr/bin/env", "bash", job.data["jobscript"]]
        logger.debug("About to submit job with command: %s", str(cmd))

        proc = Popen(cmd, stdin=None, stdout=None, stderr=None, close_fds=True)

        job.data["pid"] = proc.pid
        job.status = Job.Status.SUBMITTED
        job.save()
        logger.debug("Submitted job as %s", job)

    @checked_job  # type: ignore
    @contextmanager  # type: ignore
    def stdout(self, job: Job) -> ContextManager[IO[str]]:
        self.sync_status(job)
        if job.status not in (Job.Status.FAILED, Job.Status.COMPLETED):
            raise InvalidJobStatus("Cannot get stdout for job in status %s", job.status)

        with open(job.data["stdout"], "r") as fh:
            yield fh

    @checked_job  # type: ignore
    @contextmanager  # type: ignore
    def stderr(self, job: Job) -> ContextManager[IO[str]]:
        self.sync_status(job)
        if job.status not in (Job.Status.FAILED, Job.Status.COMPLETED):
            raise InvalidJobStatus("Cannot get stdout for job in status %s", job.status)

        with open(job.data["stderr"], "r") as fh:
            yield fh

    @checked_job
    def _wait_single(self, job: Job, timeout: Optional[int] = None) -> None:
        logger.debug("Wait for job %s requested", job)
        self.sync_status(job)
        if job.status not in (Job.Status.SUBMITTED, Job.Status.RUNNING):
            logger.info(
                "Job %s is in status %s, neither SUBMITTED nor RUNNING, wait will not complete, returning now",
                job,
                job.status,
            )
            return

        proc = psutil.Process(pid=job.data["pid"])
        try:
            proc.wait(timeout=timeout)
        except psutil.TimeoutExpired as e:
            raise TimeoutError(str(e))
        self.sync_status(job)

    def wait(self, jobs: Union[Job, List[Job]], timeout: Optional[int] = None) -> None:
        if not isinstance(jobs, list):
            jobs = [jobs]

        logger.debug("Waiting for %s jobs", len(jobs))
        for job in jobs:
            self._wait_single(job, timeout=timeout)

    @checked_job
    def resubmit(self, job: Job) -> None:
        self.sync_status(job)
        if job.status not in (
            Job.Status.COMPLETED,
            Job.Status.FAILED,
            Job.Status.UNKOWN,
        ):
            logger.error("Will not resubmit job %s in status %s", job, job.status)
            raise InvalidJobStatus(
                f"Will not resubmit job {job} in status {job.status}"
            )

        self.kill(job)

        for name in ["exit_status_file", "stdout", "stderr"]:
            path = job.data[name]
            if os.path.exists(path):
                logger.debug("Removing %s", path)
                os.remove(path)
            assert not os.path.exists(path)

        scratch_dir = job.data["scratch_dir"]
        if os.path.exists(scratch_dir):
            logger.debug("Removing %s", scratch_dir)
            shutil.rmtree(scratch_dir)
            os.makedirs(scratch_dir)

        job.status = Job.Status.CREATED
        job.save()
        self.submit(job)

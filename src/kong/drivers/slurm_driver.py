import datetime
import time
import os
import re
from abc import ABC, abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, timedelta
from typing import (
    Any,
    Iterator,
    Iterable,
    Optional,
    ContextManager,
    Union,
    List,
    Dict,
    IO,
    Sequence,
    cast,
    Collection,
)

import sh
from jinja2 import Environment, DictLoader

from ..drivers import InvalidJobStatus
from ..logger import logger
from ..config import Config, slurm_schema
from ..db import database
from ..model import Job, Folder
from ..util import make_executable, rmtree, format_timedelta, parse_timedelta, chunks
from .driver_base import DriverBase, checked_job


class SlurmAccountingItem:
    job_id: int
    status: Job.Status
    exit_code: int

    def __init__(self, job_id: int, status: Job.Status, exit_code: int):
        self.job_id = job_id
        self.status = status
        self.exit_code = exit_code

    @classmethod
    def from_parts(
        cls, job_id: str, status_str: str, exit: str
    ) -> "SlurmAccountingItem":
        exit_code, _ = exit.split(":", 1)

        if status_str == "PENDING":
            status = Job.Status.SUBMITTED
        elif status_str == "COMPLETED":
            status = Job.Status.COMPLETED
        elif status_str == "FAILED" or status_str.startswith("CANCELLED"):
            status = Job.Status.FAILED
        elif status_str == "RUNNING":
            status = Job.Status.RUNNING
        else:
            status = Job.Status.UNKNOWN

        return cls(int(job_id), status, int(exit_code))

    def __repr__(self) -> str:
        return f"SAI<{self.job_id}, {self.status}, {self.exit_code}>"

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, self.__class__)
        return (
            self.job_id == other.job_id
            and self.status == other.status
            and self.exit_code == other.exit_code
        )


class SlurmInterface(ABC):
    @abstractmethod
    def sacct(self, jobs: Collection["Job"]) -> Iterator[SlurmAccountingItem]:
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def sbatch(self, job: "Job") -> int:
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def scancel(self, job: "Job") -> None:
        raise NotImplementedError()  # pragma: no cover


class ShellSlurmInterface(SlurmInterface):
    _sacct: Optional[sh.Command] = None
    _sbatch: Optional[sh.Command] = None
    _scancel: Optional[sh.Command] = None

    subreg = re.compile(r".* (\d*)$")

    def __init__(self) -> None:  # pragma: no cover
        self._sacct = sh.Command("sacct")
        self._sbatch = sh.Command("sbatch")
        self._scancel = sh.Command("scancel")

    def sacct(self, jobs: Collection["Job"]) -> Iterator[SlurmAccountingItem]:

        logger.debug("Getting job info for %d jobs", len(jobs))
        starttime = date.today() - timedelta(days=7)

        args = dict(
            brief=True, noheader=True, parsable2=True, starttime=starttime, _iter=True
        )

        if len(jobs) > 0:
            if all(j.batch_job_id is None for j in jobs):
                logger.debug("no jobs given that are known to the scheduder")
                return []

            job_ids = ",".join(
                [str(j.batch_job_id) for j in jobs if j.batch_job_id is not None]
            )
            args["jobs"] = job_ids

            logger.debug("Job argument: %s", job_ids)

        assert self._sacct is not None
        for line in self._sacct(**args):
            job_id, status, exit = line.split("|", 3)
            if not job_id.isdigit():
                continue
            yield SlurmAccountingItem.from_parts(job_id, status, exit)

    def sbatch(self, job: Job) -> int:
        assert self._sbatch is not None
        raw = self._sbatch(job.data["batchfile"])
        logger.debug("sbatch: %s", raw)
        m = self.subreg.match(str(raw))
        assert m is not None
        return int(m.group(1))

    def scancel(self, job: Job) -> None:
        assert self._scancel is not None
        res = self._scancel(job.batch_job_id)
        logger.debug("scancel: %s", res)


jobscript_tpl_str = """
#!/usr/bin/env bash

export KONG_JOB_ID={{internal_job_id}}
export KONG_JOB_OUTPUT_DIR={{output_dir}}
export KONG_JOB_LOG_DIR={{log_dir}}
export KONG_JOB_NPROC={{cores}}
export KONG_JOB_SCRATCHDIR=/localscratch/${{ "{" }}SLURM_JOB_ID{{ "}" }}/

mkdir -p $KONG_JOB_SCRATCHDIR

stdout={{stdout}}

({{command}}) > $stdout 2>&1
""".strip()

batchfile_tpl_str = """
#!/bin/bash
#SBATCH -J {{name}}
#SBATCH -o {{slurm_out}}
#SBATCH -p {{queue}}
              
#SBATCH -n {{ntasks}}
#SBATCH -N {{nnodes}}
#SBATCH -c {{cores}}
#SBATCH --mem-per-cpu {{memory}}M 
#SBATCH -t {{walltime}}
{%- if licenses is not none %}
#SBATCH -L {{licenses}}
{% endif %}

#SBATCH -A {{account}}

srun --export=NONE {{jobscript}}
""".strip()  # noqa: W291, W293

env = Environment(
    loader=DictLoader({"batchfile": batchfile_tpl_str, "jobscript": jobscript_tpl_str})
)

batchfile_tpl = env.get_template("batchfile")
jobscript_tpl = env.get_template("jobscript")


class SlurmDriver(DriverBase):
    slurm: SlurmInterface

    def __init__(self, config: Config, slurm: Optional[SlurmInterface] = None):
        self.slurm = slurm or ShellSlurmInterface()
        super().__init__(config)
        assert "slurm_driver" in self.config.data
        self.slurm_config = slurm_schema.validate(self.config.slurm_driver)

    def create_job(
        self,
        folder: "Folder",
        command: str,
        cores: int = 1,
        memory: int = 1000,
        nnodes: int = 1,
        ntasks: int = 1,
        queue: Optional[str] = None,
        name: Optional[str] = None,
        walltime: Union[timedelta, str] = timedelta(minutes=30),
        licenses: Optional[str] = None,
    ) -> "Job":

        if queue is None:
            queue = self.slurm_config["default_queue"]

        job: Job = Job.create(
            folder=folder,
            batch_job_id=None,  # don't have one until submission
            command=command,
            driver=self.__class__,
            cores=cores,
            memory=memory,
        )

        if name is None:
            name = f"kong_job_{job.job_id}"

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
        slurm_out = os.path.abspath(os.path.join(log_dir, "slurm_out.txt"))

        batchfile = os.path.join(log_dir, "batchfile.sh")
        jobscript = os.path.join(log_dir, "jobscript.sh")

        if isinstance(walltime, str):
            norm_walltime = format_timedelta(parse_timedelta(walltime))
        elif isinstance(walltime, timedelta):
            norm_walltime = format_timedelta(walltime)
        else:
            raise ValueError("Walltime must be timedelta or string")

        job.data = dict(
            stdout=stdout,
            slurm_out=slurm_out,
            jobscript=jobscript,
            batchfile=batchfile,
            output_dir=output_dir,
            log_dir=log_dir,
            name=name,
            queue=queue,
            nnodes=nnodes,
            ntasks=ntasks,
            exit_code=0,
            walltime=norm_walltime,
            account=self.config.slurm_driver["account"],
            licenses=licenses,
        )
        job.save()

        values = dict(
            batchfile=batchfile,
            jobscript=jobscript,
            command=command,
            stdout=stdout,
            slurm_out=slurm_out,
            internal_job_id=job.job_id,
            log_dir=log_dir,
            output_dir=output_dir,
            cores=cores,
            nnodes=nnodes,
            ntasks=ntasks,
            memory=memory,
            account=self.config.slurm_driver["account"],
            name=name,
            queue=queue,
            walltime=norm_walltime,
            licenses=licenses,
        )

        batchfile_content = batchfile_tpl.render(**values)

        with open(batchfile, "w") as fh:
            fh.write(batchfile_content)

        jobscript_content = jobscript_tpl.render(**values)
        with open(job.data["jobscript"], "w") as fh:
            fh.write(jobscript_content)

        make_executable(job.data["jobscript"])

        job._driver_instance = self
        return job

    def bulk_create_jobs(self, jobs: Iterable[Dict[str, Any]]) -> List["Job"]:
        return [self.create_job(**kwargs) for kwargs in jobs]

    def sync_status(self, job: "Job") -> Job:
        return self.bulk_sync_status([job])[0]

    def bulk_sync_status(self, jobs: Collection["Job"]) -> Sequence["Job"]:
        logger.debug("Bulk sync status with %d jobs", len(jobs))
        for job in jobs:
            self._check_driver(job)

        now = datetime.datetime.now()

        def proc() -> Iterable[Job]:
            job_not_found = 0
            for item in self.slurm.sacct(jobs):
                job = Job.get_or_none(batch_job_id=item.job_id)
                if job is None:
                    job_not_found += 1
                    continue
                job.status = item.status
                job.data["exit_code"] = item.exit_code
                job.updated_at = now
                yield job
            if job_not_found > 0:
                logger.warning(
                    "Tried to fetch %d slurm jobs which where not found in the database",
                    job_not_found,
                )

        with database.atomic():
            Job.bulk_update(
                proc(),
                fields=[Job.data, Job.status, Job.updated_at],
                batch_size=self.batch_size,
            )
        # reload updated jobs
        ids = [j.job_id for j in jobs]
        logger.debug(
            "Going to reload %d jobs, batch size %d", len(ids), self.select_batch_size
        )

        fetched: List[Job] = list(
            Job.bulk_select(Job.job_id, ids, batch_size=self.select_batch_size)
        )
        return cast(Sequence[Job], fetched)

    @checked_job
    def kill(self, job: "Job", save: bool = True) -> None:
        if job.status in (Job.Status.CREATED, Job.Status.UNKNOWN):
            logger.debug("Job %s in %s, simply setting to failed", job, job.status)
            job.status = Job.Status.FAILED
        elif job.status in (Job.Status.RUNNING, Job.Status.SUBMITTED):
            self.slurm.scancel(job)
            job.status = Job.Status.FAILED
        else:
            logger.debug("Job %s in %s, do nothing")
        if save:
            job.save()

    def bulk_kill(self, jobs: Collection["Job"]) -> Iterable["Job"]:
        now = datetime.datetime.now()
        jobs = self.bulk_sync_status(jobs)

        def delete() -> Iterable["Job"]:
            for job in jobs:
                self.kill(job, save=False)
                job.updated_at = now
                yield job

        with database.atomic():
            Job.bulk_update(
                delete(),
                fields=[Job.status, Job.updated_at],
                batch_size=self.batch_size,
            )

        return jobs

    def wait_gen(
        self,
        job: Union["Job", List["Job"]],
        poll_interval: Optional[int] = None,
        timeout: Optional[int] = None,
    ) -> Iterable[List[Job]]:
        start = datetime.datetime.now()
        poll_interval = poll_interval or 30

        jobs: List[Job]
        if isinstance(job, Job):
            jobs = [job]
        elif isinstance(job, list):
            jobs = job
        else:
            raise TypeError("Argument is neither job nor list of jobs")

        # pre-check for status
        for job in jobs:
            if job.status == Job.Status.CREATED:
                raise ValueError(f"Job is in status {job.status}, cannot wait")

        logger.debug("Begin waiting for %d jobs", len(jobs))

        while True:
            now = datetime.datetime.now()
            delta: timedelta = now - start
            if timeout is not None:
                if delta.total_seconds() > timeout:
                    raise TimeoutError()

            logger.debug("Refreshing %d", len(jobs))
            jobs = list(self.bulk_sync_status(jobs))  # overwrite with updated
            # filter out all that are considered waitable
            remaining_jobs = [
                j
                for j in jobs
                if j.status
                not in (Job.Status.COMPLETED, Job.Status.FAILED, Job.Status.UNKNOWN)
            ]
            if len(remaining_jobs) == 0:
                logger.debug("Waiting completed")
                break
            yield jobs
            logger.debug(
                "Waiting. Elapsed time: %s, %d jobs remaining",
                delta,
                len(remaining_jobs),
            )
            time.sleep(poll_interval)

    @checked_job
    def submit(self, job: "Job", save: bool = True) -> None:
        if job.status != Job.Status.CREATED:
            raise InvalidJobStatus(f"Cannot submit job {job} in status {job.status}")
        job.batch_job_id = str(self.slurm.sbatch(job))
        job.status = Job.Status.SUBMITTED

        if save:
            job.save()

    def bulk_submit(self, jobs: Iterable["Job"]) -> None:
        now = datetime.datetime.now()

        def sub() -> Iterable[Job]:
            for job in jobs:
                assert job.driver == self.__class__, "Not valid for different driver"
                self.submit(job, save=False)
                job.updated_at = now
                yield job

        with database.atomic():
            Job.bulk_update(
                sub(),
                fields=[Job.batch_job_id, Job.status, Job.updated_at],
                batch_size=self.batch_size,
            )

    @checked_job  # type: ignore
    @contextmanager  # type: ignore
    def stdout(self, job: Job) -> Iterator[IO[str]]:
        with open(job.data["stdout"], "r") as fh:
            yield fh

    def stderr(self, job: "Job") -> ContextManager[None]:
        raise NotImplementedError("Stderr goes to stdout in slurm")

    def resubmit(self, job: "Job") -> "Job":
        logger.debug("Resubmit job %s", job)
        job = self.sync_status(job)
        if job.status not in (
            Job.Status.FAILED,
            Job.Status.COMPLETED,
            Job.Status.UNKNOWN,
        ):
            raise InvalidJobStatus(f"Job {job} not in valid status for resubmit")

        try:
            self.kill(job)  # attempt to kill
        except Exception:
            pass

        # need to make sure the output artifacts are gone, since we're reusing the same job dir
        for name in ["stdout"]:
            path = job.data[name]
            if os.path.exists(path):
                logger.debug("Removing %s", path)
                os.remove(path)
            assert not os.path.exists(path)

        for d in ["output_dir"]:
            path = job.data[d]
            if os.path.exists(path):
                logger.debug("Removing %s", path)
                rmtree(path)
                os.makedirs(path)

        # reset to created
        job.status = Job.Status.CREATED
        job.save()

        self.submit(job)  # this will reset the status
        return job

    def bulk_resubmit(
        self, jobs: Iterable["Job"], do_submit: bool = True
    ) -> Iterable["Job"]:

        logger.debug("Resubmitting jobs")

        jobs = self.bulk_sync_status(list(jobs))
        # check status is ok
        for job in jobs:
            if job.status not in (
                Job.Status.FAILED,
                Job.Status.COMPLETED,
                Job.Status.UNKNOWN,
            ):
                raise InvalidJobStatus(f"Job {job} not in valid status for resubmit")

        try:
            jobs = self.bulk_kill(jobs)  # attempt to kill
        except Exception:
            pass

        def clean(job: Job) -> Job:
            # need to make sure the output artifacts are gone, since we're reusing the same job dir
            for name in ["stdout"]:
                path = job.data[name]
                if os.path.exists(path):
                    logger.debug("Removing %s", path)
                    os.remove(path)
                assert not os.path.exists(path)

            for d in ["output_dir"]:
                path = job.data[d]
                if os.path.exists(path):
                    logger.debug("Removing %s", path)
                    rmtree(path)
                    os.makedirs(path)

            return job

        nthreads = 40
        logger.debug("Cleaning up on %d threads", nthreads)
        with ThreadPoolExecutor(nthreads) as ex:
            jobs = list(ex.map(clean, jobs))

        # update status
        with database.atomic():
            Job.update(
                status=Job.Status.CREATED, updated_at=datetime.datetime.now()
            ).execute()

        # jobs = Job.select().where(
        #     Job.job_id.in_([j.job_id for j in jobs])  # type: ignore
        # )
        jobs = Job.bulk_select(
            Job.job_id, [j.job_id for j in jobs], batch_size=self.select_batch_size
        )
        if do_submit:
            self.bulk_submit(jobs)

        return jobs

    def cleanup(self, job: "Job") -> "Job":
        job = self.sync_status(job)
        if job.status in (Job.Status.SUBMITTED, Job.Status.RUNNING):
            raise InvalidJobStatus(f"Job {job} might be running, please kill first")

        logger.debug("Cleanup job %s", job)
        for d in ["log_dir", "output_dir"]:
            path = job.data[d]
            if os.path.exists(path):
                logger.debug("Path %s exists, attempting to delete", path)
                rmtree(path)
        return job

    def bulk_cleanup(self, jobs: Collection["Job"]) -> List["Job"]:
        jobs = self.bulk_sync_status(jobs)
        # safety check
        for job in jobs:
            assert job.driver == self.__class__
            if job.status in (Job.Status.SUBMITTED, Job.Status.RUNNING):
                raise InvalidJobStatus(f"Job {job} might be running, please kill first")

        logger.debug("Cleaning up %d jobs", len(jobs))

        for job in jobs:
            for d in ["log_dir", "output_dir"]:
                try:
                    path = job.data[d]
                    if os.path.exists(path):
                        logger.debug("Path %s exists, attempting to delete", path)
                        rmtree(path)
                except Exception:
                    logger.error("Unable to remove directory %s", d)
        return list(jobs)

    def remove(self, job: "Job") -> None:
        logger.debug("Removing job %s", job)
        job = self.cleanup(job)
        job.delete_instance()

    def bulk_remove(self, jobs: Collection["Job"]) -> None:
        logger.debug("Removing %d jobs", len(jobs))
        jobs = self.bulk_cleanup(jobs)
        ids = [j.job_id for j in jobs]
        with database.atomic():
            for chunk in chunks(ids, self.select_batch_size):
                Job.delete().where(  # type: ignore
                    Job.job_id << chunk  # type: ignore
                ).execute()

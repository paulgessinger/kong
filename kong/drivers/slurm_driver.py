import datetime
import os
import re
import shutil
from abc import *
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
)

import sh
import schema as sc

from kong.drivers import InvalidJobStatus
from ..logger import logger
from ..db import database
from ..model import Job, Folder
from ..config import Config
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
        elif status_str == "FAILED" or status_str == "CANCELLED":
            status = Job.Status.FAILED
        elif status_str == "RUNNING":
            status = Job.Status.RUNNING
        else:
            status = Job.Status.UNKOWN

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
    def sacct(self, jobs: Iterable["Job"]) -> Iterator[SlurmAccountingItem]:
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

    def sacct(self, jobs: Iterable["Job"]) -> Iterator[SlurmAccountingItem]:
        job_ids = ",".join([str(j.batch_job_id) for j in jobs])
        starttime = date.today() - timedelta(days=7)
        assert self._sacct is not None
        for line in self._sacct(
            jobs=job_ids,
            brief=True,
            noheader=True,
            parseable2=True,
            starttime=starttime,
            _iter=True,
        ):
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


jobscript_tpl = """
#!/usr/bin/env bash

export KONG_JOB_ID={internal_job_id}
export KONG_JOB_OUTPUT_DIR={output_dir}
export KONG_JOB_LOG_DIR={log_dir}
export KONG_JOB_NPROC={cores}
export KONG_JOB_SCRATCHDIR=/localscratch/${{SLURM_JOB_ID}}/

mkdir -p $KONG_JOB_SCRATCHDIR

{command}
""".strip()

batchfile_tpl = """
#!/bin/bash
#SBATCH -J {name}
#SBATCH -o {stdout} 
#SBATCH -p {queue}                
              
#SBATCH -n {ntasks}                    
#SBATCH -N {nnodes}                    
#SBATCH -c {cores} 
#SBATCH -t {walltime}             

#SBATCH -A {account}             

srun {jobscript}
""".strip()

config_schema = sc.Schema(
    {
        "account": sc.And(str, len),
        "node_size": sc.And(int, lambda i: i > 0),
        "default_queue": sc.And(str, len),
    }
)


class SlurmDriver(DriverBase):
    slurm: SlurmInterface

    def __init__(self, config: Config, slurm: SlurmInterface):
        self.slurm = slurm
        super().__init__(config)
        assert "slurm_driver" in self.config.data
        self.slurm_config = config_schema.validate(self.config.slurm_driver)

    def create_job(
        self,
        folder: "Folder",
        command: str,
        cores: int = 1,
        queue: Optional[str] = None,
        name: Optional[str] = None,
        walltime: timedelta = timedelta(minutes=30),
    ) -> "Job":

        if queue is None:
            queue = self.slurm_config["default_queue"]

        job: Job = Job.create(
            folder=folder,
            batch_job_id=None,  # don't have one until submission
            command=command,
            driver=self.__class__,
            cores=cores,
        )

        if name is None:
            name = f"job_{job.job_id}"

        # in job dir, create output dir
        output_dir = os.path.abspath(
            os.path.join(self.config.joboutputdir, f"{job.job_id}")
        )
        os.makedirs(output_dir, exist_ok=True)

        log_dir = os.path.abspath(os.path.join(self.config.jobdir, f"{job.job_id}"))
        os.makedirs(log_dir, exist_ok=True)

        stdout = os.path.abspath(os.path.join(log_dir, "stdout.txt"))

        batchfile = os.path.join(log_dir, "batchfile.sh")
        jobscript = os.path.join(log_dir, "jobscript.sh")

        job.data = dict(
            stdout=stdout,
            jobscript=jobscript,
            batchfile=batchfile,
            output_dir=output_dir,
            log_dir=log_dir,
            name=name,
            queue=queue,
            exit_code=0,
        )
        job.save()

        values = dict(
            batchfile=batchfile,
            jobscript=jobscript,
            command=command,
            stdout=stdout,
            internal_job_id=job.job_id,
            log_dir=log_dir,
            output_dir=output_dir,
            cores=cores,
            nnodes=1,
            ntasks=1,
            ncores=1,
            account=self.config.slurm_driver["account"],
            name=name,
            queue=queue,
            walltime=self.format_timedelta(walltime),
        )

        batchfile_content = batchfile_tpl.format(**values)
        jobscript_content = jobscript_tpl.format(**values)

        with open(batchfile, "w") as fh:
            fh.write(batchfile_content)

        with open(jobscript, "w") as fh:
            fh.write(jobscript_content)

        return job

    def bulk_create_jobs(self, jobs: Iterable[Dict[str, Any]]) -> List["Job"]:
        return [self.create_job(**kwargs) for kwargs in jobs]

    @staticmethod
    def format_timedelta(delta: timedelta) -> str:
        if delta >= timedelta(hours=100):
            raise ValueError(f"{delta} is too large to format")

        days = delta.days
        hours, rem = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(rem, 60)

        total_hours = days * 24 + hours
        return f"{total_hours:02d}:{minutes:02d}:{seconds:02d}"

    def sync_status(self, job: "Job") -> Job:
        return self.bulk_sync_status([job])[0]

    def bulk_sync_status(self, jobs: Iterable["Job"]) -> Sequence["Job"]:
        for job in jobs:
            self._check_driver(job)

        now = datetime.datetime.now()

        def proc() -> Iterable[Job]:
            for item in self.slurm.sacct(jobs):
                job = Job.get_or_none(batch_job_id=item.job_id)
                if job is None:
                    logger.warning(
                        "Tried to fetch slurm job %d, but did not find it in database",
                        item.job_id,
                    )
                    continue
                job.status = item.status
                job.data["exit_code"] = item.exit_code
                job.updated_at = now
                yield job

        with database.atomic():
            Job.bulk_update(
                proc(),
                fields=[Job.data, Job.status, Job.updated_at],
                batch_size=self.batch_size,
            )
        # reload updated jobs
        ids = [j.job_id for j in jobs]
        fetched = Job.select().where(Job.job_id << ids).execute()  # type: ignore
        return cast(Sequence[Job], fetched)

    @checked_job
    def kill(self, job: "Job", save: bool = True) -> None:
        if job.status == Job.Status.CREATED:
            logger.debug("Job %s in %s, simply setting to failed", job, job.status)
            job.status = Job.Status.FAILED
        elif job.status in (Job.Status.RUNNING, Job.Status.SUBMITTED):
            self.slurm.scancel(job)
            job.status = Job.Status.FAILED
        else:
            logger.debug("Job %s in %s, do nothing")
        if save:
            job.save()

    def bulk_kill(self, jobs: Iterable["Job"]) -> Iterable["Job"]:
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

    def wait(
        self, job: Union["Job", List["Job"]], timeout: Optional[int] = None
    ) -> None:
        raise NotImplementedError()

    @checked_job
    def submit(self, job: "Job", save: bool = True) -> None:
        job.batch_job_id = str(self.slurm.sbatch(job))
        job.status = Job.Status.SUBMITTED
        if save:
            job.save()

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
                fields=[Job.batch_job_id, Job.status, Job.updated_at],
                batch_size=self.batch_size,
            )

    @checked_job  # type: ignore
    @contextmanager  # type: ignore
    def stdout(self, job: Job) -> ContextManager[IO[str]]:
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
            Job.Status.UNKOWN,
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
                shutil.rmtree(path)
                os.makedirs(path)

        self.submit(job)  # this will reset the status
        return job

    def cleanup(self, job: "Job") -> "Job":
        job = self.sync_status(job)
        if job.status in (Job.Status.SUBMITTED, Job.Status.RUNNING):
            raise InvalidJobStatus(f"Job {job} might be running, please kill first")

        logger.debug("Cleanup job %s", job)
        for d in ["log_dir", "output_dir"]:
            path = job.data[d]
            if os.path.exists(path):
                shutil.rmtree(path)
        return job

    def remove(self, job: "Job") -> None:
        logger.debug("Removing job %s", job)
        job = self.cleanup(job)
        job.delete_instance()
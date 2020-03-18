import datetime
import os
import re
from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import (
    Iterator,
    Iterable,
    Optional,
    Union,
    List,
    Sequence,
    cast,
    Collection,
    Dict,
    Any,
)

import sh
from jinja2 import Environment, DictLoader

from kong.drivers.batch_driver_base import BatchDriverBase
from ..drivers import InvalidJobStatus
from ..logger import logger
from ..config import Config
from ..db import database
from ..model.job import Job
from ..model.folder import Folder
from ..util import make_executable, format_timedelta, parse_timedelta
from .driver_base import checked_job


class SlurmAccountingItem:
    job_id: int
    status: Job.Status
    exit_code: int
    other: Dict[str, Any]

    def __init__(
        self, job_id: int, status: Job.Status, exit_code: int, other: Dict[str, Any]
    ):
        self.job_id = job_id
        self.status = status
        self.exit_code = exit_code
        self.other = other

    @classmethod
    def from_parts(
        cls, job_id: str, status_str: str, exit: str, other: Dict[str, Any]
    ) -> "SlurmAccountingItem":
        exit_code, _ = exit.split(":", 1)

        if status_str == "PENDING":
            status = Job.Status.SUBMITTED
        elif status_str == "COMPLETED":
            status = Job.Status.COMPLETED
        elif status_str in ("FAILED", "TIMEOUT") or status_str.startswith("CANCELLED"):
            status = Job.Status.FAILED
        elif status_str == "RUNNING":
            status = Job.Status.RUNNING
        else:
            status = Job.Status.UNKNOWN

        return cls(int(job_id), status, int(exit_code), other=other)

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
    def sacct(
        self, jobs: Collection["Job"], start_delta: timedelta
    ) -> Iterator[SlurmAccountingItem]:
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

    def sacct(
        self, jobs: Collection["Job"], start_delta: timedelta
    ) -> Iterator[SlurmAccountingItem]:

        logger.debug("Getting job info for %d jobs", len(jobs))
        starttime = date.today() - start_delta

        fields = [
            "JobID",
            "State",
            "ExitCode",
            "Submit",
            "Start",
            "End",
            "NodeList",
        ]

        args = dict(
            format=",".join(fields),
            noheader=True,
            parsable2=True,
            starttime=starttime,
            _iter=True,
        )

        if len(jobs) > 0 and len(jobs) < 20:
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
            data = dict(zip(fields, line.split("|")))
            job_id = data["JobID"]
            status = data["State"]
            exit = data["ExitCode"]
            # job_id, status, exit = line.split("|", 3)
            if not job_id.isdigit():
                continue
            yield SlurmAccountingItem.from_parts(
                job_id,
                status,
                exit,
                other=dict(
                    node=data["NodeList"] if data["NodeList"] != "" else None,
                    submit=data["Submit"] if data["Submit"] != "" else None,
                    start=data["Start"] if data["Start"] != "" else None,
                    end=data["End"] if data["End"] != "" else None,
                ),
            )

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


class SlurmDriver(BatchDriverBase):
    slurm: SlurmInterface

    def __init__(self, config: Config, slurm: Optional[SlurmInterface] = None):
        self.slurm = slurm or ShellSlurmInterface()
        super().__init__(config)
        self.slurm_config = self.config.data["slurm_driver"]

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
        output_dir = self.make_output_path(job)
        os.makedirs(output_dir, exist_ok=True)

        log_dir = self.make_log_path(job)
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

    def bulk_sync_status(self, jobs: Collection["Job"]) -> Sequence["Job"]:
        logger.debug("Bulk sync status with %d jobs", len(jobs))
        for job in jobs:
            self._check_driver(job)

        now = datetime.datetime.now()

        def proc() -> Iterable[Job]:
            job_not_found = 0
            for item in self.slurm.sacct(jobs, self.slurm_config["sacct_delta"]):
                job = Job.get_or_none(batch_job_id=item.job_id)
                if job is None:
                    job_not_found += 1
                    continue
                job.status = item.status
                job.data["exit_code"] = item.exit_code
                job.data.update(item.other)
                job.updated_at = now
                assert job.status != Job.Status.CREATED, "Job updated to created?"
                yield job
            if job_not_found > 0:
                logger.info(
                    "Tried to fetch %d jobs which where not found in the database",
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

    @checked_job
    def submit(self, job: "Job", save: bool = True) -> None:
        if job.status != Job.Status.CREATED:
            raise InvalidJobStatus(f"Cannot submit job {job} in status {job.status}")
        job.batch_job_id = str(self.slurm.sbatch(job))
        job.status = Job.Status.SUBMITTED

        if save:
            job.save()

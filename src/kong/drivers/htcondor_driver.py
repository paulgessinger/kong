import datetime
import itertools
import json
import os
import re
from abc import ABC, abstractmethod
from datetime import timedelta

import humanfriendly
from typing import (
    Any,
    Iterator,
    Iterable,
    Optional,
    Union,
    List,
    Dict,
    Sequence,
    cast,
    Collection,
)

import sh
from jinja2 import Environment, DictLoader

from .batch_driver_base import BatchDriverBase
from . import InvalidJobStatus
from .. import config as config_mod
from ..logger import logger
from ..config import Config
from ..db import database
from ..model.job import Job
from ..model.folder import Folder
from ..util import make_executable, parse_timedelta
from .driver_base import DriverBase, checked_job


class HTCondorAccountingItem:
    job_id: int
    condor_status: int
    exit_code: int
    status: Job.Status

    def __init__(self, job_id: int, status: Job.Status, exit_code: int):
        self.job_id = job_id
        self.status = status
        self.exit_code = exit_code

    @classmethod
    def from_parts(
        cls, job_id: int, condor_status: int, exit_code: int
    ) -> "HTCondorAccountingItem":
        # see http://pages.cs.wisc.edu/~adesmet/status.html

        if condor_status == 0:
            # 0 Unexpanded U
            status = Job.Status.SUBMITTED
        elif condor_status == 1:
            # 1 Idle I
            status = Job.Status.SUBMITTED
        elif condor_status == 2:
            # 2 Running R
            status = Job.Status.RUNNING
        elif condor_status == 3:
            # 3 Removed X
            status = Job.Status.FAILED
        elif condor_status == 4:
            # 4 Completed C
            status = Job.Status.COMPLETED
        elif condor_status == 5:
            # 5 Held H
            status = Job.Status.FAILED
        elif condor_status == 6:
            # 6 Submission_err E
            status = Job.Status.FAILED
        else:
            status = Job.Status.UNKNOWN

        if status == Job.Status.COMPLETED:
            # at scheduler level, this is completed, might have exited with failure though
            if exit_code != 0 and exit_code != -1:
                status = Job.Status.FAILED
        return cls(job_id, status, exit_code)

    def __repr__(self) -> str:
        return f"HTCondorAI<{self.job_id}, {repr(self.status)}, {self.exit_code}>"

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, self.__class__)
        return (
            self.job_id == other.job_id
            and self.status == other.status
            and self.exit_code == other.exit_code
        )


class HTCondorInterface(ABC):
    @abstractmethod
    def condor_submit(self, job: Job) -> int:
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def condor_q(self) -> Iterator[HTCondorAccountingItem]:
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def condor_history(self, log_file: str) -> Iterator[HTCondorAccountingItem]:
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def condor_rm(self, job: "Job") -> None:
        raise NotImplementedError()  # pragma: no cover


class ShellHTCondorInterface(HTCondorInterface):
    _condor_submit: Optional[sh.Command] = None
    _condor_q: Optional[sh.Command] = None
    _condor_history: Optional[sh.Command] = None
    _condor_rm: Optional[sh.Command] = None

    config: Dict[str, Any]

    subreg = re.compile(r".* (\d+)\.$")

    def __init__(self, config: Dict[str, Any]) -> None:  # pragma: no cover
        self.config = config
        self._condor_submit = sh.Command("condor_submit")
        self._condor_q = sh.Command("condor_q")
        self._condor_history = sh.Command("condor_history")
        self._condor_rm = sh.Command("condor_rm")

    def _parse_output(self, output: str) -> Iterator[HTCondorAccountingItem]:
        if output.strip() == "":
            return []
        data = json.loads(output)
        for item in data:
            job_id = item["ClusterId"]
            assert item["ProcId"] == 0, "Clusters with more than one jobs not supported"
            condor_status = item["JobStatus"]
            exit_code = item["ExitCode"] if "ExitCode" in item else -1
            yield HTCondorAccountingItem.from_parts(job_id, condor_status, exit_code)

    def condor_q(self) -> Iterator[HTCondorAccountingItem]:

        logger.debug("Getting job infos")

        args = ["-attributes", ",".join(["ClusterId", "ProcId", "JobStatus"]), "-json"]

        assert self._condor_q is not None
        return self._parse_output(str(self._condor_q(*args)))

    def condor_history(self, log_file: str) -> Iterator[HTCondorAccountingItem]:

        logger.debug("Getting job infos")

        if not os.path.exists(log_file):
            logger.debug(
                "Userlog file does not exist. There will be no output, don't call"
            )
            return iter([])

        args = [
            "-userlog",
            log_file,
            "-attributes",
            ",".join(["ClusterId", "ProcId", "JobStatus", "ExitCode"]),
            "-json",
            "-limit",
            "10000",
        ]

        assert self._condor_history is not None
        return self._parse_output(str(self._condor_history(*args)))

    def condor_submit(self, job: Job) -> int:
        assert self._condor_submit is not None
        raw = self._condor_submit(job.data["batchfile"])
        logger.debug("condor_submit: %s", raw)
        m = self.subreg.search(str(raw))
        assert m is not None
        return int(m.group(1))

    def condor_rm(self, job: Job) -> None:
        assert self._condor_rm is not None
        res = self._condor_rm(job.batch_job_id)
        logger.debug("condor_rm: %s", res)


jobscript_tpl_str = """
#!/usr/bin/env bash

export KONG_JOB_ID={{internal_job_id}}
export KONG_JOB_OUTPUT_DIR={{output_dir}}
export KONG_JOB_LOG_DIR={{log_dir}}
export KONG_JOB_NPROC={{cores}}
export KONG_JOB_SCRATCHDIR=$_CONDOR_SCRATCH_DIR
export HTCONDOR_CLUSTER_ID=$(grep "^ClusterId" $_CONDOR_JOB_AD | cut -d= - -f2 | awk '{$1=$1};1')

mkdir -p $KONG_JOB_SCRATCHDIR

stdout={{stdout}}

({{command}}) > $stdout 2>&1
""".strip()

batchfile_tpl_str = """
universe = vanilla
log = {{htcondor_out}}
executable = {{jobscript}}
request_cpus = {{cores}}
request_memory = {{memory}}
batch_name = {{name}}
+MaxRuntime = {{walltime}}

{{submitfile_extra}}

queue 1
""".strip()  # noqa: W291, W293

env = Environment(
    loader=DictLoader({"batchfile": batchfile_tpl_str, "jobscript": jobscript_tpl_str})
)

batchfile_tpl = env.get_template("batchfile")
jobscript_tpl = env.get_template("jobscript")


class HTCondorDriver(BatchDriverBase):
    htcondor: HTCondorInterface

    def __init__(self, config: Config, htcondor: Optional[HTCondorInterface] = None):
        DriverBase.__init__(self, config)
        self.htcondor_config = self.config.data["htcondor_driver"]
        self.htcondor = htcondor or ShellHTCondorInterface(self.htcondor_config)
        log_dir = os.path.join(config_mod.APP_DIR, "htcondor_log")
        os.makedirs(log_dir, exist_ok=True)
        self.log_file = os.path.join(log_dir, "htcondor.log")

        if os.path.exists(self.log_file):
            log_size = os.path.getsize(self.log_file)
            if log_size > 50 * 1e6:
                logger.warning(
                    "HTCondor log file at %s is large: %s. Consider deleting it,"
                    + "Finished but unsynced jobs will not be able to be updated after this.",
                    self.log_file,
                    humanfriendly.format_size(log_size),
                )

    def create_job(
        self,
        folder: "Folder",
        command: str,
        cores: int = 1,
        memory: int = 2000,
        universe: Optional[str] = None,
        name: Optional[str] = None,
        walltime: Union[timedelta, str] = timedelta(minutes=30),
    ) -> "Job":  # type: ignore

        if universe is None:
            universe = self.htcondor_config["default_universe"]

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

        batchfile = os.path.join(log_dir, "batchfile.sh")
        jobscript = os.path.join(log_dir, "jobscript.sh")

        if isinstance(walltime, str):
            norm_walltime = int(parse_timedelta(walltime).total_seconds())
        elif isinstance(walltime, timedelta):
            norm_walltime = int(walltime.total_seconds())
        else:
            raise ValueError("Walltime must be timedelta or string")

        job.data = dict(
            stdout=stdout,
            htcondor_out=self.log_file,
            jobscript=jobscript,
            batchfile=batchfile,
            output_dir=output_dir,
            log_dir=log_dir,
            name=name,
            exit_code=0,
            universe=universe,
            walltime=norm_walltime,
            submitfile_extra=self.htcondor_config["submitfile_extra"],
        )
        job.save()

        values = dict(
            batchfile=batchfile,
            jobscript=jobscript,
            command=command,
            stdout=stdout,
            htcondor_out=self.log_file,
            internal_job_id=job.job_id,
            log_dir=log_dir,
            output_dir=output_dir,
            cores=cores,
            memory=memory,
            universe=universe,
            name=name,
            walltime=norm_walltime,
            submitfile_extra=self.htcondor_config["submitfile_extra"],
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

        now = datetime.datetime.utcnow()

        def proc() -> Iterable[Job]:
            job_not_found = 0
            for item in itertools.chain(
                self.htcondor.condor_q(), self.htcondor.condor_history(self.log_file)
            ):
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
    def submit(self, job: "Job", save: bool = True) -> None:
        if job.status != Job.Status.CREATED:
            raise InvalidJobStatus(f"Cannot submit job {job} in status {job.status}")
        job.batch_job_id = str(self.htcondor.condor_submit(job))
        job.status = Job.Status.SUBMITTED

        if save:
            job.save()

    @checked_job
    def kill(self, job: "Job", save: bool = True) -> None:
        if job.status in (Job.Status.CREATED, Job.Status.UNKNOWN):
            logger.debug("Job %s in %s, simply setting to failed", job, job.status)
            job.status = Job.Status.FAILED
        elif job.status in (Job.Status.RUNNING, Job.Status.SUBMITTED):
            self.htcondor.condor_rm(job)
            job.status = Job.Status.FAILED
        else:
            logger.debug("Job %s in %s, do nothing")
        if save:
            job.save()

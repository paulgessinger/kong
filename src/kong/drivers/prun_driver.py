import datetime
import functools
import os
import sys
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
import shlex
from typing import (
    Collection,
    Sequence,
    Iterable,
    List,
    cast,
    Union,
    Optional,
    ContextManager,
    Dict,
    Any,
)
from concurrent.futures import Executor, ThreadPoolExecutor
import re

from .batch_driver_base import BatchDriverBase
from .driver_base import DriverBase
from . import InvalidJobStatus
from ..executor import SerialExecutor
from ..logger import logger
from ..config import Config
from ..db import database
from ..model.job import Job
from ..model.folder import Folder

import sh

# from ..util import make_executable, format_timedelta, parse_timedelta
from .driver_base import checked_job


def commands_get_status_output_decorator(emi_path, fn):
    @functools.wraps(fn)
    def wrapped(com, *args, **kwargs):
        rep, _ = re.subn(
            r"(voms-proxy-\w+)", lambda m: os.path.join(emi_path, m.group(1)), com
        )
        logger.info("Rewrite Panda CMD: %s -> %s", com, rep)
        return fn(rep, *args, **kwargs)

    return wrapped


def map_status(status: str) -> Job.Status:
    if status == "done":
        return Job.Status.COMPLETED
    elif status in (
        "broken",
        "failed",
        "finished",
        "aborting",
        "aborted",
        "finishing",
        "tobroken",
        "exhausted",
        "passed",
    ):
        return Job.Status.FAILED
    elif status in (
        "registered",
        "defined",
        "assigning",
        "ready",
        "pending",
        "scouting",
        "scouted",
        "topreprocess",
        "preprocessing",
        "toretry",
        "toincexec",
        "rerefine",
        "paused",
        "throttled",
    ):
        return Job.Status.SUBMITTED
    elif status in ("running", "prepared"):
        return Job.Status.RUNNING
    else:
        return Job.Status.UNKNOWN


_first_run = True


class PrunDriver(DriverBase):
    _pandatools: Any

    def __init__(self, config: Config):
        super().__init__(config)
        self.prun_config = self.config.data["prun_driver"]

        # when we make this, we need to make sure the python path is set so panda can be picked up

        global _first_run

        logger.debug("Is PrunDriver first run (init)? %s", _first_run)
        if _first_run:
            path = os.environ.get("PATH", "")
            os.environ["PATH"] = ":".join([self.prun_config["emi_path"], path])

            os.environ["PATHENA_GRID_SETUP_SH"] = self.prun_config[
                "PATHENA_GRID_SETUP_SH"
            ]

            path = self.prun_config["PANDA_PYTHONPATH"]
            if path not in sys.path:
                sys.path.append(path)
            import pandatools
            import pandatools.PsubUtils
            import pandatools.MiscUtils

            decorated = commands_get_status_output_decorator(
                emi_path=self.prun_config["emi_path"],
                fn=pandatools.PsubUtils.commands_get_status_output,
            )

            pandatools.MiscUtils.commands_get_status_output = decorated

            _first_run = False
            # logger.debug("pandatools hook set up: %s", self._pandatools)

        import pandatools
        import pandatools.queryPandaMonUtils
        import pandatools.Client

        self._pandatools = pandatools

    def create_job(
        self,
        folder: "Folder",
        command: str,
        cores: int = 0,
        task_id: Optional[int] = None,  # , *args: Any, **kwargs: Any
    ) -> "Job":

        job: Job = Job.create(
            folder=folder,
            batch_job_id=task_id,
            command=command,
            driver=self.__class__,
            cores=cores,
        )

        log_dir = self.make_log_path(job)
        os.makedirs(log_dir, exist_ok=True)

        job.data = {"log_dir": log_dir}
        job.save()

        return job

    def bulk_sync_status(self, jobs: Collection["Job"]) -> Sequence["Job"]:
        logger.debug("Bulk sync status with %d jobs", len(jobs))
        for job in jobs:
            self._check_driver(job)

        task_ids = "|".join([str(job.batch_job_id) for job in jobs])

        now = datetime.datetime.now()

        def proc() -> Iterable[Job]:
            _, _, data = self._pandatools.queryPandaMonUtils.query_tasks(
                jeditaskid=task_ids
            )

            job_not_found = 0
            for item in data:
                job = Job.get_or_none(batch_job_id=item.pop("jeditaskid"))
                if job is None:
                    job_not_found += 1
                    continue
                job.status = map_status(item["status"])

                if "dsinfo" in item and item["dsinfo"]["nfilesfailed"] > 0:
                    logger.debug("Update based on dsinfo: %s", item["dsinfo"])
                    job.status = Job.Status.FAILED

                if (
                    "scoutinghascritfailures" in item
                    and item["scoutinghascritfailures"]
                ):
                    logger.debug("Info says job has 'scoutinghascritfailures'")
                    job.status = Job.Status.FAILED

                job.data.update(item)
                job.data["url"] = f"https://bigpanda.cern.ch/task/{job.batch_job_id}"
                creationdate = datetime.datetime.strptime(
                    item["creationdate"], "%Y-%m-%d %H:%M:%S"
                )
                updated = datetime.datetime.strptime(
                    item["statechangetime"], "%Y-%m-%d %H:%M:%S"
                )
                job.created_at = creationdate
                job.updated_at = updated
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
                fields=[Job.data, Job.status, Job.created_at, Job.updated_at],
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

    def bulk_create_jobs(self, jobs: Iterable[Dict[str, Any]]) -> List["Job"]:
        raise NotImplementedError()

    def sync_status(self, job: "Job") -> "Job":
        return self.bulk_sync_status([job])[0]

    def kill(self, job: "Job", save: bool = True) -> "Job":
        raise NotImplementedError()

    def bulk_kill(self, jobs: Sequence["Job"]) -> Sequence["Job"]:
        raise NotImplementedError()

    def wait_gen(
        self,
        job: Union["Job", List["Job"]],
        poll_interval: Optional[int] = None,
        timeout: Optional[int] = None,
    ) -> Iterable[List["Job"]]:
        yield from BatchDriverBase.wait_gen(self, job, poll_interval, timeout)

    def submit(self, job: "Job", save: bool = True) -> None:
        if job.status > Job.Status.CREATED:
            raise InvalidJobStatus(f"Cannot submit job in state {job.status}")
        logger.info(
            "PrunDriver currently does not support submitting. Will change status only"
        )
        job.status = Job.Status.SUBMITTED

    def bulk_submit(self, jobs: Iterable["Job"]) -> None:
        raise NotImplementedError()

    def stdout(self, job: "Job") -> Path:
        self._check_driver(job)

        if not job.status in (Job.Status.COMPLETED, Job.Status.FAILED):
            raise ValueError("Job needs to have terminated")

        log_dir = Path(job.data["log_dir"])

        combined_log = log_dir / "combined_stdout.txt"

        if not combined_log.exists():

            bash = sh.Command("bash")

            def rucio(*args, **kwargs):
                args = " ".join(map(shlex.quote, args))
                c = f"source ${{ATLAS_LOCAL_ROOT_BASE}}/user/atlasLocalSetup.sh > /dev/null 2>&1;lsetup rucio > /dev/null 2>&1; rucio {args}"
                logger.debug(c)
                return bash("-c", c, **kwargs)

            tar = sh.Command("tar")

            def get_datasets(pattern: str) -> List[str]:
                return rucio("ls", "--short", pattern).strip().splitlines()

            taskname = job.data["taskname"]
            if taskname.endswith("/"):
                taskname = taskname[:-1]

            with ThreadPoolExecutor() as ex:
                datasets = sum(
                    ex.map(
                        get_datasets,
                        (f"{taskname}.log.*", f"panda.um.{taskname}.log.*"),
                    ),
                    [],
                )

                logger.debug("Datasets: %s", datasets)

                assert log_dir.exists()

                download_dir = log_dir / "rucio_download"
                download_dir.mkdir(exist_ok=True)

                def download(d):
                    try:
                        rucio("download", d, "--no-subdir", _cwd=download_dir)
                    except BaseException as e:
                        logger.error(e.stderr.decode("utf8"))

                datasets = list(ex.map(download, datasets))

            for file in download_dir.iterdir():
                if file.suffix == ".tgz":
                    tar("-xvf", file, "-C", log_dir)

            with combined_log.open("w") as ofh:
                for outfile in log_dir.rglob("*/payload.stdout"):
                    logger.debug(outfile)
                    with outfile.open("r") as ifh:
                        head = f"#### {outfile} ####"
                        ofh.write("#" * len(head) + "\n")
                        ofh.write(f"{head}\n")
                        ofh.write("#" * len(head) + "\n")
                        ofh.write("\n\n")
                        ofh.write(ifh.read())
                        ofh.write("\n\n")

        return combined_log

    def stderr(self, job: "Job") -> ContextManager[None]:
        raise NotImplementedError()

    def resubmit(self, job: "Job") -> "Job":
        raise NotImplementedError()

    def bulk_resubmit(
        self, jobs: Collection["Job"], do_submit: bool = True
    ) -> Iterable["Job"]:
        raise NotImplementedError()

    def cleanup(self, job: "Job") -> "Job":
        raise NotImplementedError()

    def bulk_cleanup(
        self, jobs: Sequence["Job"], progress: bool, ex: Executor = SerialExecutor()
    ) -> Iterable["Job"]:
        raise NotImplementedError()

    def remove(self, job: "Job") -> None:
        raise NotImplementedError()

    def bulk_remove(self, jobs: Sequence["Job"], do_cleanup: bool) -> None:
        raise NotImplementedError()

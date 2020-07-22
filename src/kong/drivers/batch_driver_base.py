import datetime
import os
import time
from concurrent.futures import ThreadPoolExecutor, Executor, as_completed
from contextlib import contextmanager
from typing import (
    Iterable,
    Dict,
    List,
    Any,
    Union,
    Optional,
    Iterator,
    IO,
    ContextManager,
    Sequence,
    cast,
)

from kong.drivers import InvalidJobStatus
from kong.logger import logger
from kong.util import rmtree, chunks
from .driver_base import DriverBase, checked_job
from ..executor import SerialExecutor
from ..model.job import Job
from ..db import database


class BatchDriverBase(DriverBase):
    def bulk_create_jobs(self, jobs: Iterable[Dict[str, Any]]) -> List["Job"]:
        return [self.create_job(**kwargs) for kwargs in jobs]

    def sync_status(self, job: "Job") -> Job:
        return self.bulk_sync_status([job])[0]

    def bulk_kill(self, jobs: Sequence["Job"]) -> Sequence["Job"]:
        now = datetime.datetime.utcnow()
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
            delta: datetime.timedelta = now - start
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

    def bulk_submit(self, jobs: Iterable["Job"]) -> None:
        now = datetime.datetime.utcnow()

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

            with database.atomic():
                now = datetime.datetime.utcnow()

                def jobit() -> Iterable[Job]:
                    for job in jobs:
                        job.status = Job.Status.CREATED
                        job.updated_at = now
                        yield job

                Job.bulk_update(
                    jobit(),
                    fields=[Job.status, Job.updated_at],
                    batch_size=self.batch_size,
                )

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

    def _bulk_cleanup(self, jobs: Sequence["Job"], ex: Executor) -> Iterable["Job"]:
        jobs = self.bulk_sync_status(jobs)
        # safety check
        for job in jobs:
            assert job.driver == self.__class__
            if job.status in (Job.Status.SUBMITTED, Job.Status.RUNNING):
                raise InvalidJobStatus(f"Job {job} might be running, please kill first")

        logger.debug("Cleaning up %d jobs", len(jobs))

        def run(job: Job) -> Job:
            for d in ["log_dir", "output_dir"]:
                try:
                    path = job.data[d]
                    if os.path.exists(path):
                        logger.debug("Path %s exists, attempting to delete", path)
                        rmtree(path)
                except Exception:
                    logger.error("Unable to remove directory %s", d)
            return job

        futures = [ex.submit(run, j) for j in jobs]

        for f in as_completed(futures):
            yield f.result()

    def bulk_cleanup(
        self,
        jobs: Sequence["Job"],
        progress: bool = False,
        ex: Executor = SerialExecutor(),
    ) -> Iterable["Job"]:
        it = self._bulk_cleanup(jobs, ex)
        if progress:
            return it
        else:
            return list(it)

    def remove(self, job: "Job") -> None:
        logger.debug("Removing job %s", job)
        job = self.cleanup(job)
        job.delete_instance()

    def bulk_remove(self, jobs: Sequence["Job"], do_cleanup: bool = True) -> None:
        logger.debug("Removing %d jobs", len(jobs))
        if do_cleanup:
            jobs = cast(Sequence[Job], self.bulk_cleanup(jobs))
        ids = [j.job_id for j in jobs]
        with database.atomic():
            for chunk in chunks(ids, self.select_batch_size):
                Job.delete().where(  # type: ignore
                    Job.job_id << chunk  # type: ignore
                ).execute()

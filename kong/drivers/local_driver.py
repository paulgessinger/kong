import os
from typing import Any, Optional
import uuid

from ..model import Folder, Job
from ..logger import logger
from ..config import Config
from . import DriverBase

jobscript_tpl = """
#!/usr/bin/env bash

sig_handler() {{
    exit_status=$?
    echo $exit_status > {exit_status_file}
    exit "$exit_status"
}}
trap sig_handler INT HUP TERM QUIT

export KONG_JOB_ID={internal_job_id}
export KONG_JOB_OUTPUT_DIR={output_dir}
export KONG_JOB_NPROC={nproc}

stdout={stdout}
stderr={stderr}

({command}) > $stdout 2> $stderr$

""".strip()


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

    def create_job(self, folder: Folder, command: str, cores: int = 1, *args: Any, **kwargs: Any) -> Job:
        assert len(args) == 0 and len(kwargs) == 0, "No extra arguments allowed"
        # assert (
        #     "batch_job_id" not in kwargs
        # ), "Cannot override batch job id, it is set by the driver"
        # assert "driver" not in kwargs, "Cannot override driver"

        batch_job_id = str(uuid.uuid1())


        # create folder structure
        jobdir = os.path.abspath(os.path.join(self.config.jobdir, batch_job_id))
        os.makedirs(jobdir)

        # in job dir, create output dir
        output_dir = os.path.abspath(os.path.join(jobdir, "output"))
        os.makedirs(output_dir)

        stdout = os.path.abspath(os.path.join(self.config.jobdir, batch_job_id, "stdout.txt"))
        stderr = os.path.abspath(os.path.join(self.config.jobdir, batch_job_id, "stderr.txt"))
        exit_status_file = os.path.abspath(os.path.join(
            self.config.jobdir, batch_job_id, "exit_status.txt"
        ))
        scriptpath = os.path.join(jobdir, "jobscript.sh")

        data = dict(
            stdout=stdout,
            stderr=stderr,
            exit_status_file=exit_status_file,
            scriptpath=scriptpath,
            nproc=cores
        )

        job: Job = Job.create(
            folder=folder,
            batch_job_id=batch_job_id,
            command=command,
            driver=self.__class__.__name__,
            data=data
        )

        values = dict(
            command=command,
            stdout=stdout,
            stderr=stderr,
            internal_job_id=job.job_id,
            output_dir=output_dir,
            exit_status_file=exit_status_file,
            nproc=cores,
        )
        logger.debug("Creating job with values: %s", str(values))

        jobscript = jobscript_tpl.format(**values)


        with open(scriptpath, "w") as fh:
            fh.write(jobscript)


        return job

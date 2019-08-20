import os
import random
import time

import psutil
import pytest
from unittest.mock import Mock

from kong.drivers import LocalDriver
import kong
from kong.model import Folder, Job


@pytest.fixture
def state(app_env, db, monkeypatch):
    app_dir, config_path, tmp_path = app_env
    with monkeypatch.context() as m:
        m.setattr(
            "click.prompt",
            Mock(side_effect=["LocalDriver", os.path.join(app_dir, "joblog")]),
        )
        kong.setup.setup(None)
    return kong.get_instance()


@pytest.fixture
def driver(state):
    return LocalDriver(state.config)


def test_create_job(driver, tree, state):
    with pytest.raises(AssertionError):
        driver.create_job(tree, command="sleep 1", batch_job_id=42)
    with pytest.raises(AssertionError):
        driver.create_job(tree, command="sleep 1", driver="OtherDriver")

    config = state.config
    assert len(os.listdir(config.jobdir)) == 0, "Job dir is not empty"

    j1 = driver.create_job(tree, command="sleep 1")
    assert j1 is not None
    assert j1.folder == tree
    assert len(tree.jobs) == 1
    assert tree.jobs[0] == j1
    assert j1.status == Job.Status.CREATED

    assert os.path.exists(
        os.path.join(config.jobdir, j1.batch_job_id)
    ), "Does not create job directory"

    assert os.path.exists(
        os.path.join(config.jobdir, j1.batch_job_id, "output")
    ), "Does not create job output directory"

    assert os.path.isfile(
        os.path.join(config.jobdir, j1.batch_job_id, "jobscript.sh")
    ), "Does not create job script"

    f2 = tree.subfolder(("f2"))
    j2 = driver.create_job(f2, command="sleep 1")
    assert j2 is not None
    assert j2.folder == f2
    assert len(f2.jobs) == 1
    assert f2.jobs[0] == j2
    assert j2.status == Job.Status.CREATED
    assert os.path.exists(
        os.path.join(config.jobdir, j2.batch_job_id)
    ), "Does not create job directory"
    assert os.path.exists(
        os.path.join(config.jobdir, j2.batch_job_id, "output")
    ), "Does not create job output directory"
    assert os.path.isfile(
        os.path.join(config.jobdir, j2.batch_job_id, "jobscript.sh")
    ), "Does not create job script"

    print(j2.data)

    assert j1.batch_job_id != j2.batch_job_id


import logging

logging.getLogger("kong").setLevel(logging.DEBUG)


def test_job_env_is_valid(driver, state):
    root = Folder.get_root()

    def run_get_env(**kwargs):
        j1 = driver.create_job(folder=root, command="env", **kwargs)
        j1.submit()
        j1.wait()
        env = {}
        with j1.stdout() as fh:
            raw = fh.read().strip().split("\n")
            for line in raw:
                k, v = line.split("=", 1)
                env[k] = v
        return j1, env

    job, env = run_get_env()
    assert env["KONG_JOB_NPROC"] == "1"
    assert env["KONG_JOB_ID"] == str(job.job_id)
    assert env["KONG_JOB_OUTPUT_DIR"] == job.data["output_dir"]

    job, env = run_get_env(cores=8)
    assert env["KONG_JOB_NPROC"] == "8"
    assert env["KONG_JOB_ID"] == str(job.job_id)
    assert env["KONG_JOB_OUTPUT_DIR"] == job.data["output_dir"]


def test_run_job(driver, state, db):
    root = Folder.get_root()

    value = "I AM THE EXPECTED OUTPUT"
    script = f"echo '{value}'"

    j1 = driver.create_job(command=script, folder=root)
    assert j1.status == Job.Status.CREATED

    driver.submit(j1)
    assert j1.status == Job.Status.SUBMITTED

    print("WAIT")
    driver.wait(j1, timeout=2)
    print("WAIT DONE")
    driver.wait(j1, timeout=2)
    assert j1.status == Job.Status.COMPLETED

    with driver.stdout(j1) as so:
        out = so.read().strip()

    assert out == value


def test_run_stdout_stderr(driver, state):
    root = Folder.get_root()
    error = "ERRORERROR"
    value = "VALUEVALUE"

    j1 = driver.create_job(command=f"echo '{error}' 1>&2 ; echo '{value}'", folder=root)
    j1.submit()
    j1.wait()
    assert j1.status == Job.Status.COMPLETED

    # os.system("ls -al "+os.path.join(state.config.jobdir, os.listdir(state.config.jobdir)[0]))
    # os.system("cat "+os.path.join(state.config.jobdir, os.listdir(state.config.jobdir)[0], "stdout.txt"))
    # os.system("cat "+os.path.join(state.config.jobdir, os.listdir(state.config.jobdir)[0], "stderr.txt"))

    with j1.stderr() as fh:
        assert fh.read().strip() == error
    with j1.stdout() as fh:
        assert fh.read().strip() == value


def test_run_job_already_completed(driver, state):
    root = Folder.get_root()
    j1 = driver.create_job(command="echo 'hi'", folder=root)
    j1.submit()

    # already waited, process is reaped
    print(j1.data["pid"])
    proc = psutil.Process(j1.data["pid"])
    print(proc.status())
    proc.wait()

    driver.wait(j1)
    assert j1.status == Job.Status.COMPLETED


def test_run_job_timeout(driver, state):
    root = Folder.get_root()
    j1 = driver.create_job(command="sleep 0.3", folder=root)
    j1.submit()

    with pytest.raises(TimeoutError):
        driver.wait(j1, timeout=0.1)
    assert j1.status == Job.Status.RUNNING
    time.sleep(0.3)
    driver.wait(j1, timeout=0.1)
    assert j1.status == Job.Status.COMPLETED


def test_run_failed(driver, state):
    root = Folder.get_root()
    j1 = driver.create_job(command="exit 1", folder=root)
    j2 = driver.create_job(command="exit 127", folder=root)

    j1.submit()
    j2.submit()

    j1.wait()
    j2.wait()

    assert j1.status == Job.Status.FAILED
    assert j1.data["exit_code"] == 1

    assert j2.status == Job.Status.FAILED
    assert j2.data["exit_code"] == 127


def test_run_killed(driver, state):
    root = Folder.get_root()
    j1 = driver.create_job(command="sleep 10", folder=root)
    j1.submit()
    proc = psutil.Process(pid=j1.data["pid"])
    proc.kill()
    j1.wait()
    assert j1.status == Job.Status.UNKOWN


def test_run_terminated(driver, state):
    root = Folder.get_root()
    j1 = driver.create_job(command="echo 'begin'; sleep 10 ; echo 'end'", folder=root)
    j1.submit()
    proc = psutil.Process(pid=j1.data["pid"])
    time.sleep(0.2)
    driver.sync_status(j1)
    assert j1.status == Job.Status.RUNNING
    for child in proc.children(
        recursive=True
    ):  # or parent.children() for recursive=False
        child.terminate()
    proc.terminate()
    j1.wait()
    assert j1.status == Job.Status.FAILED


def test_run_kill(driver, state):
    root = Folder.get_root()
    j1 = driver.create_job(command="echo 'begin'; sleep 10 ; echo 'end'", folder=root)
    j1.submit()
    driver.kill(j1)
    j1.wait()
    assert j1.status == Job.Status.FAILED

    j2 = driver.create_job(command="echo 'begin'; sleep 10 ; echo 'end'", folder=root)
    j2.submit()
    time.sleep(0.2)  # wait a bit until running
    assert j2.get_status() == Job.Status.RUNNING
    j2.kill()
    assert j2.status == Job.Status.FAILED  # should be failed right away
    j2.wait()
    assert j2.status == Job.Status.FAILED  # shouldn't change after waiting


def test_bulk_wait(driver, state):
    root = Folder.get_root()

    jobs = []
    for i in range(15):
        job = driver.create_job(
            folder=root, command=f"sleep {random.random()} ; echo 'JOB{i}'"
        )
        job.submit()
        jobs.append(job)

    sjobs = len(jobs)

    for i in range(15):
        job = driver.create_job(
            folder=root,
            command=f"sleep {random.random()} ; echo 'JOB{i+sjobs}' 1>&2 ; exit 1",
        )
        job.submit()
        jobs.append(job)

    driver.wait(jobs)

    for i, job in enumerate(jobs[:15]):
        assert job.status == Job.Status.COMPLETED
        with job.stdout() as fh:
            assert fh.read().strip() == f"JOB{i}"
    for i, job in enumerate(jobs[15:]):
        assert job.status == Job.Status.FAILED
        with job.stderr() as fh:
            assert fh.read().strip() == f"JOB{i+sjobs}"


def test_bulk_sync(driver, state):
    root = Folder.get_root()

    jobs = []
    for i in range(15):
        job = driver.create_job(
            folder=root, command=f"sleep {0.2 + random.random()*0.2} ; echo 'JOB{i}'"
        )
        job.submit()
        jobs.append(job)

    sjobs = len(jobs)

    for i in range(15):
        job = driver.create_job(
            folder=root,
            command=f"sleep {0.2 + random.random()*0.2} ; echo 'JOB{i+sjobs}' 1>&2 ; exit 1",
        )
        job.submit()
        jobs.append(job)

    time.sleep(0.01)  # should all be running now
    driver.bulk_sync_status(jobs)

    for job in jobs:
        assert job.status == Job.Status.RUNNING

    time.sleep(0.5)  # should all be finished now
    driver.bulk_sync_status(jobs)

    for i, job in enumerate(jobs[:15]):
        assert job.status == Job.Status.COMPLETED
        with job.stdout() as fh:
            assert fh.read().strip() == f"JOB{i}"
    for i, job in enumerate(jobs[15:]):
        assert job.status == Job.Status.FAILED
        with job.stderr() as fh:
            assert fh.read().strip() == f"JOB{i+sjobs}"


def test_job_resubmit(driver, state):
    root = Folder.get_root()
    j1 = driver.create_job(
        command="echo 'begin'; sleep 0.2 ; echo 'end' ; exit 1", folder=root
    )
    j1.submit()
    j1.wait()
    assert j1.status == Job.Status.FAILED
    with j1.stdout() as fh:
        assert fh.read().strip() == "begin\nend"

    driver.resubmit(j1)
    for path in ["output_dir", "exit_status_file", "stdout", "stderr"]:
        assert not os.path.exists(j1.data[path])

    assert j1.status == Job.Status.SUBMITTED
    j1.wait()
    assert j1.status == Job.Status.FAILED

import os
import shutil
import tempfile
from datetime import timedelta
from typing import Collection, Iterator
from unittest.mock import Mock, ANY, call

import pytest

from kong import util
from kong.config import Config, slurm_schema
from kong.drivers import InvalidJobStatus, get_driver
from kong.drivers.htcondor_driver import (
    HTCondorInterface,
    HTCondorDriver,
    HTCondorAccountingItem,
    ShellHTCondorInterface,
)
from kong.model.job import Job
from kong.model.folder import Folder
from kong.util import is_executable


@pytest.fixture
def driver(monkeypatch, state):
    # set some config values
    data = state.config.data.copy()
    data["htcondor_driver"] = dict()
    state.config = Config(data)

    monkeypatch.setattr(HTCondorInterface, "__abstractmethods__", set())
    monkeypatch.setattr(ShellHTCondorInterface, "__init__", Mock(return_value=None))

    sif = ShellHTCondorInterface()
    sif.config = state.config.htcondor_driver

    return HTCondorDriver(state.config, sif)


def test_condor_q(driver, monkeypatch, state):
    condor_q_output = """
[
{
  "ClusterId": 5184659,
  "JobStatus": 0,
  "ProcId": 0
}
,
{
  "ClusterId": 5184660,
  "JobStatus": 1,
  "ProcId": 0
}
,
{
  "ClusterId": 5184661,
  "JobStatus": 2,
  "ProcId": 0
}
,
{
  "ClusterId": 5184662,
  "JobStatus": 3,
  "ProcId": 0
}
,
{
  "ClusterId": 5184663,
  "JobStatus": 4,
  "ProcId": 0
}
,
{
  "ClusterId": 5184664,
  "JobStatus": 5,
  "ProcId": 0
}
,
{
  "ClusterId": 5184665,
  "JobStatus": 6,
  "ProcId": 0
}
,
{
  "ClusterId": 5184666,
  "JobStatus": 42,
  "ProcId": 0
}
]
    """.strip()

    with monkeypatch.context() as m:

        mock = Mock(return_value=condor_q_output)
        m.setattr(driver.htcondor, "_condor_q", mock)

        res = list(driver.htcondor.condor_q())

        mock.assert_called_once_with(
            "-attributes", "ClusterId,ProcId,JobStatus", "-json"
        )

        ref = [
            HTCondorAccountingItem(5184659, Job.Status.SUBMITTED, -1),
            HTCondorAccountingItem(5184660, Job.Status.SUBMITTED, -1),
            HTCondorAccountingItem(5184661, Job.Status.RUNNING, -1),
            HTCondorAccountingItem(5184662, Job.Status.FAILED, -1),
            HTCondorAccountingItem(5184663, Job.Status.COMPLETED, -1),
            HTCondorAccountingItem(5184664, Job.Status.FAILED, -1),
            HTCondorAccountingItem(5184665, Job.Status.FAILED, -1),
            HTCondorAccountingItem(5184666, Job.Status.UNKNOWN, -1),
        ]

        assert len(ref) == len(res)
        for a, b in zip(ref, res):
            assert a == b


def test_condor_q_empty(driver, monkeypatch, state):
    mock = Mock(return_value="")
    monkeypatch.setattr(driver.htcondor, "_condor_q", mock)
    res = list(driver.htcondor.condor_q())
    assert res == []


def test_condor_history_empty(driver, monkeypatch, state):
    mock = Mock(return_value="")
    monkeypatch.setattr(driver.htcondor, "_condor_history", mock)
    with tempfile.NamedTemporaryFile() as f:
        res = list(driver.htcondor.condor_history(f.name))
    assert res == []
    assert mock.call_count == 1

def test_condor_history_no_userlog(driver, monkeypatch, state):
    mock = Mock(return_value="")
    monkeypatch.setattr(driver.htcondor, "_condor_history", mock)
    monkeypatch.setattr("os.path.exists", Mock(return_value=False))
    assert list(driver.htcondor.condor_history("blub")) == []
    assert mock.call_count == 0



def test_condor_history(driver, monkeypatch, state):
    condor_history_output = """
[
{
  "ClusterId": 5184659,
  "JobStatus": 0,
  "ProcId": 0,
  "ExitCode": 0
}
,
{
  "ClusterId": 5184660,
  "JobStatus": 1,
  "ProcId": 0,
  "ExitCode": 0
}
,
{
  "ClusterId": 5184661,
  "JobStatus": 2,
  "ProcId": 0,
  "ExitCode": 0
}
,
{
  "ClusterId": 5184662,
  "JobStatus": 3,
  "ProcId": 0,
  "ExitCode": 0
}
,
{
  "ClusterId": 5184663,
  "JobStatus": 4,
  "ProcId": 0,
  "ExitCode": 0
}
,
{
  "ClusterId": 5184664,
  "JobStatus": 5,
  "ProcId": 0,
  "ExitCode": 3
}
,
{
  "ClusterId": 5184665,
  "JobStatus": 6,
  "ProcId": 0,
  "ExitCode": 4
}
,
{
  "ClusterId": 5184666,
  "JobStatus": 42,
  "ProcId": 0,
  "ExitCode": 0
}
,
{
  "ClusterId": 5184667,
  "JobStatus": 4,
  "ProcId": 0,
  "ExitCode": 0
}
,
{
  "ClusterId": 5184668,
  "JobStatus": 4,
  "ProcId": 0,
  "ExitCode": 1
}
]
    """.strip()

    with monkeypatch.context() as m:
        mock = Mock(return_value=condor_history_output)
        m.setattr(driver.htcondor, "_condor_history", mock)
        m.setattr("os.path.exists", Mock(return_value=True))

        res = list(driver.htcondor.condor_history("blubb"))

        mock.assert_called_once_with(
            "-userlog",
            ANY,
            "-attributes",
            "ClusterId,ProcId,JobStatus,ExitCode",
            "-json",
            "-limit",
            ANY,
        )

        ref = [
            HTCondorAccountingItem(5184659, Job.Status.SUBMITTED, 0),
            HTCondorAccountingItem(5184660, Job.Status.SUBMITTED, 0),
            HTCondorAccountingItem(5184661, Job.Status.RUNNING, 0),
            HTCondorAccountingItem(5184662, Job.Status.FAILED, 0),
            HTCondorAccountingItem(5184663, Job.Status.COMPLETED, 0),
            HTCondorAccountingItem(5184664, Job.Status.FAILED, 3),
            HTCondorAccountingItem(5184665, Job.Status.FAILED, 4),
            HTCondorAccountingItem(5184666, Job.Status.UNKNOWN, 0),
            HTCondorAccountingItem(5184667, Job.Status.COMPLETED, 0),
            HTCondorAccountingItem(5184668, Job.Status.FAILED, 1),
        ]

        assert len(ref) == len(res)
        for a, b in zip(ref, res):
            assert a == b


def test_driver_create(state, monkeypatch):
    # set some config values
    data = state.config.data.copy()
    data["htcondor_driver"] = dict()
    state.config = Config(data)

    monkeypatch.setattr(HTCondorInterface, "__abstractmethods__", set())
    monkeypatch.setattr(ShellHTCondorInterface, "__init__", Mock(return_value=None))

    sif = ShellHTCondorInterface()
    sif.config = state.config.htcondor_driver

    monkeypatch.setattr("os.path.exists", Mock(return_value=True))
    monkeypatch.setattr("os.path.getsize", Mock(return_value=40*1e6))
    warning = Mock()
    monkeypatch.setattr("kong.drivers.htcondor_driver.logger.warning", warning)

    assert warning.call_count == 0

    driver = HTCondorDriver(state.config, sif)

    monkeypatch.setattr("os.path.getsize", Mock(return_value=60*1e6))
    driver = HTCondorDriver(state.config, sif)
    warning.assert_called_once()

def test_condor_submit_rm(driver, monkeypatch, state):
    condor_submit_output = """
Submitting job(s).
1 job(s) submitted to cluster {batch_job_id}.
    """.strip()

    with monkeypatch.context() as m:
        batch_job_id = 5184660
        condor_submit = Mock(
            return_value=condor_submit_output.format(batch_job_id=batch_job_id)
        )
        m.setattr(driver.htcondor, "_condor_submit", condor_submit)
        job = Job()
        job.data["batchfile"] = "somefile.sh"

        jid = driver.htcondor.condor_submit(job)
        condor_submit.assert_called_once_with("somefile.sh")
        assert jid == batch_job_id
        job.batch_job_id = jid

        condor_rm = Mock()
        m.setattr(driver.htcondor, "_condor_rm", condor_rm)
        driver.htcondor.condor_rm(job)
        condor_rm.assert_called_once_with(batch_job_id)


def test_repr():
    htai = HTCondorAccountingItem(1, Job.Status.UNKNOWN, 0)
    assert repr(htai) != ""


def test_create_job(driver, state):
    root = Folder.get_root()
    j1 = driver.create_job(
        command="sleep 1",
        folder=root,
        cores=1,
        memory=1500,
        name="job1",
        universe="amazing",
        walltime=timedelta(hours=5),
    )

    extra = 'requirements = (OpSysAndVer =?= "CentOS7")'
    driver.htcondor_config["submitfile_extra"] = extra

    assert j1.status == Job.Status.CREATED
    assert len(root.jobs) == 1 and root.jobs[0] == j1
    assert j1.batch_job_id is None
    assert os.path.exists(j1.data["log_dir"])
    assert os.path.exists(j1.data["output_dir"])
    assert os.path.exists(j1.data["jobscript"])
    assert os.path.exists(j1.data["batchfile"])
    assert is_executable(j1.data["jobscript"])

    j2 = driver.create_job(command="sleep 1", walltime="03:00:00", folder=root)
    assert j2.data["walltime"] == 3 * 60 * 60

    with open(j2.data["jobscript"]) as f:
        jobscript = f.read()
        assert str(j2.job_id) in jobscript
        assert str(j2.cores) in jobscript
        assert j2.command in jobscript
        for v in ["output_dir", "log_dir", "stdout"]:
            assert j2.data[v] in jobscript
    with open(j2.data["batchfile"]) as f:
        batchfile = f.read()
        assert str(j2.cores) in batchfile
        assert str(j2.memory) in batchfile
        for v in ["name", "htcondor_out", "universe", "walltime", "jobscript"]:
            assert str(j2.data[v]) in batchfile

    assert extra in batchfile

    with pytest.raises(ValueError):
        driver.create_job(command="sleep 1", walltime="100:00:00", folder=root)

    with pytest.raises(ValueError):
        driver.create_job(command="sleep 1", walltime=42, folder=root)


def test_submit_job(driver, state, monkeypatch):
    root = Folder.get_root()
    j1 = driver.create_job(
        command="sleep 1",
        folder=root,
        cores=1,
        name="job1",
        walltime=timedelta(hours=5),
    )

    assert j1.status == Job.Status.CREATED

    j1.status = Job.Status.SUBMITTED
    j1.save()

    with pytest.raises(InvalidJobStatus):
        driver.submit(j1)

    j1.status = Job.Status.CREATED
    j1.save()

    batch_job_id = 5_207_375

    with monkeypatch.context() as m:
        condor_submit = Mock(return_value=batch_job_id)
        m.setattr(driver.htcondor, "condor_submit", condor_submit)
        driver.submit(j1)
        condor_submit.assert_called_once_with(j1)

    assert j1.status == Job.Status.SUBMITTED
    assert j1.batch_job_id == str(batch_job_id)


def test_resubmit_job(driver, state, monkeypatch):
    root = Folder.get_root()
    j1 = driver.create_job(command="sleep 1", folder=root)

    assert j1.status == Job.Status.CREATED

    batch_job_id = 5_207_375
    condor_submit = Mock(return_value=batch_job_id)
    monkeypatch.setattr(driver.htcondor, "condor_submit", condor_submit)
    driver.submit(j1)
    condor_submit.assert_called_once_with(j1)

    assert j1.status == Job.Status.SUBMITTED
    assert j1.batch_job_id == str(batch_job_id)

    monkeypatch.setattr(driver.htcondor, "condor_q", Mock(return_value=[]))
    monkeypatch.setattr(driver.htcondor, "condor_history", Mock(return_value=[]))
    with pytest.raises(InvalidJobStatus):
        driver.resubmit(j1)

    HTAI = HTCondorAccountingItem
    monkeypatch.setattr(
        driver.htcondor,
        "condor_q",
        Mock(return_value=[HTAI(j1.batch_job_id, Job.Status.FAILED, 0)]),
    )

    bjid2 = 42
    condor_submit = Mock(return_value=bjid2)
    monkeypatch.setattr(driver.htcondor, "condor_submit", condor_submit)

    with monkeypatch.context() as m:
        # job errors on kill, resubmits anyway
        m.setattr(driver, "kill", Mock(side_effect=RuntimeError()))
        m.setattr("os.path.exists", Mock(side_effect=[True, False, False]))
        m.setattr("os.remove", Mock())
        j1 = driver.resubmit(j1)

    condor_submit.assert_called_once()
    assert j1.status == Job.Status.SUBMITTED
    assert j1.batch_job_id == str(bjid2)  # gets new batch job id

    with monkeypatch.context() as m:
        m.setattr(driver, "sync_status", Mock())  # disable sync for a second
        with pytest.raises(InvalidJobStatus):
            driver.resubmit(j1)  # stays in SUBMITTED, not accepted

    monkeypatch.setattr(
        driver.htcondor,
        "condor_history",
        Mock(return_value=[HTAI(j1.batch_job_id, Job.Status.FAILED, 0)]),
    )

    # will go to failed

    bjid3 = 99
    condor_submit = Mock(return_value=bjid3)
    monkeypatch.setattr(driver.htcondor, "condor_submit", condor_submit)
    j1 = driver.resubmit(j1)
    condor_submit.assert_called_once()
    assert j1.status == Job.Status.SUBMITTED
    assert j1.batch_job_id == str(bjid3)


def test_job_bulk_resubmit(driver, state, monkeypatch):
    root = Folder.get_root()

    jobs = [
        driver.create_job(
            command="echo 'begin'; sleep 0.2 ; echo 'end' ; exit 1", folder=root
        ),
        driver.create_job(
            command="echo 'begin'; sleep 0.2 ; echo 'end' ; exit 1", folder=root
        ),
        driver.create_job(
            command="echo 'begin'; sleep 0.2 ; echo 'end' ; exit 1", folder=root
        ),
    ]

    other_job = driver.create_job(
        command="echo 'begin'; sleep 0.2 ; echo 'end' ; exit 1", folder=root
    )
    other_job.status = Job.Status.COMPLETED
    other_job.save()

    jobs[0].status = Job.Status.FAILED
    jobs[0].save()

    condor_submit = Mock(side_effect=[1, 2, 3])
    monkeypatch.setattr(driver.htcondor, "condor_submit", condor_submit)
    driver.bulk_submit(jobs[1:])
    assert condor_submit.call_count == 2

    for job in jobs[1:]:
        job.status = Job.Status.COMPLETED

        with open(job.data["stdout"], "w") as f:
            f.write("hurz")

        job.save()

    shutil.rmtree(jobs[0].data["output_dir"])

    # we need to prevent driver from actually calling submit
    submit = Mock()
    remove = Mock(wraps=os.remove)
    makedirs = Mock()
    with monkeypatch.context() as m:
        m.setattr(driver, "submit", submit)
        m.setattr(driver.htcondor, "condor_q", Mock(return_value=[]))
        m.setattr(driver.htcondor, "condor_history", Mock(return_value=[]))
        m.setattr(driver, "bulk_kill", Mock(side_effect=RuntimeError))
        m.setattr("os.remove", remove)
        m.setattr("os.makedirs", makedirs)
        driver.bulk_resubmit(jobs)
    assert submit.call_count == len(jobs)
    remove.assert_has_calls([call(j.data["stdout"]) for j in jobs[1:]], any_order=True)
    makedirs.assert_has_calls(
        [call(j.data["output_dir"]) for j in jobs[1:]], any_order=True
    )

    for job in jobs:
        job.reload()
        assert job.status == Job.Status.CREATED

    # bug: all jobs where reset to created. Check this is not the case anymore
    other_job.reload()
    assert other_job.status != Job.Status.CREATED


def test_resubmit_bulk_invalid_status(driver, state, monkeypatch):
    monkeypatch.setattr(driver, "sync_status", Mock())
    j1 = driver.create_job(command="sleep 1", folder=state.cwd)
    monkeypatch.setattr(driver, "bulk_sync_status", Mock(return_value=[j1]))
    for status in (Job.Status.CREATED, Job.Status.SUBMITTED, Job.Status.RUNNING):
        j1.status = status
        j1.save()
        with pytest.raises(InvalidJobStatus):
            driver.bulk_resubmit([j1])


def test_job_bulk_resubmit_no_submit(driver, state, monkeypatch):
    root = Folder.get_root()

    jobs = [
        driver.create_job(
            command="echo 'begin'; sleep 0.2 ; echo 'end' ; exit 1", folder=root
        ),
        driver.create_job(
            command="echo 'begin'; sleep 0.2 ; echo 'end' ; exit 1", folder=root
        ),
        driver.create_job(
            command="echo 'begin'; sleep 0.2 ; echo 'end' ; exit 1", folder=root
        ),
    ]

    condor_submit = Mock(side_effect=[1, 2, 3])
    monkeypatch.setattr(driver.htcondor, "condor_submit", condor_submit)
    driver.bulk_submit(jobs)
    assert condor_submit.call_count == 3

    for job in jobs:
        job.status = Job.Status.COMPLETED
        job.save()

    bulk_submit = Mock()
    with monkeypatch.context() as m:
        m.setattr(driver.htcondor, "condor_q", Mock(return_value=[]))
        m.setattr(driver.htcondor, "condor_history", Mock(return_value=[]))
        m.setattr(driver, "bulk_submit", bulk_submit)
        driver.bulk_resubmit(jobs, do_submit=False)
    assert bulk_submit.call_count == 0


def test_stdout_stderr(driver, state, monkeypatch):
    root = Folder.get_root()
    j1 = driver.create_job(
        command="sleep 1",
        folder=root,
        cores=1,
        name="job1",
        walltime=timedelta(hours=5),
    )

    assert j1.status == Job.Status.CREATED

    batch_job_id = 5_207_375
    condor_submit = Mock(return_value=batch_job_id)
    monkeypatch.setattr(driver.htcondor, "condor_submit", condor_submit)
    driver.submit(j1)

    assert j1.status == Job.Status.SUBMITTED
    assert j1.batch_job_id == str(batch_job_id)

    stdout = "VALUE VALUE VALUE"

    with open(j1.data["stdout"], "w") as fh:
        fh.write(stdout)

    with driver.stdout(j1) as fh:
        assert stdout == fh.read()

    with pytest.raises(NotImplementedError):
        driver.stderr(j1)


def test_sync_status(driver, monkeypatch):
    root = Folder.get_root()
    j1 = driver.create_job(command="sleep 1", folder=root)

    # with monkeypatch.context() as m:
    #     condor_submit = Mock(return_value=1)
    #     m.setattr(driver.htcondor, "condor_submit", condor_submit)

    assert j1.status == Job.Status.CREATED

    batch_job_id = 5_207_375
    monkeypatch.setattr(
        driver.htcondor, "condor_submit", Mock(return_value=batch_job_id)
    )
    driver.submit(j1)
    assert j1.status == Job.Status.SUBMITTED
    assert j1.batch_job_id == str(batch_job_id)

    condor_q_return = [
        [HTCondorAccountingItem(batch_job_id, Job.Status.RUNNING, 0)],
        [HTCondorAccountingItem(batch_job_id, Job.Status.FAILED, 0)],
    ]
    condor_q = Mock(side_effect=condor_q_return)
    monkeypatch.setattr(driver.htcondor, "condor_q", condor_q)
    monkeypatch.setattr(driver.htcondor, "condor_history", Mock(return_value=[]))

    j1 = driver.sync_status(j1)
    assert j1.status == Job.Status.RUNNING
    j1 = driver.sync_status(j1)
    assert j1.status == Job.Status.FAILED


def test_bulk_create(driver, state):
    root = Folder.get_root()
    jobs = driver.bulk_create_jobs(
        [{"folder": root, "command": "sleep 1"} for i in range(10)]
    )
    assert len(jobs) == 10
    for job in jobs:
        assert job.status == Job.Status.CREATED


def test_bulk_submit(driver, state, monkeypatch):
    root = Folder.get_root()

    jobs = [
        driver.create_job(folder=root, command=f"sleep 0.1; echo 'JOB{i}'")
        for i in range(15)
    ]

    assert len(jobs) == 15
    for job in jobs:
        assert job.status == Job.Status.CREATED

    condor_submit = Mock(side_effect=[i + 1 for i in range(len(jobs))])

    monkeypatch.setattr(driver.htcondor, "condor_submit", condor_submit)
    driver.bulk_submit(jobs)

    assert condor_submit.call_count == len(jobs)

    for job in jobs:
        assert job.status == Job.Status.SUBMITTED
        assert str(job.job_id) == job.batch_job_id


def test_bulk_sync_status(driver, state, monkeypatch):

    root = Folder.get_root()

    jobs = [
        driver.create_job(folder=root, command=f"sleep 0.1; echo 'JOB{i}'")
        for i in range(15)
    ]

    assert len(jobs) == 15
    for job in jobs:
        assert job.status == Job.Status.CREATED

    condor_submit = Mock(side_effect=[i + 1 for i in range(len(jobs))])
    monkeypatch.setattr(driver.htcondor, "condor_submit", condor_submit)
    driver.bulk_submit(jobs)

    HTAI = HTCondorAccountingItem

    condor_q = Mock(
        return_value=[HTAI(i + 1, Job.Status.RUNNING, -1) for i in range(len(jobs))]
    )

    # pretend they're all running now

    with monkeypatch.context() as m:
        m.setattr(driver.htcondor, "condor_q", condor_q)
        m.setattr(driver.htcondor, "condor_history", Mock(return_value=[]))

        jobs = driver.bulk_sync_status(jobs)

        driver.htcondor.condor_history.assert_called_once_with(ANY)
        condor_q.assert_called_once_with()

    for job in jobs:
        assert job.status == Job.Status.RUNNING

    with monkeypatch.context() as m:
        condor_history = Mock(
            return_value=[
                HTAI(
                    i + 1,
                    Job.Status.COMPLETED if i < 6 else Job.Status.FAILED,
                    0 if i < 6 else 1,
                )
                for i in range(len(jobs))
            ]
        )
        m.setattr(driver.htcondor, "condor_history", condor_history)
        m.setattr(driver.htcondor, "condor_q", Mock(return_value=[]))

        jobs = driver.bulk_sync_status(jobs)
        condor_history.assert_called_once_with(ANY)
        driver.htcondor.condor_q.assert_called_once_with()

    for job in jobs[:6]:
        assert job.status == Job.Status.COMPLETED
    for job in jobs[6:]:
        assert job.status == Job.Status.FAILED


def test_bulk_sync_status_invalid_id(driver, state, monkeypatch):

    root = Folder.get_root()

    jobs = driver.bulk_create_jobs(
        [{"folder": root, "command": "sleep 1"} for i in range(10)]
    )

    condor_submit = Mock(side_effect=[i + 1 for i in range(len(jobs))])
    monkeypatch.setattr(driver.htcondor, "condor_submit", condor_submit)
    driver.bulk_submit(jobs)

    HTAI = HTCondorAccountingItem
    condor_q_return = [HTAI(i + 1, Job.Status.RUNNING, 0) for i in range(len(jobs))]
    condor_history_return = [HTAI(12_345_665, Job.Status.UNKNOWN, 0)]
    # pretend they're all running now
    monkeypatch.setattr(driver.htcondor, "condor_q", Mock(return_value=condor_q_return))
    monkeypatch.setattr(
        driver.htcondor, "condor_history", Mock(return_value=condor_history_return)
    )
    jobs = driver.bulk_sync_status(jobs)

    for job in jobs:
        assert job.status == Job.Status.RUNNING


def test_kill_job(driver, state, monkeypatch):
    root = Folder.get_root()
    j1 = driver.create_job(command="sleep 1", folder=root)

    driver.kill(j1)
    assert j1.status == Job.Status.FAILED

    j1.status = Job.Status.CREATED

    monkeypatch.setattr(driver.htcondor, "condor_submit", Mock(return_value=1))
    driver.submit(j1)

    assert j1.status == Job.Status.SUBMITTED

    condor_rm = Mock()
    monkeypatch.setattr(driver.htcondor, "condor_rm", condor_rm)
    driver.kill(j1)
    condor_rm.assert_called_once_with(j1)

    assert j1.status == Job.Status.FAILED

    driver.kill(j1)
    assert j1.status == Job.Status.FAILED


def test_bulk_kill(driver, state, monkeypatch):
    root = Folder.get_root()

    jobs = [
        driver.create_job(folder=root, command=f"sleep 0.1; echo 'JOB{i}'")
        for i in range(15)
    ]

    for job in jobs:
        assert job.status == Job.Status.CREATED

    condor_submit = Mock(side_effect=[i for i in range(len(jobs))])
    monkeypatch.setattr(driver.htcondor, "condor_submit", condor_submit)
    driver.bulk_submit(jobs)
    assert condor_submit.call_count == len(jobs)

    for job in jobs:
        assert job.status == Job.Status.SUBMITTED

    condor_rm = Mock()
    monkeypatch.setattr(driver.htcondor, "condor_rm", condor_rm)
    monkeypatch.setattr(driver.htcondor, "condor_q", Mock(return_value=[]))
    monkeypatch.setattr(driver.htcondor, "condor_history", Mock(return_value=[]))

    jobs = driver.bulk_kill(jobs)

    assert condor_rm.call_count == len(jobs)

    for job in jobs:
        assert job.status == Job.Status.FAILED


def test_wait(driver, state, monkeypatch):
    root = Folder.get_root()

    class HTCondorInterfaceDummy(HTCondorInterface):
        def __init__(self):
            self.max_job_id = 1
            self.id_map = {}
            self.state_idx = 0
            self.jobs = []

        def condor_q(self) -> Iterator[HTCondorAccountingItem]:
            values = [
                [
                    HTCondorAccountingItem(j.batch_job_id, Job.Status.RUNNING, 0)
                    for j in self.jobs
                ],
                [  # first half done
                    HTCondorAccountingItem(j.batch_job_id, Job.Status.COMPLETED, 0)
                    for j in self.jobs[len(self.jobs) // 2 :]
                ],
                [  # all done
                    HTCondorAccountingItem(j.batch_job_id, Job.Status.COMPLETED, 0)
                    for j in self.jobs
                ],
            ]

            v = values[self.state_idx]
            self.state_idx += 1
            return v

        def condor_history(self, log_file: str) -> Iterator[HTCondorAccountingItem]:
            return []

        def condor_submit(self, job: Job) -> int:
            self.jobs.append(job)
            self.max_job_id += 1
            job.batch_job_id = self.max_job_id
            self.id_map[job] = self.max_job_id
            return self.max_job_id

        def scancel(self, job: Job) -> None:
            pass

    hti = HTCondorInterfaceDummy()
    monkeypatch.setattr(driver, "htcondor", hti)

    jobs = [
        driver.create_job(folder=root, command=f"sleep 0.1; echo 'JOB{i}'")
        for i in range(15)
    ]

    for job in jobs:
        assert job.status == Job.Status.CREATED

    driver.bulk_submit(jobs)

    for job in jobs:
        job.reload()
        i = hti.id_map[job]
        assert job.batch_job_id == str(i)
        assert job.status == Job.Status.SUBMITTED

    with monkeypatch.context() as m:
        condor_q = Mock(wraps=hti.condor_q)
        m.setattr(hti, "condor_q", condor_q)
        driver.wait(jobs, poll_interval=0.01)
        assert condor_q.call_count == 3

    for job in jobs:
        job.reload()
        assert job.status == Job.Status.COMPLETED


def test_wait_single(driver, monkeypatch):
    root = Folder.get_root()
    j1 = driver.create_job(folder=root, command=f"sleep 0.1; echo 'JOB'")

    monkeypatch.setattr(driver, "bulk_sync_status", Mock(return_value=[j1]))

    with pytest.raises(ValueError):
        driver.wait(j1)  # job is in CREATED

    j1.status = Job.Status.COMPLETED
    driver.wait(j1)

    # timeout
    j1.status = Job.Status.RUNNING
    with pytest.raises(TimeoutError):
        driver.wait(j1, timeout=0.05, poll_interval=0.1)

    with pytest.raises(TypeError):
        driver.wait("nope")


def test_cleanup_driver(driver, state, monkeypatch):
    root = Folder.get_root()
    j1 = driver.create_job(command="sleep 1", folder=root)

    assert j1.status == Job.Status.CREATED
    assert os.path.exists(j1.data["log_dir"])
    assert os.path.exists(j1.data["output_dir"])

    # disable job updates
    monkeypatch.setattr(driver, "sync_status", Mock(side_effect=lambda j: j))

    j1.status = Job.Status.SUBMITTED
    with pytest.raises(InvalidJobStatus):
        driver.cleanup(j1)
    assert os.path.exists(j1.data["log_dir"])
    assert os.path.exists(j1.data["output_dir"])
    j1.status = Job.Status.RUNNING
    with pytest.raises(InvalidJobStatus):
        driver.cleanup(j1)
    assert os.path.exists(j1.data["log_dir"])
    assert os.path.exists(j1.data["output_dir"])

    j1.status = Job.Status.COMPLETED

    driver.cleanup(j1)
    assert not os.path.exists(j1.data["log_dir"])
    assert not os.path.exists(j1.data["output_dir"])


def test_cleanup_driver_already_deleted(driver, state, monkeypatch):
    root = Folder.get_root()
    j1 = driver.create_job(command="sleep 1", folder=root)

    assert j1.status == Job.Status.CREATED
    assert os.path.exists(j1.data["log_dir"])
    assert os.path.exists(j1.data["output_dir"])

    shutil.rmtree(j1.data["log_dir"])

    # disable job updates
    monkeypatch.setattr(driver, "sync_status", Mock(side_effect=lambda j: j))

    j1.status = Job.Status.COMPLETED

    assert not os.path.exists(j1.data["log_dir"])
    rmtree = Mock(wraps=util.rmtree)
    with monkeypatch.context() as m:
        m.setattr("kong.drivers.batch_driver_base.rmtree", rmtree)
        driver.cleanup(j1)
    rmtree.assert_has_calls([call(j1.data["output_dir"])])

    assert not os.path.exists(j1.data["log_dir"])
    assert not os.path.exists(j1.data["output_dir"])


def test_job_bulk_cleanup(driver, state, monkeypatch):
    jobs = [
        driver.create_job(command="sleep 1", folder=state.cwd),
        driver.create_job(command="sleep 1", folder=state.cwd),
        driver.create_job(command="sleep 1", folder=state.cwd),
    ]

    for job in jobs:
        assert os.path.exists(job.data["log_dir"])
        assert os.path.exists(job.data["output_dir"])

    jobs[0].status = Job.Status.RUNNING
    jobs[0].save()

    monkeypatch.setattr(driver, "bulk_sync_status", Mock(side_effect=lambda j: j))

    with pytest.raises(InvalidJobStatus):
        driver.bulk_cleanup(jobs)

    for job in jobs:
        assert os.path.exists(job.data["log_dir"])
        assert os.path.exists(job.data["output_dir"])

    shutil.rmtree(jobs[0].data["log_dir"])
    jobs[0].status = Job.Status.CREATED
    jobs[0].save()

    rmtree = Mock(side_effect=OSError)
    with monkeypatch.context() as m:
        m.setattr("kong.drivers.batch_driver_base.rmtree", rmtree)
        driver.bulk_cleanup(jobs)
    rmtree.assert_has_calls(
        [
            call(jobs[0].data["output_dir"]),
            call(jobs[1].data["log_dir"]),
            call(jobs[1].data["output_dir"]),
            call(jobs[2].data["log_dir"]),
            call(jobs[2].data["output_dir"]),
        ]
    )

    driver.bulk_cleanup(jobs)

    for job in jobs:
        assert not os.path.exists(job.data["log_dir"])
        assert not os.path.exists(job.data["output_dir"])


def test_remove_driver(driver, state, monkeypatch):
    root = Folder.get_root()
    j1 = driver.create_job(command="sleep 1", folder=root)

    # disable job updates
    monkeypatch.setattr(driver, "sync_status", Mock(side_effect=lambda j: j))
    cleanup = Mock(side_effect=lambda j: j)
    monkeypatch.setattr(driver, "cleanup", cleanup)

    j1.status = Job.Status.COMPLETED

    driver.remove(j1)

    cleanup.assert_called_once_with(j1)

    assert Job.get_or_none(job_id=j1.job_id) is None


def test_job_bulk_remove(driver, state, monkeypatch):
    jobs = [
        driver.create_job(command="sleep 1", folder=state.cwd),
        driver.create_job(command="sleep 1", folder=state.cwd),
        driver.create_job(command="sleep 1", folder=state.cwd),
    ]
    for job in jobs:
        assert os.path.exists(job.data["log_dir"]), "Does not create job directory"
        assert os.path.exists(
            job.data["output_dir"]
        ), "Does not create output directory"

    monkeypatch.setattr(driver, "bulk_sync_status", Mock(side_effect=lambda j: j))
    driver.bulk_remove(jobs)

    for job in jobs:
        assert not os.path.exists(
            job.data["log_dir"]
        ), "Driver does not cleanup job directory"
        assert not os.path.exists(
            job.data["output_dir"]
        ), "Driver does not cleanup output directory"


def test_get_htcondor_driver():
    driver_class = get_driver("kong.drivers.htcondor_driver.HTCondorDriver")
    assert driver_class == HTCondorDriver

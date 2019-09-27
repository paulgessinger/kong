import os
import shutil
from datetime import timedelta
from unittest.mock import Mock, ANY, call

import pytest

from kong import util
from kong.config import Config
from kong.drivers import InvalidJobStatus
from kong.drivers.slurm_driver import (
    SlurmInterface,
    SlurmDriver,
    SlurmAccountingItem,
    ShellSlurmInterface,
    config_schema,
)
from kong.model import Job, Folder
from kong.util import is_executable


@pytest.fixture
def driver(monkeypatch, state):
    # set some config values
    data = state.config.data.copy()
    data["slurm_driver"] = dict(
        account="pseudo_account", node_size=42, default_queue="somequeue"
    )
    state.config = Config(data)

    monkeypatch.setattr(SlurmInterface, "__abstractmethods__", set())
    monkeypatch.setattr(ShellSlurmInterface, "__init__", Mock(return_value=None))

    sif = ShellSlurmInterface()

    return SlurmDriver(state.config, sif)


def test_sacct_parse(driver, monkeypatch, state):
    sacct_output = """
5205197|FAILED|2:0
5205197.batch|FAILED|2:0
5205197.extern|COMPLETED|0:0
5205197.0|FAILED|2:0
5205206|FAILED|2:0
5205206.batch|FAILED|2:0
5205206.extern|COMPLETED|0:0
5205206.0|FAILED|2:0
5205209|FAILED|2:0
5205209.batch|FAILED|2:0
5205209.extern|COMPLETED|0:0
5205209.0|FAILED|2:0
5205223|FAILED|13:0
5205223.batch|FAILED|13:0
5205223.extern|COMPLETED|0:0
5205223.0|FAILED|13:0
5205350|FAILED|13:0
5205350.batch|FAILED|13:0
5205350.extern|COMPLETED|0:0
5205350.0|FAILED|13:0
5205355|PENDING|0:0
5205757|COMPLETED|0:0
5205757.batch|COMPLETED|0:0
5205757.extern|COMPLETED|0:0
5205757.0|COMPLETED|0:0
22822|NOCLUE|0:0
    """.strip()

    with monkeypatch.context() as m:

        mock = Mock(return_value=sacct_output.split("\n"))
        m.setattr(driver.slurm, "_sacct", mock)

        res = list(driver.slurm.sacct([]))

        mock.assert_called_once_with(
            brief=True, noheader=True, parsable2=True, starttime=ANY, _iter=True
        )

        ref = [
            SlurmAccountingItem(5_205_197, Job.Status.FAILED, 2),
            SlurmAccountingItem(5_205_206, Job.Status.FAILED, 2),
            SlurmAccountingItem(5_205_209, Job.Status.FAILED, 2),
            SlurmAccountingItem(5_205_223, Job.Status.FAILED, 13),
            SlurmAccountingItem(5_205_350, Job.Status.FAILED, 13),
            SlurmAccountingItem(5_205_355, Job.Status.SUBMITTED, 0),
            SlurmAccountingItem(5_205_757, Job.Status.COMPLETED, 0),
            SlurmAccountingItem(22822, Job.Status.UNKOWN, 0),
        ]

        assert len(ref) == len(res)
        for a, b in zip(ref, res):
            assert a == b

        batch_job_id = 5_207_375
        sbatch = Mock(return_value=f"Submitted batch job {batch_job_id}")
        m.setattr(driver.slurm, "_sbatch", sbatch)

        job = Job()
        job.data["batchfile"] = "somefile.sh"

        jid = driver.slurm.sbatch(job)

        sbatch.assert_called_once_with("somefile.sh")

        assert jid == batch_job_id
        job.batch_job_id = jid

        scancel = Mock()
        m.setattr(driver.slurm, "_scancel", scancel)
        driver.slurm.scancel(job)
        scancel.assert_called_once_with(batch_job_id)


def test_repr():
    sai = SlurmAccountingItem(1, Job.Status.UNKOWN, 0)
    assert repr(sai) != ""


def test_create_job(driver, state):
    root = Folder.get_root()
    j1 = driver.create_job(
        command="sleep 1",
        folder=root,
        cores=1,
        name="job1",
        queue="somequeue",
        walltime=timedelta(hours=5),
    )
    assert j1.status == Job.Status.CREATED
    assert len(root.jobs) == 1 and root.jobs[0] == j1
    assert j1.batch_job_id is None
    assert os.path.exists(j1.data["log_dir"])
    assert os.path.exists(j1.data["output_dir"])
    assert os.path.exists(j1.data["jobscript"])
    assert os.path.exists(j1.data["batchfile"])
    assert is_executable(j1.data["jobscript"])

    j2 = driver.create_job(command="sleep 1", walltime="03:00:00", folder=root)
    assert j2.data["walltime"] == "03:00:00"

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
        queue="somequeue",
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
        sbatch = Mock(return_value=f"Submitted batch job {batch_job_id}")
        m.setattr(driver.slurm, "_sbatch", sbatch)
        driver.submit(j1)
        sbatch.assert_called_once_with(j1.data["batchfile"])

    assert j1.status == Job.Status.SUBMITTED
    assert j1.batch_job_id == str(batch_job_id)


def test_resubmit_job(driver, state, monkeypatch):
    root = Folder.get_root()
    j1 = driver.create_job(command="sleep 1", folder=root)

    assert j1.status == Job.Status.CREATED

    batch_job_id = 5_207_375
    sbatch = Mock(return_value=batch_job_id)
    monkeypatch.setattr(driver.slurm, "sbatch", sbatch)
    driver.submit(j1)
    sbatch.assert_called_once_with(j1)

    assert j1.status == Job.Status.SUBMITTED
    assert j1.batch_job_id == str(batch_job_id)

    monkeypatch.setattr(driver.slurm, "sacct", Mock(return_value=[]))
    with pytest.raises(InvalidJobStatus):
        driver.resubmit(j1)

    SAI = SlurmAccountingItem
    monkeypatch.setattr(
        driver.slurm,
        "sacct",
        Mock(return_value=[SAI(j1.batch_job_id, Job.Status.FAILED, 0)]),
    )

    bjid2 = 42
    sbatch = Mock(return_value=bjid2)
    monkeypatch.setattr(driver.slurm, "sbatch", sbatch)

    with monkeypatch.context() as m:
        # job errors on kill, resubmits anyway
        m.setattr(driver, "kill", Mock(side_effect=RuntimeError()))
        m.setattr("os.path.exists", Mock(side_effect=[True, False, False]))
        m.setattr("os.remove", Mock())
        j1 = driver.resubmit(j1)

    sbatch.assert_called_once()
    assert j1.status == Job.Status.SUBMITTED
    assert j1.batch_job_id == str(bjid2)  # gets new batch job id

    with monkeypatch.context() as m:
        m.setattr(driver, "sync_status", Mock())  # disable sync for a second
        with pytest.raises(InvalidJobStatus):
            driver.resubmit(j1)  # stays in SUBMITTED, not accepted

    monkeypatch.setattr(
        driver.slurm,
        "sacct",
        Mock(return_value=[SAI(j1.batch_job_id, Job.Status.FAILED, 0)]),
    )

    # will go to failed

    bjid3 = 99
    sbatch = Mock(return_value=bjid3)
    monkeypatch.setattr(driver.slurm, "sbatch", sbatch)
    j1 = driver.resubmit(j1)
    sbatch.assert_called_once()
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

    jobs[0].status = Job.Status.FAILED
    jobs[0].save()

    sbatch = Mock(side_effect=[1, 2, 3])
    monkeypatch.setattr(driver.slurm, "sbatch", sbatch)
    driver.bulk_submit(jobs[1:])
    assert sbatch.call_count == 2

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
        m.setattr(driver.slurm, "sacct", Mock(return_value=[]))
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


def test_resubmit_bulk_invalid_status(driver, state, monkeypatch):
    monkeypatch.setattr(driver, "sync_status", Mock())
    j1 = driver.create_job(command="sleep 1", folder=state.cwd)
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

    sbatch = Mock(side_effect=[1, 2, 3])
    monkeypatch.setattr(driver.slurm, "sbatch", sbatch)
    driver.bulk_submit(jobs)
    assert sbatch.call_count == 3

    for job in jobs:
        job.status = Job.Status.COMPLETED
        job.save()

    bulk_submit = Mock()
    with monkeypatch.context() as m:
        m.setattr(driver.slurm, "sacct", Mock(return_value=[]))
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
        queue="somequeue",
        walltime=timedelta(hours=5),
    )

    assert j1.status == Job.Status.CREATED

    batch_job_id = 5_207_375
    sbatch = Mock(return_value=batch_job_id)
    monkeypatch.setattr(driver.slurm, "sbatch", sbatch)
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

    with monkeypatch.context() as m:
        sbatch = Mock(return_value=1)
        m.setattr(driver.slurm, "sbatch", sbatch)

    assert j1.status == Job.Status.CREATED

    batch_job_id = 5_207_375
    monkeypatch.setattr(driver.slurm, "sbatch", Mock(return_value=batch_job_id))
    driver.submit(j1)
    assert j1.status == Job.Status.SUBMITTED
    assert j1.batch_job_id == str(batch_job_id)

    sacct_return = [
        [SlurmAccountingItem(batch_job_id, Job.Status.RUNNING, 0)],
        [SlurmAccountingItem(batch_job_id, Job.Status.FAILED, 0)],
    ]
    sacct = Mock(side_effect=sacct_return)
    monkeypatch.setattr(driver.slurm, "sacct", sacct)

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

    sbatch = Mock(
        side_effect=[f"Submitted batch job {i + 1}" for i in range(len(jobs))]
    )
    monkeypatch.setattr(driver.slurm, "_sbatch", sbatch)
    driver.bulk_submit(jobs)

    assert sbatch.call_count == len(jobs)

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

    sbatch = Mock(side_effect=[i + 1 for i in range(len(jobs))])
    monkeypatch.setattr(driver.slurm, "sbatch", sbatch)
    driver.bulk_submit(jobs)

    sacct_return = ["|".join([str(i + 1), "RUNNING", "0:0"]) for i in range(len(jobs))]
    sacct = Mock(return_value=sacct_return)
    # pretend they're all running now
    monkeypatch.setattr(driver.slurm, "_sacct", sacct)

    jobs = driver.bulk_sync_status(jobs)

    sacct.assert_called_once_with(
        jobs=",".join([j.batch_job_id for j in jobs]),
        brief=True,
        noheader=True,
        parsable2=True,
        starttime=ANY,
        _iter=True,
    )

    for job in jobs:
        assert job.status == Job.Status.RUNNING

    sacct_return = [
        "|".join([str(i + 1), "COMPLETED" if i < 6 else "FAILED", "0:0"])
        for i in range(len(jobs))
    ]

    sacct = Mock(return_value=sacct_return)
    monkeypatch.setattr(driver.slurm, "_sacct", sacct)

    jobs = driver.bulk_sync_status(jobs)
    sacct.assert_called_once_with(
        jobs=",".join([j.batch_job_id for j in jobs]),
        brief=True,
        noheader=True,
        parsable2=True,
        starttime=ANY,
        _iter=True,
    )

    for job in jobs[:6]:
        assert job.status == Job.Status.COMPLETED
    for job in jobs[6:]:
        assert job.status == Job.Status.FAILED


def test_bulk_sync_status_invalid_id(driver, state, monkeypatch):

    root = Folder.get_root()

    jobs = driver.bulk_create_jobs(
        [{"folder": root, "command": "sleep 1"} for i in range(10)]
    )

    sbatch = Mock(side_effect=[i + 1 for i in range(len(jobs))])
    monkeypatch.setattr(driver.slurm, "sbatch", sbatch)
    driver.bulk_submit(jobs)

    SAI = SlurmAccountingItem
    sacct_return = [SAI(i + 1, Job.Status.RUNNING, 0) for i in range(len(jobs))]
    sacct_return += [SAI(12_345_665, Job.Status.UNKOWN, 0)]
    sacct = Mock(return_value=sacct_return)
    # pretend they're all running now
    monkeypatch.setattr(driver.slurm, "sacct", sacct)
    jobs = driver.bulk_sync_status(jobs)

    for job in jobs:
        assert job.status == Job.Status.RUNNING


def test_kill_job(driver, state, monkeypatch):
    root = Folder.get_root()
    j1 = driver.create_job(command="sleep 1", folder=root)

    driver.kill(j1)
    assert j1.status == Job.Status.FAILED

    j1.status = Job.Status.CREATED

    monkeypatch.setattr(driver.slurm, "sbatch", Mock(return_value=1))
    driver.submit(j1)

    assert j1.status == Job.Status.SUBMITTED

    scancel = Mock()
    monkeypatch.setattr(driver.slurm, "_scancel", scancel)
    driver.kill(j1)
    scancel.assert_called_once_with(j1.batch_job_id)

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

    sbatch = Mock(side_effect=[i for i in range(len(jobs))])
    monkeypatch.setattr(driver.slurm, "sbatch", sbatch)
    driver.bulk_submit(jobs)
    assert sbatch.call_count == len(jobs)

    for job in jobs:
        assert job.status == Job.Status.SUBMITTED

    scancel = Mock()
    monkeypatch.setattr(driver.slurm, "scancel", scancel)
    monkeypatch.setattr(driver.slurm, "sacct", Mock(return_value=[]))

    jobs = driver.bulk_kill(jobs)

    assert scancel.call_count == len(jobs)

    for job in jobs:
        assert job.status == Job.Status.FAILED


def test_wait(driver, state):
    root = Folder.get_root()

    jobs = [
        driver.create_job(folder=root, command=f"sleep 0.1; echo 'JOB{i}'")
        for i in range(15)
    ]

    for job in jobs:
        assert job.status == Job.Status.CREATED

    with pytest.raises(NotImplementedError):
        driver.wait(jobs)


def test_config_schema():
    assert not config_schema.is_valid(dict())
    assert config_schema.is_valid(
        dict(account="bla", node_size=80, default_queue="whatever")
    )
    assert not config_schema.is_valid(dict(account="bla", node_size="blub"))
    assert not config_schema.is_valid(dict(account=42))


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
        m.setattr("kong.drivers.slurm_driver.rmtree", rmtree)
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
        m.setattr("kong.drivers.slurm_driver.rmtree", rmtree)
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


def test_job_bulk_remove(driver, state):
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

    driver.bulk_remove(jobs)

    for job in jobs:
        assert not os.path.exists(
            job.data["log_dir"]
        ), "Driver does not cleanup job directory"
        assert not os.path.exists(
            job.data["output_dir"]
        ), "Driver does not cleanup output directory"

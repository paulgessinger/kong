from contextlib import contextmanager
from io import StringIO
from unittest.mock import Mock, ANY, MagicMock

import pytest

from kong import config
from kong.drivers.local_driver import LocalDriver
import kong.drivers
from kong.drivers import DriverMismatch
from kong.model import Job, Folder
import peewee as pw


@pytest.fixture
def job(tree):
    return Job.create(
        batch_job_id=42, folder=tree, command="sleep 1", driver=LocalDriver
    )


def test_create(tree):
    j1 = Job.create(batch_job_id=42, folder=tree, command="sleep 2", driver=LocalDriver)
    assert j1.job_id is not None
    job = Job.get(batch_job_id=42)
    assert job == j1
    assert len(tree.jobs) == 1
    assert tree.jobs[0] == j1
    assert j1.folder == tree
    assert j1.command == "sleep 2"

    with pytest.raises(pw.IntegrityError):
        Job.create(batch_job_id=42, command="sleep 4", folder=tree, driver=LocalDriver)
    assert (
        Job.create(batch_job_id=43, folder=tree, command="sleep 4", driver=LocalDriver)
        is not None
    )

    f2 = tree.subfolder("f2")
    j2 = Job.create(batch_job_id=44, folder=f2, command="sleep 4", driver=LocalDriver)
    assert j2 is not None
    assert j2.command == "sleep 4"
    assert len(f2.jobs) == 1
    assert f2.jobs[0] == j2
    assert j2.folder == f2


def test_properties(tree):
    j1 = Job.create(batch_job_id=42, folder=tree, command="a", driver=LocalDriver)
    j1.data["log_dir"] = "LOGDIR_PATH"
    j1.data["output_dir"] = "OUTPUT_PATH"
    j1.save()
    assert j1.log_dir == "LOGDIR_PATH"
    assert j1.output_dir == "OUTPUT_PATH"


def test_set_driver(state, monkeypatch):
    j1 = Job.create(
        batch_job_id=42, folder=state.cwd, command="sleep 2", driver=LocalDriver
    )

    driver = LocalDriver(state.config)

    class DummyDriver:
        pass

    j1.ensure_driver_instance(driver)
    assert j1.driver_instance == driver

    # forcibly unset driver instance
    j1._driver_instance = None
    j1.ensure_driver_instance(state.config)
    assert isinstance(j1.driver_instance, LocalDriver)

    j1._driver_instance = None

    with pytest.raises(DriverMismatch):
        j1.ensure_driver_instance(DummyDriver())
    assert j1._driver_instance is None

    # bypass interface
    monkeypatch.setattr(
        "kong.drivers.driver_base.DriverBase.__abstractmethods__", set()
    )

    class ValidDriver(kong.drivers.driver_base.DriverBase):
        def __init__(self):
            pass

    with pytest.raises(DriverMismatch):
        j1.ensure_driver_instance(ValidDriver())
    assert j1._driver_instance is None

    j2 = Job.create(
        batch_job_id=42, folder=state.cwd, command="sleep 2", driver=ValidDriver
    )

    valid_driver = ValidDriver()
    j2.ensure_driver_instance(valid_driver)
    assert j2.driver_instance == valid_driver

    j2.ensure_driver_instance(driver)  # nothing happens
    assert not isinstance(j2.driver_instance, LocalDriver)


def test_rm(tree, state):
    root = tree

    j1 = Job.create(folder=tree, command="sleep 1", driver=LocalDriver)

    class PseudoDriver(LocalDriver):
        pass

    pseudo_driver = PseudoDriver(state.config)
    pseudo_driver.cleanup = Mock()
    j1._driver_instance = pseudo_driver

    assert j1.folder == root
    assert len(root.jobs) == 1 and root.jobs[0] == j1
    j1.remove()
    root = Folder.get_root()
    assert len(root.jobs) == 0

    pseudo_driver.cleanup.assert_called_once_with(j1)


def test_resubmit(job):
    driver = Mock()
    driver.resubmit = Mock()
    job._driver_instance = driver

    job.resubmit()
    driver.resubmit.assert_called_once_with(job)


def test_submit(job):
    driver = Mock()
    driver.submit = Mock()
    job._driver_instance = driver

    job.submit()
    driver.submit.assert_called_once_with(job)


def test_wait(job):
    driver = Mock()
    driver.wait = Mock()
    job._driver_instance = driver

    job.wait()
    driver.wait.assert_called_once_with(job, timeout=ANY)


def test_get_status(job):
    driver = Mock()
    driver.sync_status = Mock()
    job._driver_instance = driver

    job.get_status()
    driver.sync_status.assert_called_once_with(job)


def test_stdout_stderr(job):
    stdout = StringIO("TESTVALUE")
    stderr = StringIO("2TESTVALUE2")

    @contextmanager
    def get_stdout(self):
        yield stdout

    @contextmanager
    def get_stderr(self):
        yield stderr

    driver = Mock(stdout=get_stdout, stderr=get_stderr)
    job._driver_instance = driver

    with job.stdout() as f:
        assert f.read() == "TESTVALUE"

    with job.stderr() as f:
        assert f.read() == "2TESTVALUE2"


def test_kill(job):
    driver = Mock()
    driver.kill = Mock()
    job._driver_instance = driver

    job.kill()
    driver.kill.assert_called_once_with(job)


def test_bulk_select(state, monkeypatch):
    jobs = [state.create_job(command="sleep 1") for _ in range(50)]
    ids = [j.job_id for j in jobs]

    # make sure it's actually batched
    with monkeypatch.context() as m:
        execute = Mock(return_value=[])
        m.setattr("peewee.BaseQuery.execute", execute)
        list(Job.bulk_select(Job.job_id, ids, batch_size=10))
        assert execute.call_count == 5

    selected = list(Job.bulk_select(Job.job_id, ids, batch_size=10))
    assert len(selected) == len(jobs)
    for exp, act in zip(jobs, selected):
        assert exp.job_id == act.job_id

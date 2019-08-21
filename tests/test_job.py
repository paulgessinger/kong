from unittest.mock import Mock

import pytest

from kong import config
from kong.drivers import LocalDriver
import kong.drivers
from kong.drivers import DriverMismatch
from kong.model import Job
import peewee as pw


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


def test_set_driver(state, monkeypatch):
    j1 = Job.create(
        batch_job_id=42, folder=state.cwd, command="sleep 2", driver=LocalDriver
    )

    driver = LocalDriver(state.config)

    class DummyDriver:
        pass

    j1.ensure_driver_instance(driver)
    assert j1.driver_instance == driver

    # forcibly unset driver instanc
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


def test_rm(tree):
    root = tree

    j1 = Job.create(folder=tree, command="sleep 1", driver=LocalDriver)

    pseudo_driver = Mock()
    pseudo_driver.cleanup = Mock()
    j1._driver_instance = pseudo_driver

    assert j1.folder == root
    assert len(root.jobs) == 1 and root.jobs[0] == j1
    j1.delete_instance()
    assert len(root.jobs) == 0

    pseudo_driver.cleanup.assert_called_once_with(j1)

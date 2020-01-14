from unittest.mock import Mock

import pytest
import inspect

from kong.drivers import DriverMismatch
from kong.drivers.driver_base import DriverBase, checked_job
from kong.model import Job


@pytest.fixture
def noabc(monkeypatch):
    monkeypatch.setattr(
        "kong.drivers.driver_base.DriverBase.__abstractmethods__", set()
    )


@pytest.fixture
def driver(noabc, state):
    return DriverBase(state.config)


def test_driver_base_all_methods(driver):
    methods = inspect.getmembers(driver, predicate=inspect.ismethod)

    excl = ["__init__", "_check_driver"]

    for name, method in methods:
        if not hasattr(method, "__isabstractmethod__"):
            continue

        if not method.__isabstractmethod__:
            continue

        sig = inspect.signature(method)

        args = len(sig.parameters) * [None]

        try:
            method(*args)
        except NotImplementedError:
            pass


def test_init(noabc, state):
    DriverBase(None)
    DriverBase(state.config)


def test_check_driver(driver):
    job = Mock()

    class NotADriver:
        pass

    job.driver = NotADriver
    with pytest.raises(DriverMismatch):
        driver._check_driver(job)

    job.driver = DriverBase
    driver._check_driver(job)


def test_checked_job(driver):

    inner = Mock()
    wrapped = checked_job(inner)

    job = Mock()

    class NotADriver:
        pass

    job.driver = NotADriver
    with pytest.raises(DriverMismatch):
        wrapped(driver, job)
    assert inner.call_count == 0

    inner.reset_mock()

    job.driver = DriverBase
    wrapped(driver, job)
    inner.assert_called_once_with(driver, job)


def test_wait(driver, monkeypatch):
    wait_gen = Mock(return_value=iter([]))
    monkeypatch.setattr(driver, "wait_gen", wait_gen)

    driver.wait("*", progress=True)
    assert wait_gen.call_count == 1

    wait_gen.reset_mock()

    driver.wait("*", progress=False)
    assert wait_gen.call_count == 1


def test_make_log_path(driver, monkeypatch):
    monkeypatch.setitem(driver.config.data, "jobdir", "JOB_BASE")

    job = Mock()

    job.job_id = 123456
    assert driver.make_log_path(job).endswith("JOB_BASE/12/34/123456")

    job.job_id = 994784367
    assert driver.make_log_path(job).endswith("JOB_BASE/99/47/994784367")

    job.job_id = 456
    assert driver.make_log_path(job).endswith("JOB_BASE/00/04/000456")

    job.job_id = 3854
    assert driver.make_log_path(job).endswith("JOB_BASE/00/38/003854")


def test_make_output_path(driver, monkeypatch):
    monkeypatch.setitem(driver.config.data, "joboutputdir", "JOB_BASE")

    job = Mock()

    job.job_id = 123456
    assert driver.make_output_path(job).endswith("JOB_BASE/12/34/123456")

    job.job_id = 994784367
    assert driver.make_output_path(job).endswith("JOB_BASE/99/47/994784367")

    job.job_id = 456
    assert driver.make_output_path(job).endswith("JOB_BASE/00/04/000456")

    job.job_id = 3854
    assert driver.make_output_path(job).endswith("JOB_BASE/00/38/003854")

from unittest.mock import Mock

import pytest
import inspect

from kong.drivers import DriverMismatch
from kong.drivers.driver_base import DriverBase, checked_job


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
        if name in excl:
            continue

        sig = inspect.signature(method)

        args = len(sig.parameters) * [None]

        with pytest.raises(NotImplementedError):
            method(*args)


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


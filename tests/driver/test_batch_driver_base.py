from concurrent.futures import Executor, Future
from unittest.mock import Mock

import pytest
from typing import Optional

from kong.driver.batch_driver_base import BatchDriverBase
from kong.model.job import Job


@pytest.fixture
def noabc(monkeypatch):
    monkeypatch.setattr(
        "kong.driver.batch_driver_base.BatchDriverBase.__abstractmethods__", set()
    )


@pytest.fixture
def driver(noabc, state):
    return BatchDriverBase(state.config)


def test_bulk_cleanup(state, driver, monkeypatch):
    jobs = [state.create_job(command="sleep 1") for _ in range(3)]

    for j in jobs:
        j.driver = BatchDriverBase

    sync = Mock(side_effect=lambda j: j)
    monkeypatch.setattr(
        "kong.driver.batch_driver_base.BatchDriverBase.bulk_sync_status", sync
    )

    with monkeypatch.context() as m:
        rmtree = Mock()
        m.setattr("kong.driver.batch_driver_base.rmtree", rmtree)
        driver.bulk_cleanup(jobs)
        assert rmtree.call_count == 3 * 2

    class PseudoFuture(Future):
        def __init__(self, fn, *args, **kwargs):
            self.fn = fn
            self.args = args
            self.kwargs = kwargs

            super().__init__()

            with self._condition:
                self._state = "FINISHED"
                for waiter in self._waiters:
                    waiter.add_result(self)
                self._condition.notify_all()
            self._invoke_callbacks()

        def result(self, timeout=None):
            return self.fn(*self.args, **self.kwargs)

    class DelayedExecutor(Executor):
        def submit(self, fn, *args, **kwargs):
            return PseudoFuture(fn, *args, **kwargs)

    ex = DelayedExecutor()

    with monkeypatch.context() as m:
        rmtree = Mock()
        m.setattr("kong.driver.batch_driver_base.rmtree", rmtree)
        it = driver.bulk_cleanup(jobs, progress=True, ex=ex)
        assert rmtree.call_count == 0
        next(it)
        assert rmtree.call_count == 2
        next(it)
        assert rmtree.call_count == 4
        next(it)
        assert rmtree.call_count == 6


def test_bulk_remove(state, driver, monkeypatch):
    jobs = [state.create_job(command="sleep 1") for _ in range(3)]
    for j in jobs:
        j.driver = BatchDriverBase

    sync = Mock(side_effect=lambda j: j)
    monkeypatch.setattr(
        "kong.driver.batch_driver_base.BatchDriverBase.bulk_sync_status", sync
    )

    with monkeypatch.context() as m:
        monkeypatch.setattr(driver, "bulk_cleanup", Mock(side_effect=lambda j: j))
        assert len(list(Job.select())) == 3
        driver.bulk_remove(jobs, do_cleanup=True)
        assert driver.bulk_cleanup.call_count == 1
        assert len(list(Job.select())) == 0

    jobs = [state.create_job(command="sleep 1") for _ in range(3)]
    for j in jobs:
        j.driver = BatchDriverBase

    with monkeypatch.context() as m:
        monkeypatch.setattr(driver, "bulk_cleanup", Mock(side_effect=lambda j: j))
        assert len(list(Job.select())) == 3
        driver.bulk_remove(jobs, do_cleanup=False)
        assert driver.bulk_cleanup.call_count == 0
        assert len(list(Job.select())) == 0

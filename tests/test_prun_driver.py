import sys
import os
from unittest.mock import Mock, call

import pytest

from kong.config import Config
from kong.drivers.prun_driver import PrunDriver
from kong.model.folder import Folder
from kong.model.job import Job


@pytest.fixture
def driver(monkeypatch, state, pandatools):
    # set some config values
    data = state.config.data.copy()
    data["prun_driver"] = dict(
        PANDA_PYTHONPATH="PANDA_PYTHONPATH_VALUE",
        emi_path="emi_path_value",
        PATHENA_GRID_SETUP_SH="PATHENA_GRID_SETUP_SH_VALUE",
    )
    state.config = Config(data)

    with monkeypatch.context() as m:
        paths = []
        m.setattr("sys.path", paths)
        env = {}
        m.setattr("os.environ", env)
        driver = PrunDriver(state.config)
        assert paths == ["PANDA_PYTHONPATH_VALUE"]
        assert "emi_path_value" in env["PATH"]
        assert env["PATHENA_GRID_SETUP_SH"] == "PATHENA_GRID_SETUP_SH_VALUE"

    assert driver._pandatools == pandatools
    return driver


@pytest.fixture
def pandatools(monkeypatch):
    pandatools = Mock()
    pandatools.queryPandaMonUtils = Mock()
    monkeypatch.setitem(sys.modules, "pandatools", pandatools)

    PsubUtils = Mock()
    PsubUtils.commands_get_status_output = Mock()
    monkeypatch.setitem(sys.modules, "pandatools.PsubUtils", PsubUtils)

    MiscUtils = Mock()
    MiscUtils.commands_get_status_output = Mock()
    monkeypatch.setitem(sys.modules, "pandatools.MiscUtils", MiscUtils)

    return pandatools


def test_bulk_sync_status(driver, pandatools):
    root = Folder.get_root()

    task_ids = [21948780, 21948716, 21956507, 21953913, 21962217]

    jobs = [
        driver.create_job(folder=root, command="sleep 10", task_id=i) for i in task_ids
    ]

    from panda_query_return1 import result

    query = Mock(return_value=result)
    pandatools.queryPandaMonUtils.query_tasks = query

    jobs = driver.bulk_sync_status(jobs)

    assert query.call_count == 1
    query.assert_called_once_with(jeditaskid="|".join(map(str, task_ids)))

    assert [j.status for j in jobs] == [
        Job.Status.COMPLETED,
        Job.Status.COMPLETED,
        Job.Status.FAILED,
        Job.Status.FAILED,
        Job.Status.SUBMITTED,
    ]

def test_sync_status(driver, pandatools):
    root = Folder.get_root()

    task_ids = [21948780, 21948716, 21956507, 21953913, 21962217]

    jobs = [
        driver.create_job(folder=root, command="sleep 10", task_id=i) for i in task_ids
    ]

    from panda_query_return1 import result

    ts, url, items = result

    data = {item["jeditaskid"]: item for item in items}

    def get_data(jeditaskid):
        tids = jeditaskid.split("|")
        return ts, url, [data[int(tid)] for tid in tids]

    query = Mock(side_effect=get_data)
    pandatools.queryPandaMonUtils.query_tasks = query

    jobs = [driver.sync_status(j) for j in jobs]

    assert query.call_count == len(jobs)
    query.assert_has_calls([call(jeditaskid=str(tid)) for tid in task_ids])

    assert [j.status for j in jobs] == [
        Job.Status.COMPLETED,
        Job.Status.COMPLETED,
        Job.Status.FAILED,
        Job.Status.FAILED,
        Job.Status.SUBMITTED,
    ]

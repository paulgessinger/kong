import os

import pytest
from unittest.mock import Mock

from kong.drivers import LocalDriver
import kong


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


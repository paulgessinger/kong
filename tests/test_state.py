import os
import time

import pytest
from unittest.mock import Mock
import peewee as pw

import kong
from kong.drivers import LocalDriver
from kong.model import Folder, Job
import kong.drivers
from kong.state import DoesNotExist


@pytest.fixture
def cfg(app_env, monkeypatch):
    app_dir, config_path, tmp_path = app_env
    with monkeypatch.context() as m:
        m.setattr(
            "click.prompt",
            Mock(
                side_effect=[
                    "LocalDriver",
                    os.path.join(app_dir, "joblog"),
                    os.path.join(app_dir, "joboutput"),
                ]
            ),
        )
        kong.setup.setup(None)
    _cfg = kong.config.Config()
    return _cfg


def test_init(cfg, db):
    # this requires a database to be created externally
    s = kong.state.State(cfg, kong.model.Folder.get_root())
    assert s is not None


def test_get_instance(cfg, monkeypatch):
    orig_init = kong.db.database.init
    init = Mock(side_effect=lambda _: orig_init(":memory:"))
    monkeypatch.setattr("kong.db.database.init", init)
    s = kong.state.State.get_instance()
    assert s is not None
    init.assert_called_once()
    assert s.cwd == kong.model.Folder.get_root()


def test_module_get_instance(cfg, monkeypatch):
    orig_init = kong.db.database.init
    init = Mock(side_effect=lambda _: orig_init(":memory:"))
    monkeypatch.setattr("kong.db.database.init", init)
    s = kong.get_instance()
    assert s is not None
    init.assert_called_once()
    assert s.cwd == kong.model.Folder.get_root()


def test_ls(tree, state, sample_jobs):
    root = tree

    folders, jobs = state.ls(".")
    assert all(a == b for a, b in zip(folders, root.children))
    assert all(a == b for a, b in zip(jobs, root.jobs))

    with pytest.raises(pw.DoesNotExist):
        state.ls("/nope")

    f2 = Folder.find_by_path(state.cwd, "/f2")
    state.cwd = f2
    folders, jobs = state.ls(".")
    assert all(a == b for a, b in zip(folders, f2.children))
    assert all(a == b for a, b in zip(jobs, f2.jobs))


def test_ls_refresh(tree, state, sample_jobs):
    _, jobs = state.ls(".")
    for job in jobs:
        assert job.status == Job.Status.CREATED

    for job in sample_jobs:
        job.submit()

    time.sleep(0.2)

    # without refresh
    _, jobs = state.ls(".")
    for job in jobs:
        # status is the same
        assert job.status == Job.Status.SUBMITTED

    # with refresh
    _, jobs = state.ls(".", refresh=True)
    for job in jobs:
        # status is the changes
        assert job.status == Job.Status.COMPLETED


def test_cd(state):
    root = Folder.get_root()
    assert state.cwd == root

    with pytest.raises(pw.DoesNotExist):
        state.cd("nope")
    assert state.cwd == root

    nope = root.add_folder("nope")

    state.cd("nope")
    assert state.cwd == nope

    state.cd("")
    assert state.cwd == root

    with pytest.raises(pw.DoesNotExist):
        state.cd("..")
    assert state.cwd == root

    with pytest.raises(pw.DoesNotExist):
        state.cd("../nope")
    assert state.cwd == root

    more = root.add_folder("more")
    another = nope.add_folder("another")

    state.cd("/nope")
    assert state.cwd == nope

    state.cd("/nope/another")
    assert state.cwd == another

    with pytest.raises(pw.DoesNotExist):
        state.cd("/../")
    assert state.cwd == another

    state.cd("..")
    assert state.cwd == nope

    state.cd("/more")
    assert state.cwd == more


def test_mkdir(state, db):
    root = Folder.get_root()
    sub = root.add_folder("sub")
    for cwd in [root, sub]:
        state.cwd = cwd

        assert cwd.subfolder("alpha") is None
        state.mkdir("alpha")
        alpha = cwd.subfolder("alpha")
        assert alpha is not None

        # one down
        assert alpha.subfolder("beta") is None
        state.mkdir("alpha/beta")
        beta = alpha.subfolder("beta")
        assert beta is not None

        # cannot create outside of root
        if cwd == root:
            with pytest.raises(kong.state.CannotCreateError):
                state.mkdir("../nope")
        else:
            state.mkdir("../nope")
            assert root.subfolder("nope") is not None

        # cannot create again
        with pytest.raises(pw.IntegrityError):
            state.mkdir("alpha")

        # cannot create in nonexistant
        with pytest.raises(kong.state.CannotCreateError):
            state.mkdir("omega/game")

        state.cwd = beta
        assert cwd.subfolder("gamma") is None
        state.mkdir("../../gamma")
        gamma = cwd.subfolder("gamma")
        assert gamma is not None


def test_rm_folder(state, db):
    root = Folder.get_root()

    with pytest.raises(DoesNotExist):
        state.rm("../nope")

    with pytest.raises(kong.state.CannotRemoveRoot):
        state.rm("/")

    root.add_folder("alpha")
    confirm = Mock(return_value=False)
    state.rm("alpha", confirm)
    confirm.assert_called_once()

    assert root.subfolder("alpha") is not None
    confirm = Mock(return_value=True)
    state.rm("alpha", confirm)
    confirm.assert_called_once()
    assert root.subfolder("alpha") is None

    # works further down
    beta = root.add_folder("beta")
    gamma = beta.add_folder("gamma")
    assert beta.subfolder("gamma") is not None
    state.rm("/beta/gamma")
    assert beta.subfolder("gamma") is None

    # should also work with instance
    assert root.subfolder("beta") is not None
    state.rm(beta)
    assert root.subfolder("beta") is None


def test_rm_job(state, db):
    root = Folder.get_root()
    j1 = state.create_job(command="sleep 1")
    assert len(root.jobs) == 1 and root.jobs[0] == j1
    assert Job.get_or_none(job_id=j1.job_id) is not None
    state.rm(str(j1.job_id))
    assert len(root.jobs) == 0
    assert Job.get_or_none(job_id=j1.job_id) is None

    # should also work with instance
    j2 = state.create_job(command="sleep 1")
    assert len(root.jobs) == 1 and root.jobs[0] == j2
    assert Job.get_or_none(job_id=j2.job_id) is not None
    state.rm(j2)
    assert len(root.jobs) == 0
    assert Job.get_or_none(job_id=j2.job_id) is None

    # works in other cwd too
    alpha = root.add_folder("alpha")
    j3 = state.default_driver.create_job(command="sleep 1", folder=alpha)
    assert len(alpha.jobs) == 1 and alpha.jobs[0] == j3
    assert Job.get_or_none(job_id=j3.job_id) is not None
    state.rm(str(j3.job_id))
    assert len(alpha.jobs) == 0
    assert Job.get_or_none(job_id=j3.job_id) is None


def test_get_driver(state, db):
    driver = state.default_driver
    assert isinstance(driver, LocalDriver)


def test_create_job(state, db):
    root = Folder.get_root()
    j1 = state.create_job(command="sleep 1")
    assert j1.folder == root
    assert len(root.jobs) == 1 and root.jobs[0] == j1

    f2 = root.add_folder("f2")
    state.cd("f2")
    j2 = state.create_job(command="sleep 1")
    assert j2.folder == f2
    assert len(f2.jobs) == 1 and f2.jobs[0] == j2


def test_get_job(state):
    root = Folder.get_root()
    j1 = state.create_job(command="sleep 1")

    f2 = root.add_folder("f2")
    state.cd("f2")
    j2 = state.create_job(command="sleep 1")

    assert state.get_job(f"{j1.job_id}") == j1
    assert state.get_job(f"f2/{j2.job_id}") == j2
    assert state.get_job("42") is None


def test_run_job(state, db):
    root = Folder.get_root()

    j1 = state.create_job(command="sleep 1")
    assert j1.status == Job.Status.CREATED
    state.submit_job(j1.job_id)
    j1.reload()
    assert j1.status == Job.Status.SUBMITTED

    root.add_folder("f1")
    state.cd("f1")
    j3 = state.create_job(command="sleep 1")
    j4 = state.create_job(command="sleep 1")
    assert j3.status == Job.Status.CREATED
    state.cd("..")
    assert state.cwd == root
    state.submit_job(j3.job_id)
    j3.reload()
    assert j3.status == Job.Status.SUBMITTED

    assert j4.status == Job.Status.CREATED
    assert state.cwd == root
    state.submit_job(f"f1/{j4.job_id}")
    j4.reload()
    assert j4.status == Job.Status.SUBMITTED

    with pytest.raises(DoesNotExist):
        state.submit_job("4242")


def test_kill_job(state):
    root = Folder.get_root()
    j1 = state.create_job(command="sleep 1")
    j1.submit()
    assert j1.status == Job.Status.SUBMITTED
    time.sleep(0.1)
    assert j1.get_status() == Job.Status.RUNNING
    state.kill_job(j1.job_id)
    assert j1.get_status() == Job.Status.FAILED


def test_job_resubmit(state):
    root = Folder.get_root()
    j1 = state.create_job(command="sleep 1")
    j1.submit()
    assert j1.status == Job.Status.SUBMITTED
    time.sleep(0.1)
    assert j1.get_status() == Job.Status.RUNNING
    state.kill_job(j1.job_id)
    assert j1.get_status() == Job.Status.FAILED

    state.resubmit_job(str(j1.job_id))
    j1.reload()
    assert j1.status == Job.Status.SUBMITTED

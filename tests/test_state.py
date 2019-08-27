import os
import time

import pytest
from unittest.mock import Mock
import peewee as pw

import kong
from kong.drivers.local_driver import LocalDriver
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
                    "kong.drivers.local_driver.LocalDriver",
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

    state.cd(root)
    assert state.cwd == root

    state.cd(more)
    assert state.cwd == more


def test_mv_folder(state, db):
    root = Folder.get_root()

    f1, f2, f3, f4, f5 = [root.add_folder(n) for n in ("f1", "f2", "f3", "f4", "f5")]

    assert len(root.children) == 5

    # actual move
    state.mv("f1", f2)
    assert len(root.children) == 4
    assert len(f2.children) == 1 and f2.children[0] == f1
    f1.reload()
    assert f1.parent == f2

    # rename f3 -> f3x
    state.mv(f3, "f3x")
    assert len(root.children) == 4
    assert f3.name == "f3x"

    # another move
    state.mv(f3, "f4")
    assert len(f4.children) == 1 and f4.children[0] == f3
    assert f3.parent == f4
    assert f3.name == "f3x"

    # move rename at the same time
    state.cd("f2")
    state.mv(f5, "../f4/f5x")
    assert len(f4.children) == 2
    assert f5.name == "f5x"
    assert f5.parent == f4

    # try move to nonexistant
    state.cd("/")
    with pytest.raises(ValueError):
        state.mv(f1, "/nope/blub")

    # try to move nonexistant
    with pytest.raises(DoesNotExist):
        state.mv("../nope", f1)


def test_mv_job(state, db):
    root = Folder.get_root()

    f1, f2 = [root.add_folder(n) for n in ("f1", "f2")]

    assert len(root.children) == 2

    j1, j2, j3, j4, j5 = [state.create_job(command="sleep 1") for _ in range(5)]
    assert len(root.jobs) == 5

    state.mv(j1, "f1")
    j1.reload()
    assert j1.folder == f1
    assert len(f1.jobs) == 1
    assert len(root.jobs) == 4

    state.mv(str(j2.job_id), f2)
    j2.reload()
    assert j2.folder == f2
    assert len(f2.jobs) == 1
    assert len(root.jobs) == 3

    state.cd(f2)
    state.mv(j3, ".")
    j3.reload()
    assert j3.folder == f2

    state.mv(j2, "..")
    j2.reload()
    assert j2.folder == root

    state.mv(f"../{j4.job_id}", "../f1")
    j4.reload()
    assert j4.folder == f1

    state.cd(root)

    # renaming does not work
    with pytest.raises(ValueError):
        state.mv(f"{j5.job_id}", "42")


def test_mv_bulk_job(state):
    root = Folder.get_root()

    f1, f2, f3 = [root.add_folder(n) for n in ("f1", "f2", "f3")]
    assert len(root.children) == 3

    state.cwd = f1
    j1, j2, j3, j4, j5 = [state.create_job(command="sleep 1") for _ in range(5)]
    assert len(f1.jobs) == 5

    state.cwd = root

    state.mv("f1/*", f3)
    assert len(f1.jobs) == 0 and len(f3.jobs) == 5
    assert len(root.children) == 3
    for j in (j1, j2, j3, j4, j5):
        j.reload()
        assert j.folder == f3

    state.cwd = f2
    state.mv("../f3/*", ".")
    assert len(f3.jobs) == 0 and len(f2.jobs) == 5
    for j in (j1, j2, j3, j4, j5):
        j.reload()
        assert j.folder == f2


def test_mv_bulk_both(state):
    root = Folder.get_root()
    f1, f2 = [root.add_folder(n) for n in ("f1", "f2")]
    f3 = f1.add_folder("f3")

    with state.pushd(f1):
        j1 = state.create_job(command="sleep 1")

    state.mv("f1/*", "f2")
    j1.reload()
    assert j1.folder == f2
    f3.reload()
    assert f3.parent == f2

    f3.parent = root
    f3.save()
    j1.folder = root
    j1.save()

    # attempt to move all in root to f2. this will fail for f2, but only for f2
    state.mv("*", "f2")
    for o in (f1, f2, f3, j1):
        o.reload()
    assert f1.parent == f2
    assert f2.parent == root
    assert f3.parent == f2
    assert j1.folder == f2


def test_cwd_context_manager(state):
    root = Folder.get_root()
    f1 = root.add_folder("f1")
    assert state.cwd == root

    with state.pushd(f1):
        assert state.cwd == f1
    assert state.cwd == root

    with state.pushd("f1"):
        assert state.cwd == f1
    assert state.cwd == root


def test_mv_bulk_folder(state):
    root = Folder.get_root()

    r1, r2 = [root.add_folder(n) for n in ("r1", "r2")]

    folders = [r1.add_folder(f"f{n}") for n in range(5)]
    assert len(r1.children) == len(folders)
    assert len(r2.children) == 0

    state.mv("r1/*", "r2")
    assert len(r1.children) == 0
    assert len(r2.children) == len(folders)
    for f in folders:
        f.reload()
        assert f.parent == r2

    state.cwd = r1
    state.mv("../r2/*", ".")
    assert len(r1.children) == len(folders)
    assert len(r2.children) == 0
    for f in folders:
        f.reload()
        assert f.parent == r1


def test_get_folders(state):
    root = Folder.get_root()

    folders = [root.add_folder(f"f{n}") for n in range(10)]

    globbed = state.get_folders("*")

    assert len(globbed) == len(folders)
    for a, b in zip(globbed, folders):
        assert a == b


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


def test_get_jobs(state):
    root = Folder.get_root()
    j1 = state.create_job(command="sleep 1")

    f2 = root.add_folder("f2")
    state.cd("f2")
    j2 = state.create_job(command="sleep 1")
    j3 = state.create_job(command="sleep 1")

    assert state.get_jobs(f"{j1.job_id}")[0] == j1
    assert state.get_jobs(f"f2/{j2.job_id}")[0] == j2
    with pytest.raises(DoesNotExist):
        state.get_jobs("42")

    assert all(a == b for a, b in zip(state.get_jobs("../*"), [j1]))
    assert all(a == b for a, b in zip(state.get_jobs("*"), [j2, j3]))
    state.cwd = root
    assert all(a == b for a, b in zip(state.get_jobs("f2/*"), [j2, j3]))


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
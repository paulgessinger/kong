import os

import pytest
from unittest.mock import Mock
import peewee as pw

import kong
from kong.model import Folder


@pytest.fixture
def cfg(app_env, monkeypatch):
    app_dir, config_path, tmp_path = app_env
    with monkeypatch.context() as m:
        m.setattr(
            "click.prompt",
            Mock(side_effect=["LocalDriver", os.path.join(app_dir, "joblog")]),
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


def test_ls(tree, state):
    root = tree

    res = state.ls(".")
    assert all(a == b for a, b in zip(res, root.children))

    with pytest.raises(pw.DoesNotExist):
        state.ls("/nope")

    f2 = Folder.find_by_path(state.cwd, "/f2")
    state.cwd = f2
    res = state.ls(".")
    assert all(a == b for a, b in zip(res, f2.children))

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

def test_rm(state, db):
    root = Folder.get_root()

    with pytest.raises(pw.DoesNotExist):
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

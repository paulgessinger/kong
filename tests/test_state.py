import os

import pytest
from unittest.mock import Mock

import kong


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

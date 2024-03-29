import os
import logging

from unittest.mock import Mock

import pytest

from kong.cli import main
import kong
from kong.model.folder import Folder
from kong import logger


def test_verbosity(app_env, cli):
    app_dir, config_path, tmp_path = app_env

    result = cli.invoke(main, ["--version"])
    assert result.exception is None
    assert result.exit_code == 0
    assert logger.logger.getEffectiveLevel() == logging.WARNING
    assert logging.getLogger().getEffectiveLevel() == logging.WARNING

    result = cli.invoke(main, ["--version", "-v"])
    assert result.exit_code == 0
    assert result.exception is None
    assert logger.logger.getEffectiveLevel() == logging.INFO
    assert logging.getLogger().getEffectiveLevel() == logging.INFO

    result = cli.invoke(main, ["--version", "-vv"])
    assert result.exit_code == 0
    assert result.exception is None
    assert logger.logger.getEffectiveLevel() == logging.DEBUG
    assert logging.getLogger().getEffectiveLevel() == logging.INFO

    result = cli.invoke(main, ["--version", "-vvv"])
    assert result.exit_code == 0
    assert result.exception is None
    assert logger.logger.getEffectiveLevel() == logging.DEBUG
    assert logging.getLogger().getEffectiveLevel() == logging.DEBUG


def test_setup_implicit(app_env, db, cli, monkeypatch):
    app_dir, config_path, tmp_path = app_env
    print("APPDIR:", kong.config.APP_DIR)
    print("CONFDIR:", kong.config.CONFIG_FILE)

    repl = Mock()
    monkeypatch.setattr("kong.cli.Repl", repl)

    assert not os.path.exists(config_path)
    result = cli.invoke(main, ["-vv"], input="\n\n")
    assert result.exit_code == 0
    assert result.exception is None
    assert os.path.exists(config_path)
    cfg = kong.config.Config()
    assert cfg.default_driver == "kong.drivers.local_driver.LocalDriver"
    assert cfg.jobdir == os.path.join(app_dir, "jobdir")

    # assert db and root folder was created
    assert Folder.get_or_none(name="root", parent=None) is not None

    # run again
    result = cli.invoke(main, ["-vv"])


def test_setup_invalid_driver(app_env, db, cli):
    app_dir, config_path, tmp_path = app_env

    assert not os.path.exists(config_path)
    result = cli.invoke(main, [], input="NotADriver\n\n")
    assert result.exit_code == 1
    assert result.exception is not None


def test_setup_explicit(app_env, db, cli):
    app_dir, config_path, tmp_path = app_env

    assert not os.path.exists(config_path)
    result = cli.invoke(main, ["-vv", "setup"], input="\n\n")
    assert result.exit_code == 0
    assert result.exception is None
    assert os.path.exists(config_path)
    cfg = kong.config.Config()
    assert cfg.default_driver == "kong.drivers.local_driver.LocalDriver"
    assert cfg.jobdir == os.path.join(app_dir, "jobdir")

    # assert db and root folder was created
    assert Folder.get_or_none(name="root", parent=None) is not None


def test_repl_raises(app_env, db, cli, monkeypatch):
    app_dir, config_path, tmp_path = app_env
    cmdloop = Mock(side_effect=RuntimeError())
    monkeypatch.setattr("kong.repl.Repl.cmdloop", cmdloop)
    result = cli.invoke(main, [])
    assert result.exit_code == 1
    assert result.exception is not None


try:
    import IPython

    have_ipython = True
except:
    have_ipython = False


@pytest.mark.skipif(not have_ipython, reason="IPython needs to be installed")
def test_interactive(app_env, cli, monkeypatch):
    mock = Mock()
    monkeypatch.setattr("IPython.embed", mock)

    cli.invoke(main, ["interactive"])
    mock.assert_called_once()

import os
import logging

import pytest
from click.testing import CliRunner
from unittest.mock import Mock

from kong.cli import main
import kong
from kong.model import Folder


def test_verbosity(app_env, db, cli):
    app_dir, config_path, tmp_path = app_env

    result = cli.invoke(main, ["--version"])
    assert result.exception is None
    assert result.exit_code == 0
    assert kong.logger.logger.getEffectiveLevel() == logging.WARNING
    result = cli.invoke(main, ["--version", "-v"])
    assert result.exit_code == 0
    assert result.exception is None
    assert kong.logger.logger.getEffectiveLevel() == logging.INFO
    result = cli.invoke(main, ["--version", "-vv"])
    assert result.exit_code == 0
    assert result.exception is None
    assert kong.logger.logger.getEffectiveLevel() == logging.DEBUG


def test_setup_implicit(app_env, db, cli):
    app_dir, config_path, tmp_path = app_env

    assert not os.path.exists(config_path)
    result = cli.invoke(main, ["-vv"], input="\n\n")
    assert result.exit_code == 0
    assert result.exception is None
    assert os.path.exists(config_path)
    cfg = kong.config.Config()
    assert isinstance(cfg.driver, kong.drivers.LocalDriver)
    assert cfg.logdir == os.path.join(app_dir, "joblog")

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
    result = cli.invoke(main, ["setup"], input="\n\n")
    assert result.exit_code == 0
    assert result.exception is None
    assert os.path.exists(config_path)
    cfg = kong.config.Config()
    assert isinstance(cfg.driver, kong.drivers.LocalDriver)
    assert cfg.logdir == os.path.join(app_dir, "joblog")

    # assert db and root folder was created
    assert Folder.get_or_none(name="root", parent=None) is not None


def test_repl_raises(app_env, db, cli, monkeypatch):
    app_dir, config_path, tmp_path = app_env
    cmdloop = Mock(side_effect=RuntimeError())
    monkeypatch.setattr("kong.repl.Repl.cmdloop", cmdloop)
    result = cli.invoke(main, [])
    assert result.exit_code == 1
    assert result.exception is not None
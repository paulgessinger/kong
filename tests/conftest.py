import os
import functools

import pytest
import peewee as pw
import click
from unittest.mock import Mock

from kong.db import database
from kong.model import Folder
import kong.setup
import kong


@pytest.yield_fixture
def db():
    database.init(":memory:")
    database.connect()
    database.create_tables([Folder])
    yield database
    database.close()


@pytest.fixture
def app_env(tmp_path, monkeypatch):
    app_dir = os.path.join(tmp_path, "app")
    config_path = os.path.join(app_dir, "config.yml")
    monkeypatch.setattr("kong.config.APP_DIR", app_dir)
    monkeypatch.setattr("kong.config.CONFIG_FILE", config_path)
    monkeypatch.setattr("kong.config.DB_FILE", os.path.join(app_dir, "database.sqlite"))
    monkeypatch.setattr("kong.repl.history_file", os.path.join(tmp_path, "histfile"))
    assert not os.path.exists(config_path)
    assert kong.config.APP_DIR == app_dir
    assert kong.config.CONFIG_FILE == config_path
    return app_dir, config_path, tmp_path


@pytest.fixture
def cli():
    """Yield a click.testing.CliRunner to invoke the CLI."""
    class_ = click.testing.CliRunner

    def invoke_wrapper(f):
        """Augment CliRunner.invoke to emit its output to stdout.

        This enables pytest to show the output in its logs on test
        failures.

        """

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            echo = kwargs.pop("echo", False)
            result = f(*args, **kwargs)

            if echo is True:
                sys.stdout.write(result.output)

            # if result.exception is not None:
            # raise result.exception

            return result

        return wrapper

    class_.invoke = invoke_wrapper(class_.invoke)
    cli_runner = class_()

    yield cli_runner


@pytest.fixture
def tree(db):
    root = Folder.get_root()
    f1 = root.add_folder("f1")
    f2 = root.add_folder("f2")
    alpha = f2.add_folder("alpha")
    beta = f2.add_folder("beta")
    gamma = f2.add_folder("gamma")
    delta = gamma.add_folder("delta")
    f3 = root.add_folder("f3")
    omega = f3.add_folder("omega")
    return root


@pytest.fixture
def state(app_env, db, monkeypatch):
    app_dir, config_path, tmp_path = app_env
    with monkeypatch.context() as m:
        m.setattr(
            "click.prompt",
            Mock(side_effect=["LocalDriver", os.path.join(app_dir, "joblog")]),
        )
        kong.setup.setup(None)
    cfg = kong.config.Config()
    _state = kong.state.State(cfg, Folder.get_root())
    return _state

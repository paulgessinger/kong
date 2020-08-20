from datetime import timedelta
from unittest.mock import Mock, ANY

import pytest
import os

from pydantic import ValidationError

from kong.config import Config, Notifier, NotificationManager, SlurmConfig


def test_config_creation(state):
    config = Config()


def test_notifier(monkeypatch):
    notifier = Mock()
    get_notifier = Mock(return_value=notifier)
    monkeypatch.setattr("kong.config.notifiers.get_notifier", get_notifier)

    inst = Notifier("hurz")

    get_notifier.assert_called_once_with("hurz")

    inst.notify("a message", title=None)

    notifier.notify.assert_called_once_with(message=ANY)
    notifier.reset_mock()

    notifier.schema = {"properties": []}
    inst.notify("a message", title="some title")
    notifier.notify.assert_called_once_with(message=ANY)
    notifier.reset_mock()

    notifier.schema = {"properties": ["title"]}

    inst.notify("a message", title=None)
    notifier.notify.assert_called_once_with(message=ANY)
    notifier.reset_mock()

    inst.notify("a message", title="some title")
    notifier.notify.assert_called_once_with(message=ANY, title="some title")
    notifier.reset_mock()

    notifier.schema = {"properties": ["subject"]}

    inst.notify("a message", title=None)
    notifier.notify.assert_called_once_with(message=ANY)
    notifier.reset_mock()

    inst.notify("a message", title="some title")
    notifier.notify.assert_called_once_with(message=ANY, subject="some title")
    notifier.reset_mock()


def test_notificationmanager(monkeypatch):
    inst = Mock()
    inst.notify = Mock(return_value="RETURN_TAG")
    notifier = Mock(return_value=inst)
    monkeypatch.setattr("kong.config.Notifier", notifier)

    config = Mock()

    config.notify = []
    nm = NotificationManager(config)
    assert notifier.call_count == 0
    assert not nm.enabled

    config.notify = [{"name": "foo", "arg1": 5, "arg2": "yep"}]

    nm = NotificationManager(config)
    assert nm.enabled

    notifier.assert_called_once_with(name="foo", arg1=5, arg2="yep")

    res = nm.notify("a message", "some title", "extra pos", extra_kw=42)

    inst.notify.assert_called_once_with(
        "a message", "some title", "extra pos", extra_kw=42
    )

    assert len(res) == 1
    assert res[0] == "RETURN_TAG"


def test_slurm_schema():
    SlurmConfig(account="abc", default_queue="queue", node_size=2).node_size == 2
    with pytest.raises(ValidationError):
        SlurmConfig(account="abc", default_queue="queue", node_size=-1)

    defs = SlurmConfig(account="blubb", default_queue="queue")
    assert isinstance(defs.sacct_delta, timedelta)
    assert defs.sacct_delta > timedelta(seconds=0)


def test_slurm_schema_file(app_env):
    app_dir, config_path, tmp_path = app_env
    os.makedirs(app_dir)

    with open(config_path, "w") as fh:
        fh.write(
            """
slurm_driver:
    account: blub
    default_queue: queue
    sacct_delta: blablurz
        """.strip()
        )

    with pytest.raises(ValidationError):
        Config.from_yaml(config_path)

    with open(config_path, "w") as fh:
        fh.write(
            """
slurm_driver:
    default_queue: queue
    account: blub
    sacct_delta: 10 weeks
        """.strip()
        )

    cfg = Config.from_yaml(config_path)
    assert cfg.slurm_driver.sacct_delta == timedelta(weeks=10)

    with open(config_path, "w") as fh:
        fh.write(
            """
slurm_driver:
    default_queue: queue
    account: blub
    sacct_delta: 50 weeks
        """.strip()
        )
    cfg = Config.from_yaml(config_path)
    assert cfg.slurm_driver.sacct_delta == timedelta(weeks=50)

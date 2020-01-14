from datetime import timedelta
from unittest.mock import Mock, ANY

import pytest
import os
from schema import SchemaError

from kong.config import Config, Notifier, NotificationManager, slurm_schema


def test_config_creation(state):
    config = Config({})

def test_attribute_access(state):
    config = Config({"exist": 42})
    assert config.exist == 42

    with pytest.raises(AttributeError):
        config.noexist


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

    config.data = {}
    nm = NotificationManager(config)
    assert notifier.call_count == 0

    config.data = {"notify": []}
    nm = NotificationManager(config)
    assert notifier.call_count == 0

    config.data = {"notify": [{"name": "foo", "arg1": 5, "arg2": "yep"}]}

    nm = NotificationManager(config)

    notifier.assert_called_once_with(name="foo", arg1=5, arg2="yep")

    res = nm.notify("a message", "some title", "extra pos", extra_kw=42)

    inst.notify.assert_called_once_with(
        "a message", "some title", "extra pos", extra_kw=42
    )

    assert len(res) == 1
    assert res[0] == "RETURN_TAG"


def test_slurm_schema():
    assert slurm_schema.is_valid({"node_size": 2})
    assert not slurm_schema.is_valid({"node_size": -1})

    defs = slurm_schema.validate({})
    assert "sacct_delta" in defs
    assert isinstance(defs["sacct_delta"], timedelta)

    cfg = slurm_schema.validate({"sacct_delta": "10 weeks"})
    assert cfg["sacct_delta"] == timedelta(weeks=10)

    cfg = slurm_schema.validate({"sacct_delta": "50 weeks"})
    assert cfg["sacct_delta"] == timedelta(weeks=50)

    with pytest.raises(SchemaError):
        slurm_schema.validate({"sacct_delta": "blablurz"})


def test_slurm_schema_file(app_env):
    app_dir, config_path, tmp_path = app_env
    os.makedirs(app_dir)

    with open(config_path, "w") as fh:
        fh.write(
            """
slurm_driver:
    sacct_delta: blablurz
        """.strip()
        )

    with pytest.raises(SchemaError):
        Config()

    with open(config_path, "w") as fh:
        fh.write(
            """
slurm_driver:
    sacct_delta: 10 weeks
        """.strip()
        )

    cfg = Config()
    assert cfg.data["slurm_driver"]["sacct_delta"] == timedelta(weeks=10)

    with open(config_path, "w") as fh:
        fh.write(
            """
slurm_driver:
    sacct_delta: 50 weeks
        """.strip()
        )

    cfg = Config()
    print(cfg.data)
    assert cfg.data["slurm_driver"]["sacct_delta"] == timedelta(weeks=50)

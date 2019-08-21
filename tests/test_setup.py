import os
from unittest.mock import Mock, call
from unittest import mock

import yaml
import pytest

import kong.setup
import kong


def test_setup(app_env, monkeypatch):
    app_dir, config_path, tmp_path = app_env

    # first time setup
    with monkeypatch.context() as m:
        prompt = Mock(
            side_effect=[
                "LocalDriver",
                os.path.join(tmp_path, "jobs"),
                os.path.join(tmp_path, "joboutput"),
            ]
        )
        m.setattr("click.prompt", prompt)
        kong.setup.setup(None)
        prompt.assert_has_calls(
            [
                call(mock.ANY, default="LocalDriver"),
                call(mock.ANY, default=mock.ANY),
                call(mock.ANY, default=mock.ANY),
            ]
        )

    assert os.path.exists(config_path)

    with open(config_path) as f:
        data = yaml.safe_load(f)
    assert data["default_driver"] == "LocalDriver"
    assert data["jobdir"] == os.path.join(tmp_path, "jobs")
    assert data["joboutputdir"] == os.path.join(tmp_path, "joboutput")
    assert "history_length" in data

    # re-run setup
    cfg = kong.config.Config()
    assert cfg.data == data
    assert cfg.jobdir == os.path.join(tmp_path, "jobs")

    with monkeypatch.context() as m:
        prompt = Mock(
            side_effect=[
                "LocalDriver",
                os.path.join(tmp_path, "jobs_new"),
                os.path.join(tmp_path, "joboutput_new"),
            ]
        )
        m.setattr("click.prompt", prompt)
        kong.setup.setup(cfg)
        # should provide default from before
        prompt.assert_has_calls(
            [
                call(mock.ANY, default="LocalDriver"),
                call(mock.ANY, default=os.path.join(tmp_path, "jobs")),
                call(mock.ANY, default=os.path.join(tmp_path, "joboutput")),
            ]
        )

    with open(config_path) as f:
        data = yaml.safe_load(f)
    assert data["default_driver"] == "LocalDriver"  # still the same
    assert data["jobdir"] == os.path.join(tmp_path, "jobs_new")  # this has changed
    assert data["joboutputdir"] == os.path.join(
        tmp_path, "joboutput_new"
    )  # this has changed


def test_setup_invalid_driver(app_env, monkeypatch):
    app_dir, config_path, tmp_path = app_env

    with monkeypatch.context() as m:
        prompt = Mock(side_effect=["NotADriver", os.path.join(tmp_path, "jobs")])
        m.setattr("click.prompt", prompt)
        with pytest.raises(AssertionError):
            kong.setup.setup(None)
        prompt.assert_has_calls([call(mock.ANY, default="LocalDriver")])

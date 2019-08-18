import os

import pytest
from unittest.mock import Mock

from kong.drivers import LocalDriver
import kong

@pytest.fixture
def state(app_env, db, monkeypatch):
    app_dir, config_path, tmp_path = app_env
    with monkeypatch.context() as m:
        m.setattr(
            "click.prompt",
            Mock(side_effect=["LocalDriver", os.path.join(app_dir, "joblog")]),
        )
        kong.setup.setup(None)
    return kong.get_instance()

def test_create_job(state):
    pass

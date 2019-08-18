import os
from typing import Optional, Any, Dict

import yaml
import click

from . import config
from .logger import logger
from . import drivers


def setup(cfg: Optional[config.Config]) -> None:
    logger.debug("Running setup")
    if not os.path.exists(config.APP_DIR):
        os.makedirs(config.APP_DIR)

    data: Dict[str, Any]
    if cfg is None:
        data = dict()
    else:
        data = dict(cfg.data)

    available_drivers = [d for d in drivers.__all__]

    data["default_driver"] = click.prompt(
        f"Which batch system driver shall we use by default? ({', '.join(available_drivers)})",
        default=data.get("default_driver", "LocalDriver"),
    )

    assert data["default_driver"] in available_drivers, "Please select a valid driver"

    data["logdir"] = os.path.expanduser(
        click.prompt(
            "Where is a good place to put job log files?",
            default=data.get("logdir", os.path.join(config.APP_DIR, "joblog")),
        )
    )

    if not os.path.exists(data["logdir"]):
        os.makedirs(data["logdir"])

    data["history_length"] = data.get("history_length", 1000)

    logger.debug("Config: %s", data)
    with open(config.CONFIG_FILE, "w") as f:
        yaml.dump(data, f)

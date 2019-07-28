import os

import yaml
import click

from . import config
from .logger import logger
from . import drivers


def setup(cfg):
    logger.debug("Running setup")
    if not os.path.exists(config.APP_DIR):
        os.makedirs(config.APP_DIR)

    if cfg is None:
        data = dict()
    else:
        data = dict(cfg.data)

    available_drivers = [d.__name__ for d in drivers.__all__ if d != drivers.DriverBase]

    data["driver"] = click.prompt(
        f"Which batch system driver shall we use? ({', '.join(available_drivers)})",
        default=data.get("driver", "LocalDriver"),
    )

    assert data["driver"] in available_drivers, "Please select a valid driver"

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

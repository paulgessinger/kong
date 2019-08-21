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

    data["jobdir"] = os.path.expanduser(
        click.prompt(
            "Where is a good place to put job (e.g. log) files?",
            default=data.get("jobdir", os.path.join(config.APP_DIR, "jobdir")),
        )
    )

    if not os.path.exists(data["jobdir"]):
        os.makedirs(data["jobdir"])

    data["joboutputdir"] = os.path.expanduser(
        click.prompt(
            "Where is a good place to put job output files?",
            default=data.get("joboutputdir", os.path.join(config.APP_DIR, "joboutput")),
        )
    )

    if not os.path.exists(data["joboutputdir"]):
        os.makedirs(data["joboutputdir"])

    data["history_length"] = data.get("history_length", 1000)

    logger.debug("Config: %s", data)
    with open(config.CONFIG_FILE, "w") as f:
        yaml.dump(data, f)

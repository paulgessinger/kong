import os
from typing import Optional, Any, Dict

import yaml
import click

from . import config
from .logger import logger
from . import drivers
from .drivers import get_driver


def setup(cfg: Optional[config.Config]) -> None:
    logger.debug("Running setup")
    if not os.path.exists(config.APP_DIR):
        os.makedirs(config.APP_DIR)

    data: Dict[str, Any]
    if cfg is None:
        data = config.config_schema.validate({})
    else:
        data = dict(cfg.data)

    data["default_driver"] = click.prompt(
        "Which batch system driver shall we use by default?",
        default=data.get("default_driver"),
    )

    try:
        assert issubclass(
            get_driver(data["default_driver"]), drivers.driver_base.DriverBase
        ), "Please provide a valid driver"
    except Exception:
        raise ValueError(f"{data['default_driver']} is not a valid driver")

    data["jobdir"] = os.path.expanduser(
        click.prompt(
            "Where is a good place to put job (e.g. log) files?",
            default=data.get("jobdir"),
        )
    )

    if not os.path.exists(data["jobdir"]):
        os.makedirs(data["jobdir"])

    data["joboutputdir"] = os.path.expanduser(
        click.prompt(
            "Where is a good place to put job output files?",
            default=data.get("joboutputdir"),
        )
    )

    if not os.path.exists(data["joboutputdir"]):
        os.makedirs(data["joboutputdir"])

    data["history_length"] = data.get("history_length")

    data = config.config_schema.validate(data)

    logger.debug("Config: %s", data)
    with open(config.CONFIG_FILE, "w") as f:
        yaml.dump(data, f)

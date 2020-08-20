import os
from typing import Optional

from . import config
from .logger import logger


def setup(cfg: Optional[config.Config]) -> None:
    logger.debug("Running setup")
    if not os.path.exists(config.APP_DIR):
        os.makedirs(config.APP_DIR)

    if cfg is None:
        raise NotImplementedError()

from . import config
from .db import database
from .model import *
from .logger import logger


class State:
    def __init__(self, config, cwd):
        self.config = config
        self.cwd = cwd

    @classmethod
    def get_instance(cls):
        cfg = config.Config()
        logger.debug("Initialized config: %s", cfg.data)

        logger.debug(
            "Initializing database '%s' at '%s'", config.APP_NAME, config.DB_FILE
        )
        database.init(config.DB_FILE)

        # ensure database is set up
        database.connect()
        database.create_tables([Folder])

        cwd = Folder.get_root()

        return cls(cfg, cwd)

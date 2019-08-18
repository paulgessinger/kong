import os
from typing import List, Callable

import peewee as pw

from . import config, drivers
from .db import database
from . import model
from .model import Folder
from .logger import logger

class CannotCreateError(RuntimeError):
    pass

class CannotRemoveRoot(RuntimeError):
    pass

class State:
    def __init__(self, config: config.Config, cwd: Folder) -> None:
        self.config = config
        self.cwd = cwd
        self.default_driver = getattr(drivers, self.config.default_driver)

    @classmethod
    def get_instance(cls) -> 'State':
        cfg = config.Config()
        logger.debug("Initialized config: %s", cfg.data)

        logger.debug(
            "Initializing database '%s' at '%s'", config.APP_NAME, config.DB_FILE
        )
        database.init(config.DB_FILE)

        # ensure database is set up
        database.connect()
        database.create_tables([getattr(model, m) for m in model.__all__])

        cwd = Folder.get_root()

        return cls(cfg, cwd)

    def ls(self, path: str=".") -> List['Folder']:
        "List the current directory content"
        logger.debug("%s", list(self.cwd.children))
        folder = Folder.find_by_path(self.cwd, path)
        if folder is None:
            raise pw.DoesNotExist()
        return folder.children

    def cd(self, name: str=".") -> None:
        if name == "":
            folder = Folder.get_root()
        else:
            _folder = Folder.find_by_path(self.cwd, name)
            assert _folder is not None
            folder = _folder
        if folder is None:
            raise pw.DoesNotExist()
        self.cwd = folder

    def mkdir(self, path: str) -> None:
        head, tail = os.path.split(path)

        location = Folder.find_by_path(self.cwd, head)
        if location is None:
            raise CannotCreateError(f"Cannot create folder at '{path}'")
        logger.debug("Attempt to create folder named '%s' in '%s'", tail, location.path)

        Folder.create(name=tail, parent=location)

    def rm(self, name: str, confirm: Callable[[], bool] = lambda: True) -> bool:
        if name == "/":
            raise CannotRemoveRoot()
        folder = Folder.find_by_path(self.cwd, name)
        if folder is None:
            raise pw.DoesNotExist()
        if confirm():
            folder.delete_instance(recursive=True, delete_nullable=True)
            return True
        return False

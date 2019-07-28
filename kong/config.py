import os

import click
import yaml
from attrdict import AttrDict

from . import drivers

APP_NAME = "kong"
APP_DIR = click.get_app_dir(APP_NAME, force_posix=True)
CONFIG_FILE = os.path.join(APP_DIR, "config.yml")
DB_FILE = os.path.join(APP_DIR, "database.sqlite")


class Config:
    def __init__(self):
        with open(CONFIG_FILE) as f:
            self.data = AttrDict(yaml.safe_load(f))
            self.driver = getattr(drivers, self.data.driver)(self)

    def __getattr__(self, key):
        return getattr(self.data, key)

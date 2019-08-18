from typing import Any

import os

import click
import yaml

APP_NAME = "kong"
APP_DIR = click.get_app_dir(APP_NAME, force_posix=True)
CONFIG_FILE = os.path.join(APP_DIR, "config.yml")
DB_FILE = os.path.join(APP_DIR, "database.sqlite")


class Config:
    def __init__(self) -> None:
        with open(CONFIG_FILE) as f:
            self.data = yaml.safe_load(f)

    def __getattr__(self, key: str) -> Any:
        return self.data[key]

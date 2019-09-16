import os
import typing

import click
import yaml
from schema import And, Optional, Schema

APP_NAME = "kong"
APP_DIR = click.get_app_dir(APP_NAME, force_posix=True)
CONFIG_FILE = os.path.join(APP_DIR, "config.yml")
DB_FILE = os.path.join(APP_DIR, "database.sqlite")


config_schema = Schema(
    {
        "default_driver": And(str, len),
        "jobdir": os.path.exists,
        "joboutputdir": os.path.exists,
        "history_length": int,
        Optional(object): object,
    }
)


class Config:
    def __init__(
        self, data: typing.Optional[typing.Dict[str, typing.Any]] = None
    ) -> None:
        if data is not None:
            self.data = data
        else:
            with open(CONFIG_FILE) as f:
                self.data = yaml.safe_load(f)

        self.data = config_schema.validate(self.data)

    def __getattr__(self, key: str) -> typing.Any:
        return self.data[key]

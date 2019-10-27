import os
import typing
import socket

import click
import yaml
from schema import And, Optional, Schema
import notifiers  # type: ignore


APP_NAME = "kong"
APP_DIR = click.get_app_dir(APP_NAME, force_posix=True)
CONFIG_FILE = os.path.join(APP_DIR, "config.yml")
DB_FILE = os.path.join(APP_DIR, "database.sqlite")

slurm_schema = Schema(
    {
        Optional("account", default="account"): And(str, len),
        Optional("node_size", default=1): And(int, lambda i: i > 0),
        Optional("default_queue", default="queue"): And(str, len),
    }
)

config_schema = Schema(
    {
        Optional(
            "default_driver", default="kong.drivers.local_driver.LocalDriver"
        ): And(str, len),
        Optional(
            "jobdir", default=lambda: os.path.join(APP_DIR, "jobdir")
        ): os.path.exists,
        Optional(
            "joboutputdir", default=lambda: os.path.join(APP_DIR, "joboutput")
        ): os.path.exists,
        Optional("history_length", default=1000): int,
        Optional("slurm_driver"): slurm_schema,
        Optional("notify", default=[]): [{"name": str, Optional(object): object}],
        Optional(object): object,
    }
)


class Notifier:
    name: typing.Optional[str] = None
    args: typing.Optional[typing.Dict[str, typing.Any]] = None
    _notifier: notifiers.core.Provider

    def __init__(self, name: str, **kwargs: typing.Any):
        from .logger import logger

        self.logger = logger
        self.name = name
        self._notifier = notifiers.get_notifier(self.name)
        self._kwargs = kwargs

    def notify(
        self,
        message: str,
        title: typing.Optional[str] = None,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> notifiers.core.Response:
        self.logger.debug("Sending notification '%s' via %s", message, self.name)

        kwargs = kwargs.copy()
        kwargs["message"] = f"{socket.gethostname()}: {message}"
        if title is not None:
            if "title" in self._notifier.schema["properties"]:
                kwargs["title"] = title
            elif "subject" in self._notifier.schema["properties"]:
                kwargs["subject"] = title
            else:
                kwargs["message"] = title + ":\n" + kwargs["message"]

        return self._notifier.notify(**self._kwargs, **kwargs)


class NotificationManager:
    notifiers: typing.List[Notifier]

    def __init__(self, config: "Config"):
        from .logger import logger

        self.logger = logger

        self.notifiers = []
        if "notify" in config.data and config.data["notify"] is not None:
            for item in config.data["notify"]:
                name = item["name"]
                args = dict(item)
                del args["name"]
                self.notifiers.append(Notifier(name=name, **args))

    def notify(
        self,
        message: str,
        title: typing.Optional[str] = None,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> typing.List[notifiers.core.Response]:  # type: ignore
        self.logger.debug("%d notifiers configured", len(self.notifiers))

        responses: typing.List[notifiers.core.Response] = []
        for notifier in self.notifiers:
            responses.append(notifier.notify(message, title, *args, **kwargs))
        return responses


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

        self.notifications = NotificationManager(self)

    def __getattr__(self, key: str) -> typing.Any:
        return self.data[key]

"""
Holds the config schema, a class to load the config, and some helpers to
perform notifications that can be configured in the config file
"""
import os
from datetime import timedelta

import pytimeparse
import socket
import getpass

import click
import yaml

import notifiers  # type: ignore
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any, Union, TypeVar, Type

APP_NAME = "kong"
APP_DIR = click.get_app_dir(APP_NAME, force_posix=True)
CONFIG_FILE = os.path.join(APP_DIR, "config.yml")
DB_FILE = os.path.join(APP_DIR, "database.sqlite")

T = TypeVar("T", bound="BaseConfig")


class BaseConfig(BaseModel):
    class Config:
        extra = "forbid"

    @classmethod
    def from_yaml(cls: Type[T], file: str) -> T:
        with open(file) as f:
            data = yaml.safe_load(f)
        return cls(**data)


class SlurmConfig(BaseConfig):
    account: str
    node_size: int = Field(1, gt=0)
    default_queue: str
    sacct_delta: timedelta = timedelta(weeks=4)

    @validator("sacct_delta", pre=True)  # type: ignore
    def parse(cls, value: Union[str, timedelta]) -> timedelta:
        if isinstance(value, timedelta):
            return value
        return timedelta(seconds=pytimeparse.parse(value))


class HTCondorConfig(BaseConfig):
    user: str = Field(default_factory=getpass.getuser)
    default_universe: Optional[str]
    submitfile_extra: Optional[str]


class Config(BaseConfig):
    default_driver: str = "kong.drivers.local_driver.LocalDriver"
    jobdir: str = Field(default_factory=lambda: os.path.join(APP_DIR, "jobdir"))
    joboutputdir: str = Field(
        default_factory=lambda: os.path.join(APP_DIR, "joboutput")
    )
    repl_extra_columns: List[str] = []
    history_length: int = Field(1000, gt=0)
    notify: List[Dict[str, Any]] = []

    slurm_driver: Optional[SlurmConfig]
    htcondor_driver: Optional[HTCondorConfig]


class Notifier:
    """
    Class that manages a notification provider. This essentially wraps :class:`notifiers.core.Provider` and enables
    a bit more automatic filling of fields depending on the accepted provider schema.
    """

    name: Optional[str] = None
    args: Optional[Dict[str, Any]] = None
    _notifier: notifiers.core.Provider

    def __init__(self, name: str, **kwargs: Any):
        """
        Initialize method for the generic notifier
        :param name: The name of this notifier, is used to instantiate the underlying provider
        :param kwargs: Any additional arguments to be passed to the provider at construction
        """
        from .logger import logger

        self.logger = logger
        self.name = name
        self._notifier = notifiers.get_notifier(self.name)
        self._kwargs = kwargs

    def notify(
        self, message: str, title: Optional[str] = None, **kwargs: Any
    ) -> notifiers.core.Response:
        """
        Send a notification through this notificer instance

        .. note::
           If you specify a title it will be set as the title or subject field if the provider
           supports it, otherwise it will be prepended to the message

        :param message: The message to send
        :param title: A title to send, optional
        :param kwargs: Any additional keyword arguments to pass to the provider's notify call
        :return: a :class:`notifiers.core.Response` instance
        """
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
    """
    Class to group and handle multiple notifiers/providers. Will always send to all of them.
    """

    notifiers: List[Notifier]

    def __init__(self, config: "Config"):
        """
        Initializer for the notification manager
        :param config: Config object that is used to configure the :class:`Notifier` instances.
        """
        from .logger import logger

        self.logger = logger

        self.notifiers = []
        if config.notify is not None:
            for item in config.notify:
                name = item["name"]
                args = dict(item)
                del args["name"]
                self.notifiers.append(Notifier(name=name, **args))

    def notify(
        self, message: str, title: Optional[str] = None, *args: Any, **kwargs: Any
    ) -> List[notifiers.core.Response]:  # type: ignore
        """
        Sends a notification using all the configured notifiers/providers.

        .. note::
           If you specify a title it will be set as the title or subject field if the provider
           supports it, otherwise it will be prepended to the message

        :param message: The message to send
        :param title: The title for the notification
        :param args: Any additional positional arguments to pass to the providers
        :param kwargs: Any additional keyword arguments to pass to the providers
        :return: List of :class:`notifiers.core.Response` instances
        """
        self.logger.debug("%d notifiers configured", len(self.notifiers))

        responses: List[notifiers.core.Response] = []
        for notifier in self.notifiers:
            responses.append(notifier.notify(message, title, *args, **kwargs))
        return responses

    @property
    def enabled(self) -> bool:
        return len(self.notifiers) > 0


# class Config:
#     """
#     Class to handle loading the config data from disk.
#     """
#
#     def __init__(
#         self, data: typing.Optional[typing.Dict[str, typing.Any]] = None
#     ) -> None:
#         """
#         Initalize method for the config. Will load the config file from the app directory (OS dependant)
#
#         Parameters
#         ----------
#         data
#             Dictionary with pre-loaded data. Will be used as is if provided (optional)
#         """
#
#         if data is not None:
#             self.data = data
#         else:
#             with open(CONFIG_FILE) as f:
#                 self.data = yaml.safe_load(f)
#
#         self.data = config_schema.validate(self.data)
#
#         self.notifications = NotificationManager(self)
#
#     def __getattr__(self, key: str) -> typing.Any:
#         if key not in self.data:
#             raise AttributeError()
#         return self.data[key]

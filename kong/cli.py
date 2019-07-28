import os
import sys
import logging

import click
import coloredlogs

from . import config
from . import setup
from .logger import logger
from .db import database
from .repl import Repl
from .model import *

import pkg_resources  # part of setuptools

version = pkg_resources.require(config.APP_NAME)[0].version


class CatchAllExceptions(click.Group):
    def __call__(self, *args, **kwargs):
        try:
            return self.main(*args, **kwargs)
        except AssertionError as exc:
            logger.debug("Exception caught", exc_info=True)
            click.secho(f"AssertionFailed: {exc}", fg="red")
            sys.exit(1)


@click.group(invoke_without_command=True, cls=CatchAllExceptions)
@click.option("--version", "show_version", is_flag=True)
@click.option("-v", "--verbose", "verbosity", count=True)
@click.pass_context
def main(ctx, show_version, verbosity):
    global_level = logging.WARNING
    if verbosity == 0:
        level = logging.WARNING
    elif verbosity == 1:
        level = logging.INFO
    elif verbosity == 2:
        level = logging.DEBUG
    else:
        level = logging.DEBUG
        global_level = logging.DEBUG

    coloredlogs.install(
        fmt="%(asctime)s %(levelname)s %(name)s %(filename)s:%(funcName)s %(message)s",
        level=level,
    )

    logger.setLevel(level)

    if show_version:
        click.echo(config.APP_NAME, "version:", version)
        return

    # check if setup was executed
    if not os.path.exists(config.CONFIG_FILE):
        logger.debug("Setup was not executed yet")
        setup.setup(None)

    ctx.ensure_object(config.Config)

    logging.debug("Initializing database '%s' at '%s'", config.APP_NAME, config.DB_FILE)
    database.init(config.DB_FILE)

    # ensure database is set up
    database.connect()
    database.create_tables([Folder])

    # ensure we have a root folder
    ctx.obj.cwd = Folder.get_root()

    if ctx.invoked_subcommand is None:
        Repl(ctx.obj).cmdloop()


@main.command("setup")
@click.pass_obj
def setup_command(config):
    setup.setup(config)

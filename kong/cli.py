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


@click.group(invoke_without_command=True)
@click.option("--version", "show_version", is_flag=True)
@click.option("-v", "--verbose", "verbosity", count=True)
@click.pass_context
def main(ctx, show_version, verbosity):
    if verbosity == 0:
        level = logging.WARNING
    elif verbosity == 1:
        level = logging.INFO
    elif verbosity == 2:
        level = logging.DEBUG

    coloredlogs.install(
        fmt="%(asctime)s %(levelname)s %(name)s %(filename)s:%(funcName)s %(message)s",
        level=level,
    )

    logger.setLevel(level)

    if show_version:
        click.echo(f"{config.APP_NAME} version: {version}")
        return

    # check if setup was executed
    if not os.path.exists(config.CONFIG_FILE):
        logger.debug("Setup was not executed yet")
        try:
            setup.setup(None)
        except Exception as e:
            raise click.ClickException(e)

    ctx.ensure_object(config.Config)

    logging.debug("Initializing database '%s' at '%s'", config.APP_NAME, config.DB_FILE)
    database.init(config.DB_FILE)

    # ensure database is set up
    database.connect()
    database.create_tables([Folder])

    # ensure we have a root folder
    ctx.obj.cwd = Folder.get_root()

    if ctx.invoked_subcommand is None:
        try:
            Repl(ctx.obj).cmdloop()
        except Exception as e:
            logger.error("Exception caught", exc_info=True)
            raise click.ClickException(e)


@main.command("setup")
@click.pass_obj
def setup_command(config):
    setup.setup(config)

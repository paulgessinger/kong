import os
import logging
from typing import Any

import click
import coloredlogs

from . import config
from . import setup
from .state import State
from .logger import logger
from .repl import Repl

import pkg_resources  # part of setuptools

version = pkg_resources.get_distribution("kong-batch").version


@click.group(invoke_without_command=True)
@click.option("--version", "show_version", is_flag=True)
@click.option("-v", "--verbose", "verbosity", count=True)
@click.pass_context
def main(ctx: Any, show_version: bool, verbosity: int) -> None:
    if verbosity == 0:
        level = logging.WARNING
        global_level = logging.WARNING
    elif verbosity <= 1:
        level = logging.INFO
        global_level = logging.INFO
    elif verbosity <= 2:
        level = logging.DEBUG
        global_level = logging.INFO
    else:
        level = logging.DEBUG
        global_level = logging.DEBUG

    coloredlogs.install(
        fmt="%(asctime)s %(levelname)s %(name)s %(filename)s:%(funcName)s %(message)s",
        level=level,
    )

    logger.setLevel(level)
    logging.getLogger().setLevel(global_level)

    if show_version:
        click.echo(f"{config.APP_NAME} version: {version}")
        return

    # check if setup was executed
    if not os.path.exists(config.CONFIG_FILE):
        logger.debug(
            "Setup was not executed yet, config file at %s does not exist",
            config.CONFIG_FILE,
        )
        try:
            setup.setup(None)
        except Exception as e:
            logger.error("Got error during setup", exc_info=True)
            raise click.ClickException(str(e))
    else:
        logger.debug("Setup executed already")
    logger.debug("Setup completed")

    inst = State.get_instance()

    ctx.obj = inst

    if ctx.invoked_subcommand is None:
        try:
            logger.debug("Entering REPL")
            Repl(ctx.obj).cmdloop()
        except Exception as e:
            logger.error("Exception caught", exc_info=True)
            raise click.ClickException(str(e))


@main.command("setup")
@click.pass_obj
def setup_command(state: State) -> None:
    setup.setup(state.config)


@main.command()
@click.pass_obj
def interactive(state: State) -> None:
    logger.debug("Launching IPython session")
    import IPython

    IPython.embed(colors="neutral")

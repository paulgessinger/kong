import os
from typing import Any

import click
from peewee import sqlite3

from . import config
from . import setup
from .state import State
from .logger import logger
from .repl import Repl
from . import __version__
from .util import set_verbosity


@click.group(invoke_without_command=True)
@click.option("--version", "show_version", is_flag=True, help="Show version and exit")
@click.option("-v", "--verbose", "verbosity", count=True, help="Increase the verbosity")
@click.pass_context
def main(ctx: Any, show_version: bool, verbosity: int) -> None:
    """
    Starts the main kong command loop. Will automatically perform setup the
    first time it is invoked.
    """
    set_verbosity(verbosity)

    if show_version:
        click.echo(f"{config.APP_NAME} version: {__version__}")
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

    logger.debug("sqlite3 version: %s", ".".join(map(str, sqlite3.sqlite_version_info)))

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
    """Perform first time setup, or change values"""
    setup.setup(state.config)


@main.command()
@click.pass_obj
def interactive(state: State) -> None:
    """
    Launch an interactive ipython instance with a kong.state.State object
    already created. This is useful for fixing things in the database and such.
    Most of the time, the main command loop invoked with `kong` should be sufficient, however.
    """
    logger.debug("Launching IPython session")
    import IPython

    IPython.embed(colors="neutral")

import contextlib
import os
import logging
from pathlib import Path
from typing import Any

import click
import coloredlogs

from kong.config import Config
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



cwd_file = Path(config.APP_DIR, "cwd.txt")

def _wrap_repl_func(fn):
    orig_fn = fn.__orig_fn__

    def wrapped(state, *args, **kwargs):
        if cwd_file.exists():
            with cwd_file.open("r") as fh:
                state.cd(fh.read().strip())
        repl = Repl(state)
        return orig_fn(repl, *args, **kwargs)

    if hasattr(orig_fn, "__click_params__"):
        wrapped.__click_params__ = orig_fn.__click_params__
    wrapped.__doc__ = orig_fn.__doc__

    return wrapped


def repl_func(fn, name=None):
    if name is None:
        _, name = fn.__name__.split("_", 1)
    return main.command(name=name)(click.pass_obj(_wrap_repl_func(fn)))


for fn in [
    Repl.do_ls,
    Repl.do_create_job,
    Repl.do_submit_job,
    Repl.do_resubmit_job,
    Repl.do_update,
    Repl.do_status,
    Repl.do_wait,
    Repl.do_mv,
    Repl.do_mkdir,
    Repl.do_rm,
    Repl.do_info,
    Repl.do_kill_job,
    Repl.do_cwd,
    Repl.do_less,
    Repl.do_tail,
]:
    repl_func(fn)

@main.command()
@click.pass_obj
@click.argument("name", required=False, default="")
def cd(state: State, name: str) -> None:
    if cwd_file.exists():
        with cwd_file.open("r") as f:
            state.cd(f.read().strip())
    state.cd(name)
    with cwd_file.open("w") as f:
        f.write(state.cwd.path)


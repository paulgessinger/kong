import functools
import shlex
import cmd
import readline
import os

import click
import peewee as pw

from .config import APP_NAME, APP_DIR
from .logger import logger
from .model import *
from . import state

history_file = os.path.join(APP_DIR, "history")


def parse(f):
    @functools.wraps(f)
    def wrapper(self, args=""):
        return f(self, *shlex.split(args))

    return wrapper


class Repl(cmd.Cmd):
    intro = f"This is {APP_NAME} shell"
    prompt = f"({APP_NAME} > /) "

    def __init__(self, state):
        self.state = state
        super().__init__()

    def precmd(self, line):
        logger.debug("called '%s'", line)
        return line

    def onecmd(self, *args):
        try:
            return super().onecmd(*args)
        except TypeError as e:
            click.secho(f"{e}", fg="red")
        except Exception as e:
            logger.error("Exception occured", exc_info=True)

    def complete_path(self, path):
        if path.endswith("/"):
            folder = Folder.find_by_path(self.state.cwd, path)
            prefix = ""
        else:
            head, prefix = os.path.split(path)
            folder = Folder.find_by_path(self.state.cwd, head)

        options = []
        for child in folder.children:
            if child.name.startswith(prefix):
                options.append(child.name + "/")
        return options

    @parse
    def do_ls(self, arg="."):
        "List the current directory content"
        try:
            children = self.state.ls(arg)
            for child in children:
                click.echo(child.name)
        except pw.DoesNotExist:
            click.secho(f"Folder {arg} does not exist", fg="red")

    def complete_ls(self, text, line, begidx, endidx):
        args = shlex.split(line)
        path = args[1]
        return self.complete_path(path)

    @parse
    def do_mkdir(self, path):
        "Create a directory at the current location"
        try:
            self.state.mkdir(path)
        except state.CannotCreateError:
            click.secho(f"Cannot create folder at '{path}'", fg="red")
        except pw.IntegrityError:
            click.secho(
                f"Folder {path} in {self.state.cwd.path} already exists", fg="red"
            )

    def complete_mkdir(self, text, line, begidx, endidx):
        args = shlex.split(line)
        path = args[1]
        return self.complete_path(path)

    @parse
    def do_cd(self, name=""):
        # find the folder
        try:
            self.state.cd(name)
        except pw.DoesNotExist:
            click.secho(f"Folder {name} does not exist", fg="red")
        self.prompt = f"({APP_NAME} > {self.state.cwd.path}) "

    def complete_cd(self, text, line, begidx, endidx):
        args = shlex.split(line)
        path = args[1]
        return self.complete_path(path)

    @parse
    def do_rm(self, name):
        try:
            if self.state.rm(name, lambda: click.confirm(f"Sure you want to delete {name}?")):
                click.echo(f"{name} is gone")
        except state.CannotRemoveRoot:
            click.secho("Cannot delete root folder", fg="red")
        except pw.DoesNotExist:
            click.secho(f"Folder {name} does not exist", fg="red")

    def complete_rm(self, text, line, begidx, endidx):
        args = shlex.split(line)
        path = args[1]
        return self.complete_path(path)

    @parse
    def do_cwd(self):
        "Show the current location"
        click.echo(self.state.cwd.path)

    def do_exit(self, arg):
        return True

    def do_EOF(self, arg):
        return self.do_exit(arg)

    def preloop(self):
        if os.path.exists(history_file):
            logger.debug("Loading history from %s", history_file)
            readline.read_history_file(history_file)
        else:
            logger.debug("No history file found")

    def postloop(self):
        logger.debug(
            "Writing history of length %d to file %s",
            self.state.config.history_length,
            history_file,
        )
        readline.set_history_length(self.state.config.history_length)
        readline.write_history_file(history_file)

    def cmdloop(self):
        print(self.intro)
        while True:
            try:
                super().cmdloop(intro="")
                break
            except KeyboardInterrupt:
                print("^C")

    def emptyline(self):
        # do nothing when called with empty
        pass

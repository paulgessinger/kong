import functools
import shlex
import cmd
import readline

import click
import peewee as pw

from .config import APP_NAME, APP_DIR
from .logger import logger
from .model import *

history_file = os.path.join(APP_DIR, "history")


def parse(f):
    @functools.wraps(f)
    def wrapper(self, args):
        return f(self, *shlex.split(args))

    return wrapper


class Repl(cmd.Cmd):
    intro = f"This is {APP_NAME} shell"
    prompt = f"({APP_NAME} > /) "

    def __init__(self, config):
        self.config = config
        self.cwd = "/"
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
            folder = Folder.find_by_path(self.config.cwd, path)
            prefix = ""
        else:
            head, prefix = os.path.split(path)
            folder = Folder.find_by_path(self.config.cwd, head)

        options = []
        for child in folder.children:
            if child.name.startswith(prefix):
                options.append(child.name + "/")
        return options

    @parse
    def do_ls(self, arg="."):
        "List the current directory content"
        try:
            folder = Folder.find_by_path(self.config.cwd, arg)
            if folder is None:
                raise pw.DoesNotExist()
            for child in folder.children:
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
        # check if exists in this folder
        head, tail = os.path.split(path)

        location = Folder.find_by_path(self.config.cwd, head)
        if location is None:
            click.secho(f"Cannot create folder at '{path}'", fg="red")
            return
        logger.debug("Attempt to create folder named '%s' in '%s'", tail, location.path)

        try:
            folder = Folder.create(name=tail, parent=location)
        except pw.IntegrityError:
            click.secho(
                f"Folder {name} in {self.config.cwd.path} already exists", fg="red"
            )

    def complete_mkdir(self, text, line, begidx, endidx):
        args = shlex.split(line)
        path = args[1]
        return self.complete_path(path)

    @parse
    def do_cd(self, name):
        # find the folder
        try:
            folder = Folder.find_by_path(self.config.cwd, name)
            if folder is None:
                raise pw.DoesNotExist()
            self.config.cwd = folder
        except pw.DoesNotExist:
            click.secho(f"Folder {name} does not exist", fg="red")
        self.prompt = f"({APP_NAME} > {self.config.cwd.path}) "

    def complete_cd(self, text, line, begidx, endidx):
        args = shlex.split(line)
        path = args[1]
        return self.complete_path(path)

    @parse
    def do_rm(self, name):
        if name == "/":
            click.secho("Cannot delete root folder", fg="red")
            return
        try:
            folder = Folder.find_by_path(self.config.cwd, name)
            if folder is None:
                raise pw.DoesNotExist()
            path = folder.path
            if click.confirm(f"Sure you want to delete {path}?"):
                folder.delete_instance(recursive=True, delete_nullable=True)
                click.echo(f"{path} is gone")
        except pw.DoesNotExist:
            click.secho(f"Folder {name} does not exist", fg="red")

    def complete_rm(self, text, line, begidx, endidx):
        args = shlex.split(line)
        path = args[1]
        return self.complete_path(path)

    @parse
    def do_cwd(self):
        "Show the current location"
        click.echo(self.config.cwd.path)

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
            self.config.history_length,
            history_file,
        )
        readline.set_history_length(self.config.history_length)
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

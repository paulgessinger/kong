import argparse
import datetime
import functools
import logging
import shlex
import cmd
import readline
import os
from typing import Any, Callable, List, Tuple, Optional, Union
import shutil

import click
import peewee as pw
from click import style

from .util import rjust
from .state import DoesNotExist
from .config import APP_NAME, APP_DIR
from .logger import logger
from .model import *
from . import state

history_file = os.path.join(APP_DIR, "history")


def parse(f: Any) -> Callable[[str], Any]:
    @functools.wraps(f)
    def wrapper(self: Any, args: str = "") -> Any:
        return f(self, *shlex.split(args))

    return wrapper


class Repl(cmd.Cmd):
    intro = f"This is {APP_NAME} shell"
    prompt = f"({APP_NAME} > /) "

    def __init__(self, state: state.State) -> None:
        self.state = state
        super().__init__()

    def precmd(self, line: str) -> str:
        if line != "":
            logger.debug("called '%s'", line)
        return line

    def onecmd(self, *args: str) -> bool:
        try:
            return super().onecmd(*args)
        except (BaseException, Exception) as e:
            logger.debug("Exception occured", exc_info=True)
            click.secho(f"{e}", fg="red")
        return False

    def complete_path(self, path: str) -> List[str]:
        if path.endswith("/"):
            folder = Folder.find_by_path(self.state.cwd, path)
            prefix = ""
        else:
            head, prefix = os.path.split(path)
            folder = Folder.find_by_path(self.state.cwd, head)

        assert folder is not None
        options = []
        for child in folder.children:
            if child.name.startswith(prefix):
                options.append(child.name + "/")
        return options

    def do_ls(self, arg: str = "") -> None:
        "List the current directory content"
        argv = shlex.split(arg)
        p = argparse.ArgumentParser()
        p.add_argument("dir", default=".", nargs="?")
        p.add_argument("--refresh", "-r", action="store_true")
        p.add_argument("--recursive", "-R", action="store_true")

        try:
            args = p.parse_args(argv)
            folders, jobs = self.state.ls(
                args.dir,
                refresh=args.refresh or args.recursive,
                recursive=args.recursive,
            )
            width, height = shutil.get_terminal_size((80, 40))

            folder_name_length = 0
            if len(folders) > 0:
                folder_name_length = max([len(f.name) for f in folders])

            headers = ("name", "job counts")

            folder_name_length = max(folder_name_length, len(headers[0]))

            click.echo(
                headers[0].ljust(folder_name_length)
                + " "
                + headers[1].rjust(width - folder_name_length - 1)
            )

            click.echo(
                "-" * folder_name_length + " " + "-" * (width - folder_name_length - 1)
            )

            color_dict = {
                Job.Status.UNKOWN: "red",
                Job.Status.CREATED: "black",
                Job.Status.SUBMITTED: "yellow",
                Job.Status.RUNNING: "blue",
                Job.Status.FAILED: "red",
                Job.Status.COMPLETED: "green",
            }

            for folder in folders:
                folder_jobs = folder.jobs_recursive()
                # accumulate counts
                # @TODO: SLOW! Optimize to query
                counts = {
                    Job.Status.UNKOWN: 0,
                    Job.Status.CREATED: 0,
                    Job.Status.SUBMITTED: 0,
                    Job.Status.RUNNING: 0,
                    Job.Status.FAILED: 0,
                    Job.Status.COMPLETED: 0,
                }
                for job in folder_jobs:
                    counts[job.status] += 1

                output = ""
                for (k, c), l in zip(counts.items(), "UCSRFC"):
                    output += style(f" {c:> 6d}{l}", fg=color_dict[k])
                output = folder.name.ljust(folder_name_length) + rjust(
                    output, width - folder_name_length
                )

                click.echo(output)

            click.echo("")

            headers_jobs = ("job id", "batch job id", "created", "updated", "status")

            name_length = 0
            if len(jobs) > 0:
                name_length = max(name_length, max([len(str(j.job_id)) for j in jobs]))
            name_length = max(name_length, len(headers_jobs[0]))

            status_len = len("SUBMITTED")
            status_len = max(status_len, len(headers_jobs[2]))

            def dtfmt(dt: datetime.datetime) -> str:
                return dt.strftime("%Y-%m-%d %H:%M:%S")

            datetime_len = len(dtfmt(jobs[0].updated_at))

            bjobid_len = (
                width - name_length - status_len - 2 * datetime_len - len(headers)
            )
            bjobid_len = max(bjobid_len, len(headers[1]))

            click.echo(
                headers_jobs[0].rjust(name_length)
                + " "
                + headers_jobs[1].rjust(bjobid_len)
                + " "
                + headers_jobs[2].ljust(datetime_len)
                + " "
                + headers_jobs[3].ljust(datetime_len)
                + " "
                + headers_jobs[4].ljust(status_len)
            )
            click.echo(
                "-" * name_length
                + " "
                + "-" * bjobid_len
                + " "
                + "-" * datetime_len
                + " "
                + "-" * datetime_len
                + " "
                + "-" * status_len
            )

            for job in jobs:
                job_id = str(job.job_id).rjust(name_length)
                batch_job_id = job.batch_job_id.rjust(bjobid_len)
                _, status_name = str(job.status).split(".", 1)
                color = color_dict[job.status]

                click.secho(
                    f"{job_id} {batch_job_id} {dtfmt(job.created_at)} {dtfmt(job.updated_at)} {status_name}",
                    fg=color,
                )

            # for folder in folders:
            #     click.echo(folder.name)
            # for job in jobs:
            #     click.echo(str(job))

        except SystemExit as e:
            if e.code != 0:
                click.secho("Error parsing arguments", fg="red")
                p.print_help()
        except pw.DoesNotExist:
            click.secho(f"Folder {arg} does not exist", fg="red")

    def complete_ls(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        args = shlex.split(line)
        path = args[1]
        return self.complete_path(path)

    @parse
    def do_mkdir(self, path: str) -> None:
        "Create a directory at the current location"
        try:
            self.state.mkdir(path)
        except state.CannotCreateError:
            click.secho(f"Cannot create folder at '{path}'", fg="red")
        except pw.IntegrityError:
            click.secho(
                f"Folder {path} in {self.state.cwd.path} already exists", fg="red"
            )

    def complete_mkdir(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> List[str]:
        args = shlex.split(line)
        path = args[1]
        return self.complete_path(path)

    @parse
    def do_cd(self, name: str = "") -> None:
        # find the folder
        try:
            self.state.cd(name)
        except pw.DoesNotExist:
            click.secho(f"Folder {name} does not exist", fg="red")
        self.prompt = f"({APP_NAME} > {self.state.cwd.path}) "

    def complete_cd(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        args = shlex.split(line)
        path = args[1]
        return self.complete_path(path)

    def do_mv(self, arg: str) -> None:
        argv = shlex.split(arg)
        p = argparse.ArgumentParser()
        p.add_argument("src")
        p.add_argument("dest")

        try:
            args = p.parse_args(argv)
            items: List[Union[Job, Folder]] = self.state.mv(args.src, args.dest)
            names = []
            for item in items:
                if isinstance(item, Job):
                    names.append(str(item.job_id))
                else:
                    names.append(item.name)

            click.secho(f"Moved {', '.join(names)} -> {args.dest}")
        except SystemExit as e:
            if e.code != 0:
                click.secho("Error parsing arguments", fg="red")
                p.print_help()

    def do_info(self, arg: str) -> None:
        argv = shlex.split(arg)
        p = argparse.ArgumentParser()
        p.add_argument("job")

        try:
            args = p.parse_args(argv)
            jobs = self.state.get_jobs(args.job)
            assert len(jobs) == 1
            job = jobs[0]

            click.echo(job)
            for field in (
                "driver",
                "folder",
                "command",
                "cores",
                "status",
                "created_at",
                "updated_at",
            ):
                click.echo(f"{field}: {str(getattr(job, field))}")
            click.echo("data:")
            for k, v in job.data.items():
                click.echo(f"{k}: {v}")

        except SystemExit as e:
            if e.code != 0:
                click.secho("Error parsing arguments", fg="red")
                p.print_help()

    @parse
    def do_rm(self, name: str) -> None:
        try:
            if self.state.rm(name, lambda s: click.confirm(s)):
                click.echo(f"{name} is gone")
        except state.CannotRemoveRoot:
            click.secho("Cannot delete root folder", fg="red")
        except DoesNotExist:
            click.secho(f"Folder {name} does not exist", fg="red")

    def complete_rm(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        args = shlex.split(line)
        path = args[1]
        return self.complete_path(path)

    @parse
    def do_cwd(self) -> None:
        "Show the current location"
        click.echo(self.state.cwd.path)

    def do_create_job(self, arg: str) -> None:
        argv = shlex.split(arg)
        p = argparse.ArgumentParser()
        p.add_argument("--cores", "-c", type=int, default=1)
        p.add_argument("command", nargs=argparse.REMAINDER)

        try:
            args = p.parse_args(argv)
            if len(args.command) == 0:
                click.secho("Please provide a command to run", fg="red")
                p.print_help()
                return
            if args.command[0] == "--":
                del args.command[0]
            args.command = " ".join(args.command)

            job = self.state.create_job(**vars(args))
            click.secho(f"Created job {job}")
        except SystemExit as e:
            if e.code != 0:
                click.secho("Error parsing arguments", fg="red")
                p.print_help()

    def do_submit_job(self, arg: str) -> None:
        argv = shlex.split(arg)
        p = argparse.ArgumentParser()
        p.add_argument("job_id")

        try:
            args = p.parse_args(argv)
            self.state.submit_job(args.job_id)

        except SystemExit as e:
            if e.code != 0:
                click.secho("Error parsing arguments", fg="red")
                p.print_help()

    def complete_submit_job(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> List[str]:
        args = shlex.split(line)
        path = args[1]
        return self.complete_path(path)

    def do_kill_job(self, arg: str) -> None:
        argv = shlex.split(arg)
        p = argparse.ArgumentParser()
        p.add_argument("job_id")

        try:
            args = p.parse_args(argv)
            self.state.kill_job(args.job_id)

        except SystemExit as e:
            if e.code != 0:
                click.secho("Error parsing arguments", fg="red")
                p.print_help()

    def do_resubmit_job(self, arg: str) -> None:
        argv = shlex.split(arg)
        p = argparse.ArgumentParser()
        p.add_argument("job_id")

        try:
            args = p.parse_args(argv)
            self.state.resubmit_job(args.job_id)

        except SystemExit as e:
            if e.code != 0:
                click.secho("Error parsing arguments", fg="red")
                p.print_help()

    def do_status(self, arg: str) -> None:
        argv = shlex.split(arg)
        p = argparse.ArgumentParser()
        p.add_argument("job_id")
        p.add_argument("--refresh", "-r", action="store_true")

        try:
            args = p.parse_args(argv)
            jobs = self.state.get_jobs(args.job_id)

            if args.refresh:
                self.state.refresh_jobs(jobs)

            for job in jobs:
                click.echo(f"{job}")

        except SystemExit as e:
            if e.code != 0:
                click.secho("Error parsing arguments", fg="red")
                p.print_help()

    def do_update(self, arg: str) -> None:
        argv = shlex.split(arg)
        p = argparse.ArgumentParser()
        p.add_argument("job_id")

        try:
            args = p.parse_args(argv)
            jobs = self.state.get_jobs(args.job_id)
            self.state.refresh_jobs(jobs)

        except SystemExit as e:
            if e.code != 0:
                click.secho("Error parsing arguments", fg="red")
                p.print_help()

    def do_exit(self, arg: str) -> bool:
        return True

    def do_EOF(self, arg: str) -> bool:
        return self.do_exit(arg)

    def preloop(self) -> None:
        if os.path.exists(history_file):
            logger.debug("Loading history from %s", history_file)
            readline.read_history_file(history_file)
        else:
            logger.debug("No history file found")

    def postloop(self) -> None:
        logger.debug(
            "Writing history of length %d to file %s",
            self.state.config.history_length,
            history_file,
        )
        readline.set_history_length(self.state.config.history_length)
        readline.write_history_file(history_file)

    def cmdloop(self, intro: Optional[Any] = None) -> Any:
        print(self.intro)
        while True:
            try:
                super().cmdloop(intro="")
                break
            except KeyboardInterrupt:
                print("^C")

    def emptyline(self) -> bool:
        # do nothing when called with empty
        pass

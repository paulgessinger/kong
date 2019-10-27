import datetime
import shlex
import cmd
import readline
import os
import sys
from typing import Any, Callable, List, Optional, Union, Iterable
import shutil

import click
import peewee as pw
from click import style
from kong.model.job import color_dict

from .util import rjust, shorten_path, Spinner
from .state import DoesNotExist
from .config import APP_NAME, APP_DIR
from .logger import logger
from .model import Job, Folder
from . import state

history_file = os.path.join(APP_DIR, "history")


def complete_path(cwd: Folder, path: str) -> List[str]:
    logger.debug("Completion of '%s'", path)
    if path.endswith("/"):
        folder = Folder.find_by_path(cwd, path)
        prefix = ""
    else:
        head, prefix = os.path.split(path)
        folder = Folder.find_by_path(cwd, head)

    assert folder is not None
    options = []
    for child in folder.children:
        if child.name.startswith(prefix):
            options.append(child.name + "/")
    return options


def add_completion(*names: str) -> Callable[[type], type]:
    def decorator(cls: type) -> type:
        for name in names:
            method_name = f"complete_{name}"

            def handler(
                self: type, text: str, line: str, begidx: int, endidx: int
            ) -> List[str]:
                logger.debug(
                    "%s: text: %s, line: %s, begidx: %d, endidx: %d",
                    method_name,
                    text,
                    line,
                    begidx,
                    endidx,
                )

                # find component
                parts = shlex.split(line)
                base: Optional[str] = None
                for i, part in enumerate(parts):  # pragma: no branch
                    prelength = len(" ".join(parts[: i + 1]))
                    if prelength >= begidx:
                        base = part
                        break
                assert base is not None, "Error extracting active part"

                return complete_path(self.state.cwd, base)  # type: ignore

            setattr(cls, method_name, handler)
        return cls

    return decorator


def parse_arguments(fn: Any) -> Callable[[Any, str], None]:
    _, prog_name = fn.__name__.split("_", 1)

    fn = click.pass_obj(fn)
    command = click.command()(fn)

    def wrapped(self: Any, argstr: str) -> None:
        argv = shlex.split(argstr)
        logger.debug("%s", argv)
        try:
            command.main(
                args=argv,
                prog_name=prog_name,
                standalone_mode=False,
                obj=self,
                help_option_names=["-h", "--help"],
            )
        except click.MissingParameter:
            click.echo(f"usage: {fn.__doc__}")

    wrapped.__doc__ = fn.__doc__  # type: ignore
    wrapped.__name__ = fn.__name__  # type: ignore

    return wrapped


@add_completion(
    "ls",
    "mkdir",
    "mv",
    "rm",
    "cd",
    "submit_job",
    "kill_job",
    "info",
    "resubmit_job",
    "wait",
)
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
            res = super().onecmd(*args)
            logger.debug(
                "Writing history of length %d to file %s",
                self.state.config.history_length,
                history_file,
            )
            readline.set_history_length(self.state.config.history_length)
            readline.write_history_file(history_file)
            return res
        except (BaseException, Exception) as e:
            logger.debug("Exception occured", exc_info=True)
            click.secho(f"{e}", fg="red")
        return False

    @parse_arguments
    @click.argument("dir", default="", required=False)
    @click.option("--refresh", "-r", is_flag=True)
    @click.option("--recursive", "-R", is_flag=True)
    def do_ls(self, dir: str, refresh: bool, recursive: bool) -> None:
        "List the current directory content"
        try:
            width, height = shutil.get_terminal_size((80, 40))

            with Spinner("Getting info", persist=False, enabled=refresh or recursive):
                folders, jobs = self.state.ls(dir, refresh=refresh)

                if recursive:
                    arg_folder = Folder.find_by_path(self.state.cwd, dir)
                    assert arg_folder is not None  # should be a folder
                    self.state.refresh_jobs(arg_folder.jobs_recursive())
                    # refresh folder jobs
                    for folder in folders:
                        self.state.refresh_jobs(folder.jobs_recursive())
                    folders, jobs = self.state.ls(dir, refresh=False)

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
                    "-" * folder_name_length
                    + " "
                    + "-" * (width - folder_name_length - 1)
                )

                for folder in folders:
                    folder_jobs = folder.jobs_recursive()
                    # accumulate counts
                    # @TODO: SLOW! Optimize to query
                    counts = {k: 0 for k in Job.Status}

                    for job in folder_jobs:
                        counts[job.status] += 1

                    output = ""
                    for k, c in counts.items():
                        output += style(f" {c:> 6d}{k.name[:1]}", fg=color_dict[k])
                    output = folder.name.ljust(folder_name_length) + rjust(
                        output, width - folder_name_length
                    )

                    click.echo(output)

                click.echo("")

            if len(jobs) > 0:
                headers_jobs = (
                    "job id",
                    "batch job id",
                    "created",
                    "updated",
                    "status",
                )

                name_length = max(
                    max([len(str(j.job_id)) for j in jobs]), len(headers_jobs[0])
                )

                status_len = len("SUBMITTED")
                status_len = max(status_len, len(headers_jobs[-1]))

                def dtfmt(dt: datetime.datetime) -> str:
                    return dt.strftime("%Y-%m-%d %H:%M:%S")

                datetime_len = len(dtfmt(jobs[0].updated_at))

                bjobid_len = (
                    width
                    - name_length
                    - status_len
                    - 2 * datetime_len
                    - len(headers_jobs)
                )
                bjobid_len = max(bjobid_len, len(headers_jobs[1]))

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
                    if job.batch_job_id is None:
                        batch_job_id = " " * bjobid_len
                    else:
                        batch_job_id = job.batch_job_id.rjust(bjobid_len)
                    _, status_name = str(job.status).split(".", 1)
                    color = color_dict[job.status]

                    # table.append_row(())

                    click.secho(
                        f"{job_id} {batch_job_id} {dtfmt(job.created_at)} {dtfmt(job.updated_at)} {status_name}",
                        fg=color,
                    )

        except pw.DoesNotExist:
            click.secho(f"Folder {dir} does not exist", fg="red")

    @parse_arguments
    @click.argument("path")
    @click.option("--create-parent", "-p", is_flag=True)
    def do_mkdir(self, path: str, create_parent: bool) -> None:
        "Create a directory at the current location"
        try:
            self.state.mkdir(path, create_parent=create_parent)
        except state.CannotCreateError:
            click.secho(f"Cannot create folder at '{path}'", fg="red")
        except pw.IntegrityError:
            click.secho(
                f"Folder {path} in {self.state.cwd.path} already exists", fg="red"
            )

    @parse_arguments
    @click.argument("name", required=False, default="")
    def do_cd(self, name: str = "") -> None:
        # find the folder
        try:
            self.state.cd(name)
        except pw.DoesNotExist:
            click.secho(f"Folder {name} does not exist", fg="red")
        self.prompt = f"({APP_NAME} > {shorten_path(self.state.cwd.path, 40)}) "

    @parse_arguments
    @click.argument("src")
    @click.argument("dest")
    def do_mv(self, src: str, dest: str) -> None:
        items: List[Union[Job, Folder]] = self.state.mv(src, dest)
        names = []
        for item in items:
            if isinstance(item, Job):
                names.append(str(item.job_id))
            else:
                names.append(item.name)

        click.secho(f"Moved {', '.join(names)} -> {dest}")

    @parse_arguments
    @click.argument("job_arg")
    @click.option("--refresh", "-r", is_flag=True)
    @click.option("--recursive", "-R", is_flag=True)
    @click.option("--full", is_flag=True)
    def do_info(self, job_arg: str, refresh: bool, recursive: bool, full: bool) -> None:
        jobs = self.state.get_jobs(job_arg, recursive)
        if refresh:
            jobs = list(self.state.refresh_jobs(jobs))

        for job in jobs:
            click.echo(job)
            for field in (
                "batch_job_id",
                "driver",
                "folder",
                "command",
                "cores",
                "status",
                "created_at",
                "updated_at",
            ):
                fg: Optional[str] = None
                if field == "status":
                    fg = color_dict[job.status]
                if field == "command" and not full:
                    cmd = job.command
                    if len(cmd) > 500:
                        cmd = cmd[:500] + "..."
                    click.secho(f"{field}: {cmd}", fg=fg)
                    continue

                click.secho(f"{field}: {str(getattr(job, field))}", fg=fg)
            click.echo("data:")
            for k, v in job.data.items():
                click.secho(f"{k}: {v}")

    @parse_arguments
    @click.argument("job")
    @click.option("--recursive", "-R", is_flag=True)
    def do_rm(self, job: str, recursive: bool) -> None:
        try:
            if self.state.rm(
                job, recursive=recursive, confirm=lambda s: click.confirm(s)
            ):
                click.echo(f"{job} is gone")
        except state.CannotRemoveRoot:
            click.secho("Cannot delete root folder", fg="red")
        except DoesNotExist:
            click.secho(f"Folder {job} does not exist", fg="red")

    def do_cwd(self, *arg: Any) -> None:
        "Show the current location"
        click.echo(self.state.cwd.path)

    @parse_arguments
    @click.argument("command", nargs=-1)
    @click.option("--cores", "-c", type=int, default=1)
    def do_create_job(self, command: List[str], cores: int) -> None:
        if len(command) == 0:
            click.secho("Please provide a command to run", fg="red")
            return
        command_str = " ".join(command)

        job = self.state.create_job(command=command_str, cores=cores)
        click.secho(f"Created job {job}")

    @parse_arguments
    @click.argument("job")
    @click.option("--recursive", "-R", is_flag=True)
    def do_submit_job(self, job: str, recursive: bool) -> None:
        self.state.submit_job(job, click.confirm, recursive=recursive)

    @parse_arguments
    @click.argument("job")
    @click.option("--recursive", "-R", is_flag=True)
    def do_kill_job(self, job: str, recursive: bool) -> None:
        self.state.kill_job(job, recursive=recursive, confirm=click.confirm)

    @parse_arguments
    @click.argument("job")
    @click.option("--recursive", "-R", is_flag=True)
    @click.option("--failed", "-F", is_flag=True)
    def do_resubmit_job(self, job: str, recursive: bool, failed: bool) -> None:
        self.state.resubmit_job(
            job, click.confirm, recursive=recursive, failed_only=failed
        )

    @parse_arguments
    @click.argument("job_arg")
    @click.option("--refresh", "-r", is_flag=True)
    def do_status(self, job_arg: str, refresh: bool) -> None:
        """Print status of JOB"""
        jobs = self.state.get_jobs(job_arg)

        if refresh:
            self.state.refresh_jobs(jobs)

        for job in jobs:
            click.echo(f"{job}")

    @parse_arguments
    @click.argument("job")
    def do_update(self, job: str) -> None:
        jobs = self.state.get_jobs(job)
        self.state.refresh_jobs(jobs)

    @parse_arguments
    @click.argument("job_str")
    @click.option("--number-of-lines", "-n", default=20, type=int)
    def do_tail(self, job_str: str, number_of_lines: int) -> None:
        from sh import tail  # type: ignore

        jobs = self.state.get_jobs(job_str)
        assert len(jobs) == 1
        job = jobs[0]

        if not os.path.exists(job.data["stdout"]):
            raise ValueError(f"Job hasn't created stdout file yet {job.data['stdout']}")
        width, _ = shutil.get_terminal_size((80, 40))
        hw = width // 2
        click.echo("=" * hw + " STDOUT " + "=" * (width - hw - 8))
        for line in tail("-f", job.data["stdout"], n=number_of_lines, _iter=True):
            sys.stdout.write(line)

    @parse_arguments
    @click.argument("job_str")
    def do_less(self, job_str: str) -> None:
        jobs = self.state.get_jobs(job_str)
        assert len(jobs) == 1
        job = jobs[0]

        def reader() -> Iterable[str]:
            with open(job.data["stdout"]) as fp:
                line = fp.readline()
                yield line
                while line:
                    line = fp.readline()
                    yield line

        click.echo_via_pager(reader())

    @parse_arguments
    @click.argument("job_arg")
    @click.option("--notify/--no-notify", default=True)
    @click.option("--recursive", "-R", is_flag=True)
    @click.option("--poll-interval", "-i", type=int, default=None)
    def do_wait(
        self, job_arg: str, notify: bool, recursive: bool, poll_interval: int
    ) -> None:
        self.state.wait(
            job_arg, notify=notify, recursive=recursive, poll_interval=poll_interval
        )

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
        pass

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

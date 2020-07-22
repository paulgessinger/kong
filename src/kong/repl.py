import copy
import datetime
import shlex
import cmd
import readline
import os
import subprocess
import sys
import time
from concurrent.futures import wait, ThreadPoolExecutor

import humanfriendly
import sh
from typing import Any, Callable, List, Optional, Union, Iterable, cast, Dict, Tuple
import shutil
import dateutil.tz
import pytz

import click
import peewee as pw
from click import style
from kong.model.job import color_dict
from .table import format_table

from .util import shorten_path, Spinner, set_verbosity
from .state import DoesNotExist
from .config import APP_NAME, APP_DIR
from .logger import logger
from .model.job import Job
from .model.folder import Folder
from . import state

history_file = os.path.join(APP_DIR, "history")


def complete_path(cwd: Folder, path: str) -> List[str]:
    logger.debug("Completion of '%s'", path)
    if path.endswith("/"):
        folder = Folder.find_by_path(path, cwd)
        prefix = ""
    else:
        head, prefix = os.path.split(path)
        folder = Folder.find_by_path(head, cwd)

    assert folder is not None
    options = []
    for child in folder.children:
        if child.name.startswith(prefix):
            options.append(child.name + "/")
    return options


def parse_arguments(fn: Any) -> Callable[[Any, str], None]:
    _, prog_name = fn.__name__.split("_", 1)

    orig_fn = copy.deepcopy(fn)
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
    wrapped.__orig_fn__ = orig_fn  # type: ignore

    return wrapped


class Repl(cmd.Cmd):
    intro = f"This is {APP_NAME} shell"
    prompt = f"({APP_NAME} > /) "

    _raise: bool = False

    def __init__(self, state: state.State) -> None:
        self.state = state
        super().__init__()

    def precmd(self, line: str) -> str:
        if line != "":
            logger.debug("called '%s'", line)
        return line

    # For whatever reason, Cmd.completedefault has this signature...
    def completedefault(self, *args: Any) -> List[str]:
        # forward to typed implementation
        return self.completedefault_impl(*args)

    def completedefault_impl(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> List[str]:
        logger.debug(
            "text: %s, line: %s, begidx: %d, endidx: %d", text, line, begidx, endidx
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

    def onecmd(self, *args: str) -> bool:
        try:
            res = super().onecmd(*args)
            # logger.debug(
            #     "Writing history of length %d to file %s",
            #     self.state.config.history_length,
            #     history_file,
            # )
            readline.set_history_length(self.state.config.history_length)
            readline.write_history_file(history_file)
            return res
        except (BaseException, Exception) as e:
            logger.debug("Exception occured", exc_info=True)
            click.secho(f"{e}", fg="red")
            if self._raise:
                raise e
        return False

    @parse_arguments
    @click.argument("dir", default="", required=False)
    @click.option(
        "--refresh",
        "-R",
        is_flag=True,
        help="Synchronize status on all jobs selected for display",
    )
    @click.option(
        "--recursive",
        "-r",
        is_flag=True,
        help="Recursively select all jobs from target directory",
    )
    @click.option(
        "--show-sizes",
        "-s",
        is_flag=True,
        help="Collect size of job outputs. Note: this can potentially take a while.",
    )
    @click.option(
        "--status",
        "-S",
        "status_filter_str",
        type=click.Choice([s.name for s in Job.Status]),
        help="Only list jobs with this status. Will still show other jobs in folder summaries",
    )
    @click.option(
        "--extra",
        "-e",
        "extra_columns",
        type=str,
        default="",
        help="Additional columns to grab from jobs' data dict. Comma separated list.",
    )
    def do_ls(
        self,
        dir: str,
        refresh: bool,
        recursive: bool,
        show_sizes: bool,
        status_filter_str: Optional[str],
        extra_columns: str,
    ) -> None:
        "List the directory content of DIR: jobs and folders"
        try:
            ex: Optional[ThreadPoolExecutor] = None
            if show_sizes:
                ex = ThreadPoolExecutor()

            folders, jobs = self.state.ls(dir, refresh=refresh)

            _extra_columns = extra_columns.split(",") if extra_columns != "" else []

            _extra_columns = self.state.config.repl_extra_columns + _extra_columns

            logger.debug("Extra columns: %s", _extra_columns)

            if recursive:
                arg_folder = Folder.find_by_path(dir, self.state.cwd)
                assert arg_folder is not None  # should be a folder
                jobs = list(arg_folder.jobs_recursive())

            with Spinner("Refreshing jobs", persist=False, enabled=refresh):

                if refresh:
                    jobs = cast(list, self.state.refresh_jobs(jobs))

            def get_size(job: Job) -> int:
                return job.size(cast(ThreadPoolExecutor, ex))

            def get_folder_size(folder: Folder) -> int:
                # print("get folder size: ", folder.path)
                return sum(
                    cast(ThreadPoolExecutor, ex).map(get_size, folder.jobs_recursive())
                )

            folder_sizes: List[int] = []
            jobs_sizes: List[int] = []
            if show_sizes:
                ex_ = cast(ThreadPoolExecutor, ex)
                with Spinner("Calculating output sizes", persist=False):
                    folder_size_futures = []
                    for folder in folders:
                        folder_size_futures.append(ex_.submit(get_folder_size, folder))

                    job_size_futures = []
                    for job in jobs:
                        job_size_futures.append(ex_.submit(get_size, job))

                    wait(folder_size_futures)
                    folder_sizes = [f.result() for f in folder_size_futures]
                    wait(job_size_futures)
                    jobs_sizes = [f.result() for f in job_size_futures]

            if len(folders) > 0:
                with Spinner("Collection folder information", persist=False):
                    headers = ["name"]
                    align = ["l+"]

                    if show_sizes:
                        headers.append("output size")
                        align = ["l", "l+"]

                    for s in Job.Status:
                        headers.append(click.style(s.name, fg=color_dict[s]))
                        align.append("r")

                rows = []
                for idx, folder in enumerate(folders):
                    counts = folder.job_stats()

                    output = ""
                    for k, c in counts.items():
                        output += style(f" {c:> 6d}{k.name[:1]}", fg=color_dict[k])

                    row = [folder.name]
                    if show_sizes:
                        row.append(humanfriendly.format_size(folder_sizes[idx]))
                    # row += [output]
                    for k, c in counts.items():
                        row.append(click.style(str(c), fg=color_dict[k]))

                    rows.append(tuple(row))

                click.echo(format_table(tuple(headers), rows, align=tuple(align)))

            if len(folders) > 0 and len(jobs) > 0:
                print()

            if len(jobs) > 0:
                with Spinner("Collection job information", persist=False):
                    headers = ["job id"]
                    align = ["l"]

                    if show_sizes:
                        headers.append("output size")
                        align.append("l")

                    for col in _extra_columns:
                        headers.append(col)
                        align.append("l")

                    headers += ["batch job id", "created", "updated", "status"]
                    align += ["r+", "l", "l", "l"]

                    def dfcnv(dt: datetime.datetime) -> datetime.datetime:
                        return dt.replace(tzinfo=pytz.utc).astimezone(
                            dateutil.tz.tzlocal()
                        )

                    tfmt = "%H:%M:%S"
                    dtfmt = f"%Y-%m-%d {tfmt}"

                    rows = []
                    status_filter = (
                        Job.Status[status_filter_str]
                        if status_filter_str is not None
                        else None
                    )
                    for idx, job in enumerate(jobs):

                        if status_filter is not None:
                            if job.status != status_filter:
                                continue

                        job_id = str(job.job_id)
                        batch_job_id = str(job.batch_job_id)
                        _, status_name = str(job.status).split(".", 1)
                        color = color_dict[job.status]
                        row = [job_id]
                        if show_sizes:
                            row.append(humanfriendly.format_size(jobs_sizes[idx]))

                        created_at = dfcnv(job.created_at)
                        updated_at = dfcnv(job.updated_at)

                        if created_at.date() == updated_at.date():
                            updated_at_str = updated_at.strftime(tfmt)
                        else:
                            updated_at_str = updated_at.strftime(dtfmt)

                        for col in _extra_columns:
                            row.append(job.data.get(col, "-"))

                        row += [
                            batch_job_id,
                            created_at.strftime(dtfmt),
                            updated_at_str,
                            status_name,
                        ]

                        rows.append(tuple(click.style(c, fg=color) for c in row))

                click.echo(format_table(tuple(headers), rows, align=tuple(align)))

                if show_sizes:
                    click.echo(
                        "Size of jobs listed above: "
                        + click.style(
                            humanfriendly.format_size(sum(jobs_sizes)), bold=True
                        )
                    )

        except pw.DoesNotExist:
            click.secho(f"Folder {dir} does not exist", fg="red")
        finally:
            if ex is not None:
                ex.shutdown()

    @parse_arguments
    @click.argument("path")
    @click.option(
        "--create-parent",
        "-p",
        is_flag=True,
        help="Create parent directories to 'path' if they don't exist",
    )
    def do_mkdir(self, path: str, create_parent: bool) -> None:
        "Create a directory at PATH"
        try:
            self.state.mkdir(path, create_parent=create_parent)
        except state.CannotCreateError:
            click.secho(f"Cannot create folder at '{path}'", fg="red")
        except pw.IntegrityError:
            click.secho(
                f"Folder {path} in {self.state.cwd.path} already exists", fg="red"
            )

    @parse_arguments
    @click.argument("path", required=False, default="")
    def do_cd(self, path: str = "") -> None:
        """
        Change current working directory into PATH.

        \b
        Examples:
            cd # root /
            cd . # current directory
            cd .. # parent directory
            cd another/folder # some folder from this directory
            cd ../another # path but starting from parent
        """
        # find the folder
        try:
            self.state.cd(path)
        except pw.DoesNotExist:
            click.secho(f"Folder {path} does not exist", fg="red")
        self.prompt = f"({APP_NAME} > {shorten_path(self.state.cwd.path, 40)}) "

    @parse_arguments
    @click.argument("src")
    @click.argument("dest")
    def do_mv(self, src: str, dest: str) -> None:
        """
        Move SRC into DEST.

        SRC can be either jobs or folders. If there are jobs in SRC, then
        DEST needs to be a folder that exists. If SRC is exactly one folder, and DEST
        does not exist, SRC will be renamed to DEST. If SRC is more than one folder,
        DEST needs to exist, and SRC folders will be moved into DEST.
        """
        items: List[Union[Job, Folder]] = self.state.mv(src, dest)
        names = []
        for item in items:
            if isinstance(item, Job):
                names.append(str(item.job_id))
            else:
                names.append(item.name)

        click.secho(f"Moved -> {dest}")

    @parse_arguments
    @click.argument("path")
    @click.option(
        "--refresh", "-R", is_flag=True, help="Do a refresh on all jobs found"
    )
    @click.option(
        "--recursive",
        "-r",
        is_flag=True,
        help="Go recursively through PATH to find jobs (if PATH is a folder)",
    )
    @click.option("--full", is_flag=True, help="Do not truncate job info if too long")
    def do_info(self, path: str, refresh: bool, recursive: bool, full: bool) -> None:
        """
        Show information on jobs from PATH.

        PATH can be a job id (range) or a folder.
        """
        jobs = self.state.get_jobs(path, recursive)
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
                if field == "folder":
                    click.echo(f"folder: {job.folder.path}")
                    continue
                if field == "command" and not full:
                    cmd = job.command
                    if len(cmd) > 500:
                        cmd = cmd[:500] + "..."
                    click.secho(f"{field}: {cmd}", fg=fg)
                    continue

                click.secho(f"{field}: {str(getattr(job, field))}", fg=fg)
            click.echo("data:")
            for k, v in job.data.items():
                click.secho(f"- {k}: {v}")

    @parse_arguments
    @click.argument("path")
    @click.option("--recursive", "-r", is_flag=True, help="Recursively delete")
    @click.option("--yes", "-y", is_flag=True)
    def do_rm(self, path: str, recursive: bool, yes: bool) -> None:
        """
        Delete item at PATH.

        If rm deletes jobs, cleanup routines will be run. Use `rm -r` to delete folders.
        """
        confirm: Callable[[str], bool] = click.confirm
        if yes:

            def confirm(arg: str) -> bool:
                return True  # pragma: no cover

        try:
            if self.state.rm(path, recursive=recursive, confirm=confirm):
                click.echo(f"{path} is gone")
        except state.CannotRemoveRoot:
            click.secho("Cannot delete root folder", fg="red")
        except DoesNotExist:
            click.secho(f"Folder {path} does not exist", fg="red")

    @parse_arguments
    def do_cwd(self) -> None:
        "Show the current location"
        click.echo(self.state.cwd.path)

    @parse_arguments
    def do_pwd(self) -> None:
        "Show the current location"
        click.echo(self.state.cwd.path)

    @parse_arguments
    @click.argument("command", nargs=-1)
    @click.option(
        "-a",
        "--argument",
        "arguments_raw",
        multiple=True,
        help="Provide extra arguments like `--argument name=value`",
    )
    def do_create_job(self, command: List[str], arguments_raw: List[str]) -> None:
        """
        Create a job with command COMMAND for processing.
        Additional arguments can be provided and are passed to the driver for verification.
        """
        if len(command) == 0:
            click.secho("Please provide a command to run", fg="red")
            return
        command_str = " ".join(command)

        logger.debug("Raw extra arguments: %s", arguments_raw)
        arg_str: Dict[str, str] = dict(
            [cast(Tuple[str, str], s.split("=", 1)) for s in arguments_raw]
        )

        # cast to int for numeric values
        # @TODO: This might need to be smarter at some point
        arguments: Dict[str, Union[str, int]] = {}
        for k, v in arg_str.items():
            if v.isdigit():
                arguments[k] = int(v)
            else:
                arguments[k] = v

        logger.debug("Got extra arguments: %s", arguments)

        job = self.state.create_job(command=command_str, **arguments)
        click.secho(f"Created job {job}")

    @parse_arguments
    @click.argument("path")
    @click.option("--recursive", "-r", is_flag=True, help="Search recursively for jobs")
    def do_submit_job(self, path: str, recursive: bool) -> None:
        """
        Submit job(s) at PATH.
        """
        self.state.submit_job(path, click.confirm, recursive=recursive)

    @parse_arguments
    @click.argument("path")
    @click.option("--recursive", "-r", is_flag=True, help="Search recursively for jobs")
    def do_kill_job(self, path: str, recursive: bool) -> None:
        """Kill job(s) at PATH"""
        self.state.kill_job(path, recursive=recursive, confirm=click.confirm)

    @parse_arguments
    @click.argument("path")
    @click.option("--recursive", "-r", is_flag=True, help="Search recursively for jobs")
    @click.option(
        "--failed", "-F", is_flag=True, help="Only resubmit jobs in status FAILED"
    )
    def do_resubmit_job(self, path: str, recursive: bool, failed: bool) -> None:
        """Resubmit jobs at PATH."""
        self.state.resubmit_job(
            path, click.confirm, recursive=recursive, failed_only=failed
        )

    @parse_arguments
    @click.argument("path", default=".")
    @click.option(
        "--recursive/--no-recursive",
        "-r/-f",
        default=True,
        help="Select jobs recursively",
    )
    def do_update(self, path: str, recursive: bool) -> None:
        """Update the job at PATH."""
        with Spinner("Updating jobs"):
            jobs = self.state.get_jobs(path, recursive=recursive)
            jobs = list(self.state.refresh_jobs(jobs))
        counts = {k: 0 for k in Job.Status}

        click.echo(f"{len(jobs)} job(s) updated")

        if len(jobs) > 1:
            for job in jobs:
                counts[job.status] += 1

            output = ""
            for k, c in counts.items():
                output += style(f" {c:> 6d}{k.name[:1]}", fg=color_dict[k])

            click.echo(output)

    @parse_arguments
    @click.argument("path")
    @click.option(
        "--number-of-lines",
        "-n",
        default=20,
        type=int,
        help="How many lines to print when starting",
    )
    def do_tail(self, path: str, number_of_lines: int) -> None:
        """
        Tail the stdout of the job at PATH.
        Will wait for the creation of the stdout file if it hasn't already been created.
        """
        from sh import tail  # type: ignore

        jobs = self.state.get_jobs(path)
        assert len(jobs) == 1
        job = jobs[0]

        if not os.path.exists(job.data["stdout"]):
            with Spinner(
                text=f"Waiting for job to create stdout file '{job.data['stdout']}'"
            ):
                while not os.path.exists(job.data["stdout"]):
                    time.sleep(1)
                logger.info("Outfile exists now")

        width, _ = shutil.get_terminal_size((80, 40))
        hw = width // 2
        click.echo("=" * hw + " STDOUT " + "=" * (width - hw - 8))

        def show(line: str) -> None:  # pragma: no cover
            sys.stdout.write(line)

        try:
            proc = tail(
                "-n",
                number_of_lines,
                "-f",
                job.data["stdout"],
                _bg=True,
                _bg_exc=False,
                _out=show,
            )
            try:
                proc.wait()
            except KeyboardInterrupt:  # pragma: no cover
                proc.terminate()
                proc.wait()

        except sh.SignalException_SIGTERM:  # pragma: no cover
            pass

    @parse_arguments
    @click.argument("path")
    def do_less(self, path: str) -> None:
        """Open program less on the stdout file of the job at PATH."""
        jobs = self.state.get_jobs(path)
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
    @click.argument("path", nargs=-1)
    @click.option(
        "--notify/--no-notify",
        default=True,
        help="Send a notification when wait terminates",
    )
    @click.option("--recursive", "-r", is_flag=True, help="Search recursively for jobs")
    @click.option(
        "--poll-interval",
        "-i",
        type=str,
        default=None,
        help="Interval with which to poll for job status updates",
    )
    @click.option(
        "--notify-interval",
        "-n",
        type=str,
        default="30m",
        help="Send periodic status updates with this interval",
    )
    def do_wait(
        self,
        path: List[str],
        notify: bool,
        recursive: bool,
        poll_interval: Optional[str],
        notify_interval: Optional[str],
    ) -> None:
        """
        Wait on the job(s) at PATH.

        This command will periodically poll the driver for updates on the given jobs,
        and will display a tally of the jobs' status.

        Note: notifications will only be sent if a notification provider is configured in
        the config file.
        """

        update_interval: Optional[datetime.timedelta]
        if (
            notify
            and notify_interval is not None
            and notify_interval not in ("none", "None")
        ):
            update_interval = datetime.timedelta(
                seconds=humanfriendly.parse_timespan(notify_interval)
            )
        else:
            update_interval = None
        logger.debug("Update interval is %s", update_interval)

        poll_interval_seconds: Optional[int] = None

        if poll_interval is not None:
            if poll_interval.isdigit():
                poll_interval_seconds = int(poll_interval)
            else:
                print(poll_interval)
                poll_interval_seconds = int(humanfriendly.parse_timespan(poll_interval))

        logger.debug("Poll interval is %s", poll_interval_seconds)

        self.state.wait(
            path,
            notify=notify,
            recursive=recursive,
            poll_interval=poll_interval_seconds,
            update_interval=update_interval,
        )

    @parse_arguments
    @click.argument("verbosity", type=int)
    def do_set_verbosity(self, verbosity: int) -> None:
        """
        Set verbosity level to VERBOSITY
        """
        if verbosity < 0:
            raise ValueError("Verbosity must be >= 0")
        set_verbosity(verbosity)

    def do_shell(self, cmd: str) -> None:
        """
        Run the command given in a shell.
        The environment variable $KONG_PWD is set to the current working directory
        """
        logger.debug("cmd: %s", cmd)
        env = os.environ.copy()
        env.update({"KONG_PWD": self.state.cwd.path})
        logger.debug("Expanded: %s", cmd)
        subprocess.run(cmd, shell=True, env=env)

    def do_exit(self, arg: str) -> bool:
        """Exit the repl"""
        return True

    def do_EOF(self, arg: str) -> bool:
        """Helper command to handle Ctrl+D"""
        return self.do_exit(arg)

    def preloop(self) -> None:
        if os.path.exists(history_file):
            # logger.debug("Loading history from %s", history_file)
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

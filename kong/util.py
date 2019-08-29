import re
import os
import shutil
import stat
from datetime import timedelta

from .logger import logger

strip = re.compile(r"\x1b(\[.*?[@-~]|\].*?(\x07|\x1b\\))")


def strip_colors(string: str) -> str:
    return strip.sub("", string)


def ljust(string: str, width: int, fillchar: str = " ") -> str:
    l = len(strip_colors(string))
    return string + (width - l) * fillchar


def rjust(string: str, width: int, fillchar: str = " ") -> str:
    l = len(strip_colors(string))
    return (width - l) * fillchar + string


def make_executable(path: str) -> None:
    mode = os.stat(path).st_mode
    os.chmod(path, mode | stat.S_IEXEC)


def is_executable(path: str) -> bool:
    mode = os.stat(path).st_mode
    return (mode & stat.S_IEXEC) != 0


def rmtree(path: str) -> None:
    # we'll try using shutil, and fall back to 'rm' if that fails
    try:
        shutil.rmtree(path)
    except OSError as e:
        logger.warning("shutil.rmtree failed: %s", e)
        os.system(f"rm -rf {path}")


def format_timedelta(delta: timedelta) -> str:
    if delta >= timedelta(hours=100):
        raise ValueError(f"{delta} is too large to format")

    days = delta.days
    hours, rem = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(rem, 60)

    total_hours = days * 24 + hours
    return f"{total_hours:02d}:{minutes:02d}:{seconds:02d}"


timedelta_regex = re.compile(r"\d\d:\d\d:\d\d")


def parse_timedelta(string: str) -> timedelta:
    if not timedelta_regex.match(string):
        raise ValueError(f"{string} does not have the right format")
    hours, minutes, seconds = string.split(":", 3)
    return timedelta(hours=int(hours), minutes=int(minutes), seconds=int(seconds))


import sys
import contextlib

from halo import Halo


@contextlib.contextmanager
def Spinner(text, persist=True, *args, **kwargs):
    stream = kwargs.get("stream", sys.stdout)
    if not "spinner" in kwargs:
        kwargs["spinner"] = "bouncingBar"
    if stream.isatty() and Halo is not None:
        spinner = Halo(text, *args, **kwargs)
        spinner.start()
        try:
            yield
            if persist:
                spinner.succeed()
        except:
            if persist:
                spinner.fail()
            raise
        finally:
            if not persist:
                spinner.stop()
    else:
        sys.stdout.write(text + "\n")
        yield


from tqdm import tqdm


def Progress(iter, *args, **kwargs):
    if sys.stdout.isatty():
        return tqdm(iter, *args, **kwargs)
    else:
        return iter

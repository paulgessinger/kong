import math
import re
import os
import shutil
import stat
from concurrent.futures._base import Executor, wait
from datetime import timedelta
import click
from typing import Optional, Any, TypeVar, Iterable, Iterator, List
import sys
import contextlib
from collections import deque

from tqdm import tqdm  # type: ignore
from halo import Halo  # type: ignore

from .logger import logger

strip = re.compile(r"\x1b(\[.*?[@-~]|\].*?(\x07|\x1b\\))")


def strip_colors(string: str) -> str:
    return strip.sub("", string)


def ljust(string: str, width: int, fillchar: str = " ") -> str:
    length = len(strip_colors(string))
    return string + (width - length) * fillchar


def rjust(string: str, width: int, fillchar: str = " ") -> str:
    length = len(strip_colors(string))
    return (width - length) * fillchar + string


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
        # @TODO: This is absolutely not portable, maybe fix
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


@contextlib.contextmanager
def Spinner(
    text: str, persist: bool = True, *args: Any, **kwargs: Any
) -> Iterator[Halo]:
    stream = kwargs.get("stream", sys.stdout)
    if "spinner" not in kwargs:
        kwargs["spinner"] = "line"
        kwargs["interval"] = 80
    if stream.isatty():
        spinner = Halo(text, *args, **kwargs)  # type: ignore
        spinner.start()
        try:
            yield spinner
            if persist:
                spinner.succeed()
        except:  # noqa: E722
            if persist:
                spinner.fail()
            raise
        finally:
            if not persist:
                spinner.stop()
    else:
        sys.stdout.write(text + "\n")
        yield


T = TypeVar("T")


def Progress(*args: Any, **kwargs: Any) -> Iterable[T]:
    return tqdm(*args, **kwargs)  # type: ignore


def shorten(string: str, length: int) -> str:
    ANSIRE = "\x1b\\[(?:K|.*?m)"

    if length <= 4:
        raise ValueError("Shortening to <= 4 does not make sense")
    if length >= len(click.unstyle(string)):
        return string

    m = re.match("^(" + ANSIRE + ").*(" + ANSIRE + "$)", string)
    if m is None:
        # Not a single style, unstyle all
        start = ""
        end = ""
    else:
        start = m.group(1)
        end = m.group(2)

    string = click.unstyle(string)

    leftover = length - 3

    left_length = max(1, math.floor(leftover / 2))

    left = string[:left_length]
    right = string[-(length - left_length - 3) :]

    return f"{start}{left}...{right}{end}"


def shorten_path(path: str, last_length: Optional[int] = None) -> str:
    parts = path.split("/")
    shortened = [s[:1] for s in parts[:-1]]
    basename = parts[-1]
    if last_length is not None:
        basename = shorten(basename, last_length)
    shortened.append(basename)
    return "/".join(shortened)


def chunks(items: List[T], nchunks: int) -> Iterable[List[T]]:
    for i in range(0, len(items), nchunks):
        yield items[i : i + nchunks]


def exhaust(generator: Iterable[Any]) -> None:
    deque(generator, maxlen=0)


def get_size(path: str, ex: Optional[Executor] = None) -> int:
    size = 0
    futures = []
    for d, _, files in os.walk(path):
        if ex is None:
            size += sum([os.path.getsize(os.path.join(d, f)) for f in files])
        else:
            for f in files:
                futures.append(ex.submit(os.path.getsize, os.path.join(d, f)))

    if ex is not None:
        wait(futures)
        size = sum(f.result() for f in futures)

    return size

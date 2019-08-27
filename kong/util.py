import re
import os
import stat

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

import re

strip = re.compile(r"\x1b(\[.*?[@-~]|\].*?(\x07|\x1b\\))")


def strip_colors(string: str) -> str:
    return strip.sub("", string)


def ljust(string: str, width: int, fillchar: str = " ") -> str:
    l = len(strip_colors(string))
    return string + (width - l) * fillchar


def rjust(string: str, width: int, fillchar: str = " ") -> str:
    l = len(strip_colors(string))
    return (width - l) * fillchar + string

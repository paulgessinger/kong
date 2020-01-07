from datetime import timedelta

from typing import Union


def format_timespan(seconds: Union[float, timedelta]) -> str:
    ...

def parse_timespan(arg: str) -> float:
    ...

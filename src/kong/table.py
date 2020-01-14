import shutil

import click
from typing import List, Tuple, Optional

from .util import shorten


def _do_align(s: str, align: str, width: int, padstr: str) -> str:
    length = len(click.unstyle(s))
    if length > width:
        s = shorten(s, width)

    if align == "l":
        return s + (width - length) * padstr
    elif align == "r":
        return (width - length) * padstr + s
    elif align == "c":
        left = (width - length) // 2
        right = width - length - left
        return padstr * left + s + padstr * right
    else:
        raise ValueError(f"Invalid align string '{align}'")


def _format_row(
    cols: Tuple[str, ...],
    widths: List[int],
    aligns: Tuple[str, ...],
    max_width: Optional[int] = None,
    padstr: str = " ",
) -> str:
    assert len(cols) == len(aligns), "Number of aligns must match columns"
    colstrs = ["" for _ in cols]
    stretch_col = None
    for idx, (col, width, align) in enumerate(zip(cols, widths, aligns)):
        if align.endswith("+"):
            stretch_col = idx
            colstr = _do_align(col, align[:-1], width, padstr)
        else:
            colstr = _do_align(col, align, width, padstr)

        colstrs[idx] = colstr

    if stretch_col is not None and max_width is not None:
        total_len = sum(
            len(click.unstyle(c)) for i, c in enumerate(colstrs) if i != stretch_col
        ) + (len(cols) - 1) * len(padstr)
        align = aligns[stretch_col][:-1]
        width = max_width - total_len

        col = cols[stretch_col]

        colstr = _do_align(col, align, width, padstr)

        colstrs[stretch_col] = colstr

    return " ".join(colstrs)


def format_table(
    headers: Tuple[str, ...],
    rows: List[Tuple[str, ...]],
    align: Tuple[str, ...],
    width: Optional[int] = None,
) -> str:
    assert len(headers) == len(align), "Number of aligns must match columns"
    if width is None and any("+" in a for a in align):
        width, _ = shutil.get_terminal_size((80, 40))

    col_widths = [len(click.unstyle(h)) for h in headers]
    for row in rows:
        for idx, col in enumerate(row):
            col_widths[idx] = max(col_widths[idx], len(click.unstyle(col)))

    output = ""
    output += _format_row(headers, col_widths, align, max_width=width) + "\n"

    output += (
        _format_row(
            tuple(["" for _ in col_widths]),
            col_widths,
            align,
            max_width=width,
            padstr="-",
        )
        + "\n"
    )

    for row in rows:
        output += _format_row(row, col_widths, align, max_width=width) + "\n"

    return output[:-1]

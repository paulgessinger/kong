import click

from kong.util import strip_colors, ljust, rjust


def test_strip_colors():
    s = "abcdef"
    assert strip_colors(s) == s

    assert strip_colors(click.style(s, fg="red")) == s


def test_ljust():
    s = "abcdlkjhdlfkjh"
    assert ljust(s, 30) == s.ljust(30)
    s_style = click.style(s, fg="red")
    assert ljust(s_style, 30)[-(30 - len(s)) :] == " " * (30 - len(s))


def test_rjust():
    s = "abcdlkjhdlfkjh"
    assert rjust(s, 30) == s.rjust(30)
    s_style = click.style(s, fg="red")
    assert rjust(s_style, 30)[: (30 - len(s))] == " " * (30 - len(s))

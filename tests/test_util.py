import logging
import os
import shutil
import stat
import subprocess
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import timedelta
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, ANY

import click
import pytest

from kong.util import (
    strip_colors,
    ljust,
    rjust,
    format_timedelta,
    parse_timedelta,
    shorten,
    shorten_path,
    make_executable,
    is_executable,
    rmtree,
    chunks,
    Spinner,
    Progress,
    get_size,
    set_verbosity,
)


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


def test_format_timedelta():
    fmt = format_timedelta
    assert fmt(timedelta(seconds=30)) == "00:00:30"
    assert fmt(timedelta(minutes=42)) == "00:42:00"
    assert fmt(timedelta(minutes=42, seconds=14)) == "00:42:14"
    assert fmt(timedelta(hours=8)) == "08:00:00"
    assert fmt(timedelta(hours=99)) == "99:00:00"
    assert fmt(timedelta(days=3)) == f"{3*24}:00:00"
    with pytest.raises(ValueError):
        fmt(timedelta(days=5))
    assert fmt(timedelta(hours=99, minutes=59, seconds=59)) == "99:59:59"
    with pytest.raises(ValueError):
        fmt(timedelta(hours=100))
    assert fmt(timedelta(hours=8, minutes=6)) == "08:06:00"
    assert fmt(timedelta(hours=8, minutes=6, seconds=23)) == "08:06:23"


def test_parse_timedelta():
    ptd = parse_timedelta
    assert ptd("00:00:30") == timedelta(seconds=30)
    assert ptd("00:42:00") == timedelta(minutes=42)
    assert ptd("00:42:14") == timedelta(minutes=42, seconds=14)
    assert ptd("08:00:00") == timedelta(hours=8)
    assert ptd("99:00:00") == timedelta(hours=99)
    assert ptd(f"{3 * 24}:00:00") == timedelta(days=3)
    with pytest.raises(ValueError):
        ptd("100:00:00")
    assert ptd("99:59:59") == timedelta(hours=99, minutes=59, seconds=59)
    assert ptd("08:06:00") == timedelta(hours=8, minutes=6)
    assert ptd("08:06:23") == timedelta(hours=8, minutes=6, seconds=23)


def test_shorten():
    assert shorten("abcabcabcabc", 5) == "a...c"
    assert shorten("abcabcabcabc", 6) == "a...bc"
    assert shorten("abcabcabcabc", 7) == "ab...bc"
    assert shorten("abcdefghijklmnopqrstuvwxyz", 8) == "ab...xyz"
    assert shorten("abcdefghijklmnopqrstuvwxyz", 9) == "abc...xyz"
    assert shorten("abcabcabcabc", 12) == "abcabcabcabc"
    assert shorten("abcabcabcabc", 15) == "abcabcabcabc"
    with pytest.raises(ValueError):
        shorten("abcdefgh", 4)  # doesn't make sense

    s = click.style("abcabcabcabc", bold=True)
    out = shorten(s, 6)
    assert len(click.unstyle(out)) == 6
    assert click.unstyle(out) == "a...bc"
    assert out != click.unstyle(out)

    # mixed, will be stripped
    s = click.style("abcabcabcab", bold=True) + "c"
    out = shorten(s, 6)
    assert len(click.unstyle(out)) == 6
    assert click.unstyle(out) == "a...bc"
    assert out == click.unstyle(out)


def test_shorten_path():
    assert (
        shorten_path("/a/very/long/path/with/many/segments") == "/a/v/l/p/w/m/segments"
    )
    assert shorten_path("a/very/long/path/with/many/segments") == "a/v/l/p/w/m/segments"

    assert (
        shorten_path("a/very/long/path/with/many/segments_is_very_long_too", 10)
        == "a/v/l/p/w/m/seg..._too"
    )


def test_make_executable(tmp_path):
    p = tmp_path.joinpath("test.sh")
    p.write_text("hallo")

    mode = os.stat(p).st_mode
    assert (mode & stat.S_IEXEC) == 0

    make_executable(p)
    mode = os.stat(p).st_mode
    assert (mode & stat.S_IEXEC) != 0


def test_is_executable(tmp_path):
    p = tmp_path.joinpath("test.sh")
    p.write_text("hallo")
    assert is_executable(p) == False

    os.chmod(p, os.stat(p).st_mode | stat.S_IEXEC)
    assert is_executable(p) == True


def test_rmtree(monkeypatch):
    with monkeypatch.context() as m:
        rmtree_mock = Mock(side_effect=OSError)
        m.setattr("shutil.rmtree", rmtree_mock)
        system = Mock()
        m.setattr("os.system", system)
        rmtree("whatever/blaaa")
        rmtree_mock.assert_called_once_with("whatever/blaaa")
        system.assert_called_once_with("rm -rf whatever/blaaa")

    with monkeypatch.context() as m:
        rmtree_mock = Mock()
        m.setattr("shutil.rmtree", rmtree_mock)
        system = Mock()
        m.setattr("os.system", system)
        rmtree("whatever/blaaa")
        rmtree_mock.assert_called_once_with("whatever/blaaa")
        assert system.call_count == 0


def test_chunks():
    l = [1, 2, 3, 4, 5, 6, 7]
    ch = list(chunks(l, 2))

    assert ch == [[1, 2], [3, 4], [5, 6], [7]]


def test_spinner(monkeypatch):

    with monkeypatch.context() as m:
        write = Mock()
        isatty = Mock(return_value=False)
        m.setattr("sys.stdout.isatty", isatty)
        m.setattr("sys.stdout.write", write)
        with Spinner(text="blub"):
            pass
        write.assert_called_once_with("blub\n")

    with monkeypatch.context() as m:
        HaloInstance = Mock()
        HaloInstance.start = Mock()
        HaloInstance.succeed = Mock()
        HaloInstance.fail = Mock()
        HaloInstance.stop = Mock()
        Halo = Mock(return_value=HaloInstance)

        isatty = Mock(return_value=True)
        m.setattr("sys.stdout.isatty", isatty)
        m.setattr("kong.util.Halo", Halo)

        with Spinner(text="blub"):
            pass

        assert HaloInstance.start.call_count == 1
        assert HaloInstance.succeed.call_count == 1
        assert HaloInstance.stop.call_count == 0
        assert HaloInstance.fail.call_count == 0
        Halo.assert_called_once()

    with monkeypatch.context() as m:
        HaloInstance = Mock()
        HaloInstance.start = Mock()
        HaloInstance.succeed = Mock()
        HaloInstance.fail = Mock()
        HaloInstance.stop = Mock()
        Halo = Mock(return_value=HaloInstance)

        isatty = Mock(return_value=True)
        m.setattr("sys.stdout.isatty", isatty)
        m.setattr("kong.util.Halo", Halo)

        with pytest.raises(RuntimeError):
            with Spinner(text="blub", spinner="dots"):
                raise RuntimeError()

        assert HaloInstance.start.call_count == 1
        assert HaloInstance.succeed.call_count == 0
        assert HaloInstance.stop.call_count == 0
        assert HaloInstance.fail.call_count == 1
        Halo.assert_called_once_with("blub", spinner="dots")

    with monkeypatch.context() as m:
        HaloInstance = Mock()
        HaloInstance.start = Mock()
        HaloInstance.succeed = Mock()
        HaloInstance.fail = Mock()
        HaloInstance.stop = Mock()
        Halo = Mock(return_value=HaloInstance)

        isatty = Mock(return_value=True)
        m.setattr("sys.stdout.isatty", isatty)
        m.setattr("kong.util.Halo", Halo)

        with pytest.raises(RuntimeError):
            with Spinner(text="blub", persist=False):
                raise RuntimeError()

        assert HaloInstance.start.call_count == 1
        assert HaloInstance.succeed.call_count == 0
        assert HaloInstance.stop.call_count == 1
        assert HaloInstance.fail.call_count == 0

    with monkeypatch.context() as m:
        HaloInstance = Mock()
        HaloInstance.start = Mock()
        HaloInstance.succeed = Mock()
        HaloInstance.fail = Mock()
        HaloInstance.stop = Mock()
        Halo = Mock(return_value=HaloInstance)

        isatty = Mock(return_value=True)
        m.setattr("sys.stdout.isatty", isatty)
        m.setattr("kong.util.Halo", Halo)

        with Spinner(text="blub", persist=False):
            pass

        assert HaloInstance.start.call_count == 1
        assert HaloInstance.succeed.call_count == 0
        assert HaloInstance.stop.call_count == 1
        assert HaloInstance.fail.call_count == 0


def test_progress(monkeypatch):
    tqdm = Mock(return_value=[])
    monkeypatch.setattr("kong.util.tqdm", tqdm)
    for i in Progress(range(10)):
        pass
    assert tqdm.call_count == 1


@pytest.fixture
def cleaned_tmpdir(tmpdir):
    sub = Path(tmpdir) / "subdir"
    sub.mkdir(exist_ok=True)
    shutil.rmtree(sub)
    sub.mkdir()
    yield sub


def test_get_size(cleaned_tmpdir):
    tmpdir = cleaned_tmpdir

    A = tmpdir / "A"
    B = tmpdir / "B"
    C = B / "C"

    A.mkdir()
    B.mkdir()
    C.mkdir()

    files = (
        [A / f"file_{i}" for i in range(4)]
        + [B / f"file_{i}" for i in range(8)]
        + [C / f"file_{i}" for i in range(2)]
    )

    for file in files:
        with file.open("wb") as fh:
            fh.write(b"X" * 1024)

    size = sum(os.path.getsize(f) for f in files)

    assert get_size(tmpdir) == size
    with ThreadPoolExecutor() as ex:
        assert get_size(tmpdir, ex) == size


def test_set_verbosity(monkeypatch):
    coloredlogs = Mock()
    monkeypatch.setattr("kong.util.coloredlogs", coloredlogs)

    logger = Mock()
    monkeypatch.setattr("kong.util.logger", logger)

    setLevel = Mock()
    monkeypatch.setattr(logging.getLogger(), "setLevel", setLevel)

    maps = {
        0: (logging.WARNING, logging.WARNING),
        1: (logging.INFO, logging.INFO),
        2: (logging.DEBUG, logging.INFO),
        3: (logging.DEBUG, logging.DEBUG),
        42: (logging.DEBUG, logging.DEBUG),
    }

    for verb, (level, glevel) in maps.items():
        coloredlogs.reset_mock()
        logger.reset_mock()
        setLevel.reset_mock()

        set_verbosity(verb)
        coloredlogs.install.assert_called_once_with(fmt=ANY, level=level)
        logger.setLevel.assert_called_once_with(level)
        setLevel.assert_called_once_with(glevel)

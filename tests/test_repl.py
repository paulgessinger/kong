import os

import pytest
from unittest import mock
from unittest.mock import Mock
import peewee as pw

from kong.model import Folder
from kong.repl import Repl
import kong.setup
import kong

import logging

kong.logger.logger.setLevel(logging.DEBUG)


@pytest.fixture
def repl(state):
    return Repl(state)


def test_ls(tree, state, repl, capsys):
    repl.do_ls(".")
    out, err = capsys.readouterr()
    assert out == "f1\nf2\nf3\n"

    repl.do_ls("/nope")
    out, err = capsys.readouterr()
    assert "not exist" in out

    state.cwd = Folder.find_by_path(state.cwd, "/f2")
    repl.do_ls(".")
    out, err = capsys.readouterr()
    assert out == "alpha\nbeta\ngamma\n"


def test_complete_funcs(state, tree, repl, monkeypatch):
    cmpl = Mock()
    monkeypatch.setattr("kong.repl.Repl.complete_path", cmpl)

    for c in ["ls", "mkdir", "cd", "rm"]:
        func = getattr(repl, f"complete_{c}")

        func("", "ls hurz", 0, 0)
        cmpl.assert_called_once_with("hurz")
        cmpl.reset_mock()

        func("", "ls hurz/schmurz", 0, 0)
        cmpl.assert_called_once_with("hurz/schmurz")
        cmpl.reset_mock()


def test_complete_path(state, tree, repl):
    alts = repl.complete_path("f")
    assert alts == ["f1/", "f2/", "f3/"]

    alts = repl.complete_path("f1")
    assert alts == ["f1/"]

    alts = repl.complete_path("f2/")
    assert alts == ["alpha/", "beta/", "gamma/"]

    state.cwd = Folder.find_by_path(state.cwd, "/f2")

    alts = repl.complete_path("a")
    assert alts == ["alpha/"]


def test_mkdir(state, repl, db, capsys):
    root = Folder.get_root()
    sub = root.add_folder("sub")

    for cwd in [root, sub]:
        state.cwd = cwd

        assert cwd.subfolder("alpha") is None
        repl.do_mkdir("alpha")
        out, err = capsys.readouterr()
        alpha = cwd.subfolder("alpha")
        assert alpha is not None

        # one down
        assert alpha.subfolder("beta") is None
        repl.do_mkdir("alpha/beta")
        out, err = capsys.readouterr()
        beta = alpha.subfolder("beta")
        assert beta is not None

        # cannot create outside of root
        repl.do_mkdir("../nope")
        if cwd == root:
            out, err = capsys.readouterr()
            assert "annot create" in out and "../nope" in out
        else:
            assert root.subfolder("nope") is not None

        # cannot create again
        repl.do_mkdir("alpha")
        out, err = capsys.readouterr()
        assert "alpha" in out and "already exists" in out

        # cannot create in nonexistant
        repl.do_mkdir("omega/game")
        out, err = capsys.readouterr()
        assert "omega/game" in out and "annot create" in out

        state.cwd = beta
        assert cwd.subfolder("gamma") is None
        repl.do_mkdir("../../gamma")
        out, err = capsys.readouterr()
        gamma = cwd.subfolder("gamma")
        assert gamma is not None


def test_cd(state, repl, db, capsys):
    root = Folder.get_root()
    assert state.cwd == root

    repl.do_cd("nope")
    out, err = capsys.readouterr()
    assert "not exist" in out, "nope" in out
    assert state.cwd == root

    nope = root.add_folder("nope")
    repl.do_cd("nope")
    out, err = capsys.readouterr()
    assert state.cwd == nope

    repl.do_cd("")
    out, err = capsys.readouterr()
    assert state.cwd == root

    repl.do_cd("..")
    out, err = capsys.readouterr()
    assert "not exist" in out
    assert state.cwd == root

    repl.do_cd("../nope")
    out, err = capsys.readouterr()
    assert "not exist" in out
    assert state.cwd == root

    more = root.add_folder("more")
    another = nope.add_folder("another")

    repl.do_cd("/nope")
    out, err = capsys.readouterr()
    assert state.cwd == nope

    repl.do_cd("/nope/another")
    out, err = capsys.readouterr()
    assert state.cwd == another

    repl.do_cd("/../")
    out, err = capsys.readouterr()
    assert state.cwd == another

    repl.do_cd("..")
    out, err = capsys.readouterr()
    assert state.cwd == nope

    repl.do_cd("/more")
    out, err = capsys.readouterr()
    assert state.cwd == more


def test_rm(state, repl, db, capsys, monkeypatch):
    root = Folder.get_root()

    repl.do_rm("../nope")
    out, err = capsys.readouterr()
    assert "not exist" in out

    repl.do_rm("/")
    out, err = capsys.readouterr()
    assert "annot delete" in out, "root" in out

    root.add_folder("alpha")
    with monkeypatch.context() as m:
        confirm = Mock(return_value=False)
        m.setattr("click.confirm", confirm)
        repl.do_rm("alpha")
        confirm.assert_called_once()

    assert root.subfolder("alpha") is not None
    with monkeypatch.context() as m:
        confirm = Mock(return_value=True)
        m.setattr("click.confirm", confirm)
        repl.do_rm("alpha")
        confirm.assert_called_once()
    assert root.subfolder("alpha") is None
    out, err = capsys.readouterr()
    assert len(out) > 0


def test_cwd(state, repl, tree, capsys):
    root = tree
    repl.do_cwd()
    out, err = capsys.readouterr()
    assert out.strip() == "/"

    state.cwd = root / "f1"
    repl.do_cwd()
    out, err = capsys.readouterr()
    assert out.strip() == "/f1"

    state.cwd = root / "f2" / "gamma"
    repl.do_cwd()
    out, err = capsys.readouterr()
    assert out.strip() == "/f2/gamma"


def test_exit(repl):
    assert repl.do_exit("") == True
    assert repl.do_EOF("") == True


def test_preloop(repl, monkeypatch):
    m = Mock()
    monkeypatch.setattr("readline.read_history_file", m)
    monkeypatch.setattr("os.path.exists", Mock(return_value=True))
    repl.preloop()
    m.assert_called_once()
    monkeypatch.setattr("os.path.exists", Mock(return_value=False))
    repl.preloop()


def test_postloop(state, repl, monkeypatch):
    set_length = Mock()
    write = Mock()
    monkeypatch.setattr("readline.set_history_length", set_length)
    monkeypatch.setattr("readline.write_history_file", write)

    repl.postloop()

    set_length.assert_called_once_with(state.config.history_length)
    write.assert_called_once()


def test_precmd(repl):
    assert repl.precmd("whatever") == "whatever"


def test_onecmd(repl, monkeypatch, capsys):
    m = Mock(return_value="ok")
    monkeypatch.setattr("cmd.Cmd.onecmd", m)
    assert repl.onecmd("whatever") == "ok"
    m.assert_called_once()
    m = Mock(side_effect=TypeError("MESSAGE"))
    monkeypatch.setattr("cmd.Cmd.onecmd", m)
    repl.onecmd("whatever")
    out, err = capsys.readouterr()
    assert "MESSAGE" in out
    m = Mock(side_effect=RuntimeError())
    monkeypatch.setattr("cmd.Cmd.onecmd", m)
    repl.onecmd("whatever")  # swallows other exceptions


def test_cmdloop(repl, monkeypatch, capsys):
    m = Mock(return_value="ok")
    monkeypatch.setattr("cmd.Cmd.cmdloop", m)
    repl.cmdloop()
    m.assert_called_once()

    m = Mock(side_effect=[KeyboardInterrupt(), "ok"])
    monkeypatch.setattr("cmd.Cmd.cmdloop", m)
    repl.cmdloop()
    out, err = capsys.readouterr()
    m.assert_called()
    assert m.call_count == 2
    assert "^C" in out


def test_emptyline(repl):
    repl.emptyline()

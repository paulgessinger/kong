import os

import pytest
from unittest import mock
from unittest.mock import Mock
import peewee as pw

from kong.model import Folder
from kong.repl import Repl
import kong.setup
import kong


@pytest.fixture
def cfg(app_env, db, monkeypatch):
    app_dir, config_path, tmp_path = app_env
    with monkeypatch.context() as m:
        m.setattr(
            "click.prompt",
            Mock(side_effect=["LocalDriver", os.path.join(app_dir, "joblog")]),
        )
        kong.setup.setup(None)
    _cfg = kong.config.Config()
    _cfg.cwd = Folder.get_root()
    return _cfg


@pytest.fixture
def repl(cfg):
    return Repl(cfg)


@pytest.fixture
def tree(db):
    root = Folder.get_root()
    f1 = root.add_folder("f1")
    f2 = root.add_folder("f2")
    alpha = f2.add_folder("alpha")
    beta = f2.add_folder("beta")
    gamma = f2.add_folder("gamma")
    delta = gamma.add_folder("delta")
    f3 = root.add_folder("f3")
    omega = f3.add_folder("omega")
    return root


def test_ls(tree, cfg, repl, capsys):
    repl.do_ls(".")
    out, err = capsys.readouterr()
    assert out == "f1\nf2\nf3\n"

    repl.do_ls("/nope")
    out, err = capsys.readouterr()
    assert "not exist" in out

    cfg.cwd = Folder.find_by_path(cfg.cwd, "/f2")
    repl.do_ls(".")
    out, err = capsys.readouterr()
    assert out == "alpha\nbeta\ngamma\n"


def test_complete_funcs(cfg, tree, repl, monkeypatch):
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


def test_complete_path(cfg, tree, repl):
    alts = repl.complete_path("f")
    assert alts == ["f1/", "f2/", "f3/"]

    alts = repl.complete_path("f1")
    assert alts == ["f1/"]

    alts = repl.complete_path("f2/")
    assert alts == ["alpha/", "beta/", "gamma/"]

    cfg.cwd = Folder.find_by_path(cfg.cwd, "/f2")

    alts = repl.complete_path("a")
    assert alts == ["alpha/"]


def test_mkdir(cfg, repl, db, capsys):
    root = Folder.get_root()
    sub = root.add_folder("sub")

    for cwd in [root, sub]:
        cfg.cwd = cwd

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

        cfg.cwd = beta
        assert cwd.subfolder("gamma") is None
        repl.do_mkdir("../../gamma")
        out, err = capsys.readouterr()
        gamma = cwd.subfolder("gamma")
        assert gamma is not None


def test_cd(cfg, repl, db, capsys):
    root = Folder.get_root()
    assert cfg.cwd == root

    repl.do_cd("nope")
    out, err = capsys.readouterr()
    assert "not exist" in out, "nope" in out
    assert cfg.cwd == root

    nope = root.add_folder("nope")
    repl.do_cd("nope")
    out, err = capsys.readouterr()
    assert cfg.cwd == nope

    repl.do_cd("")
    out, err = capsys.readouterr()
    assert cfg.cwd == root

    repl.do_cd("..")
    out, err = capsys.readouterr()
    assert "not exist" in out
    assert cfg.cwd == root

    repl.do_cd("../nope")
    out, err = capsys.readouterr()
    assert "not exist" in out
    assert cfg.cwd == root

    more = root.add_folder("more")
    another = nope.add_folder("another")

    repl.do_cd("/nope")
    out, err = capsys.readouterr()
    assert cfg.cwd == nope

    repl.do_cd("/nope/another")
    out, err = capsys.readouterr()
    assert cfg.cwd == another

    repl.do_cd("/../")
    out, err = capsys.readouterr()
    assert cfg.cwd == another

    repl.do_cd("..")
    out, err = capsys.readouterr()
    assert cfg.cwd == nope

    repl.do_cd("/more")
    out, err = capsys.readouterr()
    assert cfg.cwd == more


def test_rm(cfg, repl, db, capsys, monkeypatch):
    root = Folder.get_root()

    repl.do_rm("../nope")
    out, err = capsys.readouterr()
    assert "not exist" in out

    repl.do_rm("/")
    out, err = capsys.readouterr()
    assert "annot delete" in out, "root" in out

    root.add_folder("alpha")
    with monkeypatch.context() as m:
        m.setattr("click.confirm", Mock(return_value=False))
        repl.do_rm("alpha")
    assert root.subfolder("alpha") is not None
    with monkeypatch.context() as m:
        m.setattr("click.confirm", Mock(return_value=True))
        repl.do_rm("alpha")
    assert root.subfolder("alpha") is None
    out, err = capsys.readouterr()


def test_cwd(cfg, repl, tree, capsys):
    root = tree
    repl.do_cwd()
    out, err = capsys.readouterr()
    assert out.strip() == "/"

    cfg.cwd = root / "f1"
    repl.do_cwd()
    out, err = capsys.readouterr()
    assert out.strip() == "/f1"

    cfg.cwd = root / "f2" / "gamma"
    repl.do_cwd()
    out, err = capsys.readouterr()
    assert out.strip() == "/f2/gamma"


def test_exit(repl):
    assert repl.do_exit("") == True
    assert repl.do_EOF("") == True


def test_preloop(repl, monkeypatch):
    m = Mock()
    monkeypatch.setattr("readline.read_history_file", m)
    repl.preloop()
    m.assert_called_once()
    monkeypatch.setattr("os.path.exists", Mock(return_value=False))
    repl.preloop()


def test_postloop(cfg, repl, monkeypatch):
    set_length = Mock()
    write = Mock()
    monkeypatch.setattr("readline.set_history_length", set_length)
    monkeypatch.setattr("readline.write_history_file", write)

    repl.postloop()

    set_length.assert_called_once_with(cfg.history_length)
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

import os
import time

import pytest
from unittest import mock
from unittest.mock import Mock
import peewee as pw

from kong.model import Folder, Job
from kong.repl import Repl
import kong.setup
import kong

import logging

kong.logger.logger.setLevel(logging.DEBUG)


@pytest.fixture
def repl(state):
    return Repl(state)


def test_ls(tree, state, repl, capsys, sample_jobs):
    repl.do_ls(".")
    out, err = capsys.readouterr()
    assert out == "f1\nf2\nf3\n" + "\n".join([str(j) for j in state.cwd.jobs]) + "\n"

    repl.do_ls("")
    out, err = capsys.readouterr()
    assert out == "f1\nf2\nf3\n" + "\n".join([str(j) for j in state.cwd.jobs]) + "\n"

    repl.do_ls("/nope")
    out, err = capsys.readouterr()
    assert "not exist" in out

    state.cwd = Folder.find_by_path(state.cwd, "/f2")
    repl.do_ls(".")
    out, err = capsys.readouterr()
    assert (
        out
        == "alpha\nbeta\ngamma\n" + "\n".join([str(j) for j in state.cwd.jobs]) + "\n"
    )


def test_ls_refresh(repl, state, capsys, sample_jobs):
    repl.do_ls(".")
    out, err = capsys.readouterr()
    assert "CREATED" in out

    for job in sample_jobs:
        job.submit()

    time.sleep(0.2)

    # without refresh
    repl.do_ls(".")
    out, err = capsys.readouterr()
    assert "CREATED" not in out
    assert "COMPLETED" not in out
    assert "SUBMITTED" in out

    # with refresh
    repl.do_ls(". --refresh")
    out, err = capsys.readouterr()
    assert "SUBMITTED" not in out
    assert "COMPLETED" in out


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


def test_rm_job(state, repl, db, capsys, monkeypatch):
    root = Folder.get_root()
    j1 = state.create_job(command="sleep 1")
    assert len(root.jobs) == 1 and root.jobs[0] == j1
    assert Job.get_or_none(job_id=j1.job_id) is not None

    with monkeypatch.context() as m:
        confirm = Mock(return_value=True)
        m.setattr("click.confirm", confirm)
        repl.do_rm(str(j1.job_id))
        confirm.assert_called_once()

    out, err = capsys.readouterr()
    assert len(root.jobs) == 0
    assert Job.get_or_none(job_id=j1.job_id) is None

    # works in other cwd too
    alpha = root.add_folder("alpha")
    j2 = state.default_driver.create_job(command="sleep 1", folder=alpha)
    assert Job.get_or_none(job_id=j2.job_id) is not None
    assert len(alpha.jobs) == 1 and alpha.jobs[0] == j2
    assert state.cwd == root
    with monkeypatch.context() as m:
        confirm = Mock(return_value=True)
        m.setattr("click.confirm", confirm)
        repl.do_rm(str(j2.job_id))
        confirm.assert_called_once()
    out, err = capsys.readouterr()
    assert Job.get_or_none(job_id=j2.job_id) is None
    assert len(alpha.jobs) == 0


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


def test_create_job(repl, state, tree, capsys):
    root = tree

    repl.do_create_job("")
    out, err = capsys.readouterr()
    assert "usage" in out

    cmd = "sleep 1"
    repl.do_create_job(f"{cmd}")

    assert len(root.jobs) == 1
    j1 = root.jobs[-1]
    assert j1.command == cmd
    assert j1.status == Job.Status.CREATED
    out, err = capsys.readouterr()
    assert "reated" in out
    assert str(j1.job_id) in out
    assert j1.batch_job_id in out

    cores = 16
    repl.do_create_job(f"-c {cores} '{cmd}'")
    assert len(root.jobs) == 2
    j2 = root.jobs[-1]
    assert j2.command == cmd
    assert j2.status == Job.Status.CREATED
    assert j2.cores == cores
    out, err = capsys.readouterr()
    assert str(j2.job_id) in out
    assert j2.batch_job_id in out

    repl.do_create_job(f"--cores {cores} -- {cmd}")
    assert len(root.jobs) == 3
    j3 = root.jobs[-1]
    assert j3.command == cmd
    assert j3.status == Job.Status.CREATED
    assert j3.cores == cores
    out, err = capsys.readouterr()
    assert str(j3.job_id) in out
    assert j3.batch_job_id in out

    repl.do_create_job("--help")
    out, err = capsys.readouterr()
    assert "usage" in out

    repl.do_create_job("--nope 5 sleep 2")  # wrong option --core
    out, err = capsys.readouterr()
    assert "usage" in out


def test_submit_job(repl, state, capsys):
    root = Folder.get_root()
    value = "VALUE VALUE VALUE"
    cmd = f"sleep 0.3; echo '{value}'"

    repl.do_create_job(cmd)
    j1 = root.jobs[-1]
    j1.ensure_driver_instance(state.config)
    assert j1.status == Job.Status.CREATED

    repl.do_submit_job(f"{j1.job_id}")
    j1.reload()
    assert j1.status == Job.Status.SUBMITTED
    time.sleep(0.1)
    assert j1.get_status() == Job.Status.RUNNING
    time.sleep(0.3)
    assert j1.get_status() == Job.Status.COMPLETED


def test_kill_job(repl, state, capsys):
    root = Folder.get_root()
    repl.do_create_job("sleep 1")
    j1 = root.jobs[-1]
    j1.ensure_driver_instance(state.config)

    assert j1.status == Job.Status.CREATED
    j1.submit()
    assert j1.status == Job.Status.SUBMITTED
    time.sleep(0.1)
    assert j1.get_status() == Job.Status.RUNNING

    repl.do_kill_job(str(j1.job_id))

    assert j1.get_status() == Job.Status.FAILED


def test_resubmit_job(repl, state, capsys):
    root = Folder.get_root()
    repl.do_create_job("sleep 1")
    j1 = root.jobs[-1]
    j1.ensure_driver_instance(state.config)

    assert j1.status == Job.Status.CREATED
    j1.submit()
    assert j1.status == Job.Status.SUBMITTED
    time.sleep(0.1)
    assert j1.get_status() == Job.Status.RUNNING

    repl.do_kill_job(str(j1.job_id))

    assert j1.get_status() == Job.Status.FAILED

    repl.do_resubmit_job(str(j1.job_id))
    j1.reload()
    assert j1.status == Job.Status.SUBMITTED


def test_status(repl, state, capsys):
    root = Folder.get_root()

    repl.onecmd("status")
    out, err = capsys.readouterr()
    assert "usage" in out

    repl.onecmd("status 42")
    out, err = capsys.readouterr()
    assert "not find" in out

    j1 = state.create_job(command="sleep 0.2")

    repl.onecmd(f"status {j1.job_id}")
    out, err = capsys.readouterr()
    assert "CREATED" in out

    j1.submit()

    time.sleep(0.1)

    repl.onecmd(f"status {j1.job_id}")
    out, err = capsys.readouterr()
    assert "RUNNING" in out

    time.sleep(0.2)

    repl.onecmd(f"status {j1.job_id}")
    out, err = capsys.readouterr()
    assert "COMPLETED" in out

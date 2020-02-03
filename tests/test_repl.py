import os
import re
import tempfile
import time
from concurrent.futures import Executor, Future
from datetime import timedelta

import pytest
from unittest import mock
from unittest.mock import Mock, ANY, MagicMock
import peewee as pw
from click import UsageError
from conftest import skip_lxplus

from kong.model.folder import Folder
from kong.model.job import Job

from kong.repl import Repl, complete_path
import kong

import logging

from kong.state import DoesNotExist

kong.logger.logger.setLevel(logging.DEBUG)


@pytest.fixture
def repl(state):
    r = Repl(state)
    r._raise = True
    return r


def test_ls(tree, state, repl, capsys, sample_jobs, monkeypatch):
    repl.do_ls(".")
    out, err = capsys.readouterr()
    assert all(f.name in out for f in state.cwd.children)
    assert all(f"{j.job_id}" in out for j in state.cwd.jobs)

    state.mkdir("f4")
    repl.onecmd("ls f4")

    j1 = Job.get(1)
    j1.batch_job_id = None
    j1.save()
    state.mv(j1, "f1")

    repl.onecmd("ls")
    out, err = capsys.readouterr()
    assert all(f.name in out for f in state.cwd.children)
    assert all(f"{j}" in out for j in [2, 3])

    repl.onecmd("ls --recursive")
    out, err = capsys.readouterr()
    # assert all(f.name in out for f in state.cwd.children)
    all_jobs = [j.job_id for j in Job.select()]
    assert all(f"{j}" in out for j in all_jobs)

    repl.do_ls("/nope")
    out, err = capsys.readouterr()
    assert "not exist" in out

    state.cwd = Folder.find_by_path("/f2", state.cwd)
    repl.do_ls(".")
    out, err = capsys.readouterr()
    assert all(f.name in out for f in state.cwd.children)
    assert all(f"{j.job_id}" in out for j in state.cwd.jobs)

    with pytest.raises(UsageError):
        repl.onecmd("ls --nope")
    out, err = capsys.readouterr()
    assert "no such option" in out

    with monkeypatch.context() as m:

        job = state.create_job(command="sleep 1")
        job.batch_job_id = None

        m.setattr(state, "refresh_jobs", Mock(return_value=[]))
        m.setattr(state, "ls", Mock(return_value=([], [job])))

        repl.onecmd("ls -R /")

        state.ls.assert_called_once()
        assert state.refresh_jobs.call_count == 1

def test_ls_status(state, repl, capsys):
    j1 = state.create_job(command="sleep 1")
    j2 = state.create_job(command="sleep 1")
    j3 = state.create_job(command="sleep 1")

    j2.status = Job.Status.FAILED
    j2.save()

    repl.onecmd("ls")
    out, err = capsys.readouterr()
    assert len(out.strip().split("\n")) == 6

    repl.onecmd("ls -S CREATED")
    out, err = capsys.readouterr()
    assert len(out.strip().split("\n")) == 5

    repl.onecmd("ls -S FAILED")
    out, err = capsys.readouterr()
    assert len(out.strip().split("\n")) == 4



def test_ls_sizes(db, tree, state, repl, capsys, sample_jobs, monkeypatch):
    class DirectExecutor(Executor):
        def __init__(self, *args, **kwargs):
            pass

        def submit(self, fn, *args, **kwargs):
            future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as e:
                future.set_exception(e)
            return future

        def __enter__(self):
            return self

    monkeypatch.setattr(
        "kong.repl.ThreadPoolExecutor", DirectExecutor
    )  # disable threads
    monkeypatch.setattr("kong.repl.Job.size", Mock(return_value=42))
    repl.onecmd("ls -s .")
    out, err = capsys.readouterr()
    lines = out.strip().split("\n")
    exp = """
name output size              UNKNOWN CREATED SUBMITTED RUNNING FAILED COMPLETED
---- ------------------------ ------- ------- --------- ------- ------ ---------
f1   168 bytes                      0       4         0       0      0         0
f2   252 bytes                      0       6         0       0      0         0
f3   0 bytes                        0       0         0       0      0         0
"""[
        1:-1
    ]
    assert "\n".join(lines[2:7]) == exp


@skip_lxplus
def test_ls_refresh(repl, state, capsys, sample_jobs, monkeypatch):
    repl.do_ls(".")
    out, err = capsys.readouterr()
    assert "CREATED" in out

    for job in sample_jobs:
        job.submit()

    time.sleep(0.2)

    # without refresh
    repl.do_ls(".")
    out, err = capsys.readouterr()
    lines = out.split("\n")[:-1]
    assert all("SUBMITTED" in l for l in lines[-3:])
    assert all("COMPLETED" not in l for l in lines[-3:])

    # with refresh
    repl.do_ls(". --refresh")
    out, err = capsys.readouterr()
    lines = out.split("\n")[:-1]
    assert all("SUBMITTED" not in l for l in lines[-3:])
    assert all("COMPLETED" in l for l in lines[-3:])

    with monkeypatch.context() as m:
        mock = Mock(return_value=[])
        m.setattr(state, "refresh_jobs", mock)
        repl.onecmd("ls -R /")
        assert mock.call_count == 2 # called once for folders, once for jobs


def test_complete_path(state, tree, repl):
    root = Folder.get_root()
    alts = complete_path(root, "f")
    assert alts == ["f1/", "f2/", "f3/"]

    alts = complete_path(root, "f1")
    assert alts == ["f1/"]

    alts = complete_path(root, "f2/")
    assert alts == ["alpha/", "beta/", "gamma/"]

    state.cwd = Folder.find_by_path("/f2", state.cwd)

    alts = complete_path(root.subfolder("f2"), "a")
    assert alts == ["alpha/"]

def test_completed_default(repl):
    root = Folder.get_root()
    root.add_folder("alpha")
    root.add_folder("beta_delta")
    root.add_folder("beta_gamma")

    assert repl.completedefault("al", "ls al", 3, 5) == ["alpha/"]
    assert repl.completedefault("be", "ls alpha be", 9, 11) == ["beta_delta/", "beta_gamma/"]
    assert repl.completedefault("alp", "ls alp beta_delta", 3, 6) == ["alpha/"]
    assert repl.completedefault("alp", "ls alp nope", 3, 6) == ["alpha/"]
    assert repl.completedefault("be", "ls be", 3, 5) == ["beta_delta/", "beta_gamma/"]


def test_mkdir(state, repl, db, capsys, monkeypatch):
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
        assert "alpha" in out

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

        # force integrity error
        with monkeypatch.context() as m:
            m.setattr(state, "mkdir", Mock(side_effect=pw.IntegrityError))
            repl.onecmd("mkdir hurz")
            out, err = capsys.readouterr()
            assert "already exists" in out

    assert Repl.do_mkdir.__doc__ is not None


def test_mkdir_create_parents(state, repl, capsys):
    root = Folder.get_root()

    repl.onecmd("mkdir /a1/b2/c3/d4")
    out, err = capsys.readouterr()
    assert "Cannot create folder" in out
    assert Folder.find_by_path("/a1/b2/c3/d4", state.cwd) is None

    repl.onecmd("mkdir -p /a1/b2/c3/d4")
    out, err = capsys.readouterr()
    assert Folder.find_by_path("/a1", state.cwd) is not None
    assert Folder.find_by_path("/a1/b2", state.cwd) is not None
    assert Folder.find_by_path("/a1/b2/c3", state.cwd) is not None
    assert Folder.find_by_path("/a1/b2/c3/d4", state.cwd) is not None



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


def test_mv_folder(state, repl, capsys):
    root = Folder.get_root()

    repl.onecmd("mv --help")
    out, err = capsys.readouterr()

    f1, f2, f3, f4, f5 = [root.add_folder(n) for n in ("f1", "f2", "f3", "f4", "f5")]

    assert len(root.children) == 5
    assert len(f2.children) == 0

    # actual move
    repl.onecmd("mv f1 f2")
    assert len(root.children) == 4
    f2.reload()
    assert len(f2.children) == 1 and f2.children[0] == f1
    f1.reload()
    assert f1.parent == f2
    out, err = capsys.readouterr()

    # rename f3 -> f3x
    repl.onecmd("mv f3 f3x")
    out, err = capsys.readouterr()
    f3.reload()
    assert len(root.children) == 4
    assert f3.name == "f3x"

    # another move
    repl.onecmd("mv f3x f4")
    assert len(f4.children) == 1 and f4.children[0] == f3
    f3.reload()
    assert f3.parent == f4
    assert f3.name == "f3x"
    out, err = capsys.readouterr()

    # move rename at the same time
    repl.onecmd("cd f2")
    repl.onecmd("mv ../f5 ../f4/f5x")
    out, err = capsys.readouterr()
    f5.reload()
    assert len(f4.children) == 2
    assert f5.name == "f5x"
    assert f5.parent == f4

    # try move to nonexistant
    repl.onecmd("cd /")
    with pytest.raises(ValueError):
        repl.onecmd("mv f2/f1 /nope/blub")
    out, err = capsys.readouterr()
    assert "/nope" in out and "not exist" in out

    # try to move nonexistant
    with pytest.raises(DoesNotExist):
        repl.onecmd("mv ../nope f1")
    out, err = capsys.readouterr()
    assert "../nope" in out and "No such" in out


def test_mv_job(state, repl, capsys):
    root = Folder.get_root()

    f1, f2 = [root.add_folder(n) for n in ("f1", "f2")]

    assert len(root.children) == 2

    j1, j2, j3, j4, j5 = [state.create_job(command="sleep 1") for _ in range(5)]
    assert len(root.jobs) == 5

    repl.onecmd(f"mv {j1.job_id} f1")
    j1.reload()
    assert j1.folder == f1
    assert len(f1.jobs) == 1
    assert len(root.jobs) == 4
    out, err = capsys.readouterr()

    repl.onecmd(f"mv {j2.job_id} f2")
    j2.reload()
    assert j2.folder == f2
    assert len(f2.jobs) == 1
    assert len(root.jobs) == 3
    out, err = capsys.readouterr()

    state.cd(f2)
    repl.onecmd(f"mv {j3.job_id} .")
    j3.reload()
    assert j3.folder == f2
    out, err = capsys.readouterr()

    repl.onecmd(f"mv {j2.job_id} ..")
    j2.reload()
    assert j2.folder == root
    out, err = capsys.readouterr()

    repl.onecmd(f"mv ../{j4.job_id} ../f1")
    j4.reload()
    assert j4.folder == f1
    out, err = capsys.readouterr()

    state.cd(root)

    # renaming does not work
    with pytest.raises(ValueError):
        repl.onecmd(f"mv {j5.job_id} 42")
    out, err = capsys.readouterr()
    assert "42" in out and "not exist" in out


def test_mv_bulk_job(state, repl):
    root = Folder.get_root()

    f1, f2 = [root.add_folder(n) for n in ("f1", "f2")]
    assert len(root.children) == 2

    j1, j2, j3, j4, j5 = [state.create_job(command="sleep 1") for _ in range(5)]
    assert len(root.jobs) == 5

    repl.onecmd("mv * f1")
    assert len(root.jobs) == 0 and len(f1.jobs) == 5
    assert len(root.children) == 1  # Didn't move the folders
    for j in (j1, j2, j3, j4, j5):
        j.reload()
        assert j.folder == f1

    state.cwd = f2
    repl.onecmd("mv ../f1/* .")
    assert len(f1.jobs) == 0 and len(f2.jobs) == 5
    for j in (j1, j2, j3, j4, j5):
        j.reload()
        assert j.folder == f2


def test_mv_bulk_folder(state, repl):
    root = Folder.get_root()

    r1, r2 = [root.add_folder(n) for n in ("r1", "r2")]

    folders = [r1.add_folder(f"f{n}") for n in range(5)]
    assert len(r1.children) == len(folders)
    assert len(r2.children) == 0

    repl.onecmd("mv r1/* r2")
    assert len(r1.children) == 0
    assert len(r2.children) == len(folders)
    for f in folders:
        f.reload()
        assert f.parent == r2

    state.cwd = r1
    repl.onecmd("mv ../r2/* .")
    assert len(r1.children) == len(folders)
    assert len(r2.children) == 0
    for f in folders:
        f.reload()
        assert f.parent == r1


def test_mv_error(state, repl, capsys, monkeypatch):
    with pytest.raises(UsageError):
        repl.onecmd("mv --nope")
    out, err = capsys.readouterr()
    assert "no such option" in out


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
        repl.onecmd("rm -r alpha")
        confirm.assert_called_once()

    assert root.subfolder("alpha") is not None
    with monkeypatch.context() as m:
        confirm = Mock(return_value=True)
        m.setattr("click.confirm", confirm)
        repl.onecmd("rm -r alpha")
        confirm.assert_called_once()
    assert root.subfolder("alpha") is None
    out, err = capsys.readouterr()
    assert len(out) > 0


def test_rm_job(state, repl, db, capsys, monkeypatch):
    root = Folder.get_root()
    j1 = state.default_driver.create_job(command="sleep 1", folder=root)
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
    assert j1.job_id != j2.job_id
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
    for cmd in ("cwd", "pwd"):
        state.cwd = root
        repl.onecmd(cmd)
        out, err = capsys.readouterr()
        assert out.strip() == "/"

        state.cwd = root / "f1"
        repl.onecmd(cmd)
        out, err = capsys.readouterr()
        assert out.strip() == "/f1"

        state.cwd = root / "f2" / "gamma"
        repl.onecmd(cmd)
        out, err = capsys.readouterr()
        assert out.strip() == "/f2/gamma"


def test_wait(repl, state, monkeypatch):
    wait = Mock(return_value=iter([]))
    monkeypatch.setattr(state, "wait", wait)

    repl.onecmd("wait * --no-notify --recursive --poll-interval 50")
    wait.assert_called_once_with(
        "*", notify=False, recursive=True, poll_interval=50, update_interval=None
    )

    wait.reset_mock()

    repl.onecmd("wait * --notify --recursive --poll-interval 50 --notify-interval 30m")
    wait.assert_called_once_with(
        "*",
        notify=True,
        recursive=True,
        poll_interval=50,
        update_interval=timedelta(minutes=30),
    )



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
    repl.postloop()


def test_precmd(repl):
    assert repl.precmd("whatever") == "whatever"
    assert repl.precmd("") == ""


def test_onecmd(repl, monkeypatch, capsys):
    set_length = Mock()
    write = Mock()
    monkeypatch.setattr("readline.set_history_length", set_length)
    monkeypatch.setattr("readline.write_history_file", write)
    m = Mock(return_value="ok")
    monkeypatch.setattr("cmd.Cmd.onecmd", m)
    assert repl.onecmd("whatever") == "ok"
    set_length.assert_called_once_with(repl.state.config.history_length)
    write.assert_called_once()
    m.assert_called_once()

    m = Mock(side_effect=TypeError("MESSAGE"))
    monkeypatch.setattr("cmd.Cmd.onecmd", m)
    with pytest.raises(TypeError):
        repl.onecmd("whatever")
    out, err = capsys.readouterr()
    assert "MESSAGE" in out
    m = Mock(side_effect=RuntimeError())
    monkeypatch.setattr("cmd.Cmd.onecmd", m)
    with monkeypatch.context() as m:
        m.setattr(repl, "_raise", False) # disable debug mode for this check
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
    assert "provide a command" in out

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

    repl.do_create_job("--help")
    out, err = capsys.readouterr()
    assert "Usage" in out

    with pytest.raises(UsageError):
        repl.onecmd("create_job --nope 5 sleep 2")  # wrong option --core
    out, err = capsys.readouterr()
    assert "no such" in out

    repl.onecmd("create_job -- exe --and --some arguments --and options")
    out, err = capsys.readouterr()

    j4 = root.jobs[-1]
    assert j4.command == "exe --and --some arguments --and options"


def test_create_job_extra_arguments(repl, state, tree, monkeypatch):
    root = tree

    cmd = "sleep 1"
    cores = 16

    monkeypatch.setattr(state, "create_job", Mock())
    repl.onecmd(f"create_job -a cores={cores} '{cmd}'")
    assert state.create_job.call_count == 1
    kwargs = state.create_job.call_args[1]
    assert kwargs["cores"] == cores

    state.create_job.reset_mock()
    repl.onecmd(f"create_job -a ARGA=hurz --argument ARGB=blurz -a ARGC=42 '{cmd}'")
    assert state.create_job.call_count == 1
    kwargs = state.create_job.call_args[1]
    print(kwargs)
    assert kwargs["ARGA"] == "hurz"
    assert kwargs["ARGB"] == "blurz"
    assert kwargs["ARGC"] == 42


@skip_lxplus
def test_submit_job(repl, state, capsys, monkeypatch):
    root = Folder.get_root()
    value = "VALUE VALUE VALUE"
    cmd = f"sleep 0.3; echo '{value}'"

    monkeypatch.setattr("click.confirm", Mock(return_value=True))

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

    out, err = capsys.readouterr()

    with pytest.raises(UsageError):
        repl.onecmd("submit_job --nope")
    out, err = capsys.readouterr()
    assert "no such option" in out


def test_kill_job(repl, state, capsys, monkeypatch):
    root = Folder.get_root()
    repl.do_create_job("sleep 1")
    j1 = root.jobs[-1]
    j1.ensure_driver_instance(state.config)

    assert j1.status == Job.Status.CREATED
    j1.submit()
    assert j1.status == Job.Status.SUBMITTED
    time.sleep(0.1)
    assert j1.get_status() == Job.Status.RUNNING

    monkeypatch.setattr("click.confirm", Mock(return_value=True))
    repl.do_kill_job(str(j1.job_id))
    out, err = capsys.readouterr()
    assert j1.get_status() == Job.Status.FAILED

    with pytest.raises(UsageError):
        repl.onecmd("kill_job --nope")
    out, err = capsys.readouterr()
    assert "no such option" in out


def test_resubmit_job(repl, state, capsys, monkeypatch):
    root = Folder.get_root()
    repl.do_create_job("sleep 1")
    j1 = root.jobs[-1]
    j1.ensure_driver_instance(state.config)

    assert j1.status == Job.Status.CREATED
    j1.submit()
    assert j1.status == Job.Status.SUBMITTED
    time.sleep(0.1)
    assert j1.get_status() == Job.Status.RUNNING

    monkeypatch.setattr("click.confirm", Mock(return_value=True))
    repl.do_kill_job(str(j1.job_id))

    assert j1.get_status() == Job.Status.FAILED

    repl.do_resubmit_job(str(j1.job_id))
    out, err = capsys.readouterr()
    j1.reload()
    assert j1.status == Job.Status.SUBMITTED

    with pytest.raises(UsageError):
        repl.onecmd("resubmit_job --nope")
    out, err = capsys.readouterr()
    assert "no such option" in out


@skip_lxplus
def test_update(repl, state, capsys, monkeypatch):
    root = Folder.get_root()

    repl.onecmd("update")
    out, err = capsys.readouterr()

    repl.onecmd("update --help")
    out, err = capsys.readouterr()
    assert "Usage" in out

    with pytest.raises(DoesNotExist):
        repl.onecmd("update 42")
    out, err = capsys.readouterr()
    assert "not find" in out

    j1 = state.create_job(command="sleep 0.2")

    def update():
        repl.onecmd(f"update {j1.job_id}")
        out, err = capsys.readouterr()

    update()

    j1.reload()
    assert j1.status == Job.Status.CREATED

    j1.submit()
    assert j1.status == Job.Status.SUBMITTED

    time.sleep(0.1)

    update()
    j1.reload()
    assert j1.status == Job.Status.RUNNING

    time.sleep(0.2)

    update()
    j1.reload()
    assert j1.status == Job.Status.COMPLETED

    with pytest.raises(UsageError):
        repl.onecmd("update --nope")
    out, err = capsys.readouterr()
    assert "no such option" in out

    j2 = state.create_job(command="sleep 0.2")

    repl.onecmd("update -r .")
    out, err = capsys.readouterr()
    lines = out.strip("").split("\n")
    # assert lines[2] == "bla"
    assert re.match(r"\s*0U\s*1C\s*0S\s*0R\s*0F\s*1C", lines[2]) is not None


def test_info(state, repl, capsys, monkeypatch):
    repl.onecmd("info") # missing arg
    out, err = capsys.readouterr()
    assert "usage" in out


    job = state.create_job(command="sleep 1")

    repl.onecmd("info 1")
    out, err = capsys.readouterr()
    assert str(job.job_id) in out
    assert job.batch_job_id in out
    for k, v in job.data.items():
        assert k in out
        assert v in out

    with monkeypatch.context() as m:
        refresh = Mock(return_value=[])
        m.setattr(state, "refresh_jobs", refresh)
        repl.onecmd("info 1 -R")
        out, err = capsys.readouterr()
        refresh.assert_called_once()

    command_string = "X" * 600

    long_job = state.create_job(command=command_string)

    repl.onecmd("info 2")
    out, err = capsys.readouterr()
    assert not command_string in out

    repl.onecmd("info 2 --full")
    out, err = capsys.readouterr()
    assert command_string in out


def test_tail(state, repl, capsys, monkeypatch):
    job = state.create_job(command="sleep 1")

    with monkeypatch.context() as m:
        tail = Mock()
        m.setattr("sh.tail", tail)
        m.setattr("time.sleep", Mock())
        m.setattr("os.path.exists", Mock(side_effect=[False, False, True]))
        spinner = MagicMock()
        m.setattr("kong.repl.Spinner", spinner)
        repl.onecmd(f"tail {job.job_id}")
        out, err = capsys.readouterr()
        spinner.assert_called_once()
        assert tail.call_count == 1

    with monkeypatch.context() as m:
        tail = Mock()
        m.setattr("sh.tail", tail)
        m.setattr("os.path.exists", Mock(side_effect=[True]))
        spinner = MagicMock()
        m.setattr("kong.repl.Spinner", spinner)
        repl.onecmd(f"tail {job.job_id}")
        out, err = capsys.readouterr()
        assert spinner.call_count == 0
        assert tail.call_count == 1

    with pytest.raises(UsageError):
        repl.onecmd(f"tail --nope")
    out, err = capsys.readouterr()
    assert "no such option" in out


def test_less(state, repl, capsys, monkeypatch):
    job = state.create_job(command="sleep 1")

    content = "SOMECONTENT: BLABLBALBALBALBLA\nNEWLINE"
    with tempfile.NamedTemporaryFile("wt") as f:
        f.write(content)
        f.flush()
        job.data["stdout"] = f.name
        job.save()

        lines = []

        def agg(it):
            nonlocal lines
            lines = list(it)

        with monkeypatch.context() as m:
            pager = Mock(side_effect=agg)
            m.setattr("click.echo_via_pager", pager)
            repl.onecmd(f"less {job.job_id}")
            out, err = capsys.readouterr()
            pager.assert_called_once()
            assert "".join(lines) == content

    with pytest.raises(UsageError):
        repl.onecmd(f"less --nope")
    out, err = capsys.readouterr()
    assert "no such option" in out

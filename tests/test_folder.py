import os

import pytest
import peewee as pw

from kong.db import database
from kong.model.folder import Folder

import logging

from kong.model.job import Job

logging.getLogger("kong").setLevel(logging.DEBUG)


def test_create(db):
    root = Folder.get_root()
    f1 = Folder.create(name="hallo", parent=root)

    assert f1.parent is root
    assert len(root.children) == 1

    f2 = Folder.create(name="number2", parent=f1)
    assert len(root.children) == 1
    assert f2.name == "number2"
    assert f2.parent == f1
    assert f2.parent_id == f1.folder_id

    with pytest.raises(AssertionError):
        Folder.create(name=".")
    with pytest.raises(AssertionError):
        Folder.create(name="..")
    with pytest.raises(AssertionError):
        Folder.create(name="/")

    f3 = root.add_folder("f3")
    assert f3.name == "f3"
    assert f3.parent == root
    assert len(root.children) == 2


def test_cannot_be_own_parent(db):
    root = Folder.get_root()
    f1 = root.add_folder("f1")
    assert f1.parent == root

    with pytest.raises(pw.IntegrityError):
        f1.parent = f1
        f1.save()
    f1.reload()
    assert f1.parent == root


def test_create_name_unique(db):
    root = Folder.get_root()
    f1 = root.add_folder("f1")

    with pytest.raises(pw.IntegrityError):
        Folder.create(name="f1", parent=root)
    with pytest.raises(pw.IntegrityError):
        root.add_folder("f1")

    # another one is fine
    assert root.add_folder("f2").name == "f2"
    # one further down is also fine
    assert f1.add_folder("f1").name == "f1"


def test_create_name_validate(db):
    root = Folder.get_root()
    with pytest.raises(AssertionError):
        Folder.create(name="123", parent=root)


def test_get_subfolder(db):
    root = Folder.get_root()

    f1 = root.add_folder("f1")
    f2 = root.add_folder("f2")
    f2_f1 = f2.add_folder("f1")

    assert root.subfolder("f1") == f1
    assert root.subfolder("f2") == f2
    assert f2.subfolder("f1") == f2_f1


def test_get_root_creates(db):
    assert Folder.get_or_none(name="root", parent=None) is None

    root = Folder.get_root()
    assert root.name == "root"
    assert root.parent is None

    assert Folder.get_or_none(name="root", parent=None) is not None


def test_only_one_root(db):
    root = Folder.get_root()
    # try to create another one

    with pytest.raises(AssertionError):
        Folder.create(name="something else")
    with pytest.raises(AssertionError):
        Folder.create(name="something else", parent=None)

    # make sure it wasnt created
    assert Folder.select().where(Folder.parent == None).count() == 1


def test_path(db):
    root = Folder.get_root()
    assert root.path == "/"

    f1 = Folder.create(name="f1", parent=root)
    assert f1.path == "/f1"
    f2 = Folder.create(name="f2", parent=f1)
    assert f2.path == "/f1/f2"
    f3 = Folder.create(name="f3", parent=f2)
    assert f3.path == "/f1/f2/f3"
    f4 = Folder.create(name="f4", parent=f3)
    assert f4.path == "/f1/f2/f3/f4"

    # move stuff around
    f4.parent = f2
    f4.save()
    assert f4.path == "/f1/f2/f4"


def test_find_by_path(db):
    root = Folder.get_root()

    f1 = root.add_folder("f1")
    f2 = f1.add_folder("f2")
    f3 = f2.add_folder("f3")
    f4 = f2.add_folder("f4")

    # Check it works with implicit CWD
    assert Folder.find_by_path("/") == root
    assert Folder.find_by_path("../blubb") == None
    assert Folder.find_by_path("/f1") == f1
    assert Folder.find_by_path("/f1/f2") == f2
    assert Folder.find_by_path("/f1/f2/f3") == f3
    assert Folder.find_by_path("/f1/f2/f4") == f4

    for f in [root, f1, f2, f3, f4]:
        # absolute paths work regardless of cwd
        assert Folder.find_by_path("/", f) == root
        assert Folder.find_by_path("/f1", f) == f1
        assert Folder.find_by_path("/f1/f2", f) == f2
        assert Folder.find_by_path("/f1/f2/f3", f) == f3
        assert Folder.find_by_path("/f1/f2/f4", f) == f4
        # self referential paths work everywhere
        assert Folder.find_by_path(".", f) == f
        assert Folder.find_by_path(f.path, f) == f
        assert Folder.find_by_path(f.path + "/", f) == f

        # for root, it's None
        assert Folder.find_by_path("..", f) == f.parent

        assert Folder.find_by_path("nope", f) is None
        assert Folder.find_by_path("../nope", f) is None

    assert Folder.find_by_path("f1", root) == f1
    assert Folder.find_by_path("f1") == f1
    assert Folder.find_by_path("../", f1) == root
    assert Folder.find_by_path("../f1", f1) == f1
    assert Folder.find_by_path("../f1/f2", f1) == f2
    assert Folder.find_by_path("f2", f1) == f2
    assert Folder.find_by_path("f3", f2) == f3
    assert Folder.find_by_path("f4", f2) == f4
    assert Folder.find_by_path("../f4", f3) == f4
    assert Folder.find_by_path("../f3", f4) == f3


def test_fancy_operator(db):
    root = Folder.get_root()

    f1 = root.add_folder("f1")
    f2 = f1.add_folder("f2")
    f3 = f2.add_folder("f3")
    f4 = f2.add_folder("f4")

    assert root / "f1" == f1
    assert root / "f1" / "f2" == f2
    assert root / "f1" / "f2" / "f3" == f3
    assert root / "f1" / "f2" / "f4" == f4


def test_folders_recursive(db, state, monkeypatch, sqlite_version):
    root = Folder.get_root()
    f1 = root.add_folder("f1")
    f2 = f1.add_folder("f2")
    f3 = f1.add_folder("f3")
    f4 = root.add_folder("f4")

    folders = root.folders_recursive()
    assert set(folders) == set([f1, f2, f3, f4])

    with monkeypatch.context() as m:
        m.setattr("sqlite3.sqlite_version_info", (3, 7, 17))
        folders = root.folders_recursive()
        assert set(folders) == set([f1, f2, f3, f4])


def test_jobs_recursive(db, state, monkeypatch, sqlite_version):
    root = Folder.get_root()
    f1 = root.add_folder("f1")
    f2 = f1.add_folder("f2")

    with state.pushd(f1):
        j1 = state.create_job(command="sleep 1")
    with state.pushd(f2):
        j2 = state.create_job(command="sleep 1")

    jobs = root.jobs_recursive()
    assert len(jobs) == 2
    assert all(a == b for a, b in zip(jobs, [j1, j2]))


def test_job_stats(db, state, monkeypatch):

    root = Folder.get_root()

    f1 = root.add_folder("f1")
    f2 = f1.add_folder("f2")

    state.cd(f1)
    j1 = state.create_job(command="sleep 1")
    j2 = state.create_job(command="sleep 1")
    j3 = state.create_job(command="sleep 1")

    state.cd(f2)
    j4 = state.create_job(command="sleep 1")
    j5 = state.create_job(command="sleep 1")
    j6 = state.create_job(command="sleep 1")

    j2.status = Job.Status.RUNNING
    j2.save()
    j3.status = Job.Status.RUNNING
    j3.save()
    j4.status = Job.Status.FAILED
    j4.save()
    j5.status = Job.Status.COMPLETED
    j5.save()
    j6.status = Job.Status.UNKNOWN
    j6.save()

    exp = {
        Job.Status.RUNNING: 2,
        Job.Status.SUBMITTED: 0,
        Job.Status.FAILED: 1,
        Job.Status.COMPLETED: 1,
        Job.Status.UNKNOWN: 1,
        Job.Status.CREATED: 1,
    }

    assert exp == f1.job_stats()
    assert exp == root.job_stats()

    with monkeypatch.context() as m:
        assert exp == f1.job_stats()
        assert exp == root.job_stats()

import os
import datetime

from typing import Any, cast, Optional, TYPE_CHECKING, List, Iterable, Dict, Tuple

import peewee as pw
from peewee import sqlite3

from ..logger import logger
from ..db import AutoIncrementField, database
from . import BaseModel

if TYPE_CHECKING:  # pragma: no cover
    from .job import Job


class Folder(BaseModel):
    """
    Represents a folder in the internal hierarchy for organizing jobs.

    :ivar folder_id: The ID of a folder instance
    :ivar name: Name of this folder instance
    :ivar parent: Points to the parent instance of this folder. Can be `None`
                  for the root folder
    :ivar created_at: Timestamp of creation of this folder instance
    :ivar updated_at: When the instance was last updated.


    """

    if TYPE_CHECKING:  # pragma: no cover
        folder_id: int
        parent: "Folder"
        children: List["Folder"]
        name: str
        jobs: List[Job]
    else:
        folder_id = AutoIncrementField()
        name = pw.CharField()
        parent = pw.ForeignKeyField("self", null=True, backref="children")
        created_at = pw.DateTimeField(default=datetime.datetime.now)
        updated_at = pw.DateTimeField()

    _ignore_save_assert = False

    class Meta:
        indexes = ((("parent", "name"), True),)

    def save(self, *args: Any, **kwargs: Any) -> None:
        if not self._ignore_save_assert:
            assert (
                self.name not in (".", "..", "")
                and "/" not in self.name
                and not self.name.isdigit()
            ), f"Invalid folder name '{self.name}'"
            assert self.parent is not None, "Need to specify a parent folder"
        self._ignore_save_assert = False

        # can never be its own parent
        if self.parent == self:
            raise pw.IntegrityError("Folder can not be its own parent")

        self.updated_at = datetime.datetime.now()

        return super().save(*args, **kwargs)

    @property
    def path(self) -> str:
        """
        The path to this folder

        :return: The path
        """
        if self.parent is None:
            return "/"
        # this will be slow, could optimize with CTE
        return cast(str, os.path.join(self.parent.path, self.name))

    @classmethod
    def get_root(cls) -> "Folder":
        """
        Retrieve the root folder (/). There can be only one.

        :return: Root folder instance
        """
        folder = Folder.get_or_none(Folder.parent.is_null(), name="root")
        if folder is None:
            # bypass assertions
            folder = super(BaseModel, cls).create(name="root", _ignore_save_assert=True)
        return cast(Folder, folder)

    @staticmethod
    def find_by_path(path: str, cwd: Optional["Folder"] = None) -> Optional["Folder"]:
        """
        Retrieve a folder instance by path

        :param path: Path to the folder
        :param cwd: Directory to start working from, defaults to root folder
        :return: The found folder or `None` if the path doesn't exist
        """

        assert isinstance(path, str)

        if cwd is None:
            cwd = Folder.get_root()

        assert isinstance(cwd, Folder)

        if path == "/":
            return Folder.get_root()
        if path.startswith("/"):
            return Folder.find_by_path(path[1:], Folder.get_root())
        if path.endswith("/"):
            path = path[:-1]
        if path == "..":
            return cwd.parent
        if path == "" or path == ".":
            return cwd
        if "/" not in path:
            return cwd.subfolder(path)
        head, tail = path.split(os.sep, 1)
        logger.debug("Resolve path %s in %s: %s, %s", path, cwd.path, head, tail)

        if head == "..":
            if cwd.parent is None:
                return None
            return Folder.find_by_path(tail, cwd.parent)
        else:
            next_cwd: Optional[Folder] = cwd.subfolder(head)
            if next_cwd is None:
                return None
            return Folder.find_by_path(tail, next_cwd)

    def __truediv__(self, name: str) -> Optional["Folder"]:
        return self.subfolder(name)

    def add_folder(self, name: str) -> "Folder":
        """
        Add a subfolder to this folder instance.

        :param name: Name of the new folder name (without /)
        :return: The new folder instance
        """
        return Folder.create(name=name, parent=self)

    def subfolder(self, name: str) -> Optional["Folder"]:
        """
        Retrieve a direct subfolder of this folder instance

        :param name: The unqualified name of the subfolder to retrieve.
        :return: Folder instance if `name` exists, else `None`
        """
        return Folder.get_or_none(Folder.parent == self, Folder.name == name)

    def folders_recursive(self) -> Iterable["Folder"]:
        """
        Recursively find all folders below this one.

        :return: All folders in the hierarchy from this folder. (Excludes this folder)
        """
        crit = (3, 8, 3)
        if sqlite3.sqlite_version_info < crit:  # pragma: no cover
            logger.debug(
                "sqlite3 version %s < %s: use slow python recursion",
                sqlite3.sqlite_version_info,
                crit,
            )
            folders: List["Folder"] = []
            for child in self.children:
                folders.append(child)
                folders += child.folders_recursive()
            return folders

        else:
            logger.debug(
                "sqlite3 version %s >= %s: use CTE", sqlite3.sqlite_version_info, crit
            )
            sql = """
    WITH RECURSIVE
      children(n) AS (
        VALUES(?)
        UNION
        SELECT folder_id FROM folder, children
         WHERE folder.parent_id=children.n
      )
    SELECT * FROM folder where folder_id in children AND folder_id != ?;
    """

            return Folder.raw(sql, int(self.folder_id), int(self.folder_id))

    def jobs_recursive(self) -> Iterable["Job"]:
        """
        Recursively get all jobs in this folder and descendants.

        :return: Iterable over all jobs found, including jobs directly in this folder.
        """
        crit = (3, 8, 3)
        if sqlite3.sqlite_version_info < crit:  # pragma: no cover
            logger.debug(
                "sqlite3 version %s < %s: use slow python recursion",
                sqlite3.sqlite_version_info,
                crit,
            )
            jobs: List["Job"] = list(self.jobs)
            for child in self.children:
                jobs += child.jobs_recursive()
            return jobs
        else:
            logger.debug(
                "sqlite3 version %s >= %s: use CTE", sqlite3.sqlite_version_info, crit
            )
            sql = """
            WITH RECURSIVE
              children(n) AS (
                VALUES(?)
                UNION
                SELECT folder_id FROM folder, children
                 WHERE folder.parent_id=children.n
              )
            SELECT * FROM job where folder_id in children;
            """

            from .job import Job

            return Job.raw(sql, int(self.folder_id))

    def job_stats(self) -> Dict["Job.Status", int]:
        crit = (3, 8, 3)
        if sqlite3.sqlite_version_info < crit:  # pragma: no cover
            jobs = self.jobs_recursive()
            counts = {k: 0 for k in Job.Status}
            for job in jobs:
                counts[job.status] += 1
            return counts
        else:
            sql = """
WITH RECURSIVE
    children(n) AS (
       VALUES(?)
        UNION
        SELECT folder_id FROM folder, children
        WHERE folder.parent_id=children.n
    )
SELECT status, count() FROM job where folder_id in children GROUP BY status;
            """

            cursor = cast(
                Iterable[Tuple[int, int]],
                database.execute_sql(sql, (int(self.folder_id),)),
            )
            counts = {k: 0 for k in Job.Status}
            for status, count in cursor:
                counts[Job.Status(status)] = count

            return counts


# Needed for RTD
from .job import Job  # noqa: E402

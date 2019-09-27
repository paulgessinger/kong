import os
import datetime
from typing import Any, cast, Optional, TYPE_CHECKING, List

import peewee as pw

from ..logger import logger
from ..db import AutoIncrementField
from . import BaseModel

if TYPE_CHECKING:  # pragma: no cover
    from .job import Job


class Folder(BaseModel):
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
        if self.parent is None:
            return "/"
        # this will be slow, could optimize with CTE
        return cast(str, os.path.join(self.parent.path, self.name))

    @classmethod
    def get_root(cls) -> "Folder":
        folder = Folder.get_or_none(Folder.parent.is_null(), name="root")
        if folder is None:
            # bypass assertions
            folder = super(BaseModel, cls).create(name="root", _ignore_save_assert=True)
        return cast(Folder, folder)

    @staticmethod
    def find_by_path(cwd: "Folder", path: str) -> Optional["Folder"]:
        if cwd is None:
            return None
        if path == "/":
            return Folder.get_root()
        if path.startswith("/"):
            return Folder.find_by_path(Folder.get_root(), path[1:])
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
            return Folder.find_by_path(cwd.parent, tail)
        else:
            return Folder.find_by_path(cast(Folder, cwd.subfolder(head)), tail)

    def __truediv__(self, name: str) -> Optional["Folder"]:
        return self.subfolder(name)

    def add_folder(self, name: str) -> "Folder":
        return Folder.create(name=name, parent=self)

    def subfolder(self, name: str) -> Optional["Folder"]:
        return Folder.get_or_none(Folder.parent == self, Folder.name == name)

    def jobs_recursive(self) -> List["Job"]:
        # @TODO: Optimize, hierarchical expression?
        jobs: List[Job] = list(self.jobs)
        for child in self.children:
            jobs += child.jobs_recursive()
        return jobs

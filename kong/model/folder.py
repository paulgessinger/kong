import os
import datetime
from typing import Any, cast, Optional, ClassVar, TYPE_CHECKING, List

import peewee as pw

from ..logger import logger
from . import BaseModel


class Folder(BaseModel):
    if TYPE_CHECKING:
        folder_id: int
        parent: "Folder"
        children: List["Folder"]
        name: str
    else:
        folder_id = pw.AutoField()
        name = pw.CharField()
        parent = pw.ForeignKeyField("self", null=True, backref="children")
        created_at = pw.DateTimeField(default=datetime.datetime.now)
        updated_at = pw.TimestampField()

    _ignore_save_assert = False

    class Meta:
        indexes = ((("parent", "name"), True),)

    def save(self, *args: Any, **kwargs: Any) -> None:
        if not self._ignore_save_assert:
            if "name" in kwargs:
                    name = kwargs["name"]
                    assert (
                        name not in (".", "..", "") and "/" not in name
                    ), f"Invalid folder name '{name}'"
            assert self.parent is not None, "Need to specify a parent folder"
        self._ignore_save_assert = False
        return super().save(*args, **kwargs)


    @property
    def path(self) -> str:
        if self.parent == None:
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
        if cwd == None:
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
        if not "/" in path:
            return cwd.subfolder(path)
        head, tail = path.split(os.sep, 1)
        logger.debug("Resolve path %s in %s: %s, %s", path, cwd.path, head, tail)

        if head == "..":
            return Folder.find_by_path(cwd.parent, tail)
        else:
            # if head != "":
            return Folder.find_by_path(cwd.subfolder(head), tail)
            # else:
            # return Folder.find_by_path(cwd, tail)

    def __truediv__(self, name: str) -> "Folder":
        return self.subfolder(name)

    def add_folder(self, name: str) -> "Folder":
        return Folder.create(name=name, parent=self)

    def subfolder(self, name: str) -> "Folder":
        return Folder.get_or_none(Folder.parent == self, Folder.name == name)

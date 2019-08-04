import os

import peewee as pw

from .. import db
from ..logger import logger


class BaseModel(pw.Model):
    class Meta:
        database = db.database


class Folder(BaseModel):
    folder_id = pw.AutoField()
    name = pw.CharField()
    parent = pw.ForeignKeyField("self", null=True, backref="children")

    class Meta:
        indexes = ((("parent", "name"), True),)

    @classmethod
    def create(cls, **kwargs):
        if "name" in kwargs:
            name = kwargs["name"]
            assert (
                name not in (".", "..", "") and "/" not in name
            ), f"Invalid folder name '{name}'"
        assert "parent" in kwargs, "Need to specify a parent folder"
        assert kwargs["parent"] is not None, "Need to specify a parent folder"
        return super(BaseModel, cls).create(**kwargs)

    @property
    def path(self):
        if self.parent == None:
            return "/"
        # this will be slow, could optimize with CTE
        return os.path.join(self.parent.path, self.name)

    @staticmethod
    def get_root():
        folder = Folder.get_or_none(Folder.parent.is_null(), name="root")
        if folder is None:
            # bypass assertions
            folder = super(BaseModel, Folder).create(name="root")
        return folder

    @staticmethod
    def find_by_path(cwd, path):
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

    def __truediv__(self, name):
        return self.subfolder(name)

    def add_folder(self, name):
        return Folder.create(name=name, parent=self)

    def subfolder(self, name):
        return Folder.get_or_none(Folder.parent == self, Folder.name == name)
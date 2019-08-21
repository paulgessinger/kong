import peewee as pw

from .. import db

__all__ = ["Folder", "Job"]


class BaseModel(pw.Model):
    class Meta:
        database = db.database

    def reload(self) -> None:
        newer_self = type(self).get_by_id(self._pk)  # type: ignore
        for field_name in self._meta.fields.keys():  # type: ignore
            val = getattr(newer_self, field_name)  # type: ignore
            setattr(self, field_name, val)  # type: ignore
        self._dirty.clear()  # type: ignore


from .folder import Folder
from .job import Job

import peewee as pw

from .. import db

__all__ = ["Folder", "Job"]


class BaseModel(pw.Model):
    class Meta:
        database = db.database

    def reload(self):
        newer_self = type(self).get_by_id(self._pk)
        for field_name in self._meta.fields.keys():
            val = getattr(newer_self, field_name)
            setattr(self, field_name, val)
        self._dirty.clear()


from .folder import Folder
from .job import Job

import peewee as pw

from .. import db

__all__ = ["Folder", "Job"]


class BaseModel(pw.Model):
    class Meta:
        database = db.database


from .folder import Folder
from .job import Job

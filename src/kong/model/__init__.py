# flake8: noqa
from typing import Any, List, Iterable, TypeVar

import peewee as pw

from ..util import chunks
from .. import db

T = TypeVar("T", bound="BaseModel")


class BaseModel(pw.Model):
    class Meta:
        database = db.database

    def reload(self) -> None:
        newer_self = type(self).get_by_id(self._pk)  # type: ignore
        for field_name in self._meta.fields.keys():  # type: ignore
            val = getattr(newer_self, field_name)  # type: ignore
            setattr(self, field_name, val)  # type: ignore
        self._dirty.clear()  # type: ignore

    @classmethod
    def bulk_select(
        cls, field: Any, values: List[Any], batch_size: int = 999
    ) -> Iterable[T]:
        for chunk in chunks(values, batch_size):
            yield from cls.select().where(field.in_(chunk)).execute()  # type: ignore

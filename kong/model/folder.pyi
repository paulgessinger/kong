from typing import Optional, List, Any


class BaseModel:
    def delete_instance(self, **kwargs: Any) -> None: ...



class Folder(BaseModel):
    parent: 'Folder'
    children: List['Folder']
    path: str
    name: str

    @classmethod
    def create(cls, **kwargs: Any) -> 'Folder': ...

    @staticmethod
    def get_root() -> 'Folder': ...
    def find_by_path(self, path: str) -> Optional['Folder']: ...

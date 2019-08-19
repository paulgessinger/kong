from typing import List, Any, Union, TYPE_CHECKING

from abc import *

__all__: List[str] = ["LocalDriver"]

Driver = Union["LocalDriver"]

from ..model import Folder, Job
from ..config import Config


class DriverBase(ABC):
    @abstractmethod
    def __init__(self, config: Config) -> None:
        raise NotImplemented()

    @abstractmethod
    def create_job(self, folder: 'Folder', command: str, cores: int, *args: Any, **kwargs: Any) -> 'Job':
        raise NotImplemented()


from .local_driver import LocalDriver

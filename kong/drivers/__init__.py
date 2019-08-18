from typing import List, Any

from abc import *

from ..model import Folder, Job

__all__: List[str] = ['DriverBase', 'LocalDriver']

class DriverBase(ABC):
    def __init__(self)->None:
        raise TypeError("Driver classes cannot be instantiated")

    def __new__(cls, *args:Any, **kwargs: Any)->None:
        raise TypeError("Driver classes cannot be instantiated")

    @classmethod
    @abstractmethod
    def create_job(self, folder: Folder, *args:Any, **kwargs:Any) -> Job:
        raise NotImplemented()


from .local_driver import LocalDriver, Job

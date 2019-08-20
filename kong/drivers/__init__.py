from contextlib import _GeneratorContextManager
from typing import List, Any, Union, Optional, ContextManager, Iterable

from abc import *

__all__: List[str] = ["LocalDriver"]

Driver = Union["LocalDriver"]


class InvalidJobStatus(BaseException):
    pass


class DriverMismatch(BaseException):
    pass


from ..model import Folder, Job
from ..config import Config


class DriverBase(ABC):
    @abstractmethod
    def __init__(self, config: Config) -> None:
        raise NotImplemented()

    @abstractmethod
    def create_job(
        self, folder: "Folder", command: str, cores: int, *args: Any, **kwargs: Any
    ) -> "Job":
        raise NotImplemented()

    @abstractmethod
    def sync_status(self, job: Job) -> None:
        raise NotImplemented()

    @abstractmethod
    def bulk_sync_status(self, jobs: Iterable[Job]) -> None:
        raise NotImplemented()

    @abstractmethod
    def kill(self, job: Job) -> None:
        raise NotImplemented()

    @abstractmethod
    def wait(self, job: Union[Job, List[Job]], timeout: Optional[int] = None) -> None:
        raise NotImplemented()

    @abstractmethod
    def submit(self, job: Job) -> None:
        raise NotImplemented()

    @abstractmethod
    def stdout(self, job: Job) -> ContextManager[None]:
        raise NotImplemented()

    @abstractmethod
    def stderr(self, job: Job) -> ContextManager[None]:
        raise NotImplemented()

    @abstractmethod
    def resubmit(self, job: Job) -> None:
        raise NotImplemented()

    @abstractmethod
    def cleanup(self, job: Job) -> None:
        raise NotImplemented()


from .local_driver import LocalDriver

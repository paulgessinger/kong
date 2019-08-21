from typing import List, Any, Union, Optional, ContextManager, Iterable, TYPE_CHECKING
from abc import *


if TYPE_CHECKING:
    from ..config import Config
    from ..model import Job, Folder


class DriverBase(ABC):
    @abstractmethod
    def __init__(self, config: "Config") -> None:
        raise NotImplemented()

    @abstractmethod
    def create_job(
        self, folder: "Folder", command: str, cores: int, *args: Any, **kwargs: Any
    ) -> "Job":
        raise NotImplemented()

    @abstractmethod
    def sync_status(self, job: "Job") -> None:
        raise NotImplemented()

    @abstractmethod
    def bulk_sync_status(self, jobs: Iterable["Job"]) -> None:
        raise NotImplemented()

    @abstractmethod
    def kill(self, job: "Job") -> None:
        raise NotImplemented()

    @abstractmethod
    def wait(
        self, job: Union["Job", List["Job"]], timeout: Optional[int] = None
    ) -> None:
        raise NotImplemented()

    @abstractmethod
    def submit(self, job: "Job") -> None:
        raise NotImplemented()

    @abstractmethod
    def stdout(self, job: "Job") -> ContextManager[None]:
        raise NotImplemented()

    @abstractmethod
    def stderr(self, job: "Job") -> ContextManager[None]:
        raise NotImplemented()

    @abstractmethod
    def resubmit(self, job: "Job") -> None:
        raise NotImplemented()

    @abstractmethod
    def cleanup(self, job: "Job") -> None:
        raise NotImplemented()

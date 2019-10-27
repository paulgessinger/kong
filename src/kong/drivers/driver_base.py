import functools
import os
from typing import (
    List,
    Any,
    Union,
    Optional,
    ContextManager,
    Iterable,
    TYPE_CHECKING,
    Dict,
    Collection,
)
from abc import abstractmethod, ABC

from kong.drivers import DriverMismatch
from kong.util import exhaust
from ..logger import logger
from ..config import Config

if TYPE_CHECKING:  # pragma: no cover
    from ..model import Job, Folder


def checked_job(f: Any) -> Any:
    @functools.wraps(f)
    def wrapper(self: Any, job: "Job", *args: Any, **kwargs: Any) -> Any:
        self._check_driver(job)
        return f(self, job, *args, **kwargs)

    return wrapper


class DriverBase(ABC):  # pragma: no-cover
    config: Config

    batch_size: int = 50
    select_batch_size: int = 500

    def __init__(self, config: Optional["Config"]) -> None:
        if config is None:
            logger.debug("Attempt to default-construct configuration object")
            self.config = Config()
        else:
            logger.debug("Taking explicit config")
            self.config = config

        logger.debug("Checking jobdir filesystem at %s", self.config.jobdir)
        assert os.path.exists(self.config.jobdir)

    @classmethod
    def _check_driver(cls, job: "Job") -> None:
        # check if we're the right driver for this
        if cls != job.driver:
            raise DriverMismatch(f"Job {job} is has driver {job.driver}, not {cls}")

    @abstractmethod
    def create_job(
        self, folder: "Folder", command: str, cores: int  # , *args: Any, **kwargs: Any
    ) -> "Job":
        raise NotImplementedError()

    @abstractmethod
    def bulk_create_jobs(self, jobs: Iterable[Dict[str, Any]]) -> List["Job"]:
        raise NotImplementedError()

    @abstractmethod
    def sync_status(self, job: "Job") -> "Job":
        raise NotImplementedError()

    @abstractmethod
    def bulk_sync_status(self, jobs: Collection["Job"]) -> Iterable["Job"]:
        raise NotImplementedError()

    @abstractmethod
    def kill(self, job: "Job") -> "Job":
        raise NotImplementedError()

    @abstractmethod
    def bulk_kill(self, jobs: Collection["Job"]) -> Iterable["Job"]:
        raise NotImplementedError()

    @abstractmethod
    def wait_gen(
        self,
        job: Union["Job", List["Job"]],
        poll_interval: Optional[int] = None,
        timeout: Optional[int] = None,
    ) -> Iterable[List["Job"]]:
        raise NotImplementedError()

    def wait(
        self,
        job: Union["Job", List["Job"]],
        poll_interval: Optional[int] = None,
        timeout: Optional[int] = None,
        progress: bool = False,
    ) -> Optional[Iterable[List["Job"]]]:
        it = self.wait_gen(job, poll_interval=poll_interval, timeout=timeout)
        if progress:
            return it
        else:
            exhaust(it)
            return None

    @abstractmethod
    def submit(self, job: "Job") -> None:
        raise NotImplementedError()

    @abstractmethod
    def bulk_submit(self, jobs: Iterable["Job"]) -> None:
        raise NotImplementedError()

    @abstractmethod
    def stdout(self, job: "Job") -> ContextManager[None]:
        raise NotImplementedError()

    @abstractmethod
    def stderr(self, job: "Job") -> ContextManager[None]:
        raise NotImplementedError()

    @abstractmethod
    def resubmit(self, job: "Job") -> "Job":
        raise NotImplementedError()

    @abstractmethod
    def bulk_resubmit(
        self, jobs: Collection["Job"], do_submit: bool = True
    ) -> Iterable["Job"]:
        raise NotImplementedError()

    @abstractmethod
    def cleanup(self, job: "Job") -> "Job":
        raise NotImplementedError()

    @abstractmethod
    def bulk_cleanup(self, jobs: Collection["Job"]) -> Collection["Job"]:
        raise NotImplementedError()

    @abstractmethod
    def remove(self, job: "Job") -> None:
        raise NotImplementedError()

    @abstractmethod
    def bulk_remove(self, jobs: Collection["Job"]) -> None:
        raise NotImplementedError()

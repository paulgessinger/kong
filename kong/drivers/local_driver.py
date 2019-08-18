from typing import Any

from . import DriverBase
from ..model import *

class LocalDriver(DriverBase):

    def __init__(self) -> None:
        print("LOCAL INIT")
        pass

    @classmethod
    def create_job(self, folder: Folder, *args: Any, **kwargs: Any) -> Job:
        pass

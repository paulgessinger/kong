from . import DriverBase


class LocalDriver(DriverBase):
    def __init__(self, config):
        self.config = config

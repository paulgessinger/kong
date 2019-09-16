from ..logger import logger


class InvalidJobStatus(BaseException):
    pass


class DriverMismatch(BaseException):
    pass


def get_driver(value: str) -> type:
    logger.debug("Attempting loading driver %s", value)
    import importlib

    components = value.split(".")
    module_name = ".".join(components[:-1])
    class_name = components[-1]

    module = importlib.import_module(module_name)
    class_: type = getattr(module, class_name)
    return class_

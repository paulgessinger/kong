from concurrent.futures import Executor, Future

from typing import Callable, Any, TypeVar

T = TypeVar("T")


class SerialExecutor(Executor):
    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Future:
        f: Future = Future()
        try:
            f.set_result(fn(*args, **kwargs))
        except Exception as e:
            f.set_exception(e)
        return f

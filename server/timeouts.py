"""
Bounds how long the server waits for a blocking database call.

The engine has no way to cancel a running query, and DatabasePool serializes
all access to a given database, so one runaway query can otherwise block
every other request against that database indefinitely. This gives the
client a clear, timely error instead of an unbounded hang.
"""

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from typing import Callable, TypeVar

T = TypeVar('T')


class QueryTimeoutError(Exception):
    pass


class TimeoutGuard:
    def __init__(self, max_workers: int = 32) -> None:
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def run(self, action: Callable[[], T], seconds: float) -> T:
        future = self._executor.submit(action)
        try:
            return future.result(timeout=seconds)
        except FutureTimeoutError:
            raise QueryTimeoutError(f"Query exceeded {seconds}s timeout") from None

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False)

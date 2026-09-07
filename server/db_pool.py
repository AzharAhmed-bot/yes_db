"""
Thread-safe pool of open YesDB connections, keyed by (user_id, db_name).

The underlying engine (Pager/BTree) has no internal concurrency control,
so every access to a pooled connection is serialized through a per-key lock.
"""

import threading
from typing import Callable, Dict, List, Tuple

from chidb.api import YesDB

PoolKey = Tuple[str, str]


class DatabasePool:
    def __init__(self) -> None:
        self._connections: Dict[PoolKey, YesDB] = {}
        self._locks: Dict[PoolKey, threading.Lock] = {}
        self._pool_lock = threading.Lock()

    def get_or_create(self, key: PoolKey, factory: Callable[[], YesDB]) -> YesDB:
        """Return the pooled connection for key, opening it via factory if needed."""
        with self._pool_lock:
            if key not in self._connections:
                self._connections[key] = factory()
                self._locks[key] = threading.Lock()
            return self._connections[key]

    def register(self, key: PoolKey, db: YesDB) -> None:
        """Add an already-open connection to the pool."""
        with self._pool_lock:
            self._connections[key] = db
            self._locks.setdefault(key, threading.Lock())

    def is_open(self, key: PoolKey) -> bool:
        with self._pool_lock:
            return key in self._connections

    def run_exclusive(self, key: PoolKey, action: Callable):
        """Run action while holding the per-key lock, serializing engine access."""
        with self._pool_lock:
            lock = self._locks[key]
        with lock:
            return action()

    def values(self) -> List[YesDB]:
        with self._pool_lock:
            return list(self._connections.values())

    def clear(self) -> None:
        with self._pool_lock:
            self._connections.clear()
            self._locks.clear()

    def close_all(self) -> None:
        for db in self.values():
            try:
                db.close()
            except Exception:
                pass
        self.clear()

"""Tests for server/db_pool.py — thread-safe connection pooling and locking."""

import threading
import time

import pytest

from server.db_pool import DatabasePool


class FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class TestGetOrCreate:
    def test_returns_new_connection_on_first_call(self):
        pool = DatabasePool()
        conn = pool.get_or_create(("u1", "db1"), FakeConnection)
        assert isinstance(conn, FakeConnection)

    def test_returns_same_connection_on_repeat_calls(self):
        pool = DatabasePool()
        first = pool.get_or_create(("u1", "db1"), FakeConnection)
        second = pool.get_or_create(("u1", "db1"), FakeConnection)
        assert first is second

    def test_concurrent_first_access_opens_connection_only_once(self):
        pool = DatabasePool()
        open_count = 0
        open_count_lock = threading.Lock()

        def slow_factory():
            nonlocal open_count
            time.sleep(0.02)
            with open_count_lock:
                open_count += 1
            return FakeConnection()

        threads = [
            threading.Thread(target=pool.get_or_create, args=(("u1", "db1"), slow_factory))
            for _ in range(10)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert open_count == 1


class TestRunExclusive:
    def test_serializes_concurrent_access_to_same_key(self):
        pool = DatabasePool()
        pool.get_or_create(("u1", "db1"), FakeConnection)
        counter = {"value": 0}

        def unsafe_increment():
            current = counter["value"]
            time.sleep(0.01)
            counter["value"] = current + 1

        threads = [
            threading.Thread(
                target=pool.run_exclusive, args=(("u1", "db1"), unsafe_increment)
            )
            for _ in range(20)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert counter["value"] == 20

    def test_raises_for_unknown_key(self):
        pool = DatabasePool()
        with pytest.raises(KeyError):
            pool.run_exclusive(("missing", "db"), lambda: None)

    def test_returns_action_result(self):
        pool = DatabasePool()
        pool.get_or_create(("u1", "db1"), FakeConnection)
        result = pool.run_exclusive(("u1", "db1"), lambda: 42)
        assert result == 42


class TestPoolManagement:
    def test_is_open_reflects_pool_state(self):
        pool = DatabasePool()
        assert pool.is_open(("u1", "db1")) is False
        pool.get_or_create(("u1", "db1"), FakeConnection)
        assert pool.is_open(("u1", "db1")) is True

    def test_register_adds_existing_connection(self):
        pool = DatabasePool()
        conn = FakeConnection()
        pool.register(("u1", "db1"), conn)
        assert pool.is_open(("u1", "db1")) is True
        assert pool.run_exclusive(("u1", "db1"), lambda: "ok") == "ok"

    def test_clear_empties_pool_without_closing(self):
        pool = DatabasePool()
        conn = pool.get_or_create(("u1", "db1"), FakeConnection)
        pool.clear()
        assert pool.is_open(("u1", "db1")) is False
        assert conn.closed is False

    def test_close_all_closes_every_connection(self):
        pool = DatabasePool()
        conn_a = pool.get_or_create(("u1", "db1"), FakeConnection)
        conn_b = pool.get_or_create(("u2", "db2"), FakeConnection)
        pool.close_all()
        assert conn_a.closed is True
        assert conn_b.closed is True
        assert pool.is_open(("u1", "db1")) is False

    def test_close_all_survives_close_errors(self):
        class BrokenConnection:
            def close(self):
                raise RuntimeError("boom")

        pool = DatabasePool()
        pool.get_or_create(("u1", "db1"), BrokenConnection)
        pool.close_all()
        assert pool.is_open(("u1", "db1")) is False

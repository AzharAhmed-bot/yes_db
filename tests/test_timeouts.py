"""Tests for server/timeouts.py — bounding how long the server waits on a call."""

import threading
import time

import pytest

from server.timeouts import TimeoutGuard, QueryTimeoutError


class TestTimeoutGuard:
    def test_returns_result_when_action_finishes_in_time(self):
        guard = TimeoutGuard()
        result = guard.run(lambda: 42, seconds=1)
        assert result == 42

    def test_raises_on_slow_action(self):
        guard = TimeoutGuard()

        def slow_action():
            time.sleep(0.2)
            return "too late"

        with pytest.raises(QueryTimeoutError):
            guard.run(slow_action, seconds=0.05)

    def test_propagates_action_exceptions(self):
        guard = TimeoutGuard()

        def failing_action():
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            guard.run(failing_action, seconds=1)

    def test_runs_multiple_actions_concurrently(self):
        guard = TimeoutGuard(max_workers=4)
        results = [None] * 4

        def sleep_briefly(index):
            results[index] = guard.run(lambda: time.sleep(0.1) or "done", seconds=1)

        start = time.monotonic()
        threads = [threading.Thread(target=sleep_briefly, args=(i,)) for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = time.monotonic() - start

        assert results == ["done"] * 4
        assert elapsed < 0.3

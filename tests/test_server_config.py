"""Tests for server/config.py"""

import os
import pytest


class TestSettings:
    """Test server configuration and defaults."""

    def test_default_data_dir(self):
        """Defaults should be set when no env vars are present."""
        # Re-import to pick up current env
        from server.config import Settings
        s = Settings()
        assert s.DATA_DIR == "/var/lib/yesdb/data"

    def test_default_accounts_db(self):
        from server.config import Settings
        s = Settings()
        assert s.ACCOUNTS_DB_PATH == "/var/lib/yesdb/accounts.db"

    def test_default_host_and_port(self):
        from server.config import Settings
        s = Settings()
        assert s.HOST == "127.0.0.1"
        assert s.PORT == 8000

    def test_env_override_data_dir(self, monkeypatch):
        """Environment variables should override defaults."""
        monkeypatch.setenv("YESDB_DATA_DIR", "/tmp/custom_data")
        from server.config import Settings
        s = Settings()
        assert s.DATA_DIR == "/tmp/custom_data"

    def test_env_override_accounts_db(self, monkeypatch):
        monkeypatch.setenv("YESDB_ACCOUNTS_DB", "/tmp/custom_accounts.db")
        from server.config import Settings
        s = Settings()
        assert s.ACCOUNTS_DB_PATH == "/tmp/custom_accounts.db"

    def test_env_override_host(self, monkeypatch):
        monkeypatch.setenv("YESDB_HOST", "0.0.0.0")
        from server.config import Settings
        s = Settings()
        assert s.HOST == "0.0.0.0"

    def test_env_override_port(self, monkeypatch):
        monkeypatch.setenv("YESDB_PORT", "9090")
        from server.config import Settings
        s = Settings()
        assert s.PORT == 9090

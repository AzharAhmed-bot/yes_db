"""Tests for chidb/client.py — CloudConnection and ExecuteResult."""

import json
import os
import pytest
from unittest.mock import patch, MagicMock

from chidb.client import (
    ExecuteResult,
    CloudConnection,
    load_credentials,
    save_credentials,
)


# ── ExecuteResult ────────────────────────────────────────────────


class TestExecuteResult:
    def test_rows_and_logs(self):
        r = ExecuteResult(
            rows=[[1, "Alice"], [2, "Bob"]],
            logs=[{"level": "DEBUG", "component": "sql", "message": "Parsing", "timestamp": ""}],
        )
        assert r.rows == [[1, "Alice"], [2, "Bob"]]
        assert r.row_count == 2
        assert len(r.logs) == 1

    def test_empty_result(self):
        r = ExecuteResult(rows=[], logs=[])
        assert r.rows == []
        assert r.row_count == 0
        assert len(r) == 0
        assert not r  # __bool__ is False for empty

    def test_iter(self):
        r = ExecuteResult(rows=[[1], [2], [3]], logs=[])
        assert list(r) == [[1], [2], [3]]

    def test_len(self):
        r = ExecuteResult(rows=[[1], [2]], logs=[])
        assert len(r) == 2

    def test_bool_true_when_rows(self):
        r = ExecuteResult(rows=[[1]], logs=[])
        assert r

    def test_repr(self):
        r = ExecuteResult(rows=[[1]], logs=[{"a": "b"}, {"c": "d"}])
        assert "rows=1" in repr(r)
        assert "logs=2" in repr(r)

    def test_print_logs(self, capsys):
        r = ExecuteResult(
            rows=[],
            logs=[
                {
                    "level": "DEBUG",
                    "component": "sql",
                    "message": "Parsing SQL: SELECT * FROM users",
                    "timestamp": "2026-02-17 10:30:01",
                },
                {
                    "level": "INFO",
                    "component": "btree",
                    "message": "Splitting node at page 2",
                    "timestamp": "2026-02-17 10:30:01",
                },
            ],
        )
        r.print_logs()
        captured = capsys.readouterr()
        assert "chidb.sql" in captured.out
        assert "Parsing SQL" in captured.out
        assert "chidb.btree" in captured.out
        assert "Splitting node" in captured.out

    def test_row_count_override(self):
        r = ExecuteResult(rows=[[1], [2]], logs=[], row_count=5)
        assert r.row_count == 5  # respects explicit count


# ── Credentials ──────────────────────────────────────────────────


class TestCredentials:
    def test_save_and_load(self, tmp_path):
        cred_path = str(tmp_path / ".yesdb" / "credentials.json")
        save_credentials("a@b.com", "yesdb_key123", "https://example.com", path=cred_path)

        creds = load_credentials(path=cred_path)
        assert creds["email"] == "a@b.com"
        assert creds["api_key"] == "yesdb_key123"
        assert creds["server_url"] == "https://example.com"

    def test_load_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No credentials found"):
            load_credentials(path=str(tmp_path / "nonexistent.json"))

    def test_save_creates_directory(self, tmp_path):
        cred_path = str(tmp_path / "deep" / "nested" / "creds.json")
        save_credentials("x@y.com", "key", "https://srv.com", path=cred_path)
        assert os.path.exists(cred_path)


# ── CloudConnection (unit tests with mocked HTTP) ────────────────


class TestCloudConnectionUnit:
    @pytest.fixture
    def mock_creds(self, tmp_path):
        """Create a temporary credentials file."""
        cred_path = str(tmp_path / "credentials.json")
        save_credentials("test@uni.edu", "yesdb_testkey", "https://yesdb.test.com", path=cred_path)
        return cred_path

    def test_init_with_explicit_params(self):
        conn = CloudConnection(
            db_name="mydb",
            api_key="yesdb_key",
            server_url="https://example.com",
        )
        assert conn.db_name == "mydb"
        assert conn.api_key == "yesdb_key"
        assert conn.server_url == "https://example.com"

    def test_init_loads_credentials(self, mock_creds):
        with patch("chidb.client.CREDENTIALS_PATH", mock_creds), \
             patch("chidb.client.load_credentials", wraps=lambda path=mock_creds: load_credentials(path)):
            # Need to patch the default arg, so patch the function itself
            import chidb.client as client_mod
            original_path = client_mod.CREDENTIALS_PATH
            client_mod.CREDENTIALS_PATH = mock_creds
            try:
                conn = CloudConnection(db_name="mydb")
                assert conn.api_key == "yesdb_testkey"
                assert conn.server_url == "https://yesdb.test.com"
            finally:
                client_mod.CREDENTIALS_PATH = original_path

    def test_init_no_credentials_raises(self, tmp_path):
        with patch("chidb.client.CREDENTIALS_PATH", str(tmp_path / "nope.json")):
            with pytest.raises(FileNotFoundError):
                CloudConnection(db_name="mydb")

    def test_url_building(self):
        conn = CloudConnection("mydb", api_key="k", server_url="https://srv.com")
        assert conn._url("/execute") == "https://srv.com/api/v1/databases/mydb/execute"
        assert conn._url("/tables") == "https://srv.com/api/v1/databases/mydb/tables"

    def test_strips_trailing_slash(self):
        conn = CloudConnection("mydb", api_key="k", server_url="https://srv.com/")
        assert conn.server_url == "https://srv.com"

    def test_repr(self):
        conn = CloudConnection("mydb", api_key="k", server_url="https://srv.com")
        r = repr(conn)
        assert "mydb" in r
        assert "srv.com" in r

    def test_context_manager(self):
        conn = CloudConnection("mydb", api_key="k", server_url="https://srv.com")
        with conn as c:
            assert c is conn

    def test_execute_calls_correct_endpoint(self):
        conn = CloudConnection("mydb", api_key="k", server_url="https://srv.com")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"rows": [[1, "Alice"]], "row_count": 1, "logs": []}
        conn.session.post = MagicMock(return_value=mock_resp)

        result = conn.execute("SELECT * FROM users")
        conn.session.post.assert_called_once_with(
            "https://srv.com/api/v1/databases/mydb/execute",
            json={"sql": "SELECT * FROM users"},
        )
        assert result.rows == [[1, "Alice"]]
        assert result.row_count == 1

    def test_execute_returns_execute_result(self):
        conn = CloudConnection("mydb", api_key="k", server_url="https://srv.com")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "rows": [],
            "row_count": 0,
            "logs": [{"level": "INFO", "component": "api", "message": "Created table", "timestamp": ""}],
        }
        conn.session.post = MagicMock(return_value=mock_resp)

        result = conn.execute("CREATE TABLE t (id INTEGER)")
        assert isinstance(result, ExecuteResult)
        assert len(result.logs) == 1

    def test_execute_401_raises_permission_error(self):
        conn = CloudConnection("mydb", api_key="k", server_url="https://srv.com")
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        conn.session.post = MagicMock(return_value=mock_resp)

        with pytest.raises(PermissionError, match="Invalid API key"):
            conn.execute("SELECT 1")

    def test_execute_400_raises_value_error(self):
        conn = CloudConnection("mydb", api_key="k", server_url="https://srv.com")
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.json.return_value = {"detail": "Syntax error in SQL"}
        conn.session.post = MagicMock(return_value=mock_resp)

        with pytest.raises(ValueError, match="Syntax error"):
            conn.execute("BAD SQL")

    def test_execute_404_raises_value_error(self):
        conn = CloudConnection("nope", api_key="k", server_url="https://srv.com")
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.json.return_value = {"detail": "Database 'nope' not found"}
        conn.session.post = MagicMock(return_value=mock_resp)

        with pytest.raises(ValueError, match="not found"):
            conn.execute("SELECT 1")

    def test_get_table_names(self):
        conn = CloudConnection("mydb", api_key="k", server_url="https://srv.com")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"tables": ["users", "posts"], "logs": []}
        conn.session.get = MagicMock(return_value=mock_resp)

        tables = conn.get_table_names()
        assert tables == ["users", "posts"]

    def test_table_exists_true(self):
        conn = CloudConnection("mydb", api_key="k", server_url="https://srv.com")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"tables": ["users"], "logs": []}
        conn.session.get = MagicMock(return_value=mock_resp)

        assert conn.table_exists("users") is True
        assert conn.table_exists("nope") is False


# ── CloudConnection integration test (against real FastAPI server) ──


class TestCloudConnectionIntegration:
    """
    End-to-end tests using FastAPI TestClient as the server
    and CloudConnection as the client.
    """

    @pytest.fixture(autouse=True)
    def setup_server(self, tmp_path, monkeypatch):
        """Set up a real server backed by tmp_path."""
        data_dir = str(tmp_path / "data")
        accounts_db = str(tmp_path / "accounts.db")
        os.makedirs(data_dir)

        from server.config import settings
        monkeypatch.setattr(settings, "DATA_DIR", data_dir)
        monkeypatch.setattr(settings, "ACCOUNTS_DB_PATH", accounts_db)

        import server.auth as auth_module
        auth_module._accounts_db = None

        import server.main as main_module
        main_module._db_pool.clear()

        yield

        for db in main_module._db_pool.values():
            try:
                db.close()
            except Exception:
                pass
        main_module._db_pool.clear()
        auth_module.close_accounts_db()

    @pytest.fixture
    def server_and_key(self):
        """Start a test server, create a user, and return (base_url, api_key)."""
        from fastapi.testclient import TestClient
        from server.main import app

        client = TestClient(app)

        # Sign up
        resp = client.post(
            "/api/v1/signup",
            json={"email": "integ@test.com", "password": "pass123"},
        )
        api_key = resp.json()["api_key"]

        # Create a database
        client.post(
            "/api/v1/databases",
            json={"name": "testdb"},
            headers={"Authorization": f"Bearer {api_key}"},
        )

        return client, api_key

    def _make_cloud_conn(self, test_client, api_key, db_name="testdb"):
        """Create a CloudConnection that routes through the test client."""
        conn = CloudConnection(
            db_name=db_name,
            api_key=api_key,
            server_url="http://testserver",
        )
        # Replace the requests session with the test client's session adapter
        conn.session = test_client
        conn.session.headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        return conn

    def test_create_table_and_insert(self, server_and_key):
        client, api_key = server_and_key
        conn = self._make_cloud_conn(client, api_key)

        result = conn.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        assert isinstance(result, ExecuteResult)
        assert len(result.logs) > 0  # Engine logs returned

        result = conn.execute("INSERT INTO users VALUES (1, 'Alice')")
        assert result.rows == []

    def test_select_returns_rows(self, server_and_key):
        client, api_key = server_and_key
        conn = self._make_cloud_conn(client, api_key)

        conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO items VALUES (1, 'Pen')")
        conn.execute("INSERT INTO items VALUES (2, 'Book')")

        result = conn.execute("SELECT * FROM items")
        assert result.row_count == 2
        assert [1, "Pen"] in result.rows
        assert [2, "Book"] in result.rows

    def test_get_table_names(self, server_and_key):
        client, api_key = server_and_key
        conn = self._make_cloud_conn(client, api_key)

        conn.execute("CREATE TABLE alpha (id INTEGER)")
        conn.execute("CREATE TABLE beta (id INTEGER)")

        tables = conn.get_table_names()
        assert "alpha" in tables
        assert "beta" in tables

    def test_table_exists(self, server_and_key):
        client, api_key = server_and_key
        conn = self._make_cloud_conn(client, api_key)

        conn.execute("CREATE TABLE real_table (id INTEGER)")
        assert conn.table_exists("real_table") is True
        assert conn.table_exists("fake_table") is False

    def test_logs_always_present(self, server_and_key):
        """Every response must include engine logs."""
        client, api_key = server_and_key
        conn = self._make_cloud_conn(client, api_key)

        result = conn.execute("CREATE TABLE logged (id INTEGER)")
        assert len(result.logs) > 0
        assert any("component" in log for log in result.logs)

"""Tests for chidb/cli/cloud.py — Cloud CLI commands."""

import json
import os
import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from chidb.cli.cloud import main, _print_logs, PROJECT_CONFIG_FILE


@pytest.fixture(autouse=True)
def fresh_server(tmp_path, monkeypatch):
    """Set up a fresh server and temp home for each test."""
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

    # Use tmp_path for credentials
    cred_path = str(tmp_path / ".yesdb" / "credentials.json")
    monkeypatch.setattr("chidb.cli.cloud.CREDENTIALS_PATH", cred_path)
    monkeypatch.setattr("chidb.client.CREDENTIALS_PATH", cred_path)

    # Run CLI from a temp working directory
    work_dir = str(tmp_path / "project")
    os.makedirs(work_dir)
    monkeypatch.chdir(work_dir)

    yield tmp_path

    for db in main_module._db_pool.values():
        try:
            db.close()
        except Exception:
            pass
    main_module._db_pool.clear()
    auth_module.close_accounts_db()


@pytest.fixture
def test_client():
    """FastAPI test client."""
    from server.main import app
    return TestClient(app)


def _patch_requests_with_test_client(test_client):
    """
    Create a mock `requests` module that routes through FastAPI's TestClient.
    This lets CLI commands talk to a real server without actual HTTP.
    """
    mock_requests = MagicMock()

    def _post(url, json=None, headers=None, **kwargs):
        # Extract path from URL
        path = "/" + url.split("/", 3)[-1] if "/" in url else url
        # Remove the scheme://host part
        for prefix in ("https://", "http://"):
            if url.startswith(prefix):
                after_scheme = url[len(prefix):]
                slash_idx = after_scheme.find("/")
                if slash_idx >= 0:
                    path = after_scheme[slash_idx:]
                break
        return test_client.post(path, json=json, headers=headers)

    def _get(url, headers=None, **kwargs):
        path = "/" + url.split("/", 3)[-1] if "/" in url else url
        for prefix in ("https://", "http://"):
            if url.startswith(prefix):
                after_scheme = url[len(prefix):]
                slash_idx = after_scheme.find("/")
                if slash_idx >= 0:
                    path = after_scheme[slash_idx:]
                break
        return test_client.get(path, headers=headers)

    mock_requests.post = _post
    mock_requests.get = _get
    mock_requests.ConnectionError = ConnectionError
    return mock_requests


def _signup_user(test_client, tmp_path):
    """Helper: sign up a user via the server directly and save credentials."""
    resp = test_client.post(
        "/api/v1/signup",
        json={"email": "test@uni.edu", "password": "pass123", "name": "Test"},
    )
    assert resp.status_code == 200
    api_key = resp.json()["api_key"]

    # Save credentials to the temp path
    from chidb.client import save_credentials, CREDENTIALS_PATH
    save_credentials("test@uni.edu", api_key, "https://testserver")
    return api_key


# ── print_logs ───────────────────────────────────────────────────


class TestPrintLogs:
    def test_prints_formatted_logs(self, capsys):
        logs = [
            {"level": "DEBUG", "component": "sql", "message": "Parsing SQL", "timestamp": "2026-01-01"},
            {"level": "INFO", "component": "btree", "message": "Split node", "timestamp": "2026-01-01"},
        ]
        _print_logs(logs)
        out = capsys.readouterr().out
        assert "chidb.sql" in out
        assert "Parsing SQL" in out
        assert "chidb.btree" in out

    def test_empty_logs(self, capsys):
        _print_logs([])
        assert capsys.readouterr().out == ""


# ── signup ───────────────────────────────────────────────────────


class TestSignup:
    def test_signup_success(self, test_client, tmp_path, capsys):
        mock_req = _patch_requests_with_test_client(test_client)

        with patch("chidb.cli.cloud.requests", mock_req), \
             patch("builtins.input", return_value="alice@uni.edu"), \
             patch("getpass.getpass", side_effect=["secret", "secret"]):
            result = main(["signup", "--server", "https://testserver"])

        assert result == 0
        out = capsys.readouterr().out
        assert "Account created" in out
        assert "yesdb_" in out

        # Credentials saved
        cred_path = str(tmp_path / ".yesdb" / "credentials.json")
        assert os.path.exists(cred_path)
        with open(cred_path) as f:
            creds = json.load(f)
        assert creds["email"] == "alice@uni.edu"
        assert creds["api_key"].startswith("yesdb_")

    def test_signup_duplicate_email(self, test_client, tmp_path, capsys):
        mock_req = _patch_requests_with_test_client(test_client)

        # First signup
        with patch("chidb.cli.cloud.requests", mock_req), \
             patch("builtins.input", return_value="dup@uni.edu"), \
             patch("getpass.getpass", side_effect=["pass", "pass"]):
            main(["signup", "--server", "https://testserver"])

        # Second signup — same email
        with patch("chidb.cli.cloud.requests", mock_req), \
             patch("builtins.input", return_value="dup@uni.edu"), \
             patch("getpass.getpass", side_effect=["pass2", "pass2"]):
            result = main(["signup", "--server", "https://testserver"])

        assert result == 1

    def test_signup_password_mismatch(self, capsys):
        with patch("builtins.input", return_value="x@y.com"), \
             patch("getpass.getpass", side_effect=["pass1", "pass2"]):
            result = main(["signup", "--server", "https://testserver"])
        assert result == 1
        assert "do not match" in capsys.readouterr().err


# ── login ────────────────────────────────────────────────────────


class TestLogin:
    def test_login_success(self, test_client, tmp_path, capsys):
        # First signup
        _signup_user(test_client, tmp_path)

        mock_req = _patch_requests_with_test_client(test_client)

        with patch("chidb.cli.cloud.requests", mock_req), \
             patch("builtins.input", return_value="test@uni.edu"), \
             patch("getpass.getpass", return_value="pass123"):
            result = main(["login", "--server", "https://testserver"])

        assert result == 0
        out = capsys.readouterr().out
        assert "Logged in" in out

    def test_login_wrong_password(self, test_client, tmp_path, capsys):
        _signup_user(test_client, tmp_path)
        mock_req = _patch_requests_with_test_client(test_client)

        with patch("chidb.cli.cloud.requests", mock_req), \
             patch("builtins.input", return_value="test@uni.edu"), \
             patch("getpass.getpass", return_value="wrong"):
            result = main(["login", "--server", "https://testserver"])

        assert result == 1


# ── init ─────────────────────────────────────────────────────────


class TestInit:
    def test_init_creates_folder_and_schema(self, test_client, tmp_path, capsys):
        _signup_user(test_client, tmp_path)
        mock_req = _patch_requests_with_test_client(test_client)

        with patch("chidb.cli.cloud.requests", mock_req):
            result = main(["init", "myproject"])

        assert result == 0

        # Check local files created
        assert os.path.exists("yesdb/schema.py")
        assert os.path.exists("yesdb/.yesdb.json")

        with open("yesdb/.yesdb.json") as f:
            config = json.load(f)
        assert config["database"] == "myproject"

        out = capsys.readouterr().out
        assert "myproject" in out
        assert "schema.py" in out

    def test_init_not_logged_in(self, capsys):
        result = main(["init", "mydb"])
        assert result == 1
        assert "Not logged in" in capsys.readouterr().err


# ── push ─────────────────────────────────────────────────────────


class TestPush:
    def test_push_schema(self, test_client, tmp_path, capsys):
        api_key = _signup_user(test_client, tmp_path)
        mock_req = _patch_requests_with_test_client(test_client)

        # Init project
        with patch("chidb.cli.cloud.requests", mock_req):
            main(["init", "pushtest"])

        # Write a real schema
        with open("yesdb/schema.py", "w") as f:
            f.write(
                "from chidb.schema import Table, Column, Integer, Text\n\n"
                "users = Table('users', [\n"
                "    Column('id', Integer, primary_key=True),\n"
                "    Column('name', Text),\n"
                "])\n\n"
                "posts = Table('posts', [\n"
                "    Column('id', Integer, primary_key=True),\n"
                "    Column('title', Text),\n"
                "])\n"
            )

        # Push
        with patch("chidb.cli.cloud.requests", mock_req):
            result = main(["push"])

        assert result == 0
        out = capsys.readouterr().out
        assert "users" in out
        assert "posts" in out
        assert "2 statement(s) pushed" in out

    def test_push_no_project(self, capsys):
        result = main(["push"])
        assert result == 1
        assert "No yesdb project" in capsys.readouterr().err

    def test_push_empty_schema(self, test_client, tmp_path, capsys):
        _signup_user(test_client, tmp_path)
        mock_req = _patch_requests_with_test_client(test_client)

        with patch("chidb.cli.cloud.requests", mock_req):
            main(["init", "emptydb"])

        # schema.py has no tables (just the template comments)
        with patch("chidb.cli.cloud.requests", mock_req):
            result = main(["push"])

        assert result == 0
        assert "Nothing to push" in capsys.readouterr().out


# ── databases ────────────────────────────────────────────────────


class TestDatabases:
    def test_list_empty(self, test_client, tmp_path, capsys):
        _signup_user(test_client, tmp_path)
        mock_req = _patch_requests_with_test_client(test_client)

        with patch("chidb.cli.cloud.requests", mock_req):
            result = main(["databases"])

        assert result == 0
        assert "No databases" in capsys.readouterr().out

    def test_list_after_init(self, test_client, tmp_path, capsys):
        _signup_user(test_client, tmp_path)
        mock_req = _patch_requests_with_test_client(test_client)

        with patch("chidb.cli.cloud.requests", mock_req):
            main(["init", "proj1"])
            main(["init", "proj2"])
            result = main(["databases"])

        assert result == 0
        out = capsys.readouterr().out
        assert "proj1" in out
        assert "proj2" in out

    def test_list_not_logged_in(self, capsys):
        result = main(["databases"])
        assert result == 1
        assert "Not logged in" in capsys.readouterr().err


# ── No command shows help ────────────────────────────────────────


class TestNoCommand:
    def test_no_args_shows_help(self, capsys):
        result = main([])
        assert result == 0
        out = capsys.readouterr().out
        assert "signup" in out
        assert "login" in out
        assert "init" in out
        assert "push" in out

"""Tests for server/main.py — FastAPI routes and log capture."""

import os
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def fresh_server(tmp_path, monkeypatch):
    """Set up a fresh server environment for each test."""
    data_dir = str(tmp_path / "data")
    accounts_db = str(tmp_path / "accounts.db")
    os.makedirs(data_dir)

    from server.config import settings
    monkeypatch.setattr(settings, "DATA_DIR", data_dir)
    monkeypatch.setattr(settings, "ACCOUNTS_DB_PATH", accounts_db)

    # Reset auth module state
    import server.auth as auth_module
    auth_module._accounts_db = None

    # Reset db pool
    import server.main as main_module
    main_module._pool.clear()

    yield

    # Cleanup
    main_module._pool.close_all()
    auth_module.close_accounts_db()


@pytest.fixture
def client():
    """FastAPI test client."""
    from server.main import app
    return TestClient(app)


@pytest.fixture
def api_key(client):
    """Create a test user and return their API key."""
    resp = client.post(
        "/api/v1/signup",
        json={"email": "test@uni.edu", "password": "pass123", "name": "Test User"},
    )
    assert resp.status_code == 200
    return resp.json()["api_key"]


@pytest.fixture
def auth_headers(api_key):
    """Authorization headers with the test user's API key."""
    return {"Authorization": f"Bearer {api_key}"}


@pytest.fixture
def test_db(client, auth_headers):
    """Create a test database and return its name."""
    resp = client.post(
        "/api/v1/databases",
        json={"name": "testdb"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    return "testdb"


# ── Health ───────────────────────────────────────────────────────


class TestHealth:
    def test_health_check(self, client):
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


# ── Signup ───────────────────────────────────────────────────────


class TestSignup:
    def test_signup_success(self, client):
        resp = client.post(
            "/api/v1/signup",
            json={"email": "alice@uni.edu", "password": "secret", "name": "Alice"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["api_key"].startswith("yesdb_")
        assert "alice@uni.edu" in data["message"]

    def test_signup_without_name(self, client):
        resp = client.post(
            "/api/v1/signup",
            json={"email": "bob@uni.edu", "password": "secret"},
        )
        assert resp.status_code == 200
        assert resp.json()["api_key"].startswith("yesdb_")

    def test_signup_duplicate_email(self, client):
        client.post(
            "/api/v1/signup",
            json={"email": "dup@uni.edu", "password": "pass"},
        )
        resp = client.post(
            "/api/v1/signup",
            json={"email": "dup@uni.edu", "password": "other"},
        )
        assert resp.status_code == 409


# ── Login ────────────────────────────────────────────────────────


class TestLogin:
    def test_login_success(self, client, api_key):
        resp = client.post(
            "/api/v1/login",
            json={"email": "test@uni.edu", "password": "pass123"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["api_key"].startswith("yesdb_")
        assert data["api_key"] != api_key  # new key generated

    def test_login_wrong_password(self, client, api_key):
        resp = client.post(
            "/api/v1/login",
            json={"email": "test@uni.edu", "password": "wrong"},
        )
        assert resp.status_code == 401

    def test_login_nonexistent_email(self, client):
        resp = client.post(
            "/api/v1/login",
            json={"email": "ghost@uni.edu", "password": "pass"},
        )
        assert resp.status_code == 401


# ── Auth middleware ──────────────────────────────────────────────


class TestAuth:
    def test_no_auth_header_returns_401(self, client):
        resp = client.get("/api/v1/databases")
        assert resp.status_code in (401, 403)  # depends on FastAPI version

    def test_invalid_api_key_returns_401(self, client):
        resp = client.get(
            "/api/v1/databases",
            headers={"Authorization": "Bearer yesdb_invalid_key"},
        )
        assert resp.status_code == 401

    def test_valid_api_key_works(self, client, auth_headers):
        resp = client.get("/api/v1/databases", headers=auth_headers)
        assert resp.status_code == 200


# ── Create database ─────────────────────────────────────────────


class TestCreateDatabase:
    def test_create_database(self, client, auth_headers):
        resp = client.post(
            "/api/v1/databases",
            json={"name": "myproject"},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "myproject"
        assert "created" in data["message"].lower()
        assert isinstance(data["logs"], list)
        assert len(data["logs"]) > 0  # Engine should log something on DB creation

    def test_create_duplicate_database(self, client, auth_headers):
        client.post(
            "/api/v1/databases",
            json={"name": "dupdb"},
            headers=auth_headers,
        )
        resp = client.post(
            "/api/v1/databases",
            json={"name": "dupdb"},
            headers=auth_headers,
        )
        assert resp.status_code == 409

    def test_create_database_invalid_name(self, client, auth_headers):
        resp = client.post(
            "/api/v1/databases",
            json={"name": "bad name!"},
            headers=auth_headers,
        )
        assert resp.status_code == 400


# ── List databases ───────────────────────────────────────────────


class TestListDatabases:
    def test_list_empty(self, client, auth_headers):
        resp = client.get("/api/v1/databases", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["databases"] == []

    def test_list_after_create(self, client, auth_headers, test_db):
        resp = client.get("/api/v1/databases", headers=auth_headers)
        assert resp.status_code == 200
        assert "testdb" in resp.json()["databases"]

    def test_list_multiple(self, client, auth_headers):
        for name in ["alpha", "beta", "gamma"]:
            client.post(
                "/api/v1/databases",
                json={"name": name},
                headers=auth_headers,
            )
        resp = client.get("/api/v1/databases", headers=auth_headers)
        dbs = resp.json()["databases"]
        assert "alpha" in dbs
        assert "beta" in dbs
        assert "gamma" in dbs


# ── Execute SQL ──────────────────────────────────────────────────


class TestExecuteSQL:
    def test_create_table(self, client, auth_headers, test_db):
        resp = client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "CREATE TABLE users (id INTEGER, name TEXT, email TEXT)"},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["rows"] == []
        assert isinstance(data["logs"], list)

    def test_insert_and_select(self, client, auth_headers, test_db):
        # Create table
        client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "CREATE TABLE users (id INTEGER, name TEXT)"},
            headers=auth_headers,
        )
        # Insert
        resp = client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "INSERT INTO users VALUES (1, 'Alice')"},
            headers=auth_headers,
        )
        assert resp.status_code == 200

        # Select
        resp = client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "SELECT * FROM users"},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 1
        assert data["rows"][0] == [1, "Alice"]

    def test_execute_invalid_sql(self, client, auth_headers, test_db):
        resp = client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "INVALID SQL GIBBERISH"},
            headers=auth_headers,
        )
        assert resp.status_code == 400

    def test_execute_nonexistent_db(self, client, auth_headers):
        resp = client.post(
            "/api/v1/databases/nope/execute",
            json={"sql": "SELECT 1"},
            headers=auth_headers,
        )
        assert resp.status_code == 404

    def test_execute_returns_logs(self, client, auth_headers, test_db):
        """Every response should include engine logs."""
        resp = client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "CREATE TABLE logged (id INTEGER)"},
            headers=auth_headers,
        )
        data = resp.json()
        assert len(data["logs"]) > 0
        # Logs should have the expected structure
        log = data["logs"][0]
        assert "level" in log
        assert "component" in log
        assert "message" in log
        assert "timestamp" in log

    def test_multiple_inserts_and_select_all(self, client, auth_headers, test_db):
        client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "CREATE TABLE items (id INTEGER, title TEXT)"},
            headers=auth_headers,
        )
        for i in range(3):
            client.post(
                f"/api/v1/databases/{test_db}/execute",
                json={"sql": f"INSERT INTO items VALUES ({i}, 'Item {i}')"},
                headers=auth_headers,
            )
        resp = client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "SELECT * FROM items"},
            headers=auth_headers,
        )
        data = resp.json()
        assert data["row_count"] == 3


# ── Push schema ──────────────────────────────────────────────────


class TestPushSchema:
    def test_push_creates_tables(self, client, auth_headers, test_db):
        resp = client.post(
            f"/api/v1/databases/{test_db}/push",
            json={
                "statements": [
                    "CREATE TABLE users (id INTEGER, name TEXT)",
                    "CREATE TABLE posts (id INTEGER, title TEXT, user_id INTEGER)",
                ]
            },
            headers=auth_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["executed"] == 2
        assert len(data["logs"]) > 0
        assert data["results"] == [
            {"success": True, "message": "created"},
            {"success": True, "message": "created"},
        ]

    def test_push_with_error(self, client, auth_headers, test_db):
        """If one statement fails, it should be reported but others still run."""
        resp = client.post(
            f"/api/v1/databases/{test_db}/push",
            json={
                "statements": [
                    "CREATE TABLE good (id INTEGER)",
                    "INVALID SQL",
                    "CREATE TABLE also_good (id INTEGER)",
                ]
            },
            headers=auth_headers,
        )
        data = resp.json()
        assert data["executed"] == 2  # 2 succeeded
        assert [r["success"] for r in data["results"]] == [True, False, True]
        assert data["results"][1]["message"]  # some non-empty failure reason

    def test_push_reports_already_exists_clearly(self, client, auth_headers, test_db):
        """Re-pushing an existing table must report a clear reason, not a
        generic sanitized message."""
        client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "CREATE TABLE users (id INTEGER)"},
            headers=auth_headers,
        )
        resp = client.post(
            f"/api/v1/databases/{test_db}/push",
            json={"statements": ["CREATE TABLE users (id INTEGER)"]},
            headers=auth_headers,
        )
        data = resp.json()
        assert data["executed"] == 0
        assert data["results"] == [
            {"success": False, "message": "Table 'users' already exists"}
        ]

    def test_push_preserves_logs_for_failed_statements(self, client, auth_headers, test_db):
        """Logs captured before a statement fails (e.g. the engine's own
        ERROR log entry) must still reach the response, not be dropped."""
        client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "CREATE TABLE users (id INTEGER)"},
            headers=auth_headers,
        )
        resp = client.post(
            f"/api/v1/databases/{test_db}/push",
            json={"statements": ["CREATE TABLE users (id INTEGER)"]},
            headers=auth_headers,
        )
        data = resp.json()
        assert len(data["logs"]) > 0
        assert any("already exists" in log["message"] for log in data["logs"])


# ── List tables ──────────────────────────────────────────────────


class TestListTables:
    def test_list_empty(self, client, auth_headers, test_db):
        resp = client.get(
            f"/api/v1/databases/{test_db}/tables",
            headers=auth_headers,
        )
        assert resp.status_code == 200
        assert resp.json()["tables"] == []

    def test_list_after_create(self, client, auth_headers, test_db):
        client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "CREATE TABLE users (id INTEGER, name TEXT)"},
            headers=auth_headers,
        )
        resp = client.get(
            f"/api/v1/databases/{test_db}/tables",
            headers=auth_headers,
        )
        assert "users" in resp.json()["tables"]


# ── Concurrency ──────────────────────────────────────────────────


class TestConcurrency:
    def test_concurrent_inserts_do_not_lose_writes(self, client, auth_headers, test_db):
        import threading

        client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "CREATE TABLE counters (id INTEGER, label TEXT)"},
            headers=auth_headers,
        )

        def _insert(i):
            client.post(
                f"/api/v1/databases/{test_db}/execute",
                json={"sql": f"INSERT INTO counters VALUES ({i}, 'row{i}')"},
                headers=auth_headers,
            )

        threads = [threading.Thread(target=_insert, args=(i,)) for i in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        resp = client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "SELECT * FROM counters"},
            headers=auth_headers,
        )
        assert resp.json()["row_count"] == 10


# ── Query timeout ────────────────────────────────────────────────


class TestQueryTimeout:
    def test_slow_query_returns_504(self, client, auth_headers, test_db, monkeypatch):
        import time
        from server.config import settings
        from chidb.api import YesDB

        monkeypatch.setattr(settings, "QUERY_TIMEOUT_SECONDS", 0.05)

        original_execute = YesDB.execute

        def slow_execute(self, sql):
            if sql == "SELECT SLOW":
                time.sleep(0.3)
                return []
            return original_execute(self, sql)

        monkeypatch.setattr(YesDB, "execute", slow_execute)

        resp = client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "SELECT SLOW"},
            headers=auth_headers,
        )
        assert resp.status_code == 504

    def test_fast_query_is_unaffected(self, client, auth_headers, test_db, monkeypatch):
        from server.config import settings

        monkeypatch.setattr(settings, "QUERY_TIMEOUT_SECONDS", 0.05)

        resp = client.post(
            f"/api/v1/databases/{test_db}/execute",
            json={"sql": "CREATE TABLE quick (id INTEGER)"},
            headers=auth_headers,
        )
        assert resp.status_code == 200


# ── Data isolation ───────────────────────────────────────────────


class TestDataIsolation:
    def test_separate_users_see_own_databases(self, client):
        """Two users should not see each other's databases."""
        # User A
        r1 = client.post(
            "/api/v1/signup",
            json={"email": "a@uni.edu", "password": "pass"},
        )
        key_a = r1.json()["api_key"]
        headers_a = {"Authorization": f"Bearer {key_a}"}

        # User B
        r2 = client.post(
            "/api/v1/signup",
            json={"email": "b@uni.edu", "password": "pass"},
        )
        key_b = r2.json()["api_key"]
        headers_b = {"Authorization": f"Bearer {key_b}"}

        # User A creates a database
        client.post(
            "/api/v1/databases",
            json={"name": "secret"},
            headers=headers_a,
        )

        # User B should not see it
        resp = client.get("/api/v1/databases", headers=headers_b)
        assert "secret" not in resp.json()["databases"]

        # User A should see it
        resp = client.get("/api/v1/databases", headers=headers_a)
        assert "secret" in resp.json()["databases"]

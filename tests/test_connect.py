"""Tests for the dual-mode connect() function and package exports."""

import os
import pytest


# ── connect() ────────────────────────────────────────────────────


class TestConnect:
    def test_local_mode_with_filename(self, tmp_path):
        """connect(filename) returns a YesDB instance (existing behavior)."""
        from chidb.api import connect, YesDB

        db_path = str(tmp_path / "local.db")
        db = connect(db_path)
        assert isinstance(db, YesDB)
        db.close()

    def test_local_mode_works_end_to_end(self, tmp_path):
        """Local connect should work exactly as before."""
        from chidb.api import connect

        db = connect(str(tmp_path / "test.db"))
        db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        db.execute("INSERT INTO t VALUES (1, 'Alice')")
        rows = db.execute("SELECT * FROM t")
        assert len(rows) == 1
        db.close()

    def test_cloud_mode_with_db_name(self, monkeypatch, tmp_path):
        """connect(db_name=...) returns a CloudConnection."""
        from chidb.api import connect
        from chidb.client import CloudConnection

        conn = connect(
            db_name="myproject",
            api_key="yesdb_testkey",
            server_url="https://example.com",
        )
        assert isinstance(conn, CloudConnection)
        assert conn.db_name == "myproject"
        conn.close()

    def test_both_filename_and_db_name_raises(self):
        """Cannot specify both local and cloud."""
        from chidb.api import connect

        with pytest.raises(ValueError, match="not both"):
            connect(filename="test.db", db_name="myproject")

    def test_neither_filename_nor_db_name_raises(self):
        """Must specify at least one."""
        from chidb.api import connect

        with pytest.raises(ValueError, match="Must specify"):
            connect()

    def test_positional_arg_is_filename(self, tmp_path):
        """First positional arg should still be filename for backwards compat."""
        from chidb.api import connect, YesDB

        db_path = str(tmp_path / "positional.db")
        db = connect(db_path)
        assert isinstance(db, YesDB)
        db.close()


# ── Package exports ──────────────────────────────────────────────


class TestExports:
    def test_yesdb_class(self):
        from chidb import YesDB
        assert YesDB is not None

    def test_connect_function(self):
        from chidb import connect
        assert callable(connect)

    def test_cloud_connection(self):
        from chidb import CloudConnection
        assert CloudConnection is not None

    def test_execute_result(self):
        from chidb import ExecuteResult
        assert ExecuteResult is not None

    def test_schema_types(self):
        from chidb import Table, Column, Integer, Text, Real, Float, Blob
        assert Integer == "INTEGER"
        assert Text == "TEXT"
        assert Real == "REAL"
        assert Float == "REAL"
        assert Blob == "BLOB"
        assert Table is not None
        assert Column is not None

    def test_security_errors(self):
        from chidb import SecurityError, PathTraversalError, ResourceLimitError
        assert SecurityError is not None
        assert PathTraversalError is not None
        assert ResourceLimitError is not None

    def test_version(self):
        import chidb
        assert chidb.__version__ == "0.1.2"

    def test_all_list(self):
        import chidb
        expected = {
            'YesDB', 'connect', 'CloudConnection', 'ExecuteResult',
            'Table', 'Column', 'Integer', 'Text', 'Real', 'Float', 'Blob',
            'SecurityError', 'PathTraversalError', 'ResourceLimitError',
        }
        assert set(chidb.__all__) == expected

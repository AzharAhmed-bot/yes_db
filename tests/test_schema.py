"""Tests for chidb/schema.py — declarative schema DSL."""

import pytest
from chidb.schema import Column, Table, Integer, Text, Float, Real, Blob, collect_tables


# ── Type aliases ─────────────────────────────────────────────────


class TestTypeAliases:
    def test_integer(self):
        assert Integer == "INTEGER"

    def test_text(self):
        assert Text == "TEXT"

    def test_real(self):
        assert Real == "REAL"

    def test_float_alias(self):
        assert Float == "REAL"  # Float is an alias for REAL

    def test_blob(self):
        assert Blob == "BLOB"


# ── Column ───────────────────────────────────────────────────────


class TestColumn:
    def test_basic_column(self):
        col = Column("name", Text)
        assert col.name == "name"
        assert col.type_ == "TEXT"
        assert col.primary_key is False

    def test_primary_key_column(self):
        col = Column("id", Integer, primary_key=True)
        assert col.primary_key is True

    def test_strips_whitespace_from_name(self):
        col = Column("  name  ", Text)
        assert col.name == "name"

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="empty"):
            Column("", Text)

    def test_whitespace_only_name_raises(self):
        with pytest.raises(ValueError, match="empty"):
            Column("   ", Text)

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Invalid column type"):
            Column("name", "VARCHAR")

    def test_float_string_is_invalid(self):
        """Raw 'FLOAT' string is not valid — use Float alias (which maps to REAL)."""
        with pytest.raises(ValueError, match="Invalid column type"):
            Column("val", "FLOAT")

    def test_repr(self):
        col = Column("id", Integer, primary_key=True)
        r = repr(col)
        assert "id" in r
        assert "INTEGER" in r
        assert "primary_key=True" in r

    def test_repr_no_pk(self):
        col = Column("name", Text)
        r = repr(col)
        assert "primary_key" not in r


# ── Table ────────────────────────────────────────────────────────


class TestTable:
    def test_basic_table(self):
        t = Table("users", [Column("id", Integer), Column("name", Text)])
        assert t.name == "users"
        assert len(t.columns) == 2

    def test_strips_whitespace_from_name(self):
        t = Table("  users  ", [Column("id", Integer)])
        assert t.name == "users"

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="empty"):
            Table("", [Column("id", Integer)])

    def test_no_columns_raises(self):
        with pytest.raises(ValueError, match="at least one column"):
            Table("users", [])

    def test_repr(self):
        t = Table("users", [Column("id", Integer)])
        r = repr(t)
        assert "users" in r
        assert "id" in r


# ── to_sql ───────────────────────────────────────────────────────


class TestToSql:
    def test_single_column(self):
        t = Table("users", [Column("id", Integer)])
        assert t.to_sql() == "CREATE TABLE users (id INTEGER)"

    def test_multiple_columns(self):
        t = Table(
            "users",
            [
                Column("id", Integer, primary_key=True),
                Column("name", Text),
                Column("email", Text),
            ],
        )
        sql = t.to_sql()
        assert sql == "CREATE TABLE users (id INTEGER, name TEXT, email TEXT)"

    def test_all_types(self):
        t = Table(
            "mixed",
            [
                Column("a", Integer),
                Column("b", Text),
                Column("c", Real),
                Column("d", Blob),
            ],
        )
        sql = t.to_sql()
        assert sql == "CREATE TABLE mixed (a INTEGER, b TEXT, c REAL, d BLOB)"

    def test_float_alias_generates_real(self):
        t = Table("f", [Column("val", Float)])
        assert t.to_sql() == "CREATE TABLE f (val REAL)"

    def test_generated_sql_is_valid(self):
        """The generated SQL should actually work with yesdb."""
        import os
        import tempfile
        from chidb.api import YesDB

        t = Table(
            "students",
            [
                Column("id", Integer, primary_key=True),
                Column("name", Text),
                Column("grade", Float),
            ],
        )
        sql = t.to_sql()

        # Actually execute it against a real yesdb instance
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            db = YesDB(db_path)
            db.execute(sql)
            assert db.table_exists("students")
            # Insert and query to prove it works end-to-end
            db.execute("INSERT INTO students VALUES (1, 'Alice', 3.8)")
            rows = db.execute("SELECT * FROM students")
            assert len(rows) == 1
            db.close()
        finally:
            os.unlink(db_path)


# ── collect_tables ───────────────────────────────────────────────


class TestCollectTables:
    def test_collects_tables_from_dict(self):
        """Simulates importing a schema module and collecting its Table objects."""
        fake_module = {
            "__name__": "schema",
            "Integer": Integer,
            "Text": Text,
            "Column": Column,
            "Table": Table,
            "users": Table("users", [Column("id", Integer), Column("name", Text)]),
            "posts": Table("posts", [Column("id", Integer), Column("title", Text)]),
            "some_string": "not a table",
            "some_int": 42,
        }
        tables = collect_tables(fake_module)
        assert len(tables) == 2
        names = {t.name for t in tables}
        assert names == {"users", "posts"}

    def test_empty_module(self):
        tables = collect_tables({})
        assert tables == []

    def test_no_tables_in_module(self):
        tables = collect_tables({"x": 1, "y": "hello"})
        assert tables == []

"""
Declarative schema DSL for YesDB.

Students define their database tables in a schema.py file using this DSL,
then run `yesdb push` to sync the schema to the cloud.

Example usage:
    from chidb.schema import Table, Column, Integer, Text

    users = Table("users", [
        Column("id", Integer, primary_key=True),
        Column("name", Text),
        Column("email", Text),
    ])

    posts = Table("posts", [
        Column("id", Integer, primary_key=True),
        Column("title", Text),
        Column("body", Text),
        Column("user_id", Integer),
    ])
"""

from typing import List, Optional


# ── Type aliases ─────────────────────────────────────────────────

Integer = "INTEGER"
Text = "TEXT"
Real = "REAL"
Blob = "BLOB"

# Alias for convenience — maps to REAL in SQL
Float = "REAL"


# ── Column ───────────────────────────────────────────────────────


class Column:
    """
    Represents a column in a table definition.

    Args:
        name: Column name.
        type_: Column type (use Integer, Text, Float, or Blob).
        primary_key: Whether this column is the primary key.
    """

    VALID_TYPES = {"INTEGER", "TEXT", "REAL", "BLOB"}

    def __init__(self, name: str, type_: str, primary_key: bool = False):
        if not name or not name.strip():
            raise ValueError("Column name cannot be empty")
        if type_ not in self.VALID_TYPES:
            raise ValueError(
                f"Invalid column type '{type_}'. Must be one of: {', '.join(sorted(self.VALID_TYPES))}"
            )
        self.name = name.strip()
        self.type_ = type_
        self.primary_key = primary_key

    def __repr__(self) -> str:
        pk = ", primary_key=True" if self.primary_key else ""
        return f"Column('{self.name}', {self.type_}{pk})"


# ── Table ────────────────────────────────────────────────────────


class Table:
    """
    Represents a table definition.

    Args:
        name: Table name.
        columns: List of Column objects.
    """

    def __init__(self, name: str, columns: List[Column]):
        if not name or not name.strip():
            raise ValueError("Table name cannot be empty")
        if not columns:
            raise ValueError("Table must have at least one column")

        self.name = name.strip()
        self.columns = columns

    def to_sql(self) -> str:
        """
        Generate a CREATE TABLE SQL statement from this table definition.

        Returns:
            SQL string like: CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)
        """
        col_defs = []
        for col in self.columns:
            col_def = f"{col.name} {col.type_}"
            if col.primary_key:
                col_def += " PRIMARY KEY"
            col_defs.append(col_def)
        columns_sql = ", ".join(col_defs)
        return f"CREATE TABLE {self.name} ({columns_sql})"

    def __repr__(self) -> str:
        return f"Table('{self.name}', [{', '.join(repr(c) for c in self.columns)}])"


# ── Helpers ──────────────────────────────────────────────────────


def collect_tables(module_dict: dict) -> List[Table]:
    """
    Collect all Table instances from a module's namespace.
    Used by `yesdb push` to find all tables defined in a schema file.

    Args:
        module_dict: The module's __dict__ (e.g. from importlib).

    Returns:
        List of Table instances found in the module.
    """
    tables = []
    for value in module_dict.values():
        if isinstance(value, Table):
            tables.append(value)
    return tables

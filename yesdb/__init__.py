"""
YesDB - compatibility shim.

This package re-exports everything from the `chidb` internal module so that
`from yesdb import connect` works as documented.
"""

from chidb import (
    YesDB,
    connect,
    CloudConnection,
    ExecuteResult,
    Table,
    Column,
    Integer,
    Text,
    Real,
    Float,
    Blob,
    SecurityError,
    PathTraversalError,
    ResourceLimitError,
)

__version__ = "0.1.5"
__author__ = "Azhar"
__license__ = "MIT"

__all__ = [
    "YesDB",
    "connect",
    "CloudConnection",
    "ExecuteResult",
    "Table",
    "Column",
    "Integer",
    "Text",
    "Real",
    "Float",
    "Blob",
    "SecurityError",
    "PathTraversalError",
    "ResourceLimitError",
]

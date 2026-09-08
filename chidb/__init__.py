"""
YesDB (Beta) - A relational database built from scratch in Python.

An embedded database implementing B-tree storage, SQL parsing, and query
optimization in pure Python. Also available as a cloud Backend-as-a-Service.
"""

from chidb.api import YesDB, connect
from chidb.security import SecurityError, PathTraversalError, ResourceLimitError, QueryError
from chidb.client import CloudConnection, ExecuteResult
from chidb.schema import Table, Column, Integer, Text, Real, Float, Blob

__version__ = '0.6.0'
__author__ = 'Azhar'
__license__ = 'MIT'

__all__ = [
    'YesDB',
    'connect',
    'CloudConnection',
    'ExecuteResult',
    'Table',
    'Column',
    'Integer',
    'Text',
    'Real',
    'Float',
    'Blob',
    'SecurityError',
    'PathTraversalError',
    'ResourceLimitError',
    'QueryError',
]

"""
YesDB - A fully functional relational database built from scratch in Python.

An educational embedded database implementing B-tree storage, SQL parsing,
and query optimization in pure Python.
"""

from chidb.api import YesDB, connect
from chidb.security import SecurityError, PathTraversalError, ResourceLimitError

__version__ = '0.1.1'
__author__ = 'Azhar'
__license__ = 'MIT'

__all__ = [
    'YesDB',
    'connect',
    'SecurityError',
    'PathTraversalError',
    'ResourceLimitError',
]
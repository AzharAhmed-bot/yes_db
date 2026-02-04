"""
Security module for YesDB.
Provides input validation, sanitization, and resource limits.
"""

import os
import re
from pathlib import Path
from typing import Optional


class SecurityError(Exception):
    """Base exception for security-related errors."""
    pass


class PathTraversalError(SecurityError):
    """Raised when path traversal is detected."""
    pass


class ResourceLimitError(SecurityError):
    """Raised when resource limits are exceeded."""
    pass


# Configuration for resource limits
class SecurityConfig:
    """Security configuration with sensible defaults."""

    # File path restrictions
    MAX_FILENAME_LENGTH = 255
    ALLOWED_EXTENSIONS = {'.db', '.cdb', '.sqlite', '.sqlite3'}

    # Resource limits
    MAX_SQL_LENGTH = 1_000_000  # 1MB SQL statement
    MAX_RECORD_SIZE = 10_000_000  # 10MB per record
    MAX_TABLE_COUNT = 1000  # Max tables per database
    MAX_COLUMN_COUNT = 1000  # Max columns per table
    MAX_TABLE_NAME_LENGTH = 128
    MAX_COLUMN_NAME_LENGTH = 128

    # Operation limits
    MAX_SCAN_RECORDS = 10_000_000  # Max records to scan in one query

    # File size limits
    MAX_DATABASE_SIZE = 10 * 1024 * 1024 * 1024  # 10GB


def validate_database_path(filepath: str, allow_create: bool = True) -> str:
    """
    Validate and sanitize a database file path.

    Args:
        filepath: The file path to validate
        allow_create: Whether to allow creating new files

    Returns:
        Absolute, normalized path

    Raises:
        PathTraversalError: If path is unsafe
        SecurityError: If path validation fails
    """
    if not filepath:
        raise SecurityError("Database path cannot be empty")

    if len(filepath) > SecurityConfig.MAX_FILENAME_LENGTH:
        raise SecurityError(f"Filename too long (max {SecurityConfig.MAX_FILENAME_LENGTH})")

    # Detect null bytes (directory traversal attack)
    if '\0' in filepath:
        raise PathTraversalError("Null bytes not allowed in path")

    # Convert to Path object and resolve
    try:
        path = Path(filepath).resolve()
    except (ValueError, OSError) as e:
        raise SecurityError(f"Invalid path: {e}")

    # Check if trying to access system files
    system_paths = {
        Path('/etc'),
        Path('/sys'),
        Path('/proc'),
        Path('/dev'),
        Path('C:\\Windows'),
        Path('C:\\System32'),
    }

    for system_path in system_paths:
        try:
            system_path_resolved = system_path.resolve()
            if path == system_path_resolved or system_path_resolved in path.parents:
                raise PathTraversalError(f"Access to system paths not allowed: {system_path}")
        except (ValueError, OSError):
            # Path doesn't exist or can't be resolved, skip check
            continue

    # Check file extension
    if path.suffix.lower() not in SecurityConfig.ALLOWED_EXTENSIONS:
        # Allow files with no extension (user might want 'mydb')
        if path.suffix and path.suffix != '':
            raise SecurityError(
                f"Invalid file extension. Allowed: {', '.join(SecurityConfig.ALLOWED_EXTENSIONS)}"
            )

    # Check if file exists and is a regular file (not directory, symlink, etc)
    if path.exists():
        if not path.is_file():
            raise SecurityError(f"Path must be a regular file, not directory or special file")

        # Check file size
        size = path.stat().st_size
        if size > SecurityConfig.MAX_DATABASE_SIZE:
            raise ResourceLimitError(
                f"Database file too large: {size} bytes (max {SecurityConfig.MAX_DATABASE_SIZE})"
            )

    # Return absolute path as string
    return str(path.absolute())


def validate_sql_length(sql: str) -> None:
    """
    Validate SQL statement length.

    Args:
        sql: SQL statement to validate

    Raises:
        ResourceLimitError: If SQL is too long
    """
    if len(sql) > SecurityConfig.MAX_SQL_LENGTH:
        raise ResourceLimitError(
            f"SQL statement too long: {len(sql)} chars (max {SecurityConfig.MAX_SQL_LENGTH})"
        )


def validate_table_name(name: str) -> None:
    """
    Validate table name.

    Args:
        name: Table name to validate

    Raises:
        SecurityError: If name is invalid
    """
    if not name:
        raise SecurityError("Table name cannot be empty")

    if len(name) > SecurityConfig.MAX_TABLE_NAME_LENGTH:
        raise SecurityError(
            f"Table name too long: {len(name)} chars (max {SecurityConfig.MAX_TABLE_NAME_LENGTH})"
        )

    # Must start with letter or underscore
    if not re.match(r'^[a-zA-Z_]', name):
        raise SecurityError("Table name must start with letter or underscore")

    # Must contain only alphanumeric and underscore
    if not re.match(r'^[a-zA-Z0-9_]+$', name):
        raise SecurityError("Table name must contain only letters, numbers, and underscores")

    # Reserved names
    reserved = {'system', 'catalog', 'schema', 'sqlite_master', 'sqlite_sequence'}
    if name.lower() in reserved:
        raise SecurityError(f"Table name '{name}' is reserved")


def validate_column_name(name: str) -> None:
    """
    Validate column name.

    Args:
        name: Column name to validate

    Raises:
        SecurityError: If name is invalid
    """
    if not name:
        raise SecurityError("Column name cannot be empty")

    if len(name) > SecurityConfig.MAX_COLUMN_NAME_LENGTH:
        raise SecurityError(
            f"Column name too long: {len(name)} chars (max {SecurityConfig.MAX_COLUMN_NAME_LENGTH})"
        )

    # Must start with letter or underscore
    if not re.match(r'^[a-zA-Z_]', name):
        raise SecurityError("Column name must start with letter or underscore")

    # Must contain only alphanumeric and underscore
    if not re.match(r'^[a-zA-Z0-9_]+$', name):
        raise SecurityError("Column name must contain only letters, numbers, and underscores")


def validate_record_size(size: int) -> None:
    """
    Validate record size.

    Args:
        size: Size in bytes

    Raises:
        ResourceLimitError: If record is too large
    """
    if size > SecurityConfig.MAX_RECORD_SIZE:
        raise ResourceLimitError(
            f"Record too large: {size} bytes (max {SecurityConfig.MAX_RECORD_SIZE})"
        )


def sanitize_error_message(error: Exception, debug_mode: bool = False) -> str:
    """
    Sanitize error messages for production.

    Args:
        error: Exception to sanitize
        debug_mode: Whether to include full details (for development)

    Returns:
        Sanitized error message
    """
    if debug_mode:
        return str(error)

    # Generic messages for production
    error_messages = {
        PathTraversalError: "Invalid file path",
        ResourceLimitError: "Resource limit exceeded",
        SecurityError: "Security validation failed",
        ValueError: "Invalid input",
        IOError: "File operation failed",
        OSError: "System operation failed",
    }

    for error_type, message in error_messages.items():
        if isinstance(error, error_type):
            return message

    # Generic fallback
    return "An error occurred"


def check_table_count(current_count: int) -> None:
    """
    Check if table count limit would be exceeded.

    Args:
        current_count: Current number of tables

    Raises:
        ResourceLimitError: If limit would be exceeded
    """
    if current_count >= SecurityConfig.MAX_TABLE_COUNT:
        raise ResourceLimitError(
            f"Maximum number of tables reached: {SecurityConfig.MAX_TABLE_COUNT}"
        )


def check_column_count(column_count: int) -> None:
    """
    Check if column count limit would be exceeded.

    Args:
        column_count: Number of columns

    Raises:
        ResourceLimitError: If limit would be exceeded
    """
    if column_count > SecurityConfig.MAX_COLUMN_COUNT:
        raise ResourceLimitError(
            f"Too many columns: {column_count} (max {SecurityConfig.MAX_COLUMN_COUNT})"
        )

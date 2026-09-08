"""Tests for chidb/security.py — validation helpers and error sanitization."""

import pytest

from chidb.security import (
    QueryError,
    SecurityError,
    PathTraversalError,
    ResourceLimitError,
    sanitize_error_message,
)


class TestQueryError:
    def test_is_a_value_error(self):
        assert issubclass(QueryError, ValueError)

    def test_carries_its_message(self):
        error = QueryError("Table 'users' already exists")
        assert str(error) == "Table 'users' already exists"


class TestParseErrorIsAQueryError:
    def test_parse_error_is_a_query_error(self):
        from chidb.sql.parser import ParseError
        assert issubclass(ParseError, QueryError)

    def test_parse_error_message_survives_sanitization(self):
        from chidb.sql.parser import ParseError

        error = ParseError("Unexpected token: Token(IDENTIFIER, 'SELCT', 1:1)")
        assert sanitize_error_message(error, debug_mode=False) == str(error)


class TestSanitizeErrorMessage:
    def test_debug_mode_returns_full_message_for_any_error(self):
        error = ValueError("some internal detail")
        assert sanitize_error_message(error, debug_mode=True) == "some internal detail"

    def test_query_error_passes_through_unsanitized(self):
        error = QueryError("Table 'users' already exists")
        assert sanitize_error_message(error, debug_mode=False) == "Table 'users' already exists"

    def test_query_error_passes_through_even_with_generic_text(self):
        error = QueryError("Unknown column: bad_col")
        assert sanitize_error_message(error, debug_mode=False) == "Unknown column: bad_col"

    def test_bare_value_error_is_sanitized_in_production(self):
        error = ValueError("some internal detail")
        assert sanitize_error_message(error, debug_mode=False) == "Invalid input"

    def test_path_traversal_error_is_sanitized(self):
        error = PathTraversalError("/etc/passwd leaked")
        assert sanitize_error_message(error, debug_mode=False) == "Invalid file path"

    def test_resource_limit_error_is_sanitized(self):
        error = ResourceLimitError("too big")
        assert sanitize_error_message(error, debug_mode=False) == "Resource limit exceeded"

    def test_unknown_error_type_gets_generic_fallback(self):
        error = RuntimeError("whatever")
        assert sanitize_error_message(error, debug_mode=False) == "An error occurred"

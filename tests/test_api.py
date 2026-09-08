"""
Tests for YesDB/api.py
"""

import pytest
import tempfile
import os
from chidb.api import YesDB, connect


@pytest.fixture
def temp_db_path():
    """Create a temporary database file path."""
    fd, path = tempfile.mkstemp(suffix='.cdb')
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


class TestYesDBBasics:
    """Test basic YesDB functionality."""
    
    def test_create_database(self, temp_db_path):
        db = YesDB(temp_db_path)
        assert db.filename == temp_db_path
        db.close()
    
    def test_connect_function(self, temp_db_path):
        db = connect(temp_db_path)
        assert isinstance(db, YesDB)
        db.close()
    
    def test_context_manager(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            assert db is not None
        # Database should be closed after context


class TestCreateTable:
    """Test CREATE TABLE functionality."""
    
    def test_create_simple_table(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER, name TEXT)')
            assert db.table_exists('users')
    
    def test_create_table_registers_table(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE test (id INTEGER)')
            assert 'test' in db.get_table_names()
    
    def test_create_duplicate_table_raises(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER)')
            
            with pytest.raises(ValueError):
                db.execute('CREATE TABLE users (id INTEGER)')
    
    def test_create_multiple_tables(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER)')
            db.execute('CREATE TABLE posts (id INTEGER)')
            
            tables = db.get_table_names()
            assert 'users' in tables
            assert 'posts' in tables


class TestInsert:
    """Test INSERT functionality."""
    
    def test_insert_single_row(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER, name TEXT)')
            result = db.execute("INSERT INTO users VALUES (1, 'Alice')")
            
            # INSERT returns empty result
            assert result == []
    
    def test_insert_multiple_rows(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER)')
            db.execute("INSERT INTO users VALUES (1)")
            db.execute("INSERT INTO users VALUES (2)")
            db.execute("INSERT INTO users VALUES (3)")
    
    def test_insert_with_null(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE test (a INTEGER, b TEXT)')
            db.execute("INSERT INTO test VALUES (1, NULL)")
    
    def test_insert_different_types(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE test (id INTEGER, name TEXT, age INTEGER)')
            db.execute("INSERT INTO test VALUES (1, 'Bob', 25)")


class TestSelect:
    """Test SELECT functionality."""
    
    def test_select_from_empty_table(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER)')
            results = db.execute('SELECT * FROM users')
            
            assert results == []
    
    def test_select_after_insert(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER)')
            db.execute("INSERT INTO users VALUES (42)")
            
            results = db.execute('SELECT * FROM users')
            
            assert len(results) > 0
    
    def test_select_multiple_rows(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE test (id INTEGER)')
            db.execute("INSERT INTO test VALUES (1)")
            db.execute("INSERT INTO test VALUES (2)")
            db.execute("INSERT INTO test VALUES (3)")
            
            results = db.execute('SELECT * FROM test')
            
            assert len(results) == 3
    
    def test_select_specific_columns(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)')
            db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")

            results = db.execute('SELECT name FROM users')

            assert results[0][0].get_values() == ['Alice']

    def test_select_where_filters_correctly(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)')
            db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            db.execute("INSERT INTO users VALUES (2, 'Bob', 15)")

            results = db.execute('SELECT * FROM users WHERE age > 20')

            assert len(results) == 1
            assert results[0][0].get_values() == [1, 'Alice', 30]

    def test_select_where_excludes_non_matching_rows(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)')
            db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            db.execute("INSERT INTO users VALUES (2, 'Bob', 15)")

            results = db.execute('SELECT * FROM users WHERE age > 100')

            assert results == []

    def test_select_where_with_specific_columns(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)')
            db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            db.execute("INSERT INTO users VALUES (2, 'Bob', 15)")

            results = db.execute('SELECT name FROM users WHERE age > 20')

            assert len(results) == 1
            assert results[0][0].get_values() == ['Alice']


class TestEndToEnd:
    """Test complete end-to-end workflows."""
    
    def test_complete_workflow(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            # Create table
            db.execute('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)')
            
            # Insert data
            db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
            db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
            
            # Query data
            results = db.execute('SELECT * FROM users')
            
            assert len(results) == 3
    
    def test_multiple_tables(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            # Create multiple tables
            db.execute('CREATE TABLE users (id INTEGER, name TEXT)')
            db.execute('CREATE TABLE posts (id INTEGER, title TEXT)')
            
            # Insert into both
            db.execute("INSERT INTO users VALUES (1, 'Alice')")
            db.execute("INSERT INTO posts VALUES (1, 'First Post')")
            
            # Query both
            user_results = db.execute('SELECT * FROM users')
            post_results = db.execute('SELECT * FROM posts')
            
            assert len(user_results) == 1
            assert len(post_results) == 1
    
    def test_reopen_database(self, temp_db_path):
        # Create and populate database
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE test (id INTEGER)')
            db.execute("INSERT INTO test VALUES (100)")
        
        # Reopen database
        with YesDB(temp_db_path) as db:
            # Table metadata is lost (no system catalog yet)
            # Need to recreate table structure
            # This is a known limitation
            pass


class TestTableOperations:
    """Test table-related operations."""
    
    def test_get_table_names_empty(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            assert db.get_table_names() == []
    
    def test_get_table_names_with_tables(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE t1 (id INTEGER)')
            db.execute('CREATE TABLE t2 (id INTEGER)')
            
            tables = db.get_table_names()
            assert len(tables) == 2
            assert 't1' in tables
            assert 't2' in tables
    
    def test_table_exists(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            assert not db.table_exists('users')
            
            db.execute('CREATE TABLE users (id INTEGER)')
            
            assert db.table_exists('users')
            assert not db.table_exists('posts')


class TestErrorHandling:
    """Test error handling."""
    
    def test_invalid_sql_raises(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            with pytest.raises(Exception):
                db.execute('INVALID SQL STATEMENT')
    
    def test_select_nonexistent_table_raises(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            with pytest.raises(Exception):
                db.execute('SELECT * FROM nonexistent')
    
    def test_insert_nonexistent_table_raises(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            with pytest.raises(Exception):
                db.execute("INSERT INTO nonexistent VALUES (1)")


class TestQueryErrorMessages:
    """User-facing schema/query errors must raise QueryError with a clear,
    unsanitized message — these never leak internals, so hiding them helps
    no one (see chidb.security.sanitize_error_message)."""

    def test_duplicate_table_raises_query_error_with_table_name(self, temp_db_path):
        from chidb import QueryError

        with YesDB(temp_db_path, debug_mode=True) as db:
            db.execute('CREATE TABLE users (id INTEGER)')
            with pytest.raises(QueryError, match="users"):
                db.execute('CREATE TABLE users (id INTEGER)')

    def test_select_nonexistent_table_raises_query_error(self, temp_db_path):
        from chidb import QueryError

        with YesDB(temp_db_path, debug_mode=True) as db:
            with pytest.raises(QueryError, match="ghost"):
                db.execute('SELECT * FROM ghost')

    def test_query_error_message_survives_non_debug_mode(self, temp_db_path):
        """Even with debug_mode=False, QueryError messages must not be
        sanitized away to a generic string."""
        with YesDB(temp_db_path, debug_mode=False) as db:
            db.execute('CREATE TABLE users (id INTEGER)')
            with pytest.raises(ValueError, match="users' already exists"):
                db.execute('CREATE TABLE users (id INTEGER)')

    def test_syntax_error_message_survives_non_debug_mode(self, temp_db_path):
        """A SQL syntax typo must show what's actually wrong, not the
        generic 'An error occurred' fallback for unrecognized error types."""
        with YesDB(temp_db_path, debug_mode=False) as db:
            with pytest.raises(ValueError) as exc_info:
                db.execute('SELCT * FROM users')
            assert str(exc_info.value) != "An error occurred"
            assert "SELCT" in str(exc_info.value)


class TestPersistence:
    """Test data persistence."""
    
    def test_data_persists_across_sessions(self, temp_db_path):
        # Session 1: Create and insert
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER)')
            # Get root page for later
            root_page = db.tables['users']
            db.execute("INSERT INTO users VALUES (42)")
        
        # Session 2: Should be able to access data if we know the root page
        # (In a full implementation with system catalog, this would be automatic)
        with YesDB(temp_db_path) as db:
            # Manually register table for testing
            db.tables['users'] = root_page
            
            results = db.execute('SELECT * FROM users')
            assert len(results) == 1


class TestComplexQueries:
    """Test more complex query scenarios."""
    
    def test_insert_and_select_many_rows(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE numbers (value INTEGER)')
            
            # Insert many rows
            for i in range(20):
                db.execute(f"INSERT INTO numbers VALUES ({i})")
            
            # Select all
            results = db.execute('SELECT * FROM numbers')
            
            assert len(results) == 20
    
    def test_multiple_inserts_different_types(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE mixed (id INTEGER, name TEXT, active INTEGER)')
            
            db.execute("INSERT INTO mixed VALUES (1, 'Alice', 1)")
            db.execute("INSERT INTO mixed VALUES (2, 'Bob', 0)")
            
            results = db.execute('SELECT * FROM mixed')
            assert len(results) == 2


def _row_values(results):
    return [row[0].get_values() for row in results]


class TestAggregates:
    """Test GROUP BY and aggregate functions (COUNT, SUM, AVG, MIN, MAX)."""

    @pytest.fixture
    def products_db(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price INTEGER)')
            db.execute("INSERT INTO products VALUES (NULL, 'fruit', 10)")
            db.execute("INSERT INTO products VALUES (NULL, 'fruit', 20)")
            db.execute("INSERT INTO products VALUES (NULL, 'veg', 5)")
            yield db

    def test_count_star_whole_table(self, products_db):
        results = products_db.execute('SELECT COUNT(*) FROM products')
        assert _row_values(results) == [[3]]

    def test_count_column(self, products_db):
        results = products_db.execute('SELECT COUNT(price) FROM products')
        assert _row_values(results) == [[3]]

    def test_sum(self, products_db):
        results = products_db.execute('SELECT SUM(price) FROM products')
        assert _row_values(results) == [[35]]

    def test_avg(self, products_db):
        results = products_db.execute('SELECT AVG(price) FROM products')
        assert _row_values(results)[0] == pytest.approx([35 / 3])

    def test_min_and_max(self, products_db):
        results = products_db.execute('SELECT MIN(price), MAX(price) FROM products')
        assert _row_values(results) == [[5, 20]]

    def test_group_by_single_column(self, products_db):
        results = products_db.execute(
            'SELECT category, COUNT(*) FROM products GROUP BY category ORDER BY category'
        )
        assert _row_values(results) == [['fruit', 2], ['veg', 1]]

    def test_group_by_with_multiple_aggregates(self, products_db):
        results = products_db.execute(
            'SELECT category, SUM(price), AVG(price) FROM products '
            'GROUP BY category ORDER BY category'
        )
        assert _row_values(results) == [
            ['fruit', 30, 15.0],
            ['veg', 5, 5.0],
        ]

    def test_group_by_with_where(self, products_db):
        results = products_db.execute(
            "SELECT category, SUM(price) FROM products WHERE price > 5 GROUP BY category"
        )
        assert _row_values(results) == [['fruit', 30]]

    def test_group_by_with_limit(self, products_db):
        results = products_db.execute(
            'SELECT category, COUNT(*) FROM products GROUP BY category ORDER BY category LIMIT 1'
        )
        assert _row_values(results) == [['fruit', 2]]

    def test_count_on_empty_table(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE empty_table (id INTEGER)')
            results = db.execute('SELECT COUNT(*) FROM empty_table')
            assert _row_values(results) == [[0]]

    def test_sum_of_empty_table_is_null(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE empty_table (id INTEGER)')
            results = db.execute('SELECT SUM(id) FROM empty_table')
            assert _row_values(results) == [[None]]

    def test_ungrouped_plain_column_raises(self, products_db):
        with pytest.raises(ValueError):
            products_db.execute('SELECT category, price FROM products GROUP BY category')
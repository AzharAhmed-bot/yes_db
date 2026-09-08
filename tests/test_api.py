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


class TestJoins:
    """Test INNER and LEFT JOIN support."""

    @pytest.fixture
    def shop_db(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
            db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)')
            db.execute("INSERT INTO users VALUES (1, 'Alice')")
            db.execute("INSERT INTO users VALUES (2, 'Bob')")
            db.execute("INSERT INTO users VALUES (3, 'Carol')")
            db.execute("INSERT INTO orders VALUES (1, 1, 10)")
            db.execute("INSERT INTO orders VALUES (2, 1, 20)")
            db.execute("INSERT INTO orders VALUES (3, 2, 5)")
            yield db

    def test_inner_join_basic(self, shop_db):
        results = shop_db.execute(
            'SELECT users.name, orders.total FROM orders JOIN users ON orders.user_id = users.id '
            'ORDER BY orders.total DESC'
        )
        assert _row_values(results) == [['Alice', 20], ['Alice', 10], ['Bob', 5]]

    def test_inner_join_excludes_unmatched(self, shop_db):
        # Carol has no orders and must not appear in an INNER JOIN.
        results = shop_db.execute(
            'SELECT users.name FROM users JOIN orders ON users.id = orders.user_id'
        )
        names = {row[0] for row in _row_values(results)}
        assert 'Carol' not in names

    def test_left_join_includes_unmatched_with_nulls(self, shop_db):
        results = shop_db.execute(
            'SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id '
            'ORDER BY users.name'
        )
        assert _row_values(results) == [
            ['Alice', 10], ['Alice', 20], ['Bob', 5], ['Carol', None],
        ]

    def test_join_with_where(self, shop_db):
        results = shop_db.execute(
            'SELECT users.name FROM orders JOIN users ON orders.user_id = users.id '
            'WHERE orders.total > 8'
        )
        assert _row_values(results) == [['Alice'], ['Alice']]

    def test_join_with_unqualified_unambiguous_column(self, shop_db):
        results = shop_db.execute(
            "SELECT name, total FROM orders JOIN users ON orders.user_id = users.id WHERE total > 8"
        )
        assert _row_values(results) == [['Alice', 10], ['Alice', 20]]

    def test_join_ambiguous_column_raises(self, shop_db):
        with pytest.raises(ValueError, match="Ambiguous"):
            shop_db.execute('SELECT id FROM orders JOIN users ON orders.user_id = users.id')

    def test_join_select_star(self, shop_db):
        results = shop_db.execute(
            'SELECT * FROM orders JOIN users ON orders.user_id = users.id WHERE orders.id = 1'
        )
        assert _row_values(results) == [[1, 1, 10, 1, 'Alice']]

    def test_join_missing_table_raises(self, shop_db):
        with pytest.raises(ValueError):
            shop_db.execute('SELECT * FROM orders JOIN ghosts ON orders.id = ghosts.id')

    def test_join_with_limit_and_offset(self, shop_db):
        results = shop_db.execute(
            'SELECT orders.total FROM orders JOIN users ON orders.user_id = users.id '
            'ORDER BY orders.total ASC LIMIT 1 OFFSET 1'
        )
        assert _row_values(results) == [[10]]

    def test_multi_way_join(self, shop_db):
        db = shop_db
        db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER, sku TEXT)')
        db.execute("INSERT INTO items VALUES (1, 1, 'widget')")
        db.execute("INSERT INTO items VALUES (2, 2, 'gadget')")

        results = db.execute(
            'SELECT users.name, orders.id, items.sku FROM orders '
            'JOIN users ON orders.user_id = users.id '
            'JOIN items ON items.order_id = orders.id '
            'ORDER BY orders.id'
        )
        assert _row_values(results) == [
            ['Alice', 1, 'widget'],
            ['Alice', 2, 'gadget'],
        ]

    def test_join_with_aggregate_raises_clear_error(self, shop_db):
        with pytest.raises(ValueError, match="JOIN"):
            shop_db.execute(
                'SELECT COUNT(*) FROM orders JOIN users ON orders.user_id = users.id GROUP BY users.name'
            )


class TestSecondaryIndexes:
    """Test CREATE INDEX / DROP INDEX and index-accelerated equality lookups."""

    @pytest.fixture
    def users_db(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
            for i in range(1, 6):
                db.execute(f"INSERT INTO users VALUES ({i}, 'user{i}', {20 + i})")
            yield db

    def test_create_index_registers_name(self, users_db):
        users_db.execute('CREATE INDEX idx_name ON users (name)')
        assert users_db.get_index_names() == ['idx_name']

    def test_create_duplicate_index_raises(self, users_db):
        users_db.execute('CREATE INDEX idx_name ON users (name)')
        with pytest.raises(ValueError, match="already exists"):
            users_db.execute('CREATE INDEX idx_name ON users (age)')

    def test_create_index_on_missing_table_raises(self, users_db):
        with pytest.raises(ValueError):
            users_db.execute('CREATE INDEX idx_x ON ghosts (name)')

    def test_create_index_on_missing_column_raises(self, users_db):
        with pytest.raises(ValueError):
            users_db.execute('CREATE INDEX idx_x ON users (nonexistent)')

    def test_indexed_equality_lookup_returns_correct_row(self, users_db):
        users_db.execute('CREATE INDEX idx_name ON users (name)')
        results = users_db.execute("SELECT * FROM users WHERE name = 'user3'")
        assert _row_values(results) == [[3, 'user3', 23]]

    def test_indexed_lookup_does_not_full_scan(self, users_db, monkeypatch):
        from chidb.btree import BTree

        users_db.execute('CREATE INDEX idx_name ON users (name)')

        def guarded_scan(self):
            raise AssertionError("full table scan happened even though an index should have been used")

        monkeypatch.setattr(BTree, 'scan', guarded_scan)
        results = users_db.execute("SELECT * FROM users WHERE name = 'user3'")
        assert _row_values(results) == [[3, 'user3', 23]]

    def test_non_indexed_where_still_works(self, users_db):
        users_db.execute('CREATE INDEX idx_name ON users (name)')
        results = users_db.execute('SELECT * FROM users WHERE age > 23')
        assert _row_values(results) == [[4, 'user4', 24], [5, 'user5', 25]]

    def test_index_reflects_update(self, users_db):
        users_db.execute('CREATE INDEX idx_name ON users (name)')
        users_db.execute("UPDATE users SET name = 'renamed' WHERE id = 3")
        assert _row_values(users_db.execute("SELECT * FROM users WHERE name = 'user3'")) == []
        assert _row_values(users_db.execute("SELECT * FROM users WHERE name = 'renamed'")) == [
            [3, 'renamed', 23]
        ]

    def test_index_reflects_delete(self, users_db):
        users_db.execute('CREATE INDEX idx_name ON users (name)')
        users_db.execute('DELETE FROM users WHERE id = 1')
        assert _row_values(users_db.execute("SELECT * FROM users WHERE name = 'user1'")) == []

    def test_index_reflects_insert(self, users_db):
        users_db.execute('CREATE INDEX idx_name ON users (name)')
        users_db.execute("INSERT INTO users VALUES (6, 'user6', 26)")
        assert _row_values(users_db.execute("SELECT * FROM users WHERE name = 'user6'")) == [
            [6, 'user6', 26]
        ]

    def test_index_survives_reopen(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
            db.execute("INSERT INTO t VALUES (1, 'a')")
            db.execute('CREATE INDEX idx_val ON t (val)')

        with YesDB(temp_db_path) as db:
            assert db.get_index_names() == ['idx_val']
            assert _row_values(db.execute("SELECT * FROM t WHERE val = 'a'")) == [[1, 'a']]

    def test_drop_index(self, users_db):
        users_db.execute('CREATE INDEX idx_name ON users (name)')
        users_db.execute('DROP INDEX idx_name')
        assert users_db.get_index_names() == []

    def test_drop_nonexistent_index_raises(self, users_db):
        with pytest.raises(ValueError, match="does not exist"):
            users_db.execute('DROP INDEX ghost_index')

    def test_drop_table_cascades_index_drop(self, users_db):
        users_db.execute('CREATE INDEX idx_name ON users (name)')
        users_db.execute('DROP TABLE users')
        assert users_db.get_index_names() == []


class TestTransactions:
    """Test BEGIN / COMMIT / ROLLBACK."""

    @pytest.fixture
    def t_db(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
            db.execute("INSERT INTO t VALUES (1, 'a')")
            yield db

    def test_rollback_discards_insert(self, t_db):
        t_db.execute('BEGIN')
        t_db.execute("INSERT INTO t VALUES (2, 'b')")
        t_db.execute('ROLLBACK')
        assert _row_values(t_db.execute('SELECT * FROM t')) == [[1, 'a']]

    def test_commit_persists_insert(self, t_db):
        t_db.execute('BEGIN')
        t_db.execute("INSERT INTO t VALUES (2, 'b')")
        t_db.execute('COMMIT')
        assert _row_values(t_db.execute('SELECT * FROM t')) == [[1, 'a'], [2, 'b']]

    def test_rollback_discards_update(self, t_db):
        t_db.execute('BEGIN')
        t_db.execute("UPDATE t SET val = 'zzz' WHERE id = 1")
        t_db.execute('ROLLBACK')
        assert _row_values(t_db.execute('SELECT * FROM t')) == [[1, 'a']]

    def test_rollback_discards_delete(self, t_db):
        t_db.execute('BEGIN')
        t_db.execute('DELETE FROM t WHERE id = 1')
        t_db.execute('ROLLBACK')
        assert _row_values(t_db.execute('SELECT * FROM t')) == [[1, 'a']]

    def test_reads_inside_transaction_see_uncommitted_writes(self, t_db):
        t_db.execute('BEGIN')
        t_db.execute("INSERT INTO t VALUES (2, 'b')")
        assert _row_values(t_db.execute('SELECT * FROM t')) == [[1, 'a'], [2, 'b']]
        t_db.execute('ROLLBACK')

    def test_ddl_rejected_inside_transaction(self, t_db):
        t_db.execute('BEGIN')
        with pytest.raises(ValueError, match="DDL"):
            t_db.execute('CREATE TABLE other (id INTEGER)')
        t_db.execute('ROLLBACK')

    def test_nested_begin_rejected(self, t_db):
        t_db.execute('BEGIN')
        with pytest.raises(ValueError, match="already in progress"):
            t_db.execute('BEGIN')
        t_db.execute('ROLLBACK')

    def test_commit_without_begin_rejected(self, t_db):
        with pytest.raises(ValueError, match="No transaction"):
            t_db.execute('COMMIT')

    def test_rollback_without_begin_rejected(self, t_db):
        with pytest.raises(ValueError, match="No transaction"):
            t_db.execute('ROLLBACK')

    def test_committed_data_survives_reopen(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
            db.execute('BEGIN')
            db.execute("INSERT INTO t VALUES (1, 'a')")
            db.execute('COMMIT')

        with YesDB(temp_db_path) as db:
            assert _row_values(db.execute('SELECT * FROM t')) == [[1, 'a']]

    def test_rolled_back_data_absent_after_reopen(self, temp_db_path):
        with YesDB(temp_db_path) as db:
            db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
            db.execute("INSERT INTO t VALUES (1, 'a')")
            db.execute('BEGIN')
            db.execute("INSERT INTO t VALUES (2, 'b')")
            db.execute('ROLLBACK')

        with YesDB(temp_db_path) as db:
            assert _row_values(db.execute('SELECT * FROM t')) == [[1, 'a']]

    def test_close_with_active_transaction_rolls_back(self, temp_db_path):
        db = YesDB(temp_db_path)
        db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
        db.execute("INSERT INTO t VALUES (1, 'a')")
        db.execute('BEGIN')
        db.execute("INSERT INTO t VALUES (99, 'ghost')")
        db.close()

        with YesDB(temp_db_path) as db2:
            assert _row_values(db2.execute('SELECT * FROM t')) == [[1, 'a']]
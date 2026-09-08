"""
Tests for chidb/sql/lexer.py and chidb/sql/parser.py
"""

import pytest
from chidb.sql.lexer import Lexer, Token, TokenType, tokenize
from chidb.sql.parser import (
    Parser, parse, ParseError,
    SelectStatement, InsertStatement, CreateTableStatement, DropTableStatement,
    BinaryOp, Literal, Identifier, ColumnDef, AggregateCall, JoinClause,
    CreateIndexStatement, DropIndexStatement, TransactionStatement
)


class TestTokenCreation:
    """Test Token class."""
    
    def test_create_token(self):
        token = Token(TokenType.SELECT, 'SELECT', 1, 1)
        assert token.type == TokenType.SELECT
        assert token.value == 'SELECT'
        assert token.line == 1
        assert token.column == 1
    
    def test_token_repr(self):
        token = Token(TokenType.INTEGER_LITERAL, 42, 1, 5)
        assert 'INTEGER_LITERAL' in repr(token)
        assert '42' in repr(token)


class TestLexerBasics:
    """Test basic lexer functionality."""
    
    def test_empty_source(self):
        lexer = Lexer('')
        token = lexer.get_next_token()
        assert token.type == TokenType.EOF
    
    def test_whitespace_only(self):
        lexer = Lexer('   \n  \t  ')
        token = lexer.get_next_token()
        assert token.type == TokenType.EOF
    
    def test_single_keyword(self):
        lexer = Lexer('SELECT')
        token = lexer.get_next_token()
        assert token.type == TokenType.SELECT
        assert token.value == 'SELECT'


class TestKeywords:
    """Test keyword recognition."""
    
    def test_select_keyword(self):
        tokens = tokenize('SELECT')
        assert tokens[0].type == TokenType.SELECT
    
    def test_from_keyword(self):
        tokens = tokenize('FROM')
        assert tokens[0].type == TokenType.FROM
    
    def test_where_keyword(self):
        tokens = tokenize('WHERE')
        assert tokens[0].type == TokenType.WHERE
    
    def test_insert_keyword(self):
        tokens = tokenize('INSERT')
        assert tokens[0].type == TokenType.INSERT
    
    def test_create_keyword(self):
        tokens = tokenize('CREATE')
        assert tokens[0].type == TokenType.CREATE
    
    def test_table_keyword(self):
        tokens = tokenize('TABLE')
        assert tokens[0].type == TokenType.TABLE
    
    def test_keywords_case_insensitive(self):
        tokens = tokenize('select SELECT SeLeCt')
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.SELECT
        assert tokens[2].type == TokenType.SELECT

    def test_group_keyword(self):
        tokens = tokenize('GROUP')
        assert tokens[0].type == TokenType.GROUP

    def test_group_by_sequence(self):
        tokens = tokenize('GROUP BY')
        assert tokens[0].type == TokenType.GROUP
        assert tokens[1].type == TokenType.BY

    def test_aggregate_function_keywords(self):
        tokens = tokenize('COUNT SUM AVG MIN MAX')
        assert [t.type for t in tokens[:5]] == [
            TokenType.COUNT,
            TokenType.SUM,
            TokenType.AVG,
            TokenType.MIN,
            TokenType.MAX,
        ]


class TestLiterals:
    """Test literal values."""
    
    def test_integer_literal(self):
        tokens = tokenize('42')
        assert tokens[0].type == TokenType.INTEGER_LITERAL
        assert tokens[0].value == 42
    
    def test_float_literal(self):
        tokens = tokenize('3.14')
        assert tokens[0].type == TokenType.FLOAT_LITERAL
        assert abs(tokens[0].value - 3.14) < 0.01
    
    def test_string_literal(self):
        tokens = tokenize("'hello world'")
        assert tokens[0].type == TokenType.STRING_LITERAL
        assert tokens[0].value == 'hello world'
    
    def test_empty_string(self):
        tokens = tokenize("''")
        assert tokens[0].type == TokenType.STRING_LITERAL
        assert tokens[0].value == ''
    
    def test_string_with_escaped_quote(self):
        tokens = tokenize("'it\\'s'")
        assert tokens[0].type == TokenType.STRING_LITERAL
        assert tokens[0].value == "it's"
    
    def test_multiple_integers(self):
        tokens = tokenize('1 2 3')
        assert tokens[0].value == 1
        assert tokens[1].value == 2
        assert tokens[2].value == 3


class TestIdentifiers:
    """Test identifier recognition."""
    
    def test_simple_identifier(self):
        tokens = tokenize('users')
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[0].value == 'users'
    
    def test_identifier_with_underscore(self):
        tokens = tokenize('user_name')
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[0].value == 'user_name'
    
    def test_identifier_with_numbers(self):
        tokens = tokenize('table123')
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[0].value == 'table123'
    
    def test_identifier_starting_with_underscore(self):
        tokens = tokenize('_private')
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[0].value == '_private'


class TestOperators:
    """Test operator recognition."""
    
    def test_equals(self):
        tokens = tokenize('=')
        assert tokens[0].type == TokenType.EQUALS
    
    def test_not_equals(self):
        tokens = tokenize('!=')
        assert tokens[0].type == TokenType.NOT_EQUALS
    
    def test_less_than(self):
        tokens = tokenize('<')
        assert tokens[0].type == TokenType.LESS_THAN
    
    def test_less_equal(self):
        tokens = tokenize('<=')
        assert tokens[0].type == TokenType.LESS_EQUAL
    
    def test_greater_than(self):
        tokens = tokenize('>')
        assert tokens[0].type == TokenType.GREATER_THAN
    
    def test_greater_equal(self):
        tokens = tokenize('>=')
        assert tokens[0].type == TokenType.GREATER_EQUAL
    
    def test_arithmetic_operators(self):
        tokens = tokenize('+ - * /')
        assert tokens[0].type == TokenType.PLUS
        assert tokens[1].type == TokenType.MINUS
        assert tokens[2].type == TokenType.STAR
        assert tokens[3].type == TokenType.SLASH


class TestDelimiters:
    """Test delimiter recognition."""
    
    def test_parentheses(self):
        tokens = tokenize('()')
        assert tokens[0].type == TokenType.LPAREN
        assert tokens[1].type == TokenType.RPAREN
    
    def test_comma(self):
        tokens = tokenize(',')
        assert tokens[0].type == TokenType.COMMA
    
    def test_semicolon(self):
        tokens = tokenize(';')
        assert tokens[0].type == TokenType.SEMICOLON
    
    def test_dot(self):
        tokens = tokenize('.')
        assert tokens[0].type == TokenType.DOT


class TestComments:
    """Test comment handling."""
    
    def test_single_line_comment(self):
        tokens = tokenize('SELECT -- this is a comment\nFROM')
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.FROM
    
    def test_comment_at_end(self):
        tokens = tokenize('SELECT -- comment')
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.EOF


class TestComplexSQL:
    """Test tokenizing complete SQL statements."""
    
    def test_simple_select(self):
        tokens = tokenize('SELECT * FROM users')
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.STAR
        assert tokens[2].type == TokenType.FROM
        assert tokens[3].type == TokenType.IDENTIFIER
        assert tokens[3].value == 'users'
    
    def test_select_with_where(self):
        tokens = tokenize('SELECT name FROM users WHERE id = 5')
        types = [t.type for t in tokens[:-1]]  # Exclude EOF
        assert TokenType.SELECT in types
        assert TokenType.FROM in types
        assert TokenType.WHERE in types
        assert TokenType.EQUALS in types
    
    def test_insert_statement(self):
        tokens = tokenize("INSERT INTO users VALUES (1, 'John')")
        assert tokens[0].type == TokenType.INSERT
        assert tokens[1].type == TokenType.INTO
        assert tokens[2].type == TokenType.IDENTIFIER
        assert tokens[3].type == TokenType.VALUES
        assert tokens[4].type == TokenType.LPAREN
        assert tokens[5].type == TokenType.INTEGER_LITERAL
        assert tokens[6].type == TokenType.COMMA
        assert tokens[7].type == TokenType.STRING_LITERAL
        assert tokens[8].type == TokenType.RPAREN
    
    def test_create_table(self):
        tokens = tokenize('CREATE TABLE users (id INTEGER, name TEXT)')
        assert tokens[0].type == TokenType.CREATE
        assert tokens[1].type == TokenType.TABLE
        assert tokens[2].type == TokenType.IDENTIFIER
        assert tokens[2].value == 'users'
    
    def test_tokenize_with_semicolon(self):
        tokens = tokenize('SELECT * FROM users;')
        assert tokens[-2].type == TokenType.SEMICOLON
        assert tokens[-1].type == TokenType.EOF


class TestLineAndColumn:
    """Test line and column tracking."""
    
    def test_single_line(self):
        tokens = tokenize('SELECT FROM WHERE')
        assert all(t.line == 1 for t in tokens[:-1])
    
    def test_multiple_lines(self):
        sql = '''SELECT
FROM
WHERE'''
        tokens = tokenize(sql)
        assert tokens[0].line == 1  # SELECT
        assert tokens[1].line == 2  # FROM
        assert tokens[2].line == 3  # WHERE
    
    def test_column_tracking(self):
        tokens = tokenize('SELECT FROM')
        assert tokens[0].column == 1  # SELECT starts at column 1
        assert tokens[1].column == 8  # FROM starts at column 8


class TestEdgeCases:
    """Test edge cases."""
    
    def test_operators_without_spaces(self):
        tokens = tokenize('a=b')
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[1].type == TokenType.EQUALS
        assert tokens[2].type == TokenType.IDENTIFIER
    
    def test_multiple_spaces(self):
        tokens = tokenize('SELECT     FROM')
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.FROM
    
    def test_mixed_case_keywords(self):
        tokens = tokenize('SeLeCt FrOm WhErE')
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.FROM
        assert tokens[2].type == TokenType.WHERE


# Parser Tests

class TestParserBasics:
    """Test basic parser functionality."""
    
    def test_parse_simple_select(self):
        ast = parse('SELECT * FROM users')
        assert isinstance(ast, SelectStatement)
        assert ast.columns == ['*']
        assert ast.table == 'users'
        assert ast.where is None
    
    def test_parse_select_specific_columns(self):
        ast = parse('SELECT name, age FROM users')
        assert isinstance(ast, SelectStatement)
        assert ast.columns == ['name', 'age']
        assert ast.table == 'users'
    
    def test_parse_select_with_where(self):
        ast = parse('SELECT * FROM users WHERE id = 5')
        assert isinstance(ast, SelectStatement)
        assert ast.where is not None
        assert isinstance(ast.where, BinaryOp)
        assert ast.where.operator == '='


class TestParserGroupBy:
    """Test GROUP BY clause parsing."""

    def test_parse_group_by_single_column(self):
        ast = parse('SELECT category FROM products GROUP BY category')
        assert ast.group_by == ['category']

    def test_parse_group_by_multiple_columns(self):
        ast = parse('SELECT category, region FROM products GROUP BY category, region')
        assert ast.group_by == ['category', 'region']

    def test_parse_without_group_by_is_none(self):
        ast = parse('SELECT * FROM products')
        assert ast.group_by is None

    def test_parse_group_by_with_where(self):
        ast = parse("SELECT category FROM products WHERE price > 10 GROUP BY category")
        assert ast.where is not None
        assert ast.group_by == ['category']

    def test_parse_group_by_with_order_by(self):
        ast = parse('SELECT category FROM products GROUP BY category ORDER BY category')
        assert ast.group_by == ['category']
        assert ast.order_by == [('category', 'ASC')]


class TestParserAggregates:
    """Test aggregate function parsing in SELECT columns."""

    def test_parse_count_star(self):
        ast = parse('SELECT COUNT(*) FROM products')
        assert ast.columns == [AggregateCall(function='COUNT', column='*')]

    def test_parse_count_column(self):
        ast = parse('SELECT COUNT(price) FROM products')
        assert ast.columns == [AggregateCall(function='COUNT', column='price')]

    def test_parse_sum(self):
        ast = parse('SELECT SUM(price) FROM products')
        assert ast.columns == [AggregateCall(function='SUM', column='price')]

    def test_parse_avg(self):
        ast = parse('SELECT AVG(price) FROM products')
        assert ast.columns == [AggregateCall(function='AVG', column='price')]

    def test_parse_min(self):
        ast = parse('SELECT MIN(price) FROM products')
        assert ast.columns == [AggregateCall(function='MIN', column='price')]

    def test_parse_max(self):
        ast = parse('SELECT MAX(price) FROM products')
        assert ast.columns == [AggregateCall(function='MAX', column='price')]

    def test_parse_group_by_with_aggregate(self):
        ast = parse('SELECT category, COUNT(*) FROM products GROUP BY category')
        assert ast.columns == ['category', AggregateCall(function='COUNT', column='*')]
        assert ast.group_by == ['category']

    def test_parse_multiple_aggregates(self):
        ast = parse('SELECT category, SUM(price), AVG(price) FROM products GROUP BY category')
        assert ast.columns == [
            'category',
            AggregateCall(function='SUM', column='price'),
            AggregateCall(function='AVG', column='price'),
        ]


class TestParserJoins:
    """Test JOIN clause parsing."""

    def test_parse_without_join_is_none(self):
        ast = parse('SELECT * FROM orders')
        assert ast.joins is None

    def test_parse_default_join_is_inner(self):
        ast = parse('SELECT * FROM orders JOIN users ON orders.user_id = users.id')
        assert ast.joins == [
            JoinClause(
                table='users',
                join_type='INNER',
                on=BinaryOp(
                    left=Identifier(name='orders.user_id'),
                    operator='=',
                    right=Identifier(name='users.id'),
                ),
            )
        ]

    def test_parse_explicit_inner_join(self):
        ast = parse('SELECT * FROM orders INNER JOIN users ON orders.user_id = users.id')
        assert ast.joins[0].join_type == 'INNER'

    def test_parse_left_join(self):
        ast = parse('SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id')
        assert ast.joins[0].join_type == 'LEFT'
        assert ast.joins[0].table == 'orders'

    def test_parse_left_outer_join(self):
        ast = parse('SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id')
        assert ast.joins[0].join_type == 'LEFT'

    def test_parse_multiple_joins(self):
        ast = parse(
            'SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id'
        )
        assert len(ast.joins) == 2
        assert ast.joins[0].table == 'b'
        assert ast.joins[1].table == 'c'

    def test_parse_qualified_select_columns(self):
        ast = parse('SELECT orders.total, users.name FROM orders JOIN users ON orders.user_id = users.id')
        assert ast.columns == ['orders.total', 'users.name']

    def test_parse_qualified_where_column(self):
        ast = parse(
            'SELECT * FROM orders JOIN users ON orders.user_id = users.id WHERE users.name = \'Alice\''
        )
        assert ast.where == BinaryOp(
            left=Identifier(name='users.name'), operator='=', right=Literal(value='Alice')
        )

    def test_parse_join_with_where_and_order_by(self):
        ast = parse(
            'SELECT * FROM orders JOIN users ON orders.user_id = users.id '
            'WHERE orders.total > 10 ORDER BY orders.total DESC'
        )
        assert ast.where is not None
        assert ast.order_by == [('orders.total', 'DESC')]

    def test_parse_qualified_aggregate_column(self):
        ast = parse(
            'SELECT SUM(orders.total) FROM orders JOIN users ON orders.user_id = users.id'
        )
        assert ast.columns == [AggregateCall(function='SUM', column='orders.total')]


class TestParserInsert:
    """Test INSERT statement parsing."""
    
    def test_parse_insert_single_value(self):
        ast = parse("INSERT INTO users VALUES (1)")
        assert isinstance(ast, InsertStatement)
        assert ast.table == 'users'
        assert ast.values == [1]
    
    def test_parse_insert_multiple_values(self):
        ast = parse("INSERT INTO users VALUES (1, 'John', 25)")
        assert isinstance(ast, InsertStatement)
        assert ast.table == 'users'
        assert ast.values == [1, 'John', 25]
    
    def test_parse_insert_with_null(self):
        ast = parse("INSERT INTO users VALUES (1, NULL)")
        assert isinstance(ast, InsertStatement)
        assert ast.values == [1, None]


class TestParserCreateTable:
    """Test CREATE TABLE statement parsing."""
    
    def test_parse_create_table_single_column(self):
        ast = parse('CREATE TABLE users (id INTEGER)')
        assert isinstance(ast, CreateTableStatement)
        assert ast.table == 'users'
        assert len(ast.columns) == 1
        assert ast.columns[0].name == 'id'
        assert ast.columns[0].type == 'INTEGER'
    
    def test_parse_create_table_multiple_columns(self):
        ast = parse('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)')
        assert isinstance(ast, CreateTableStatement)
        assert len(ast.columns) == 3
        assert ast.columns[0].name == 'id'
        assert ast.columns[1].name == 'name'
        assert ast.columns[2].name == 'age'
    
    def test_parse_create_table_with_primary_key(self):
        ast = parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
        assert isinstance(ast, CreateTableStatement)
        assert ast.columns[0].primary_key is True
        assert ast.columns[1].primary_key is False
    
    def test_parse_create_table_all_types(self):
        ast = parse('CREATE TABLE test (a INTEGER, b TEXT, c REAL)')
        assert ast.columns[0].type == 'INTEGER'
        assert ast.columns[1].type == 'TEXT'
        assert ast.columns[2].type == 'REAL'


class TestParserForeignKeys:
    """Test REFERENCES clause parsing in column definitions."""

    def test_parse_column_with_references(self):
        ast = parse('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id))')
        fk_col = ast.columns[1]
        assert fk_col.references_table == 'users'
        assert fk_col.references_column == 'id'

    def test_parse_column_without_references_defaults_none(self):
        ast = parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
        assert ast.columns[0].references_table is None
        assert ast.columns[1].references_table is None

    def test_parse_primary_key_and_references_together(self):
        ast = parse(
            'CREATE TABLE items (id INTEGER PRIMARY KEY REFERENCES products(sku))'
        )
        assert ast.columns[0].primary_key is True
        assert ast.columns[0].references_table == 'products'
        assert ast.columns[0].references_column == 'sku'

    def test_parse_alter_table_add_column_with_references(self):
        ast = parse('ALTER TABLE orders ADD COLUMN customer_id INTEGER REFERENCES customers(id)')
        assert ast.column.references_table == 'customers'
        assert ast.column.references_column == 'id'


class TestParserTransactions:
    """Test BEGIN / COMMIT / ROLLBACK statement parsing."""

    def test_parse_begin(self):
        ast = parse('BEGIN')
        assert isinstance(ast, TransactionStatement)
        assert ast.action == 'BEGIN'

    def test_parse_begin_transaction(self):
        ast = parse('BEGIN TRANSACTION')
        assert ast.action == 'BEGIN'

    def test_parse_commit(self):
        ast = parse('COMMIT')
        assert isinstance(ast, TransactionStatement)
        assert ast.action == 'COMMIT'

    def test_parse_rollback(self):
        ast = parse('ROLLBACK')
        assert isinstance(ast, TransactionStatement)
        assert ast.action == 'ROLLBACK'


class TestParserCreateDropIndex:
    """Test CREATE INDEX and DROP INDEX statement parsing."""

    def test_parse_create_index(self):
        ast = parse('CREATE INDEX idx_users_name ON users (name)')
        assert isinstance(ast, CreateIndexStatement)
        assert ast.index_name == 'idx_users_name'
        assert ast.table == 'users'
        assert ast.column == 'name'

    def test_parse_drop_index(self):
        ast = parse('DROP INDEX idx_users_name')
        assert isinstance(ast, DropIndexStatement)
        assert ast.index_name == 'idx_users_name'

    def test_create_table_still_parses(self):
        ast = parse('CREATE TABLE users (id INTEGER PRIMARY KEY)')
        assert isinstance(ast, CreateTableStatement)

    def test_drop_table_still_parses(self):
        ast = parse('DROP TABLE users')
        assert isinstance(ast, DropTableStatement)


class TestParserExpressions:
    """Test expression parsing."""
    
    def test_parse_equality_expression(self):
        ast = parse('SELECT * FROM users WHERE id = 5')
        expr = ast.where
        assert isinstance(expr, BinaryOp)
        assert expr.operator == '='
        assert isinstance(expr.left, Identifier)
        assert expr.left.name == 'id'
        assert isinstance(expr.right, Literal)
        assert expr.right.value == 5
    
    def test_parse_comparison_operators(self):
        operators = ['<', '>', '<=', '>=', '!=']
        for op in operators:
            sql = f'SELECT * FROM users WHERE age {op} 18'
            ast = parse(sql)
            assert ast.where.operator == op
    
    def test_parse_and_expression(self):
        ast = parse('SELECT * FROM users WHERE age > 18 AND name = \'John\'')
        expr = ast.where
        assert isinstance(expr, BinaryOp)
        assert expr.operator == 'AND'
        assert isinstance(expr.left, BinaryOp)
        assert isinstance(expr.right, BinaryOp)
    
    def test_parse_or_expression(self):
        ast = parse('SELECT * FROM users WHERE age < 18 OR age > 65')
        expr = ast.where
        assert isinstance(expr, BinaryOp)
        assert expr.operator == 'OR'
    
    def test_parse_string_literal_in_expression(self):
        ast = parse("SELECT * FROM users WHERE name = 'Alice'")
        expr = ast.where
        assert isinstance(expr.right, Literal)
        assert expr.right.value == 'Alice'


class TestParserErrors:
    """Test parser error handling."""
    
    def test_missing_from(self):
        with pytest.raises(ParseError):
            parse('SELECT * users')
    
    def test_missing_table_name(self):
        with pytest.raises(ParseError):
            parse('SELECT * FROM')
    
    def test_invalid_statement(self):
        with pytest.raises(ParseError):
            parse('INVALID STATEMENT')
    
    def test_missing_values_keyword(self):
        with pytest.raises(ParseError):
            parse('INSERT INTO users (1, 2)')
    
    def test_unclosed_parenthesis(self):
        with pytest.raises(ParseError):
            parse('INSERT INTO users VALUES (1, 2')


class TestParserComplexQueries:
    """Test parsing complex queries."""
    
    def test_select_multiple_columns_with_where(self):
        ast = parse('SELECT id, name, age FROM users WHERE age >= 21')
        assert isinstance(ast, SelectStatement)
        assert len(ast.columns) == 3
        assert ast.where.operator == '>='
    
    def test_create_table_complex(self):
        sql = '''CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT,
            salary REAL,
            department TEXT
        )'''
        ast = parse(sql)
        assert isinstance(ast, CreateTableStatement)
        assert len(ast.columns) == 4
        assert ast.columns[0].primary_key is True


class TestASTNodes:
    """Test AST node classes."""
    
    def test_literal_repr(self):
        lit = Literal(42)
        assert '42' in repr(lit)
    
    def test_identifier_repr(self):
        ident = Identifier('users')
        assert 'users' in repr(ident)
    
    def test_binary_op_repr(self):
        op = BinaryOp(Identifier('x'), '=', Literal(5))
        assert '=' in repr(op)
    
    def test_column_def_repr(self):
        col = ColumnDef('id', 'INTEGER', primary_key=True)
        assert 'id' in repr(col)
        assert 'INTEGER' in repr(col)
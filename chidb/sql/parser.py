"""
SQL Parser - Builds Abstract Syntax Trees from token streams.
Converts tokens into structured AST representations.
"""

from typing import List, Optional, Any
from dataclasses import dataclass
from chidb.sql.lexer import Token, TokenType, Lexer
from chidb.security import QueryError


# AST Node Classes

@dataclass
class ASTNode:
    """Base class for all AST nodes."""
    pass


@dataclass
class JoinClause(ASTNode):
    """A single JOIN in a SELECT's FROM clause: [INNER|LEFT] JOIN table ON condition."""
    table: str
    join_type: str  # 'INNER' or 'LEFT'
    on: 'Expression'

    def __repr__(self) -> str:
        return f"JoinClause({self.join_type} JOIN {self.table} ON {self.on})"


@dataclass
class SelectStatement(ASTNode):
    """SELECT statement AST node."""
    columns: List[Any]  # Column names ('*', str) or AggregateCall entries
    table: str
    where: Optional['Expression'] = None
    group_by: Optional[List[str]] = None
    order_by: Optional[List[tuple]] = None  # List of (column_name, direction) tuples
    limit: Optional[int] = None
    offset: Optional[int] = None
    distinct: bool = False
    joins: Optional[List[JoinClause]] = None

    def __repr__(self) -> str:
        return f"SelectStatement(columns={self.columns}, table={self.table}, joins={self.joins}, where={self.where}, group_by={self.group_by}, order_by={self.order_by}, limit={self.limit})"


@dataclass
class InsertStatement(ASTNode):
    """INSERT statement AST node."""
    table: str
    values: List[Any]
    
    def __repr__(self) -> str:
        return f"InsertStatement(table={self.table}, values={self.values})"


@dataclass
class CreateTableStatement(ASTNode):
    """CREATE TABLE statement AST node."""
    table: str
    columns: List['ColumnDef']
    
    def __repr__(self) -> str:
        return f"CreateTableStatement(table={self.table}, columns={self.columns})"


@dataclass
class UpdateStatement(ASTNode):
    """UPDATE statement AST node."""
    table: str
    assignments: List[tuple]  # List of (column_name, value) tuples
    where: Optional['Expression'] = None
    
    def __repr__(self) -> str:
        return f"UpdateStatement(table={self.table}, assignments={self.assignments}, where={self.where})"


@dataclass
class DeleteStatement(ASTNode):
    """DELETE statement AST node."""
    table: str
    where: Optional['Expression'] = None
    
    def __repr__(self) -> str:
        return f"DeleteStatement(table={self.table}, where={self.where})"


@dataclass
class DropTableStatement(ASTNode):
    """DROP TABLE statement AST node."""
    table: str
    
    def __repr__(self) -> str:
        return f"DropTableStatement(table={self.table})"


@dataclass
class AlterTableStatement(ASTNode):
    """ALTER TABLE statement AST node."""
    table: str
    action: str  # 'ADD'
    column: Optional['ColumnDef'] = None
    
    def __repr__(self) -> str:
        return f"AlterTableStatement(table={self.table}, action={self.action}, column={self.column})"


@dataclass
class CreateIndexStatement(ASTNode):
    """CREATE INDEX statement AST node."""
    index_name: str
    table: str
    column: str

    def __repr__(self) -> str:
        return f"CreateIndexStatement(index_name={self.index_name}, table={self.table}, column={self.column})"


@dataclass
class DropIndexStatement(ASTNode):
    """DROP INDEX statement AST node."""
    index_name: str

    def __repr__(self) -> str:
        return f"DropIndexStatement(index_name={self.index_name})"


@dataclass
class TransactionStatement(ASTNode):
    """BEGIN / COMMIT / ROLLBACK statement AST node."""
    action: str  # 'BEGIN', 'COMMIT', or 'ROLLBACK'

    def __repr__(self) -> str:
        return f"TransactionStatement(action={self.action})"


@dataclass
class ColumnDef:
    """Column definition in CREATE TABLE."""
    name: str
    type: str  # INTEGER, TEXT, REAL
    primary_key: bool = False
    references_table: Optional[str] = None
    references_column: Optional[str] = None

    def __repr__(self) -> str:
        ref = f", references={self.references_table}({self.references_column})" if self.references_table else ""
        return f"ColumnDef({self.name}, {self.type}, pk={self.primary_key}{ref})"


@dataclass
class Expression(ASTNode):
    """Base class for expressions."""
    pass


@dataclass
class BinaryOp(Expression):
    """Binary operation (e.g., a = b, x > 5)."""
    left: Expression
    operator: str  # '=', '!=', '<', '>', '<=', '>=', 'AND', 'OR'
    right: Expression
    
    def __repr__(self) -> str:
        return f"BinaryOp({self.left} {self.operator} {self.right})"


@dataclass
class Literal(Expression):
    """Literal value (number, string, null)."""
    value: Any
    
    def __repr__(self) -> str:
        return f"Literal({self.value!r})"


@dataclass
class Identifier(Expression):
    """Identifier (column name, table name)."""
    name: str

    def __repr__(self) -> str:
        return f"Identifier({self.name})"


@dataclass
class AggregateCall(Expression):
    """Aggregate function call in a SELECT column list, e.g. COUNT(*), SUM(price)."""
    function: str  # 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'
    column: str    # column name, or '*' for COUNT(*)

    def __repr__(self) -> str:
        return f"AggregateCall({self.function}({self.column}))"


AGGREGATE_TOKENS = {
    TokenType.COUNT: 'COUNT',
    TokenType.SUM: 'SUM',
    TokenType.AVG: 'AVG',
    TokenType.MIN: 'MIN',
    TokenType.MAX: 'MAX',
}


class ParseError(QueryError):
    """
    Exception raised for parsing errors.

    A QueryError subclass: the message only ever echoes back tokens from
    the caller's own SQL, so it's always safe to show as-is.
    """
    pass


class Parser:
    """
    SQL Parser for converting tokens into AST.
    """
    
    def __init__(self, tokens: List[Token]):
        """
        Initialize the parser.
        
        Args:
            tokens: List of tokens from the lexer
        """
        self.tokens = tokens
        self.position = 0
        self.current_token = tokens[0] if tokens else None
    
    def advance(self) -> None:
        """Move to the next token."""
        self.position += 1
        if self.position < len(self.tokens):
            self.current_token = self.tokens[self.position]
        else:
            self.current_token = None
    
    def peek(self, offset: int = 1) -> Optional[Token]:
        """Look ahead at the next token."""
        peek_pos = self.position + offset
        if peek_pos < len(self.tokens):
            return self.tokens[peek_pos]
        return None
    
    def expect(self, token_type: TokenType) -> Token:
        """
        Expect a specific token type and advance.
        
        Raises ParseError if token doesn't match.
        """
        if self.current_token is None or self.current_token.type != token_type:
            raise ParseError(
                f"Expected {token_type}, got {self.current_token.type if self.current_token else 'EOF'}"
            )
        token = self.current_token
        self.advance()
        return token
    
    def match(self, *token_types: TokenType) -> bool:
        """Check if current token matches any of the given types."""
        if self.current_token is None:
            return False
        return self.current_token.type in token_types
    
    def parse(self) -> ASTNode:
        """
        Parse the token stream into an AST.
        
        Returns:
            Root AST node
        """
        if self.match(TokenType.SELECT):
            return self.parse_select()
        elif self.match(TokenType.INSERT):
            return self.parse_insert()
        elif self.match(TokenType.CREATE):
            if self.peek() and self.peek().type == TokenType.INDEX:
                return self.parse_create_index()
            return self.parse_create_table()
        elif self.match(TokenType.UPDATE):
            return self.parse_update()
        elif self.match(TokenType.DELETE):
            return self.parse_delete()
        elif self.match(TokenType.DROP):
            if self.peek() and self.peek().type == TokenType.INDEX:
                return self.parse_drop_index()
            return self.parse_drop_table()
        elif self.match(TokenType.ALTER):
            return self.parse_alter_table()
        elif self.match(TokenType.BEGIN):
            return self.parse_begin()
        elif self.match(TokenType.COMMIT):
            return self.parse_commit()
        elif self.match(TokenType.ROLLBACK):
            return self.parse_rollback()
        else:
            raise ParseError(f"Unexpected token: {self.current_token}")
    
    def parse_select(self) -> SelectStatement:
        """
        Parse SELECT statement.
        
        Grammar:
        SELECT [DISTINCT] column [, column]* FROM table [WHERE expression] [ORDER BY column [ASC|DESC]] [LIMIT num] [OFFSET num]
        """
        self.expect(TokenType.SELECT)
        
        # Check for DISTINCT
        distinct = False
        if self.match(TokenType.DISTINCT):
            distinct = True
            self.advance()
        
        # Parse columns
        columns = []
        if self.match(TokenType.STAR):
            columns.append('*')
            self.advance()
        else:
            columns.append(self.parse_select_column())

            while self.match(TokenType.COMMA):
                self.advance()
                columns.append(self.parse_select_column())

        # FROM clause
        self.expect(TokenType.FROM)
        table = self.expect(TokenType.IDENTIFIER).value

        # Optional JOIN clauses
        joins = []
        while self.match(TokenType.JOIN, TokenType.INNER, TokenType.LEFT):
            joins.append(self.parse_join_clause())
        joins = joins or None

        # Optional WHERE clause
        where = None
        if self.match(TokenType.WHERE):
            self.advance()
            where = self.parse_expression()

        # Optional GROUP BY clause
        group_by = None
        if self.match(TokenType.GROUP):
            self.advance()
            self.expect(TokenType.BY)

            group_by = [self.parse_qualified_name()]
            while self.match(TokenType.COMMA):
                self.advance()
                group_by.append(self.parse_qualified_name())

        # Optional ORDER BY clause
        order_by = None
        if self.match(TokenType.ORDER):
            self.advance()
            self.expect(TokenType.BY)
            
            order_by = []
            # Parse column name
            col = self.parse_qualified_name()
            direction = 'ASC'  # Default

            if self.match(TokenType.ASC):
                direction = 'ASC'
                self.advance()
            elif self.match(TokenType.DESC):
                direction = 'DESC'
                self.advance()

            order_by.append((col, direction))

            # Multiple ORDER BY columns
            while self.match(TokenType.COMMA):
                self.advance()
                col = self.parse_qualified_name()
                direction = 'ASC'
                
                if self.match(TokenType.ASC):
                    direction = 'ASC'
                    self.advance()
                elif self.match(TokenType.DESC):
                    direction = 'DESC'
                    self.advance()
                
                order_by.append((col, direction))
        
        # Optional LIMIT clause
        limit = None
        if self.match(TokenType.LIMIT):
            self.advance()
            limit = self.expect(TokenType.INTEGER_LITERAL).value
        
        # Optional OFFSET clause
        offset = None
        if self.match(TokenType.OFFSET):
            self.advance()
            offset = self.expect(TokenType.INTEGER_LITERAL).value
        
        return SelectStatement(
            columns=columns,
            table=table,
            where=where,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            offset=offset,
            distinct=distinct,
            joins=joins
        )

    def parse_join_clause(self) -> JoinClause:
        """
        Parse a single JOIN clause.

        Grammar:
        [INNER | LEFT [OUTER]] JOIN table ON expression
        """
        join_type = 'INNER'
        if self.match(TokenType.INNER):
            self.advance()
        elif self.match(TokenType.LEFT):
            join_type = 'LEFT'
            self.advance()
            if self.match(TokenType.OUTER):
                self.advance()

        self.expect(TokenType.JOIN)
        table = self.expect(TokenType.IDENTIFIER).value
        self.expect(TokenType.ON)
        on = self.parse_expression()

        return JoinClause(table=table, join_type=join_type, on=on)

    def parse_qualified_name(self) -> str:
        """Parse an identifier, optionally qualified as 'table.column'."""
        name = self.expect(TokenType.IDENTIFIER).value
        if self.match(TokenType.DOT):
            self.advance()
            column = self.expect(TokenType.IDENTIFIER).value
            return f"{name}.{column}"
        return name

    def parse_select_column(self) -> Any:
        """Parse a single SELECT column: a plain (optionally qualified) identifier or an aggregate call."""
        if self.current_token and self.current_token.type in AGGREGATE_TOKENS:
            return self.parse_aggregate_call()
        return self.parse_qualified_name()

    def parse_aggregate_call(self) -> AggregateCall:
        """
        Parse an aggregate function call.

        Grammar:
        (COUNT | SUM | AVG | MIN | MAX) ( * | column )
        """
        function = AGGREGATE_TOKENS[self.current_token.type]
        self.advance()

        self.expect(TokenType.LPAREN)
        if self.match(TokenType.STAR):
            column = '*'
            self.advance()
        else:
            column = self.parse_qualified_name()
        self.expect(TokenType.RPAREN)

        return AggregateCall(function=function, column=column)
    
    def parse_insert(self) -> InsertStatement:
        """
        Parse INSERT statement.
        
        Grammar:
        INSERT INTO table VALUES (value [, value]*)
        """
        self.expect(TokenType.INSERT)
        self.expect(TokenType.INTO)
        
        table = self.expect(TokenType.IDENTIFIER).value
        
        self.expect(TokenType.VALUES)
        self.expect(TokenType.LPAREN)
        
        # Parse values
        values = []
        values.append(self.parse_literal_value())
        
        while self.match(TokenType.COMMA):
            self.advance()
            values.append(self.parse_literal_value())
        
        self.expect(TokenType.RPAREN)
        
        return InsertStatement(table=table, values=values)
    
    def parse_create_table(self) -> CreateTableStatement:
        """
        Parse CREATE TABLE statement.
        
        Grammar:
        CREATE TABLE table (column_def [, column_def]*)
        column_def: name type [PRIMARY KEY]
        """
        self.expect(TokenType.CREATE)
        self.expect(TokenType.TABLE)
        
        table = self.expect(TokenType.IDENTIFIER).value
        
        self.expect(TokenType.LPAREN)
        
        # Parse column definitions
        columns = []
        columns.append(self.parse_column_def())
        
        while self.match(TokenType.COMMA):
            self.advance()
            columns.append(self.parse_column_def())
        
        self.expect(TokenType.RPAREN)
        
        return CreateTableStatement(table=table, columns=columns)
    
    def parse_create_index(self) -> CreateIndexStatement:
        """
        Parse CREATE INDEX statement.

        Grammar:
        CREATE INDEX index_name ON table (column)
        """
        self.expect(TokenType.CREATE)
        self.expect(TokenType.INDEX)

        index_name = self.expect(TokenType.IDENTIFIER).value

        self.expect(TokenType.ON)
        table = self.expect(TokenType.IDENTIFIER).value

        self.expect(TokenType.LPAREN)
        column = self.expect(TokenType.IDENTIFIER).value
        self.expect(TokenType.RPAREN)

        return CreateIndexStatement(index_name=index_name, table=table, column=column)

    def parse_drop_index(self) -> DropIndexStatement:
        """
        Parse DROP INDEX statement.

        Grammar:
        DROP INDEX index_name
        """
        self.expect(TokenType.DROP)
        self.expect(TokenType.INDEX)

        index_name = self.expect(TokenType.IDENTIFIER).value

        return DropIndexStatement(index_name=index_name)

    def parse_begin(self) -> TransactionStatement:
        """
        Parse BEGIN statement.

        Grammar:
        BEGIN [TRANSACTION]
        """
        self.expect(TokenType.BEGIN)
        if self.match(TokenType.TRANSACTION):
            self.advance()
        return TransactionStatement(action='BEGIN')

    def parse_commit(self) -> TransactionStatement:
        """Parse COMMIT statement."""
        self.expect(TokenType.COMMIT)
        return TransactionStatement(action='COMMIT')

    def parse_rollback(self) -> TransactionStatement:
        """Parse ROLLBACK statement."""
        self.expect(TokenType.ROLLBACK)
        return TransactionStatement(action='ROLLBACK')

    def parse_update(self) -> UpdateStatement:
        """
        Parse UPDATE statement.
        
        Grammar:
        UPDATE table SET column = value [, column = value]* [WHERE expression]
        """
        self.expect(TokenType.UPDATE)
        
        table = self.expect(TokenType.IDENTIFIER).value
        
        self.expect(TokenType.SET)
        
        # Parse assignments
        assignments = []
        
        # First assignment
        col = self.expect(TokenType.IDENTIFIER).value
        self.expect(TokenType.EQUALS)
        val = self.parse_literal_value()
        assignments.append((col, val))
        
        # Additional assignments
        while self.match(TokenType.COMMA):
            self.advance()
            col = self.expect(TokenType.IDENTIFIER).value
            self.expect(TokenType.EQUALS)
            val = self.parse_literal_value()
            assignments.append((col, val))
        
        # Optional WHERE clause
        where = None
        if self.match(TokenType.WHERE):
            self.advance()
            where = self.parse_expression()
        
        return UpdateStatement(table=table, assignments=assignments, where=where)
    
    def parse_delete(self) -> DeleteStatement:
        """
        Parse DELETE statement.
        
        Grammar:
        DELETE FROM table [WHERE expression]
        """
        self.expect(TokenType.DELETE)
        self.expect(TokenType.FROM)
        
        table = self.expect(TokenType.IDENTIFIER).value
        
        # Optional WHERE clause
        where = None
        if self.match(TokenType.WHERE):
            self.advance()
            where = self.parse_expression()
        
        return DeleteStatement(table=table, where=where)
    
    def parse_drop_table(self) -> DropTableStatement:
        """
        Parse DROP TABLE statement.
        
        Grammar:
        DROP TABLE table_name
        """
        self.expect(TokenType.DROP)
        self.expect(TokenType.TABLE)
        
        table = self.expect(TokenType.IDENTIFIER).value
        
        return DropTableStatement(table=table)
    
    def parse_alter_table(self) -> AlterTableStatement:
        """
        Parse ALTER TABLE statement.
        
        Grammar:
        ALTER TABLE table_name ADD COLUMN column_def
        """
        self.expect(TokenType.ALTER)
        self.expect(TokenType.TABLE)
        
        table = self.expect(TokenType.IDENTIFIER).value
        
        self.expect(TokenType.ADD)
        
        # Optional COLUMN keyword
        if self.match(TokenType.COLUMN):
            self.advance()
        
        # Parse column definition
        column = self.parse_column_def()
        
        return AlterTableStatement(table=table, action='ADD', column=column)
    
    def parse_column_def(self) -> ColumnDef:
        """
        Parse a column definition.

        Grammar:
        name type [PRIMARY KEY] [REFERENCES table (column)]
        """
        name = self.expect(TokenType.IDENTIFIER).value
        
        # Parse type
        if self.match(TokenType.INTEGER):
            col_type = 'INTEGER'
            self.advance()
        elif self.match(TokenType.TEXT):
            col_type = 'TEXT'
            self.advance()
        elif self.match(TokenType.REAL):
            col_type = 'REAL'
            self.advance()
        else:
            raise ParseError(f"Expected type (INTEGER, TEXT, REAL), got {self.current_token}")
        
        # Check for PRIMARY KEY
        primary_key = False
        if self.match(TokenType.PRIMARY):
            self.advance()
            self.expect(TokenType.KEY)
            primary_key = True

        # Check for REFERENCES table (column)
        references_table = None
        references_column = None
        if self.match(TokenType.REFERENCES):
            self.advance()
            references_table = self.expect(TokenType.IDENTIFIER).value
            self.expect(TokenType.LPAREN)
            references_column = self.expect(TokenType.IDENTIFIER).value
            self.expect(TokenType.RPAREN)

        return ColumnDef(
            name=name, type=col_type, primary_key=primary_key,
            references_table=references_table, references_column=references_column
        )
    
    def parse_expression(self) -> Expression:
        """
        Parse an expression.
        
        For now, we support simple binary comparisons and AND/OR.
        """
        return self.parse_or_expression()
    
    def parse_or_expression(self) -> Expression:
        """Parse OR expression (lowest precedence)."""
        left = self.parse_and_expression()
        
        while self.match(TokenType.OR):
            self.advance()
            right = self.parse_and_expression()
            left = BinaryOp(left=left, operator='OR', right=right)
        
        return left
    
    def parse_and_expression(self) -> Expression:
        """Parse AND expression."""
        left = self.parse_comparison()
        
        while self.match(TokenType.AND):
            self.advance()
            right = self.parse_comparison()
            left = BinaryOp(left=left, operator='AND', right=right)
        
        return left
    
    def parse_comparison(self) -> Expression:
        """Parse comparison expression."""
        left = self.parse_primary()
        
        if self.match(TokenType.EQUALS):
            operator = '='
            self.advance()
        elif self.match(TokenType.NOT_EQUALS):
            operator = '!='
            self.advance()
        elif self.match(TokenType.LESS_THAN):
            operator = '<'
            self.advance()
        elif self.match(TokenType.LESS_EQUAL):
            operator = '<='
            self.advance()
        elif self.match(TokenType.GREATER_THAN):
            operator = '>'
            self.advance()
        elif self.match(TokenType.GREATER_EQUAL):
            operator = '>='
            self.advance()
        else:
            return left
        
        right = self.parse_primary()
        return BinaryOp(left=left, operator=operator, right=right)
    
    def parse_primary(self) -> Expression:
        """Parse primary expression (identifier or literal)."""
        if self.match(TokenType.IDENTIFIER):
            name = self.parse_qualified_name()
            return Identifier(name=name)
        
        elif self.match(TokenType.INTEGER_LITERAL, TokenType.STRING_LITERAL, 
                        TokenType.FLOAT_LITERAL, TokenType.NULL):
            return self.parse_literal()
        
        elif self.match(TokenType.LPAREN):
            self.advance()
            expr = self.parse_expression()
            self.expect(TokenType.RPAREN)
            return expr
        
        else:
            raise ParseError(f"Unexpected token in expression: {self.current_token}")
    
    def parse_literal(self) -> Literal:
        """Parse a literal value."""
        if self.match(TokenType.INTEGER_LITERAL):
            value = self.current_token.value
            self.advance()
            return Literal(value=value)
        
        elif self.match(TokenType.STRING_LITERAL):
            value = self.current_token.value
            self.advance()
            return Literal(value=value)
        
        elif self.match(TokenType.FLOAT_LITERAL):
            value = self.current_token.value
            self.advance()
            return Literal(value=value)
        
        elif self.match(TokenType.NULL):
            self.advance()
            return Literal(value=None)
        
        else:
            raise ParseError(f"Expected literal, got {self.current_token}")
    
    def parse_literal_value(self) -> Any:
        """Parse a literal value and return its Python value."""
        if self.match(TokenType.INTEGER_LITERAL):
            value = self.current_token.value
            self.advance()
            return value
        
        elif self.match(TokenType.STRING_LITERAL):
            value = self.current_token.value
            self.advance()
            return value
        
        elif self.match(TokenType.FLOAT_LITERAL):
            value = self.current_token.value
            self.advance()
            return value
        
        elif self.match(TokenType.NULL):
            self.advance()
            return None
        
        else:
            raise ParseError(f"Expected literal value, got {self.current_token}")


def parse(source: str) -> ASTNode:
    """
    Convenience function to parse SQL source.
    
    Args:
        source: SQL source code
        
    Returns:
        AST root node
    """
    lexer = Lexer(source)
    tokens = lexer.tokenize()
    parser = Parser(tokens)
    return parser.parse()
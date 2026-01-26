"""
SQL Parser - Builds Abstract Syntax Trees from token streams.
Converts tokens into structured AST representations.
"""

from typing import List, Optional, Any
from dataclasses import dataclass
from chidb.sql.lexer import Token, TokenType, Lexer


# AST Node Classes

@dataclass
class ASTNode:
    """Base class for all AST nodes."""
    pass


@dataclass
class SelectStatement(ASTNode):
    """SELECT statement AST node."""
    columns: List[str]  # Column names or '*'
    table: str
    where: Optional['Expression'] = None
    
    def __repr__(self) -> str:
        return f"SelectStatement(columns={self.columns}, table={self.table}, where={self.where})"


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
class ColumnDef:
    """Column definition in CREATE TABLE."""
    name: str
    type: str  # INTEGER, TEXT, REAL
    primary_key: bool = False
    
    def __repr__(self) -> str:
        return f"ColumnDef({self.name}, {self.type}, pk={self.primary_key})"


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


class ParseError(Exception):
    """Exception raised for parsing errors."""
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
            return self.parse_create_table()
        elif self.match(TokenType.UPDATE):
            return self.parse_update()
        elif self.match(TokenType.DELETE):
            return self.parse_delete()
        else:
            raise ParseError(f"Unexpected token: {self.current_token}")
    
    def parse_select(self) -> SelectStatement:
        """
        Parse SELECT statement.
        
        Grammar:
        SELECT column [, column]* FROM table [WHERE expression]
        """
        self.expect(TokenType.SELECT)
        
        # Parse columns
        columns = []
        if self.match(TokenType.STAR):
            columns.append('*')
            self.advance()
        else:
            columns.append(self.expect(TokenType.IDENTIFIER).value)
            
            while self.match(TokenType.COMMA):
                self.advance()
                columns.append(self.expect(TokenType.IDENTIFIER).value)
        
        # FROM clause
        self.expect(TokenType.FROM)
        table = self.expect(TokenType.IDENTIFIER).value
        
        # Optional WHERE clause
        where = None
        if self.match(TokenType.WHERE):
            self.advance()
            where = self.parse_expression()
        
        return SelectStatement(columns=columns, table=table, where=where)
    
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
    
    def parse_column_def(self) -> ColumnDef:
        """
        Parse a column definition.
        
        Grammar:
        name type [PRIMARY KEY]
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
        
        return ColumnDef(name=name, type=col_type, primary_key=primary_key)
    
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
            name = self.current_token.value
            self.advance()
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
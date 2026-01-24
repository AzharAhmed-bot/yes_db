"""
SQL Lexer - Tokenizes SQL source code.
Converts SQL text into a stream of tokens for parsing.
"""

from typing import List, Optional
from dataclasses import dataclass
from enum import Enum, auto


class TokenType(Enum):
    """Types of tokens in SQL."""
    # Keywords
    SELECT = auto()
    FROM = auto()
    WHERE = auto()
    INSERT = auto()
    INTO = auto()
    VALUES = auto()
    CREATE = auto()
    TABLE = auto()
    INTEGER = auto()
    TEXT = auto()
    REAL = auto()
    DELETE = auto()
    UPDATE = auto()
    SET = auto()
    AND = auto()
    OR = auto()
    NOT = auto()
    NULL = auto()
    PRIMARY = auto()
    KEY = auto()
    
    # Literals
    INTEGER_LITERAL = auto()
    STRING_LITERAL = auto()
    FLOAT_LITERAL = auto()
    
    # Identifiers
    IDENTIFIER = auto()
    
    # Operators
    EQUALS = auto()           # =
    NOT_EQUALS = auto()       # !=
    LESS_THAN = auto()        # <
    LESS_EQUAL = auto()       # <=
    GREATER_THAN = auto()     # >
    GREATER_EQUAL = auto()    # >=
    PLUS = auto()             # +
    MINUS = auto()            # -
    STAR = auto()             # *
    SLASH = auto()            # /
    
    # Delimiters
    LPAREN = auto()           # (
    RPAREN = auto()           # )
    COMMA = auto()            # ,
    SEMICOLON = auto()        # ;
    DOT = auto()              # .
    
    # Special
    EOF = auto()
    UNKNOWN = auto()


@dataclass
class Token:
    """Represents a single token."""
    type: TokenType
    value: any
    line: int
    column: int
    
    def __repr__(self) -> str:
        return f"Token({self.type.name}, {self.value!r}, {self.line}:{self.column})"


# SQL Keywords mapping
KEYWORDS = {
    'SELECT': TokenType.SELECT,
    'FROM': TokenType.FROM,
    'WHERE': TokenType.WHERE,
    'INSERT': TokenType.INSERT,
    'INTO': TokenType.INTO,
    'VALUES': TokenType.VALUES,
    'CREATE': TokenType.CREATE,
    'TABLE': TokenType.TABLE,
    'INTEGER': TokenType.INTEGER,
    'TEXT': TokenType.TEXT,
    'REAL': TokenType.REAL,
    'DELETE': TokenType.DELETE,
    'UPDATE': TokenType.UPDATE,
    'SET': TokenType.SET,
    'AND': TokenType.AND,
    'OR': TokenType.OR,
    'NOT': TokenType.NOT,
    'NULL': TokenType.NULL,
    'PRIMARY': TokenType.PRIMARY,
    'KEY': TokenType.KEY,
}


class Lexer:
    """
    SQL Lexer for tokenizing SQL statements.
    """
    
    def __init__(self, source: str):
        """
        Initialize the lexer.
        
        Args:
            source: SQL source code to tokenize
        """
        self.source = source
        self.position = 0
        self.line = 1
        self.column = 1
        self.current_char: Optional[str] = source[0] if source else None
    
    def advance(self) -> None:
        """Move to the next character."""
        if self.current_char == '\n':
            self.line += 1
            self.column = 1
        else:
            self.column += 1
        
        self.position += 1
        if self.position >= len(self.source):
            self.current_char = None
        else:
            self.current_char = self.source[self.position]
    
    def peek(self, offset: int = 1) -> Optional[str]:
        """Look ahead at the next character without advancing."""
        peek_pos = self.position + offset
        if peek_pos >= len(self.source):
            return None
        return self.source[peek_pos]
    
    def skip_whitespace(self) -> None:
        """Skip whitespace characters."""
        while self.current_char is not None and self.current_char.isspace():
            self.advance()
    
    def skip_comment(self) -> None:
        """Skip SQL comments (-- to end of line)."""
        if self.current_char == '-' and self.peek() == '-':
            # Skip until end of line
            while self.current_char is not None and self.current_char != '\n':
                self.advance()
            if self.current_char == '\n':
                self.advance()
    
    def read_number(self) -> Token:
        """Read a numeric literal (integer or float)."""
        start_line = self.line
        start_column = self.column
        num_str = ''
        is_float = False
        
        while self.current_char is not None and (self.current_char.isdigit() or self.current_char == '.'):
            if self.current_char == '.':
                if is_float:
                    break  # Second dot, stop
                is_float = True
            num_str += self.current_char
            self.advance()
        
        if is_float:
            return Token(TokenType.FLOAT_LITERAL, float(num_str), start_line, start_column)
        else:
            return Token(TokenType.INTEGER_LITERAL, int(num_str), start_line, start_column)
    
    def read_string(self) -> Token:
        """Read a string literal (single-quoted)."""
        start_line = self.line
        start_column = self.column
        
        # Skip opening quote
        self.advance()
        
        string_value = ''
        while self.current_char is not None and self.current_char != "'":
            if self.current_char == '\\' and self.peek() == "'":
                # Escaped quote
                self.advance()
                string_value += "'"
                self.advance()
            else:
                string_value += self.current_char
                self.advance()
        
        if self.current_char == "'":
            self.advance()  # Skip closing quote
        
        return Token(TokenType.STRING_LITERAL, string_value, start_line, start_column)
    
    def read_identifier(self) -> Token:
        """Read an identifier or keyword."""
        start_line = self.line
        start_column = self.column
        identifier = ''
        
        while self.current_char is not None and (self.current_char.isalnum() or self.current_char == '_'):
            identifier += self.current_char
            self.advance()
        
        # Check if it's a keyword
        upper_identifier = identifier.upper()
        if upper_identifier in KEYWORDS:
            return Token(KEYWORDS[upper_identifier], identifier, start_line, start_column)
        
        return Token(TokenType.IDENTIFIER, identifier, start_line, start_column)
    
    def get_next_token(self) -> Token:
        """
        Get the next token from the source.
        
        Returns:
            The next token
        """
        while self.current_char is not None:
            # Skip whitespace
            if self.current_char.isspace():
                self.skip_whitespace()
                continue
            
            # Skip comments
            if self.current_char == '-' and self.peek() == '-':
                self.skip_comment()
                continue
            
            start_line = self.line
            start_column = self.column
            
            # Numbers
            if self.current_char.isdigit():
                return self.read_number()
            
            # Strings
            if self.current_char == "'":
                return self.read_string()
            
            # Identifiers and keywords
            if self.current_char.isalpha() or self.current_char == '_':
                return self.read_identifier()
            
            # Operators and delimiters
            if self.current_char == '=':
                self.advance()
                return Token(TokenType.EQUALS, '=', start_line, start_column)
            
            if self.current_char == '!':
                if self.peek() == '=':
                    self.advance()
                    self.advance()
                    return Token(TokenType.NOT_EQUALS, '!=', start_line, start_column)
            
            if self.current_char == '<':
                self.advance()
                if self.current_char == '=':
                    self.advance()
                    return Token(TokenType.LESS_EQUAL, '<=', start_line, start_column)
                return Token(TokenType.LESS_THAN, '<', start_line, start_column)
            
            if self.current_char == '>':
                self.advance()
                if self.current_char == '=':
                    self.advance()
                    return Token(TokenType.GREATER_EQUAL, '>=', start_line, start_column)
                return Token(TokenType.GREATER_THAN, '>', start_line, start_column)
            
            if self.current_char == '+':
                self.advance()
                return Token(TokenType.PLUS, '+', start_line, start_column)
            
            if self.current_char == '-':
                self.advance()
                return Token(TokenType.MINUS, '-', start_line, start_column)
            
            if self.current_char == '*':
                self.advance()
                return Token(TokenType.STAR, '*', start_line, start_column)
            
            if self.current_char == '/':
                self.advance()
                return Token(TokenType.SLASH, '/', start_line, start_column)
            
            if self.current_char == '(':
                self.advance()
                return Token(TokenType.LPAREN, '(', start_line, start_column)
            
            if self.current_char == ')':
                self.advance()
                return Token(TokenType.RPAREN, ')', start_line, start_column)
            
            if self.current_char == ',':
                self.advance()
                return Token(TokenType.COMMA, ',', start_line, start_column)
            
            if self.current_char == ';':
                self.advance()
                return Token(TokenType.SEMICOLON, ';', start_line, start_column)
            
            if self.current_char == '.':
                self.advance()
                return Token(TokenType.DOT, '.', start_line, start_column)
            
            # Unknown character
            char = self.current_char
            self.advance()
            return Token(TokenType.UNKNOWN, char, start_line, start_column)
        
        # End of file
        return Token(TokenType.EOF, None, self.line, self.column)
    
    def tokenize(self) -> List[Token]:
        """
        Tokenize the entire source code.
        
        Returns:
            List of all tokens
        """
        tokens = []
        
        while True:
            token = self.get_next_token()
            tokens.append(token)
            
            if token.type == TokenType.EOF:
                break
        
        return tokens


def tokenize(source: str) -> List[Token]:
    """
    Convenience function to tokenize SQL source.
    
    Args:
        source: SQL source code
        
    Returns:
        List of tokens
    """
    lexer = Lexer(source)
    return lexer.tokenize()
"""
Public API for the ChiDB database.
Provides high-level interface for applications to interact with the database.
"""

from typing import List, Any, Optional, Dict
from chidb.pager import Pager
from chidb.btree import BTree
from chidb.dbm import DatabaseMachine
from chidb.sql.lexer import Lexer
from chidb.sql.parser import Parser, CreateTableStatement
from chidb.sql.optimizer import Optimizer
from chidb.sql.codegen import CodeGenerator
from chidb.log import get_logger


class YesDB:
    """
    Main database interface.
    
    Usage:
        db = ChiDB('mydb.cdb')
        db.execute('CREATE TABLE users (id INTEGER, name TEXT)')
        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        results = db.execute('SELECT * FROM users')
        db.close()
    
    Or with context manager:
        with ChiDB('mydb.cdb') as db:
            db.execute('SELECT * FROM users')
    """
    
    def __init__(self, filename: str):
        """
        Open or create a database.
        
        Args:
            filename: Path to the database file
        """
        self.filename = filename
        self.pager = Pager(filename)
        self.dbm = DatabaseMachine(self.pager)
        self.codegen = CodeGenerator()
        self.optimizer = Optimizer()
        self.logger = get_logger("api")
        
        # Table registry: maps table name -> root page ID
        self.tables: Dict[str, int] = {}
        
        # Initialize system (load existing tables if any)
        self._initialize()
    
    def _initialize(self) -> None:
        """Initialize database (load metadata if it exists)."""
        # For now, we don't have a system catalog
        # Tables need to be created in each session
        pass
    
    def execute(self, sql: str) -> List[List[Any]]:
        """
        Execute a SQL statement.
        
        Args:
            sql: SQL statement to execute
            
        Returns:
            List of result rows (for SELECT), empty list otherwise
        """
        try:
            # Lexical analysis
            lexer = Lexer(sql)
            tokens = lexer.tokenize()
            
            # Parsing
            parser = Parser(tokens)
            ast = parser.parse()
            
            # Handle CREATE TABLE specially (creates B-tree)
            if isinstance(ast, CreateTableStatement):
                return self._execute_create_table(ast)
            
            # Optimization
            ast = self.optimizer.optimize(ast)
            
            # Code generation
            self.codegen.table_registry = self.tables
            instructions = self.codegen.generate(ast)
            
            # Execution
            results = self.dbm.execute(instructions)
            
            return results
        
        except Exception as e:
            self.logger.error(f"Error executing SQL: {e}")
            raise
    
    def _execute_create_table(self, stmt: CreateTableStatement) -> List[List[Any]]:
        """
        Execute CREATE TABLE statement.
        
        This creates a new B-tree for the table.
        """
        table_name = stmt.table
        
        if table_name in self.tables:
            raise ValueError(f"Table '{table_name}' already exists")
        
        # Create a new B-tree for this table
        btree = BTree(self.pager)
        root_page = btree.get_root_page()
        
        # Register the table
        self.tables[table_name] = root_page
        
        self.logger.info(f"Created table '{table_name}' with root page {root_page}")
        
        return []  # CREATE TABLE returns no rows
    
    def close(self) -> None:
        """Close the database."""
        self.pager.close()
        self.logger.info(f"Closed database '{self.filename}'")
    
    def get_table_names(self) -> List[str]:
        """Get list of table names."""
        return list(self.tables.keys())
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        return table_name in self.tables
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False


def connect(filename: str) -> YesDB:
    """
    Connect to a database (convenience function).
    
    Args:
        filename: Path to the database file
        
    Returns:
        ChiDB instance
    """
    return YesDB(filename)
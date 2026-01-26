"""
Public API for the ChiDB database.
Provides high-level interface for applications to interact with the database.
"""

from typing import List, Any, Optional, Dict
from dataclasses import dataclass
from chidb.pager import Pager
from chidb.btree import BTree
from chidb.dbm import DatabaseMachine
from chidb.record import Record
from chidb.sql.lexer import Lexer
from chidb.sql.parser import Parser, CreateTableStatement, UpdateStatement, DeleteStatement, ColumnDef
from chidb.sql.optimizer import Optimizer
from chidb.sql.codegen import CodeGenerator
from chidb.log import get_logger


@dataclass
class TableMetadata:
    """Metadata about a table."""
    name: str
    root_page: int
    columns: List[ColumnDef]
    primary_key_column: Optional[str] = None
    next_auto_increment: int = 1


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
        
        # Table metadata: maps table name -> TableMetadata
        self.table_metadata: Dict[str, TableMetadata] = {}
        
        # Legacy tables dict for backward compatibility
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
            
            # Handle UPDATE specially (direct B-tree operation)
            if isinstance(ast, UpdateStatement):
                return self._execute_update(ast)
            
            # Handle DELETE specially (direct B-tree operation)
            if isinstance(ast, DeleteStatement):
                return self._execute_delete(ast)
            
            # Optimization
            ast = self.optimizer.optimize(ast)
            
            # Code generation
            self.codegen.table_registry = self.tables
            self.codegen.table_metadata = self.table_metadata
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
        
        # Find primary key column
        primary_key_column = None
        for col in stmt.columns:
            if col.primary_key:
                primary_key_column = col.name
                break
        
        # Create table metadata
        metadata = TableMetadata(
            name=table_name,
            root_page=root_page,
            columns=stmt.columns,
            primary_key_column=primary_key_column,
            next_auto_increment=1
        )
        
        # Register the table
        self.table_metadata[table_name] = metadata
        self.tables[table_name] = root_page
        
        self.logger.info(f"Created table '{table_name}' with root page {root_page}, PK: {primary_key_column}")
        
        return []  # CREATE TABLE returns no rows
    
    def _execute_update(self, stmt: UpdateStatement) -> List[List[Any]]:
        """
        Execute UPDATE statement.
        
        Updates records in the table.
        """
        from chidb.record import Record
        
        table_name = stmt.table
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
        
        root_page = self.tables[table_name]
        table_meta = self.table_metadata.get(table_name)
        
        # Get the B-tree
        btree = BTree(self.pager, root_page)
        
        # Scan all records
        all_records = btree.scan()
        
        updated_count = 0
        for key, record in all_records:
            # Check if this record matches WHERE clause (if any)
            # For now, update all records if no WHERE clause
            should_update = True
            
            if stmt.where:
                # Simple WHERE evaluation (only supports column = value)
                should_update = self._evaluate_where(record, stmt.where, table_meta)
            
            if should_update:
                # Get current values
                values = list(record.get_values())
                
                # Apply updates
                for col_name, new_value in stmt.assignments:
                    # Find column index
                    for i, col_def in enumerate(table_meta.columns):
                        if col_def.name == col_name:
                            values[i] = new_value
                            break
                
                # Update the record
                new_record = Record(values)
                btree.update(key, new_record)
                updated_count += 1
        
        self.logger.info(f"Updated {updated_count} rows in '{table_name}'")
        return []
    
    def _execute_delete(self, stmt: DeleteStatement) -> List[List[Any]]:
        """
        Execute DELETE statement.
        
        Deletes records from the table.
        """
        table_name = stmt.table
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
        
        root_page = self.tables[table_name]
        table_meta = self.table_metadata.get(table_name)
        
        # Get the B-tree
        btree = BTree(self.pager, root_page)
        
        # Scan all records to find ones to delete
        all_records = btree.scan()
        keys_to_delete = []
        
        for key, record in all_records:
            # Check if this record matches WHERE clause
            should_delete = True
            
            if stmt.where:
                should_delete = self._evaluate_where(record, stmt.where, table_meta)
            
            if should_delete:
                keys_to_delete.append(key)
        
        # Delete the keys
        for key in keys_to_delete:
            btree.delete(key)
        
        self.logger.info(f"Deleted {len(keys_to_delete)} rows from '{table_name}'")
        return []
    
    def _evaluate_where(self, record: 'Record', where_expr, table_meta) -> bool:
        """
        Evaluate WHERE clause for a record.
        
        Simplified implementation - only supports: column = value
        """
        from chidb.sql.parser import BinaryOp, Literal, Identifier
        
        if isinstance(where_expr, BinaryOp):
            # Get left side (should be column name)
            if isinstance(where_expr.left, Identifier):
                col_name = where_expr.left.name
                
                # Find column index
                col_index = None
                for i, col_def in enumerate(table_meta.columns):
                    if col_def.name == col_name:
                        col_index = i
                        break
                
                if col_index is None:
                    return False
                
                # Get record value
                record_value = record.get_value(col_index)
                
                # Get comparison value
                if isinstance(where_expr.right, Literal):
                    compare_value = where_expr.right.value
                else:
                    return False
                
                # Perform comparison
                if where_expr.operator == '=':
                    return record_value == compare_value
                elif where_expr.operator == '!=':
                    return record_value != compare_value
                elif where_expr.operator == '<':
                    return record_value < compare_value
                elif where_expr.operator == '>':
                    return record_value > compare_value
                elif where_expr.operator == '<=':
                    return record_value <= compare_value
                elif where_expr.operator == '>=':
                    return record_value >= compare_value
        
        return True
    
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
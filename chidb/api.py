"""
Public API for the ChiDB database.
Provides high-level interface for applications to interact with the database.
"""

from typing import List, Any, Optional, Dict
from dataclasses import dataclass
import json
from chidb.pager import Pager
from chidb.btree import BTree
from chidb.dbm import DatabaseMachine
from chidb.record import Record
from chidb.sql.lexer import Lexer
from chidb.sql.parser import Parser, CreateTableStatement, UpdateStatement, DeleteStatement, DropTableStatement, AlterTableStatement, ColumnDef, SelectStatement
from chidb.sql.optimizer import Optimizer
from chidb.sql.codegen import CodeGenerator
from chidb.log import get_logger


# System catalog constants
SYSTEM_CATALOG_PAGE = 1  # Reserved page for system catalog


@dataclass
class TableMetadata:
    """Metadata about a table."""
    name: str
    root_page: int
    columns: List[ColumnDef]
    primary_key_column: Optional[str] = None
    next_auto_increment: int = 1
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            'name': self.name,
            'root_page': self.root_page,
            'columns': [
                {
                    'name': col.name,
                    'type': col.type,
                    'primary_key': col.primary_key
                }
                for col in self.columns
            ],
            'primary_key_column': self.primary_key_column,
            'next_auto_increment': self.next_auto_increment
        }
    
    @staticmethod
    def from_dict(data: dict) -> 'TableMetadata':
        """Create from dictionary."""
        columns = [
            ColumnDef(
                name=col['name'],
                type=col['type'],
                primary_key=col['primary_key']
            )
            for col in data['columns']
        ]
        return TableMetadata(
            name=data['name'],
            root_page=data['root_page'],
            columns=columns,
            primary_key_column=data.get('primary_key_column'),
            next_auto_increment=data.get('next_auto_increment', 1)
        )


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
        # Check if this is a new database or existing
        if self.pager.get_num_pages() <= 1:
            # New database - create system catalog
            self._create_system_catalog()
        else:
            # Existing database - load system catalog
            self._load_system_catalog()
    
    def _create_system_catalog(self) -> None:
        """Create the system catalog B-tree."""
        # Create a B-tree for the system catalog
        self.catalog_btree = BTree(self.pager)
        self.catalog_root = self.catalog_btree.get_root_page()
        self.logger.info(f"Created system catalog at page {self.catalog_root}")
    
    def _load_system_catalog(self) -> None:
        """Load table metadata from system catalog."""
        # The catalog is at a known page (page 1)
        self.catalog_root = SYSTEM_CATALOG_PAGE
        self.catalog_btree = BTree(self.pager, self.catalog_root)
        
        # Scan the catalog and load all table metadata
        try:
            catalog_records = self.catalog_btree.scan()
            
            for key, record in catalog_records:
                # Record contains JSON-serialized table metadata
                json_data = record.get_value(0)
                if json_data:
                    metadata_dict = json.loads(json_data)
                    metadata = TableMetadata.from_dict(metadata_dict)
                    
                    self.table_metadata[metadata.name] = metadata
                    self.tables[metadata.name] = metadata.root_page
                    
                    self.logger.info(f"Loaded table '{metadata.name}' from catalog")
        except Exception as e:
            self.logger.warning(f"Could not load system catalog: {e}")
            # If catalog is corrupt, start fresh
            self.catalog_btree = BTree(self.pager, self.catalog_root)
    
    def _save_table_to_catalog(self, metadata: TableMetadata) -> None:
        """Save table metadata to system catalog."""
        # Serialize metadata to JSON
        metadata_dict = metadata.to_dict()
        json_data = json.dumps(metadata_dict)
        
        # Create a record with the JSON data
        record = Record([json_data])
        
        # Use a simple key (could use hash of table name)
        # For simplicity, use incremental keys
        key = len(self.table_metadata)
        
        # Insert into catalog
        self.catalog_btree.insert(key, record)
        self.pager.flush()
        
        self.logger.info(f"Saved table '{metadata.name}' to catalog")
    
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
            
            # Handle DROP TABLE
            if isinstance(ast, DropTableStatement):
                return self._execute_drop_table(ast)
            
            # Handle ALTER TABLE
            if isinstance(ast, AlterTableStatement):
                return self._execute_alter_table(ast)
            
            # Handle SELECT with ORDER BY/LIMIT
            if isinstance(ast, SelectStatement) and (ast.order_by or ast.limit or ast.offset or ast.distinct):
                return self._execute_select_advanced(ast)
            
            # Optimization
            ast = self.optimizer.optimize(ast)
            
            # Code generation
            self.codegen.table_registry = self.tables
            self.codegen.table_metadata = self.table_metadata
            instructions = self.codegen.generate(ast)
            
            # Execution
            results = self.dbm.execute(instructions)

            # Sync root page changes from BTrees back to metadata
            # (splits may have created new roots)
            metadata_changed = False
            for root_page, btree in list(self.dbm.btrees.items()):
                if btree.root_page != root_page:
                    # Root page changed, update metadata
                    for table_name, metadata in self.table_metadata.items():
                        if metadata.root_page == root_page:
                            metadata.root_page = btree.root_page
                            self._save_table_to_catalog(metadata)
                            metadata_changed = True
                            break

            # Clear the btrees cache to force fresh lookups with updated metadata
            # This ensures subsequent operations use the correct root pages
            if metadata_changed:
                self.dbm.btrees.clear()

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
        
        # Save to system catalog
        self._save_table_to_catalog(metadata)
        
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
    
    def _execute_select_advanced(self, stmt: SelectStatement) -> List[List[Any]]:
        """
        Execute SELECT with ORDER BY, LIMIT, OFFSET, or DISTINCT.
        """
        from chidb.record import Record
        
        table_name = stmt.table
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
        
        root_page = self.tables[table_name]
        table_meta = self.table_metadata.get(table_name)
        btree = BTree(self.pager, root_page)
        
        # Scan all records
        all_records = btree.scan()
        
        # Convert to result rows
        results = []
        for key, record in all_records:
            # Apply WHERE filter if present
            if stmt.where:
                if not self._evaluate_where(record, stmt.where, table_meta):
                    continue
            
            # Extract values
            values = record.get_values()
            
            # Filter columns if not SELECT *
            if stmt.columns != ['*'] and table_meta:
                filtered_values = []
                for col_name in stmt.columns:
                    for i, col_def in enumerate(table_meta.columns):
                        if col_def.name == col_name:
                            if i < len(values):
                                filtered_values.append(values[i])
                            break
                values = filtered_values
            
            results.append([Record(values)])
        
        # Apply DISTINCT
        if stmt.distinct:
            seen = set()
            unique_results = []
            for row in results:
                row_tuple = tuple(row[0].get_values())
                if row_tuple not in seen:
                    seen.add(row_tuple)
                    unique_results.append(row)
            results = unique_results
        
        # Apply ORDER BY
        if stmt.order_by:
            for col_name, direction in reversed(stmt.order_by):
                # Find column index
                col_index = None
                for i, col_def in enumerate(table_meta.columns):
                    if col_def.name == col_name:
                        col_index = i
                        break
                
                if col_index is not None:
                    results.sort(
                        key=lambda row: row[0].get_values()[col_index] if col_index < len(row[0].get_values()) else None,
                        reverse=(direction == 'DESC')
                    )
        
        # Apply OFFSET
        if stmt.offset:
            results = results[stmt.offset:]
        
        # Apply LIMIT
        if stmt.limit:
            results = results[:stmt.limit]
        
        return results
    
    def _execute_drop_table(self, stmt: DropTableStatement) -> List[List[Any]]:
        """
        Execute DROP TABLE statement.
        """
        table_name = stmt.table
        
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
        
        # Remove from metadata
        del self.table_metadata[table_name]
        del self.tables[table_name]
        
        # Update catalog
        self._save_all_metadata()
        
        self.logger.info(f"Dropped table '{table_name}'")
        return []
    
    def _execute_alter_table(self, stmt: AlterTableStatement) -> List[List[Any]]:
        """
        Execute ALTER TABLE statement.
        """
        table_name = stmt.table
        
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
        
        table_meta = self.table_metadata[table_name]
        
        if stmt.action == 'ADD' and stmt.column:
            # Add column to metadata
            table_meta.columns.append(stmt.column)
            
            # Update catalog
            self._save_all_metadata()
            
            self.logger.info(f"Added column '{stmt.column.name}' to table '{table_name}'")
        
        return []
    
    def close(self) -> None:
        """Close the database."""
        # Save all table metadata before closing
        self._save_all_metadata()
        
        self.pager.close()
        self.logger.info(f"Closed database '{self.filename}'")
    
    def _save_all_metadata(self) -> None:
        """Save all table metadata to catalog."""
        # Clear catalog and rewrite all metadata
        # This ensures auto-increment counters are saved
        
        # For simplicity, we'll just update existing entries
        # A full implementation would rebuild the catalog
        
        try:
            # Re-save each table's metadata
            catalog_records = self.catalog_btree.scan()
            
            # Delete all existing catalog entries
            for key, _ in catalog_records:
                self.catalog_btree.delete(key)
            
            # Re-insert all current metadata
            for i, (table_name, metadata) in enumerate(self.table_metadata.items()):
                metadata_dict = metadata.to_dict()
                json_data = json.dumps(metadata_dict)
                record = Record([json_data])
                self.catalog_btree.insert(i + 1, record)
            
            self.pager.flush()
            self.logger.info("Saved all table metadata to catalog")
        except Exception as e:
            self.logger.error(f"Error saving metadata: {e}")
    
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
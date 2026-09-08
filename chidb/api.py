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
from chidb.sql.parser import Parser, CreateTableStatement, InsertStatement, UpdateStatement, DeleteStatement, DropTableStatement, AlterTableStatement, CreateIndexStatement, DropIndexStatement, ColumnDef, SelectStatement, AggregateCall, JoinClause, BinaryOp, Literal, Identifier
from chidb.sql.optimizer import Optimizer
from chidb.sql.codegen import CodeGenerator
from chidb.log import get_logger
from chidb.security import (
    validate_database_path,
    validate_sql_length,
    validate_table_name,
    validate_column_name,
    check_table_count,
    check_column_count,
    sanitize_error_message,
    SecurityError,
    QueryError
)


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
            'kind': 'table',
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


@dataclass
class IndexMetadata:
    """Metadata about a secondary index: an in-memory value -> row-key map, rebuilt on load."""
    name: str
    table: str
    column: str

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {'kind': 'index', 'name': self.name, 'table': self.table, 'column': self.column}

    @staticmethod
    def from_dict(data: dict) -> 'IndexMetadata':
        """Create from dictionary."""
        return IndexMetadata(name=data['name'], table=data['table'], column=data['column'])


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
    
    def __init__(self, filename: str, debug_mode: bool = False):
        """
        Open or create a database.

        Args:
            filename: Path to the database file
            debug_mode: Enable debug mode with verbose errors (default: False)

        Raises:
            SecurityError: If path validation fails
        """
        # Validate and sanitize the file path
        self.filename = validate_database_path(filename)
        self.debug_mode = debug_mode
        self.pager = Pager(self.filename)
        self.dbm = DatabaseMachine(self.pager)
        self.codegen = CodeGenerator()
        self.optimizer = Optimizer()
        self.logger = get_logger("api")
        
        # Table metadata: maps table name -> TableMetadata
        self.table_metadata: Dict[str, TableMetadata] = {}

        # Legacy tables dict for backward compatibility
        self.tables: Dict[str, int] = {}

        # Secondary indexes: maps index name -> IndexMetadata (definition)
        # and index name -> {column_value: [btree_keys]} (in-memory data,
        # rebuilt from the table on load since it isn't itself persisted).
        self.indexes: Dict[str, IndexMetadata] = {}
        self._index_data: Dict[str, Dict[Any, List[int]]] = {}

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
        
        # Scan the catalog and load all table and index metadata
        try:
            catalog_records = self.catalog_btree.scan()

            for key, record in catalog_records:
                # Record contains JSON-serialized table or index metadata
                json_data = record.get_value(0)
                if not json_data:
                    continue

                metadata_dict = json.loads(json_data)

                if metadata_dict.get('kind') == 'index':
                    index_meta = IndexMetadata.from_dict(metadata_dict)
                    self.indexes[index_meta.name] = index_meta
                    self.logger.info(f"Loaded index '{index_meta.name}' from catalog")
                else:
                    metadata = TableMetadata.from_dict(metadata_dict)
                    self.table_metadata[metadata.name] = metadata
                    self.tables[metadata.name] = metadata.root_page
                    self.logger.info(f"Loaded table '{metadata.name}' from catalog")

            self._rebuild_all_indexes()
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

        Raises:
            SecurityError: If security validation fails
            ValueError: If SQL is invalid
        """
        try:
            # Validate SQL length to prevent resource exhaustion
            validate_sql_length(sql)
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

            # Handle CREATE INDEX / DROP INDEX
            if isinstance(ast, CreateIndexStatement):
                return self._execute_create_index(ast)
            if isinstance(ast, DropIndexStatement):
                return self._execute_drop_index(ast)
            
            # Handle SELECT with GROUP BY or aggregate functions
            if isinstance(ast, SelectStatement) and self._is_aggregate_select(ast):
                return self._execute_select_aggregate(ast)

            # Handle every other SELECT (WHERE/ORDER BY/LIMIT/OFFSET/DISTINCT,
            # or a bare SELECT *). All routed through the same Python-level
            # path — the codegen/DBM bytecode path never actually resolved
            # WHERE column values or projected specific columns correctly.
            if isinstance(ast, SelectStatement):
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
                            # CRITICAL FIX: Also update self.tables dict
                            self.tables[table_name] = btree.root_page
                            self._save_table_to_catalog(metadata)
                            metadata_changed = True
                            break

            # Clear the btrees cache to force fresh lookups with updated metadata
            # This ensures subsequent operations use the correct root pages
            if metadata_changed:
                self.dbm.btrees.clear()

            # This fallback path currently only handles INSERT (every other
            # statement is special-cased above), so keep any indexes on the
            # affected table in sync.
            if isinstance(ast, InsertStatement) and self.indexes:
                self._maintain_indexes_for_table(ast.table)

            return results

        except SecurityError:
            # Re-raise security errors as-is
            raise
        except Exception as e:
            # Sanitize error message in production mode
            self.logger.error(f"Error executing SQL: {e}")
            if not self.debug_mode:
                # Provide sanitized error message
                safe_message = sanitize_error_message(e, self.debug_mode)
                raise type(e)(safe_message) from None
            raise
    
    def _execute_create_table(self, stmt: CreateTableStatement) -> List[List[Any]]:
        """
        Execute CREATE TABLE statement.

        This creates a new B-tree for the table.
        """
        table_name = stmt.table

        # Security validations
        check_table_count(len(self.tables))
        validate_table_name(table_name)
        check_column_count(len(stmt.columns))

        # Validate all column names
        for col in stmt.columns:
            validate_column_name(col.name)

        if table_name in self.tables:
            raise QueryError(f"Table '{table_name}' already exists")
        
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
            raise QueryError(f"Table '{table_name}' does not exist")
        
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

        if updated_count and self.indexes:
            self._maintain_indexes_for_table(table_name)

        self.logger.info(f"Updated {updated_count} rows in '{table_name}'")
        return []
    
    def _execute_delete(self, stmt: DeleteStatement) -> List[List[Any]]:
        """
        Execute DELETE statement.
        
        Deletes records from the table.
        """
        table_name = stmt.table
        if table_name not in self.tables:
            raise QueryError(f"Table '{table_name}' does not exist")
        
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

        if keys_to_delete and self.indexes:
            self._maintain_indexes_for_table(table_name)

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
        Execute SELECT with ORDER BY, LIMIT, OFFSET, DISTINCT, or JOINs.
        """
        from chidb.record import Record

        if stmt.joins:
            return self._execute_select_with_joins(stmt)

        table_name = stmt.table
        if table_name not in self.tables:
            raise QueryError(f"Table '{table_name}' does not exist")
        
        root_page = self.tables[table_name]
        table_meta = self.table_metadata.get(table_name)
        btree = BTree(self.pager, root_page)

        # Use a secondary index for a simple `column = value` WHERE clause
        # when one is available, instead of a full table scan.
        index_match = self._find_index_for_where(table_name, stmt.where)
        if index_match:
            index_metadata, value = index_match
            keys = self._index_data.get(index_metadata.name, {}).get(value, [])
            all_records = [(key, btree.search(key)) for key in keys]
            all_records = [(key, record) for key, record in all_records if record is not None]
        else:
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

    def _execute_select_with_joins(self, stmt: SelectStatement) -> List[List[Any]]:
        """
        Execute a SELECT with one or more JOINs, via nested-loop join.

        Supports INNER and LEFT JOIN, qualified ('table.column') and
        unqualified column references (unqualified names must be
        unambiguous across the joined tables).
        """
        from chidb.record import Record

        table_order = [stmt.table] + [join.table for join in stmt.joins]
        table_metas: Dict[str, TableMetadata] = {}
        table_rows: Dict[str, List['Record']] = {}

        for table_name in table_order:
            if table_name not in self.tables:
                raise QueryError(f"Table '{table_name}' does not exist")
            table_metas[table_name] = self.table_metadata.get(table_name)
            btree = BTree(self.pager, self.tables[table_name])
            table_rows[table_name] = [record for _, record in btree.scan()]

        joined_rows: List[Dict[str, Optional['Record']]] = [
            {stmt.table: record} for record in table_rows[stmt.table]
        ]

        for join in stmt.joins:
            joined_rows = self._apply_join(join, joined_rows, table_rows[join.table], table_metas)

        if stmt.where:
            joined_rows = [
                row for row in joined_rows
                if self._evaluate_join_expression(stmt.where, row, table_metas)
            ]

        output_column_names = self._join_output_column_names(stmt.columns, table_order, table_metas)
        results = [
            [Record(self._project_join_row(stmt.columns, row, table_order, table_metas))]
            for row in joined_rows
        ]

        if stmt.distinct:
            seen = set()
            unique_results = []
            for row in results:
                row_tuple = tuple(row[0].get_values())
                if row_tuple not in seen:
                    seen.add(row_tuple)
                    unique_results.append(row)
            results = unique_results

        if stmt.order_by:
            for col_name, direction in reversed(stmt.order_by):
                index = self._find_join_output_index(col_name, output_column_names)
                results.sort(
                    key=lambda row: row[0].get_values()[index],
                    reverse=(direction == 'DESC')
                )

        if stmt.offset:
            results = results[stmt.offset:]
        if stmt.limit:
            results = results[:stmt.limit]

        return results

    def _apply_join(
        self, join: JoinClause, left_rows: List[Dict[str, Optional['Record']]],
        right_records: List['Record'], table_metas: Dict[str, TableMetadata]
    ) -> List[Dict[str, Optional['Record']]]:
        """Nested-loop join: match each left-side row against every candidate right-side record."""
        joined_rows: List[Dict[str, Optional['Record']]] = []

        for left_row in left_rows:
            matched = False
            for right_record in right_records:
                candidate = dict(left_row)
                candidate[join.table] = right_record
                if self._evaluate_join_expression(join.on, candidate, table_metas):
                    joined_rows.append(candidate)
                    matched = True

            if not matched and join.join_type == 'LEFT':
                unmatched = dict(left_row)
                unmatched[join.table] = None
                joined_rows.append(unmatched)

        return joined_rows

    def _evaluate_join_expression(
        self, expr: Any, joined_row: Dict[str, Optional['Record']], table_metas: Dict[str, TableMetadata]
    ) -> bool:
        """Evaluate a WHERE/ON expression (comparisons, AND, OR) against a joined row."""
        if isinstance(expr, BinaryOp):
            if expr.operator == 'AND':
                return (
                    self._evaluate_join_expression(expr.left, joined_row, table_metas)
                    and self._evaluate_join_expression(expr.right, joined_row, table_metas)
                )
            if expr.operator == 'OR':
                return (
                    self._evaluate_join_expression(expr.left, joined_row, table_metas)
                    or self._evaluate_join_expression(expr.right, joined_row, table_metas)
                )

            left_value = self._resolve_join_operand(expr.left, joined_row, table_metas)
            right_value = self._resolve_join_operand(expr.right, joined_row, table_metas)
            return self._compare(expr.operator, left_value, right_value)

        raise QueryError(f"Unsupported expression in JOIN condition: {expr}")

    def _resolve_join_operand(
        self, node: Any, joined_row: Dict[str, Optional['Record']], table_metas: Dict[str, TableMetadata]
    ) -> Any:
        """Resolve one side of a comparison: a column reference or a literal value."""
        if isinstance(node, Identifier):
            return self._resolve_join_column(node.name, joined_row, table_metas)
        if isinstance(node, Literal):
            return node.value
        raise QueryError(f"Unsupported operand in JOIN condition: {node}")

    def _compare(self, operator: str, left: Any, right: Any) -> bool:
        """Apply a comparison operator, treating any NULL operand as non-matching."""
        if left is None or right is None:
            return False
        if operator == '=':
            return left == right
        if operator == '!=':
            return left != right
        if operator == '<':
            return left < right
        if operator == '>':
            return left > right
        if operator == '<=':
            return left <= right
        if operator == '>=':
            return left >= right
        raise QueryError(f"Unsupported operator: {operator}")

    def _resolve_join_column(
        self, name: str, joined_row: Dict[str, Optional['Record']], table_metas: Dict[str, TableMetadata]
    ) -> Any:
        """Resolve a (optionally 'table.column'-qualified) column reference against a joined row."""
        if '.' in name:
            table_name, column_name = name.split('.', 1)
            if table_name not in joined_row:
                raise QueryError(f"Unknown table qualifier '{table_name}' in '{name}'")
            record = joined_row[table_name]
            if record is None:
                return None
            index = self._column_index(column_name, table_metas.get(table_name))
            return record.get_values()[index]

        matches = [
            table_name for table_name, meta in table_metas.items()
            if meta and any(col.name == name for col in meta.columns)
        ]
        if not matches:
            raise QueryError(f"Unknown column: {name}")
        if len(matches) > 1:
            raise QueryError(f"Ambiguous column '{name}' — qualify it, e.g. '{matches[0]}.{name}'")

        table_name = matches[0]
        record = joined_row[table_name]
        if record is None:
            return None
        index = self._column_index(name, table_metas[table_name])
        return record.get_values()[index]

    def _join_output_column_names(
        self, columns: List[str], table_order: List[str], table_metas: Dict[str, TableMetadata]
    ) -> List[str]:
        """Compute the qualified output column names, in projection order."""
        if columns == ['*']:
            return [
                f"{table_name}.{col.name}"
                for table_name in table_order
                for col in table_metas[table_name].columns
            ]
        return list(columns)

    def _project_join_row(
        self, columns: List[str], joined_row: Dict[str, Optional['Record']],
        table_order: List[str], table_metas: Dict[str, TableMetadata]
    ) -> List[Any]:
        """Build one flat output row from a joined row, per the SELECT column list."""
        if columns == ['*']:
            values = []
            for table_name in table_order:
                record = joined_row.get(table_name)
                meta = table_metas[table_name]
                if record is None:
                    values.extend([None] * len(meta.columns))
                else:
                    values.extend(record.get_values())
            return values

        return [self._resolve_join_column(col, joined_row, table_metas) for col in columns]

    def _find_join_output_index(self, name: str, output_column_names: List[str]) -> int:
        """Find a column's position in the joined output, resolving unqualified names if unambiguous."""
        if name in output_column_names:
            return output_column_names.index(name)

        candidates = [i for i, n in enumerate(output_column_names) if n.endswith(f".{name}")]
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            raise QueryError(f"Ambiguous column '{name}' — qualify it with a table name")
        raise QueryError(f"Unknown column '{name}' in ORDER BY")

    def _is_aggregate_select(self, stmt: SelectStatement) -> bool:
        """Check whether a SELECT statement uses GROUP BY or an aggregate function."""
        if stmt.group_by:
            return True
        return any(isinstance(column, AggregateCall) for column in stmt.columns)

    def _execute_select_aggregate(self, stmt: SelectStatement) -> List[List[Any]]:
        """
        Execute SELECT with GROUP BY and/or aggregate functions
        (COUNT, SUM, AVG, MIN, MAX).
        """
        if stmt.joins:
            raise QueryError("JOIN is not yet supported together with GROUP BY/aggregate functions")

        table_name = stmt.table
        if table_name not in self.tables:
            raise QueryError(f"Table '{table_name}' does not exist")

        table_meta = self.table_metadata.get(table_name)
        btree = BTree(self.pager, self.tables[table_name])

        matching_records = [
            record for _, record in btree.scan()
            if not stmt.where or self._evaluate_where(record, stmt.where, table_meta)
        ]

        groups = self._group_records(matching_records, stmt.group_by, table_meta)
        result_rows = [
            self._aggregate_group(stmt.columns, key_lookup, group_records, table_meta)
            for key_lookup, group_records in groups
        ]

        return self._finalize_aggregate_rows(result_rows, stmt)

    def _group_records(
        self, records: List['Record'], group_by: Optional[List[str]], table_meta: Optional[TableMetadata]
    ) -> List[tuple]:
        """Partition records into groups, returning (key_lookup, records) pairs in first-seen order."""
        if not group_by:
            return [({}, records)]

        column_indexes = {name: self._column_index(name, table_meta) for name in group_by}
        group_records: Dict[tuple, List['Record']] = {}
        group_order: List[tuple] = []

        for record in records:
            values = record.get_values()
            key = tuple(values[column_indexes[name]] for name in group_by)
            if key not in group_records:
                group_records[key] = []
                group_order.append(key)
            group_records[key].append(record)

        return [
            (dict(zip(group_by, key)), group_records[key])
            for key in group_order
        ]

    def _aggregate_group(
        self, columns: List[Any], key_lookup: Dict[str, Any],
        group_records: List['Record'], table_meta: Optional[TableMetadata]
    ) -> List[Any]:
        """Build one output row for a group: resolved GROUP BY values and computed aggregates."""
        row = []
        for column in columns:
            if isinstance(column, AggregateCall):
                row.append(self._compute_aggregate(column, group_records, table_meta))
            elif column in key_lookup:
                row.append(key_lookup[column])
            else:
                raise QueryError(
                    f"Column '{column}' must appear in GROUP BY or be an aggregate function"
                )
        return row

    def _compute_aggregate(
        self, call: AggregateCall, records: List['Record'], table_meta: Optional[TableMetadata]
    ) -> Any:
        """Compute a single aggregate function's value over a group of records."""
        if call.function == 'COUNT' and call.column == '*':
            return len(records)

        column_index = self._column_index(call.column, table_meta)
        non_null_values = [
            record.get_values()[column_index]
            for record in records
            if record.get_values()[column_index] is not None
        ]

        if call.function == 'COUNT':
            return len(non_null_values)
        if not non_null_values:
            return None
        if call.function == 'SUM':
            return sum(non_null_values)
        if call.function == 'AVG':
            return sum(non_null_values) / len(non_null_values)
        if call.function == 'MIN':
            return min(non_null_values)
        if call.function == 'MAX':
            return max(non_null_values)

        raise QueryError(f"Unsupported aggregate function: {call.function}")

    def _column_index(self, name: str, table_meta: Optional[TableMetadata]) -> int:
        """Find a column's position in the table schema."""
        if not table_meta:
            raise QueryError(f"No metadata for column '{name}'")
        for index, col_def in enumerate(table_meta.columns):
            if col_def.name == name:
                return index
        raise QueryError(f"Unknown column: {name}")

    def _finalize_aggregate_rows(self, rows: List[List[Any]], stmt: SelectStatement) -> List[List[Any]]:
        """Apply ORDER BY/OFFSET/LIMIT to aggregated rows and wrap them as Records."""
        if stmt.order_by:
            rows = self._sort_aggregate_rows(rows, stmt.columns, stmt.order_by)
        if stmt.offset:
            rows = rows[stmt.offset:]
        if stmt.limit:
            rows = rows[:stmt.limit]

        return [[Record(row)] for row in rows]

    def _sort_aggregate_rows(
        self, rows: List[List[Any]], columns: List[Any], order_by: List[tuple]
    ) -> List[List[Any]]:
        """Sort aggregated rows by GROUP BY column position (aggregate aliases are not supported)."""
        for col_name, direction in reversed(order_by):
            if col_name not in columns:
                continue
            position = columns.index(col_name)
            rows.sort(key=lambda row: row[position], reverse=(direction == 'DESC'))
        return rows

    def _execute_drop_table(self, stmt: DropTableStatement) -> List[List[Any]]:
        """
        Execute DROP TABLE statement.
        """
        table_name = stmt.table
        
        if table_name not in self.tables:
            raise QueryError(f"Table '{table_name}' does not exist")
        
        # Remove from metadata
        del self.table_metadata[table_name]
        del self.tables[table_name]

        # Cascade: drop any indexes defined on this table
        for index_name in [name for name, meta in self.indexes.items() if meta.table == table_name]:
            del self.indexes[index_name]
            self._index_data.pop(index_name, None)

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
            raise QueryError(f"Table '{table_name}' does not exist")
        
        table_meta = self.table_metadata[table_name]
        
        if stmt.action == 'ADD' and stmt.column:
            # Add column to metadata
            table_meta.columns.append(stmt.column)
            
            # Update catalog
            self._save_all_metadata()
            
            self.logger.info(f"Added column '{stmt.column.name}' to table '{table_name}'")

        return []

    def _execute_create_index(self, stmt: CreateIndexStatement) -> List[List[Any]]:
        """
        Execute CREATE INDEX statement.

        Builds an in-memory {value: [row_keys]} index over the table, used
        to accelerate equality WHERE lookups instead of a full table scan.
        """
        if stmt.index_name in self.indexes:
            raise QueryError(f"Index '{stmt.index_name}' already exists")

        if stmt.table not in self.tables:
            raise QueryError(f"Table '{stmt.table}' does not exist")

        table_meta = self.table_metadata[stmt.table]
        if not any(col.name == stmt.column for col in table_meta.columns):
            raise QueryError(f"Unknown column '{stmt.column}' on table '{stmt.table}'")

        metadata = IndexMetadata(name=stmt.index_name, table=stmt.table, column=stmt.column)
        self.indexes[stmt.index_name] = metadata
        self._build_index(metadata)
        self._save_all_metadata()

        self.logger.info(f"Created index '{stmt.index_name}' on {stmt.table}({stmt.column})")
        return []

    def _execute_drop_index(self, stmt: DropIndexStatement) -> List[List[Any]]:
        """Execute DROP INDEX statement."""
        if stmt.index_name not in self.indexes:
            raise QueryError(f"Index '{stmt.index_name}' does not exist")

        del self.indexes[stmt.index_name]
        self._index_data.pop(stmt.index_name, None)
        self._save_all_metadata()

        self.logger.info(f"Dropped index '{stmt.index_name}'")
        return []

    def _build_index(self, metadata: IndexMetadata) -> None:
        """(Re)build one index's in-memory value -> [row_keys] map by scanning its table."""
        btree = BTree(self.pager, self.tables[metadata.table])
        table_meta = self.table_metadata[metadata.table]
        column_index = self._column_index(metadata.column, table_meta)

        index_map: Dict[Any, List[int]] = {}
        for key, record in btree.scan():
            value = record.get_values()[column_index]
            index_map.setdefault(value, []).append(key)

        self._index_data[metadata.name] = index_map

    def _rebuild_all_indexes(self) -> None:
        """Rebuild every index's in-memory data, e.g. after loading the catalog."""
        for metadata in self.indexes.values():
            self._build_index(metadata)

    def _maintain_indexes_for_table(self, table_name: str) -> None:
        """Refresh every index defined on a table after its data changed."""
        for metadata in self.indexes.values():
            if metadata.table == table_name:
                self._build_index(metadata)

    def _find_index_for_where(
        self, table_name: str, where_expr: Optional[Any]
    ) -> Optional[tuple]:
        """
        If WHERE is a simple `column = literal` on an indexed column of this
        table, return (IndexMetadata, value); otherwise None.
        """
        if where_expr is None:
            return None
        if not isinstance(where_expr, BinaryOp) or where_expr.operator != '=':
            return None
        if not isinstance(where_expr.left, Identifier) or not isinstance(where_expr.right, Literal):
            return None

        for metadata in self.indexes.values():
            if metadata.table == table_name and metadata.column == where_expr.left.name:
                return metadata, where_expr.right.value

        return None

    def get_index_names(self) -> List[str]:
        """Get list of index names."""
        return list(self.indexes.keys())


    def close(self) -> None:
        """Close the database."""
        # Save all table metadata before closing
        self._save_all_metadata()
        
        self.pager.close()
        self.logger.info(f"Closed database '{self.filename}'")
    
    def _save_all_metadata(self) -> None:
        """Save all table and index metadata to catalog."""
        # Clear catalog and rewrite all metadata
        # This ensures auto-increment counters are saved

        try:
            # Re-save each table's/index's metadata
            catalog_records = self.catalog_btree.scan()

            # Delete all existing catalog entries
            for key, _ in catalog_records:
                self.catalog_btree.delete(key)

            # Re-insert all current metadata (tables, then indexes)
            entries = list(self.table_metadata.values()) + list(self.indexes.values())
            for i, metadata in enumerate(entries):
                json_data = json.dumps(metadata.to_dict())
                record = Record([json_data])
                self.catalog_btree.insert(i + 1, record)

            self.pager.flush()
            self.logger.info("Saved all table/index metadata to catalog")
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


def connect(filename: str = None, db_name: str = None, api_key: str = None, server_url: str = None):
    """
    Connect to a database — locally or in the cloud.

    Local mode (existing behavior):
        db = connect("my.db")

    Cloud mode:
        db = connect("myproject")           # auto-loads credentials
        db = connect(db_name="myproject")   # explicit
        db = connect(db_name="myproject", api_key="yesdb_...")

    Args:
        filename: Path to a local database file.
        db_name: Name of a cloud database.
        api_key: API key for cloud auth (optional, loaded from ~/.yesdb/ if omitted).
        server_url: Cloud server URL (optional, loaded from ~/.yesdb/ if omitted).

    Returns:
        YesDB instance (local) or CloudConnection instance (cloud).
    """
    if filename and db_name:
        raise ValueError("Specify either filename (local) or db_name (cloud), not both.")

    if filename:
        return YesDB(filename)

    if db_name:
        from chidb.client import CloudConnection
        return CloudConnection(db_name, api_key=api_key, server_url=server_url)

    raise ValueError("Must specify either filename (local) or db_name (cloud).")
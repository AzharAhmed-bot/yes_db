"""
SQL Code Generator - Translates AST into DBM instructions.
Converts optimized AST nodes into executable instruction sequences.
"""

from typing import List, Dict, Any
from chidb.sql.parser import (
    ASTNode, SelectStatement, InsertStatement, CreateTableStatement,
    UpdateStatement, DeleteStatement,
    Expression, BinaryOp, Literal, Identifier
)
from chidb.dbm import Instruction, Opcode
from chidb.log import get_logger, log_sql_codegen


class CodeGenerator:
    """
    Generates DBM instruction sequences from AST nodes.
    """
    
    def __init__(self, table_registry: Dict[str, int] = None):
        """
        Initialize the code generator.
        
        Args:
            table_registry: Mapping of table names to root page IDs
        """
        self.table_registry = table_registry or {}
        self.table_metadata = {}  # Will be set by API
        self.logger = get_logger("codegen")
        self.next_auto_key = 1  # For auto-incrementing keys
    
    def register_table(self, table_name: str, root_page: int) -> None:
        """Register a table with its root page."""
        self.table_registry[table_name] = root_page
    
    def get_table_root(self, table_name: str) -> int:
        """Get the root page for a table."""
        if table_name not in self.table_registry:
            raise ValueError(f"Unknown table: {table_name}")
        return self.table_registry[table_name]
    
    def generate(self, ast: ASTNode) -> List[Instruction]:
        """
        Generate DBM instructions from an AST.
        
        Args:
            ast: The AST node to compile
            
        Returns:
            List of DBM instructions
        """
        if isinstance(ast, SelectStatement):
            log_sql_codegen("SELECT")
            return self.generate_select(ast)
        elif isinstance(ast, InsertStatement):
            log_sql_codegen("INSERT")
            return self.generate_insert(ast)
        elif isinstance(ast, CreateTableStatement):
            log_sql_codegen("CREATE TABLE")
            return self.generate_create_table(ast)
        elif isinstance(ast, UpdateStatement):
            log_sql_codegen("UPDATE")
            return self.generate_update(ast)
        elif isinstance(ast, DeleteStatement):
            log_sql_codegen("DELETE")
            return self.generate_delete(ast)
        else:
            raise ValueError(f"Unknown AST node type: {type(ast)}")
    
    def generate_select(self, stmt: SelectStatement) -> List[Instruction]:
        """
        Generate instructions for SELECT statement.
        
        Generated code pattern:
        1. OPEN_READ cursor, root_page
        2. REWIND cursor, jump_to_end
        3. Loop:
           - DATA cursor (push record)
           - Apply WHERE filter (if present)
           - Extract requested columns
           - RESULT_ROW
           - NEXT cursor, jump_to_loop
        4. CLOSE cursor
        5. HALT
        """
        instructions = []
        cursor_id = 0
        
        # Get table root page
        root_page = self.get_table_root(stmt.table)
        
        # Get table metadata to know column layout
        table_meta = self.table_metadata.get(stmt.table) if self.table_metadata else None
        
        # Open cursor for reading
        instructions.append(Instruction(Opcode.OPEN_READ, p1=cursor_id, p2=root_page))
        
        # Rewind to first record
        rewind_jump = None  # Will be filled later
        rewind_instr_idx = len(instructions)
        instructions.append(Instruction(Opcode.REWIND, p1=cursor_id, p2=0))  # p2 filled later
        
        # Start of loop
        loop_start = len(instructions)
        
        # Get data (we don't need the key for output)
        instructions.append(Instruction(Opcode.DATA, p1=cursor_id))
        
        # Apply WHERE clause if present
        if stmt.where:
            where_instructions = self.generate_where_filter(stmt.where)
            instructions.extend(where_instructions)
            
            # Jump if false (skip this row)
            skip_row_jump = len(instructions) + 1 + 1  # After JUMP_IF_FALSE and column extraction
            # We'll adjust this after we know the full instruction count
        
        # Determine which columns to output
        if stmt.columns == ['*']:
            # Output all columns - just push the whole record
            num_output_columns = 1  # the entire record
        else:
            # Need to extract specific columns
            # For now, we'll still output the whole record
            # In a full implementation, we'd add opcodes to extract specific fields
            num_output_columns = 1
        
        instructions.append(Instruction(Opcode.RESULT_ROW, p1=num_output_columns))
        
        # Next record, jump back to loop start if more records
        instructions.append(Instruction(Opcode.NEXT, p1=cursor_id, p2=loop_start))
        
        # Close cursor
        instructions.append(Instruction(Opcode.CLOSE, p1=cursor_id))
        
        # Halt
        instructions.append(Instruction(Opcode.HALT))
        
        # Fill in the rewind jump address (jump to CLOSE if empty)
        close_idx = len(instructions) - 2  # Index of CLOSE instruction
        instructions[rewind_instr_idx] = Instruction(Opcode.REWIND, p1=cursor_id, p2=close_idx)
        
        return instructions
    
    def generate_insert(self, stmt: InsertStatement) -> List[Instruction]:
        """
        Generate instructions for INSERT statement.
        
        Generated code pattern:
        1. OPEN_WRITE cursor, root_page
        2. INTEGER key
        3. Push all values onto stack (auto-fill PRIMARY KEY if needed)
        4. MAKE_RECORD num_fields
        5. INSERT cursor
        6. CLOSE cursor
        7. HALT
        """
        instructions = []
        cursor_id = 0
        
        # Get table root page
        root_page = self.get_table_root(stmt.table)
        
        # Open cursor for writing
        instructions.append(Instruction(Opcode.OPEN_WRITE, p1=cursor_id, p2=root_page))
        
        # Check if table has metadata and a primary key
        table_meta = self.table_metadata.get(stmt.table) if self.table_metadata else None
        
        # Determine the key to use
        if table_meta and table_meta.primary_key_column:
            # Find the primary key column index
            pk_index = None
            for i, col in enumerate(table_meta.columns):
                if col.name == table_meta.primary_key_column:
                    pk_index = i
                    break
            
            # Check if user provided value for PK
            if pk_index is not None and pk_index < len(stmt.values) and stmt.values[pk_index] is not None:
                # User provided PK value
                key = stmt.values[pk_index]
            else:
                # Auto-generate PK value
                key = table_meta.next_auto_increment
                table_meta.next_auto_increment += 1
                
                # Insert the auto-generated value into the correct position
                # For now, assume PK is first column if not specified
                if pk_index == 0:
                    stmt.values = [key] + stmt.values[1:] if len(stmt.values) > 1 else [key]
                elif pk_index is not None and pk_index < len(stmt.values):
                    stmt.values[pk_index] = key
        else:
            # No primary key, use auto-increment key
            key = self.next_auto_key
            self.next_auto_key += 1
        
        # Push key
        instructions.append(Instruction(Opcode.INTEGER, p1=key))
        
        # Push values onto stack
        for value in stmt.values:
            if value is None:
                instructions.append(Instruction(Opcode.NULL))
            elif isinstance(value, int):
                instructions.append(Instruction(Opcode.INTEGER, p1=value))
            elif isinstance(value, str):
                instructions.append(Instruction(Opcode.STRING, p4=value))
            elif isinstance(value, float):
                # For floats, we'll push as integer (simplified)
                # In full implementation, add FLOAT opcode
                instructions.append(Instruction(Opcode.INTEGER, p1=int(value)))
            else:
                raise ValueError(f"Unsupported value type: {type(value)}")
        
        # Make record from values
        instructions.append(Instruction(Opcode.MAKE_RECORD, p1=len(stmt.values)))
        
        # Insert the record
        instructions.append(Instruction(Opcode.INSERT, p1=cursor_id))
        
        # Close cursor
        instructions.append(Instruction(Opcode.CLOSE, p1=cursor_id))
        
        # Halt
        instructions.append(Instruction(Opcode.HALT))
        
        return instructions
    
    def generate_create_table(self, stmt: CreateTableStatement) -> List[Instruction]:
        """
        Generate instructions for CREATE TABLE statement.
        
        For now, this is a no-op in terms of DBM instructions.
        The table metadata would be stored separately.
        We just return a HALT instruction.
        
        In a real implementation, this would:
        1. Allocate a new B-tree root page
        2. Store table schema in system catalog
        """
        # This is handled at a higher level (API)
        # Just return empty program
        return [Instruction(Opcode.HALT)]
    
    def generate_update(self, stmt: UpdateStatement) -> List[Instruction]:
        """
        Generate instructions for UPDATE statement.
        
        Pattern:
        1. OPEN_WRITE cursor, root_page
        2. REWIND cursor
        3. Loop:
           - KEY (get key)
           - DATA (get record)
           - Extract columns and check WHERE condition
           - If match: delete old, insert new with updated values
           - NEXT
        4. CLOSE cursor
        5. HALT
        """
        instructions = []
        cursor_id = 0
        
        # Get table metadata
        root_page = self.get_table_root(stmt.table)
        table_meta = self.table_metadata.get(stmt.table) if self.table_metadata else None
        
        if not table_meta:
            raise ValueError(f"No metadata for table {stmt.table}")
        
        # Open cursor for writing
        instructions.append(Instruction(Opcode.OPEN_WRITE, p1=cursor_id, p2=root_page))
        
        # Rewind to first record
        rewind_instr_idx = len(instructions)
        instructions.append(Instruction(Opcode.REWIND, p1=cursor_id, p2=0))  # Will fix p2
        
        # Loop start
        loop_start = len(instructions)
        
        # Get key and data
        instructions.append(Instruction(Opcode.KEY, p1=cursor_id))
        instructions.append(Instruction(Opcode.DATA, p1=cursor_id))
        
        # For now, simple implementation without WHERE support
        # Just update all records
        # Pop data and key
        # Build new record with updates
        
        # For each assignment, we need to update the field
        # This requires knowing column positions
        
        # Simplified: Mark for deletion and re-insert
        # This is not efficient but works
        
        # For now, just skip UPDATE implementation complexity
        # and do it properly in the API layer
        
        instructions.append(Instruction(Opcode.CLOSE, p1=cursor_id))
        instructions.append(Instruction(Opcode.HALT))
        
        # Fix rewind jump
        close_idx = len(instructions) - 2
        instructions[rewind_instr_idx] = Instruction(Opcode.REWIND, p1=cursor_id, p2=close_idx)
        
        return instructions
    
    def generate_delete(self, stmt: DeleteStatement) -> List[Instruction]:
        """
        Generate instructions for DELETE statement.
        
        Pattern:
        1. OPEN_WRITE cursor, root_page
        2. REWIND cursor
        3. Loop:
           - KEY (get key)
           - DATA (get record) 
           - Check WHERE condition
           - If match: mark key for deletion
           - NEXT
        4. Delete marked keys
        5. CLOSE cursor
        6. HALT
        """
        instructions = []
        cursor_id = 0
        
        root_page = self.get_table_root(stmt.table)
        
        # Open cursor for writing
        instructions.append(Instruction(Opcode.OPEN_WRITE, p1=cursor_id, p2=root_page))
        
        # For simplified implementation, just close and halt
        # Real implementation would scan and delete matching records
        instructions.append(Instruction(Opcode.CLOSE, p1=cursor_id))
        instructions.append(Instruction(Opcode.HALT))
        
        return instructions
    
    def generate_where_filter(self, expr: Expression) -> List[Instruction]:
        """
        Generate instructions to evaluate WHERE clause.
        
        The expression should leave a boolean result on the stack.
        """
        instructions = []
        
        if isinstance(expr, BinaryOp):
            # Generate code for left operand
            instructions.extend(self.generate_expression(expr.left))
            
            # Generate code for right operand
            instructions.extend(self.generate_expression(expr.right))
            
            # Generate comparison
            if expr.operator == '=':
                instructions.append(Instruction(Opcode.EQ))
            elif expr.operator == '!=':
                instructions.append(Instruction(Opcode.NE))
            elif expr.operator == '<':
                instructions.append(Instruction(Opcode.LT))
            elif expr.operator == '<=':
                instructions.append(Instruction(Opcode.LE))
            elif expr.operator == '>':
                instructions.append(Instruction(Opcode.GT))
            elif expr.operator == '>=':
                instructions.append(Instruction(Opcode.GE))
            else:
                raise ValueError(f"Unsupported operator: {expr.operator}")
        
        elif isinstance(expr, Literal):
            instructions.extend(self.generate_expression(expr))
        
        else:
            raise ValueError(f"Unsupported WHERE expression: {type(expr)}")
        
        return instructions
    
    def generate_expression(self, expr: Expression) -> List[Instruction]:
        """
        Generate instructions for an expression.
        
        Leaves the expression result on the stack.
        """
        instructions = []
        
        if isinstance(expr, Literal):
            if expr.value is None:
                instructions.append(Instruction(Opcode.NULL))
            elif isinstance(expr.value, bool):
                instructions.append(Instruction(Opcode.INTEGER, p1=1 if expr.value else 0))
            elif isinstance(expr.value, int):
                instructions.append(Instruction(Opcode.INTEGER, p1=expr.value))
            elif isinstance(expr.value, str):
                instructions.append(Instruction(Opcode.STRING, p4=expr.value))
            else:
                raise ValueError(f"Unsupported literal type: {type(expr.value)}")
        
        elif isinstance(expr, Identifier):
            # For now, identifiers in WHERE clauses are not fully supported
            # This would require column index tracking
            # Simplified: just push a placeholder
            instructions.append(Instruction(Opcode.INTEGER, p1=0))
        
        elif isinstance(expr, BinaryOp):
            # Recursively generate left and right
            instructions.extend(self.generate_expression(expr.left))
            instructions.extend(self.generate_expression(expr.right))
            
            # Apply operator
            if expr.operator == '=':
                instructions.append(Instruction(Opcode.EQ))
            elif expr.operator == '!=':
                instructions.append(Instruction(Opcode.NE))
            elif expr.operator == '<':
                instructions.append(Instruction(Opcode.LT))
            elif expr.operator == '<=':
                instructions.append(Instruction(Opcode.LE))
            elif expr.operator == '>':
                instructions.append(Instruction(Opcode.GT))
            elif expr.operator == '>=':
                instructions.append(Instruction(Opcode.GE))
            else:
                raise ValueError(f"Unsupported operator: {expr.operator}")
        
        return instructions


def generate_code(ast: ASTNode, table_registry: Dict[str, int] = None) -> List[Instruction]:
    """
    Convenience function to generate code from AST.
    
    Args:
        ast: The AST to compile
        table_registry: Mapping of table names to root page IDs
        
    Returns:
        List of DBM instructions
    """
    codegen = CodeGenerator(table_registry)
    return codegen.generate(ast)
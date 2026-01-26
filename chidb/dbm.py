"""
Database Machine (DBM) - Virtual machine for executing database operations.
The DBM executes a sequence of low-level instructions to perform database tasks.
"""

from typing import List, Any, Optional, Dict
from dataclasses import dataclass
from enum import IntEnum
from chidb.pager import Pager
from chidb.btree import BTree
from chidb.record import Record
from chidb.log import get_logger, log_dbm_instruction


class Opcode(IntEnum):
    """Operation codes for DBM instructions."""
    OPEN_READ = 1      # Open a table for reading
    OPEN_WRITE = 2     # Open a table for writing
    CLOSE = 3          # Close a cursor
    REWIND = 4         # Move cursor to first record
    NEXT = 5           # Move cursor to next record
    KEY = 6            # Get current key
    DATA = 7           # Get current record data
    INSERT = 8         # Insert a record
    HALT = 9           # Stop execution
    RESULT_ROW = 10    # Output a result row
    INTEGER = 11       # Push integer constant onto stack
    STRING = 12        # Push string constant onto stack
    NULL = 13          # Push null onto stack
    MAKE_RECORD = 14   # Create record from stack values
    EQ = 15            # Compare equal
    NE = 16            # Compare not equal
    LT = 17            # Compare less than
    LE = 18            # Compare less than or equal
    GT = 19            # Compare greater than
    GE = 20            # Compare greater than or equal
    JUMP = 21          # Unconditional jump
    JUMP_IF_FALSE = 22 # Jump if top of stack is false
    SEEK = 23          # Seek to specific key
    DELETE = 24        # Delete current record
    COLUMN = 25        # Extract column from record


@dataclass
class Instruction:
    """Represents a single DBM instruction."""
    opcode: Opcode
    p1: int = 0        # First parameter
    p2: int = 0        # Second parameter
    p3: int = 0        # Third parameter
    p4: Any = None     # Fourth parameter (usually string/data)
    
    def __repr__(self) -> str:
        if self.p4 is not None:
            return f"{Opcode(self.opcode).name}({self.p1}, {self.p2}, {self.p3}, {self.p4!r})"
        elif self.p3 != 0:
            return f"{Opcode(self.opcode).name}({self.p1}, {self.p2}, {self.p3})"
        elif self.p2 != 0:
            return f"{Opcode(self.opcode).name}({self.p1}, {self.p2})"
        elif self.p1 != 0:
            return f"{Opcode(self.opcode).name}({self.p1})"
        else:
            return f"{Opcode(self.opcode).name}()"


class Cursor:
    """
    Represents a cursor for iterating over a B-tree.
    """
    
    def __init__(self, btree: BTree, writable: bool = False):
        self.btree = btree
        self.writable = writable
        self.data: List[tuple] = []  # (key, record) pairs
        self.position = -1
        self.valid = False
    
    def rewind(self) -> None:
        """Move cursor to the beginning."""
        self.data = self.btree.scan()
        self.position = -1
        self.valid = False
        if self.data:
            self.position = 0
            self.valid = True
    
    def next(self) -> bool:
        """
        Move to the next record.
        
        Returns:
            True if moved successfully, False if at end
        """
        if not self.valid:
            return False
        
        self.position += 1
        if self.position >= len(self.data):
            self.valid = False
            return False
        
        return True
    
    def get_key(self) -> Optional[int]:
        """Get the current key."""
        if not self.valid or self.position < 0 or self.position >= len(self.data):
            return None
        return self.data[self.position][0]
    
    def get_data(self) -> Optional[Record]:
        """Get the current record."""
        if not self.valid or self.position < 0 or self.position >= len(self.data):
            return None
        return self.data[self.position][1]
    
    def seek(self, key: int) -> bool:
        """
        Seek to a specific key.
        
        Returns:
            True if key found, False otherwise
        """
        if not self.data:
            self.data = self.btree.scan()
        
        for i, (k, record) in enumerate(self.data):
            if k == key:
                self.position = i
                self.valid = True
                return True
        
        self.valid = False
        return False
    
    def is_valid(self) -> bool:
        """Check if cursor is pointing to a valid record."""
        return self.valid


class DatabaseMachine:
    """
    Virtual machine that executes database programs.
    
    A program is a list of instructions that perform database operations.
    """
    
    def __init__(self, pager: Pager):
        """
        Initialize the database machine.
        
        Args:
            pager: The pager for disk I/O
        """
        self.pager = pager
        self.logger = get_logger("dbm")
        
        # Execution state
        self.cursors: Dict[int, Cursor] = {}
        self.btrees: Dict[int, BTree] = {}
        self.stack: List[Any] = []
        self.result_rows: List[List[Any]] = []
        self.pc = 0  # Program counter
        self.halted = False
    
    def execute(self, program: List[Instruction]) -> List[List[Any]]:
        """
        Execute a database program.
        
        Args:
            program: List of instructions to execute
            
        Returns:
            List of result rows
        """
        self.reset()
        
        while self.pc < len(program) and not self.halted:
            instruction = program[self.pc]
            log_dbm_instruction(str(instruction))
            
            self._execute_instruction(instruction)
            
            if not self.halted:
                self.pc += 1
        
        return self.result_rows
    
    def reset(self) -> None:
        """Reset the machine state."""
        self.cursors.clear()
        self.btrees.clear()
        self.stack.clear()
        self.result_rows.clear()
        self.pc = 0
        self.halted = False
    
    def _execute_instruction(self, instr: Instruction) -> None:
        """Execute a single instruction."""
        opcode = instr.opcode
        
        if opcode == Opcode.OPEN_READ:
            self._op_open_read(instr.p1, instr.p2)
        
        elif opcode == Opcode.OPEN_WRITE:
            self._op_open_write(instr.p1, instr.p2)
        
        elif opcode == Opcode.CLOSE:
            self._op_close(instr.p1)
        
        elif opcode == Opcode.REWIND:
            self._op_rewind(instr.p1, instr.p2)
        
        elif opcode == Opcode.NEXT:
            self._op_next(instr.p1, instr.p2)
        
        elif opcode == Opcode.KEY:
            self._op_key(instr.p1)
        
        elif opcode == Opcode.DATA:
            self._op_data(instr.p1)
        
        elif opcode == Opcode.INSERT:
            self._op_insert(instr.p1)
        
        elif opcode == Opcode.HALT:
            self._op_halt()
        
        elif opcode == Opcode.RESULT_ROW:
            self._op_result_row(instr.p1)
        
        elif opcode == Opcode.INTEGER:
            self._op_integer(instr.p1)
        
        elif opcode == Opcode.STRING:
            self._op_string(instr.p4)
        
        elif opcode == Opcode.NULL:
            self._op_null()
        
        elif opcode == Opcode.MAKE_RECORD:
            self._op_make_record(instr.p1)
        
        elif opcode == Opcode.SEEK:
            self._op_seek(instr.p1, instr.p2)
        
        elif opcode == Opcode.JUMP:
            self._op_jump(instr.p1)
        
        elif opcode == Opcode.JUMP_IF_FALSE:
            self._op_jump_if_false(instr.p1)
        
        elif opcode == Opcode.DELETE:
            self._op_delete(instr.p1)
        
        elif opcode == Opcode.COLUMN:
            self._op_column(instr.p1, instr.p2)
        
        elif opcode in (Opcode.EQ, Opcode.NE, Opcode.LT, Opcode.LE, Opcode.GT, Opcode.GE):
            self._op_compare(opcode)
        
        else:
            raise ValueError(f"Unknown opcode: {opcode}")
    
    def _op_open_read(self, cursor_id: int, root_page: int) -> None:
        """Open a table for reading."""
        if root_page not in self.btrees:
            self.btrees[root_page] = BTree(self.pager, root_page)
        
        btree = self.btrees[root_page]
        self.cursors[cursor_id] = Cursor(btree, writable=False)
    
    def _op_open_write(self, cursor_id: int, root_page: int) -> None:
        """Open a table for writing."""
        if root_page not in self.btrees:
            self.btrees[root_page] = BTree(self.pager, root_page)
        
        btree = self.btrees[root_page]
        self.cursors[cursor_id] = Cursor(btree, writable=True)
    
    def _op_close(self, cursor_id: int) -> None:
        """Close a cursor."""
        if cursor_id in self.cursors:
            del self.cursors[cursor_id]
    
    def _op_rewind(self, cursor_id: int, jump_addr: int) -> None:
        """Move cursor to first record, jump if empty."""
        cursor = self.cursors[cursor_id]
        cursor.rewind()
        
        if not cursor.is_valid():
            self.pc = jump_addr - 1  # -1 because pc will be incremented
    
    def _op_next(self, cursor_id: int, jump_addr: int) -> None:
        """Move to next record, jump to addr if moved successfully."""
        cursor = self.cursors[cursor_id]
        if cursor.next():
            self.pc = jump_addr - 1  # -1 because pc will be incremented
    
    def _op_key(self, cursor_id: int) -> None:
        """Push current key onto stack."""
        cursor = self.cursors[cursor_id]
        key = cursor.get_key()
        self.stack.append(key)
    
    def _op_data(self, cursor_id: int) -> None:
        """Push current record onto stack."""
        cursor = self.cursors[cursor_id]
        record = cursor.get_data()
        self.stack.append(record)
    
    def _op_insert(self, cursor_id: int) -> None:
        """Insert record. Stack: [key, record]"""
        if len(self.stack) < 2:
            raise RuntimeError("INSERT requires key and record on stack")
        
        record = self.stack.pop()
        key = self.stack.pop()
        
        cursor = self.cursors[cursor_id]
        if not cursor.writable:
            raise RuntimeError("Cannot insert into read-only cursor")
        
        cursor.btree.insert(key, record)
    
    def _op_halt(self) -> None:
        """Stop execution."""
        self.halted = True
    
    def _op_result_row(self, num_columns: int) -> None:
        """Output a result row. Pops num_columns values from stack."""
        if len(self.stack) < num_columns:
            raise RuntimeError(f"RESULT_ROW requires {num_columns} values on stack")
        
        row = []
        for _ in range(num_columns):
            row.append(self.stack.pop())
        
        row.reverse()  # We popped in reverse order
        self.result_rows.append(row)
    
    def _op_integer(self, value: int) -> None:
        """Push integer constant onto stack."""
        self.stack.append(value)
    
    def _op_string(self, value: str) -> None:
        """Push string constant onto stack."""
        self.stack.append(value)
    
    def _op_null(self) -> None:
        """Push null onto stack."""
        self.stack.append(None)
    
    def _op_make_record(self, num_fields: int) -> None:
        """Create a record from top num_fields stack values."""
        if len(self.stack) < num_fields:
            raise RuntimeError(f"MAKE_RECORD requires {num_fields} values on stack")
        
        values = []
        for _ in range(num_fields):
            values.append(self.stack.pop())
        
        values.reverse()
        record = Record(values)
        self.stack.append(record)
    
    def _op_seek(self, cursor_id: int, key: int) -> None:
        """Seek cursor to specific key."""
        cursor = self.cursors[cursor_id]
        cursor.seek(key)
    
    def _op_jump(self, addr: int) -> None:
        """Unconditional jump."""
        self.pc = addr - 1  # -1 because pc will be incremented
    
    def _op_jump_if_false(self, addr: int) -> None:
        """Jump if top of stack is false."""
        if not self.stack:
            raise RuntimeError("JUMP_IF_FALSE requires value on stack")
        
        value = self.stack.pop()
        if not value:
            self.pc = addr - 1
    
    def _op_compare(self, opcode: Opcode) -> None:
        """Compare two values on stack and push result."""
        if len(self.stack) < 2:
            raise RuntimeError("Comparison requires 2 values on stack")
        
        right = self.stack.pop()
        left = self.stack.pop()
        
        if opcode == Opcode.EQ:
            result = left == right
        elif opcode == Opcode.NE:
            result = left != right
        elif opcode == Opcode.LT:
            result = left < right
        elif opcode == Opcode.LE:
            result = left <= right
        elif opcode == Opcode.GT:
            result = left > right
        elif opcode == Opcode.GE:
            result = left >= right
        else:
            raise ValueError(f"Invalid comparison opcode: {opcode}")
        
        self.stack.append(result)
    
    def _op_delete(self, cursor_id: int) -> None:
        """
        Delete current record from cursor.
        Note: This is a simplified implementation.
        """
        cursor = self.cursors[cursor_id]
        if not cursor.writable:
            raise RuntimeError("Cannot delete from read-only cursor")
        
        if not cursor.is_valid():
            raise RuntimeError("Cursor not pointing to valid record")
        
        # Get current key
        key = cursor.get_key()
        
        # For simplicity, we'll mark it as deleted by reloading data
        # In a real implementation, we'd remove from B-tree
        # This is a placeholder - actual deletion would require B-tree delete
        cursor.valid = False
    
    def _op_column(self, cursor_id: int, column_index: int) -> None:
        """
        Extract a column value from the current record.
        
        Args:
            cursor_id: The cursor ID
            column_index: Index of the column to extract
        """
        cursor = self.cursors[cursor_id]
        record = cursor.get_data()
        
        if record is None:
            self.stack.append(None)
        else:
            values = record.get_values()
            if column_index < len(values):
                self.stack.append(values[column_index])
            else:
                self.stack.append(None)
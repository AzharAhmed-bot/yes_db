"""
Tests for chidb/dbm.py
"""

import pytest
import tempfile
import os
from chidb.pager import Pager
from chidb.btree import BTree
from chidb.record import Record
from chidb.dbm import DatabaseMachine, Instruction, Opcode, Cursor


@pytest.fixture
def temp_db():
    """Create a temporary database."""
    fd, path = tempfile.mkstemp(suffix='.cdb')
    os.close(fd)
    pager = Pager(path)
    yield pager
    pager.close()
    if os.path.exists(path):
        os.unlink(path)


class TestInstruction:
    """Test Instruction class."""
    
    def test_create_instruction(self):
        instr = Instruction(Opcode.HALT)
        assert instr.opcode == Opcode.HALT
        assert instr.p1 == 0
        assert instr.p2 == 0
        assert instr.p3 == 0
        assert instr.p4 is None
    
    def test_instruction_with_parameters(self):
        instr = Instruction(Opcode.OPEN_READ, p1=1, p2=2)
        assert instr.opcode == Opcode.OPEN_READ
        assert instr.p1 == 1
        assert instr.p2 == 2
    
    def test_instruction_repr(self):
        instr = Instruction(Opcode.HALT)
        assert "HALT" in repr(instr)


class TestCursor:
    """Test Cursor class."""
    
    def test_create_cursor(self, temp_db):
        btree = BTree(temp_db)
        cursor = Cursor(btree)
        
        assert cursor.btree is btree
        assert not cursor.writable
        assert not cursor.is_valid()
    
    def test_cursor_rewind_empty(self, temp_db):
        btree = BTree(temp_db)
        cursor = Cursor(btree)
        
        cursor.rewind()
        assert not cursor.is_valid()
    
    def test_cursor_rewind_with_data(self, temp_db):
        btree = BTree(temp_db)
        btree.insert(1, Record([100]))
        btree.insert(2, Record([200]))
        
        cursor = Cursor(btree)
        cursor.rewind()
        
        assert cursor.is_valid()
        assert cursor.get_key() == 1
    
    def test_cursor_next(self, temp_db):
        btree = BTree(temp_db)
        btree.insert(1, Record([100]))
        btree.insert(2, Record([200]))
        
        cursor = Cursor(btree)
        cursor.rewind()
        
        assert cursor.get_key() == 1
        
        cursor.next()
        assert cursor.get_key() == 2
        
        cursor.next()
        assert not cursor.is_valid()
    
    def test_cursor_seek(self, temp_db):
        btree = BTree(temp_db)
        btree.insert(1, Record([100]))
        btree.insert(5, Record([500]))
        btree.insert(10, Record([1000]))
        
        cursor = Cursor(btree)
        
        assert cursor.seek(5)
        assert cursor.get_key() == 5
        
        assert not cursor.seek(99)
        assert not cursor.is_valid()


class TestDatabaseMachine:
    """Test DatabaseMachine execution."""
    
    def test_create_dbm(self, temp_db):
        dbm = DatabaseMachine(temp_db)
        assert dbm.pager is temp_db
        assert len(dbm.cursors) == 0
        assert len(dbm.stack) == 0
    
    def test_halt_instruction(self, temp_db):
        dbm = DatabaseMachine(temp_db)
        program = [
            Instruction(Opcode.HALT)
        ]
        
        results = dbm.execute(program)
        assert results == []
        assert dbm.halted
    
    def test_integer_instruction(self, temp_db):
        dbm = DatabaseMachine(temp_db)
        program = [
            Instruction(Opcode.INTEGER, p1=42),
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        assert len(dbm.stack) == 1
        assert dbm.stack[0] == 42
    
    def test_string_instruction(self, temp_db):
        dbm = DatabaseMachine(temp_db)
        program = [
            Instruction(Opcode.STRING, p4="hello"),
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        assert len(dbm.stack) == 1
        assert dbm.stack[0] == "hello"
    
    def test_null_instruction(self, temp_db):
        dbm = DatabaseMachine(temp_db)
        program = [
            Instruction(Opcode.NULL),
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        assert len(dbm.stack) == 1
        assert dbm.stack[0] is None
    
    def test_make_record_instruction(self, temp_db):
        dbm = DatabaseMachine(temp_db)
        program = [
            Instruction(Opcode.INTEGER, p1=42),
            Instruction(Opcode.STRING, p4="test"),
            Instruction(Opcode.MAKE_RECORD, p1=2),
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        assert len(dbm.stack) == 1
        record = dbm.stack[0]
        assert isinstance(record, Record)
        assert record.get_value(0) == 42
        assert record.get_value(1) == "test"


class TestDatabaseMachineTableOperations:
    """Test table operations."""
    
    def test_open_and_close_cursor(self, temp_db):
        # Create a table
        btree = BTree(temp_db)
        root_page = btree.get_root_page()
        
        dbm = DatabaseMachine(temp_db)
        program = [
            Instruction(Opcode.OPEN_READ, p1=0, p2=root_page),
            Instruction(Opcode.CLOSE, p1=0),
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        assert 0 not in dbm.cursors
    
    def test_insert_and_scan(self, temp_db):
        # Create a table
        btree = BTree(temp_db)
        root_page = btree.get_root_page()
        
        dbm = DatabaseMachine(temp_db)
        
        # Insert a record: key=1, record=[42, "test"]
        program = [
            Instruction(Opcode.OPEN_WRITE, p1=0, p2=root_page),
            Instruction(Opcode.INTEGER, p1=1),           # key
            Instruction(Opcode.INTEGER, p1=42),          # field 1
            Instruction(Opcode.STRING, p4="test"),       # field 2
            Instruction(Opcode.MAKE_RECORD, p1=2),       # make record
            Instruction(Opcode.INSERT, p1=0),            # insert
            Instruction(Opcode.CLOSE, p1=0),
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        
        # Verify the record was inserted
        result = btree.search(1)
        assert result is not None
        assert result.get_value(0) == 42
        assert result.get_value(1) == "test"
    
    def test_scan_table(self, temp_db):
        # Create and populate table
        btree = BTree(temp_db)
        btree.insert(1, Record([100, "first"]))
        btree.insert(2, Record([200, "second"]))
        root_page = btree.get_root_page()
        
        dbm = DatabaseMachine(temp_db)
        
        # Scan and output all records
        program = [
            Instruction(Opcode.OPEN_READ, p1=0, p2=root_page),
            Instruction(Opcode.REWIND, p1=0, p2=8),      # jump to 8 if empty
            # Loop start (pc=2)
            Instruction(Opcode.KEY, p1=0),               # push key
            Instruction(Opcode.DATA, p1=0),              # push record
            Instruction(Opcode.RESULT_ROW, p1=2),        # output row (key, record)
            Instruction(Opcode.NEXT, p1=0, p2=2),        # next, jump to 2 if success
            # Loop end
            Instruction(Opcode.CLOSE, p1=0),
            Instruction(Opcode.HALT)
        ]
        
        results = dbm.execute(program)
        assert len(results) == 2
        assert results[0][0] == 1
        assert results[1][0] == 2


class TestDatabaseMachineComparisons:
    """Test comparison operations."""
    
    def test_eq_true(self, temp_db):
        dbm = DatabaseMachine(temp_db)
        program = [
            Instruction(Opcode.INTEGER, p1=5),
            Instruction(Opcode.INTEGER, p1=5),
            Instruction(Opcode.EQ),
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        assert dbm.stack[0] is True
    
    def test_eq_false(self, temp_db):
        dbm = DatabaseMachine(temp_db)
        program = [
            Instruction(Opcode.INTEGER, p1=5),
            Instruction(Opcode.INTEGER, p1=10),
            Instruction(Opcode.EQ),
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        assert dbm.stack[0] is False
    
    def test_lt_true(self, temp_db):
        dbm = DatabaseMachine(temp_db)
        program = [
            Instruction(Opcode.INTEGER, p1=5),
            Instruction(Opcode.INTEGER, p1=10),
            Instruction(Opcode.LT),
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        assert dbm.stack[0] is True
    
    def test_gt_false(self, temp_db):
        dbm = DatabaseMachine(temp_db)
        program = [
            Instruction(Opcode.INTEGER, p1=5),
            Instruction(Opcode.INTEGER, p1=10),
            Instruction(Opcode.GT),
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        assert dbm.stack[0] is False


class TestDatabaseMachineJumps:
    """Test jump instructions."""
    
    def test_unconditional_jump(self, temp_db):
        dbm = DatabaseMachine(temp_db)
        program = [
            Instruction(Opcode.INTEGER, p1=1),
            Instruction(Opcode.JUMP, p1=3),              # jump to pc=3
            Instruction(Opcode.INTEGER, p1=2),           # skipped
            Instruction(Opcode.INTEGER, p1=3),           # executed
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        assert dbm.stack == [1, 3]  # 2 was skipped
    
    def test_conditional_jump_taken(self, temp_db):
        dbm = DatabaseMachine(temp_db)
        program = [
            Instruction(Opcode.INTEGER, p1=0),           # false value
            Instruction(Opcode.JUMP_IF_FALSE, p1=3),     # jump to 3
            Instruction(Opcode.INTEGER, p1=99),          # skipped
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        assert dbm.stack == []  # 0 was popped, 99 was skipped
    
    def test_conditional_jump_not_taken(self, temp_db):
        dbm = DatabaseMachine(temp_db)
        program = [
            Instruction(Opcode.INTEGER, p1=1),           # true value
            Instruction(Opcode.JUMP_IF_FALSE, p1=3),     # not taken
            Instruction(Opcode.INTEGER, p1=99),          # executed
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        assert dbm.stack == [99]


class TestDatabaseMachineComplexProgram:
    """Test complex programs."""
    
    def test_insert_multiple_records(self, temp_db):
        btree = BTree(temp_db)
        root_page = btree.get_root_page()
        
        dbm = DatabaseMachine(temp_db)
        
        # Insert 3 records
        program = [
            Instruction(Opcode.OPEN_WRITE, p1=0, p2=root_page),
            
            # Insert key=1, value=[10]
            Instruction(Opcode.INTEGER, p1=1),
            Instruction(Opcode.INTEGER, p1=10),
            Instruction(Opcode.MAKE_RECORD, p1=1),
            Instruction(Opcode.INSERT, p1=0),
            
            # Insert key=2, value=[20]
            Instruction(Opcode.INTEGER, p1=2),
            Instruction(Opcode.INTEGER, p1=20),
            Instruction(Opcode.MAKE_RECORD, p1=1),
            Instruction(Opcode.INSERT, p1=0),
            
            # Insert key=3, value=[30]
            Instruction(Opcode.INTEGER, p1=3),
            Instruction(Opcode.INTEGER, p1=30),
            Instruction(Opcode.MAKE_RECORD, p1=1),
            Instruction(Opcode.INSERT, p1=0),
            
            Instruction(Opcode.CLOSE, p1=0),
            Instruction(Opcode.HALT)
        ]
        
        dbm.execute(program)
        
        # Verify all records
        assert btree.search(1).get_value(0) == 10
        assert btree.search(2).get_value(0) == 20
        assert btree.search(3).get_value(0) == 30
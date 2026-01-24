"""
Tests for chidb/sql/codegen.py
"""

import pytest
from chidb.sql.parser import (
    SelectStatement, InsertStatement, CreateTableStatement,
    BinaryOp, Literal, Identifier, ColumnDef
)
from chidb.sql.codegen import CodeGenerator, generate_code
from chidb.dbm import Opcode


class TestCodeGeneratorBasics:
    """Test basic code generator functionality."""
    
    def test_create_codegen(self):
        codegen = CodeGenerator()
        assert codegen is not None
    
    def test_register_table(self):
        codegen = CodeGenerator()
        codegen.register_table('users', 1)
        assert codegen.get_table_root('users') == 1
    
    def test_unknown_table_raises(self):
        codegen = CodeGenerator()
        with pytest.raises(ValueError):
            codegen.get_table_root('nonexistent')


class TestSelectCodeGeneration:
    """Test SELECT statement code generation."""
    
    def test_generate_simple_select(self):
        stmt = SelectStatement(columns=['*'], table='users')
        codegen = CodeGenerator({'users': 1})
        
        instructions = codegen.generate(stmt)
        
        assert len(instructions) > 0
        assert instructions[0].opcode == Opcode.OPEN_READ
        assert instructions[-1].opcode == Opcode.HALT
    
    def test_select_opens_correct_table(self):
        stmt = SelectStatement(columns=['*'], table='users')
        codegen = CodeGenerator({'users': 5})
        
        instructions = codegen.generate(stmt)
        
        # First instruction should open the correct root page
        assert instructions[0].opcode == Opcode.OPEN_READ
        assert instructions[0].p2 == 5
    
    def test_select_includes_rewind(self):
        stmt = SelectStatement(columns=['*'], table='users')
        codegen = CodeGenerator({'users': 1})
        
        instructions = codegen.generate(stmt)
        
        # Should have REWIND instruction
        opcodes = [instr.opcode for instr in instructions]
        assert Opcode.REWIND in opcodes
    
    def test_select_includes_next(self):
        stmt = SelectStatement(columns=['*'], table='users')
        codegen = CodeGenerator({'users': 1})
        
        instructions = codegen.generate(stmt)
        
        # Should have NEXT instruction for looping
        opcodes = [instr.opcode for instr in instructions]
        assert Opcode.NEXT in opcodes
    
    def test_select_includes_result_row(self):
        stmt = SelectStatement(columns=['*'], table='users')
        codegen = CodeGenerator({'users': 1})
        
        instructions = codegen.generate(stmt)
        
        # Should have RESULT_ROW instruction
        opcodes = [instr.opcode for instr in instructions]
        assert Opcode.RESULT_ROW in opcodes
    
    def test_select_closes_cursor(self):
        stmt = SelectStatement(columns=['*'], table='users')
        codegen = CodeGenerator({'users': 1})
        
        instructions = codegen.generate(stmt)
        
        # Should close cursor before HALT
        assert instructions[-2].opcode == Opcode.CLOSE


class TestInsertCodeGeneration:
    """Test INSERT statement code generation."""
    
    def test_generate_simple_insert(self):
        stmt = InsertStatement(table='users', values=[1, 'John'])
        codegen = CodeGenerator({'users': 1})
        
        instructions = codegen.generate(stmt)
        
        assert len(instructions) > 0
        assert instructions[0].opcode == Opcode.OPEN_WRITE
        assert instructions[-1].opcode == Opcode.HALT
    
    def test_insert_opens_for_write(self):
        stmt = InsertStatement(table='users', values=[42])
        codegen = CodeGenerator({'users': 3})
        
        instructions = codegen.generate(stmt)
        
        assert instructions[0].opcode == Opcode.OPEN_WRITE
        assert instructions[0].p2 == 3
    
    def test_insert_pushes_values(self):
        stmt = InsertStatement(table='users', values=[42, 'test'])
        codegen = CodeGenerator({'users': 1})
        
        instructions = codegen.generate(stmt)
        
        # Should have INTEGER and STRING instructions
        opcodes = [instr.opcode for instr in instructions]
        assert Opcode.INTEGER in opcodes
        assert Opcode.STRING in opcodes
    
    def test_insert_makes_record(self):
        stmt = InsertStatement(table='users', values=[1, 2, 3])
        codegen = CodeGenerator({'users': 1})
        
        instructions = codegen.generate(stmt)
        
        # Should have MAKE_RECORD instruction
        opcodes = [instr.opcode for instr in instructions]
        assert Opcode.MAKE_RECORD in opcodes
        
        # Find MAKE_RECORD and check it has correct field count
        for instr in instructions:
            if instr.opcode == Opcode.MAKE_RECORD:
                assert instr.p1 == 3  # 3 fields
    
    def test_insert_includes_insert_opcode(self):
        stmt = InsertStatement(table='users', values=[1])
        codegen = CodeGenerator({'users': 1})
        
        instructions = codegen.generate(stmt)
        
        opcodes = [instr.opcode for instr in instructions]
        assert Opcode.INSERT in opcodes
    
    def test_insert_with_null(self):
        stmt = InsertStatement(table='users', values=[1, None, 'test'])
        codegen = CodeGenerator({'users': 1})
        
        instructions = codegen.generate(stmt)
        
        # Should have NULL instruction
        opcodes = [instr.opcode for instr in instructions]
        assert Opcode.NULL in opcodes


class TestCreateTableCodeGeneration:
    """Test CREATE TABLE statement code generation."""
    
    def test_generate_create_table(self):
        columns = [ColumnDef('id', 'INTEGER')]
        stmt = CreateTableStatement(table='users', columns=columns)
        codegen = CodeGenerator()
        
        instructions = codegen.generate(stmt)
        
        # For now, just returns HALT
        assert len(instructions) >= 1
        assert instructions[-1].opcode == Opcode.HALT


class TestExpressionGeneration:
    """Test expression code generation."""
    
    def test_generate_literal_integer(self):
        codegen = CodeGenerator()
        expr = Literal(42)
        
        instructions = codegen.generate_expression(expr)
        
        assert len(instructions) == 1
        assert instructions[0].opcode == Opcode.INTEGER
        assert instructions[0].p1 == 42
    
    def test_generate_literal_string(self):
        codegen = CodeGenerator()
        expr = Literal('hello')
        
        instructions = codegen.generate_expression(expr)
        
        assert len(instructions) == 1
        assert instructions[0].opcode == Opcode.STRING
        assert instructions[0].p4 == 'hello'
    
    def test_generate_literal_null(self):
        codegen = CodeGenerator()
        expr = Literal(None)
        
        instructions = codegen.generate_expression(expr)
        
        assert len(instructions) == 1
        assert instructions[0].opcode == Opcode.NULL
    
    def test_generate_binary_comparison(self):
        codegen = CodeGenerator()
        expr = BinaryOp(Literal(5), '=', Literal(10))
        
        instructions = codegen.generate_expression(expr)
        
        # Should push both literals and compare
        assert len(instructions) == 3
        assert instructions[0].opcode == Opcode.INTEGER
        assert instructions[1].opcode == Opcode.INTEGER
        assert instructions[2].opcode == Opcode.EQ


class TestWhereFilterGeneration:
    """Test WHERE clause filter generation."""
    
    def test_generate_simple_where(self):
        codegen = CodeGenerator()
        expr = BinaryOp(Literal(5), '=', Literal(5))
        
        instructions = codegen.generate_where_filter(expr)
        
        assert len(instructions) > 0
        # Should have comparison opcode
        opcodes = [instr.opcode for instr in instructions]
        assert Opcode.EQ in opcodes
    
    def test_generate_where_with_comparison_operators(self):
        operators = {
            '<': Opcode.LT,
            '<=': Opcode.LE,
            '>': Opcode.GT,
            '>=': Opcode.GE,
            '!=': Opcode.NE
        }
        
        codegen = CodeGenerator()
        
        for op_str, op_code in operators.items():
            expr = BinaryOp(Literal(5), op_str, Literal(10))
            instructions = codegen.generate_where_filter(expr)
            
            opcodes = [instr.opcode for instr in instructions]
            assert op_code in opcodes


class TestGenerateCodeFunction:
    """Test the generate_code() convenience function."""
    
    def test_generate_code_select(self):
        stmt = SelectStatement(columns=['*'], table='users')
        instructions = generate_code(stmt, {'users': 1})
        
        assert len(instructions) > 0
        assert instructions[0].opcode == Opcode.OPEN_READ
    
    def test_generate_code_insert(self):
        stmt = InsertStatement(table='users', values=[1, 2])
        instructions = generate_code(stmt, {'users': 1})
        
        assert len(instructions) > 0
        assert instructions[0].opcode == Opcode.OPEN_WRITE


class TestAutoIncrementKey:
    """Test auto-incrementing key generation."""
    
    def test_auto_increment_keys(self):
        codegen = CodeGenerator({'users': 1})
        
        stmt1 = InsertStatement(table='users', values=[100])
        instructions1 = codegen.generate(stmt1)
        
        stmt2 = InsertStatement(table='users', values=[200])
        instructions2 = codegen.generate(stmt2)
        
        # Keys should be auto-incremented
        # First INSERT uses key 1, second uses key 2
        # Find INTEGER instructions for keys (should be first INTEGER after OPEN_WRITE)
        key1 = None
        key2 = None
        
        for instr in instructions1:
            if instr.opcode == Opcode.INTEGER:
                key1 = instr.p1
                break
        
        for instr in instructions2:
            if instr.opcode == Opcode.INTEGER:
                key2 = instr.p1
                break
        
        assert key1 == 1
        assert key2 == 2


class TestComplexCodeGeneration:
    """Test code generation for complex statements."""
    
    def test_select_with_where(self):
        where = BinaryOp(Literal(5), '=', Literal(5))
        stmt = SelectStatement(columns=['*'], table='users', where=where)
        codegen = CodeGenerator({'users': 1})
        
        instructions = codegen.generate(stmt)
        
        # Should include comparison operations
        opcodes = [instr.opcode for instr in instructions]
        assert Opcode.EQ in opcodes
    
    def test_insert_multiple_types(self):
        stmt = InsertStatement(table='test', values=[1, 'text', None])
        codegen = CodeGenerator({'test': 1})
        
        instructions = codegen.generate(stmt)
        
        opcodes = [instr.opcode for instr in instructions]
        assert Opcode.INTEGER in opcodes
        assert Opcode.STRING in opcodes
        assert Opcode.NULL in opcodes
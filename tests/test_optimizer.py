"""
Tests for chidb/sql/optimizer.py
"""

import pytest
from chidb.sql.parser import (
    SelectStatement, InsertStatement, CreateTableStatement,
    BinaryOp, Literal, Identifier, ColumnDef
)
from chidb.sql.optimizer import Optimizer, optimize


class TestOptimizerBasics:
    """Test basic optimizer functionality."""
    
    def test_create_optimizer(self):
        opt = Optimizer()
        assert opt is not None
    
    def test_optimize_returns_ast(self):
        stmt = SelectStatement(columns=['*'], table='users')
        result = optimize(stmt)
        assert isinstance(result, SelectStatement)


class TestConstantFolding:
    """Test constant folding optimization."""
    
    def test_fold_equality_true(self):
        # WHERE 5 = 5
        expr = BinaryOp(Literal(5), '=', Literal(5))
        opt = Optimizer()
        result = opt.optimize_expression(expr)
        
        assert isinstance(result, Literal)
        assert result.value is True
    
    def test_fold_equality_false(self):
        # WHERE 5 = 10
        expr = BinaryOp(Literal(5), '=', Literal(10))
        opt = Optimizer()
        result = opt.optimize_expression(expr)
        
        assert isinstance(result, Literal)
        assert result.value is False
    
    def test_fold_less_than(self):
        # WHERE 3 < 5
        expr = BinaryOp(Literal(3), '<', Literal(5))
        opt = Optimizer()
        result = opt.optimize_expression(expr)
        
        assert isinstance(result, Literal)
        assert result.value is True
    
    def test_fold_greater_than(self):
        # WHERE 10 > 5
        expr = BinaryOp(Literal(10), '>', Literal(5))
        opt = Optimizer()
        result = opt.optimize_expression(expr)
        
        assert isinstance(result, Literal)
        assert result.value is True
    
    def test_fold_and_expression(self):
        # WHERE true AND false
        expr = BinaryOp(Literal(True), 'AND', Literal(False))
        opt = Optimizer()
        result = opt.optimize_expression(expr)
        
        assert isinstance(result, Literal)
        assert result.value is False
    
    def test_fold_or_expression(self):
        # WHERE true OR false
        expr = BinaryOp(Literal(True), 'OR', Literal(False))
        opt = Optimizer()
        result = opt.optimize_expression(expr)
        
        assert isinstance(result, Literal)
        assert result.value is True


class TestExpressionSimplification:
    """Test expression simplification."""
    
    def test_simplify_x_equals_x(self):
        # WHERE id = id -> true
        expr = BinaryOp(Identifier('id'), '=', Identifier('id'))
        opt = Optimizer()
        result = opt.optimize_expression(expr)
        
        assert isinstance(result, Literal)
        assert result.value is True
    
    def test_simplify_x_not_equals_x(self):
        # WHERE id != id -> false
        expr = BinaryOp(Identifier('id'), '!=', Identifier('id'))
        opt = Optimizer()
        result = opt.optimize_expression(expr)
        
        assert isinstance(result, Literal)
        assert result.value is False
    
    def test_no_simplification_different_identifiers(self):
        # WHERE id = name (cannot simplify)
        expr = BinaryOp(Identifier('id'), '=', Identifier('name'))
        opt = Optimizer()
        result = opt.optimize_expression(expr)
        
        assert isinstance(result, BinaryOp)
        assert result.operator == '='


class TestSelectOptimization:
    """Test SELECT statement optimization."""
    
    def test_optimize_select_without_where(self):
        stmt = SelectStatement(columns=['*'], table='users')
        opt = Optimizer()
        result = opt.optimize_select(stmt)
        
        assert isinstance(result, SelectStatement)
        assert result.columns == ['*']
        assert result.table == 'users'
    
    def test_optimize_select_with_constant_where(self):
        # SELECT * FROM users WHERE 5 = 5
        where = BinaryOp(Literal(5), '=', Literal(5))
        stmt = SelectStatement(columns=['*'], table='users', where=where)
        
        opt = Optimizer()
        result = opt.optimize_select(stmt)
        
        # WHERE clause should be folded to true
        assert isinstance(result.where, Literal)
        assert result.where.value is True
    
    def test_optimize_select_with_variable_where(self):
        # SELECT * FROM users WHERE id = 5
        where = BinaryOp(Identifier('id'), '=', Literal(5))
        stmt = SelectStatement(columns=['*'], table='users', where=where)
        
        opt = Optimizer()
        result = opt.optimize_select(stmt)
        
        # WHERE clause should remain as is
        assert isinstance(result.where, BinaryOp)


class TestInsertOptimization:
    """Test INSERT statement optimization."""
    
    def test_optimize_insert(self):
        stmt = InsertStatement(table='users', values=[1, 'John', 25])
        opt = Optimizer()
        result = opt.optimize_insert(stmt)
        
        assert isinstance(result, InsertStatement)
        assert result.table == 'users'
        assert result.values == [1, 'John', 25]


class TestCreateTableOptimization:
    """Test CREATE TABLE statement optimization."""
    
    def test_optimize_create_table(self):
        columns = [
            ColumnDef('id', 'INTEGER', primary_key=True),
            ColumnDef('name', 'TEXT')
        ]
        stmt = CreateTableStatement(table='users', columns=columns)
        
        opt = Optimizer()
        result = opt.optimize_create_table(stmt)
        
        assert isinstance(result, CreateTableStatement)
        assert result.table == 'users'
        assert len(result.columns) == 2


class TestExpressionEquality:
    """Test expression equality checking."""
    
    def test_literals_equal(self):
        opt = Optimizer()
        expr1 = Literal(5)
        expr2 = Literal(5)
        
        assert opt.expressions_equal(expr1, expr2)
    
    def test_literals_not_equal(self):
        opt = Optimizer()
        expr1 = Literal(5)
        expr2 = Literal(10)
        
        assert not opt.expressions_equal(expr1, expr2)
    
    def test_identifiers_equal(self):
        opt = Optimizer()
        expr1 = Identifier('id')
        expr2 = Identifier('id')
        
        assert opt.expressions_equal(expr1, expr2)
    
    def test_identifiers_not_equal(self):
        opt = Optimizer()
        expr1 = Identifier('id')
        expr2 = Identifier('name')
        
        assert not opt.expressions_equal(expr1, expr2)
    
    def test_binary_ops_equal(self):
        opt = Optimizer()
        expr1 = BinaryOp(Identifier('x'), '=', Literal(5))
        expr2 = BinaryOp(Identifier('x'), '=', Literal(5))
        
        assert opt.expressions_equal(expr1, expr2)
    
    def test_binary_ops_not_equal(self):
        opt = Optimizer()
        expr1 = BinaryOp(Identifier('x'), '=', Literal(5))
        expr2 = BinaryOp(Identifier('x'), '=', Literal(10))
        
        assert not opt.expressions_equal(expr1, expr2)


class TestAlwaysTrueFalse:
    """Test always true/false detection."""
    
    def test_is_always_true(self):
        opt = Optimizer()
        expr = Literal(True)
        
        assert opt.is_always_true(expr)
    
    def test_is_not_always_true(self):
        opt = Optimizer()
        expr1 = Literal(False)
        expr2 = Identifier('x')
        
        assert not opt.is_always_true(expr1)
        assert not opt.is_always_true(expr2)
    
    def test_is_always_false(self):
        opt = Optimizer()
        expr = Literal(False)
        
        assert opt.is_always_false(expr)
    
    def test_is_not_always_false(self):
        opt = Optimizer()
        expr1 = Literal(True)
        expr2 = Identifier('x')
        
        assert not opt.is_always_false(expr1)
        assert not opt.is_always_false(expr2)


class TestNestedExpressions:
    """Test optimization of nested expressions."""
    
    def test_nested_constant_folding(self):
        # WHERE (3 < 5) AND (10 > 8)
        left = BinaryOp(Literal(3), '<', Literal(5))
        right = BinaryOp(Literal(10), '>', Literal(8))
        expr = BinaryOp(left, 'AND', right)
        
        opt = Optimizer()
        result = opt.optimize_expression(expr)
        
        # Both sides should fold to true, then AND should fold to true
        assert isinstance(result, Literal)
        assert result.value is True
    
    def test_partially_foldable_expression(self):
        # WHERE (5 = 5) AND (id = 10)
        left = BinaryOp(Literal(5), '=', Literal(5))  # Can fold
        right = BinaryOp(Identifier('id'), '=', Literal(10))  # Cannot fold
        expr = BinaryOp(left, 'AND', right)
        
        opt = Optimizer()
        result = opt.optimize_expression(expr)
        
        # Left side should be folded, but overall expression remains
        assert isinstance(result, BinaryOp)
        assert isinstance(result.left, Literal)
        assert result.left.value is True


class TestOptimizeFunction:
    """Test the optimize() convenience function."""
    
    def test_optimize_function_select(self):
        where = BinaryOp(Literal(5), '=', Literal(5))
        stmt = SelectStatement(columns=['*'], table='users', where=where)
        
        result = optimize(stmt)
        
        assert isinstance(result, SelectStatement)
        assert isinstance(result.where, Literal)
        assert result.where.value is True
    
    def test_optimize_function_insert(self):
        stmt = InsertStatement(table='users', values=[1, 2, 3])
        result = optimize(stmt)
        
        assert isinstance(result, InsertStatement)
    
    def test_optimize_function_create_table(self):
        columns = [ColumnDef('id', 'INTEGER')]
        stmt = CreateTableStatement(table='test', columns=columns)
        result = optimize(stmt)
        
        assert isinstance(result, CreateTableStatement)
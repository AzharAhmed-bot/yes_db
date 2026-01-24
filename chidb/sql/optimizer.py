"""
SQL Optimizer - Optimizes query execution plans.
Performs query optimization before code generation.
"""

from typing import Any
from chidb.sql.parser import (
    ASTNode, SelectStatement, InsertStatement, CreateTableStatement,
    Expression, BinaryOp, Literal, Identifier
)


class Optimizer:
    """
    Query optimizer for improving execution plans.
    
    The optimizer performs transformations on the AST to improve performance:
    - Constant folding
    - Expression simplification
    - Predicate pushdown (future)
    - Index selection (future)
    """
    
    def __init__(self):
        """Initialize the optimizer."""
        pass
    
    def optimize(self, ast: ASTNode) -> ASTNode:
        """
        Optimize an AST.
        
        Args:
            ast: The input AST node
            
        Returns:
            Optimized AST node
        """
        if isinstance(ast, SelectStatement):
            return self.optimize_select(ast)
        elif isinstance(ast, InsertStatement):
            return self.optimize_insert(ast)
        elif isinstance(ast, CreateTableStatement):
            return self.optimize_create_table(ast)
        else:
            return ast
    
    def optimize_select(self, stmt: SelectStatement) -> SelectStatement:
        """
        Optimize a SELECT statement.
        
        Args:
            stmt: The SELECT statement to optimize
            
        Returns:
            Optimized SELECT statement
        """
        # Optimize WHERE clause if present
        if stmt.where:
            stmt.where = self.optimize_expression(stmt.where)
        
        return stmt
    
    def optimize_insert(self, stmt: InsertStatement) -> InsertStatement:
        """
        Optimize an INSERT statement.
        
        For now, INSERT statements don't need much optimization.
        """
        return stmt
    
    def optimize_create_table(self, stmt: CreateTableStatement) -> CreateTableStatement:
        """
        Optimize a CREATE TABLE statement.
        
        For now, CREATE TABLE statements don't need optimization.
        """
        return stmt
    
    def optimize_expression(self, expr: Expression) -> Expression:
        """
        Optimize an expression.
        
        Performs:
        - Constant folding
        - Expression simplification
        """
        if isinstance(expr, BinaryOp):
            # Recursively optimize both sides
            expr.left = self.optimize_expression(expr.left)
            expr.right = self.optimize_expression(expr.right)
            
            # Constant folding: if both sides are literals, evaluate now
            if isinstance(expr.left, Literal) and isinstance(expr.right, Literal):
                return self.fold_constants(expr)
            
            # Simplification: x = x -> true
            if expr.operator == '=' and self.expressions_equal(expr.left, expr.right):
                return Literal(True)
            
            # Simplification: x != x -> false
            if expr.operator == '!=' and self.expressions_equal(expr.left, expr.right):
                return Literal(False)
            
            return expr
        
        elif isinstance(expr, Literal):
            return expr
        
        elif isinstance(expr, Identifier):
            return expr
        
        else:
            return expr
    
    def fold_constants(self, expr: BinaryOp) -> Literal:
        """
        Fold constant expressions.
        
        Evaluates expressions with literal operands at compile time.
        """
        left_val = expr.left.value
        right_val = expr.right.value
        
        try:
            if expr.operator == '=':
                result = left_val == right_val
            elif expr.operator == '!=':
                result = left_val != right_val
            elif expr.operator == '<':
                result = left_val < right_val
            elif expr.operator == '<=':
                result = left_val <= right_val
            elif expr.operator == '>':
                result = left_val > right_val
            elif expr.operator == '>=':
                result = left_val >= right_val
            elif expr.operator == 'AND':
                result = left_val and right_val
            elif expr.operator == 'OR':
                result = left_val or right_val
            else:
                # Can't fold this operator
                return expr
            
            return Literal(result)
        except:
            # If folding fails, return original expression
            return expr
    
    def expressions_equal(self, expr1: Expression, expr2: Expression) -> bool:
        """
        Check if two expressions are equal.
        """
        if type(expr1) != type(expr2):
            return False
        
        if isinstance(expr1, Literal):
            return expr1.value == expr2.value
        
        if isinstance(expr1, Identifier):
            return expr1.name == expr2.name
        
        if isinstance(expr1, BinaryOp):
            return (expr1.operator == expr2.operator and
                    self.expressions_equal(expr1.left, expr2.left) and
                    self.expressions_equal(expr1.right, expr2.right))
        
        return False
    
    def is_always_true(self, expr: Expression) -> bool:
        """
        Check if an expression is always true.
        """
        if isinstance(expr, Literal):
            return expr.value is True
        return False
    
    def is_always_false(self, expr: Expression) -> bool:
        """
        Check if an expression is always false.
        """
        if isinstance(expr, Literal):
            return expr.value is False
        return False


def optimize(ast: ASTNode) -> ASTNode:
    """
    Convenience function to optimize an AST.
    
    Args:
        ast: The AST to optimize
        
    Returns:
        Optimized AST
    """
    optimizer = Optimizer()
    return optimizer.optimize(ast)
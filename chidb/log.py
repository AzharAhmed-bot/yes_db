"""
Logging and debugging utilities for the database.
Provides structured logging for tracing database operations.
"""

import logging
import sys
from typing import Any, Optional
from enum import IntEnum


class LogLevel(IntEnum):
    """Log levels for database operations."""
    CRITICAL = 50
    ERROR = 40
    WARNING = 30
    INFO = 20
    DEBUG = 10
    TRACE = 5


class DatabaseLogger:
    """
    Logger for database operations with support for different components.
    """
    
    def __init__(self, name: str = "chidb", level: int = LogLevel.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # Only add handler if none exists
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stderr)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def set_level(self, level: int) -> None:
        """Set the logging level."""
        self.logger.setLevel(level)
    
    def critical(self, msg: str, **kwargs) -> None:
        """Log critical error message."""
        self.logger.critical(msg, **kwargs)
    
    def error(self, msg: str, **kwargs) -> None:
        """Log error message."""
        self.logger.error(msg, **kwargs)
    
    def warning(self, msg: str, **kwargs) -> None:
        """Log warning message."""
        self.logger.warning(msg, **kwargs)
    
    def info(self, msg: str, **kwargs) -> None:
        """Log info message."""
        self.logger.info(msg, **kwargs)
    
    def debug(self, msg: str, **kwargs) -> None:
        """Log debug message."""
        self.logger.debug(msg, **kwargs)
    
    def trace(self, msg: str, **kwargs) -> None:
        """Log trace message (very verbose)."""
        if self.logger.level <= LogLevel.TRACE:
            self.logger.log(LogLevel.TRACE, msg, **kwargs)


# Global logger instances for different components
_loggers = {}


def get_logger(component: str = "chidb") -> DatabaseLogger:
    """
    Get or create a logger for a specific component.
    
    Args:
        component: Name of the component (e.g., 'pager', 'btree', 'dbm')
    
    Returns:
        DatabaseLogger instance for the component
    """
    if component not in _loggers:
        _loggers[component] = DatabaseLogger(f"chidb.{component}")
    return _loggers[component]


def set_global_level(level: int) -> None:
    """Set logging level for all components."""
    for logger in _loggers.values():
        logger.set_level(level)


def log_page_read(page_id: int, component: str = "pager") -> None:
    """Log a page read operation."""
    logger = get_logger(component)
    logger.trace(f"Reading page {page_id}")


def log_page_write(page_id: int, component: str = "pager") -> None:
    """Log a page write operation."""
    logger = get_logger(component)
    logger.trace(f"Writing page {page_id}")


def log_page_allocate(page_id: int, component: str = "pager") -> None:
    """Log a page allocation."""
    logger = get_logger(component)
    logger.debug(f"Allocated new page {page_id}")


def log_btree_insert(key: Any, component: str = "btree") -> None:
    """Log a B-tree insertion."""
    logger = get_logger(component)
    logger.debug(f"Inserting key {key} into B-tree")


def log_btree_split(page_id: int, component: str = "btree") -> None:
    """Log a B-tree node split."""
    logger = get_logger(component)
    logger.info(f"Splitting B-tree node at page {page_id}")


def log_btree_search(key: Any, component: str = "btree") -> None:
    """Log a B-tree search."""
    logger = get_logger(component)
    logger.trace(f"Searching for key {key} in B-tree")


def log_dbm_instruction(instruction: str, component: str = "dbm") -> None:
    """Log a DBM instruction execution."""
    logger = get_logger(component)
    logger.trace(f"Executing instruction: {instruction}")


def log_sql_parse(sql: str, component: str = "sql") -> None:
    """Log SQL parsing."""
    logger = get_logger(component)
    logger.debug(f"Parsing SQL: {sql}")


def log_sql_codegen(statement_type: str, component: str = "sql") -> None:
    """Log SQL code generation."""
    logger = get_logger(component)
    logger.debug(f"Generating code for {statement_type} statement")
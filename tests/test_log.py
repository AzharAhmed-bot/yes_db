"""
Tests for chidb/log.py
"""

import pytest
import logging
from chidb.log import (
    DatabaseLogger, LogLevel, get_logger, set_global_level,
    log_page_read, log_page_write, log_page_allocate,
    log_btree_insert, log_btree_split, log_btree_search,
    log_dbm_instruction, log_sql_parse, log_sql_codegen
)


class TestDatabaseLogger:
    """Test DatabaseLogger class."""
    
    def test_logger_creation(self):
        logger = DatabaseLogger("test", LogLevel.DEBUG)
        assert logger.logger.name == "test"
        assert logger.logger.level == LogLevel.DEBUG
    
    def test_set_level(self):
        logger = DatabaseLogger("test", LogLevel.INFO)
        assert logger.logger.level == LogLevel.INFO
        
        logger.set_level(LogLevel.DEBUG)
        assert logger.logger.level == LogLevel.DEBUG
    
    def test_log_methods_exist(self):
        logger = DatabaseLogger("test")
        
        # Should not raise
        logger.critical("critical")
        logger.error("error")
        logger.warning("warning")
        logger.info("info")
        logger.debug("debug")
        logger.trace("trace")


class TestGlobalLoggers:
    """Test global logger management."""
    
    def test_get_logger(self):
        logger1 = get_logger("test_component")
        logger2 = get_logger("test_component")
        
        # Should return the same instance
        assert logger1 is logger2
    
    def test_different_components(self):
        logger1 = get_logger("component1")
        logger2 = get_logger("component2")
        
        # Should be different instances
        assert logger1 is not logger2
        assert logger1.logger.name != logger2.logger.name
    
    def test_set_global_level(self):
        logger1 = get_logger("comp1")
        logger2 = get_logger("comp2")
        
        logger1.set_level(LogLevel.INFO)
        logger2.set_level(LogLevel.WARNING)
        
        set_global_level(LogLevel.DEBUG)
        
        assert logger1.logger.level == LogLevel.DEBUG
        assert logger2.logger.level == LogLevel.DEBUG


class TestLoggingHelpers:
    """Test specialized logging helper functions."""
    
    def test_page_logging(self):
        # Should not raise
        log_page_read(1)
        log_page_write(2)
        log_page_allocate(3)
    
    def test_btree_logging(self):
        # Should not raise
        log_btree_insert(42)
        log_btree_split(5)
        log_btree_search(100)
    
    def test_dbm_logging(self):
        # Should not raise
        log_dbm_instruction("OPEN_TABLE")
    
    def test_sql_logging(self):
        # Should not raise
        log_sql_parse("SELECT * FROM users")
        log_sql_codegen("SELECT")


class TestLogLevels:
    """Test log level enum."""
    
    def test_log_level_values(self):
        assert LogLevel.CRITICAL == 50
        assert LogLevel.ERROR == 40
        assert LogLevel.WARNING == 30
        assert LogLevel.INFO == 20
        assert LogLevel.DEBUG == 10
        assert LogLevel.TRACE == 5
    
    def test_log_level_ordering(self):
        assert LogLevel.TRACE < LogLevel.DEBUG
        assert LogLevel.DEBUG < LogLevel.INFO
        assert LogLevel.INFO < LogLevel.WARNING
        assert LogLevel.WARNING < LogLevel.ERROR
        assert LogLevel.ERROR < LogLevel.CRITICAL
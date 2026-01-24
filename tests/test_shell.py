"""
Tests for YesDB/cli/shell.py
"""

import pytest
import tempfile
import os
from io import StringIO
from unittest.mock import patch, MagicMock
from chidb.api import YesDB
from chidb.cli.shell import Shell, main


@pytest.fixture
def temp_db_path():
    """Create a temporary database file path."""
    fd, path = tempfile.mkstemp(suffix='.cdb')
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def test_db(temp_db_path):
    """Create a test database with sample data."""
    db = YesDB(temp_db_path)
    db.execute('CREATE TABLE users (id INTEGER, name TEXT)')
    db.execute("INSERT INTO users VALUES (1, 'Alice')")
    db.execute("INSERT INTO users VALUES (2, 'Bob')")
    return db


class TestShellBasics:
    """Test basic shell functionality."""
    
    def test_create_shell(self, test_db):
        shell = Shell(test_db)
        assert shell.db is test_db
        assert shell.running is True
        test_db.close()
    
    def test_welcome_message(self, test_db):
        shell = Shell(test_db)
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.print_welcome()
            output = fake_out.getvalue()
            assert 'yes_db' in output
        
        test_db.close()
    
    def test_help_message(self, test_db):
        shell = Shell(test_db)
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.print_help()
            output = fake_out.getvalue()
            assert '.help' in output
            assert '.tables' in output
            assert '.exit' in output
        
        test_db.close()


class TestSQLExecution:
    """Test SQL execution in shell."""
    
    def test_execute_select(self, test_db):
        shell = Shell(test_db)
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.execute_sql('SELECT * FROM users')
            output = fake_out.getvalue()
            # Should show results
            assert 'row' in output or 'Alice' in output or 'Bob' in output
        
        test_db.close()
    
    def test_execute_insert(self, test_db):
        shell = Shell(test_db)
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.execute_sql("INSERT INTO users VALUES (3, 'Charlie')")
            output = fake_out.getvalue()
            assert 'OK' in output or 'Error' not in output
        
        test_db.close()
    
    def test_execute_invalid_sql(self, test_db):
        shell = Shell(test_db)
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.execute_sql('INVALID SQL')
            output = fake_out.getvalue()
            assert 'Error' in output
        
        test_db.close()


class TestSpecialCommands:
    """Test special shell commands."""
    
    def test_exit_command(self, test_db):
        shell = Shell(test_db)
        
        with patch('sys.stdout', new=StringIO()):
            shell.handle_special_command('.exit')
            assert shell.running is False
        
        test_db.close()
    
    def test_quit_command(self, test_db):
        shell = Shell(test_db)
        
        with patch('sys.stdout', new=StringIO()):
            shell.handle_special_command('.quit')
            assert shell.running is False
        
        test_db.close()
    
    def test_help_command(self, test_db):
        shell = Shell(test_db)
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.handle_special_command('.help')
            output = fake_out.getvalue()
            assert 'help' in output.lower()
        
        test_db.close()
    
    def test_tables_command(self, test_db):
        shell = Shell(test_db)
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.show_tables()
            output = fake_out.getvalue()
            assert 'users' in output
        
        test_db.close()
    
    def test_schema_command(self, test_db):
        shell = Shell(test_db)
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.show_schema()
            output = fake_out.getvalue()
            # Should show something about tables
            assert len(output) > 0
        
        test_db.close()
    
    def test_unknown_command(self, test_db):
        shell = Shell(test_db)
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.handle_special_command('.unknown')
            output = fake_out.getvalue()
            assert 'Unknown' in output
        
        test_db.close()


class TestResultPrinting:
    """Test result formatting and printing."""
    
    def test_print_empty_results(self, test_db):
        shell = Shell(test_db)
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.print_results([])
            output = fake_out.getvalue()
            assert 'no rows' in output
        
        test_db.close()
    
    def test_print_single_row(self, test_db):
        shell = Shell(test_db)
        results = [[1, 'Alice']]
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.print_results(results)
            output = fake_out.getvalue()
            assert 'Alice' in output
            assert '1 row' in output
        
        test_db.close()
    
    def test_print_multiple_rows(self, test_db):
        shell = Shell(test_db)
        results = [[1, 'Alice'], [2, 'Bob']]
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.print_results(results)
            output = fake_out.getvalue()
            assert 'Alice' in output
            assert 'Bob' in output
            assert '2 rows' in output
        
        test_db.close()
    
    def test_print_null_values(self, test_db):
        shell = Shell(test_db)
        results = [[1, None]]
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.print_results(results)
            output = fake_out.getvalue()
            assert 'NULL' in output
        
        test_db.close()


class TestMainFunction:
    """Test the main entry point."""
    
    def test_main_with_command_mode(self, temp_db_path):
        # Test single command execution
        args = [temp_db_path, '-c', 'CREATE TABLE test (id INTEGER)']
        
        exit_code = main(args)
        assert exit_code == 0
    
    def test_main_with_invalid_command(self, temp_db_path):
        args = [temp_db_path, '-c', 'INVALID SQL']
        
        exit_code = main(args)
        assert exit_code == 1
    
    def test_main_creates_database(self, temp_db_path):
        args = [temp_db_path, '-c', 'CREATE TABLE test (id INTEGER)']
        main(args)
        
        # Database file should exist
        assert os.path.exists(temp_db_path)


class TestShellInteraction:
    """Test interactive shell behavior."""
    
    def test_shell_run_with_exit(self, test_db):
        shell = Shell(test_db)
        
        # Mock input to return .exit
        with patch('builtins.input', side_effect=['.exit']):
            with patch('sys.stdout', new=StringIO()):
                shell.run()
        
        assert shell.running is False
        test_db.close()
    
    def test_shell_handles_empty_input(self, test_db):
        shell = Shell(test_db)
        
        # Mock input: empty line then exit
        with patch('builtins.input', side_effect=['', '.exit']):
            with patch('sys.stdout', new=StringIO()):
                shell.run()
        
        test_db.close()
    
    def test_shell_handles_keyboard_interrupt(self, test_db):
        shell = Shell(test_db)
        
        # Simulate Ctrl+C then exit
        with patch('builtins.input', side_effect=[KeyboardInterrupt(), '.exit']):
            with patch('sys.stdout', new=StringIO()):
                shell.run()
        
        test_db.close()


class TestCommandLineArguments:
    """Test command-line argument parsing."""
    
    def test_no_arguments_fails(self):
        # Should require database argument
        with pytest.raises(SystemExit):
            main([])
    
    def test_help_argument(self):
        # -h or --help should work
        with pytest.raises(SystemExit) as exc_info:
            main(['--help'])
        
        # Help exits with 0
        assert exc_info.value.code == 0


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_nonexistent_database_file(self):
        # Should create new database
        fd, path = tempfile.mkstemp(suffix='.cdb')
        os.close(fd)
        os.unlink(path)  # Remove it so it doesn't exist
        
        try:
            args = [path, '-c', 'CREATE TABLE test (id INTEGER)']
            exit_code = main(args)
            assert exit_code == 0
            assert os.path.exists(path)
        finally:
            if os.path.exists(path):
                os.unlink(path)
    
    def test_show_tables_empty_database(self, temp_db_path):
        db = YesDB(temp_db_path)
        shell = Shell(db)
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            shell.show_tables()
            output = fake_out.getvalue()
            assert 'no tables' in output
        
        db.close()
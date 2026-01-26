"""
Interactive shell for ChiDB.
Provides a command-line interface for database interaction.
"""

import sys
import argparse
from typing import Optional
from chidb.api import YesDB


class Shell:
    """
    Interactive database shell.
    
    Supports SQL commands and special shell commands.
    """
    
    def __init__(self, database: YesDB):
        """
        Initialize the shell.
        
        Args:
            database: The database connection
        """
        self.db = database
        self.running = True
    
    def run(self) -> None:
        """Run the interactive shell."""
        self.print_welcome()
        
        while self.running:
            try:
                # Read input
                line = input('yes_db> ')
                line = line.strip()
                
                if not line:
                    continue
                
                # Check for special commands
                if line.startswith('.'):
                    self.handle_special_command(line)
                else:
                    # Execute SQL
                    self.execute_sql(line)
            
            except KeyboardInterrupt:
                print("\nUse .exit to quit")
                continue
            
            except EOFError:
                print("\nGoodbye!")
                break
            
            except Exception as e:
                print(f"Error: {e}")
    
    def print_welcome(self) -> None:
        """Print welcome message."""
        print("yes_db - Educational Relational Database")
        print("Enter SQL statements or .help for commands")
        print()
    
    def execute_sql(self, sql: str) -> None:
        """
        Execute a SQL statement.
        
        Args:
            sql: The SQL statement to execute
        """
        try:
            # Parse to get column info for SELECT
            from chidb.sql.lexer import Lexer
            from chidb.sql.parser import Parser, SelectStatement
            
            lexer = Lexer(sql)
            tokens = lexer.tokenize()
            parser = Parser(tokens)
            ast = parser.parse()
            
            # Store column info if it's a SELECT
            selected_columns = None
            table_name = None
            if isinstance(ast, SelectStatement):
                selected_columns = ast.columns
                table_name = ast.table
            
            # Execute the query
            results = self.db.execute(sql)
            
            if results:
                # Print results with column information
                self.print_results(results, selected_columns, table_name)
            else:
                print("OK")
        
        except Exception as e:
            print(f"SQL Error: {e}")
    
    def print_results(self, results: list, selected_columns: list = None, table_name: str = None) -> None:
        """
        Print query results in a formatted table.
        
        Args:
            results: List of result rows
            selected_columns: List of column names that were selected
            table_name: Name of the table being queried
        """
        if not results:
            print("(no rows)")
            return
        
        from chidb.record import Record
        
        # Get table metadata if available
        table_meta = None
        if table_name and hasattr(self.db, 'table_metadata'):
            table_meta = self.db.table_metadata.get(table_name)
        
        # Extract all values from results
        rows_data = []
        for row in results:
            row_values = []
            for value in row:
                # Check if value is a Record object
                if isinstance(value, Record):
                    all_values = value.get_values()
                    
                    # Filter columns if specific columns were requested
                    if selected_columns and selected_columns != ['*'] and table_meta:
                        # Map column names to indices
                        filtered_values = []
                        for col_name in selected_columns:
                            # Find the index of this column
                            for i, col_def in enumerate(table_meta.columns):
                                if col_def.name == col_name:
                                    if i < len(all_values):
                                        filtered_values.append(all_values[i])
                                    break
                        row_values.extend(filtered_values)
                    else:
                        # Use all values
                        row_values.extend(all_values)
                elif value is None:
                    row_values.append('NULL')
                else:
                    row_values.append(str(value))
            rows_data.append(row_values)
        
        if not rows_data:
            print("(no rows)")
            return
        
        # Calculate column widths
        num_cols = len(rows_data[0]) if rows_data else 0
        col_widths = [0] * num_cols
        
        for row in rows_data:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)))
        
        # Print separator line
        separator = '+' + '+'.join('-' * (w + 2) for w in col_widths) + '+'
        print(separator)
        
        # Print rows
        for row in rows_data:
            formatted_values = []
            for i, val in enumerate(row):
                formatted_values.append(str(val).ljust(col_widths[i]))
            print('| ' + ' | '.join(formatted_values) + ' |')
        
        # Print bottom separator
        print(separator)
        print(f"({len(results)} row{'s' if len(results) != 1 else ''})")
    
    def handle_special_command(self, command: str) -> None:
        """
        Handle special shell commands (starting with .).
        
        Args:
            command: The special command
        """
        command = command.lower()
        
        if command == '.exit' or command == '.quit':
            self.running = False
            print("Goodbye!")
        
        elif command == '.help':
            self.print_help()
        
        elif command == '.tables':
            self.show_tables()
        
        elif command == '.schema':
            self.show_schema()
        
        else:
            print(f"Unknown command: {command}")
            print("Type .help for list of commands")
    
    def print_help(self) -> None:
        """Print help message."""
        print("Special commands:")
        print("  .help          Show this help message")
        print("  .tables        List all tables")
        print("  .schema        Show table schemas")
        print("  .exit          Exit the shell")
        print("  .quit          Exit the shell")
        print()
        print("Enter SQL statements to execute them")
    
    def show_tables(self) -> None:
        """Show all tables in the database."""
        tables = self.db.get_table_names()
        
        if not tables:
            print("(no tables)")
        else:
            for table in tables:
                print(table)
    
    def show_schema(self) -> None:
        """Show table schemas."""
        tables = self.db.get_table_names()
        
        if not tables:
            print("(no tables)")
        else:
            print("Schema information not yet implemented")
            print("Tables:", ', '.join(tables))


def main(args: Optional[list] = None) -> int:
    """
    Main entry point for the shell.
    
    Args:
        args: Command-line arguments (default: sys.argv[1:])
        
    Returns:
        Exit code
    """
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='yes_db - Educational Relational Database',
        prog='yes_db'
    )
    parser.add_argument(
        'database',
        help='Database file to open or create'
    )
    parser.add_argument(
        '-c', '--command',
        help='Execute a single SQL command and exit',
        metavar='SQL'
    )
    
    if args is None:
        args = sys.argv[1:]
    
    parsed_args = parser.parse_args(args)
    
    # Open database
    try:
        db = YesDB(parsed_args.database)
    except Exception as e:
        print(f"Error opening database: {e}", file=sys.stderr)
        return 1
    
    try:
        # Execute single command mode
        if parsed_args.command:
            try:
                results = db.execute(parsed_args.command)
                if results:
                    for row in results:
                        print(' | '.join(str(v) if v is not None else 'NULL' for v in row))
                return 0
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                return 1
        
        # Interactive mode
        else:
            shell = Shell(db)
            shell.run()
            return 0
    
    finally:
        db.close()


if __name__ == '__main__':
    sys.exit(main())
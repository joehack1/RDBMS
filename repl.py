"""
Interactive REPL mode for MicroSQL Database
"""
from database import MicroSQL
from tabulate import tabulate
import sys

class MicroSQLREPL:
    """Interactive command-line interface for MicroSQL"""
    
    def __init__(self, db_name: str = "mydb"):
        self.db = MicroSQL(db_name)
        self.db_name = db_name
        self.running = True
    
    def print_banner(self):
        """Print welcome banner"""
        print("\n" + "="*60)
        print("  MicroSQL Interactive REPL")
        print("  Database: " + self.db_name)
        print("  Type '.help' for help, '.exit' to quit")
        print("="*60 + "\n")
    
    def print_help(self):
        """Print help message"""
        help_text = """
Available Commands:
  .help              - Show this help message
  .exit              - Exit REPL
  .tables            - List all tables
  .schema <table>    - Show table schema
  .clear             - Clear screen

SQL Commands:
  CREATE TABLE name (columns...)    - Create a new table
  INSERT INTO ...                    - Insert data
  SELECT ...                         - Query data
  UPDATE ...                         - Update data
  DELETE ...                         - Delete data

Examples:
  CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)
  INSERT INTO users (id, name) VALUES (1, 'Alice')
  SELECT * FROM users WHERE id = 1
  SELECT * FROM users ORDER BY name DESC LIMIT 10
  SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id

Special Features:
  - PRIMARY KEY constraint enforcement
  - UNIQUE constraint enforcement
  - Basic indexing for faster lookups
  - INNER JOIN and LEFT JOIN support
  - WHERE, ORDER BY, LIMIT clauses
"""
        print(help_text)
    
    def format_results(self, results: list):
        """Format results for display"""
        if not results:
            print("(No results)")
            return
        
        # Use tabulate for nice formatting
        try:
            print(tabulate(results, headers="keys", tablefmt="grid"))
        except:
            # Fallback if tabulate not available
            print(results)
    
    def list_tables(self):
        """List all tables"""
        tables = list(self.db.tables.keys())
        if tables:
            print("\nTables:")
            for table in tables:
                count = len(self.db.tables[table])
                print(f"  - {table} ({count} rows)")
        else:
            print("No tables found")
        print()
    
    def show_schema(self, table_name: str):
        """Show table schema"""
        if table_name not in self.db.tables:
            print(f"Table '{table_name}' not found")
            return
        
        schema = self.db.schemas[table_name]
        primary_key = self.db.primary_keys.get(table_name)
        unique_cols = self.db.unique_columns.get(table_name, [])
        
        print(f"\nSchema for table '{table_name}':")
        print("-" * 60)
        for col_name, col_type in schema.items():
            flags = []
            if col_name == primary_key:
                flags.append("PRIMARY KEY")
            if col_name in unique_cols:
                flags.append("UNIQUE")
            
            flag_str = f" [{', '.join(flags)}]" if flags else ""
            print(f"  {col_name}: {col_type}{flag_str}")
        print()
    
    def execute_sql(self, sql: str):
        """Execute SQL statement"""
        try:
            result = self.db.execute(sql)
            
            if isinstance(result, list) and result:
                self.format_results(result)
            elif isinstance(result, list) and not result:
                print("(0 rows)")
            else:
                print("OK")
        except Exception as e:
            print(f"Error: {str(e)}", file=sys.stderr)
    
    def run(self):
        """Run the REPL"""
        self.print_banner()
        
        while self.running:
            try:
                user_input = input("microsql> ").strip()
                
                if not user_input:
                    continue
                
                # Handle special commands
                if user_input.startswith('.'):
                    self.handle_special_command(user_input)
                else:
                    # SQL statement
                    self.execute_sql(user_input)
            
            except KeyboardInterrupt:
                print("\n\nUse '.exit' to quit")
            except Exception as e:
                print(f"Error: {str(e)}", file=sys.stderr)
    
    def handle_special_command(self, command: str):
        """Handle special commands"""
        cmd = command.lower().split()
        
        if cmd[0] == '.exit':
            self.running = False
            print("\nGoodbye!")
        
        elif cmd[0] == '.help':
            self.print_help()
        
        elif cmd[0] == '.tables':
            self.list_tables()
        
        elif cmd[0] == '.schema':
            if len(cmd) > 1:
                self.show_schema(cmd[1])
            else:
                print("Usage: .schema <table_name>")
        
        elif cmd[0] == '.clear':
            print("\033[2J\033[H", end='')  # Clear screen (Unix/Linux/Mac)
        
        else:
            print(f"Unknown command: {command}")
            print("Type '.help' for available commands")

def main():
    """Main entry point"""
    import sys
    
    db_name = sys.argv[1] if len(sys.argv) > 1 else "mydb"
    repl = MicroSQLREPL(db_name)
    repl.run()

if __name__ == '__main__':
    main()

"""
MicroSQL - A simple RDBMS implementation
"""
import json
import os
from datetime import datetime
from typing import List, Dict, Any

class MicroSQL:
    """Simple in-memory RDBMS with file persistence"""
    
    def __init__(self, db_name: str):
        """Initialize database"""
        self.db_name = db_name
        self.db_file = f"{db_name}.json"
        self.tables: Dict[str, List[Dict[str, Any]]] = {}
        self.schemas: Dict[str, Dict[str, str]] = {}
        self.load_from_file()
    
    def load_from_file(self):
        """Load database from JSON file if exists"""
        if os.path.exists(self.db_file):
            try:
                with open(self.db_file, 'r') as f:
                    data = json.load(f)
                    self.tables = data.get('tables', {})
                    self.schemas = data.get('schemas', {})
            except:
                pass
    
    def save_to_file(self):
        """Save database to JSON file"""
        with open(self.db_file, 'w') as f:
            json.dump({
                'tables': self.tables,
                'schemas': self.schemas
            }, f, indent=2, default=str)
    
    def execute(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL statement"""
        sql = sql.strip()
        
        if sql.upper().startswith('CREATE TABLE'):
            return self._create_table(sql)
        elif sql.upper().startswith('INSERT INTO'):
            return self._insert(sql)
        elif sql.upper().startswith('SELECT'):
            return self._select(sql)
        elif sql.upper().startswith('UPDATE'):
            return self._update(sql)
        elif sql.upper().startswith('DELETE'):
            return self._delete(sql)
        else:
            raise ValueError(f"Unknown SQL statement: {sql[:50]}")
    
    def _parse_create_table(self, sql: str) -> tuple:
        """Parse CREATE TABLE statement"""
        # Extract table name and columns
        match_start = sql.upper().find('CREATE TABLE') + len('CREATE TABLE')
        match_end = sql.find('(')
        
        table_name = sql[match_start:match_end].strip()
        
        # Extract column definitions
        columns_str = sql[sql.find('(')+1:sql.rfind(')')].strip()
        
        schema = {}
        for col_def in columns_str.split(','):
            col_def = col_def.strip()
            if 'FOREIGN KEY' in col_def.upper() or 'PRIMARY KEY' in col_def.upper():
                continue
            
            parts = col_def.split()
            col_name = parts[0]
            col_type = parts[1] if len(parts) > 1 else 'VARCHAR'
            schema[col_name] = col_type
        
        return table_name, schema
    
    def _create_table(self, sql: str) -> List:
        """Create a new table"""
        table_name, schema = self._parse_create_table(sql)
        
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists")
        
        self.tables[table_name] = []
        self.schemas[table_name] = schema
        self.save_to_file()
        
        return []
    
    def _parse_insert(self, sql: str) -> tuple:
        """Parse INSERT statement"""
        # Extract table name
        insert_start = sql.upper().find('INSERT INTO') + len('INSERT INTO')
        insert_end = sql.upper().find('VALUES')
        table_name = sql[insert_start:insert_end].strip().split('(')[0].strip()
        
        # Extract columns if specified
        col_start = sql.find('(')
        col_end = sql.find(')', col_start)
        columns = None
        if col_start < insert_end:
            columns = [c.strip() for c in sql[col_start+1:col_end].split(',')]
        
        # Extract values
        val_start = sql.upper().find('VALUES') + len('VALUES')
        val_start = sql.find('(', val_start)
        val_end = sql.rfind(')')
        values_str = sql[val_start+1:val_end].strip()
        
        # Parse values (simple parsing)
        values = []
        current = ""
        in_quotes = False
        for char in values_str:
            if char == "'" and (not current or current[-1] != "\\"):
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                values.append(current.strip())
                current = ""
                continue
            current += char
        values.append(current.strip())
        
        # Clean up values
        cleaned_values = []
        for val in values:
            val = val.strip()
            if val.startswith("'") and val.endswith("'"):
                val = val[1:-1]
            cleaned_values.append(val)
        
        return table_name, columns, cleaned_values
    
    def _insert(self, sql: str) -> List:
        """Insert a row into table"""
        table_name, columns, values = self._parse_insert(sql)
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        schema = self.schemas[table_name]
        
        # Create row
        if columns:
            row = {col: val for col, val in zip(columns, values)}
        else:
            row = {col: val for col, val in zip(schema.keys(), values)}
        
        # Type conversion
        for key, val in row.items():
            if val.upper() == 'NULL':
                row[key] = None
            elif val.upper() == 'TRUE':
                row[key] = True
            elif val.upper() == 'FALSE':
                row[key] = False
            elif key in schema:
                if 'INT' in schema[key].upper():
                    try:
                        row[key] = int(val)
                    except:
                        pass
        
        self.tables[table_name].append(row)
        self.save_to_file()
        
        return []
    
    def _select(self, sql: str) -> List[Dict]:
        """Select rows from table"""
        # Parse SELECT statement (simple implementation)
        sql_upper = sql.upper()
        
        # Extract FROM clause
        from_pos = sql_upper.find('FROM')
        where_pos = sql_upper.find('WHERE')
        order_pos = sql_upper.find('ORDER BY')
        limit_pos = sql_upper.find('LIMIT')
        
        if from_pos == -1:
            raise ValueError("SELECT statement must have FROM clause")
        
        # Get table name
        from_end = where_pos if where_pos != -1 else (order_pos if order_pos != -1 else (limit_pos if limit_pos != -1 else len(sql)))
        table_name = sql[from_pos+5:from_end].strip()
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        # Get all rows
        rows = [row.copy() for row in self.tables[table_name]]
        
        # Apply WHERE clause
        if where_pos != -1:
            where_end = order_pos if order_pos != -1 else (limit_pos if limit_pos != -1 else len(sql))
            where_clause = sql[where_pos+5:where_end].strip()
            rows = self._apply_where(rows, where_clause)
        
        # Apply ORDER BY
        if order_pos != -1:
            order_end = limit_pos if limit_pos != -1 else len(sql)
            order_clause = sql[order_pos+8:order_end].strip()
            rows = self._apply_order_by(rows, order_clause)
        
        # Apply LIMIT
        if limit_pos != -1:
            limit_str = sql[limit_pos+5:].strip()
            limit_num = int(limit_str.split()[0])
            rows = rows[:limit_num]
        
        return rows
    
    def _apply_where(self, rows: List[Dict], where_clause: str) -> List[Dict]:
        """Filter rows based on WHERE clause"""
        filtered = []
        for row in rows:
            if self._evaluate_where(row, where_clause):
                filtered.append(row)
        return filtered
    
    def _evaluate_where(self, row: Dict, where_clause: str) -> bool:
        """Evaluate WHERE clause for a row"""
        # Simple evaluation (handle basic comparisons)
        for op in ['=', '!=', '<', '>', '<=', '>=']:
            if op in where_clause:
                parts = where_clause.split(op)
                if len(parts) == 2:
                    left = parts[0].strip()
                    right = parts[1].strip()
                    
                    # Get value from row
                    left_val = row.get(left)
                    
                    # Parse right value
                    right_val = right
                    if right_val.startswith("'") and right_val.endswith("'"):
                        right_val = right_val[1:-1]
                    
                    # Compare
                    if op == '=':
                        return left_val == right_val
                    elif op == '!=':
                        return left_val != right_val
                    elif op == '<':
                        return left_val < right_val
                    elif op == '>':
                        return left_val > right_val
                    elif op == '<=':
                        return left_val <= right_val
                    elif op == '>=':
                        return left_val >= right_val
        
        return True
    
    def _apply_order_by(self, rows: List[Dict], order_clause: str) -> List[Dict]:
        """Sort rows based on ORDER BY clause"""
        # Extract column name and direction
        parts = order_clause.split()
        col_name = parts[0]
        reverse = len(parts) > 1 and parts[1].upper() == 'DESC'
        
        try:
            return sorted(rows, key=lambda x: x.get(col_name, ''), reverse=reverse)
        except:
            return rows
    
    def _update(self, sql: str) -> List:
        """Update rows in table"""
        # Parse UPDATE statement
        sql_upper = sql.upper()
        update_pos = sql_upper.find('UPDATE') + len('UPDATE')
        set_pos = sql_upper.find('SET')
        where_pos = sql_upper.find('WHERE')
        
        table_name = sql[update_pos:set_pos].strip()
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        # Parse SET clause
        set_end = where_pos if where_pos != -1 else len(sql)
        set_clause = sql[set_pos+3:set_end].strip()
        
        # Parse updates
        updates = {}
        for assignment in set_clause.split(','):
            col, val = assignment.split('=')
            col = col.strip()
            val = val.strip()
            
            if val.startswith("'") and val.endswith("'"):
                val = val[1:-1]
            
            updates[col] = val
        
        # Apply WHERE clause and update
        if where_pos != -1:
            where_clause = sql[where_pos+5:].strip()
            for row in self.tables[table_name]:
                if self._evaluate_where(row, where_clause):
                    row.update(updates)
        else:
            for row in self.tables[table_name]:
                row.update(updates)
        
        self.save_to_file()
        return []
    
    def _delete(self, sql: str) -> List:
        """Delete rows from table"""
        sql_upper = sql.upper()
        from_pos = sql_upper.find('FROM') + len('FROM')
        where_pos = sql_upper.find('WHERE')
        
        table_name = sql[from_pos:where_pos if where_pos != -1 else len(sql)].strip()
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        # Apply WHERE clause
        if where_pos != -1:
            where_clause = sql[where_pos+5:].strip()
            self.tables[table_name] = [
                row for row in self.tables[table_name]
                if not self._evaluate_where(row, where_clause)
            ]
        else:
            self.tables[table_name] = []
        
        self.save_to_file()
        return []

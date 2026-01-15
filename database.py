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
        self.primary_keys: Dict[str, str] = {}  # table_name -> column_name
        self.unique_columns: Dict[str, List[str]] = {}  # table_name -> [column_names]
        self.indexes: Dict[str, Dict[str, Dict]] = {}  # table_name -> column -> {value: [row_indices]}
        self.load_from_file()
    
    def load_from_file(self):
        """Load database from JSON file if exists"""
        if os.path.exists(self.db_file):
            try:
                with open(self.db_file, 'r') as f:
                    data = json.load(f)
                    self.tables = data.get('tables', {})
                    self.schemas = data.get('schemas', {})
                    self.primary_keys = data.get('primary_keys', {})
                    self.unique_columns = data.get('unique_columns', {})
            except:
                pass
        
        # Initialize indexes for all tables
        for table_name in self.tables:
            if table_name not in self.indexes:
                self.indexes[table_name] = {}
                primary_key = self.primary_keys.get(table_name)
                if primary_key:
                    self.indexes[table_name][primary_key] = {}
                for col in self.unique_columns.get(table_name, []):
                    self.indexes[table_name][col] = {}
    
    def save_to_file(self):
        """Save database to JSON file"""
        with open(self.db_file, 'w') as f:
            json.dump({
                'tables': self.tables,
                'schemas': self.schemas,
                'primary_keys': self.primary_keys,
                'unique_columns': self.unique_columns
            }, f, indent=2, default=str)
    
    def insert_row(self, table_name: str, data: Dict[str, Any]) -> None:
        """Insert a row directly without SQL parsing - avoids concatenation issues"""
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        schema = self.schemas[table_name]
        
        # Convert values based on schema
        row = {}
        for col, val in data.items():
            if col in schema:
                col_type = schema[col].upper()
                
                # Handle None/NULL
                if val is None:
                    row[col] = None
                # Handle booleans
                elif isinstance(val, bool):
                    row[col] = val
                # Handle integers
                elif isinstance(val, int):
                    row[col] = val
                # Handle strings
                elif isinstance(val, str):
                    val = val.strip()
                    if val.upper() == 'NULL':
                        row[col] = None
                    elif val.upper() == 'TRUE':
                        row[col] = True
                    elif val.upper() == 'FALSE':
                        row[col] = False
                    elif 'INT' in col_type:
                        try:
                            row[col] = int(val)
                        except:
                            row[col] = val
                    else:
                        row[col] = val
                else:
                    row[col] = val
        
        # Validate PRIMARY KEY constraint
        primary_key = self.primary_keys.get(table_name)
        if primary_key and primary_key in row:
            pk_value = row[primary_key]
            pk_value_str = str(pk_value) if pk_value is not None else "NULL"
            for existing_row in self.tables[table_name]:
                if existing_row.get(primary_key) == pk_value:
                    raise ValueError(f"PRIMARY KEY constraint violated: {primary_key}={pk_value_str} already exists")
        
        # Validate UNIQUE constraints
        unique_cols = self.unique_columns.get(table_name, [])
        for col in unique_cols:
            if col in row:
                unique_value = row[col]
                unique_value_str = str(unique_value) if unique_value is not None else "NULL"
                for existing_row in self.tables[table_name]:
                    if existing_row.get(col) == unique_value:
                        raise ValueError(f"UNIQUE constraint violated: {col}={unique_value_str} already exists")
        
        self.tables[table_name].append(row)
        
        # Update indexes (safely initialize if needed)
        if table_name not in self.indexes:
            self.indexes[table_name] = {}
        
        if primary_key and primary_key in row:
            if primary_key not in self.indexes[table_name]:
                self.indexes[table_name][primary_key] = {}
            self.indexes[table_name][primary_key][row[primary_key]] = len(self.tables[table_name]) - 1
        
        for col in unique_cols:
            if col in row:
                if col not in self.indexes[table_name]:
                    self.indexes[table_name][col] = {}
                self.indexes[table_name][col][row[col]] = len(self.tables[table_name]) - 1
        
        self.save_to_file()
    
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
        primary_key = None
        unique_columns = []
        
        for col_def in columns_str.split(','):
            col_def = col_def.strip()
            
            # Check for PRIMARY KEY
            if 'PRIMARY KEY' in col_def.upper():
                if '(' in col_def:
                    pk_col = col_def.split('(')[1].split(')')[0].strip()
                    primary_key = pk_col
                continue
            
            # Check for UNIQUE
            if 'UNIQUE' in col_def.upper():
                if 'FOREIGN' not in col_def.upper():
                    parts = col_def.split()
                    unique_columns.append(parts[0])
                    col_def = col_def.replace('UNIQUE', '').strip()
            
            # Skip FOREIGN KEY
            if 'FOREIGN KEY' in col_def.upper():
                continue
            
            # Parse column name and type
            parts = col_def.split()
            if parts:
                col_name = parts[0]
                col_type = parts[1] if len(parts) > 1 else 'VARCHAR'
                
                # Check for PRIMARY KEY inline
                if 'PRIMARY' in col_def.upper():
                    primary_key = col_name
                
                # Check for UNIQUE inline
                if 'UNIQUE' in col_def.upper():
                    unique_columns.append(col_name)
                
                schema[col_name] = col_type
        
        return table_name, schema, primary_key, unique_columns
    
    def _create_table(self, sql: str) -> List:
        """Create a new table"""
        table_name, schema, primary_key, unique_columns = self._parse_create_table(sql)
        
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists")
        
        self.tables[table_name] = []
        self.schemas[table_name] = schema
        self.primary_keys[table_name] = primary_key
        self.unique_columns[table_name] = unique_columns
        self.indexes[table_name] = {}
        
        # Create indexes for primary and unique columns
        if primary_key:
            self.indexes[table_name][primary_key] = {}
        for col in unique_columns:
            self.indexes[table_name][col] = {}
        
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
        
        # Keep values as strings for now - don't convert to int
        return table_name, columns, values
    
    def _insert(self, sql: str) -> List:
        """Insert a row into table"""
        table_name, columns, values = self._parse_insert(sql)
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        schema = self.schemas[table_name]
        
        # Create row with proper type conversion
        if columns:
            row = {}
            for col, val in zip(columns, values):
                row[col] = self._convert_value(val, col, schema)
        else:
            row = {}
            for col, val in zip(schema.keys(), values):
                row[col] = self._convert_value(val, col, schema)
        
        # Validate PRIMARY KEY constraint
        primary_key = self.primary_keys.get(table_name)
        if primary_key and primary_key in row:
            pk_value = row[primary_key]
            for existing_row in self.tables[table_name]:
                if existing_row.get(primary_key) == pk_value:
                    raise ValueError(f"PRIMARY KEY constraint violated: {primary_key}={pk_value} already exists")
        
        # Validate UNIQUE constraints
        unique_cols = self.unique_columns.get(table_name, [])
        for col in unique_cols:
            if col in row:
                unique_value = row[col]
                for existing_row in self.tables[table_name]:
                    if existing_row.get(col) == unique_value:
                        raise ValueError(f"UNIQUE constraint violated: {col}={unique_value} already exists")
        
        self.tables[table_name].append(row)
        
        # Update indexes (safely initialize if needed)
        if table_name not in self.indexes:
            self.indexes[table_name] = {}
        
        if primary_key and primary_key in row:
            if primary_key not in self.indexes[table_name]:
                self.indexes[table_name][primary_key] = {}
            self.indexes[table_name][primary_key][row[primary_key]] = len(self.tables[table_name]) - 1
        
        for col in unique_cols:
            if col in row:
                if col not in self.indexes[table_name]:
                    self.indexes[table_name][col] = {}
                self.indexes[table_name][col][row[col]] = len(self.tables[table_name]) - 1
        
        self.save_to_file()
        
        return []
    
    def _convert_value(self, val: str, col_name: str, schema: dict):
        """Convert value to appropriate type based on schema"""
        if isinstance(val, str):
            val = val.strip()
            # Check for NULL
            if val.upper() == 'NULL':
                return None
            # Check for BOOLEAN
            elif val.upper() == 'TRUE':
                return True
            elif val.upper() == 'FALSE':
                return False
            # Check schema type
            elif col_name in schema:
                col_type = schema[col_name].upper()
                if 'INT' in col_type:
                    try:
                        return int(val)
                    except:
                        return val
            # Return as string
            return val
        else:
            # Already converted (should not happen with current logic)
            return val
    
    def _select(self, sql: str) -> List[Dict]:
        """Select rows from table with JOIN support"""
        sql_upper = sql.upper()
        
        # Check for JOIN
        if 'JOIN' in sql_upper:
            return self._select_with_join(sql)
        
        # Simple SELECT without JOIN
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
    
    def _select_with_join(self, sql: str) -> List[Dict]:
        """Select rows with JOIN"""
        sql_upper = sql.upper()
        
        # Extract main table
        from_pos = sql_upper.find('FROM')
        join_pos = sql_upper.find('JOIN')
        where_pos = sql_upper.find('WHERE')
        
        main_table = sql[from_pos+5:join_pos].strip()
        
        # Extract JOIN type and table
        join_end = where_pos if where_pos != -1 else len(sql)
        join_clause = sql[join_pos:join_end].strip()
        
        # Determine join type
        join_type = 'INNER'
        if 'LEFT' in join_clause.upper():
            join_type = 'LEFT'
        elif 'INNER' in join_clause.upper():
            join_type = 'INNER'
        
        # Extract joined table and ON condition
        on_pos = join_clause.upper().find('ON')
        join_table = join_clause[:on_pos].replace('JOIN', '').replace('LEFT', '').replace('INNER', '').strip()
        on_condition = join_clause[on_pos+2:].strip()
        
        # Get rows from main table
        main_rows = [row.copy() for row in self.tables[main_table]]
        join_rows = [row.copy() for row in self.tables[join_table]]
        
        # Parse ON condition (e.g., "users.id = posts.user_id")
        if '=' in on_condition:
            left_col, right_col = on_condition.split('=')
            left_table, left_field = left_col.strip().split('.')
            right_table, right_field = right_col.strip().split('.')
            
            # Perform join
            result = []
            for main_row in main_rows:
                for join_row in join_rows:
                    if main_row.get(left_field) == join_row.get(right_field):
                        # Merge rows with table prefix
                        merged = {}
                        for k, v in main_row.items():
                            merged[f"{main_table}.{k}"] = v
                        for k, v in join_row.items():
                            merged[f"{join_table}.{k}"] = v
                        result.append(merged)
                    elif join_type == 'LEFT' and not any(main_row.get(left_field) == jr.get(right_field) for jr in join_rows):
                        # LEFT JOIN: include unmatched rows from left table
                        merged = {}
                        for k, v in main_row.items():
                            merged[f"{main_table}.{k}"] = v
                        for k in self.schemas[join_table]:
                            merged[f"{join_table}.{k}"] = None
                        result.append(merged)
            
            return result
        
        raise ValueError("Invalid JOIN syntax")
    
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
                parts = where_clause.split(op, 1)  # Split only on first occurrence
                if len(parts) == 2:
                    left = parts[0].strip()
                    right = parts[1].strip()
                    
                    # Get value from row
                    left_val = row.get(left)
                    
                    # Parse right value
                    right_val = right
                    if right_val.startswith("'") and right_val.endswith("'"):
                        right_val = right_val[1:-1]
                    else:
                        # Try to convert to int if it's a number
                        try:
                            right_val = int(right_val)
                        except:
                            pass
                    
                    # Ensure both values are comparable type
                    if isinstance(left_val, int) and isinstance(right_val, str):
                        try:
                            right_val = int(right_val)
                        except:
                            left_val = str(left_val)
                    elif isinstance(left_val, str) and isinstance(right_val, int):
                        try:
                            left_val = int(left_val)
                        except:
                            right_val = str(right_val)
                    
                    # Compare
                    try:
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
                    except TypeError:
                        # If comparison fails, return False
                        return False
        
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
        
        # Parse updates with proper type conversion
        schema = self.schemas.get(table_name, {})
        updates = {}
        for assignment in set_clause.split(','):
            parts = assignment.split('=', 1)
            if len(parts) == 2:
                col = parts[0].strip()
                val = parts[1].strip()
                
                # Remove quotes if present
                if val.startswith("'") and val.endswith("'"):
                    val = val[1:-1]
                
                # Convert value based on schema type
                if col in schema:
                    col_type = schema[col].upper()
                    if 'INT' in col_type:
                        try:
                            val = int(val) if val != 'NULL' else None
                        except:
                            pass
                    elif val.upper() == 'TRUE':
                        val = True
                    elif val.upper() == 'FALSE':
                        val = False
                    elif val.upper() == 'NULL':
                        val = None
                
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

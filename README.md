# MicroSQL RDBMS Web Application

## Overview

MicroSQL is a lightweight, file-based relational database management system built with Python. This project demonstrates a complete RDBMS implementation with a web interface for managing data and an interactive REPL for direct SQL queries.

## Features

- **Complete SQL Support**: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE
- **Constraint Enforcement**: PRIMARY KEY and UNIQUE constraints with validation
- **JOIN Operations**: INNER JOIN and LEFT JOIN support
- **Basic Indexing**: Automatic indexes on PRIMARY KEY and UNIQUE columns for faster lookups
- **Data Types**: INT, VARCHAR, BOOL, DATETIME, TEXT
- **WHERE Clauses**: Filter results with conditions
- **ORDER BY**: Sort results in ascending or descending order
- **LIMIT**: Limit result sets
- **File Persistence**: Database automatically saves to JSON file
- **Web Interface**: Flask-based web app for CRUD operations
- **Interactive REPL**: Command-line interface for direct database queries

## Project Structure

```
RDBMS/
├── microsql.py          # Main Flask web application
├── database.py          # MicroSQL RDBMS implementation
├── templates/           # HTML templates
│   ├── index.html       # Users list page
│   ├── create_user.html # Create user form
│   ├── edit_user.html   # Edit user form
│   └── posts.html       # Posts list page
├── webapp.json          # Database file (auto-generated)
└── README.md            # This file
```

## Installation

1. **Clone or navigate to the project directory**:
   ```bash
   cd c:\Users\user\Documents\RDBMS
   ```

2. **Create virtual environment** (if not already created):
   ```bash
   python -m venv .venv
   ```

3. **Activate virtual environment**:
   - **Windows**:
     ```bash
     .venv\Scripts\activate
     ```
   - **Mac/Linux**:
     ```bash
     source .venv/bin/activate
     ```

4. **Install dependencies**:
   ```bash
   pip install flask
   ```

## Running the Application

### 1. Web Interface (Flask)

```bash
python microsql.py
```

Then open your browser and navigate to `http://localhost:5000`

### 2. Interactive REPL Mode

```bash
python repl.py
# or
python microsql.py repl
```

This launches an interactive command-line interface where you can:
- Execute SQL queries directly
- View table schemas
- List all tables
- Get help with `.help` command

## Database Schema

### Users Table
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INT,
    is_active BOOL DEFAULT TRUE,
    created_at DATETIME
)
```

### Posts Table
```sql
CREATE TABLE posts (
    id INT PRIMARY KEY,
    user_id INT NOT NULL,
    title VARCHAR(200) NOT NULL,
    content TEXT,
    created_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id)
)
```

### Comments Table
```sql
CREATE TABLE comments (
    id INT PRIMARY KEY,
    post_id INT NOT NULL,
    user_id INT NOT NULL,
    comment TEXT NOT NULL,
    created_at DATETIME
)
```

## SQL Examples

### Create Table with Constraints
```sql
CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    salary INT,
    department VARCHAR(50)
)
```

### Insert Data
```sql
INSERT INTO employees (id, name, email, salary, department) 
VALUES (1, 'Alice', 'alice@example.com', 50000, 'Engineering')
```

### Select with WHERE
```sql
SELECT * FROM employees WHERE salary > 45000
```

### Select with ORDER BY
```sql
SELECT * FROM employees ORDER BY salary DESC
```

### Select with LIMIT
```sql
SELECT * FROM employees LIMIT 10
```

### JOIN Operations
```sql
-- INNER JOIN
SELECT users.username, posts.title 
FROM users 
JOIN posts ON users.id = posts.user_id

-- LEFT JOIN
SELECT users.username, posts.title 
FROM users 
LEFT JOIN posts ON users.id = posts.user_id
```

### Update Data
```sql
UPDATE employees SET salary = 55000 WHERE name = 'Alice'
```

### Delete Data
```sql
DELETE FROM employees WHERE id = 1
```

## REPL Commands

In interactive REPL mode, use special commands prefixed with `.`:

- `.help` - Show help message
- `.tables` - List all tables
- `.schema <table_name>` - Show table schema
- `.clear` - Clear screen
- `.exit` - Exit REPL

## Features Details

### PRIMARY KEY Constraint
- Enforces uniqueness on the specified column
- Prevents duplicate key values
- Automatically indexed for fast lookups

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    username VARCHAR(50)
)
```

### UNIQUE Constraint
- Ensures all values in the column are unique
- Multiple UNIQUE columns can exist per table
- Automatically indexed

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    username VARCHAR(50) UNIQUE
)
```

### Indexing
- Automatic indexes created for PRIMARY KEY and UNIQUE columns
- Faster lookups and constraint validation
- No manual index management required

### JOIN Support
- INNER JOIN: Returns only matching rows from both tables
- LEFT JOIN: Returns all rows from left table, matching rows from right table
- Requires explicit ON condition with column references

```sql
SELECT t1.col1, t2.col2 FROM table1 
JOIN table2 ON table1.id = table2.table1_id
```

# MicroSQL RDBMS Web Application

## Overview

MicroSQL is a lightweight, file-based relational database management system built with Python. This project demonstrates a simple RDBMS implementation with a web interface for managing users and posts.

## Features

- **Simple SQL Support**: Execute basic SQL queries (CREATE TABLE, INSERT, SELECT, UPDATE, DELETE)
- **File Persistence**: Database automatically saves to JSON file
- **Web Interface**: Flask-based web app for managing data
- **Tables**: Pre-configured with users, posts, and comments tables
- **CRUD Operations**: Create, read, update, and delete users and posts through the web interface

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

1. **Start the Flask web server**:
   ```bash
   python microsql.py
   ```

2. **Open your browser** and navigate to:
   ```
   http://localhost:5000
   ```

3. **Access the features**:
   - **Home Page**: View all users at `/`
   - **Create User**: Add new users at `/users/create`
   - **Edit User**: Update user information at `/users/<id>/edit`
   - **Delete User**: Remove users (button on home page)
   - **View Posts**: See all posts at `/posts`
   - **API Endpoint**: Get users as JSON at `/users`

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

## Executing Custom Queries

You can execute custom SQL queries through the `/api/query` endpoint:

```bash
curl -X POST http://localhost:5000/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users WHERE age > 25"}'
```

## Sample Data

The application automatically creates sample data on first run:

**Users:**
- alice (25 years old, active)
- bob (30 years old, active)
- charlie (22 years old, inactive)

**Posts:**
- "First Post" by alice
- "Second Post" by bob
- "Third Post" by alice

## Limitations

- Simple in-memory database with file persistence
- No advanced features like transactions or complex joins
- Basic SQL parsing and evaluation
- Single-threaded implementation

## Development

The application runs in debug mode by default, which means:
- Auto-reload on code changes
- Debugger PIN available for remote debugging
- Stack traces on errors

For production use, replace the development server with a proper WSGI server like Gunicorn.

## License

This is a demonstration project for learning RDBMS concepts.

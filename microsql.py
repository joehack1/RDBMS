"""
Simple Web Application using MicroSQL RDBMS
"""
from flask import Flask, render_template, request, redirect, jsonify
from werkzeug.routing import BaseConverter
import json

# Create Flask app
app = Flask(__name__, static_folder='static', static_url_path='/static')

# Initialize MicroSQL database
from database import MicroSQL

# Create database and sample tables
db = MicroSQL("webapp")

# Create tables if they don't exist
try:
    db.execute("""
        CREATE TABLE users (
            id INT PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            age INT,
            is_active BOOL DEFAULT TRUE,
            created_at DATETIME
        )
    """)
except:
    pass  # Table might already exist

try:
    db.execute("""
        CREATE TABLE posts (
            id INT PRIMARY KEY,
            user_id INT NOT NULL,
            title VARCHAR(200) NOT NULL,
            content TEXT,
            created_at DATETIME,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
except:
    pass  # Table might already exist

try:
    db.execute("""
        CREATE TABLE comments (
            id INT PRIMARY KEY,
            post_id INT NOT NULL,
            user_id INT NOT NULL,
            comment TEXT NOT NULL,
            created_at DATETIME
        )
    """)
except:
    pass  # Table might already exist

# Insert sample data if empty
if len(db.execute("SELECT * FROM users")) == 0:
    db.insert_row('users', {'id': '001', 'username': 'alice', 'email': 'alice@example.com', 'age': 25, 'is_active': True, 'created_at': '2024-01-01 10:00:00'})
    db.insert_row('users', {'id': '002', 'username': 'bob', 'email': 'bob@example.com', 'age': 30, 'is_active': True, 'created_at': '2024-01-02 11:00:00'})
    db.insert_row('users', {'id': '003', 'username': 'charlie', 'email': 'charlie@example.com', 'age': 22, 'is_active': False, 'created_at': '2024-01-03 12:00:00'})
    db.insert_row('users', {'id': '004', 'username': 'diana', 'email': 'diana@example.com', 'age': 28, 'is_active': True, 'created_at': '2024-01-04 13:00:00'})
    db.insert_row('users', {'id': '005', 'username': 'evan', 'email': 'evan@example.com', 'age': 35, 'is_active': True, 'created_at': '2024-01-05 14:00:00'})

if len(db.execute("SELECT * FROM posts")) == 0:
    db.insert_row('posts', {'id': '001', 'user_id': '001', 'title': 'First Post', 'content': 'Hello World! Welcome to my blog. I am excited to share my thoughts and experiences here.', 'created_at': '2024-01-04 09:00:00'})
    db.insert_row('posts', {'id': '002', 'user_id': '002', 'title': 'Second Post', 'content': 'Another day in paradise. Today was a great day with lots of learning opportunities.', 'created_at': '2024-01-05 10:00:00'})
    db.insert_row('posts', {'id': '003', 'user_id': '001', 'title': 'Third Post', 'content': 'Learning MicroSQL has been an amazing journey. The database system is powerful and flexible.', 'created_at': '2024-01-06 11:00:00'})
    db.insert_row('posts', {'id': '004', 'user_id': '003', 'title': 'My First Post', 'content': 'This is my first blog post. I am looking forward to sharing more content soon.', 'created_at': '2024-01-07 09:30:00'})
    db.insert_row('posts', {'id': '005', 'user_id': '004', 'title': 'Tech Talk', 'content': 'Technology is evolving rapidly. Stay tuned for more insights and discussions.', 'created_at': '2024-01-08 10:15:00'})
    db.insert_row('posts', {'id': '006', 'user_id': '005', 'title': 'Journey Begins', 'content': 'Starting my journey with database systems. Excited to learn and grow.', 'created_at': '2024-01-09 11:45:00'})

@app.route('/')
def index():
    """Home page - show all users"""
    users = db.execute("SELECT * FROM users ORDER BY id")
    return render_template('index.html', users=users)

@app.route('/users')
def list_users():
    """API endpoint to list all users"""
    users = db.execute("SELECT * FROM users ORDER BY id")
    return jsonify(users)

@app.route('/users/<int:user_id>')
def get_user(user_id):
    """Get a specific user"""
    users = db.execute(f"SELECT * FROM users WHERE id = {user_id}")
    if users:
        return jsonify(users[0])
    return jsonify({"error": "User not found"}), 404

@app.route('/users/create', methods=['GET', 'POST'])
def create_user():
    """Create a new user"""
    if request.method == 'POST':
        try:
            # Get form data
            username = request.form['username']
            email = request.form['email']
            age_input = request.form.get('age', '').strip()
            is_active = 'is_active' in request.form
            
            # Find next available ID
            users = db.execute("SELECT * FROM users ORDER BY id DESC LIMIT 1")
            if users and len(users) > 0:
                last_user = users[0]
                # Handle both dictionary and object-like access
                last_id = last_user.get('id') if isinstance(last_user, dict) else getattr(last_user, 'id', None)
                if last_id:
                    next_id_num = int(str(last_id).lstrip('0') or '0') + 1
                else:
                    next_id_num = 1
            else:
                next_id_num = 1
            
            next_id = f"{next_id_num:03d}"  # Format as zero-padded 3-digit (001, 002, etc.)
            
            # Parse age as integer or None
            age = int(age_input) if age_input else None
            
            # Build data dictionary with proper types
            user_data = {
                'id': next_id,
                'username': username,
                'email': email,
                'age': age,
                'is_active': is_active,
                'created_at': '2024-01-10 10:00:00'
            }
            
            # Insert using direct method to avoid SQL concatenation issues
            db.insert_row('users', user_data)
            
            return redirect('/')
        except Exception as e:
            return f"Error creating user: {e}", 400
    
    # Calculate next ID for display
    try:
        users = db.execute("SELECT * FROM users ORDER BY id DESC LIMIT 1")
        if users and len(users) > 0:
            last_user = users[0]
            # Handle both dictionary and object-like access
            last_id = last_user.get('id') if isinstance(last_user, dict) else getattr(last_user, 'id', None)
            if last_id:
                next_id_num = int(str(last_id).lstrip('0') or '0') + 1
            else:
                next_id_num = 1
        else:
            next_id_num = 1
    except Exception:
        next_id_num = 1
    
    next_id = f"{next_id_num:03d}"
    
    return render_template('create_user.html', next_id=next_id)

@app.route('/users/<int:user_id>/edit', methods=['GET', 'POST'])
def edit_user(user_id):
    """Edit an existing user"""
    if request.method == 'POST':
        try:
            username = request.form['username']
            email = request.form['email']
            age_input = request.form.get('age', '').strip()
            is_active = 'is_active' in request.form

            # Parse age as integer or None
            age = int(age_input) if age_input else None

            # Get current user data
            users = db.execute(f"SELECT * FROM users WHERE id = {user_id}")
            if not users:
                return "User not found", 404

            current_user = users[0]

            # Update the user data while preserving ID and created_at
            updated_user = dict(current_user)  # Create a copy
            updated_user['username'] = username
            updated_user['email'] = email
            updated_user['age'] = age
            updated_user['is_active'] = is_active

            # Delete old record and insert updated version
            db.execute(f"DELETE FROM users WHERE id = {user_id}")
            db.insert_row('users', updated_user)

            return redirect('/')
        except Exception as e:
            return f"Error updating user: {e}", 400

    # GET request - show edit form
    users = db.execute(f"SELECT * FROM users WHERE id = {user_id}")
    if not users:
        return "User not found", 404

    return render_template('edit_user.html', user=users[0])

@app.route('/users/<int:user_id>/delete', methods=['POST'])
def delete_user(user_id):
    """Delete a user"""
    try:
        db.execute(f"DELETE FROM users WHERE id = {user_id}")
        return redirect('/')
    except Exception as e:
        return f"Error deleting user: {e}", 400

@app.route('/posts')
def list_posts():
    """List all posts with user info (demonstrating join-like operation)"""
    posts = db.execute("SELECT * FROM posts ORDER BY created_at DESC")
    users = db.execute("SELECT * FROM users")
    
    # Manual join (since MicroSQL doesn't have automatic JOIN yet)
    user_dict = {}
    for user in users:
        user_id = user.get('id') or user.get('user_id')
        if user_id:
            user_dict[user_id] = user
    
    for post in posts:
        post['author'] = user_dict.get(post.get('user_id'), {'username': 'Unknown'})
    
    return render_template('posts.html', posts=posts)

@app.route('/api/query', methods=['POST'])
def execute_query():
    """Execute raw SQL query (for demonstration)"""
    if request.method == 'POST':
        sql = request.json.get('sql', '')
        try:
            result = db.execute(sql)
            return jsonify({'success': True, 'result': result})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    import sys
    
    # Check if REPL mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == 'repl':
        from repl import MicroSQLREPL
        repl = MicroSQLREPL('webapp')
        repl.run()
    else:
        print("Starting MicroSQL Web Application...")
        print("1. Web app available at: http://localhost:5000")
        print("2. For REPL mode, run: python microsql.py repl")
        
        # Start Flask app
        app.run(debug=True, port=5000)

"""
Simple Web Application using MicroSQL RDBMS
"""
from flask import Flask, render_template, request, redirect, jsonify
import json

# Create Flask app
app = Flask(__name__)

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
    from datetime import datetime
    db.execute("INSERT INTO users (id, username, email, age, is_active, created_at) VALUES (1, 'alice', 'alice@example.com', 25, TRUE, '2024-01-01 10:00:00')")
    db.execute("INSERT INTO users (id, username, email, age, is_active, created_at) VALUES (2, 'bob', 'bob@example.com', 30, TRUE, '2024-01-02 11:00:00')")
    db.execute("INSERT INTO users (id, username, email, age, is_active, created_at) VALUES (3, 'charlie', 'charlie@example.com', 22, FALSE, '2024-01-03 12:00:00')")

if len(db.execute("SELECT * FROM posts")) == 0:
    from datetime import datetime
    db.execute("INSERT INTO posts (id, user_id, title, content, created_at) VALUES (1, 1, 'First Post', 'Hello World!', '2024-01-04 09:00:00')")
    db.execute("INSERT INTO posts (id, user_id, title, content, created_at) VALUES (2, 2, 'Second Post', 'Another day in paradise', '2024-01-05 10:00:00')")
    db.execute("INSERT INTO posts (id, user_id, title, content, created_at) VALUES (3, 1, 'Third Post', 'Learning MicroSQL', '2024-01-06 11:00:00')")

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
            age = int(request.form['age']) if request.form['age'] else None
            is_active = 'is_active' in request.form
            
            # Find next available ID
            users = db.execute("SELECT * FROM users ORDER BY id DESC LIMIT 1")
            next_id = users[0]['id'] + 1 if users else 1
            
            # Insert user
            db.execute(f"""
                INSERT INTO users (id, username, email, age, is_active, created_at) 
                VALUES ({next_id}, '{username}', '{email}', {age if age else 'NULL'}, {is_active}, '2024-01-10 10:00:00')
            """)
            
            return redirect('/')
        except Exception as e:
            return f"Error creating user: {e}", 400
    
    return render_template('create_user.html')

@app.route('/users/<int:user_id>/edit', methods=['GET', 'POST'])
def edit_user(user_id):
    """Edit an existing user"""
    if request.method == 'POST':
        try:
            username = request.form['username']
            email = request.form['email']
            age = int(request.form['age']) if request.form['age'] else None
            is_active = 'is_active' in request.form
            
            db.execute(f"""
                UPDATE users 
                SET username = '{username}', email = '{email}', age = {age if age else 'NULL'}, is_active = {is_active}
                WHERE id = {user_id}
            """)
            
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
    user_dict = {user['id']: user for user in users}
    for post in posts:
        post['author'] = user_dict.get(post['user_id'], {'username': 'Unknown'})
    
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

# HTML Templates (inline for simplicity)
@app.route('/templates/<template_name>')
def serve_template(template_name):
    templates = {
        'index.html': '''
<!DOCTYPE html>
<html>
<head>
    <title>MicroSQL Web App - Users</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #4CAF50; color: white; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .btn { padding: 8px 12px; margin: 4px; text-decoration: none; border-radius: 4px; }
        .btn-primary { background-color: #4CAF50; color: white; }
        .btn-edit { background-color: #008CBA; color: white; }
        .btn-delete { background-color: #f44336; color: white; }
        .btn:hover { opacity: 0.8; }
    </style>
</head>
<body>
    <h1>Users</h1>
    <a href="/users/create" class="btn btn-primary">Create New User</a>
    <a href="/posts" class="btn">View Posts</a>
    <table>
        <tr>
            <th>ID</th>
            <th>Username</th>
            <th>Email</th>
            <th>Age</th>
            <th>Active</th>
            <th>Created At</th>
            <th>Actions</th>
        </tr>
        {% for user in users %}
        <tr>
            <td>{{ user.id }}</td>
            <td>{{ user.username }}</td>
            <td>{{ user.email }}</td>
            <td>{{ user.age if user.age else '' }}</td>
            <td>{{ 'Yes' if user.is_active else 'No' }}</td>
            <td>{{ user.created_at }}</td>
            <td>
                <a href="/users/{{ user.id }}/edit" class="btn btn-edit">Edit</a>
                <form action="/users/{{ user.id }}/delete" method="POST" style="display: inline;">
                    <button type="submit" class="btn btn-delete" onclick="return confirm('Delete user?')">Delete</button>
                </form>
            </td>
        </tr>
        {% endfor %}
    </table>
</body>
</html>
        ''',
        
        'create_user.html': '''
<!DOCTYPE html>
<html>
<head>
    <title>Create User</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        form { max-width: 400px; }
        label { display: block; margin-top: 10px; }
        input[type="text"], input[type="email"], input[type="number"] {
            width: 100%; padding: 8px; margin: 5px 0;
        }
        .btn { padding: 10px 15px; margin-top: 10px; }
    </style>
</head>
<body>
    <h1>Create New User</h1>
    <form method="POST">
        <label>Username:</label>
        <input type="text" name="username" required>
        
        <label>Email:</label>
        <input type="email" name="email" required>
        
        <label>Age:</label>
        <input type="number" name="age">
        
        <label>
            <input type="checkbox" name="is_active" checked>
            Active
        </label>
        
        <button type="submit" class="btn btn-primary">Create User</button>
        <a href="/" class="btn">Cancel</a>
    </form>
</body>
</html>
        ''',
        
        'edit_user.html': '''
<!DOCTYPE html>
<html>
<head>
    <title>Edit User</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        form { max-width: 400px; }
        label { display: block; margin-top: 10px; }
        input[type="text"], input[type="email"], input[type="number"] {
            width: 100%; padding: 8px; margin: 5px 0;
        }
        .btn { padding: 10px 15px; margin-top: 10px; }
    </style>
</head>
<body>
    <h1>Edit User</h1>
    <form method="POST">
        <label>Username:</label>
        <input type="text" name="username" value="{{ user.username }}" required>
        
        <label>Email:</label>
        <input type="email" name="email" value="{{ user.email }}" required>
        
        <label>Age:</label>
        <input type="number" name="age" value="{{ user.age if user.age else '' }}">
        
        <label>
            <input type="checkbox" name="is_active" {% if user.is_active %}checked{% endif %}>
            Active
        </label>
        
        <button type="submit" class="btn btn-primary">Update User</button>
        <a href="/" class="btn">Cancel</a>
    </form>
</body>
</html>
        ''',
        
        'posts.html': '''
<!DOCTYPE html>
<html>
<head>
    <title>Posts</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .post { border: 1px solid #ddd; padding: 20px; margin: 20px 0; }
        .post-title { font-size: 24px; color: #333; }
        .post-author { color: #666; font-style: italic; }
        .post-content { margin: 10px 0; }
        .post-date { color: #999; font-size: 14px; }
    </style>
</head>
<body>
    <h1>Posts</h1>
    <a href="/" class="btn">Back to Users</a>
    
    {% for post in posts %}
    <div class="post">
        <div class="post-title">{{ post.title }}</div>
        <div class="post-author">By {{ post.author.username }}</div>
        <div class="post-content">{{ post.content }}</div>
        <div class="post-date">{{ post.created_at }}</div>
    </div>
    {% endfor %}
</body>
</html>
        '''
    }
    
    if template_name in templates:
        return templates[template_name]
    return "Template not found", 404

if __name__ == '__main__':
    print("Starting MicroSQL Web Application...")
    print("1. Web app available at: http://localhost:5000")
    print("2. For REPL mode, run: python microsql.py")
    
    # Start Flask app
    app.run(debug=True, port=5000)

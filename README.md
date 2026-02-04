# YesDB

A lightweight relational database built from scratch in Python with SQL support and B-tree storage.

## Installation

```bash
pip install yesdb
```

## Quick Start

### Using the CLI

```bash
yesdb mydatabase.db
```

```sql
YesDB> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
YesDB> INSERT INTO users VALUES (NULL, 'Alice', 30);
YesDB> INSERT INTO users VALUES (NULL, 'Bob', 25);
YesDB> SELECT * FROM users;
YesDB> SELECT * FROM users WHERE age > 26;
```

### Using as a Library

```python
from chidb import YesDB

# Create/open database
db = YesDB('myapp.db')

# Create table
db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)')

# Insert data
db.execute("INSERT INTO products VALUES (NULL, 'Laptop', 999.99)")
db.execute("INSERT INTO products VALUES (NULL, 'Mouse', 29.99)")

# Query data
results = db.execute('SELECT * FROM products WHERE price < 100')
for row in results:
    print(row)

# Update & delete
db.execute("UPDATE products SET price = 899.99 WHERE name = 'Laptop'")
db.execute("DELETE FROM products WHERE price < 30")

# Close
db.close()
```

### Context Manager

```python
from chidb import YesDB

with YesDB('myapp.db') as db:
    db.execute('CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT)')
    db.execute("INSERT INTO tasks VALUES (NULL, 'Learn SQL')")
    results = db.execute('SELECT * FROM tasks')
```

## Features

- **SQL Support**: CREATE, SELECT, INSERT, UPDATE, DELETE, DROP, ALTER TABLE
- **Data Types**: INTEGER, TEXT, REAL, BLOB
- **Query Features**: WHERE, ORDER BY, LIMIT, OFFSET, DISTINCT
- **B-Tree Storage**: Efficient indexing and data retrieval
- **No Dependencies**: Pure Python implementation
- **Interactive Shell**: Built-in SQL shell
- **Auto-increment**: PRIMARY KEY auto-increment support

## SQL Examples

```sql
-- Create table
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT,
    age INTEGER
)

-- Insert data
INSERT INTO users VALUES (NULL, 'Alice', 'alice@example.com', 30)
INSERT INTO users VALUES (NULL, 'Bob', 'bob@example.com', 25)

-- Query with conditions
SELECT * FROM users WHERE age > 25
SELECT name, email FROM users WHERE name = 'Alice'

-- Order and limit
SELECT * FROM users ORDER BY age DESC
SELECT * FROM users LIMIT 10 OFFSET 5

-- Update and delete
UPDATE users SET age = 31 WHERE name = 'Alice'
DELETE FROM users WHERE age < 18

-- Alter table
ALTER TABLE users ADD COLUMN country TEXT

-- Drop table
DROP TABLE users
```

## Shell Commands

```
.help          Show help
.tables        List all tables
.schema        Show table schemas
.exit          Exit shell (.quit also works)
```

## Setup Guide

### 1. Install YesDB

```bash
pip install yesdb
```

### 2. Verify Installation

```bash
# Check CLI works
yesdb --help

# Test Python import
python -c "from chidb import YesDB; print('YesDB installed successfully!')"
```

### 3. Create Your First Database

**Option A: Using CLI**

```bash
# Start the shell
yesdb my_database.db

# You'll see:
# YesDB version 0.1.0
# Enter ".help" for usage hints
# YesDB>

# Try some commands:
CREATE TABLE test (id INTEGER PRIMARY KEY, message TEXT);
INSERT INTO test VALUES (NULL, 'Hello World');
SELECT * FROM test;
.exit
```

**Option B: Using Python**

```python
from chidb import YesDB

# Create database
db = YesDB('my_database.db')

# Create table
db.execute('''
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        username TEXT,
        created_at INTEGER
    )
''')

# Insert data
import time
db.execute(f"INSERT INTO users VALUES (NULL, 'admin', {int(time.time())})")

# Query
users = db.execute('SELECT * FROM users')
print(f"Found {len(users)} users")

db.close()
```

### 4. Working with Your Application

```python
# app.py
from chidb import YesDB

class UserDatabase:
    def __init__(self, db_path='users.db'):
        self.db = YesDB(db_path)
        self._init_schema()

    def _init_schema(self):
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                email TEXT
            )
        ''')

    def add_user(self, username, email):
        self.db.execute(f"INSERT INTO users VALUES (NULL, '{username}', '{email}')")

    def get_user(self, username):
        results = self.db.execute(f"SELECT * FROM users WHERE username = '{username}'")
        return results[0] if results else None

    def close(self):
        self.db.close()

# Usage
db = UserDatabase()
db.add_user('alice', 'alice@example.com')
user = db.get_user('alice')
db.close()
```

## Security & Limitations

### Security Features
- Path validation (blocks system file access)
- Resource limits (SQL length, record size)
- Input validation (table/column names)

### Important Limitations
- **Single-user only**: Not designed for concurrent access
- **No encryption**: Data stored in plaintext
- **No authentication**: File access = database access
- **Local only**: Not a client-server database

### Recommended Use Cases
✅ Single-user desktop applications
✅ Development and prototyping
✅ Data analysis scripts
✅ Educational projects
✅ Embedded applications

❌ Multi-user web applications
❌ Concurrent access scenarios
❌ Sensitive data (without encryption)
❌ Production systems with high security requirements

### Best Practices

```python
from chidb import YesDB

# ✅ Good: Use in trusted environments
db = YesDB('local_data.db')

# ✅ Good: Validate user input
safe_input = user_input.replace("'", "''")
db.execute(f"INSERT INTO logs VALUES (NULL, '{safe_input}')")

# ❌ Avoid: Don't use with untrusted file paths
# ❌ Avoid: Don't store passwords in plaintext
```

## Development

### Install from Source

```bash
git clone https://github.com/AzharAhmed-bot/yes_db.git
cd yesdb
pip install -e .
```

### Run Tests

```bash
pip install pytest
pytest
```

## Requirements

- Python 3.8+
- No external dependencies

## License

MIT License - see [LICENSE](LICENSE) file

## Contributing

Contributions welcome! This is an educational project focused on learning database internals.

## Links

- **PyPI**: https://pypi.org/project/yesdb/
- **GitHub**: https://github.com/AzharAhmed-bot/yesdb
- **Issues**: https://github.com/AzharAhmed-bot/yesdb/issues

## Version

Current version: **0.1.1**

---

**Note**: YesDB is an educational database. For production systems, consider using SQLite, PostgreSQL, or MySQL.

# YesDB

A lightweight relational database built from scratch in Python with SQL support, B-tree storage, and a cloud Backend-as-a-Service for students.

## Installation

```bash
# Local only
pip install yesdb

# With cloud support
pip install yesdb[cloud]
```

## Quick Start

### Local Mode

```python
from yesdb import connect

db = connect("myapp.db")
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
db.execute("INSERT INTO users VALUES (NULL, 'Alice', 30)")
results = db.execute("SELECT * FROM users")
for row in results:
    print(row)
db.close()
```

### Cloud Mode

YesDB Cloud lets you host your database on a remote server. Perfect for student projects that need a real backend.

#### 1. Sign up and create a database

```bash
yesdb signup
# Email: student@uni.edu
# Password: ********
# -> Account created! Logged in.

yesdb init myproject
# -> Created yesdb/ folder with schema.py
# -> Database "myproject" created on cloud.
```

#### 2. Define your schema

After running `yesdb init`, you'll have a `yesdb/schema.py` file in your project. Edit it:

```python
# yesdb/schema.py
from yesdb import Table, Column, Integer, Text, Real

users = Table("users", [
    Column("id", Integer, primary_key=True),
    Column("name", Text),
    Column("email", Text),
])

products = Table("products", [
    Column("id", Integer, primary_key=True),
    Column("name", Text),
    Column("price", Real),
])
```

#### 3. Push your schema to the cloud

```bash
yesdb push
# -> Connecting to myproject database...
# -> Table "users" created
# -> Table "products" created
# -> Schema synced. 2 tables pushed.
```

#### 4. Use in your code

```python
from yesdb import connect

db = connect("myproject")  # uses saved credentials automatically

db.execute("INSERT INTO users VALUES (NULL, 'Alice', 'alice@uni.edu')")
db.execute("INSERT INTO users VALUES (NULL, 'Bob', 'bob@uni.edu')")

rows = db.execute("SELECT * FROM users")
for row in rows:
    print(row)
```

Every response includes the database engine's internal logs (B-tree operations, SQL parsing, page allocations), so you can see exactly what's happening under the hood.

#### 5. Use with FastAPI or Flask

```python
# main.py
from fastapi import FastAPI
from yesdb import connect

app = FastAPI()
db = connect("myproject")

@app.get("/users")
def get_users():
    rows = db.execute("SELECT * FROM users")
    return {"users": [list(row) for row in rows]}

@app.post("/users")
def create_user(name: str, email: str):
    db.execute(f"INSERT INTO users VALUES (NULL, '{name}', '{email}')")
    return {"status": "created"}
```

### CLI Commands

```bash
yesdb signup              # Create an account
yesdb login               # Login to existing account
yesdb init <db_name>      # Initialize a project with a cloud database
yesdb push                # Push schema.py to the cloud
yesdb databases           # List your databases
yesdb shell <db_name>     # Interactive SQL shell against a cloud database
```

### Local CLI Shell

```bash
yesdb-local mydatabase.db
```

```sql
YesDB> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
YesDB> INSERT INTO users VALUES (NULL, 'Alice', 30);
YesDB> SELECT * FROM users;
```

```
.help          Show help
.tables        List all tables
.schema        Show table schemas
.exit          Exit shell
```

## Features

- **SQL Support**: CREATE, SELECT, INSERT, UPDATE, DELETE, DROP, ALTER TABLE
- **Data Types**: INTEGER, TEXT, REAL, BLOB
- **Query Features**: WHERE, ORDER BY, LIMIT, OFFSET, DISTINCT
- **B-Tree Storage**: Efficient indexing and data retrieval
- **No Dependencies**: Pure Python implementation (local mode)
- **Cloud BaaS**: Host your database remotely with a single command
- **Schema DSL**: Define tables in Python, push to cloud
- **Engine Logs**: See B-tree splits, page allocations, and SQL parsing in every response
- **Interactive Shell**: Built-in SQL shell (local and cloud)
- **Auto-increment**: PRIMARY KEY auto-increment support

## SQL Examples

```sql
-- Create table
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)

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

## How Cloud Mode Works

```
Your machine                   YesDB Cloud Server
────────────────               ──────────────────
yesdb CLI (signup/login/push)   ┌─────────────────┐
   <-> HTTPS                    │ nginx (SSL)      │
yesdb SDK (connect/execute)     │  └─ FastAPI      │
                                │     ├─ auth      │
                                │     └─ data/     │
                                │       ├─ user1/  │
                                │       │  └─ *.db │
                                │       └─ user2/  │
                                │          └─ *.db │
                                └─────────────────┘
```

- Each user gets their own isolated account with multiple databases
- All traffic is encrypted over HTTPS
- Authentication via API key (generated at signup, saved locally)
- Database engine logs are returned with every query for full transparency

## Security

### Local Mode
- Path validation (blocks system file access)
- Resource limits (SQL length, record size)
- Input validation (table/column names)

### Cloud Mode
- HTTPS encryption (TLS via Let's Encrypt)
- API key authentication (SHA-256 hashed, never stored in plaintext)
- Password hashing (bcrypt)
- Per-user data isolation
- Request size limits

## Development

### Install from Source

```bash
git clone https://github.com/AzharAhmed-bot/yesdb.git
cd yes_db
pip install -e ".[cloud]"
```

### Run Tests

```bash
pip install pytest
pytest
```

## Requirements

- Python 3.8+
- No external dependencies (local mode)
- `requests` (cloud mode, installed with `pip install yesdb[cloud]`)

## License

MIT License - see [LICENSE](LICENSE) file

## Links

- **PyPI**: https://pypi.org/project/yesdb/
- **GitHub**: https://github.com/AzharAhmed-bot/yesdb
- **Issues**: https://github.com/AzharAhmed-bot/yesdb/issues

## Version

Current version: **0.1.5**

### Changelog

#### v0.1.5 (bug fixes)
- **Fix**: `from yesdb import connect` now works correctly. A `yesdb` compatibility package is included so the import matches the PyPI package name.
- **Fix**: `Table.to_sql()` now emits the `PRIMARY KEY` constraint in the generated `CREATE TABLE` SQL, so auto-increment works as expected when inserting `NULL` into a primary key column.

---

**Note**: YesDB is an educational database built from scratch to teach database internals. For production systems, consider SQLite, PostgreSQL, or MySQL.

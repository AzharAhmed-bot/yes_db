# yes_db (chidb-python)

**yes_db** is a Python-based educational relational database system inspired by `chidb`. It is designed to teach the internal workings of relational databases and can be embedded into Python applications.

---

## Features

- Single-file storage (`.cdb`) using fixed-size pages
- Table and index B-Trees for efficient data storage and lookup
- Database Virtual Machine (DBM) executing low-level instructions generated from SQL
- Supports basic SQL statements: `CREATE TABLE`, `CREATE INDEX`, `INSERT`, `SELECT`
- Layered architecture separating concerns:
  - Pager: manages file I/O and pages
  - Record: handles serialization/deserialization of table rows
  - B-Tree: implements table and index structures with search, insert, split, and iteration
  - DBM: executes database instructions
  - SQL Layer: lexer, parser, optimizer, code generator
  - API: exposes database functionality to Python programs
  - CLI Shell: allows interactive SQL queries

---

## Architecture Overview

1. **Pager**: Reads and writes pages to disk, maintains in-memory cache.
2. **Record Module**: Defines how table rows are stored and retrieved.
3. **B-Tree Engine**: Organizes table and index data, supports insertion, splitting, and searching.
4. **Database Machine (DBM)**: Executes instructions such as OpenRead, OpenWrite, Insert, Seek, Column, and ResultRow.
5. **SQL Layer**: Converts SQL statements into executable DBM programs.
6. **Public API**: Provides Python functions to open databases, prepare and execute SQL queries.
7. **CLI Shell**: Offers an interactive prompt for executing SQL statements and exploring the database.

---

## How to run the database in your CLI
- **No external packages needed for python**
``` bash
python3 -m chidb.cli.shell test.cdb
```

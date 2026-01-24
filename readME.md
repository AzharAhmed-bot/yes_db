# yes_db 

**yes_db** is a Python-based relational database system inspired by `chidb` that is currently under development. It is designed for the future! All Contributions are welcome !!

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

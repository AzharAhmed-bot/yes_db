"""
Cloud CLI commands for YesDB.

Provides subcommands for managing cloud databases:
    yesdb signup          Create an account
    yesdb login           Login to existing account
    yesdb init <db_name>  Initialize a project with a cloud database
    yesdb push            Push schema to the cloud
    yesdb databases       List your databases
    yesdb shell <db_name> Interactive SQL shell against a cloud database
"""

import argparse
import getpass
import importlib.util
import json
import os
import sys
from typing import Optional

try:
    import requests
except ImportError:
    requests = None

from chidb.client import (
    CloudConnection,
    ExecuteResult,
    load_credentials,
    save_credentials,
    CREDENTIALS_PATH,
)
from chidb.schema import Table, collect_tables


DEFAULT_SERVER_URL = "https://yesdb.centralindia.cloudapp.azure.com"
PROJECT_CONFIG_FILE = os.path.join("yesdb", ".yesdb.json")


# ── Helpers ──────────────────────────────────────────────────────


def _require_requests() -> bool:
    """Ensure the requests library is available. Returns False if missing."""
    if requests is None:
        print(
            "Error: 'requests' library is required for cloud features.\n"
            "Install it with: pip install yesdb[cloud]",
            file=sys.stderr,
        )
        return False
    return True


def _get_server_url() -> str:
    """Get the server URL from credentials or use default."""
    try:
        creds = load_credentials()
        return creds.get("server_url", DEFAULT_SERVER_URL)
    except FileNotFoundError:
        return DEFAULT_SERVER_URL


def _load_project_config() -> Optional[dict]:
    """Load project config from yesdb/.yesdb.json in the current directory. Returns None if not found."""
    if not os.path.exists(PROJECT_CONFIG_FILE):
        print(
            "Error: No yesdb project found in this directory.\n"
            "Run 'yesdb init <db_name>' first.",
            file=sys.stderr,
        )
        return None
    with open(PROJECT_CONFIG_FILE, "r") as f:
        return json.load(f)


def _print_logs(logs: list):
    """Pretty-print engine logs."""
    for log in logs:
        level = log.get("level", "INFO")
        component = log.get("component", "unknown")
        message = log.get("message", "")
        timestamp = log.get("timestamp", "")
        print(f"  {timestamp} - chidb.{component} - {level} - {message}")


# ── Commands ─────────────────────────────────────────────────────


def cmd_signup(args) -> int:
    """Create a new account."""
    if not _require_requests():
        return 1

    email = input("Email: ").strip()
    if not email:
        print("Error: Email cannot be empty.", file=sys.stderr)
        return 1

    password = getpass.getpass("Password: ")
    if not password:
        print("Error: Password cannot be empty.", file=sys.stderr)
        return 1

    confirm = getpass.getpass("Confirm password: ")
    if password != confirm:
        print("Error: Passwords do not match.", file=sys.stderr)
        return 1

    server_url = args.server or DEFAULT_SERVER_URL
    name = args.name

    try:
        body = {"email": email, "password": password}
        if name:
            body["name"] = name
        resp = requests.post(f"{server_url}/api/v1/signup", json=body)

        if resp.status_code == 409:
            print("Error: Email already registered.", file=sys.stderr)
            return 1
        resp.raise_for_status()

        data = resp.json()
        api_key = data["api_key"]

        save_credentials(email, api_key, server_url)
        print(f"\n  Account created for {email}.")
        print(f"  API key: {api_key}")
        print(f"  Credentials saved to {CREDENTIALS_PATH}")
        return 0

    except requests.ConnectionError:
        print(f"Error: Could not connect to server at {server_url}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_login(args) -> int:
    """Login to an existing account."""
    if not _require_requests():
        return 1

    email = input("Email: ").strip()
    if not email:
        print("Error: Email cannot be empty.", file=sys.stderr)
        return 1

    password = getpass.getpass("Password: ")
    if not password:
        print("Error: Password cannot be empty.", file=sys.stderr)
        return 1

    server_url = args.server or _get_server_url()

    try:
        resp = requests.post(
            f"{server_url}/api/v1/login",
            json={"email": email, "password": password},
        )

        if resp.status_code == 401:
            print("Error: Invalid email or password.", file=sys.stderr)
            return 1
        resp.raise_for_status()

        data = resp.json()
        api_key = data["api_key"]

        save_credentials(email, api_key, server_url)
        print(f"\n  Logged in as {email}.")
        print(f"  New API key: {api_key}")
        print(f"  Credentials saved to {CREDENTIALS_PATH}")
        return 0

    except requests.ConnectionError:
        print(f"Error: Could not connect to server at {server_url}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_init(args) -> int:
    """Initialize a project with a cloud database."""
    if not _require_requests():
        return 1

    db_name = args.db_name

    try:
        creds = load_credentials()
    except FileNotFoundError:
        print(
            "Error: Not logged in. Run 'yesdb signup' or 'yesdb login' first.",
            file=sys.stderr,
        )
        return 1

    server_url = creds["server_url"]
    api_key = creds["api_key"]
    headers = {"Authorization": f"Bearer {api_key}"}

    # Create database on server
    try:
        resp = requests.post(
            f"{server_url}/api/v1/databases",
            json={"name": db_name},
            headers=headers,
        )

        if resp.status_code == 409:
            print(f"  Database '{db_name}' already exists on the server. Linking to it.")
        elif resp.status_code == 401:
            print("Error: Invalid API key. Run 'yesdb login' to refresh.", file=sys.stderr)
            return 1
        elif resp.status_code != 200:
            print(f"Error: {resp.json().get('detail', resp.text)}", file=sys.stderr)
            return 1
        else:
            data = resp.json()
            print(f"  Database '{db_name}' created on cloud.")
            if data.get("logs"):
                _print_logs(data["logs"])

    except requests.ConnectionError:
        print(f"Error: Could not connect to server at {server_url}", file=sys.stderr)
        return 1

    # Create local yesdb/ folder and files
    os.makedirs("yesdb", exist_ok=True)

    # Write project config
    config = {"database": db_name}
    with open(PROJECT_CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)

    # Write schema template if it doesn't exist
    schema_path = os.path.join("yesdb", "schema.py")
    if not os.path.exists(schema_path):
        with open(schema_path, "w") as f:
            f.write(
                "from yesdb import Table, Column, Integer, Text, Real, Blob\n\n"
                f"# Schema for '{db_name}'. Add your tables below, then run 'yesdb push'.\n\n"
                "users = Table('users', [\n"
                "    Column('id', Integer, primary_key=True),\n"
                "    Column('name', Text),\n"
                "    Column('email', Text),\n"
                "])\n"
            )

    print(f"  Created yesdb/ folder with schema.py")
    print(f"  Project linked to database '{db_name}'.")
    return 0


def cmd_push(args) -> int:
    """Push schema to the cloud database."""
    if not _require_requests():
        return 1

    # Load project config
    project = _load_project_config()
    if project is None:
        return 1
    db_name = project["database"]

    try:
        creds = load_credentials()
    except FileNotFoundError:
        print("Error: Not logged in. Run 'yesdb login' first.", file=sys.stderr)
        return 1

    # Load and parse schema file
    schema_path = os.path.join("yesdb", "schema.py")
    if not os.path.exists(schema_path):
        print("Error: No yesdb/schema.py found.", file=sys.stderr)
        return 1

    # Import the schema module dynamically
    spec = importlib.util.spec_from_file_location("yesdb_schema", schema_path)
    schema_module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(schema_module)
    except Exception as e:
        print(f"Error loading schema.py: {e}", file=sys.stderr)
        return 1

    # Collect Table objects and generate SQL
    tables = collect_tables(schema_module.__dict__)
    if not tables:
        print("No tables found in yesdb/schema.py. Nothing to push.")
        return 0

    statements = [t.to_sql() for t in tables]
    print(f"  Connecting to '{db_name}' database...")

    server_url = creds["server_url"]
    api_key = creds["api_key"]
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    try:
        resp = requests.post(
            f"{server_url}/api/v1/databases/{db_name}/push",
            json={"statements": statements},
            headers=headers,
        )

        if resp.status_code == 401:
            print("Error: Invalid API key. Run 'yesdb login' to refresh.", file=sys.stderr)
            return 1
        if resp.status_code == 404:
            print(
                f"Error: Database '{db_name}' not found. Run 'yesdb init {db_name}' first.",
                file=sys.stderr,
            )
            return 1
        resp.raise_for_status()

        data = resp.json()

        # Print logs
        if data.get("logs"):
            _print_logs(data["logs"])

        # Summary
        for table in tables:
            print(f"  Table '{table.name}' created")
        print(f"\n  Schema synced. {data['executed']} statement(s) pushed.")
        return 0

    except requests.ConnectionError:
        print(f"Error: Could not connect to server at {server_url}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_databases(args) -> int:
    """List all databases for the current user."""
    if not _require_requests():
        return 1

    try:
        creds = load_credentials()
    except FileNotFoundError:
        print("Error: Not logged in. Run 'yesdb login' first.", file=sys.stderr)
        return 1

    server_url = creds["server_url"]
    api_key = creds["api_key"]
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        resp = requests.get(f"{server_url}/api/v1/databases", headers=headers)

        if resp.status_code == 401:
            print("Error: Invalid API key. Run 'yesdb login' to refresh.", file=sys.stderr)
            return 1
        resp.raise_for_status()

        databases = resp.json().get("databases", [])
        if not databases:
            print("  No databases yet. Run 'yesdb init <name>' to create one.")
        else:
            print(f"  Your databases ({len(databases)}):")
            for db in databases:
                print(f"    - {db}")
        return 0

    except requests.ConnectionError:
        print(f"Error: Could not connect to server at {server_url}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_shell(args) -> int:
    """Interactive SQL shell against a cloud database."""
    if not _require_requests():
        return 1

    db_name = args.db_name

    try:
        conn = CloudConnection(db_name=db_name)
    except FileNotFoundError:
        print("Error: Not logged in. Run 'yesdb login' first.", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    print(f"  Connected to '{db_name}' (cloud)")
    print("  Enter SQL statements or 'exit' to quit.\n")

    try:
        while True:
            try:
                sql = input(f"yesdb:{db_name}> ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\nGoodbye!")
                break

            if not sql:
                continue
            if sql.lower() in ("exit", "quit", ".exit", ".quit"):
                print("Goodbye!")
                break

            if sql.lower() == ".tables":
                tables = conn.get_table_names()
                if tables:
                    for t in tables:
                        print(f"  {t}")
                else:
                    print("  (no tables)")
                continue

            try:
                result = conn.execute(sql)
                if result.rows:
                    # Print results as a simple table
                    for row in result.rows:
                        print(" | ".join(str(v) if v is not None else "NULL" for v in row))
                    print(f"({result.row_count} row{'s' if result.row_count != 1 else ''})")
                else:
                    print("Query executed.")

                # Always show logs
                if result.logs:
                    _print_logs(result.logs)

            except (PermissionError, ValueError) as e:
                print(f"Error: {e}")

    finally:
        conn.close()

    return 0


# ── Main entry point ─────────────────────────────────────────────


def main(args: Optional[list] = None) -> int:
    """
    Main entry point for the cloud CLI.

    This is registered as the 'yesdb-cloud' console script.
    It is also called from the unified 'yesdb' entry point when a
    cloud subcommand is detected.
    """
    parser = argparse.ArgumentParser(
        description="YesDB Cloud - Backend-as-a-Service for students",
        prog="yesdb",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # signup
    sp_signup = subparsers.add_parser("signup", help="Create a new account")
    sp_signup.add_argument("--name", help="Your name (optional)")
    sp_signup.add_argument("--server", help="Server URL (default: auto)")

    # login
    sp_login = subparsers.add_parser("login", help="Login to your account")
    sp_login.add_argument("--server", help="Server URL (default: auto)")

    # init
    sp_init = subparsers.add_parser("init", help="Initialize a project with a cloud database")
    sp_init.add_argument("db_name", help="Name for the database")

    # push
    subparsers.add_parser("push", help="Push schema.py to the cloud")

    # databases
    subparsers.add_parser("databases", help="List your databases")

    # shell
    sp_shell = subparsers.add_parser("shell", help="Interactive SQL shell (cloud)")
    sp_shell.add_argument("db_name", help="Database to connect to")

    if args is None:
        args = sys.argv[1:]

    parsed = parser.parse_args(args)

    if parsed.command is None:
        parser.print_help()
        return 0

    commands = {
        "signup": cmd_signup,
        "login": cmd_login,
        "init": cmd_init,
        "push": cmd_push,
        "databases": cmd_databases,
        "shell": cmd_shell,
    }

    return commands[parsed.command](parsed)


if __name__ == "__main__":
    sys.exit(main())

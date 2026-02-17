"""
Cloud client for YesDB Backend-as-a-Service.

Provides the same .execute() API as YesDB but sends requests over HTTPS
to the remote server. Credentials are auto-loaded from ~/.yesdb/credentials.json.

Usage:
    from yesdb import connect

    db = connect("myproject")
    db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    result = db.execute("SELECT * FROM users")
    print(result.rows)
    result.print_logs()
"""

import json
import os
from typing import Any, Dict, List, Optional

try:
    import requests
except ImportError:
    requests = None


CREDENTIALS_PATH = os.path.expanduser("~/.yesdb/credentials.json")


# ── ExecuteResult ────────────────────────────────────────────────


class ExecuteResult:
    """
    Wraps a response from the server, giving access to both data and logs.

    Attributes:
        rows: List of result rows (for SELECT queries), empty list otherwise.
        row_count: Number of rows returned.
        logs: List of log entries from the engine.
    """

    def __init__(self, rows: List[List[Any]], logs: List[Dict[str, str]], row_count: int = 0):
        self.rows = rows
        self.row_count = row_count or len(rows)
        self.logs = logs

    def __iter__(self):
        """Iterate over result rows."""
        return iter(self.rows)

    def __len__(self):
        """Number of result rows."""
        return len(self.rows)

    def __bool__(self):
        """True if there are any rows."""
        return len(self.rows) > 0

    def __repr__(self):
        return f"ExecuteResult(rows={len(self.rows)}, logs={len(self.logs)})"

    def print_logs(self):
        """Pretty-print engine logs to the console."""
        for log in self.logs:
            level = log.get("level", "INFO")
            component = log.get("component", "unknown")
            message = log.get("message", "")
            timestamp = log.get("timestamp", "")
            print(f"{timestamp} - chidb.{component} - {level} - {message}")


# ── Credential helpers ───────────────────────────────────────────


def load_credentials(path: Optional[str] = None) -> dict:
    """
    Load saved credentials from ~/.yesdb/credentials.json.

    Returns:
        Dict with keys: email, api_key, server_url.

    Raises:
        FileNotFoundError: If credentials file doesn't exist.
    """
    if path is None:
        path = CREDENTIALS_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"No credentials found at {path}. Run 'yesdb signup' or 'yesdb login' first."
        )
    with open(path, "r") as f:
        return json.load(f)


def save_credentials(email: str, api_key: str, server_url: str, path: Optional[str] = None):
    """Save credentials to ~/.yesdb/credentials.json."""
    if path is None:
        path = CREDENTIALS_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = {"email": email, "api_key": api_key, "server_url": server_url}
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ── CloudConnection ──────────────────────────────────────────────


class CloudConnection:
    """
    Remote database connection via HTTPS.

    Drop-in replacement for YesDB with identical .execute() API,
    but sends requests to the YesDB Cloud server.

    Args:
        db_name: Name of the database to connect to.
        api_key: API key for authentication. If None, loaded from credentials file.
        server_url: Server URL. If None, loaded from credentials file.
    """

    def __init__(
        self,
        db_name: str,
        api_key: Optional[str] = None,
        server_url: Optional[str] = None,
    ):
        if requests is None:
            raise ImportError(
                "The 'requests' library is required for cloud mode. "
                "Install it with: pip install yesdb[cloud]"
            )

        # Load credentials if not provided
        if api_key is None or server_url is None:
            creds = load_credentials()
            api_key = api_key or creds["api_key"]
            server_url = server_url or creds["server_url"]

        self.db_name = db_name
        self.api_key = api_key
        self.server_url = server_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
        )

    def _url(self, path: str) -> str:
        """Build a full URL for an API path."""
        return f"{self.server_url}/api/v1/databases/{self.db_name}{path}"

    def _handle_response(self, response):
        """Check response status and raise meaningful errors."""
        if response.status_code == 401:
            raise PermissionError("Invalid API key. Run 'yesdb login' to get a new one.")
        if response.status_code == 404:
            data = response.json()
            raise ValueError(data.get("detail", f"Database '{self.db_name}' not found."))
        if response.status_code == 400:
            data = response.json()
            raise ValueError(data.get("detail", "Bad request"))
        response.raise_for_status()

    def execute(self, sql: str) -> ExecuteResult:
        """
        Execute a SQL statement on the remote database.

        Args:
            sql: SQL statement to execute.

        Returns:
            ExecuteResult with rows and engine logs.
        """
        response = self.session.post(self._url("/execute"), json={"sql": sql})
        self._handle_response(response)

        data = response.json()
        return ExecuteResult(
            rows=data.get("rows", []),
            logs=data.get("logs", []),
            row_count=data.get("row_count", 0),
        )

    def get_table_names(self) -> List[str]:
        """Get list of table names from the remote database."""
        response = self.session.get(self._url("/tables"))
        self._handle_response(response)
        return response.json().get("tables", [])

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists on the remote database."""
        return table_name in self.get_table_names()

    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __repr__(self):
        return f"CloudConnection(db_name='{self.db_name}', server='{self.server_url}')"

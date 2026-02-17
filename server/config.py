"""
Server configuration.
Reads settings from environment variables with sensible defaults.
"""

import os


class Settings:
    """Server settings loaded from environment variables."""

    def __init__(self):
        # Directory where user database files are stored
        self.DATA_DIR: str = os.environ.get("YESDB_DATA_DIR", "/var/lib/yesdb/data")

        # Path to the accounts database (stores user accounts, API key hashes)
        self.ACCOUNTS_DB_PATH: str = os.environ.get(
            "YESDB_ACCOUNTS_DB", "/var/lib/yesdb/accounts.db"
        )

        # Server host and port (uvicorn binds to this)
        self.HOST: str = os.environ.get("YESDB_HOST", "127.0.0.1")
        self.PORT: int = int(os.environ.get("YESDB_PORT", "8000"))


settings = Settings()

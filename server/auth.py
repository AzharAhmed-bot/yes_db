"""
Authentication and user management for YesDB Cloud.
Uses yesdb itself to store user accounts (dogfooding).

Security model:
- Passwords are hashed with bcrypt before storage
- API keys are hashed with SHA-256 before storage
- API keys are sent as Bearer tokens over HTTPS
"""

import os
import secrets
import hashlib
from dataclasses import dataclass
from typing import Optional

import bcrypt
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from chidb.api import YesDB
from chidb.record import Record
from server.config import settings

security = HTTPBearer()


@dataclass
class User:
    """Represents an authenticated user."""
    user_id: str
    email: str
    name: Optional[str] = None


# ── Accounts database ────────────────────────────────────────────

_accounts_db: Optional[YesDB] = None


def get_accounts_db() -> YesDB:
    """Get or initialize the accounts database."""
    global _accounts_db
    if _accounts_db is None:
        # Ensure the directory exists
        db_dir = os.path.dirname(settings.ACCOUNTS_DB_PATH)
        os.makedirs(db_dir, exist_ok=True)

        _accounts_db = YesDB(settings.ACCOUNTS_DB_PATH)
        if not _accounts_db.table_exists("accounts"):
            _accounts_db.execute(
                "CREATE TABLE accounts ("
                "user_id TEXT, "
                "email TEXT, "
                "name TEXT, "
                "password_hash TEXT, "
                "api_key_hash TEXT"
                ")"
            )
    return _accounts_db


def close_accounts_db() -> None:
    """Close the accounts database connection."""
    global _accounts_db
    if _accounts_db is not None:
        _accounts_db.close()
        _accounts_db = None


# ── API key helpers ──────────────────────────────────────────────

def generate_api_key() -> str:
    """Generate a cryptographically secure API key."""
    return f"yesdb_{secrets.token_urlsafe(32)}"


def hash_api_key(api_key: str) -> str:
    """Hash an API key with SHA-256 for storage."""
    return hashlib.sha256(api_key.encode()).hexdigest()


# ── Password helpers ─────────────────────────────────────────────

def hash_password(password: str) -> str:
    """Hash a password with bcrypt."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against its bcrypt hash."""
    return bcrypt.checkpw(password.encode(), password_hash.encode())


# ── User operations ──────────────────────────────────────────────

def _get_all_accounts() -> list:
    """Fetch all account rows from the database, unwrapping Record objects."""
    db = get_accounts_db()
    raw_rows = db.execute("SELECT * FROM accounts")
    rows = []
    for row in raw_rows:
        if row and isinstance(row[0], Record):
            rows.append(row[0].get_values())
        else:
            rows.append(row)
    return rows


def find_user_by_email(email: str) -> Optional[list]:
    """Find an account row by email. Returns values list or None."""
    for row in _get_all_accounts():
        if len(row) >= 2 and row[1] == email:
            return row
    return None


def find_user_by_api_key_hash(key_hash: str) -> Optional[list]:
    """Find an account row by API key hash. Returns values list or None."""
    for row in _get_all_accounts():
        if len(row) >= 5 and row[4] == key_hash:
            return row
    return None


def create_user(email: str, password: str, name: Optional[str] = None) -> tuple:
    """
    Create a new user account.

    Returns:
        (User, api_key) tuple. The raw API key is returned once and never stored.

    Raises:
        HTTPException 409 if the email is already registered.
    """
    if find_user_by_email(email):
        raise HTTPException(status_code=409, detail="Email already registered")

    user_id = secrets.token_hex(8)
    api_key = generate_api_key()
    key_hash = hash_api_key(api_key)
    pw_hash = hash_password(password)
    name_val = name if name else ""

    db = get_accounts_db()
    db.execute(
        f"INSERT INTO accounts VALUES ("
        f"'{user_id}', '{email}', '{name_val}', '{pw_hash}', '{key_hash}')"
    )

    user = User(user_id=user_id, email=email, name=name)
    return user, api_key


def login_user(email: str, password: str) -> tuple:
    """
    Authenticate a user with email and password.

    Returns:
        (User, api_key) tuple. A new API key is generated on each login
        (old one is replaced).

    Raises:
        HTTPException 401 if credentials are invalid.
    """
    row = find_user_by_email(email)
    if row is None:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    user_id, email_val, name, pw_hash, old_key_hash = row[0], row[1], row[2], row[3], row[4]

    if not verify_password(password, pw_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    # Generate a new API key on login (replaces the old one)
    new_api_key = generate_api_key()
    new_key_hash = hash_api_key(new_api_key)

    db = get_accounts_db()
    # Delete old row and insert updated one
    db.execute(f"DELETE FROM accounts WHERE email = '{email_val}'")
    db.execute(
        f"INSERT INTO accounts VALUES ("
        f"'{user_id}', '{email_val}', '{name}', '{pw_hash}', '{new_key_hash}')"
    )

    user = User(user_id=user_id, email=email_val, name=name)
    return user, new_api_key


# ── FastAPI dependency ───────────────────────────────────────────

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> User:
    """
    Validate an API key and return the authenticated user.
    Use as a FastAPI dependency: user = Depends(get_current_user)
    """
    api_key = credentials.credentials
    key_hash = hash_api_key(api_key)

    row = find_user_by_api_key_hash(key_hash)
    if row is None:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return User(user_id=row[0], email=row[1], name=row[2])

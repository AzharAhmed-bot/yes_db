"""
YesDB Cloud - FastAPI server.
Wraps the yesdb engine behind a REST API with API key authentication.
Every response includes engine logs for debuggability.
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from chidb.api import YesDB
from chidb.record import Record
from server.auth import (
    User,
    get_current_user,
    create_user,
    login_user,
    close_accounts_db,
)
from server.config import settings


# ── Lifespan ─────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown logic."""
    yield
    # Shutdown: close all open database connections
    for db in _db_pool.values():
        try:
            db.close()
        except Exception:
            pass
    _db_pool.clear()
    close_accounts_db()


# ── FastAPI app ──────────────────────────────────────────────────

app = FastAPI(title="YesDB Cloud", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "GET"],
    allow_headers=["Authorization", "Content-Type"],
)


# ── Log capture ──────────────────────────────────────────────────


class LogCaptureHandler(logging.Handler):
    """
    Temporary logging handler that buffers log records.
    Attach to chidb.* loggers during execute() to capture engine logs.
    """

    def __init__(self):
        super().__init__()
        self.records: List[Dict[str, str]] = []

    def emit(self, record: logging.LogRecord):
        self.records.append(
            {
                "level": record.levelname,
                "component": record.name.replace("chidb.", ""),
                "message": record.getMessage(),
                "timestamp": self.format(record),
            }
        )


def capture_logs(func):
    """
    Run a function while capturing all chidb.* log output.
    Returns (result, logs).
    """
    handler = LogCaptureHandler()
    formatter = logging.Formatter("%(asctime)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)

    # Attach to the root chidb logger so we capture all components
    chidb_logger = logging.getLogger("chidb")
    original_level = chidb_logger.level
    chidb_logger.setLevel(logging.DEBUG)
    chidb_logger.addHandler(handler)

    try:
        result = func()
    finally:
        chidb_logger.removeHandler(handler)
        chidb_logger.setLevel(original_level)

    return result, handler.records


# ── Database pool ────────────────────────────────────────────────

# In-memory cache of open database connections: (user_id, db_name) -> YesDB
_db_pool: Dict[tuple, YesDB] = {}


def get_user_db(user: User, db_name: str) -> YesDB:
    """Get or open the database file for a user's named database."""
    pool_key = (user.user_id, db_name)
    if pool_key not in _db_pool:
        user_dir = os.path.join(settings.DATA_DIR, user.user_id)
        db_path = os.path.join(user_dir, f"{db_name}.db")

        if not os.path.exists(db_path):
            raise HTTPException(
                status_code=404,
                detail=f"Database '{db_name}' does not exist. Create it first with POST /api/v1/databases",
            )

        _db_pool[pool_key] = YesDB(db_path)

    return _db_pool[pool_key]


def unwrap_rows(raw_rows: List[List[Any]]) -> List[List[Any]]:
    """Convert Record objects from execute() results into plain Python lists."""
    rows = []
    for row in raw_rows:
        row_values = []
        for val in row:
            if isinstance(val, Record):
                row_values.extend(val.get_values())
            elif val is None:
                row_values.append(None)
            else:
                row_values.append(val)
        rows.append(row_values)
    return rows


# ── Request / Response models ────────────────────────────────────


class SignupRequest(BaseModel):
    email: str
    password: str
    name: Optional[str] = None


class SignupResponse(BaseModel):
    api_key: str
    message: str


class LoginRequest(BaseModel):
    email: str
    password: str


class LoginResponse(BaseModel):
    api_key: str
    message: str


class CreateDatabaseRequest(BaseModel):
    name: str


class CreateDatabaseResponse(BaseModel):
    name: str
    message: str
    logs: List[Dict[str, str]]


class ExecuteRequest(BaseModel):
    sql: str


class ExecuteResponse(BaseModel):
    rows: List[List[Any]]
    row_count: int
    logs: List[Dict[str, str]]


class PushRequest(BaseModel):
    statements: List[str]


class PushResponse(BaseModel):
    executed: int
    logs: List[Dict[str, str]]


class TablesResponse(BaseModel):
    tables: List[str]
    logs: List[Dict[str, str]]


class DatabaseListResponse(BaseModel):
    databases: List[str]


# ── Routes ───────────────────────────────────────────────────────


@app.post("/api/v1/signup", response_model=SignupResponse)
def signup(req: SignupRequest):
    """Create a new user account. Returns an API key (save it — shown only once)."""
    user, api_key = create_user(req.email, req.password, req.name)
    return SignupResponse(
        api_key=api_key,
        message=f"Account created for {user.email}. Save your API key — it won't be shown again.",
    )


@app.post("/api/v1/login", response_model=LoginResponse)
def login(req: LoginRequest):
    """Login with email and password. Returns a new API key (old one is invalidated)."""
    user, api_key = login_user(req.email, req.password)
    return LoginResponse(
        api_key=api_key,
        message=f"Logged in as {user.email}. Your old API key has been invalidated.",
    )


@app.post("/api/v1/databases", response_model=CreateDatabaseResponse)
def create_database(req: CreateDatabaseRequest, user: User = Depends(get_current_user)):
    """Create a new database for the authenticated user."""
    db_name = req.name.strip()

    if not db_name.isalnum() and not all(c.isalnum() or c in "-_" for c in db_name):
        raise HTTPException(
            status_code=400,
            detail="Database name must contain only letters, numbers, hyphens, and underscores.",
        )

    user_dir = os.path.join(settings.DATA_DIR, user.user_id)
    db_path = os.path.join(user_dir, f"{db_name}.db")

    if os.path.exists(db_path):
        raise HTTPException(status_code=409, detail=f"Database '{db_name}' already exists.")

    os.makedirs(user_dir, exist_ok=True)

    def _create():
        db = YesDB(db_path)
        pool_key = (user.user_id, db_name)
        _db_pool[pool_key] = db

    _, logs = capture_logs(_create)

    return CreateDatabaseResponse(
        name=db_name,
        message=f"Database '{db_name}' created.",
        logs=logs,
    )


@app.get("/api/v1/databases", response_model=DatabaseListResponse)
def list_databases(user: User = Depends(get_current_user)):
    """List all databases for the authenticated user."""
    user_dir = os.path.join(settings.DATA_DIR, user.user_id)

    if not os.path.exists(user_dir):
        return DatabaseListResponse(databases=[])

    databases = []
    for filename in sorted(os.listdir(user_dir)):
        if filename.endswith(".db"):
            databases.append(filename[:-3])

    return DatabaseListResponse(databases=databases)


@app.post("/api/v1/databases/{db_name}/execute", response_model=ExecuteResponse)
def execute_sql(db_name: str, req: ExecuteRequest, user: User = Depends(get_current_user)):
    """Execute a SQL statement against the user's named database."""
    db = get_user_db(user, db_name)

    def _execute():
        return db.execute(req.sql)

    try:
        raw_rows, logs = capture_logs(_execute)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    rows = unwrap_rows(raw_rows)
    return ExecuteResponse(rows=rows, row_count=len(rows), logs=logs)


@app.post("/api/v1/databases/{db_name}/push", response_model=PushResponse)
def push_schema(db_name: str, req: PushRequest, user: User = Depends(get_current_user)):
    """Push schema statements (CREATE TABLE, etc.) to the user's database."""
    db = get_user_db(user, db_name)
    all_logs: List[Dict[str, str]] = []
    executed_count = 0

    for sql in req.statements:
        def _exec(s=sql):
            return db.execute(s)

        try:
            _, logs = capture_logs(_exec)
            all_logs.extend(logs)
            executed_count += 1
        except Exception as e:
            all_logs.append(
                {
                    "level": "ERROR",
                    "component": "server",
                    "message": f"Failed: {sql} — {str(e)}",
                    "timestamp": "",
                }
            )

    return PushResponse(executed=executed_count, logs=all_logs)


@app.get("/api/v1/databases/{db_name}/tables", response_model=TablesResponse)
def list_tables(db_name: str, user: User = Depends(get_current_user)):
    """List all tables in the user's named database."""
    db = get_user_db(user, db_name)

    def _tables():
        return db.get_table_names()

    tables, logs = capture_logs(_tables)
    return TablesResponse(tables=tables, logs=logs)


@app.get("/api/v1/health")
def health_check():
    """Health check endpoint."""
    return {"status": "ok"}



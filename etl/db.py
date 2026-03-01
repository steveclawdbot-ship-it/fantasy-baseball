"""Shared async database connection for the ETL pipeline.

Uses aiosqlite directly (not the ORM) so the ETL can run standalone
without importing the FastAPI backend.
"""

import aiosqlite
from contextlib import asynccontextmanager
from etl.config import DB_PATH


@asynccontextmanager
async def get_connection(db_path: str | None = None):
    """Yield an aiosqlite connection with WAL mode enabled."""
    path = db_path or DB_PATH
    db = await aiosqlite.connect(path)
    try:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")
        yield db
    finally:
        await db.close()

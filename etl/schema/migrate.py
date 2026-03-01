"""Lightweight schema migration runner with version tracking.

No Alembic dependency — just ordered Python functions applied in sequence.
Safe to re-run: tracks applied versions in _schema_version table.
"""

from __future__ import annotations
import logging
from etl.schema.staging import create_staging_tables
from etl.schema.core import create_core_tables
from etl.schema.serving import rename_legacy_tables, create_serving_views

logger = logging.getLogger(__name__)

SCHEMA_VERSION_DDL = """
CREATE TABLE IF NOT EXISTS _schema_version (
    version INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


async def _get_current_version(db) -> int:
    """Return the highest applied migration version, or 0 if none."""
    await db.execute(SCHEMA_VERSION_DDL)
    cursor = await db.execute("SELECT MAX(version) FROM _schema_version")
    row = await cursor.fetchone()
    return row[0] or 0


async def _record_version(db, version: int, description: str) -> None:
    await db.execute(
        "INSERT INTO _schema_version (version, description) VALUES (?, ?)",
        (version, description),
    )
    await db.commit()


# -----------------------------------------------------------------------
# Migration functions
# -----------------------------------------------------------------------

async def _migrate_v1_staging(db) -> None:
    """V1: Create all staging tables (additive, no breaking changes)."""
    await create_staging_tables(db)


async def _migrate_v2_core(db) -> None:
    """V2: Create all core tables (additive, no breaking changes)."""
    await create_core_tables(db)


async def _migrate_v3_serving(db) -> None:
    """V3: Rename legacy tables and create serving views."""
    await rename_legacy_tables(db)
    await create_serving_views(db)


async def _migrate_v4_refresh_serving(db) -> None:
    """V5: Refresh serving views for player_type + pitching views."""
    await create_serving_views(db)


# Ordered list of migrations.
# (version, description, function)
# version=1 represents pre-existing state (no function needed).
MIGRATIONS: list[tuple[int, str, callable | None]] = [
    (1, "pre-existing legacy schema", None),
    (2, "add staging tables", _migrate_v1_staging),
    (3, "add core tables", _migrate_v2_core),
    (4, "rename legacy tables and create serving views", _migrate_v3_serving),
    (5, "refresh serving views for player_type + pitching views", _migrate_v4_refresh_serving),
]


async def run_migrations(db) -> int:
    """Apply all pending migrations. Returns number of migrations applied."""
    current = await _get_current_version(db)
    applied = 0

    for version, description, func in MIGRATIONS:
        if version <= current:
            continue
        if func is not None:
            logger.info(f"Applying migration v{version}: {description}")
            await func(db)
        await _record_version(db, version, description)
        applied += 1
        logger.info(f"Migration v{version} applied: {description}")

    if applied == 0:
        logger.info(f"Schema up to date at v{current}")
    else:
        logger.info(f"Applied {applied} migration(s), now at v{current + applied}")

    return applied


async def run_migrations_from_path(db_path: str) -> int:
    """Convenience wrapper that opens a connection and runs migrations."""
    from etl.db import get_connection

    async with get_connection(db_path) as db:
        return await run_migrations(db)

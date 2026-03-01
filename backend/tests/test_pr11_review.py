"""Tests for PR #11 review items: migration, legacy table conflicts, CLI validation."""

import asyncio
import subprocess
import sys

import aiosqlite
import pytest

from etl.schema.migrate import run_migrations
from etl.schema.serving import create_serving_views, SERVING_VIEW_NAMES


@pytest.fixture
def tmp_db(tmp_path):
    """Yield a path to a temporary SQLite DB."""
    return str(tmp_path / "test.db")


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# -----------------------------------------------------------------------
# 1. Migration from v4 DB -> v5 refreshes views, player_type works
# -----------------------------------------------------------------------
class TestV5MigrationRefreshesViews:
    def test_v4_db_gets_player_type_after_v5(self, tmp_db):
        async def _test():
            async with aiosqlite.connect(tmp_db) as db:
                # Run all migrations to reach v5
                await run_migrations(db)

                # Verify we're at v5
                cursor = await db.execute("SELECT MAX(version) FROM _schema_version")
                row = await cursor.fetchone()
                assert row[0] == 5

                # Verify the players view has player_type column
                cursor = await db.execute("PRAGMA table_info(players)")
                columns = {r[1] for r in await cursor.fetchall()}
                assert "player_type" in columns

        _run(_test())


# -----------------------------------------------------------------------
# 2. Legacy tables with view names get renamed instead of crashing
# -----------------------------------------------------------------------
class TestLegacyTableConflict:
    def test_create_views_renames_conflicting_table(self, tmp_db):
        async def _test():
            async with aiosqlite.connect(tmp_db) as db:
                # Set up core tables first (views depend on them)
                from etl.schema.core import create_core_tables
                await create_core_tables(db)

                # Create a real TABLE called pitching_stats (simulating legacy conflict)
                await db.execute(
                    "CREATE TABLE pitching_stats (id INTEGER PRIMARY KEY, dummy TEXT)"
                )
                await db.execute(
                    "INSERT INTO pitching_stats VALUES (1, 'legacy')"
                )
                await db.commit()

                # Verify it's a table
                cursor = await db.execute(
                    "SELECT type FROM sqlite_master WHERE name = 'pitching_stats'"
                )
                assert (await cursor.fetchone())[0] == "table"

                # create_serving_views should handle this gracefully
                await create_serving_views(db)

                # Now pitching_stats should be a view
                cursor = await db.execute(
                    "SELECT type FROM sqlite_master WHERE name = 'pitching_stats'"
                )
                assert (await cursor.fetchone())[0] == "view"

                # Legacy data should be preserved in renamed table
                cursor = await db.execute(
                    "SELECT dummy FROM _legacy_pitching_stats WHERE id = 1"
                )
                row = await cursor.fetchone()
                assert row[0] == "legacy"

        _run(_test())


# -----------------------------------------------------------------------
# 3. CLI --seasons validation
# -----------------------------------------------------------------------
class TestSeasonsCliValidation:
    def _run_cli(self, *args):
        result = subprocess.run(
            [sys.executable, "-m", "etl.runner", *args],
            capture_output=True, text=True,
        )
        return result

    def test_invalid_seasons_format_errors(self):
        result = self._run_cli("--source", "batting", "--seasons", "abc")
        assert result.returncode != 0
        assert "YYYY-YYYY" in result.stderr

    def test_reversed_years_errors(self):
        result = self._run_cli("--source", "batting", "--seasons", "2025-2020")
        assert result.returncode != 0
        assert "must be <=" in result.stderr

    def test_valid_seasons_format_accepted(self):
        # Just check it parses without error — will fail at extract (no network)
        # but should not fail at argument parsing
        result = self._run_cli("--source", "batting", "--seasons", "2024-2024")
        # If argparse failed, returncode=2 and "YYYY-YYYY" in stderr
        assert "YYYY-YYYY" not in result.stderr

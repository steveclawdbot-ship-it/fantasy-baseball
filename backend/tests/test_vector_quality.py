"""Smoke tests for ETL-populated tables and feature views.

These tests validate data quality in a DB that has already been through
the ETL pipeline.  They are skipped automatically when the DB file is
missing or the expected schema has not been created yet (e.g. clean/CI
environments).
"""

import os
import sqlite3
from pathlib import Path

import pytest

DB_PATH = Path(os.getenv("FANTASY_DB_PATH", "./fantasy_baseball.db"))

# Skip the entire module when there is no DB to inspect.
pytestmark = pytest.mark.skipif(
    not DB_PATH.exists(), reason=f"DB not found at {DB_PATH}"
)


def _tables_and_views():
    """Return (tables, views) sets from the DB, or empty sets on error."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT type, name FROM sqlite_master WHERE type IN ('table','view')")
        rows = cur.fetchall()
        conn.close()
        tables = {name for typ, name in rows if typ == "table"}
        views = {name for typ, name in rows if typ == "view"}
        return tables, views
    except Exception:
        return set(), set()


def _count(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


_REQUIRED_TABLES = {"youtube_videos", "youtube_transcripts", "youtube_player_mentions"}
_REQUIRED_VIEWS = {
    "feature_view_mention_gap",
    "feature_view_star_shadow",
    "feature_view_acceleration",
}


def test_required_tables_exist():
    tables, _ = _tables_and_views()
    missing = _REQUIRED_TABLES - tables
    if missing:
        pytest.skip(f"ETL tables not yet created: {sorted(missing)}")


def test_transcript_coverage_minimum():
    tables, _ = _tables_and_views()
    if not _REQUIRED_TABLES.issubset(tables):
        pytest.skip("ETL tables not yet created")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    videos = _count(cur, "youtube_videos")
    transcripts = _count(cur, "youtube_transcripts")
    conn.close()

    if videos == 0:
        assert transcripts == 0
        return

    coverage = transcripts / videos
    assert coverage >= 0.5, f"transcript coverage too low: {coverage:.2%}"


def test_vector_feature_views_present_and_nonempty():
    _, views = _tables_and_views()
    missing = _REQUIRED_VIEWS - views
    if missing:
        pytest.skip(f"Feature views not yet created: {sorted(missing)}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for v in _REQUIRED_VIEWS:
        count = _count(cur, v)
        assert count > 0, f"view {v} returned no rows"
    conn.close()


def test_mentions_have_player_ids_when_available():
    tables, _ = _tables_and_views()
    if "youtube_player_mentions" not in tables:
        pytest.skip("ETL table youtube_player_mentions not yet created")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    total = _count(cur, "youtube_player_mentions")
    if total == 0:
        conn.close()
        return

    cur.execute(
        "SELECT COUNT(*) FROM youtube_player_mentions WHERE player_id IS NOT NULL"
    )
    mapped = cur.fetchone()[0]
    conn.close()

    assert (mapped / total) >= 0.7, "player-id mapping ratio below threshold"

import os
import sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv("FANTASY_DB_PATH", "./fantasy_baseball.db"))


def _count(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


def test_required_tables_exist():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}
    conn.close()

    required = {
        'youtube_videos',
        'youtube_transcripts',
        'youtube_player_mentions',
    }
    for t in required:
        assert t in tables, f"missing required table: {t}"


def test_transcript_coverage_minimum():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    videos = _count(cur, 'youtube_videos')
    transcripts = _count(cur, 'youtube_transcripts')
    conn.close()

    if videos == 0:
        assert transcripts == 0
        return

    coverage = transcripts / videos
    assert coverage >= 0.5, f"transcript coverage too low: {coverage:.2%}"


def test_vector_feature_views_present_and_nonempty():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='view'")
    views = {r[0] for r in cur.fetchall()}

    required_views = {
        'feature_view_mention_gap',
        'feature_view_star_shadow',
        'feature_view_acceleration',
    }
    missing = required_views - views
    assert not missing, f"missing feature views: {sorted(missing)}"

    for v in required_views:
        cur.execute(f"SELECT COUNT(*) FROM {v}")
        count = cur.fetchone()[0]
        assert count > 0, f"view {v} returned no rows"

    conn.close()


def test_mentions_have_player_ids_when_available():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM youtube_player_mentions")
    total = cur.fetchone()[0]
    if total == 0:
        conn.close()
        return

    cur.execute("SELECT COUNT(*) FROM youtube_player_mentions WHERE player_id IS NOT NULL")
    mapped = cur.fetchone()[0]
    conn.close()

    assert (mapped / total) >= 0.7, "player-id mapping ratio below threshold"

#!/usr/bin/env python3
"""
daily_sync.py
Daily ETL pipeline for fantasy baseball stats using pybaseball.
"""

import os
import sys
import logging
from datetime import datetime, date
from pathlib import Path

# Add backend to path for imports
backend_path = Path(__file__).parent.parent / "backend"
sys.path.insert(0, str(backend_path))

import asyncio
import aiosqlite
from pybaseball import batting_stats, pitching_stats

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database path (matches the async database URL pattern from database.py)
DB_PATH = "/home/jesse/clawd-steve/data/fantasy_baseball.db"


async def init_db():
    """Initialize database tables if they don't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Create players table (simplified from models)
        await db.execute('''
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                espn_id INTEGER UNIQUE,
                name TEXT NOT NULL,
                position TEXT,
                team TEXT,
                age INTEGER,
                avg REAL,
                hr INTEGER,
                rbi INTEGER,
                sb INTEGER,
                ops REAL,
                war REAL,
                adp REAL,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create player_stats table for historical tracking
        await db.execute('''
            CREATE TABLE IF NOT EXISTS player_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER,
                season INTEGER,
                hr INTEGER,
                rbi INTEGER,
                sb INTEGER,
                avg REAL,
                ops REAL,
                war REAL,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (player_id) REFERENCES players (id)
            )
        ''')
        
        # Create adp_entries table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS adp_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER,
                season INTEGER,
                adp_value REAL,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (player_id) REFERENCES players (id)
            )
        ''')
        
        await db.commit()
        logger.info("Database tables initialized")


def choose_target_season(today: date) -> int:
    """Pick the season to sync. Before March, default to previous season."""
    return today.year - 1 if today.month < 3 else today.year


def fetch_batting_stats_with_fallback(target_year: int):
    """Fetch batting stats with one-step fallback for preseason/403 scenarios."""
    years_to_try = [target_year, target_year - 1]
    last_error = None

    for idx, year in enumerate(years_to_try):
        try:
            logger.info(f"Fetching batting stats for {year}...")
            df = batting_stats(year, year)
            logger.info(f"Retrieved {len(df)} player records for {year}")
            return df, year
        except Exception as e:
            last_error = e
            msg = str(e).lower()
            is_forbidden = "403" in msg or "forbidden" in msg

            if idx == 0 and is_forbidden:
                logger.warning(
                    f"Primary season {year} blocked (403). Retrying with fallback season {year - 1}."
                )
                continue

            logger.error(f"Failed to fetch season {year}: {e}")
            break

    raise RuntimeError(f"Unable to fetch batting stats. Last error: {last_error}")


async def sync_batting_stats(year: int):
    """Sync batting stats from pybaseball for a specific season."""
    try:
        # Pull stats from pybaseball with fallback
        df, synced_year = fetch_batting_stats_with_fallback(year)

        async with aiosqlite.connect(DB_PATH) as db:
            players_added = 0
            players_updated = 0

            for _, row in df.iterrows():
                name = row.get('Name')
                if not name:
                    continue

                team = row.get('Tm', 'FA')
                position = row.get('Pos', '')
                age = int(row['Age']) if pd.notna(row.get('Age')) else None

                # Parse stats
                hr = int(row['HR']) if pd.notna(row.get('HR')) else 0
                rbi = int(row['RBI']) if pd.notna(row.get('RBI')) else 0
                sb = int(row['SB']) if pd.notna(row.get('SB')) else 0
                avg = float(row['BA']) if pd.notna(row.get('BA')) else None
                ops = float(row['OPS']) if pd.notna(row.get('OPS')) else None
                war = float(row['WAR']) if pd.notna(row.get('WAR')) else None

                # Try to find existing player
                cursor = await db.execute(
                    "SELECT id FROM players WHERE name = ?",
                    (name,)
                )
                existing = await cursor.fetchone()

                if existing:
                    player_id = existing[0]
                    await db.execute('''
                        UPDATE players
                        SET team = ?, position = ?, age = ?, hr = ?, rbi = ?,
                            sb = ?, avg = ?, ops = ?, war = ?, updated_at = ?
                        WHERE id = ?
                    ''', (team, position, age, hr, rbi, sb, avg, ops, war,
                          datetime.now(), player_id))
                    players_updated += 1
                else:
                    cursor = await db.execute('''
                        INSERT INTO players (name, team, position, age, hr, rbi, sb, avg, ops, war)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (name, team, position, age, hr, rbi, sb, avg, ops, war))
                    player_id = cursor.lastrowid
                    players_added += 1

                # Insert into player_stats for historical tracking
                await db.execute('''
                    INSERT INTO player_stats (player_id, season, hr, rbi, sb, avg, ops, war)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (player_id, synced_year, hr, rbi, sb, avg, ops, war))

            await db.commit()
            logger.info(
                f"Players added: {players_added}, updated: {players_updated} (season {synced_year})"
            )
            return players_added, players_updated, synced_year

    except Exception as e:
        logger.error(f"Failed to sync batting stats: {e}")
        raise


async def main():
    """Main ETL routine."""
    now = datetime.now()
    logger.info(f"Starting daily sync at {now}")

    # Initialize database
    await init_db()

    # Preseason-safe season selection with fallback inside sync function
    year = choose_target_season(now.date())
    added, updated, synced_year = await sync_batting_stats(year)

    logger.info(f"Sync complete: {added} added, {updated} updated (season {synced_year})")
    return 0


if __name__ == "__main__":
    import pandas as pd  # Import here to avoid issues if pybaseball not installed
    sys.exit(asyncio.run(main()))

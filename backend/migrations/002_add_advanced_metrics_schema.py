"""
Database Migration: Add Advanced Offense + Statcast Schema

Creates player_offense_advanced and player_statcast tables.

Usage:
    cd /home/jesse/clawd-steve/fantasy-baseball/backend
    python3 migrations/002_add_advanced_metrics_schema.py
"""

import asyncio
import sys
sys.path.append('/home/jesse/clawd-steve/fantasy-baseball/backend')

from app.db.database import engine
from sqlalchemy import text


async def migrate():
    """Add advanced metrics tables with indexes and constraints."""
    print("Creating advanced metrics tables...")
    
    async with engine.begin() as conn:
        # PlayerOffenseAdvanced table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS player_offense_advanced (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL,
                season INTEGER NOT NULL,
                wrc_plus FLOAT,
                iso FLOAT,
                bb_pct FLOAT,
                k_pct FLOAT,
                obp FLOAT,
                slg FLOAT,
                woba FLOAT,
                xwoba FLOAT,
                extraction_timestamp DATETIME NOT NULL,
                source VARCHAR,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME,
                FOREIGN KEY (player_id) REFERENCES players(id)
            )
        """))
        
        # Unique index on player_id + season
        await conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_offense_player_season 
            ON player_offense_advanced(player_id, season)
        """))
        
        # Standard index for queries
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_offense_player 
            ON player_offense_advanced(player_id)
        """))
        
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_offense_season 
            ON player_offense_advanced(season)
        """))
        
        print("  ✅ player_offense_advanced table created")
        
        # PlayerStatcast table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS player_statcast (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL,
                season INTEGER NOT NULL,
                barrel_pct FLOAT,
                hard_hit_pct FLOAT,
                avg_exit_velocity FLOAT,
                max_exit_velocity FLOAT,
                launch_angle FLOAT,
                sweet_spot_pct FLOAT,
                xslg FLOAT,
                sprint_speed FLOAT,
                rolling_7d JSON,
                rolling_14d JSON,
                rolling_30d JSON,
                extraction_timestamp DATETIME NOT NULL,
                source VARCHAR,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME,
                FOREIGN KEY (player_id) REFERENCES players(id)
            )
        """))
        
        # Unique index on player_id + season
        await conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_statcast_player_season 
            ON player_statcast(player_id, season)
        """))
        
        # Standard indexes
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_statcast_player 
            ON player_statcast(player_id)
        """))
        
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_statcast_season 
            ON player_statcast(season)
        """))
        
        print("  ✅ player_statcast table created")
    
    print("\n✅ Migration complete!")
    print("\nCreated tables:")
    print("  - player_offense_advanced")
    print("  - player_statcast")
    print("\nIndexes created:")
    print("  - idx_offense_player_season (UNIQUE)")
    print("  - idx_offense_player")
    print("  - idx_offense_season")
    print("  - idx_statcast_player_season (UNIQUE)")
    print("  - idx_statcast_player")
    print("  - idx_statcast_season")


async def rollback():
    """Rollback migration - drop the new tables."""
    print("⚠️  Rolling back migration...")
    
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS player_offense_advanced"))
        await conn.execute(text("DROP TABLE IF EXISTS player_statcast"))
    
    print("✅ Tables dropped")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Migration 002: Advanced metrics schema')
    parser.add_argument('--rollback', action='store_true', help='Rollback migration')
    args = parser.parse_args()
    
    if args.rollback:
        asyncio.run(rollback())
    else:
        asyncio.run(migrate())

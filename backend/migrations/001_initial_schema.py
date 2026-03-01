"""
Database Migration Script

Run this to create all database tables for Fantasy Baseball application.

Usage:
    cd /home/jesse/clawd-steve/fantasy-baseball/backend
    python3 migrations/001_initial_schema.py
"""

import asyncio
import sys
sys.path.append('/home/jesse/clawd-steve/fantasy-baseball/backend')

from app.db.database import engine, Base
from app.models.models import (
    Player, 
    PlayerCard, 
    Team, 
    Scout, 
    ADPData, 
    Prospect, 
    TradeValue,
    PlayerOffenseAdvanced,
    PlayerStatcast
)

async def create_tables():
    """Create all database tables."""
    print("Creating database tables...")
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    print("✅ Tables created successfully!")
    print("\nCreated tables:")
    print("  - players")
    print("  - player_cards")
    print("  - teams")
    print("  - scouts")
    print("  - adp_data")
    print("  - prospects")
    print("  - trade_values")

async def drop_tables():
    """Drop all tables (use with caution!)."""
    print("⚠️  Dropping all tables...")
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    
    print("✅ Tables dropped")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Database migration script')
    parser.add_argument('--drop', action='store_true', help='Drop all tables first')
    args = parser.parse_args()
    
    if args.drop:
        asyncio.run(drop_tables())
    
    asyncio.run(create_tables())

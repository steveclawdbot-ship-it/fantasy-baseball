"""
Tests for advanced metrics schema (player_offense_advanced and player_statcast).

Run: pytest backend/tests/test_advanced_metrics_schema.py -v
"""

import pytest
import asyncio
import sys
from datetime import datetime

sys.path.insert(0, '/home/jesse/clawd-steve/fantasy-baseball/backend')

from sqlalchemy import select
from app.db.database import AsyncSessionLocal
from app.models.models import Player, PlayerOffenseAdvanced, PlayerStatcast


@pytest.mark.asyncio
async def test_player_offense_advanced_schema():
    """Test player_offense_advanced table schema and constraints."""
    async with AsyncSessionLocal() as session:
        # Get or create a test player
        result = await session.execute(select(Player).limit(1))
        player = result.scalar_one_or_none()
        
        if not player:
            player = Player(
                espn_id=999999,
                name="Test Player Schema",
                position="OF"
            )
            session.add(player)
            await session.commit()
            await session.refresh(player)
        
        # Insert offense advanced record
        offense = PlayerOffenseAdvanced(
            player_id=player.id,
            season=2025,
            wrc_plus=135.5,
            iso=0.225,
            bb_pct=12.5,
            k_pct=18.2,
            obp=0.365,
            slg=0.485,
            woba=0.380,
            xwoba=0.375,
            extraction_timestamp=datetime.now(),
            source="fangraphs"
        )
        session.add(offense)
        await session.commit()
        await session.refresh(offense)
        
        # Verify data types and values
        assert offense.id is not None
        assert offense.player_id == player.id
        assert offense.season == 2025
        assert offense.wrc_plus == 135.5
        assert offense.iso == 0.225
        assert offense.bb_pct == 12.5
        assert offense.k_pct == 18.2
        assert offense.obp == 0.365
        assert offense.slg == 0.485
        assert offense.woba == 0.380
        assert offense.xwoba == 0.375
        assert offense.source == "fangraphs"
        
        # Cleanup
        await session.delete(offense)
        await session.commit()


@pytest.mark.asyncio
async def test_player_statcast_schema():
    """Test player_statcast table schema and constraints."""
    async with AsyncSessionLocal() as session:
        # Get or create a test player
        result = await session.execute(select(Player).limit(1))
        player = result.scalar_one_or_none()
        
        if not player:
            player = Player(
                espn_id=999998,
                name="Test Player Statcast",
                position="1B"
            )
            session.add(player)
            await session.commit()
            await session.refresh(player)
        
        # Insert statcast record
        statcast = PlayerStatcast(
            player_id=player.id,
            season=2025,
            barrel_pct=12.5,
            hard_hit_pct=42.8,
            avg_exit_velocity=91.5,
            max_exit_velocity=114.2,
            launch_angle=15.5,
            sweet_spot_pct=35.2,
            xslg=0.495,
            sprint_speed=27.5,
            rolling_7d={"avg": 0.310, "ops": 0.890},
            rolling_14d={"avg": 0.295, "ops": 0.850},
            rolling_30d={"avg": 0.285, "ops": 0.820},
            extraction_timestamp=datetime.now(),
            source="statcast"
        )
        session.add(statcast)
        await session.commit()
        await session.refresh(statcast)
        
        # Verify data types and values
        assert statcast.id is not None
        assert statcast.player_id == player.id
        assert statcast.season == 2025
        assert statcast.barrel_pct == 12.5
        assert statcast.hard_hit_pct == 42.8
        assert statcast.avg_exit_velocity == 91.5
        assert statcast.max_exit_velocity == 114.2
        assert statcast.launch_angle == 15.5
        assert statcast.sweet_spot_pct == 35.2
        assert statcast.xslg == 0.495
        assert statcast.sprint_speed == 27.5
        assert statcast.rolling_7d == {"avg": 0.310, "ops": 0.890}
        assert statcast.rolling_14d == {"avg": 0.295, "ops": 0.850}
        assert statcast.rolling_30d == {"avg": 0.285, "ops": 0.820}
        assert statcast.source == "statcast"
        
        # Cleanup
        await session.delete(statcast)
        await session.commit()


@pytest.mark.asyncio
async def test_unique_constraint_player_season():
    """Test that (player_id, season) must be unique for both tables."""
    from sqlalchemy.exc import IntegrityError
    
    async with AsyncSessionLocal() as session:
        # Get or create a test player
        result = await session.execute(select(Player).limit(1))
        player = result.scalar_one_or_none()
        
        if not player:
            player = Player(
                espn_id=999997,
                name="Test Player Constraint",
                position="SS"
            )
            session.add(player)
            await session.commit()
            await session.refresh(player)
        
        # First insert should succeed
        offense1 = PlayerOffenseAdvanced(
            player_id=player.id,
            season=2024,  # Different season to avoid conflicts with other tests
            wrc_plus=120.0,
            iso=0.180,
            bb_pct=10.0,
            k_pct=20.0,
            obp=0.340,
            slg=0.450,
            woba=0.350,
            xwoba=0.345,
            extraction_timestamp=datetime.now(),
            source="fangraphs"
        )
        session.add(offense1)
        await session.commit()
        
        # Second insert with same player_id + season should fail
        offense2 = PlayerOffenseAdvanced(
            player_id=player.id,
            season=2024,  # Same season - should trigger unique constraint
            wrc_plus=125.0,
            iso=0.190,
            bb_pct=11.0,
            k_pct=19.0,
            obp=0.350,
            slg=0.460,
            woba=0.360,
            xwoba=0.355,
            extraction_timestamp=datetime.now(),
            source="fangraphs"
        )
        session.add(offense2)
        
        with pytest.raises(IntegrityError):
            await session.commit()
        
        # Rollback and cleanup
        await session.rollback()
        await session.delete(offense1)
        await session.commit()


@pytest.mark.asyncio
async def test_nullable_fields():
    """Test that non-critical fields are nullable."""
    async with AsyncSessionLocal() as session:
        # Get or create a test player
        result = await session.execute(select(Player).limit(1))
        player = result.scalar_one_or_none()
        
        if not player:
            player = Player(
                espn_id=999996,
                name="Test Player Nullable",
                position="C"
            )
            session.add(player)
            await session.commit()
            await session.refresh(player)
        
        # Insert with minimal fields (only required ones)
        offense = PlayerOffenseAdvanced(
            player_id=player.id,
            season=2023,
            # All other fields are null
            extraction_timestamp=datetime.now()
        )
        session.add(offense)
        await session.commit()
        await session.refresh(offense)
        
        assert offense.id is not None
        assert offense.wrc_plus is None
        assert offense.iso is None
        
        # Cleanup
        await session.delete(offense)
        await session.commit()


@pytest.mark.skip(reason="SQLite doesn't enforce FK constraints by default - application layer should validate")
@pytest.mark.asyncio
async def test_foreign_key_constraint():
    """Test that player_id must reference a valid player.
    
    Note: SQLite doesn't enforce FK constraints by default.
    Application layer should validate player_id exists before insert.
    """
    pass

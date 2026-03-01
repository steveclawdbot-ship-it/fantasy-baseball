from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Optional
from app.db.database import get_db
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

# Stats where lower is better
LOWER_IS_BETTER = {"era", "whip", "fip", "bb_per_9"}
VALID_STATS = {"era", "wins", "k_per_9", "whip", "fip", "war", "ip", "bb_per_9"}


@router.get("/leaders")
async def get_pitching_leaders(
    stat: str = Query("era", description="Stat to rank by: era, wins, k_per_9, whip, fip, war, ip, bb_per_9"),
    season: Optional[int] = None,
    limit: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
):
    """Get pitching leaders for a specific stat."""
    if stat not in VALID_STATS:
        raise HTTPException(status_code=400, detail=f"Invalid stat. Must be one of: {sorted(VALID_STATS)}")

    direction = "ASC" if stat in LOWER_IS_BETTER else "DESC"

    season_filter = ""
    params = {"limit": limit}
    if season:
        season_filter = "AND ps.season = :season"
        params["season"] = season

    sql = f"""
        SELECT p.id, p.name, p.position, p.team,
               ps.season, ps.games, ps.wins, ps.losses,
               ps.era, ps.whip, ps.ip, ps.k_per_9, ps.bb_per_9,
               ps.fip, ps.war
        FROM pitching_stats ps
        JOIN players p ON p.id = ps.player_id
        WHERE ps.{stat} IS NOT NULL {season_filter}
        ORDER BY ps.{stat} {direction}
        LIMIT :limit
    """

    try:
        result = await db.execute(text(sql), params)
        rows = [dict(r._mapping) for r in result.fetchall()]
        return rows
    except Exception as e:
        logger.error(f"Failed to get pitching leaders: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve pitching leaders")


@router.get("/player/{player_id}")
async def get_player_pitching(
    player_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get all pitching seasons for a player."""
    try:
        result = await db.execute(
            text("""
                SELECT ps.season, ps.games, ps.wins, ps.losses,
                       ps.era, ps.whip, ps.ip, ps.k_per_9, ps.bb_per_9,
                       ps.fip, ps.war
                FROM pitching_stats ps
                WHERE ps.player_id = :pid
                ORDER BY ps.season DESC
            """),
            {"pid": player_id},
        )
        rows = [dict(r._mapping) for r in result.fetchall()]
        if not rows:
            raise HTTPException(status_code=404, detail="No pitching data found for this player")
        return rows
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get player pitching: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve player pitching stats")


@router.get("/statcast/{player_id}")
async def get_pitcher_statcast(
    player_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get pitcher Statcast metrics (velocity, spin, whiff%, xERA)."""
    try:
        result = await db.execute(
            text("""
                SELECT psc.season, psc.avg_velocity, psc.max_velocity,
                       psc.spin_rate, psc.whiff_pct, psc.chase_pct,
                       psc.xera, psc.xwoba_against, psc.k_pct, psc.bb_pct,
                       psc.pitch_mix_json
                FROM pitcher_statcast psc
                WHERE psc.player_id = :pid
                ORDER BY psc.season DESC
            """),
            {"pid": player_id},
        )
        rows = [dict(r._mapping) for r in result.fetchall()]
        if not rows:
            raise HTTPException(status_code=404, detail="No pitcher statcast data found")
        return rows
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get pitcher statcast: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve pitcher statcast data")

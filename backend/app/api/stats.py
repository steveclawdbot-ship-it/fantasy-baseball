from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Optional
from app.db.database import get_db
from app.services.stats_service import create_stats_service
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


QUERY_PRESETS = {
    "power_profiles": {
        "title": "power profiles (2020-2025)",
        "description": "multi-season offense + statcast quality of contact",
        "sql": """
            WITH season_rollup AS (
              SELECT
                p.id AS player_id,
                p.name,
                COUNT(DISTINCT ps.season) AS seasons_covered,
                AVG(ps.hr) AS avg_hr,
                MAX(ps.hr) AS peak_hr,
                AVG(ps.ops) AS avg_ops
              FROM players p
              JOIN player_stats ps ON ps.player_id = p.id
              WHERE ps.season BETWEEN :start_year AND :end_year
              GROUP BY p.id, p.name
            ),
            statcast_rollup AS (
              SELECT
                psc.player_id,
                AVG(psc.barrel_pct) AS avg_barrel_pct,
                AVG(psc.hard_hit_pct) AS avg_hard_hit_pct,
                AVG(psc.avg_exit_velocity) AS avg_ev,
                MAX(psc.max_exit_velocity) AS peak_ev,
                AVG(psc.sprint_speed) AS avg_sprint_speed
              FROM player_statcast psc
              WHERE psc.season BETWEEN :start_year AND :end_year
              GROUP BY psc.player_id
            )
            SELECT
              sr.name,
              sr.seasons_covered,
              ROUND(sr.avg_hr, 1) AS avg_hr,
              sr.peak_hr,
              ROUND(sr.avg_ops, 3) AS avg_ops,
              ROUND(sc.avg_barrel_pct, 1) AS barrel_pct,
              ROUND(sc.avg_hard_hit_pct, 1) AS hard_hit_pct,
              ROUND(sc.avg_ev, 1) AS avg_exit_velo,
              ROUND(sc.peak_ev, 1) AS peak_exit_velo,
              ROUND(sc.avg_sprint_speed, 1) AS sprint_speed
            FROM season_rollup sr
            LEFT JOIN statcast_rollup sc ON sc.player_id = sr.player_id
            WHERE sr.seasons_covered >= 2
            ORDER BY sr.avg_hr DESC, sc.avg_barrel_pct DESC
            LIMIT :limit
        """,
    },
    "speed_profiles": {
        "title": "speed profiles (2020-2025)",
        "description": "stolen base production + sprint speed",
        "sql": """
            WITH sb_rollup AS (
              SELECT
                p.id AS player_id,
                p.name,
                COUNT(DISTINCT ps.season) AS seasons_covered,
                SUM(COALESCE(ps.sb, 0)) AS total_sb,
                AVG(COALESCE(ps.sb, 0)) AS avg_sb
              FROM players p
              JOIN player_stats ps ON ps.player_id = p.id
              WHERE ps.season BETWEEN :start_year AND :end_year
              GROUP BY p.id, p.name
            ),
            sprint_rollup AS (
              SELECT
                player_id,
                AVG(sprint_speed) AS avg_sprint_speed
              FROM player_statcast
              WHERE season BETWEEN :start_year AND :end_year
              GROUP BY player_id
            )
            SELECT
              sb.name,
              sb.seasons_covered,
              sb.total_sb,
              ROUND(sb.avg_sb, 1) AS avg_sb,
              ROUND(sr.avg_sprint_speed, 1) AS avg_sprint_speed
            FROM sb_rollup sb
            LEFT JOIN sprint_rollup sr ON sr.player_id = sb.player_id
            WHERE sb.seasons_covered >= 2
            ORDER BY sb.total_sb DESC, sr.avg_sprint_speed DESC
            LIMIT :limit
        """,
    },
    "xslg_gaps": {
        "title": "xslg vs ops gaps (2020-2025)",
        "description": "players underperforming contact quality",
        "sql": """
            WITH ops_rollup AS (
              SELECT
                p.id AS player_id,
                p.name,
                AVG(ps.ops) AS avg_ops
              FROM players p
              JOIN player_stats ps ON ps.player_id = p.id
              WHERE ps.season BETWEEN :start_year AND :end_year
              GROUP BY p.id, p.name
            ),
            xslg_rollup AS (
              SELECT
                player_id,
                AVG(xslg) AS avg_xslg
              FROM player_statcast
              WHERE season BETWEEN :start_year AND :end_year
              GROUP BY player_id
            )
            SELECT
              o.name,
              ROUND(o.avg_ops, 3) AS avg_ops,
              ROUND(x.avg_xslg, 3) AS avg_xslg,
              ROUND(x.avg_xslg - o.avg_ops, 3) AS xslg_ops_gap
            FROM ops_rollup o
            JOIN xslg_rollup x ON x.player_id = o.player_id
            WHERE x.avg_xslg IS NOT NULL AND o.avg_ops IS NOT NULL
            ORDER BY xslg_ops_gap DESC
            LIMIT :limit
        """,
    },
}


@router.get("/trends/overview")
async def get_trends_overview(
    window: str = Query("7d", pattern="^(today|7d|30d)$"),
    db: AsyncSession = Depends(get_db),
):
    """Trends overview scaffold for today / last 7d / last 30d dashboard surface."""
    try:
        latest_season_q = await db.execute(text("SELECT MAX(season) FROM player_stats"))
        season = latest_season_q.scalar() or 2025

        summary = await db.execute(text("""
            SELECT
              COUNT(DISTINCT ps.player_id) AS player_count,
              AVG(ps.hr) AS avg_hr,
              AVG(ps.ops) AS avg_ops,
              AVG(ps.sb) AS avg_sb
            FROM player_stats ps
            WHERE ps.season = :season
        """), {"season": season})
        s = summary.fetchone()

        leaders = await db.execute(text("""
            SELECT p.name, ps.hr, ps.ops, ps.sb
            FROM player_stats ps
            JOIN players p ON p.id = ps.player_id
            WHERE ps.season = :season
            ORDER BY ps.hr DESC
            LIMIT 12
        """), {"season": season})

        rows = [dict(r._mapping) for r in leaders.fetchall()]

        window_labels = {
            "today": "today",
            "7d": "last 7 days",
            "30d": "last 30 days",
        }

        return {
            "window": window,
            "window_label": window_labels[window],
            "season": season,
            "metrics": {
                "tracked_players": int(s.player_count or 0),
                "avg_hr": round(float(s.avg_hr or 0), 2),
                "avg_ops": round(float(s.avg_ops or 0), 3),
                "avg_sb": round(float(s.avg_sb or 0), 2),
            },
            "leaders": rows,
            "note": "Scaffold trends endpoint using current seasonal dataset. Replace with true game-window aggregates as live game ETL lands.",
        }
    except Exception as e:
        logger.error(f"Failed to load trends overview: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve trends overview")


@router.get("/queries")
async def get_query_presets():
    """List available safe query presets for dashboard exploration."""
    return [
        {
            "key": key,
            "title": preset["title"],
            "description": preset["description"],
        }
        for key, preset in QUERY_PRESETS.items()
    ]


@router.get("/queries/{query_key}")
async def run_query_preset(
    query_key: str,
    start_year: int = Query(2020, ge=2000, le=2100),
    end_year: int = Query(2025, ge=2000, le=2100),
    limit: int = Query(25, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Run a safe, parameterized query preset (no raw SQL input)."""
    preset = QUERY_PRESETS.get(query_key)
    if not preset:
        raise HTTPException(status_code=404, detail=f"Unknown query preset: {query_key}")

    if start_year > end_year:
        raise HTTPException(status_code=400, detail="start_year must be <= end_year")

    try:
        result = await db.execute(
            text(preset["sql"]),
            {"start_year": start_year, "end_year": end_year, "limit": limit},
        )
        rows = [dict(r._mapping) for r in result.fetchall()]
        return {
            "query_key": query_key,
            "title": preset["title"],
            "description": preset["description"],
            "start_year": start_year,
            "end_year": end_year,
            "limit": limit,
            "rows": rows,
            "count": len(rows),
        }
    except Exception as e:
        logger.error(f"Failed to run query preset {query_key}: {e}")
        raise HTTPException(status_code=500, detail="Failed to run query preset")


@router.get("/players/season/{year}", response_model=List[dict])
async def get_players_season_stats(
    year: int,
    player_name: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get player stats for a specific season from Baseball Reference/FanGraphs.
    
    Uses pybaseball to pull traditional and advanced baseball statistics.
    """
    try:
        stats_service = create_stats_service()
        players = stats_service.get_player_season_stats(year, player_name)
        
        if not players:
            raise HTTPException(status_code=404, detail=f"No players found for {year}")
        
        return players
    except Exception as e:
        logger.error(f"Failed to get player stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve player stats")


@router.get("/leaders/{year}")
async def get_leaders(
    year: int,
    stat: str = Query("HR", description="Stat to rank by: HR, RBI, SB, WAR, wRC+, OPS, BA, OBP, SLG"),
    limit: int = Query(10, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Get league leaders for a specific stat.
    
    Returns the top players in MLB for a given stat and season.
    """
    try:
        stats_service = create_stats_service()
        leaders = stats_service.get_league_leaders(year, stat, limit)
        
        if not leaders:
            raise HTTPException(status_code=404, detail=f"No leaders found for {stat} in {year}")
        
        return leaders
    except Exception as e:
        logger.error(f"Failed to get league leaders: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve league leaders")


@router.get("/statcast/{player_id}")
async def get_player_statcast(
    player_id: int,
    start_dt: Optional[str] = None,
    end_dt: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get Statcast data for a specific player.
    
    Returns advanced metrics from MLB's Statcast system (pitch tracking, release spin, etc.).
    """
    try:
        stats_service = create_stats_service()
        statcast = stats_service.get_player_statcast(player_id, start_dt, end_dt)
        
        if not statcast:
            raise HTTPException(status_code=404, detail=f"No statcast data found for player ID {player_id}")
        
        return statcast
    except Exception as e:
        logger.error(f"Failed to get statcast data: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve statcast data")

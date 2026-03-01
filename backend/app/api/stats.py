from datetime import date, datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Optional
from app.db.database import get_db
from app.services.stats_service import create_stats_service
import logging

try:
    from pybaseball import batting_stats_range
except Exception:
    batting_stats_range = None

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
    "pitching_aces": {
        "title": "pitching aces (2020-2025)",
        "description": "multi-season ERA/FIP leaders with Statcast stuff metrics",
        "sql": """
            WITH pit_rollup AS (
              SELECT
                p.id AS player_id,
                p.name,
                COUNT(DISTINCT ps.season) AS seasons_covered,
                ROUND(AVG(ps.era), 2) AS avg_era,
                ROUND(AVG(ps.fip), 2) AS avg_fip,
                ROUND(AVG(ps.k_per_9), 1) AS avg_k9,
                ROUND(SUM(ps.ip), 0) AS total_ip,
                ROUND(AVG(ps.war), 1) AS avg_war
              FROM players p
              JOIN pitching_stats ps ON ps.player_id = p.id
              WHERE ps.season BETWEEN :start_year AND :end_year
              GROUP BY p.id, p.name
            ),
            stuff_rollup AS (
              SELECT
                player_id,
                ROUND(AVG(avg_velocity), 1) AS avg_velo,
                ROUND(AVG(whiff_pct), 1) AS avg_whiff,
                ROUND(AVG(chase_pct), 1) AS avg_chase,
                ROUND(AVG(xera), 2) AS avg_xera
              FROM pitcher_statcast
              WHERE season BETWEEN :start_year AND :end_year
              GROUP BY player_id
            )
            SELECT
              pr.name,
              pr.seasons_covered,
              pr.avg_era,
              pr.avg_fip,
              pr.avg_k9,
              pr.total_ip,
              pr.avg_war,
              sr.avg_velo,
              sr.avg_whiff AS whiff_pct,
              sr.avg_chase AS chase_pct,
              sr.avg_xera AS xera
            FROM pit_rollup pr
            LEFT JOIN stuff_rollup sr ON sr.player_id = pr.player_id
            WHERE pr.seasons_covered >= 2 AND pr.total_ip >= 100
            ORDER BY pr.avg_era ASC
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


async def ensure_daily_trends_table(db: AsyncSession):
    await db.execute(text("""
        CREATE TABLE IF NOT EXISTS player_daily_trends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_date TEXT NOT NULL,
            player_name TEXT NOT NULL,
            team TEXT,
            hr INTEGER DEFAULT 0,
            sb INTEGER DEFAULT 0,
            avg REAL,
            ops REAL,
            pa INTEGER DEFAULT 0,
            source TEXT DEFAULT 'pybaseball',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(game_date, player_name)
        )
    """))
    await db.execute(text("CREATE INDEX IF NOT EXISTS idx_daily_trends_game_date ON player_daily_trends(game_date)"))


@router.post("/trends/sync")
async def sync_trends_window(
    window: str = Query("7d", pattern="^(today|7d|30d)$"),
    db: AsyncSession = Depends(get_db),
):
    """Pull day-window trends from pybaseball and persist for dashboard windows."""
    if batting_stats_range is None:
        raise HTTPException(status_code=500, detail="pybaseball not available in backend runtime")

    days = {"today": 1, "7d": 7, "30d": 30}[window]
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)

    try:
        await ensure_daily_trends_table(db)

        try:
            df = batting_stats_range(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        except Exception as fetch_err:
            logger.warning(f"batting_stats_range unavailable for {start_date}..{end_date}: {fetch_err}")
            return {
                "status": "empty",
                "window": window,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "rows_upserted": 0,
                "reason": str(fetch_err),
            }

        if df is None or len(df) == 0:
            return {
                "status": "empty",
                "window": window,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "rows_upserted": 0,
            }

        target_game_date = end_date.isoformat()
        upserted = 0
        for _, row in df.iterrows():
            name = row.get("Name")
            if not name:
                continue

            await db.execute(text("""
                INSERT INTO player_daily_trends (game_date, player_name, team, hr, sb, avg, ops, pa, source)
                VALUES (:game_date, :player_name, :team, :hr, :sb, :avg, :ops, :pa, :source)
                ON CONFLICT(game_date, player_name)
                DO UPDATE SET
                  team=excluded.team,
                  hr=excluded.hr,
                  sb=excluded.sb,
                  avg=excluded.avg,
                  ops=excluded.ops,
                  pa=excluded.pa,
                  source=excluded.source
            """), {
                "game_date": target_game_date,
                "player_name": name,
                "team": row.get("Tm") or "",
                "hr": int(row.get("HR") or 0),
                "sb": int(row.get("SB") or 0),
                "avg": float(row.get("AVG")) if row.get("AVG") is not None else None,
                "ops": float(row.get("OPS")) if row.get("OPS") is not None else None,
                "pa": int(row.get("PA") or 0),
                "source": "pybaseball",
            })
            upserted += 1

        await db.commit()
        return {
            "status": "ok",
            "window": window,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "rows_upserted": upserted,
        }
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to sync trends window: {e}")
        raise HTTPException(status_code=500, detail="Failed to sync trends window")


@router.get("/trends/overview")
async def get_trends_overview(
    window: str = Query("7d", pattern="^(today|7d|30d)$"),
    db: AsyncSession = Depends(get_db),
):
    """Trends overview for today / last 7d / last 30d; prefers daily trend table if populated."""
    try:
        await ensure_daily_trends_table(db)

        days = {"today": 1, "7d": 7, "30d": 30}[window]
        end_date = date.today()
        start_date = end_date - timedelta(days=days - 1)

        summary = await db.execute(text("""
            SELECT
              COUNT(DISTINCT player_name) AS player_count,
              AVG(hr) AS avg_hr,
              AVG(ops) AS avg_ops,
              AVG(sb) AS avg_sb,
              COUNT(*) AS row_count
            FROM player_daily_trends
            WHERE game_date BETWEEN :start_date AND :end_date
        """), {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()})
        s = summary.fetchone()

        use_fallback = (s is None) or int(s.row_count or 0) == 0

        if not use_fallback:
            leaders = await db.execute(text("""
                SELECT
                  player_name AS name,
                  SUM(hr) AS hr,
                  ROUND(AVG(ops), 3) AS ops,
                  SUM(sb) AS sb
                FROM player_daily_trends
                WHERE game_date BETWEEN :start_date AND :end_date
                GROUP BY player_name
                ORDER BY hr DESC, ops DESC
                LIMIT 20
            """), {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()})
            rows = [dict(r._mapping) for r in leaders.fetchall()]

            metrics = {
                "tracked_players": int(s.player_count or 0),
                "avg_hr": round(float(s.avg_hr or 0), 2),
                "avg_ops": round(float(s.avg_ops or 0), 3),
                "avg_sb": round(float(s.avg_sb or 0), 2),
            }
            note = None
            source = "daily_trends"
        else:
            latest_season_q = await db.execute(text("SELECT MAX(season) FROM player_stats"))
            season = latest_season_q.scalar() or datetime.now().year

            fallback_summary = await db.execute(text("""
                SELECT
                  COUNT(DISTINCT ps.player_id) AS player_count,
                  AVG(ps.hr) AS avg_hr,
                  AVG(ps.ops) AS avg_ops,
                  AVG(ps.sb) AS avg_sb
                FROM player_stats ps
                WHERE ps.season = :season
            """), {"season": season})
            fs = fallback_summary.fetchone()

            leaders = await db.execute(text("""
                SELECT p.name, ps.hr, ps.ops, ps.sb
                FROM player_stats ps
                JOIN players p ON p.id = ps.player_id
                WHERE ps.season = :season
                ORDER BY ps.hr DESC
                LIMIT 20
            """), {"season": season})
            rows = [dict(r._mapping) for r in leaders.fetchall()]
            metrics = {
                "tracked_players": int(fs.player_count or 0),
                "avg_hr": round(float(fs.avg_hr or 0), 2),
                "avg_ops": round(float(fs.avg_ops or 0), 3),
                "avg_sb": round(float(fs.avg_sb or 0), 2),
            }
            note = "Using seasonal fallback. Run POST /api/stats/trends/sync to hydrate true day-window trends."
            source = "seasonal_fallback"

        window_labels = {"today": "today", "7d": "last 7 days", "30d": "last 30 days"}
        return {
            "window": window,
            "window_label": window_labels[window],
            "date_range": {"start": start_date.isoformat(), "end": end_date.isoformat()},
            "metrics": metrics,
            "leaders": rows,
            "source": source,
            "note": note,
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

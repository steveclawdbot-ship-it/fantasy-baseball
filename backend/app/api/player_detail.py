from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from app.db.database import get_db
from app.models.models import Player
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/{player_id}")
async def get_player_detail(player_id: int, db: AsyncSession = Depends(get_db)):
    """Composite player detail — aggregates all available data for a single player."""

    # Base player info
    result = await db.execute(select(Player).where(Player.id == player_id))
    player = result.scalar_one_or_none()
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")

    async def query_rows(sql: str) -> list[dict]:
        try:
            r = await db.execute(text(sql), {"pid": player_id})
            return [dict(row._mapping) for row in r.fetchall()]
        except Exception as e:
            logger.warning(f"Player detail sub-query failed: {e}")
            return []

    async def query_one(sql: str) -> dict | None:
        rows = await query_rows(sql)
        return rows[0] if rows else None

    batting_seasons = await query_rows(
        "SELECT season, avg, hr, rbi, sb, ops, war FROM player_stats WHERE player_id = :pid ORDER BY season DESC"
    )

    pitching_seasons = await query_rows(
        "SELECT season, games, wins, losses, era, whip, ip, k_per_9, bb_per_9, fip, war "
        "FROM pitching_stats WHERE player_id = :pid ORDER BY season DESC"
    )

    statcast_batting = await query_rows(
        "SELECT season, barrel_pct, hard_hit_pct, avg_exit_velocity, max_exit_velocity, "
        "launch_angle, sweet_spot_pct, xslg, sprint_speed "
        "FROM player_statcast WHERE player_id = :pid ORDER BY season DESC"
    )

    statcast_pitching = await query_rows(
        "SELECT season, avg_velocity, max_velocity, spin_rate, whiff_pct, chase_pct, "
        "xera, xwoba_against, k_pct, bb_pct, pitch_mix_json "
        "FROM pitcher_statcast WHERE player_id = :pid ORDER BY season DESC"
    )

    advanced_offense = await query_rows(
        "SELECT season, wrc_plus, iso, bb_pct, k_pct, obp, slg, woba, xwoba "
        "FROM player_offense_advanced WHERE player_id = :pid ORDER BY season DESC"
    )

    prospect = await query_one(
        "SELECT overall_rank, position_rank, hit_future_value, power_future_value, "
        "speed_future_value, field_future_value, overall_future_value, "
        "eta, risk_level, ranking_source, hype_score, notes "
        "FROM prospects WHERE player_id = :pid"
    )

    scouting_notes = await query_rows(
        "SELECT notes, tags, rating, scout_name, created_at "
        "FROM player_cards WHERE player_id = :pid ORDER BY created_at DESC"
    )

    adp_history = await query_rows(
        "SELECT adp, min_pick, max_pick, source, league_type, date_recorded, "
        "adp_change_7d, adp_change_30d "
        "FROM adp_data WHERE player_id = :pid ORDER BY date_recorded DESC"
    )

    return {
        "player": player.to_dict(),
        "batting_seasons": batting_seasons,
        "pitching_seasons": pitching_seasons,
        "statcast_batting": statcast_batting,
        "statcast_pitching": statcast_pitching,
        "advanced_offense": advanced_offense,
        "prospect": prospect,
        "scouting_notes": scouting_notes,
        "adp_history": adp_history,
    }

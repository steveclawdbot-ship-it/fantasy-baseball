from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import get_db

router = APIRouter()


class RosterRow(BaseModel):
    league_name: str
    team_name: str
    player_name: str
    player_id: Optional[int] = None
    position: Optional[str] = None


class RosterImportRequest(BaseModel):
    rows: List[RosterRow]
    replace: bool = False


CREATE_ROSTER_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS league_rosters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league_name TEXT NOT NULL,
    team_name TEXT NOT NULL,
    player_name TEXT NOT NULL,
    player_id INTEGER,
    position TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(league_name, team_name, player_name)
)
"""


async def ensure_roster_table(db: AsyncSession):
    await db.execute(text(CREATE_ROSTER_TABLE_SQL))
    await db.execute(text("CREATE INDEX IF NOT EXISTS idx_league_rosters_league_team ON league_rosters(league_name, team_name)"))
    await db.execute(text("CREATE INDEX IF NOT EXISTS idx_league_rosters_player_name ON league_rosters(player_name)"))


@router.post("/rosters/import")
async def import_rosters(payload: RosterImportRequest, db: AsyncSession = Depends(get_db)):
    """Import roster rows for exposure analytics."""
    try:
        await ensure_roster_table(db)

        if payload.replace:
            await db.execute(text("DELETE FROM league_rosters"))

        inserted = 0
        for r in payload.rows:
            await db.execute(
                text(
                    """
                    INSERT INTO league_rosters (league_name, team_name, player_name, player_id, position)
                    VALUES (:league_name, :team_name, :player_name, :player_id, :position)
                    ON CONFLICT(league_name, team_name, player_name)
                    DO UPDATE SET
                      player_id=excluded.player_id,
                      position=excluded.position
                    """
                ),
                {
                    "league_name": r.league_name.strip(),
                    "team_name": r.team_name.strip(),
                    "player_name": r.player_name.strip(),
                    "player_id": r.player_id,
                    "position": (r.position or "").strip() or None,
                },
            )
            inserted += 1

        await db.commit()
        return {
            "status": "ok",
            "rows_received": len(payload.rows),
            "rows_upserted": inserted,
            "replace": payload.replace,
        }
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to import rosters: {e}")


@router.get("/exposure")
async def get_exposure(db: AsyncSession = Depends(get_db)):
    """Return player exposure across all tracked league rosters."""
    try:
        await ensure_roster_table(db)

        team_count_q = await db.execute(text("SELECT COUNT(DISTINCT league_name || '::' || team_name) FROM league_rosters"))
        team_count = team_count_q.scalar() or 0

        league_count_q = await db.execute(text("SELECT COUNT(DISTINCT league_name) FROM league_rosters"))
        league_count = league_count_q.scalar() or 0

        exposure_rows = await db.execute(
            text(
                """
                SELECT
                    player_name,
                    COUNT(DISTINCT league_name || '::' || team_name) AS teams_holding,
                    COUNT(DISTINCT league_name) AS leagues_holding
                FROM league_rosters
                GROUP BY player_name
                ORDER BY teams_holding DESC, leagues_holding DESC, player_name ASC
                LIMIT 200
                """
            )
        )

        rows = []
        for r in exposure_rows.fetchall():
            pct = (r.teams_holding / team_count * 100.0) if team_count else 0.0
            rows.append(
                {
                    "player_name": r.player_name,
                    "teams_holding": r.teams_holding,
                    "leagues_holding": r.leagues_holding,
                    "exposure_pct": round(pct, 1),
                }
            )

        by_league_q = await db.execute(
            text(
                """
                SELECT
                    league_name,
                    COUNT(DISTINCT team_name) AS teams,
                    COUNT(*) AS roster_slots
                FROM league_rosters
                GROUP BY league_name
                ORDER BY league_name ASC
                """
            )
        )

        by_league = [dict(r._mapping) for r in by_league_q.fetchall()]

        return {
            "team_count": team_count,
            "league_count": league_count,
            "rows": rows,
            "count": len(rows),
            "top_exposed": rows[:10],
            "by_league": by_league,
            "status": "ok" if team_count > 0 else "empty",
            "message": "No league roster data loaded yet" if team_count == 0 else None,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load league exposure: {e}")

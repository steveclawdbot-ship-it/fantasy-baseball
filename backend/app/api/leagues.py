from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.db.database import get_db

router = APIRouter()


@router.get("/exposure")
async def get_exposure(db: AsyncSession = Depends(get_db)):
    """Return player exposure across all tracked league rosters."""
    try:
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS league_rosters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                league_name TEXT NOT NULL,
                team_name TEXT NOT NULL,
                player_name TEXT NOT NULL,
                player_id INTEGER,
                position TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        result = await db.execute(text("SELECT COUNT(DISTINCT team_name) FROM league_rosters"))
        team_count = result.scalar() or 0

        exposure_rows = await db.execute(text("""
            SELECT
                player_name,
                COUNT(DISTINCT team_name) AS teams_holding
            FROM league_rosters
            GROUP BY player_name
            ORDER BY teams_holding DESC, player_name ASC
            LIMIT 100
        """))

        rows = []
        for r in exposure_rows.fetchall():
            pct = (r.teams_holding / team_count * 100.0) if team_count else 0.0
            rows.append({
                "player_name": r.player_name,
                "teams_holding": r.teams_holding,
                "exposure_pct": round(pct, 1),
            })

        return {
            "team_count": team_count,
            "rows": rows,
            "count": len(rows),
            "status": "ok" if team_count > 0 else "empty",
            "message": "No league roster data loaded yet" if team_count == 0 else None,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load league exposure: {e}")

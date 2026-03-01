from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, text
from typing import List, Optional
from app.db.database import get_db
from app.models.models import Player
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_model=List[dict])
async def get_players(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    position: Optional[str] = None,
    team: Optional[str] = None,
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get all players with optional filtering."""
    query = select(Player).offset(skip).limit(limit)
    
    # Apply filters
    if position:
        query = query.where(Player.position == position)
    if team:
        query = query.where(Player.team == team)
    if search:
        query = query.where(
            or_(
                Player.name.ilike(f"%{search}%"),
                Player.team.ilike(f"%{search}%")
            )
        )
    
    result = await db.execute(query)
    players = result.scalars().all()
    return [player.to_dict() for player in players]


@router.get("/{player_id}", response_model=dict)
async def get_player(player_id: int, db: AsyncSession = Depends(get_db)):
    """Get a specific player by ID."""
    query = select(Player).where(Player.id == player_id)
    result = await db.execute(query)
    player = result.scalar_one_or_none()
    
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    
    return player.to_dict()


@router.get("/espn/{espn_id}", response_model=dict)
async def get_player_by_espn_id(espn_id: int, db: AsyncSession = Depends(get_db)):
    """Get a specific player by ESPN ID."""
    query = select(Player).where(Player.espn_id == espn_id)
    result = await db.execute(query)
    player = result.scalar_one_or_none()
    
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    
    return player.to_dict()


@router.post("/", response_model=dict)
async def create_player(player_data: dict, db: AsyncSession = Depends(get_db)):
    """Create a new player.

    Writes to core_player (and core_player_source_id for espn_id) since
    the 'players' name is now a read-only serving view.
    """
    try:
        name = player_data.get("name")
        if not name:
            raise HTTPException(status_code=400, detail="'name' is required")

        result = await db.execute(
            text(
                "INSERT INTO core_player (display_name, position, mlb_team, player_level, created_at, updated_at) "
                "VALUES (:name, :position, :team, 'MLB', datetime('now'), datetime('now'))"
            ),
            {"name": name, "position": player_data.get("position"), "team": player_data.get("team")},
        )
        await db.flush()
        player_id = result.lastrowid

        espn_id = player_data.get("espn_id")
        if espn_id is not None:
            await db.execute(
                text(
                    "INSERT INTO core_player_source_id (player_id, source, source_player_id, confidence, matched_by, created_at) "
                    "VALUES (:pid, 'espn', :eid, 1.0, 'manual', datetime('now')) "
                    "ON CONFLICT(source, source_player_id) DO UPDATE SET player_id = excluded.player_id"
                ),
                {"pid": player_id, "eid": str(espn_id)},
            )

        await db.commit()
        logger.info(f"Created player: {name} (core_player.id={player_id})")

        # Read back through the view so the response matches the expected schema
        row = await db.execute(select(Player).where(Player.id == player_id))
        player = row.scalar_one_or_none()
        return player.to_dict() if player else {"id": player_id, "name": name}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create player: {e}")
        raise HTTPException(status_code=400, detail="Failed to create player")


@router.put("/{player_id}", response_model=dict)
async def update_player(player_id: int, player_data: dict, db: AsyncSession = Depends(get_db)):
    """Update a player.

    Writes to core_player directly since 'players' is a read-only view.
    """
    # Verify the player exists
    check = await db.execute(text("SELECT id FROM core_player WHERE id = :id AND is_active = 1"), {"id": player_id})
    if check.first() is None:
        raise HTTPException(status_code=404, detail="Player not found")

    # Map view column names to core_player columns
    col_map = {"name": "display_name", "team": "mlb_team", "position": "position", "player_level": "player_level"}
    sets = []
    params: dict = {"id": player_id}
    for key, value in player_data.items():
        if key == "id":
            continue
        core_col = col_map.get(key)
        if core_col:
            sets.append(f"{core_col} = :{key}")
            params[key] = value
        elif key == "espn_id" and value is not None:
            await db.execute(
                text(
                    "INSERT INTO core_player_source_id (player_id, source, source_player_id, confidence, matched_by, created_at) "
                    "VALUES (:pid, 'espn', :eid, 1.0, 'manual', datetime('now')) "
                    "ON CONFLICT(source, source_player_id) DO UPDATE SET player_id = excluded.player_id"
                ),
                {"pid": player_id, "eid": str(value)},
            )

    if sets:
        sets.append("updated_at = datetime('now')")
        await db.execute(text(f"UPDATE core_player SET {', '.join(sets)} WHERE id = :id"), params)

    await db.commit()

    row = await db.execute(select(Player).where(Player.id == player_id))
    player = row.scalar_one_or_none()
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")

    logger.info(f"Updated player: {player.name}")
    return player.to_dict()


@router.delete("/{player_id}")
async def delete_player(player_id: int, db: AsyncSession = Depends(get_db)):
    """Soft-delete a player.

    Sets is_active = 0 on core_player rather than hard-deleting, to avoid
    orphaning rows in the 10+ core_* tables that reference core_player(id).
    """
    check = await db.execute(text("SELECT id FROM core_player WHERE id = :id AND is_active = 1"), {"id": player_id})
    if check.first() is None:
        raise HTTPException(status_code=404, detail="Player not found")

    await db.execute(
        text("UPDATE core_player SET is_active = 0, updated_at = datetime('now') WHERE id = :id"),
        {"id": player_id},
    )
    await db.commit()
    logger.info(f"Soft-deleted player ID: {player_id}")
    return {"message": "Player deleted successfully"}


@router.get("/stats/leaders", response_model=List[dict])
async def get_leaders(
    stat: str = Query(..., description="Stat to rank by: hr, rbi, sb, avg, ops"),
    limit: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db)
):
    """Get league leaders for a specific stat."""
    valid_stats = ["hr", "rbi", "sb", "avg", "ops"]
    if stat not in valid_stats:
        raise HTTPException(status_code=400, detail=f"Invalid stat. Must be one of: {valid_stats}")
    
    query = select(Player).order_by(text(f"{stat} DESC")).limit(limit)
    result = await db.execute(query)
    players = result.scalars().all()
    return [player.to_dict() for player in players]

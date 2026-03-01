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
    """Create a new player."""
    try:
        player = Player(**player_data)
        db.add(player)
        await db.commit()
        await db.refresh(player)
        logger.info(f"Created player: {player.name}")
        return player.to_dict()
    except Exception as e:
        logger.error(f"Failed to create player: {e}")
        raise HTTPException(status_code=400, detail="Failed to create player")


@router.put("/{player_id}", response_model=dict)
async def update_player(player_id: int, player_data: dict, db: AsyncSession = Depends(get_db)):
    """Update a player."""
    query = select(Player).where(Player.id == player_id)
    result = await db.execute(query)
    player = result.scalar_one_or_none()
    
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    
    for key, value in player_data.items():
        if hasattr(player, key) and key != "id":
            setattr(player, key, value)
    
    await db.commit()
    await db.refresh(player)
    logger.info(f"Updated player: {player.name}")
    return player.to_dict()


@router.delete("/{player_id}")
async def delete_player(player_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a player."""
    query = select(Player).where(Player.id == player_id)
    result = await db.execute(query)
    player = result.scalar_one_or_none()
    
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    
    await db.delete(player)
    await db.commit()
    logger.info(f"Deleted player ID: {player_id}")
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

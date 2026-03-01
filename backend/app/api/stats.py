from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from app.db.database import get_db
from app.services.stats_service import create_stats_service
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


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

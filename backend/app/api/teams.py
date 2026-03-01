from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List, Optional
from app.db.database import get_db
from app.models.models import Team
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_model=List[dict])
async def get_teams(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    league_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get all fantasy teams with optional filtering."""
    query = select(Team).offset(skip).limit(limit)
    
    if league_type:
        query = query.where(Team.league_type == league_type)
    
    result = await db.execute(query)
    teams = result.scalars().all()
    
    return [
        {
            "id": team.id,
            "espn_league_id": team.espn_league_id,
            "espn_team_id": team.espn_team_id,
            "name": team.name,
            "owner": team.owner,
            "league_name": team.league_name,
            "league_type": team.league_type,
            "num_teams": team.num_teams,
            "created_at": team.created_at.isoformat() if team.created_at else None,
            "updated_at": team.updated_at.isoformat() if team.updated_at else None,
        }
        for team in teams
    ]


@router.get("/{team_id}", response_model=dict)
async def get_team(team_id: int, db: AsyncSession = Depends(get_db)):
    """Get a specific team by ID."""
    query = select(Team).where(Team.id == team_id)
    result = await db.execute(query)
    team = result.scalar_one_or_none()
    
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")
    
    return {
        "id": team.id,
        "espn_league_id": team.espn_league_id,
        "espn_team_id": team.espn_team_id,
        "name": team.name,
        "owner": team.owner,
        "league_name": team.league_name,
        "league_type": team.league_type,
        "num_teams": team.num_teams,
        "created_at": team.created_at.isoformat() if team.created_at else None,
        "updated_at": team.updated_at.isoformat() if team.updated_at else None,
    }


@router.post("/", response_model=dict)
async def create_team(team_data: dict, db: AsyncSession = Depends(get_db)):
    """Create a new team."""
    try:
        team = Team(**team_data)
        db.add(team)
        await db.commit()
        await db.refresh(team)
        logger.info(f"Created team: {team.name}")
        return {
            "id": team.id,
            "espn_league_id": team.espn_league_id,
            "espn_team_id": team.espn_team_id,
            "name": team.name,
            "owner": team.owner,
            "league_name": team.league_name,
            "league_type": team.league_type,
            "num_teams": team.num_teams,
        }
    except Exception as e:
        logger.error(f"Failed to create team: {e}")
        raise HTTPException(status_code=400, detail="Failed to create team")


@router.put("/{team_id}", response_model=dict)
async def update_team(team_id: int, team_data: dict, db: AsyncSession = Depends(get_db)):
    """Update a team."""
    query = select(Team).where(Team.id == team_id)
    result = await db.execute(query)
    team = result.scalar_one_or_none()
    
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")
    
    for key, value in team_data.items():
        if hasattr(team, key) and key != "id":
            setattr(team, key, value)
    
    await db.commit()
    await db.refresh(team)
    logger.info(f"Updated team: {team.name}")
    return {
        "id": team.id,
        "espn_league_id": team.espn_league_id,
        "espn_team_id": team.espn_team_id,
        "name": team.name,
        "owner": team.owner,
        "league_name": team.league_name,
        "league_type": team.league_type,
        "num_teams": team.num_teams,
    }

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import get_db
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    """Health check endpoint."""
    try:
        # Try to execute a simple query
        await db.execute(text("SELECT 1"))
        return {
            "status": "healthy",
            "service": "Fantasy Baseball API",
            "database": "connected"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")

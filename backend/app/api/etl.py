"""ETL trigger API — POST /api/etl/sync to run pipeline as a background task."""

import asyncio
import logging
from enum import Enum
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

router = APIRouter()
logger = logging.getLogger(__name__)

# Simple lock to prevent concurrent ETL runs
_running = False


class ETLSource(str, Enum):
    full = "full"
    batting = "batting"
    pitching = "pitching"
    statcast_batter = "statcast_batter"
    statcast_pitcher = "statcast_pitcher"
    espn = "espn"
    fantrax = "fantrax"
    milb = "milb"
    prospects = "prospects"


async def _run_etl(source: str) -> None:
    """Execute the ETL pipeline in background."""
    global _running
    if _running:
        logger.warning("ETL already running, skipping")
        return
    _running = True
    try:
        # Import here to avoid circular imports at module load
        from etl.runner import run_pipeline
        await run_pipeline(source=source)
    except Exception:
        logger.exception(f"ETL pipeline failed for source={source}")
    finally:
        _running = False


@router.post("/sync")
async def trigger_sync(
    background_tasks: BackgroundTasks,
    source: ETLSource = Query(ETLSource.full, description="Which source(s) to sync"),
):
    """Trigger an ETL sync as a background task."""
    if _running:
        raise HTTPException(status_code=409, detail="ETL pipeline is already running")
    background_tasks.add_task(_run_etl, source.value)
    return {"status": "started", "source": source.value}


@router.get("/status")
async def etl_status():
    """Check if an ETL run is in progress."""
    return {"running": _running}

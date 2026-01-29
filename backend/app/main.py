from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from app.db.database import init_db, close_db
from app.api import players, teams, health, stats

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    logger.info("Starting up Fantasy Baseball API...")
    await init_db()
    logger.info("Database initialized.")
    yield
    logger.info("Shutting down Fantasy Baseball API...")
    await close_db()
    logger.info("Database connection closed.")


app = FastAPI(
    title="Fantasy Baseball API",
    description="API for Fantasy Baseball scouting, ADP tracking, and decision engine",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, prefix="/api", tags=["Health"])
app.include_router(players.router, prefix="/api/players", tags=["Players"])
app.include_router(teams.router, prefix="/api/teams", tags=["Teams"])
app.include_router(stats.router, prefix="/api/stats", tags=["Stats"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Fantasy Baseball API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/health"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

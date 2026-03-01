from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
import logging
import os

logger = logging.getLogger(__name__)

# Keep one canonical DB path across API + ETL + crawlers.
DEFAULT_DB_PATH = "/home/jesse/clawd-steve/data/fantasy_baseball.db"
DB_PATH = os.getenv("FANTASY_DB_PATH", DEFAULT_DB_PATH)
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite+aiosqlite:///{DB_PATH}")

# Create async engine
engine = create_async_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=False  # Set to True for SQL query logging
)

# Create async session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# Base class for models
Base = declarative_base()


async def init_db():
    """Initialize database tables and run ETL schema migrations."""
    try:
        # Run ETL pipeline migrations (staging + core + serving views)
        from etl.schema.migrate import run_migrations_from_path
        await run_migrations_from_path(DB_PATH)
        logger.info("ETL schema migrations applied.")
    except Exception as e:
        # Non-fatal: ETL schema is additive, backend can still start
        logger.warning(f"ETL schema migration skipped: {e}")

    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables created successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


async def close_db():
    """Close database connections."""
    try:
        await engine.dispose()
        logger.info("Database connection closed.")
    except Exception as e:
        logger.error(f"Failed to close database: {e}")


async def get_db():
    """Dependency for getting database sessions."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

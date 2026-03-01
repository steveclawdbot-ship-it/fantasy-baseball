from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import StaticPool
import logging
import os

logger = logging.getLogger(__name__)

# SQLite database URL — use the same env var as the ETL pipeline
_db_path = os.getenv("FANTASY_DB_PATH", "./fantasy_baseball.db")
DATABASE_URL = f"sqlite+aiosqlite:///{_db_path}"

# Create async engine
engine = create_async_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
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
    """Initialize database tables."""
    try:
        # Run ETL schema migrations (staging, core, serving) before ORM tables
        try:
            from etl.schema.migrate import run_migrations
            from etl.db import get_connection
            async with get_connection(_db_path) as etl_db:
                await run_migrations(etl_db)
            logger.info("ETL schema migrations applied.")
        except Exception as mig_err:
            logger.warning(f"ETL migration skipped (non-fatal): {mig_err}")

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

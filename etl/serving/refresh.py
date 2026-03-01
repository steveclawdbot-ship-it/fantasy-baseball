"""Rebuild serving views from core tables.

Drops and recreates all serving views so the ORM reads fresh data.
"""

import logging
from etl.schema.serving import create_serving_views

logger = logging.getLogger(__name__)


async def refresh_serving_layer(db) -> None:
    """Drop and recreate all serving views."""
    await create_serving_views(db)
    logger.info("Serving views refreshed")

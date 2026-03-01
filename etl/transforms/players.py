"""Transform: Orchestrate identity resolution for a batch.

This is the first transform that must run before any stat transforms,
since stat transforms need core_player.id to write foreign keys.
"""

import logging
from etl.identity import resolve_all

logger = logging.getLogger(__name__)


async def transform_players(db, batch_id: str) -> dict:
    """Run full identity resolution for a batch.

    Returns a dict of source -> count resolved.
    """
    results = await resolve_all(db, batch_id)
    total = sum(results.values())
    logger.info(f"Player identity resolution complete: {total} total mappings")
    return results

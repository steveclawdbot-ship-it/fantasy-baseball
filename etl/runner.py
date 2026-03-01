"""ETL orchestrator — extract → identity resolution → transform → refresh serving.

Usage:
    python -m etl.runner --full
    python -m etl.runner --source batting
    python -m etl.runner --source espn
"""

import argparse
import asyncio
import logging
import sys

from etl.config import new_batch_id, ESPN_LEAGUE_YEAR as CURRENT_SEASON
from etl.db import get_connection

logger = logging.getLogger(__name__)

# All valid source names
VALID_SOURCES = [
    "batting", "pitching", "statcast_batter", "statcast_pitcher",
    "espn", "fantrax", "milb", "prospects",
]


async def run_pipeline(source: str = "full") -> dict:
    """Run the ETL pipeline for the given source(s).

    Returns a summary dict with counts per stage.
    """
    batch_id = new_batch_id()
    season = CURRENT_SEASON
    summary: dict = {"batch_id": batch_id, "extract": {}, "transform": {}}

    sources = VALID_SOURCES if source == "full" else [source]

    async with get_connection() as db:
        # Ensure schema is up to date
        from etl.schema.migrate import run_migrations
        await run_migrations(db)

        # --- EXTRACT (extractors manage their own connections) ---
        for src in sources:
            try:
                count = await _extract(src, season, batch_id)
                summary["extract"][src] = count
                logger.info(f"Extracted {src}: {count}")
            except Exception:
                logger.exception(f"Extract failed for {src}")
                summary["extract"][src] = "error"

        # --- IDENTITY RESOLUTION ---
        try:
            from etl.transforms.players import transform_players
            player_counts = await transform_players(db, batch_id)
            summary["transform"]["players"] = player_counts
        except Exception:
            logger.exception("Identity resolution failed")
            summary["transform"]["players"] = "error"

        # --- TRANSFORM ---
        for src in sources:
            try:
                counts = await _transform(db, src, batch_id)
                summary["transform"][src] = counts
                logger.info(f"Transformed {src}: {counts}")
            except Exception:
                logger.exception(f"Transform failed for {src}")
                summary["transform"][src] = "error"

        # --- REFRESH SERVING VIEWS ---
        try:
            from etl.serving.refresh import refresh_serving_layer
            await refresh_serving_layer(db)
            summary["serving"] = "refreshed"
        except Exception:
            logger.exception("Serving layer refresh failed")
            summary["serving"] = "error"

    logger.info(f"Pipeline complete: {summary}")
    return summary


async def _extract(source: str, season: int, batch_id: str):
    """Run a single extractor, returning row count or counts dict.

    Extractors manage their own DB connections internally.
    """
    if source == "batting":
        from etl.extractors.pybaseball_batting import extract_batting
        return await extract_batting(season, batch_id)
    elif source == "pitching":
        from etl.extractors.pybaseball_pitching import extract_pitching
        return await extract_pitching(season, batch_id)
    elif source == "statcast_batter":
        from etl.extractors.statcast_batter import extract_statcast_batter
        return await extract_statcast_batter(season, batch_id)
    elif source == "statcast_pitcher":
        from etl.extractors.statcast_pitcher import extract_statcast_pitcher
        return await extract_statcast_pitcher(season, batch_id)
    elif source == "espn":
        from etl.extractors.espn_leagues import extract_espn
        return await extract_espn(season, batch_id)
    elif source == "fantrax":
        from etl.extractors.fantrax_leagues import extract_fantrax
        return await extract_fantrax(batch_id)
    elif source == "milb":
        from etl.extractors.milb_stats import extract_milb_batting, extract_milb_pitching
        bat = await extract_milb_batting(season, batch_id)
        pit = await extract_milb_pitching(season, batch_id)
        return {"batting": bat, "pitching": pit}
    elif source == "prospects":
        from etl.extractors.prospect_rankings import extract_prospect_rankings
        return await extract_prospect_rankings(batch_id)
    else:
        raise ValueError(f"Unknown source: {source}")


async def _transform(db, source: str, batch_id: str):
    """Run transforms for a given source."""
    if source == "batting":
        from etl.transforms.batting import transform_batting
        return await transform_batting(db, batch_id)
    elif source == "pitching":
        from etl.transforms.pitching import transform_pitching
        return await transform_pitching(db, batch_id)
    elif source == "statcast_batter":
        from etl.transforms.statcast import transform_statcast_batter
        return await transform_statcast_batter(db, batch_id)
    elif source == "statcast_pitcher":
        from etl.transforms.statcast import transform_statcast_pitcher
        return await transform_statcast_pitcher(db, batch_id)
    elif source == "espn":
        from etl.transforms.espn import transform_espn
        return await transform_espn(db, batch_id)
    elif source == "fantrax":
        from etl.transforms.fantrax import transform_fantrax
        return await transform_fantrax(db, batch_id)
    elif source == "milb":
        from etl.transforms.milb import transform_milb
        return await transform_milb(db, batch_id)
    elif source == "prospects":
        from etl.transforms.prospects import transform_prospects
        return await transform_prospects(db, batch_id)
    else:
        raise ValueError(f"Unknown source: {source}")


def main():
    parser = argparse.ArgumentParser(description="Fantasy Baseball ETL Pipeline")
    parser.add_argument("--full", action="store_true", help="Run full pipeline (all sources)")
    parser.add_argument("--source", type=str, choices=VALID_SOURCES, help="Run a single source")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    if not args.full and not args.source:
        parser.print_help()
        sys.exit(1)

    source = "full" if args.full else args.source
    result = asyncio.run(run_pipeline(source=source))
    print(f"\nPipeline result: {result}")


if __name__ == "__main__":
    main()

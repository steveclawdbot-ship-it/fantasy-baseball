"""Extract FanGraphs pitching stats via pybaseball into stg_pybaseball_pitching."""

import asyncio
import json
import logging
from datetime import datetime

import pandas as pd
from pybaseball import pitching_stats

from etl.config import new_batch_id
from etl.db import get_connection

logger = logging.getLogger(__name__)


def _fetch_pitching_with_fallback(season: int):
    """Fetch pitching stats, falling back to prior season on 403/Forbidden."""
    years_to_try = [season, season - 1]
    last_error = None

    for idx, year in enumerate(years_to_try):
        try:
            logger.info("Fetching pitching stats for %d...", year)
            df = pitching_stats(year, year, qual=0)
            logger.info("Retrieved %d pitching rows for %d", len(df), year)
            return df, year
        except Exception as exc:
            last_error = exc
            msg = str(exc).lower()
            is_forbidden = "403" in msg or "forbidden" in msg

            if idx == 0 and is_forbidden:
                logger.warning(
                    "Season %d blocked (403). Falling back to %d.",
                    year,
                    year - 1,
                )
                continue

            logger.error("Failed to fetch pitching season %d: %s", year, exc)
            break

    raise RuntimeError(f"Unable to fetch pitching stats. Last error: {last_error}")


async def extract_pitching(season: int, batch_id: str) -> int:
    """Pull FanGraphs pitching data and insert into stg_pybaseball_pitching.

    Returns the number of rows inserted.
    """
    df, actual_season = _fetch_pitching_with_fallback(season)

    if df.empty:
        logger.warning("pybaseball returned an empty pitching DataFrame for %d", season)
        return 0

    now = datetime.utcnow().isoformat()
    rows_inserted = 0

    async with get_connection() as db:
        for _, row in df.iterrows():
            name = row.get("Name")
            if not name:
                continue

            raw_json = json.dumps(
                {k: (v if pd.notna(v) else None) for k, v in row.items()},
                default=str,
            )

            # Map DataFrame columns -> staging columns
            idfg = int(row["IDfg"]) if pd.notna(row.get("IDfg")) else None
            key_mlbam = (
                int(row["key_mlbam"]) if pd.notna(row.get("key_mlbam")) else None
            )

            await db.execute(
                """
                INSERT INTO stg_pybaseball_pitching (
                    _extracted_at, _batch_id, _season,
                    name, team, age, games,
                    wins, losses, era, whip, ip,
                    k_per_9, bb_per_9, fip, war,
                    idfg, key_mlbam,
                    _raw_json
                ) VALUES (
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?,
                    ?
                )
                """,
                (
                    now,
                    batch_id,
                    actual_season,
                    str(name),
                    str(row.get("Team", "")) or None,
                    int(row["Age"]) if pd.notna(row.get("Age")) else None,
                    int(row["G"]) if pd.notna(row.get("G")) else None,
                    int(row["W"]) if pd.notna(row.get("W")) else None,
                    int(row["L"]) if pd.notna(row.get("L")) else None,
                    float(row["ERA"]) if pd.notna(row.get("ERA")) else None,
                    float(row["WHIP"]) if pd.notna(row.get("WHIP")) else None,
                    float(row["IP"]) if pd.notna(row.get("IP")) else None,
                    float(row["K/9"]) if pd.notna(row.get("K/9")) else None,
                    float(row["BB/9"]) if pd.notna(row.get("BB/9")) else None,
                    float(row["FIP"]) if pd.notna(row.get("FIP")) else None,
                    float(row["WAR"]) if pd.notna(row.get("WAR")) else None,
                    idfg,
                    key_mlbam,
                    raw_json,
                ),
            )
            rows_inserted += 1

        await db.commit()

    logger.info(
        "Inserted %d pitching rows into stg_pybaseball_pitching (batch=%s, season=%d)",
        rows_inserted,
        batch_id,
        actual_season,
    )
    return rows_inserted


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    _season = datetime.now().year
    _batch_id = new_batch_id()
    count = asyncio.run(extract_pitching(_season, _batch_id))
    print(f"Done. {count} rows inserted.")

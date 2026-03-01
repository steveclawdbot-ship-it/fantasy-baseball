"""Extract FanGraphs batting stats via pybaseball into stg_pybaseball_batting."""

import asyncio
import json
import logging
from datetime import datetime

import pandas as pd
from pybaseball import batting_stats

from etl.config import new_batch_id
from etl.db import get_connection

logger = logging.getLogger(__name__)


def _fetch_batting_with_fallback(season: int):
    """Fetch batting stats, falling back to prior season on 403/Forbidden."""
    years_to_try = [season, season - 1]
    last_error = None

    for idx, year in enumerate(years_to_try):
        try:
            logger.info("Fetching batting stats for %d...", year)
            df = batting_stats(year, year, qual=0)
            logger.info("Retrieved %d batting rows for %d", len(df), year)
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

            logger.error("Failed to fetch batting season %d: %s", year, exc)
            break

    raise RuntimeError(f"Unable to fetch batting stats. Last error: {last_error}")


async def extract_batting(season: int, batch_id: str) -> int:
    """Pull FanGraphs batting data and insert into stg_pybaseball_batting.

    Returns the number of rows inserted.
    """
    df, actual_season = _fetch_batting_with_fallback(season)

    if df.empty:
        logger.warning("pybaseball returned an empty batting DataFrame for %d", season)
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
                INSERT INTO stg_pybaseball_batting (
                    _extracted_at, _batch_id, _season,
                    name, team, age, games, pa, ab,
                    hits, hr, rbi, sb,
                    bb_pct, k_pct,
                    avg, obp, slg, ops,
                    woba, wrc_plus, iso, war,
                    idfg, key_mlbam,
                    _raw_json
                ) VALUES (
                    ?, ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, ?, ?,
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
                    int(row["PA"]) if pd.notna(row.get("PA")) else None,
                    int(row["AB"]) if pd.notna(row.get("AB")) else None,
                    int(row["H"]) if pd.notna(row.get("H")) else None,
                    int(row["HR"]) if pd.notna(row.get("HR")) else None,
                    int(row["RBI"]) if pd.notna(row.get("RBI")) else None,
                    int(row["SB"]) if pd.notna(row.get("SB")) else None,
                    float(row["BB%"]) if pd.notna(row.get("BB%")) else None,
                    float(row["K%"]) if pd.notna(row.get("K%")) else None,
                    float(row["AVG"]) if pd.notna(row.get("AVG")) else None,
                    float(row["OBP"]) if pd.notna(row.get("OBP")) else None,
                    float(row["SLG"]) if pd.notna(row.get("SLG")) else None,
                    float(row["OPS"]) if pd.notna(row.get("OPS")) else None,
                    float(row["wOBA"]) if pd.notna(row.get("wOBA")) else None,
                    float(row["wRC+"]) if pd.notna(row.get("wRC+")) else None,
                    float(row["ISO"]) if pd.notna(row.get("ISO")) else None,
                    float(row["WAR"]) if pd.notna(row.get("WAR")) else None,
                    idfg,
                    key_mlbam,
                    raw_json,
                ),
            )
            rows_inserted += 1

        await db.commit()

    logger.info(
        "Inserted %d batting rows into stg_pybaseball_batting (batch=%s, season=%d)",
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
    count = asyncio.run(extract_batting(_season, _batch_id))
    print(f"Done. {count} rows inserted.")

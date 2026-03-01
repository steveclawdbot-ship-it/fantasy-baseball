"""Extract MiLB batting and pitching stats into stg_milb_batting / stg_milb_pitching.

Data source: MLB Stats API (statsapi.mlb.com).
pybaseball does not expose dedicated MiLB season stats functions, so we hit the
public MLB Stats API directly.  Sport IDs:
    11 = Triple-A (AAA)
    12 = Double-A (AA)
    13 = High-A (A+)
    14 = Single-A (A)
"""

import asyncio
import json
import logging
from datetime import datetime

import pandas as pd
import requests

from etl.config import new_batch_id
from etl.db import get_connection

logger = logging.getLogger(__name__)

# MLB Stats API sport-id -> human-readable level label
SPORT_ID_TO_LEVEL: dict[int, str] = {
    11: "AAA",
    12: "AA",
    13: "A+",
    14: "A",
}

_BASE_URL = "https://statsapi.mlb.com/api/v1/stats"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_int(val) -> int | None:
    try:
        if val is None:
            return None
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    try:
        if val is None:
            return None
        return float(val)
    except (ValueError, TypeError):
        return None


def _fetch_milb_stats(season: int, group: str) -> pd.DataFrame:
    """Fetch MiLB stats from the MLB Stats API.

    Parameters
    ----------
    season : int
        The year to query.
    group : str
        ``"hitting"`` or ``"pitching"``.

    Returns
    -------
    pd.DataFrame
        One row per player-level combination.
    """
    sport_ids = ",".join(str(sid) for sid in SPORT_ID_TO_LEVEL)
    params = {
        "stats": "season",
        "group": group,
        "season": season,
        "sportIds": sport_ids,
        "limit": 5000,
        "offset": 0,
    }

    all_rows: list[dict] = []
    while True:
        logger.info(
            "Fetching MiLB %s stats for %d (offset=%d)...",
            group, season, params["offset"],
        )
        resp = requests.get(_BASE_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        splits = data.get("stats", [{}])[0].get("splits", []) if data.get("stats") else []
        if not splits:
            break

        for split in splits:
            player_info = split.get("player", {})
            stat = split.get("stat", {})
            sport = split.get("sport", {})
            team = split.get("team", {})
            sport_id = sport.get("id")

            row = {
                "mlbam_id": player_info.get("id"),
                "player_name": player_info.get("fullName"),
                "level": SPORT_ID_TO_LEVEL.get(sport_id, f"sport_{sport_id}"),
                "team": team.get("name"),
                "age": _safe_int(split.get("player", {}).get("currentAge")),
                "_raw": split,
            }
            row.update(stat)
            all_rows.append(row)

        # Paginate if the API returned a full page
        if len(splits) < params["limit"]:
            break
        params["offset"] += params["limit"]

    logger.info("Retrieved %d %s rows for MiLB season %d", len(all_rows), group, season)
    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# Public extraction functions
# ---------------------------------------------------------------------------

async def extract_milb_batting(season: int, batch_id: str) -> int:
    """Pull MiLB batting data and insert into stg_milb_batting.

    Returns the number of rows inserted.
    """
    df = _fetch_milb_stats(season, "hitting")

    if df.empty:
        logger.warning("No MiLB batting data returned for season %d", season)
        return 0

    now = datetime.utcnow().isoformat()
    rows_inserted = 0

    async with get_connection() as db:
        for _, row in df.iterrows():
            player_name = row.get("player_name")
            if not player_name:
                continue

            raw_json = json.dumps(
                {k: (v if pd.notna(v) else None) for k, v in row.items() if k != "_raw"},
                default=str,
            )
            # Prefer the nested _raw dict for full fidelity
            raw_payload = row.get("_raw")
            if raw_payload is not None:
                raw_json = json.dumps(raw_payload, default=str)

            # BB% and K% are not directly in the MLB Stats API; compute from
            # component stats when available.
            pa = _safe_int(row.get("plateAppearances"))
            bb = _safe_int(row.get("baseOnBalls"))
            so = _safe_int(row.get("strikeOuts"))
            bb_pct = (bb / pa) if (pa and bb is not None) else None
            k_pct = (so / pa) if (pa and so is not None) else None

            await db.execute(
                """
                INSERT INTO stg_milb_batting (
                    _extracted_at, _batch_id, _season,
                    mlbam_id, player_name, level, team, age,
                    games, pa, ab, hits, hr, rbi, sb,
                    bb_pct, k_pct,
                    avg, obp, slg, ops, wrc_plus,
                    _raw_json
                ) VALUES (
                    ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, ?, ?, ?,
                    ?
                )
                """,
                (
                    now,
                    batch_id,
                    season,
                    _safe_int(row.get("mlbam_id")),
                    str(player_name),
                    str(row.get("level", "")) or None,
                    str(row.get("team", "")) or None,
                    _safe_int(row.get("age")),
                    _safe_int(row.get("gamesPlayed")),
                    pa,
                    _safe_int(row.get("atBats")),
                    _safe_int(row.get("hits")),
                    _safe_int(row.get("homeRuns")),
                    _safe_int(row.get("rbi")),
                    _safe_int(row.get("stolenBases")),
                    _safe_float(bb_pct),
                    _safe_float(k_pct),
                    _safe_float(row.get("avg")) if pd.notna(row.get("avg")) else None,
                    _safe_float(row.get("obp")) if pd.notna(row.get("obp")) else None,
                    _safe_float(row.get("slg")) if pd.notna(row.get("slg")) else None,
                    _safe_float(row.get("ops")) if pd.notna(row.get("ops")) else None,
                    # wRC+ is not provided by MLB Stats API; leave NULL
                    None,
                    raw_json,
                ),
            )
            rows_inserted += 1

        await db.commit()

    logger.info(
        "Inserted %d rows into stg_milb_batting (batch=%s, season=%d)",
        rows_inserted, batch_id, season,
    )
    return rows_inserted


async def extract_milb_pitching(season: int, batch_id: str) -> int:
    """Pull MiLB pitching data and insert into stg_milb_pitching.

    Returns the number of rows inserted.
    """
    df = _fetch_milb_stats(season, "pitching")

    if df.empty:
        logger.warning("No MiLB pitching data returned for season %d", season)
        return 0

    now = datetime.utcnow().isoformat()
    rows_inserted = 0

    async with get_connection() as db:
        for _, row in df.iterrows():
            player_name = row.get("player_name")
            if not player_name:
                continue

            raw_payload = row.get("_raw")
            if raw_payload is not None:
                raw_json = json.dumps(raw_payload, default=str)
            else:
                raw_json = json.dumps(
                    {k: (v if pd.notna(v) else None) for k, v in row.items() if k != "_raw"},
                    default=str,
                )

            # Derive K/9 and BB/9 from IP and component counts when possible
            ip_str = row.get("inningsPitched")
            ip = _safe_float(ip_str)
            so = _safe_int(row.get("strikeOuts"))
            bb = _safe_int(row.get("baseOnBalls"))
            k_per_9 = (so / ip * 9) if (ip and so is not None) else None
            bb_per_9 = (bb / ip * 9) if (ip and bb is not None) else None

            await db.execute(
                """
                INSERT INTO stg_milb_pitching (
                    _extracted_at, _batch_id, _season,
                    mlbam_id, player_name, level, team, age,
                    games, wins, losses,
                    era, whip, ip, k_per_9, bb_per_9, fip,
                    _raw_json
                ) VALUES (
                    ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?
                )
                """,
                (
                    now,
                    batch_id,
                    season,
                    _safe_int(row.get("mlbam_id")),
                    str(player_name),
                    str(row.get("level", "")) or None,
                    str(row.get("team", "")) or None,
                    _safe_int(row.get("age")),
                    _safe_int(row.get("gamesPlayed")),
                    _safe_int(row.get("wins")),
                    _safe_int(row.get("losses")),
                    _safe_float(row.get("era")) if pd.notna(row.get("era")) else None,
                    _safe_float(row.get("whip")) if pd.notna(row.get("whip")) else None,
                    ip,
                    _safe_float(k_per_9),
                    _safe_float(bb_per_9),
                    # FIP is not provided by MLB Stats API; leave NULL
                    None,
                    raw_json,
                ),
            )
            rows_inserted += 1

        await db.commit()

    logger.info(
        "Inserted %d rows into stg_milb_pitching (batch=%s, season=%d)",
        rows_inserted, batch_id, season,
    )
    return rows_inserted


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    _season = datetime.now().year
    _batch_id = new_batch_id()

    bat_count = asyncio.run(extract_milb_batting(_season, _batch_id))
    pit_count = asyncio.run(extract_milb_pitching(_season, _batch_id))
    print(f"Done. {bat_count} batting rows, {pit_count} pitching rows inserted.")

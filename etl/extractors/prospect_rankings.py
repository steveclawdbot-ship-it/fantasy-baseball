"""Extract prospect rankings into stg_prospect_rankings.

Primary source: pybaseball.top_prospects() which pulls from FanGraphs.
Fallback: if pybaseball doesn't provide the function or it fails, we
attempt the MLB Stats API prospect endpoint.

Notes on data availability:
- pybaseball.top_prospects() returns FanGraphs' "The Board" rankings.
  Columns typically include: Name, Team, Position, Age, FV (future value),
  and various scouting grades — but column names vary across pybaseball
  versions.  We map whatever is available and leave missing columns NULL.
- FV grades (hit_fv, power_fv, etc.) may or may not be present depending
  on the pybaseball version and FanGraphs page structure.
"""

import asyncio
import json
import logging
from datetime import datetime

import pandas as pd

from etl.config import new_batch_id
from etl.db import get_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_int(val) -> int | None:
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return int(val)
    except (ValueError, TypeError):
        return None


def _fetch_via_pybaseball() -> tuple[pd.DataFrame, str]:
    """Try pybaseball.top_prospects() first.

    Returns (DataFrame, source_label).
    """
    try:
        from pybaseball import top_prospects  # type: ignore
    except ImportError:
        logger.warning("pybaseball.top_prospects is not available in this version")
        return pd.DataFrame(), "fangraphs"

    try:
        logger.info("Fetching prospect rankings via pybaseball.top_prospects()...")
        df = top_prospects()
        logger.info("pybaseball.top_prospects() returned %d rows", len(df))
        return df, "fangraphs"
    except Exception as exc:
        logger.warning("pybaseball.top_prospects() failed: %s", exc)
        return pd.DataFrame(), "fangraphs"


def _fetch_via_mlb_api() -> tuple[pd.DataFrame, str]:
    """Fallback: MLB Stats API prospect rankings endpoint.

    The /api/v1/draft/prospects endpoint provides MLB Pipeline rankings.
    """
    import requests

    url = "https://statsapi.mlb.com/api/v1/draft/prospects"
    params = {"season": datetime.now().year}
    logger.info("Fetching prospect rankings from MLB Stats API (%s)...", url)

    try:
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
    except Exception as exc:
        logger.error("MLB Stats API prospect request failed: %s", exc)
        return pd.DataFrame(), "mlb_pipeline"

    data = resp.json()
    prospects = data.get("prospects", [])
    if not prospects:
        logger.warning("MLB Stats API returned no prospects")
        return pd.DataFrame(), "mlb_pipeline"

    rows: list[dict] = []
    for p in prospects:
        person = p.get("person", {})
        rows.append({
            "player_name": person.get("fullName"),
            "mlbam_id": person.get("id"),
            "org": p.get("team", {}).get("name"),
            "position": person.get("primaryPosition", {}).get("abbreviation"),
            "overall_rank": _safe_int(p.get("rank")),
            "_raw": p,
        })

    logger.info("MLB Stats API returned %d prospect rows", len(rows))
    return pd.DataFrame(rows), "mlb_pipeline"


def _fetch_prospect_data() -> tuple[pd.DataFrame, str]:
    """Attempt pybaseball first, then MLB Stats API."""
    df, source = _fetch_via_pybaseball()
    if not df.empty:
        return df, source

    logger.info("pybaseball source unavailable; trying MLB Stats API fallback...")
    return _fetch_via_mlb_api()


# ---------------------------------------------------------------------------
# Column mapping helpers
# ---------------------------------------------------------------------------

def _get_col(row, *candidates, default=None):
    """Return the first non-null value from candidate column names."""
    for col in candidates:
        val = row.get(col)
        if val is not None and not (isinstance(val, float) and pd.isna(val)):
            return val
    return default


# ---------------------------------------------------------------------------
# Public extraction function
# ---------------------------------------------------------------------------

async def extract_prospect_rankings(batch_id: str) -> int:
    """Pull prospect rankings and insert into stg_prospect_rankings.

    Returns the number of rows inserted.
    """
    df, source = _fetch_prospect_data()

    if df.empty:
        logger.warning("No prospect ranking data available from any source")
        return 0

    now = datetime.utcnow().isoformat()
    rows_inserted = 0

    async with get_connection() as db:
        for _, row in df.iterrows():
            player_name = _get_col(row, "player_name", "Name", "PlayerName")
            if not player_name:
                continue

            # Build _raw_json from the full raw split if available, else from row
            raw_payload = row.get("_raw")
            if raw_payload is not None and isinstance(raw_payload, dict):
                raw_json = json.dumps(raw_payload, default=str)
            else:
                raw_json = json.dumps(
                    {k: (v if pd.notna(v) else None) for k, v in row.items()},
                    default=str,
                )

            # Map columns — pybaseball uses different names than our schema.
            # We try multiple candidate column names for robustness.
            mlbam_id = _safe_int(_get_col(row, "mlbam_id", "key_mlbam", "MLBAMID", "PlayerId"))
            idfg = _safe_int(_get_col(row, "idfg", "IDfg", "minorMasterID"))
            org = _get_col(row, "org", "Team", "Org", "team")
            position = _get_col(row, "position", "Pos", "Position")
            overall_rank = _safe_int(_get_col(row, "overall_rank", "Rank", "rank", "#"))
            position_rank = _safe_int(_get_col(row, "position_rank", "Pos Rank"))

            # FV / scouting grades — may not be present
            hit_fv = _safe_int(_get_col(row, "Hit", "hit_fv", "HIT"))
            power_fv = _safe_int(_get_col(row, "Power", "power_fv", "RAW"))
            speed_fv = _safe_int(_get_col(row, "Speed", "speed_fv", "SPD"))
            field_fv = _safe_int(_get_col(row, "Field", "field_fv", "FLD"))
            overall_fv = _safe_int(_get_col(row, "FV", "overall_fv", "Future Value"))
            eta = _get_col(row, "ETA", "eta")
            if eta is not None:
                eta = str(eta)
            risk_level = _get_col(row, "Risk", "risk_level")
            if risk_level is not None:
                risk_level = str(risk_level)

            await db.execute(
                """
                INSERT INTO stg_prospect_rankings (
                    _extracted_at, _batch_id,
                    player_name, mlbam_id, idfg,
                    org, position,
                    overall_rank, position_rank,
                    hit_fv, power_fv, speed_fv, field_fv, overall_fv,
                    eta, risk_level, ranking_source,
                    _raw_json
                ) VALUES (
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?,
                    ?
                )
                """,
                (
                    now,
                    batch_id,
                    str(player_name),
                    mlbam_id,
                    idfg,
                    str(org) if org else None,
                    str(position) if position else None,
                    overall_rank,
                    position_rank,
                    hit_fv,
                    power_fv,
                    speed_fv,
                    field_fv,
                    overall_fv,
                    eta,
                    risk_level,
                    source,
                    raw_json,
                ),
            )
            rows_inserted += 1

        await db.commit()

    logger.info(
        "Inserted %d rows into stg_prospect_rankings (batch=%s, source=%s)",
        rows_inserted, batch_id, source,
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

    _batch_id = new_batch_id()
    count = asyncio.run(extract_prospect_rankings(_batch_id))
    print(f"Done. {count} prospect ranking rows inserted.")

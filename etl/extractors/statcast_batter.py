"""Extract Statcast batter data from Baseball Savant via pybaseball.

Combines two Baseball Savant leaderboards:
  - expected_statistics (xwoba, xslg)
  - exit-velocity / barrels (barrel_pct, hard_hit_pct, avg_exit_velocity, etc.)

Sprint speed is pulled from FanGraphs batting_stats when available.
"""

import json
import logging
from datetime import datetime

import pandas as pd

from etl.db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ── pybaseball imports (deferred descriptions for clarity) ──────────────
from pybaseball import (
    statcast_batter_expected_stats,
    statcast_batter_exitvelo_barrels,
    batting_stats,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_with_fallback(fetch_fn, season: int, **kwargs):
    """Try *season*, fall back to *season - 1* on 403 / Forbidden errors."""
    years_to_try = [season, season - 1]
    last_error: Exception | None = None

    for idx, year in enumerate(years_to_try):
        try:
            logger.info("Fetching %s for %d …", fetch_fn.__name__, year)
            df = fetch_fn(year, **kwargs)
            logger.info("  → %d rows for %d", len(df), year)
            return df, year
        except Exception as exc:
            last_error = exc
            msg = str(exc).lower()
            is_forbidden = "403" in msg or "forbidden" in msg

            if idx == 0 and is_forbidden:
                logger.warning(
                    "Season %d blocked (403). Retrying with %d.",
                    year,
                    year - 1,
                )
                continue

            logger.error("Failed to fetch %s for %d: %s", fetch_fn.__name__, year, exc)
            break

    raise RuntimeError(
        f"Unable to fetch {fetch_fn.__name__}. Last error: {last_error}"
    )


def _safe_float(val):
    """Convert a value to float, returning None for NaN / missing."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (ValueError, TypeError):
        return None


def _safe_int(val):
    """Convert a value to int, returning None for NaN / missing."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if pd.isna(f) else int(f)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Column-name mappings (Baseball Savant names → our staging names)
# ---------------------------------------------------------------------------

# statcast_batter_exitvelo_barrels columns (after sanitize_statcast_columns):
#   last_name, first_name, player_id, attempts, avg_hit_angle,
#   anglesweetspotpercent, max_hit_speed, avg_hit_speed, ...
#   brl_percent (or brl_pa), ev95percent (hard-hit%)

# statcast_batter_expected_stats columns:
#   last_name, first_name, player_id, pa, bip, ba, est_ba,
#   slg, est_slg, woba, est_woba, ...


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------

async def extract_statcast_batter(season: int, batch_id: str) -> int:
    """Pull Statcast batter aggregates and load into stg_statcast_batter.

    Returns the number of rows inserted.
    """
    now = datetime.utcnow().isoformat()

    # 1. Exit-velocity / barrel leaderboard  --------------------------------
    ev_df, ev_year = _fetch_with_fallback(
        statcast_batter_exitvelo_barrels, season, minBBE=0,
    )

    # 2. Expected stats leaderboard  ----------------------------------------
    xstats_df, xs_year = _fetch_with_fallback(
        statcast_batter_expected_stats, season, minPA=0,
    )

    # 3. FanGraphs batting_stats (for sprint speed / Spd) -------------------
    try:
        fg_df, fg_year = _fetch_with_fallback(
            batting_stats, season, qual=0,
        )
    except Exception:
        logger.warning("Could not fetch FanGraphs batting_stats – sprint_speed will be NULL.")
        fg_df = pd.DataFrame()

    # ── Bail early if both primary sources are empty ──────────────────────
    if ev_df.empty and xstats_df.empty:
        logger.warning("Both Statcast sources returned empty DataFrames. Nothing to insert.")
        return 0

    # ── Normalise column names to lowercase ───────────────────────────────
    ev_df.columns = [c.lower().strip() for c in ev_df.columns]
    xstats_df.columns = [c.lower().strip() for c in xstats_df.columns]
    if not fg_df.empty:
        fg_df.columns = [c.lower().strip() for c in fg_df.columns]

    # ── Build per-player lookup dicts keyed by player_id (mlbam_id) ──────
    expected_by_id: dict[int, dict] = {}
    for _, row in xstats_df.iterrows():
        pid = _safe_int(row.get("player_id"))
        if pid is None:
            continue
        expected_by_id[pid] = {
            "xslg": _safe_float(row.get("est_slg")),
            "xwoba": _safe_float(row.get("est_woba")),
        }

    sprint_by_id: dict[int, float | None] = {}
    if not fg_df.empty:
        # FanGraphs key_mlbam maps to Baseball Savant player_id
        id_col = "key_mlbam" if "key_mlbam" in fg_df.columns else None
        spd_col = next(
            (c for c in fg_df.columns if c in ("spd", "sprint_speed", "sprint speed")),
            None,
        )
        if id_col and spd_col:
            for _, row in fg_df.iterrows():
                pid = _safe_int(row.get(id_col))
                if pid is not None:
                    sprint_by_id[pid] = _safe_float(row.get(spd_col))

    # ── Assemble rows and insert ─────────────────────────────────────────
    insert_sql = """
        INSERT INTO stg_statcast_batter (
            _extracted_at, _batch_id, _season,
            mlbam_id, player_name,
            barrel_pct, hard_hit_pct,
            avg_exit_velocity, max_exit_velocity,
            launch_angle, sweet_spot_pct,
            xslg, xwoba, sprint_speed,
            _raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    rows_inserted = 0

    async with get_connection() as db:
        for _, row in ev_df.iterrows():
            pid = _safe_int(row.get("player_id"))
            if pid is None:
                continue

            # Combine first/last name
            last = str(row.get("last_name, first_name", row.get("last_name", ""))).strip()
            first = str(row.get("first_name", "")).strip()
            if "," in last:
                # "last_name, first_name" is a single column on Savant
                player_name = last
            elif first:
                player_name = f"{last}, {first}"
            else:
                player_name = last or None

            barrel_pct = _safe_float(row.get("brl_percent", row.get("brl_pa")))
            hard_hit_pct = _safe_float(row.get("ev95percent"))
            avg_ev = _safe_float(row.get("avg_hit_speed"))
            max_ev = _safe_float(row.get("max_hit_speed"))
            launch_angle = _safe_float(row.get("avg_hit_angle"))
            sweet_spot = _safe_float(row.get("anglesweetspotpercent"))

            xstats = expected_by_id.get(pid, {})
            xslg = xstats.get("xslg")
            xwoba = xstats.get("xwoba")

            sprint_speed = sprint_by_id.get(pid)

            # Build raw JSON from all sources for this player
            raw = row.to_dict()
            if pid in expected_by_id:
                raw["_xstats"] = expected_by_id[pid]
            if pid in sprint_by_id:
                raw["_sprint_speed"] = sprint_by_id[pid]
            # Convert numpy types to native Python for JSON serialisation
            raw_json = json.dumps(
                {k: (v.item() if hasattr(v, "item") else v) for k, v in raw.items()},
                default=str,
            )

            await db.execute(insert_sql, (
                now, batch_id, ev_year,
                pid, player_name,
                barrel_pct, hard_hit_pct,
                avg_ev, max_ev,
                launch_angle, sweet_spot,
                xslg, xwoba, sprint_speed,
                raw_json,
            ))
            rows_inserted += 1

        await db.commit()

    logger.info(
        "stg_statcast_batter: inserted %d rows (season=%d, batch=%s)",
        rows_inserted,
        ev_year,
        batch_id,
    )
    return rows_inserted


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio
    import sys

    from etl.config import new_batch_id

    season = int(sys.argv[1]) if len(sys.argv) > 1 else datetime.now().year
    bid = new_batch_id()

    logger.info("Starting statcast_batter extraction for season %d, batch %s", season, bid)
    count = asyncio.run(extract_statcast_batter(season, bid))
    logger.info("Done. %d rows inserted.", count)

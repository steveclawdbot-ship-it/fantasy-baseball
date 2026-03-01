"""Extract Statcast pitcher data from Baseball Savant via pybaseball.

Combines several Baseball Savant leaderboards:
  - expected_statistics (xwoba-against, xERA proxy)
  - exit-velocity / barrels (avg_velocity, max_velocity)
  - pitch-arsenal (pitch mix breakdown, velocity by type, spin by type)

K% and BB% are sourced from FanGraphs pitching_stats.
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

# ── pybaseball imports ─────────────────────────────────────────────────
from pybaseball import (
    statcast_pitcher_expected_stats,
    statcast_pitcher_exitvelo_barrels,
    statcast_pitcher_pitch_arsenal,
    statcast_pitcher_arsenal_stats,
    pitching_stats,
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
# Core extraction
# ---------------------------------------------------------------------------

async def extract_statcast_pitcher(season: int, batch_id: str) -> int:
    """Pull Statcast pitcher aggregates and load into stg_statcast_pitcher.

    Returns the number of rows inserted.
    """
    now = datetime.utcnow().isoformat()

    # 1. Exit-velocity / barrel leaderboard (avg/max velo allowed) ----------
    ev_df, ev_year = _fetch_with_fallback(
        statcast_pitcher_exitvelo_barrels, season, minBBE=0,
    )

    # 2. Expected stats leaderboard (xwoba-against, etc.) -------------------
    xstats_df, _ = _fetch_with_fallback(
        statcast_pitcher_expected_stats, season, minPA=0,
    )

    # 3. Pitch arsenal – velocity by pitch type  ----------------------------
    try:
        arsenal_speed_df, _ = _fetch_with_fallback(
            statcast_pitcher_pitch_arsenal, season, minP=0, arsenal_type="avg_speed",
        )
    except Exception:
        logger.warning("Could not fetch pitch arsenal (avg_speed). pitch_mix will be partial.")
        arsenal_speed_df = pd.DataFrame()

    # 3b. Pitch arsenal – usage counts per pitch type -----------------------
    try:
        arsenal_n_df, _ = _fetch_with_fallback(
            statcast_pitcher_pitch_arsenal, season, minP=0, arsenal_type="n_",
        )
    except Exception:
        logger.warning("Could not fetch pitch arsenal (n_). pitch_mix will be partial.")
        arsenal_n_df = pd.DataFrame()

    # 3c. Pitch arsenal – spin by pitch type --------------------------------
    try:
        arsenal_spin_df, _ = _fetch_with_fallback(
            statcast_pitcher_pitch_arsenal, season, minP=0, arsenal_type="avg_spin",
        )
    except Exception:
        logger.warning("Could not fetch pitch arsenal (avg_spin).")
        arsenal_spin_df = pd.DataFrame()

    # 4. Arsenal outcome stats (whiff%, chase%) ----------------------------
    try:
        arsenal_stats_df, _ = _fetch_with_fallback(
            statcast_pitcher_arsenal_stats, season, minPA=0,
        )
    except Exception:
        logger.warning("Could not fetch arsenal outcome stats. whiff/chase will be NULL.")
        arsenal_stats_df = pd.DataFrame()

    # 5. FanGraphs pitching stats (K%, BB%, xERA, xFIP) --------------------
    try:
        fg_df, _ = _fetch_with_fallback(
            pitching_stats, season, qual=0,
        )
    except Exception:
        logger.warning("Could not fetch FanGraphs pitching_stats – K%%/BB%% will be NULL.")
        fg_df = pd.DataFrame()

    # ── Bail early if primary source is empty ─────────────────────────────
    if ev_df.empty and xstats_df.empty:
        logger.warning("Both Statcast sources returned empty DataFrames. Nothing to insert.")
        return 0

    # ── Normalise column names ────────────────────────────────────────────
    for df in [ev_df, xstats_df, arsenal_speed_df, arsenal_n_df,
               arsenal_spin_df, arsenal_stats_df, fg_df]:
        if not df.empty:
            df.columns = [c.lower().strip() for c in df.columns]

    # ── Build lookup dicts keyed by player_id (mlbam_id) ──────────────────

    # Expected stats
    expected_by_id: dict[int, dict] = {}
    for _, row in xstats_df.iterrows():
        pid = _safe_int(row.get("player_id"))
        if pid is None:
            continue
        expected_by_id[pid] = {
            "xwoba_against": _safe_float(row.get("est_woba")),
        }

    # FanGraphs K% / BB% / xERA
    fg_by_id: dict[int, dict] = {}
    if not fg_df.empty:
        id_col = "key_mlbam" if "key_mlbam" in fg_df.columns else None
        if id_col:
            for _, row in fg_df.iterrows():
                pid = _safe_int(row.get(id_col))
                if pid is None:
                    continue
                fg_by_id[pid] = {
                    "k_pct": _safe_float(row.get("k%", row.get("k_pct"))),
                    "bb_pct": _safe_float(row.get("bb%", row.get("bb_pct"))),
                    "xera": _safe_float(row.get("xera", row.get("xfip"))),
                }

    # Arsenal outcome stats – aggregate whiff% and chase% per pitcher
    whiff_chase_by_id: dict[int, dict] = {}
    if not arsenal_stats_df.empty:
        for _, row in arsenal_stats_df.iterrows():
            pid = _safe_int(row.get("player_id", row.get("pitcher")))
            if pid is None:
                continue
            # arsenal_stats has one row per pitch type; accumulate
            entry = whiff_chase_by_id.setdefault(pid, {"whiff_vals": [], "chase_vals": []})
            w = _safe_float(row.get("whiff_percent", row.get("whiff_pct")))
            c = _safe_float(row.get("chase_percent", row.get("chase_pct")))
            if w is not None:
                entry["whiff_vals"].append(w)
            if c is not None:
                entry["chase_vals"].append(c)

    def _avg_or_none(vals: list[float]) -> float | None:
        return sum(vals) / len(vals) if vals else None

    # Pitch-mix JSON: {pitch_type: {pct, avg_speed, avg_spin}}
    pitch_mix_by_id: dict[int, dict] = {}

    # Process usage counts to derive pitch-type percentages
    if not arsenal_n_df.empty:
        # Columns like: player_id, pitcher_name, ff, sl, ch, cu, si, fc, ...
        pitch_type_cols = [
            c for c in arsenal_n_df.columns
            if c not in ("player_id", "pitcher", "pitcher_name",
                         "last_name, first_name", "first_name", "last_name",
                         "team_name_abbrev", "team_name", "total_pitches",
                         "pitch_hand")
        ]
        for _, row in arsenal_n_df.iterrows():
            pid = _safe_int(row.get("player_id", row.get("pitcher")))
            if pid is None:
                continue
            mix: dict[str, dict] = {}
            total = 0.0
            for pt in pitch_type_cols:
                v = _safe_float(row.get(pt))
                if v is not None and v > 0:
                    mix[pt] = {"count": v}
                    total += v
            # Convert counts to percentages
            if total > 0:
                for pt in mix:
                    mix[pt]["pct"] = round(mix[pt]["count"] / total * 100, 1)
            pitch_mix_by_id[pid] = mix

    # Merge in speed per pitch type
    if not arsenal_speed_df.empty:
        pitch_type_cols_spd = [
            c for c in arsenal_speed_df.columns
            if c not in ("player_id", "pitcher", "pitcher_name",
                         "last_name, first_name", "first_name", "last_name",
                         "team_name_abbrev", "team_name", "pitch_hand")
        ]
        for _, row in arsenal_speed_df.iterrows():
            pid = _safe_int(row.get("player_id", row.get("pitcher")))
            if pid is None:
                continue
            mix = pitch_mix_by_id.setdefault(pid, {})
            for pt in pitch_type_cols_spd:
                v = _safe_float(row.get(pt))
                if v is not None:
                    mix.setdefault(pt, {})["avg_speed"] = v

    # Merge in spin per pitch type
    if not arsenal_spin_df.empty:
        pitch_type_cols_spin = [
            c for c in arsenal_spin_df.columns
            if c not in ("player_id", "pitcher", "pitcher_name",
                         "last_name, first_name", "first_name", "last_name",
                         "team_name_abbrev", "team_name", "pitch_hand")
        ]
        for _, row in arsenal_spin_df.iterrows():
            pid = _safe_int(row.get("player_id", row.get("pitcher")))
            if pid is None:
                continue
            mix = pitch_mix_by_id.setdefault(pid, {})
            for pt in pitch_type_cols_spin:
                v = _safe_float(row.get(pt))
                if v is not None:
                    mix.setdefault(pt, {})["avg_spin"] = v

    # Derive a single avg spin_rate for each pitcher (across all pitch types)
    avg_spin_by_id: dict[int, float | None] = {}
    for pid, mix in pitch_mix_by_id.items():
        spins = [d["avg_spin"] for d in mix.values() if "avg_spin" in d]
        avg_spin_by_id[pid] = _avg_or_none(spins)

    # ── Assemble rows and insert ─────────────────────────────────────────
    insert_sql = """
        INSERT INTO stg_statcast_pitcher (
            _extracted_at, _batch_id, _season,
            mlbam_id, player_name,
            avg_velocity, max_velocity, spin_rate,
            whiff_pct, chase_pct,
            xera, xwoba_against,
            k_pct, bb_pct,
            pitch_mix_json,
            _raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                player_name = last
            elif first:
                player_name = f"{last}, {first}"
            else:
                player_name = last or None

            avg_velocity = _safe_float(row.get("avg_hit_speed"))
            max_velocity = _safe_float(row.get("max_hit_speed"))

            spin_rate = avg_spin_by_id.get(pid)

            wc = whiff_chase_by_id.get(pid, {})
            whiff_pct = _avg_or_none(wc.get("whiff_vals", []))
            chase_pct = _avg_or_none(wc.get("chase_vals", []))

            xstats = expected_by_id.get(pid, {})
            xwoba_against = xstats.get("xwoba_against")

            fg = fg_by_id.get(pid, {})
            k_pct = fg.get("k_pct")
            bb_pct = fg.get("bb_pct")
            xera = fg.get("xera")

            # Pitch mix JSON
            mix = pitch_mix_by_id.get(pid)
            pitch_mix_json = json.dumps(mix, default=str) if mix else None

            # Build raw JSON from all sources
            raw = row.to_dict()
            if pid in expected_by_id:
                raw["_xstats"] = expected_by_id[pid]
            if pid in fg_by_id:
                raw["_fangraphs"] = fg_by_id[pid]
            if mix:
                raw["_pitch_mix"] = mix
            raw_json = json.dumps(
                {k: (v.item() if hasattr(v, "item") else v) for k, v in raw.items()},
                default=str,
            )

            await db.execute(insert_sql, (
                now, batch_id, ev_year,
                pid, player_name,
                avg_velocity, max_velocity, spin_rate,
                whiff_pct, chase_pct,
                xera, xwoba_against,
                k_pct, bb_pct,
                pitch_mix_json,
                raw_json,
            ))
            rows_inserted += 1

        await db.commit()

    logger.info(
        "stg_statcast_pitcher: inserted %d rows (season=%d, batch=%s)",
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

    logger.info("Starting statcast_pitcher extraction for season %d, batch %s", season, bid)
    count = asyncio.run(extract_statcast_pitcher(season, bid))
    logger.info("Done. %d rows inserted.", count)

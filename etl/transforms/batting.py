"""Transform: stg_pybaseball_batting → core_batting_season.

Resolves each staging row to a core_player.id via the source ID bridge,
then upserts one row per player per season into core_batting_season.
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


async def _resolve_player_id(db, idfg, key_mlbam, name: str) -> int | None:
    """Look up core_player.id via FanGraphs ID, MLBAM ID, or name."""
    # Try FanGraphs ID first
    if idfg:
        cur = await db.execute(
            "SELECT player_id FROM core_player_source_id WHERE source = 'fangraphs' AND source_player_id = ?",
            (str(idfg),),
        )
        row = await cur.fetchone()
        if row:
            return row[0]

    # Try MLBAM ID
    if key_mlbam:
        cur = await db.execute(
            "SELECT player_id FROM core_player_source_id WHERE source = 'mlbam' AND source_player_id = ?",
            (str(key_mlbam),),
        )
        row = await cur.fetchone()
        if row:
            return row[0]

    # Fallback: name match
    if name:
        cur = await db.execute(
            "SELECT id FROM core_player WHERE lower(display_name) = lower(?) LIMIT 1",
            (name,),
        )
        row = await cur.fetchone()
        if row:
            return row[0]

    return None


async def transform_batting(db, batch_id: str) -> int:
    """Transform staging batting rows into core_batting_season.

    Returns count of rows upserted.
    """
    cursor = await db.execute(
        """SELECT name, _season, idfg, key_mlbam,
                  games, pa, ab, hits, hr, rbi, sb,
                  bb_pct, k_pct, avg, obp, slg, ops,
                  woba, wrc_plus, iso, war
           FROM stg_pybaseball_batting
           WHERE _batch_id = ? AND name IS NOT NULL""",
        (batch_id,),
    )
    rows = await cursor.fetchall()
    upserted = 0
    skipped = 0

    for row in rows:
        (name, season, idfg, key_mlbam,
         games, pa, ab, hits, hr, rbi, sb,
         bb_pct, k_pct, avg, obp, slg, ops,
         woba, wrc_plus, iso, war) = row

        player_id = await _resolve_player_id(db, idfg, key_mlbam, name)
        if not player_id:
            skipped += 1
            continue

        await db.execute(
            """INSERT INTO core_batting_season
                   (player_id, season, games, pa, ab, hits, hr, rbi, sb,
                    bb_pct, k_pct, avg, obp, slg, ops, woba, wrc_plus, iso, war, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(player_id, season)
               DO UPDATE SET
                   games=excluded.games, pa=excluded.pa, ab=excluded.ab,
                   hits=excluded.hits, hr=excluded.hr, rbi=excluded.rbi,
                   sb=excluded.sb, bb_pct=excluded.bb_pct, k_pct=excluded.k_pct,
                   avg=excluded.avg, obp=excluded.obp, slg=excluded.slg,
                   ops=excluded.ops, woba=excluded.woba, wrc_plus=excluded.wrc_plus,
                   iso=excluded.iso, war=excluded.war, updated_at=excluded.updated_at""",
            (player_id, season, games, pa, ab, hits, hr, rbi, sb,
             bb_pct, k_pct, avg, obp, slg, ops, woba, wrc_plus, iso, war,
             datetime.utcnow()),
        )
        upserted += 1

    await db.commit()
    if skipped:
        logger.warning(f"Batting transform: {skipped} rows skipped (no player_id match)")
    logger.info(f"Batting transform: {upserted} rows upserted into core_batting_season")
    return upserted

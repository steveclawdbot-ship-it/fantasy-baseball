"""Transform: stg_pybaseball_pitching → core_pitching_season."""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


async def _resolve_player_id(db, idfg, key_mlbam, name: str) -> int | None:
    if idfg:
        cur = await db.execute(
            "SELECT player_id FROM core_player_source_id WHERE source = 'fangraphs' AND source_player_id = ?",
            (str(idfg),),
        )
        row = await cur.fetchone()
        if row:
            return row[0]
    if key_mlbam:
        cur = await db.execute(
            "SELECT player_id FROM core_player_source_id WHERE source = 'mlbam' AND source_player_id = ?",
            (str(key_mlbam),),
        )
        row = await cur.fetchone()
        if row:
            return row[0]
    if name:
        cur = await db.execute(
            "SELECT id FROM core_player WHERE lower(display_name) = lower(?) LIMIT 1",
            (name,),
        )
        row = await cur.fetchone()
        if row:
            return row[0]
    return None


async def transform_pitching(db, batch_id: str) -> int:
    """Transform staging pitching rows into core_pitching_season."""
    cursor = await db.execute(
        """SELECT name, _season, idfg, key_mlbam,
                  games, wins, losses, era, whip, ip,
                  k_per_9, bb_per_9, fip, war
           FROM stg_pybaseball_pitching
           WHERE _batch_id = ? AND name IS NOT NULL""",
        (batch_id,),
    )
    rows = await cursor.fetchall()
    upserted = 0
    skipped = 0

    for row in rows:
        (name, season, idfg, key_mlbam,
         games, wins, losses, era, whip, ip,
         k_per_9, bb_per_9, fip, war) = row

        player_id = await _resolve_player_id(db, idfg, key_mlbam, name)
        if not player_id:
            skipped += 1
            continue

        await db.execute(
            """INSERT INTO core_pitching_season
                   (player_id, season, games, wins, losses, era, whip, ip,
                    k_per_9, bb_per_9, fip, war, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(player_id, season)
               DO UPDATE SET
                   games=excluded.games, wins=excluded.wins, losses=excluded.losses,
                   era=excluded.era, whip=excluded.whip, ip=excluded.ip,
                   k_per_9=excluded.k_per_9, bb_per_9=excluded.bb_per_9,
                   fip=excluded.fip, war=excluded.war, updated_at=excluded.updated_at""",
            (player_id, season, games, wins, losses, era, whip, ip,
             k_per_9, bb_per_9, fip, war, datetime.utcnow()),
        )
        upserted += 1

    await db.commit()
    if skipped:
        logger.warning(f"Pitching transform: {skipped} rows skipped (no player_id match)")
    logger.info(f"Pitching transform: {upserted} rows upserted into core_pitching_season")
    return upserted

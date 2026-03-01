"""Transform: stg_milb_batting, stg_milb_pitching → core_milb_batting_season, core_milb_pitching_season."""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


async def _resolve_player_id(db, mlbam_id, name: str) -> int | None:
    """Look up core_player.id via MLBAM ID or name."""
    if mlbam_id:
        cur = await db.execute(
            "SELECT player_id FROM core_player_source_id WHERE source = 'mlbam' AND source_player_id = ?",
            (str(mlbam_id),),
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


async def transform_milb(db, batch_id: str) -> dict:
    """Transform MiLB staging data into core tables.

    Returns counts of rows upserted per table.
    """
    now = datetime.utcnow()
    counts = {"batting": 0, "pitching": 0}

    # --- MiLB Batting ---
    cursor = await db.execute(
        """SELECT mlbam_id, player_name, _season, level,
                  games, pa, ab, hits, hr, rbi, sb,
                  bb_pct, k_pct, avg, obp, slg, ops, wrc_plus
           FROM stg_milb_batting
           WHERE _batch_id = ? AND player_name IS NOT NULL""",
        (batch_id,),
    )
    for row in await cursor.fetchall():
        (mlbam_id, name, season, level,
         games, pa, ab, hits, hr, rbi, sb,
         bb_pct, k_pct, avg, obp, slg, ops, wrc_plus) = row

        player_id = await _resolve_player_id(db, mlbam_id, name)
        if not player_id:
            continue

        await db.execute(
            """INSERT INTO core_milb_batting_season
                   (player_id, season, level, games, pa, ab, hits, hr, rbi, sb,
                    bb_pct, k_pct, avg, obp, slg, ops, wrc_plus, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(player_id, season, level)
               DO UPDATE SET
                   games=excluded.games, pa=excluded.pa, ab=excluded.ab,
                   hits=excluded.hits, hr=excluded.hr, rbi=excluded.rbi,
                   sb=excluded.sb, bb_pct=excluded.bb_pct, k_pct=excluded.k_pct,
                   avg=excluded.avg, obp=excluded.obp, slg=excluded.slg,
                   ops=excluded.ops, wrc_plus=excluded.wrc_plus,
                   updated_at=excluded.updated_at""",
            (player_id, season, level, games, pa, ab, hits, hr, rbi, sb,
             bb_pct, k_pct, avg, obp, slg, ops, wrc_plus, now),
        )
        counts["batting"] += 1

    # --- MiLB Pitching ---
    cursor = await db.execute(
        """SELECT mlbam_id, player_name, _season, level,
                  games, wins, losses, era, whip, ip,
                  k_per_9, bb_per_9, fip
           FROM stg_milb_pitching
           WHERE _batch_id = ? AND player_name IS NOT NULL""",
        (batch_id,),
    )
    for row in await cursor.fetchall():
        (mlbam_id, name, season, level,
         games, wins, losses, era, whip, ip,
         k_per_9, bb_per_9, fip) = row

        player_id = await _resolve_player_id(db, mlbam_id, name)
        if not player_id:
            continue

        await db.execute(
            """INSERT INTO core_milb_pitching_season
                   (player_id, season, level, games, wins, losses,
                    era, whip, ip, k_per_9, bb_per_9, fip, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(player_id, season, level)
               DO UPDATE SET
                   games=excluded.games, wins=excluded.wins, losses=excluded.losses,
                   era=excluded.era, whip=excluded.whip, ip=excluded.ip,
                   k_per_9=excluded.k_per_9, bb_per_9=excluded.bb_per_9,
                   fip=excluded.fip, updated_at=excluded.updated_at""",
            (player_id, season, level, games, wins, losses,
             era, whip, ip, k_per_9, bb_per_9, fip, now),
        )
        counts["pitching"] += 1

    await db.commit()
    logger.info(f"MiLB transform: {counts}")
    return counts

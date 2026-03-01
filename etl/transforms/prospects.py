"""Transform: stg_prospect_rankings → core_prospect."""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


async def _resolve_player_id(db, mlbam_id, idfg, name: str) -> int | None:
    """Look up core_player.id via MLBAM ID, FanGraphs ID, or name."""
    if mlbam_id:
        cur = await db.execute(
            "SELECT player_id FROM core_player_source_id WHERE source = 'mlbam' AND source_player_id = ?",
            (str(mlbam_id),),
        )
        row = await cur.fetchone()
        if row:
            return row[0]

    if idfg:
        cur = await db.execute(
            "SELECT player_id FROM core_player_source_id WHERE source = 'fangraphs' AND source_player_id = ?",
            (str(idfg),),
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


async def transform_prospects(db, batch_id: str) -> int:
    """Transform staging prospect rankings into core_prospect.

    Returns count of rows upserted.
    """
    now = datetime.utcnow()

    cursor = await db.execute(
        """SELECT player_name, mlbam_id, idfg, org, position,
                  overall_rank, position_rank,
                  hit_fv, power_fv, speed_fv, field_fv, overall_fv,
                  eta, risk_level, ranking_source
           FROM stg_prospect_rankings
           WHERE _batch_id = ? AND player_name IS NOT NULL""",
        (batch_id,),
    )
    rows = await cursor.fetchall()
    upserted = 0
    skipped = 0

    for row in rows:
        (name, mlbam_id, idfg, org, position,
         overall_rank, position_rank,
         hit_fv, power_fv, speed_fv, field_fv, overall_fv,
         eta, risk_level, ranking_source) = row

        player_id = await _resolve_player_id(db, mlbam_id, idfg, name)
        if not player_id:
            skipped += 1
            continue

        ranking_source = ranking_source or "fangraphs"

        await db.execute(
            """INSERT INTO core_prospect
                   (player_id, overall_rank, position_rank,
                    hit_fv, power_fv, speed_fv, field_fv, overall_fv,
                    eta, risk_level, ranking_source, ranking_date, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(player_id, ranking_source)
               DO UPDATE SET
                   overall_rank=excluded.overall_rank, position_rank=excluded.position_rank,
                   hit_fv=excluded.hit_fv, power_fv=excluded.power_fv,
                   speed_fv=excluded.speed_fv, field_fv=excluded.field_fv,
                   overall_fv=excluded.overall_fv,
                   eta=excluded.eta, risk_level=excluded.risk_level,
                   ranking_date=excluded.ranking_date, updated_at=excluded.updated_at""",
            (player_id, overall_rank, position_rank,
             hit_fv, power_fv, speed_fv, field_fv, overall_fv,
             eta, risk_level, ranking_source, now, now),
        )
        upserted += 1

    await db.commit()
    if skipped:
        logger.warning(f"Prospect transform: {skipped} rows skipped (no player_id match)")
    logger.info(f"Prospect transform: {upserted} rows upserted into core_prospect")
    return upserted

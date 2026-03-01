"""Transform: stg_statcast_batter → core_statcast_batter,
             stg_statcast_pitcher → core_statcast_pitcher.
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


async def _resolve_by_mlbam(db, mlbam_id) -> int | None:
    if not mlbam_id:
        return None
    cur = await db.execute(
        "SELECT player_id FROM core_player_source_id WHERE source = 'mlbam' AND source_player_id = ?",
        (str(mlbam_id),),
    )
    row = await cur.fetchone()
    return row[0] if row else None


async def transform_statcast_batter(db, batch_id: str) -> int:
    cursor = await db.execute(
        """SELECT mlbam_id, _season,
                  barrel_pct, hard_hit_pct, avg_exit_velocity, max_exit_velocity,
                  launch_angle, sweet_spot_pct, xslg, xwoba, sprint_speed
           FROM stg_statcast_batter
           WHERE _batch_id = ?""",
        (batch_id,),
    )
    rows = await cursor.fetchall()
    upserted = 0

    for row in rows:
        (mlbam_id, season, barrel_pct, hard_hit_pct, avg_ev, max_ev,
         la, sweet_spot, xslg, xwoba, sprint) = row

        player_id = await _resolve_by_mlbam(db, mlbam_id)
        if not player_id:
            continue

        await db.execute(
            """INSERT INTO core_statcast_batter
                   (player_id, season, barrel_pct, hard_hit_pct,
                    avg_exit_velocity, max_exit_velocity, launch_angle,
                    sweet_spot_pct, xslg, xwoba, sprint_speed, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(player_id, season)
               DO UPDATE SET
                   barrel_pct=excluded.barrel_pct, hard_hit_pct=excluded.hard_hit_pct,
                   avg_exit_velocity=excluded.avg_exit_velocity,
                   max_exit_velocity=excluded.max_exit_velocity,
                   launch_angle=excluded.launch_angle,
                   sweet_spot_pct=excluded.sweet_spot_pct,
                   xslg=excluded.xslg, xwoba=excluded.xwoba,
                   sprint_speed=excluded.sprint_speed, updated_at=excluded.updated_at""",
            (player_id, season, barrel_pct, hard_hit_pct, avg_ev, max_ev,
             la, sweet_spot, xslg, xwoba, sprint, datetime.utcnow()),
        )
        upserted += 1

    await db.commit()
    logger.info(f"Statcast batter transform: {upserted} rows upserted")
    return upserted


async def transform_statcast_pitcher(db, batch_id: str) -> int:
    cursor = await db.execute(
        """SELECT mlbam_id, _season,
                  avg_velocity, max_velocity, spin_rate,
                  whiff_pct, chase_pct, xera, xwoba_against,
                  k_pct, bb_pct, pitch_mix_json
           FROM stg_statcast_pitcher
           WHERE _batch_id = ?""",
        (batch_id,),
    )
    rows = await cursor.fetchall()
    upserted = 0

    for row in rows:
        (mlbam_id, season, avg_vel, max_vel, spin,
         whiff, chase, xera, xwoba_ag, k_pct, bb_pct, pitch_mix) = row

        player_id = await _resolve_by_mlbam(db, mlbam_id)
        if not player_id:
            continue

        await db.execute(
            """INSERT INTO core_statcast_pitcher
                   (player_id, season, avg_velocity, max_velocity, spin_rate,
                    whiff_pct, chase_pct, xera, xwoba_against,
                    k_pct, bb_pct, pitch_mix_json, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(player_id, season)
               DO UPDATE SET
                   avg_velocity=excluded.avg_velocity, max_velocity=excluded.max_velocity,
                   spin_rate=excluded.spin_rate, whiff_pct=excluded.whiff_pct,
                   chase_pct=excluded.chase_pct, xera=excluded.xera,
                   xwoba_against=excluded.xwoba_against,
                   k_pct=excluded.k_pct, bb_pct=excluded.bb_pct,
                   pitch_mix_json=excluded.pitch_mix_json, updated_at=excluded.updated_at""",
            (player_id, season, avg_vel, max_vel, spin,
             whiff, chase, xera, xwoba_ag, k_pct, bb_pct, pitch_mix,
             datetime.utcnow()),
        )
        upserted += 1

    await db.commit()
    logger.info(f"Statcast pitcher transform: {upserted} rows upserted")
    return upserted

"""Transform: stg_fantrax_* → core_fantrax_league, core_fantrax_team, core_fantrax_roster."""

import logging
from datetime import datetime
from etl.config import ESPN_LEAGUE_YEAR as CURRENT_SEASON

logger = logging.getLogger(__name__)


async def transform_fantrax(db, batch_id: str) -> dict:
    """Transform Fantrax staging data into core tables."""
    now = datetime.utcnow()
    season = CURRENT_SEASON
    counts = {"leagues": 0, "teams": 0, "roster": 0}

    # --- Leagues ---
    cursor = await db.execute(
        """SELECT DISTINCT league_id, name, num_teams, scoring_type
           FROM stg_fantrax_leagues WHERE _batch_id = ?""",
        (batch_id,),
    )
    for row in await cursor.fetchall():
        league_id, name, num_teams, scoring_type = row
        await db.execute(
            """INSERT INTO core_fantrax_league
                   (league_id, season, name, num_teams, scoring_type, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(league_id, season)
               DO UPDATE SET name=excluded.name, num_teams=excluded.num_teams,
                   scoring_type=excluded.scoring_type, updated_at=excluded.updated_at""",
            (league_id, season, name, num_teams, scoring_type, now),
        )
        counts["leagues"] += 1

    # --- Teams ---
    cursor = await db.execute(
        """SELECT DISTINCT league_id, fantrax_team_id, team_name, owner,
                  wins, losses, standing
           FROM stg_fantrax_teams WHERE _batch_id = ?""",
        (batch_id,),
    )
    for row in await cursor.fetchall():
        league_id, ftx_team_id, team_name, owner, wins, losses, standing = row
        await db.execute(
            """INSERT INTO core_fantrax_team
                   (league_id, season, fantrax_team_id, team_name, owner,
                    wins, losses, standing, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(league_id, season, fantrax_team_id)
               DO UPDATE SET team_name=excluded.team_name, owner=excluded.owner,
                   wins=excluded.wins, losses=excluded.losses,
                   standing=excluded.standing, updated_at=excluded.updated_at""",
            (league_id, season, ftx_team_id, team_name, owner,
             wins, losses, standing, now),
        )
        counts["teams"] += 1

    # --- Roster ---
    cursor = await db.execute(
        """SELECT player_id, league_id, fantrax_team_id, fantrax_team_name,
                  player_name, roster_slot
           FROM stg_fantrax_rosters
           WHERE _batch_id = ? AND player_name IS NOT NULL""",
        (batch_id,),
    )
    for row in await cursor.fetchall():
        ftx_player_id, league_id, ftx_team_id, ftx_team_name, name, slot = row

        # Resolve to core_player
        if ftx_player_id:
            cur = await db.execute(
                "SELECT player_id FROM core_player_source_id WHERE source = 'fantrax' AND source_player_id = ?",
                (str(ftx_player_id),),
            )
            link = await cur.fetchone()
        else:
            link = None

        if not link:
            # Fallback name match
            cur = await db.execute(
                "SELECT id FROM core_player WHERE lower(display_name) = lower(?) LIMIT 1",
                (name,),
            )
            link = await cur.fetchone()

        if not link:
            continue

        player_id = link[0]
        await db.execute(
            """INSERT INTO core_fantrax_roster
                   (player_id, league_id, season, fantrax_team_id,
                    fantrax_team_name, roster_slot, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(player_id, league_id, season)
               DO UPDATE SET fantrax_team_id=excluded.fantrax_team_id,
                   fantrax_team_name=excluded.fantrax_team_name,
                   roster_slot=excluded.roster_slot, updated_at=excluded.updated_at""",
            (player_id, league_id, season, ftx_team_id, ftx_team_name, slot, now),
        )
        counts["roster"] += 1

    await db.commit()
    logger.info(f"Fantrax transform: {counts}")
    return counts

"""Transform: stg_espn_* → core_espn_league, core_espn_team, core_espn_roster."""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


async def transform_espn(db, batch_id: str) -> dict:
    """Transform ESPN staging data into core tables.

    Returns counts of rows upserted per table.
    """
    now = datetime.utcnow()
    counts = {"leagues": 0, "teams": 0, "roster": 0}

    # --- Leagues ---
    cursor = await db.execute(
        """SELECT DISTINCT league_id, year, name, num_teams, scoring_type
           FROM stg_espn_leagues WHERE _batch_id = ?""",
        (batch_id,),
    )
    for row in await cursor.fetchall():
        league_id, year, name, num_teams, scoring_type = row
        await db.execute(
            """INSERT INTO core_espn_league
                   (league_id, season, name, num_teams, scoring_type, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(league_id, season)
               DO UPDATE SET name=excluded.name, num_teams=excluded.num_teams,
                   scoring_type=excluded.scoring_type, updated_at=excluded.updated_at""",
            (league_id, year, name, num_teams, scoring_type, now),
        )
        counts["leagues"] += 1

    # --- Teams ---
    cursor = await db.execute(
        """SELECT DISTINCT league_id, year, espn_team_id, team_name, owner,
                  wins, losses, ties, standing
           FROM stg_espn_teams WHERE _batch_id = ?""",
        (batch_id,),
    )
    for row in await cursor.fetchall():
        (league_id, year, espn_team_id, team_name, owner,
         wins, losses, ties, standing) = row
        await db.execute(
            """INSERT INTO core_espn_team
                   (league_id, season, espn_team_id, team_name, owner,
                    wins, losses, standing, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(league_id, season, espn_team_id)
               DO UPDATE SET team_name=excluded.team_name, owner=excluded.owner,
                   wins=excluded.wins, losses=excluded.losses,
                   standing=excluded.standing, updated_at=excluded.updated_at""",
            (league_id, year, espn_team_id, team_name, owner,
             wins, losses, standing, now),
        )
        counts["teams"] += 1

    # --- Roster (players → core_espn_roster) ---
    cursor = await db.execute(
        """SELECT espn_id, league_id, year, roster_team_id, roster_team_name,
                  ownership_pct, adp
           FROM stg_espn_players WHERE _batch_id = ? AND espn_id IS NOT NULL""",
        (batch_id,),
    )
    for row in await cursor.fetchall():
        espn_id, league_id, year, team_id, team_name, ownership, adp = row

        # Resolve to core_player via ESPN source ID
        cur = await db.execute(
            "SELECT player_id FROM core_player_source_id WHERE source = 'espn' AND source_player_id = ?",
            (str(espn_id),),
        )
        link = await cur.fetchone()
        if not link:
            continue

        player_id = link[0]
        await db.execute(
            """INSERT INTO core_espn_roster
                   (player_id, league_id, season, espn_team_id, espn_team_name,
                    ownership_pct, adp, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(player_id, league_id, season)
               DO UPDATE SET espn_team_id=excluded.espn_team_id,
                   espn_team_name=excluded.espn_team_name,
                   ownership_pct=excluded.ownership_pct, adp=excluded.adp,
                   updated_at=excluded.updated_at""",
            (player_id, league_id, year, team_id, team_name, ownership, adp, now),
        )
        counts["roster"] += 1

    await db.commit()
    logger.info(f"ESPN transform: {counts}")
    return counts

"""Extract Fantrax league data into stg_fantrax_leagues, stg_fantrax_teams, stg_fantrax_rosters."""

import asyncio
import json
import logging
import sys
from datetime import datetime

from etl import config
from etl.config import new_batch_id
from etl.db import get_connection

# Import the Fantrax service from the backend
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[2] / "backend"))
from app.services.fantrax_service import FantraxService

logger = logging.getLogger(__name__)


async def extract_fantrax(batch_id: str) -> dict:
    """Pull Fantrax league data and insert into staging tables.

    Iterates over config.FANTRAX_LEAGUES.  For each league it fetches league
    info, rosters, and standings, then writes to the corresponding
    stg_fantrax_* tables.

    Returns a dict of counts: {"leagues": n, "teams": n, "rosters": n}
    """
    if not config.FANTRAX_LEAGUES:
        logger.info("FANTRAX_LEAGUES is empty -- skipping Fantrax extraction")
        return {"leagues": 0, "teams": 0, "rosters": 0}

    if not config.FANTRAX_SESSION:
        logger.warning(
            "FANTRAX_SESSION is not set -- skipping Fantrax extraction. "
            "Set the env var to a valid session cookie from a logged-in browser."
        )
        return {"leagues": 0, "teams": 0, "rosters": 0}

    now = datetime.utcnow().isoformat()
    totals = {"leagues": 0, "teams": 0, "rosters": 0}

    async with get_connection() as db:
        for entry in config.FANTRAX_LEAGUES:
            league_id = entry["league_id"]
            league_type = entry.get("league_type", "")
            logger.info(
                "Extracting Fantrax league %s (type=%s)", league_id, league_type,
            )

            try:
                svc = FantraxService(league_id, config.FANTRAX_SESSION)

                # -- league info -----------------------------------------------
                league_info = svc.get_league()
                if league_info is None:
                    logger.warning(
                        "Could not fetch league info for %s -- skipping", league_id,
                    )
                    continue

                league_name = league_info.get("name") or league_info.get("leagueName")
                num_teams = league_info.get("numTeams") or league_info.get("numberOfTeams")
                scoring_type = league_info.get("scoringType") or league_info.get("scoring")

                await db.execute(
                    """
                    INSERT INTO stg_fantrax_leagues (
                        _extracted_at, _batch_id,
                        league_id, name, num_teams, scoring_type,
                        _raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        now,
                        batch_id,
                        league_id,
                        league_name,
                        num_teams,
                        scoring_type,
                        json.dumps(league_info, default=str),
                    ),
                )
                totals["leagues"] += 1

                # -- standings / teams -----------------------------------------
                standings = svc.get_standings()
                if standings:
                    teams_data = (
                        standings.get("teams")
                        or standings.get("standings")
                        or standings.get("teamStandings")
                        or []
                    )
                    for idx, t in enumerate(teams_data):
                        ftx_team_id = (
                            t.get("teamId")
                            or t.get("fantasyTeamId")
                            or str(idx)
                        )
                        await db.execute(
                            """
                            INSERT INTO stg_fantrax_teams (
                                _extracted_at, _batch_id,
                                league_id, fantrax_team_id,
                                team_name, owner,
                                wins, losses, standing,
                                _raw_json
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                now,
                                batch_id,
                                league_id,
                                ftx_team_id,
                                t.get("name") or t.get("teamName"),
                                t.get("owner") or t.get("ownerName"),
                                t.get("wins") or t.get("w"),
                                t.get("losses") or t.get("l"),
                                t.get("standing") or t.get("rank") or (idx + 1),
                                json.dumps(t, default=str),
                            ),
                        )
                        totals["teams"] += 1

                # -- rosters ---------------------------------------------------
                rosters_resp = svc.get_rosters()
                if rosters_resp:
                    # The response shape varies; try common keys
                    teams_rosters = (
                        rosters_resp.get("rosters")
                        or rosters_resp.get("teamRosters")
                        or rosters_resp.get("teams")
                        or {}
                    )

                    # teams_rosters could be a list or a dict keyed by teamId
                    roster_items: list = []
                    if isinstance(teams_rosters, dict):
                        for team_id, team_data in teams_rosters.items():
                            players = (
                                team_data.get("players")
                                or team_data.get("roster")
                                or []
                            )
                            team_name = (
                                team_data.get("name")
                                or team_data.get("teamName")
                                or ""
                            )
                            for p in players:
                                roster_items.append((team_id, team_name, p))
                    elif isinstance(teams_rosters, list):
                        for team_data in teams_rosters:
                            team_id = (
                                team_data.get("teamId")
                                or team_data.get("fantasyTeamId")
                                or ""
                            )
                            team_name = (
                                team_data.get("name")
                                or team_data.get("teamName")
                                or ""
                            )
                            players = (
                                team_data.get("players")
                                or team_data.get("roster")
                                or []
                            )
                            for p in players:
                                roster_items.append((team_id, team_name, p))

                    for team_id, team_name, p in roster_items:
                        # Extract roster slot info (active/minors/taxi/IL)
                        roster_slot = (
                            p.get("rosterSlot")
                            or p.get("slot")
                            or p.get("status")
                            or p.get("rosterStatus")
                        )

                        await db.execute(
                            """
                            INSERT INTO stg_fantrax_rosters (
                                _extracted_at, _batch_id,
                                league_id, fantrax_team_id, fantrax_team_name,
                                player_id, player_name,
                                position, pro_team, roster_slot,
                                _raw_json
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                now,
                                batch_id,
                                league_id,
                                team_id,
                                team_name,
                                p.get("playerId") or p.get("id"),
                                p.get("name") or p.get("playerName"),
                                p.get("position") or p.get("pos"),
                                p.get("proTeam") or p.get("team"),
                                roster_slot,
                                json.dumps(p, default=str),
                            ),
                        )
                        totals["rosters"] += 1

                logger.info(
                    "Fantrax league %s: %d teams, %d roster entries extracted",
                    league_id,
                    totals["teams"],
                    totals["rosters"],
                )

            except Exception:
                logger.exception(
                    "Error extracting Fantrax league %s -- skipping", league_id,
                )
                continue

        await db.commit()

    logger.info(
        "Fantrax extraction complete (batch=%s): %d leagues, %d teams, %d roster entries",
        batch_id, totals["leagues"], totals["teams"], totals["rosters"],
    )
    return totals


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    _batch_id = new_batch_id()
    result = asyncio.run(extract_fantrax(_batch_id))
    print(f"Done. {result}")

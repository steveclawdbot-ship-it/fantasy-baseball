"""Extract ESPN league data into stg_espn_leagues, stg_espn_teams, stg_espn_players."""

import asyncio
import json
import logging
import sys
from datetime import datetime

from etl import config
from etl.config import new_batch_id
from etl.db import get_connection

# Import the existing ESPN service from the backend
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[2] / "backend"))
from app.services.espn_service import ESPNService

logger = logging.getLogger(__name__)


async def extract_espn(season: int, batch_id: str) -> dict:
    """Pull ESPN league data and insert into staging tables.

    Iterates over config.ESPN_LEAGUES.  For each league it fetches settings,
    teams, rostered players and free agents, then writes everything to the
    corresponding stg_espn_* tables.

    Returns a dict of counts: {"leagues": n, "teams": n, "players": n}
    """
    if not config.ESPN_LEAGUES:
        logger.info("ESPN_LEAGUES is empty -- skipping ESPN extraction")
        return {"leagues": 0, "teams": 0, "players": 0}

    now = datetime.utcnow().isoformat()
    totals = {"leagues": 0, "teams": 0, "players": 0}

    async with get_connection() as db:
        for entry in config.ESPN_LEAGUES:
            league_id = entry["league_id"]
            league_type = entry.get("league_type", "")
            logger.info(
                "Extracting ESPN league %s (type=%s, season=%d)",
                league_id, league_type, season,
            )

            try:
                svc = ESPNService(
                    league_id=league_id,
                    year=season,
                    espn_s2=config.ESPN_S2 or None,
                    swid=config.ESPN_SWID or None,
                )

                # -- league settings -------------------------------------------
                settings = svc.get_league_settings()
                await db.execute(
                    """
                    INSERT INTO stg_espn_leagues (
                        _extracted_at, _batch_id,
                        league_id, year, name, num_teams,
                        scoring_type, roster_size, _raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        now,
                        batch_id,
                        league_id,
                        season,
                        settings.get("name"),
                        settings.get("num_teams"),
                        settings.get("scoring_type"),
                        settings.get("roster_size"),
                        json.dumps(settings, default=str),
                    ),
                )
                totals["leagues"] += 1

                # -- teams -----------------------------------------------------
                teams = svc.get_teams()
                for t in teams:
                    await db.execute(
                        """
                        INSERT INTO stg_espn_teams (
                            _extracted_at, _batch_id,
                            league_id, year, espn_team_id,
                            team_name, owner,
                            wins, losses, ties, standing,
                            _raw_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            now,
                            batch_id,
                            league_id,
                            season,
                            t.get("espn_team_id"),
                            t.get("name"),
                            t.get("owner"),
                            t.get("wins"),
                            t.get("losses"),
                            t.get("ties"),
                            t.get("standing"),
                            json.dumps(t, default=str),
                        ),
                    )
                    totals["teams"] += 1

                # -- players (rostered + free agents) --------------------------
                rostered = svc.get_players()
                free_agents = svc.get_free_agents()

                # Build a combined list; rostered players have roster_team info
                all_players: list[dict] = []

                # Enrich rostered players with stats
                for p in rostered:
                    player_stats = svc.get_player_stats(p["espn_id"])
                    p["stats"] = player_stats.get("stats", {}) if player_stats else {}
                    p["projected_stats"] = (
                        player_stats.get("projected_stats", {}) if player_stats else {}
                    )
                    all_players.append(p)

                for fa in free_agents:
                    fa["stats"] = {}
                    fa["projected_stats"] = {}
                    all_players.append(fa)

                for p in all_players:
                    await db.execute(
                        """
                        INSERT INTO stg_espn_players (
                            _extracted_at, _batch_id,
                            league_id, year, espn_id,
                            name, position, pro_team, age,
                            ownership_pct, adp,
                            roster_team_id, roster_team_name,
                            stats_json, projected_stats_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            now,
                            batch_id,
                            league_id,
                            season,
                            p.get("espn_id"),
                            p.get("name"),
                            p.get("position"),
                            p.get("team"),
                            p.get("age"),
                            p.get("ownership"),
                            p.get("adp"),
                            p.get("roster_team_id"),
                            p.get("roster_team_name"),
                            json.dumps(p.get("stats", {}), default=str),
                            json.dumps(p.get("projected_stats", {}), default=str),
                        ),
                    )
                    totals["players"] += 1

                logger.info(
                    "ESPN league %s: %d teams, %d players extracted",
                    league_id, len(teams), len(all_players),
                )

            except Exception:
                logger.exception("Error extracting ESPN league %s -- skipping", league_id)
                continue

        await db.commit()

    logger.info(
        "ESPN extraction complete (batch=%s): %d leagues, %d teams, %d players",
        batch_id, totals["leagues"], totals["teams"], totals["players"],
    )
    return totals


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    _season = config.ESPN_LEAGUE_YEAR
    _batch_id = new_batch_id()
    result = asyncio.run(extract_espn(_season, _batch_id))
    print(f"Done. {result}")

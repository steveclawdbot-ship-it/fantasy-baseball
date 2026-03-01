"""Service for interacting with the Fantrax Fantasy Baseball API.

Fantrax does not have an official public API.  This service wraps the private
``/fxea`` endpoints that the Fantrax web app uses internally.  Authentication
is handled via a session cookie extracted from a logged-in browser session.

Features:
- Rate limiting (1 req/sec, matching espn_service.py pattern)
- Graceful error handling -- methods return None on failure
- Factory method ``from_env`` for environment-based config
"""

import logging
import os
import time
from functools import wraps
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://www.fantrax.com/fxea"


# ---------------------------------------------------------------------------
# Rate-limit decorator (same pattern as espn_service.py)
# ---------------------------------------------------------------------------
def _rate_limit(seconds: float = 1.0):
    """Decorator to enforce a minimum gap between consecutive calls."""
    def decorator(func):
        last_call = [0.0]

        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_call[0]
            if elapsed < seconds:
                time.sleep(seconds - elapsed)
            try:
                result = func(*args, **kwargs)
                last_call[0] = time.time()
                return result
            except Exception as exc:
                last_call[0] = time.time()
                raise exc
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------
class FantraxService:
    """Synchronous REST client for the Fantrax private API.

    Each instance is scoped to a single league.

    Parameters
    ----------
    league_id : str
        The Fantrax league identifier (visible in the URL).
    session_cookie : str
        Value of the ``session`` cookie from a logged-in Fantrax browser
        session.
    """

    def __init__(self, league_id: str, session_cookie: str):
        self.league_id = league_id
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Origin": "https://www.fantrax.com",
            "Referer": "https://www.fantrax.com/",
        })
        if session_cookie:
            self._session.cookies.set("session", session_cookie)

    # -- factory -----------------------------------------------------------

    @classmethod
    def from_env(cls, league_id: str) -> "FantraxService":
        """Create a ``FantraxService`` from the ``FANTRAX_SESSION`` env var.

        Raises ``EnvironmentError`` if the variable is not set.
        """
        session_cookie = os.getenv("FANTRAX_SESSION", "")
        if not session_cookie:
            raise EnvironmentError(
                "FANTRAX_SESSION environment variable is not set. "
                "Extract the session cookie from a logged-in Fantrax browser session."
            )
        return cls(league_id, session_cookie)

    # -- low-level ---------------------------------------------------------

    @_rate_limit(seconds=1.0)
    def _post(self, endpoint: str, data: Dict) -> Optional[Dict]:
        """POST to a Fantrax endpoint and return the parsed JSON, or None."""
        url = f"{BASE_URL}{endpoint}"
        try:
            response = self._session.post(url, json=data, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            logger.error(
                "Fantrax HTTP %s for %s (league %s): %s",
                status, endpoint, self.league_id, exc,
            )
            return None
        except requests.exceptions.RequestException as exc:
            logger.error(
                "Fantrax request error for %s (league %s): %s",
                endpoint, self.league_id, exc,
            )
            return None

    # -- public API methods ------------------------------------------------

    def get_league(self) -> Optional[Dict]:
        """Fetch league info / settings."""
        data = {
            "leagueId": self.league_id,
            "method": "getLeagueInfo",
        }
        result = self._post("/general", data)
        if result:
            logger.info("Retrieved Fantrax league info for %s", self.league_id)
        return result

    def get_rosters(self) -> Optional[Dict]:
        """Fetch all team rosters (includes roster-slot data when available)."""
        data = {
            "leagueId": self.league_id,
            "method": "getTeamRosters",
        }
        result = self._post("/general", data)
        if result:
            logger.info("Retrieved Fantrax rosters for league %s", self.league_id)
        return result

    def get_players(self, page: int = 1) -> Optional[Dict]:
        """Fetch a page of available players."""
        data = {
            "leagueId": self.league_id,
            "method": "getPlayers",
            "params": {
                "page": page,
                "perPage": 50,
            },
        }
        result = self._post("/general", data)
        if result:
            logger.info(
                "Retrieved Fantrax players page %d for league %s",
                page, self.league_id,
            )
        return result

    def get_standings(self) -> Optional[Dict]:
        """Fetch league standings."""
        data = {
            "leagueId": self.league_id,
            "method": "getStandings",
        }
        result = self._post("/general", data)
        if result:
            logger.info("Retrieved Fantrax standings for league %s", self.league_id)
        return result


# ---------------------------------------------------------------------------
# Convenience factory (mirrors espn_service.create_espn_service)
# ---------------------------------------------------------------------------
def create_fantrax_service(
    league_id: str,
    session_cookie: Optional[str] = None,
) -> FantraxService:
    """Create a FantraxService, falling back to FANTRAX_SESSION env var."""
    cookie = session_cookie or os.getenv("FANTRAX_SESSION", "")
    if not cookie:
        raise EnvironmentError(
            "No session cookie provided and FANTRAX_SESSION env var is not set."
        )
    return FantraxService(league_id, cookie)


if __name__ == "__main__":
    import json

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    league = os.getenv("FANTRAX_TEST_LEAGUE", "")
    if not league:
        print("Set FANTRAX_TEST_LEAGUE and FANTRAX_SESSION to run a quick smoke test.")
        raise SystemExit(1)

    svc = FantraxService.from_env(league)
    info = svc.get_league()
    if info:
        print(json.dumps(info, indent=2)[:500])
    else:
        print("Failed to retrieve league info.")

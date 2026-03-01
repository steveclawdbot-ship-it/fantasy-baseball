"""ETL configuration.

All config is driven by environment variables with sensible defaults.
"""

import os
import uuid
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DEFAULT_DB_PATH = "/home/jesse/clawd-steve/data/fantasy_baseball.db"
DB_PATH: str = os.getenv("FANTASY_DB_PATH", DEFAULT_DB_PATH)

# Ensure parent directory exists when running on a fresh machine
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# ESPN leagues
# ---------------------------------------------------------------------------
_espn_ids = [s.strip() for s in os.getenv("ESPN_LEAGUE_IDS", "").split(",") if s.strip()]
_espn_types = [s.strip() for s in os.getenv("ESPN_LEAGUE_TYPES", "").split(",") if s.strip()]

ESPN_LEAGUES: list[dict] = [
    {"league_id": int(lid), "league_type": lt}
    for lid, lt in zip(_espn_ids, _espn_types)
]

ESPN_S2: str = os.getenv("ESPN_S2", "")
ESPN_SWID: str = os.getenv("ESPN_SWID", "")

# ---------------------------------------------------------------------------
# Fantrax leagues
# ---------------------------------------------------------------------------
_ftx_ids = [s.strip() for s in os.getenv("FANTRAX_LEAGUE_IDS", "").split(",") if s.strip()]
_ftx_types = [s.strip() for s in os.getenv("FANTRAX_LEAGUE_TYPES", "").split(",") if s.strip()]

FANTRAX_LEAGUES: list[dict] = [
    {"league_id": lid, "league_type": lt}
    for lid, lt in zip(_ftx_ids, _ftx_types)
]

FANTRAX_SESSION: str = os.getenv("FANTRAX_SESSION", "")

# ---------------------------------------------------------------------------
# ESPN credentials
# ---------------------------------------------------------------------------
ESPN_LEAGUE_YEAR: int = int(os.getenv("ESPN_LEAGUE_YEAR", str(datetime.now().year)))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def new_batch_id() -> str:
    """Generate a unique batch identifier for an ETL run."""
    return f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"

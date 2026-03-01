"""Player identity resolution across data sources.

Maps source-specific IDs (FanGraphs IDfg, MLBAM, ESPN, Fantrax) to a
stable internal core_player.id via the core_player_source_id bridge table.

Resolution order:
  1. pybaseball (provides IDfg + key_mlbam — richest ID set)
  2. MiLB / Prospects (extends with minor leaguers via MLBAM)
  3. Statcast (match on MLBAM)
  4. ESPN (match on name + team)
  5. Fantrax (match on name + team)
"""

import logging
import unicodedata
import re
from datetime import datetime

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """Normalize a player name for fuzzy matching.

    Strips accents, lowercases, removes suffixes (Jr., Sr., III, II),
    and collapses whitespace.
    """
    if not name:
        return ""
    # Decompose unicode and strip combining marks (accents)
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Lowercase
    result = ascii_only.lower().strip()
    # Remove common suffixes
    result = re.sub(r"\s+(jr\.?|sr\.?|iii|ii|iv)\s*$", "", result)
    # Collapse whitespace
    result = re.sub(r"\s+", " ", result)
    return result


async def _get_or_create_player(
    db,
    display_name: str,
    position: str | None = None,
    mlb_team: str | None = None,
    player_level: str = "MLB",
) -> int:
    """Get existing core_player by name+team or create a new one. Returns player_id."""
    norm = normalize_name(display_name)

    # Try exact name match first (within same team if provided)
    if mlb_team:
        cursor = await db.execute(
            """SELECT id FROM core_player
               WHERE lower(display_name) = ? AND mlb_team = ?
               LIMIT 1""",
            (norm, mlb_team),
        )
    else:
        cursor = await db.execute(
            "SELECT id FROM core_player WHERE lower(display_name) = ? LIMIT 1",
            (norm,),
        )
    row = await cursor.fetchone()
    if row:
        return row[0]

    # Also check normalized against existing names
    cursor = await db.execute("SELECT id, display_name FROM core_player")
    all_players = await cursor.fetchall()
    for pid, existing_name in all_players:
        if normalize_name(existing_name) == norm:
            return pid

    # Create new player
    cursor = await db.execute(
        """INSERT INTO core_player (display_name, position, mlb_team, player_level, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (display_name, position, mlb_team, player_level, datetime.utcnow(), datetime.utcnow()),
    )
    return cursor.lastrowid


async def _link_source_id(
    db,
    player_id: int,
    source: str,
    source_player_id: str,
    confidence: float = 1.0,
    matched_by: str = "id_join",
) -> None:
    """Insert or update a core_player_source_id mapping."""
    await db.execute(
        """INSERT INTO core_player_source_id
               (player_id, source, source_player_id, confidence, matched_by, created_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(source, source_player_id)
           DO UPDATE SET player_id = excluded.player_id,
                         confidence = excluded.confidence,
                         matched_by = excluded.matched_by""",
        (player_id, source, str(source_player_id), confidence, matched_by, datetime.utcnow()),
    )


async def resolve_pybaseball_batting(db, batch_id: str) -> int:
    """Resolve identities from stg_pybaseball_batting rows for a batch.

    pybaseball provides IDfg (FanGraphs) and key_mlbam — richest ID set.
    Creates core_player entries and maps both source IDs.
    Returns count of players resolved.
    """
    cursor = await db.execute(
        """SELECT DISTINCT name, team, idfg, key_mlbam
           FROM stg_pybaseball_batting
           WHERE _batch_id = ? AND name IS NOT NULL""",
        (batch_id,),
    )
    rows = await cursor.fetchall()
    count = 0

    for name, team, idfg, mlbam in rows:
        player_id = await _get_or_create_player(db, name, mlb_team=team, player_level="MLB")

        if idfg:
            await _link_source_id(db, player_id, "fangraphs", str(idfg), 1.0, "id_join")
        if mlbam:
            await _link_source_id(db, player_id, "mlbam", str(mlbam), 1.0, "id_join")
        count += 1

    await db.commit()
    logger.info(f"Resolved {count} players from pybaseball batting (batch {batch_id})")
    return count


async def resolve_pybaseball_pitching(db, batch_id: str) -> int:
    """Resolve identities from stg_pybaseball_pitching."""
    cursor = await db.execute(
        """SELECT DISTINCT name, team, idfg, key_mlbam
           FROM stg_pybaseball_pitching
           WHERE _batch_id = ? AND name IS NOT NULL""",
        (batch_id,),
    )
    rows = await cursor.fetchall()
    count = 0

    for name, team, idfg, mlbam in rows:
        player_id = await _get_or_create_player(db, name, mlb_team=team, player_level="MLB")

        if idfg:
            await _link_source_id(db, player_id, "fangraphs", str(idfg), 1.0, "id_join")
        if mlbam:
            await _link_source_id(db, player_id, "mlbam", str(mlbam), 1.0, "id_join")
        count += 1

    await db.commit()
    logger.info(f"Resolved {count} players from pybaseball pitching (batch {batch_id})")
    return count


async def resolve_statcast(db, batch_id: str, table: str = "stg_statcast_batter") -> int:
    """Resolve identities from statcast staging table by matching on MLBAM ID."""
    cursor = await db.execute(
        f"""SELECT DISTINCT mlbam_id, player_name
            FROM {table}
            WHERE _batch_id = ? AND mlbam_id IS NOT NULL""",
        (batch_id,),
    )
    rows = await cursor.fetchall()
    count = 0

    for mlbam_id, player_name in rows:
        # Check if MLBAM ID is already linked
        cur = await db.execute(
            "SELECT player_id FROM core_player_source_id WHERE source = 'mlbam' AND source_player_id = ?",
            (str(mlbam_id),),
        )
        existing = await cur.fetchone()

        if existing:
            count += 1
            continue

        # No MLBAM link yet — create player or match by name
        player_id = await _get_or_create_player(db, player_name or f"MLBAM-{mlbam_id}")
        await _link_source_id(db, player_id, "mlbam", str(mlbam_id), 1.0, "id_join")
        count += 1

    await db.commit()
    logger.info(f"Resolved {count} players from {table} (batch {batch_id})")
    return count


async def resolve_espn(db, batch_id: str) -> int:
    """Resolve ESPN players by matching on normalized name + team."""
    cursor = await db.execute(
        """SELECT DISTINCT espn_id, name, pro_team
           FROM stg_espn_players
           WHERE _batch_id = ? AND name IS NOT NULL""",
        (batch_id,),
    )
    rows = await cursor.fetchall()
    count = 0

    for espn_id, name, pro_team in rows:
        # Check if ESPN ID already linked
        cur = await db.execute(
            "SELECT player_id FROM core_player_source_id WHERE source = 'espn' AND source_player_id = ?",
            (str(espn_id),),
        )
        existing = await cur.fetchone()
        if existing:
            count += 1
            continue

        # Try to match existing core_player by name + team
        player_id = await _get_or_create_player(db, name, mlb_team=pro_team)
        await _link_source_id(db, player_id, "espn", str(espn_id), 0.9, "name_team")
        count += 1

    await db.commit()
    logger.info(f"Resolved {count} ESPN players (batch {batch_id})")
    return count


async def resolve_fantrax(db, batch_id: str) -> int:
    """Resolve Fantrax players by matching on normalized name + team."""
    cursor = await db.execute(
        """SELECT DISTINCT player_id, player_name, pro_team
           FROM stg_fantrax_rosters
           WHERE _batch_id = ? AND player_name IS NOT NULL""",
        (batch_id,),
    )
    rows = await cursor.fetchall()
    count = 0

    for ftx_player_id, name, pro_team in rows:
        if not ftx_player_id:
            continue

        # Check if Fantrax ID already linked
        cur = await db.execute(
            "SELECT player_id FROM core_player_source_id WHERE source = 'fantrax' AND source_player_id = ?",
            (str(ftx_player_id),),
        )
        existing = await cur.fetchone()
        if existing:
            count += 1
            continue

        core_id = await _get_or_create_player(db, name, mlb_team=pro_team)
        await _link_source_id(db, core_id, "fantrax", str(ftx_player_id), 0.9, "name_team")
        count += 1

    await db.commit()
    logger.info(f"Resolved {count} Fantrax players (batch {batch_id})")
    return count


async def resolve_milb(db, batch_id: str) -> int:
    """Resolve MiLB players via MLBAM ID, setting player_level from their level."""
    cursor = await db.execute(
        """SELECT DISTINCT mlbam_id, player_name, level, team
           FROM stg_milb_batting
           WHERE _batch_id = ? AND player_name IS NOT NULL""",
        (batch_id,),
    )
    rows = await cursor.fetchall()
    count = 0

    for mlbam_id, name, level, team in rows:
        player_level = level or "MiLB"

        # Check if MLBAM ID already linked
        if mlbam_id:
            cur = await db.execute(
                "SELECT player_id FROM core_player_source_id WHERE source = 'mlbam' AND source_player_id = ?",
                (str(mlbam_id),),
            )
            existing = await cur.fetchone()
            if existing:
                # Update player_level if it was MLB and now they're in minors
                await db.execute(
                    "UPDATE core_player SET player_level = ?, updated_at = ? WHERE id = ? AND player_level != 'MLB'",
                    (player_level, datetime.utcnow(), existing[0]),
                )
                count += 1
                continue

        player_id = await _get_or_create_player(
            db, name, mlb_team=team, player_level=player_level
        )
        if mlbam_id:
            await _link_source_id(db, player_id, "mlbam", str(mlbam_id), 1.0, "id_join")
        count += 1

    await db.commit()
    logger.info(f"Resolved {count} MiLB players (batch {batch_id})")
    return count


async def resolve_prospects(db, batch_id: str) -> int:
    """Resolve prospect identities, linking by MLBAM or FanGraphs ID."""
    cursor = await db.execute(
        """SELECT DISTINCT player_name, mlbam_id, idfg, org, position
           FROM stg_prospect_rankings
           WHERE _batch_id = ? AND player_name IS NOT NULL""",
        (batch_id,),
    )
    rows = await cursor.fetchall()
    count = 0

    for name, mlbam_id, idfg, org, position in rows:
        # Try to find by existing source IDs first
        player_id = None

        if mlbam_id:
            cur = await db.execute(
                "SELECT player_id FROM core_player_source_id WHERE source = 'mlbam' AND source_player_id = ?",
                (str(mlbam_id),),
            )
            row = await cur.fetchone()
            if row:
                player_id = row[0]

        if not player_id and idfg:
            cur = await db.execute(
                "SELECT player_id FROM core_player_source_id WHERE source = 'fangraphs' AND source_player_id = ?",
                (str(idfg),),
            )
            row = await cur.fetchone()
            if row:
                player_id = row[0]

        if not player_id:
            player_id = await _get_or_create_player(
                db, name, position=position, mlb_team=org, player_level="MiLB"
            )

        # Ensure source IDs are linked
        if mlbam_id:
            await _link_source_id(db, player_id, "mlbam", str(mlbam_id), 1.0, "id_join")
        if idfg:
            await _link_source_id(db, player_id, "fangraphs", str(idfg), 1.0, "id_join")

        count += 1

    await db.commit()
    logger.info(f"Resolved {count} prospects (batch {batch_id})")
    return count


async def resolve_all(db, batch_id: str) -> dict:
    """Run full identity resolution in the correct order.

    Returns a dict of source -> count resolved.
    """
    results = {}

    # 1. pybaseball first (richest ID set)
    results["pybaseball_batting"] = await resolve_pybaseball_batting(db, batch_id)
    results["pybaseball_pitching"] = await resolve_pybaseball_pitching(db, batch_id)

    # 2. MiLB / Prospects (extends with minor leaguers)
    results["milb"] = await resolve_milb(db, batch_id)
    results["prospects"] = await resolve_prospects(db, batch_id)

    # 3. Statcast (match on MLBAM)
    results["statcast_batter"] = await resolve_statcast(db, batch_id, "stg_statcast_batter")
    results["statcast_pitcher"] = await resolve_statcast(db, batch_id, "stg_statcast_pitcher")

    # 4. ESPN (name + team)
    results["espn"] = await resolve_espn(db, batch_id)

    # 5. Fantrax (name + team)
    results["fantrax"] = await resolve_fantrax(db, batch_id)

    logger.info(f"Identity resolution complete for batch {batch_id}: {results}")
    return results

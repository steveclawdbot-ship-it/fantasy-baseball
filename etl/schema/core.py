"""DDL definitions for all core (core_*) tables.

Core tables are deduplicated, properly keyed, and business-logic-applied.
Identity resolution maps source IDs to a stable core_player.id.
"""

CORE_TABLES: list[str] = [
    # ---------------------------------------------------------------
    # Player identity
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS core_player (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        display_name TEXT NOT NULL,
        first_name TEXT,
        last_name TEXT,
        position TEXT,
        mlb_team TEXT,
        player_level TEXT DEFAULT 'MLB',
        birth_date TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_core_player_name ON core_player(display_name)",
    "CREATE INDEX IF NOT EXISTS idx_core_player_team ON core_player(mlb_team)",
    "CREATE INDEX IF NOT EXISTS idx_core_player_level ON core_player(player_level)",

    """
    CREATE TABLE IF NOT EXISTS core_player_source_id (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL REFERENCES core_player(id),
        source TEXT NOT NULL,
        source_player_id TEXT NOT NULL,
        confidence REAL DEFAULT 1.0,
        matched_by TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source, source_player_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_core_psid_player ON core_player_source_id(player_id)",
    "CREATE INDEX IF NOT EXISTS idx_core_psid_lookup ON core_player_source_id(source, source_player_id)",

    # ---------------------------------------------------------------
    # MLB batting stats (one row per player per season)
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS core_batting_season (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL REFERENCES core_player(id),
        season INTEGER NOT NULL,
        games INTEGER,
        pa INTEGER,
        ab INTEGER,
        hits INTEGER,
        hr INTEGER,
        rbi INTEGER,
        sb INTEGER,
        bb_pct REAL,
        k_pct REAL,
        avg REAL,
        obp REAL,
        slg REAL,
        ops REAL,
        woba REAL,
        wrc_plus REAL,
        iso REAL,
        war REAL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(player_id, season)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_core_bat_player ON core_batting_season(player_id)",
    "CREATE INDEX IF NOT EXISTS idx_core_bat_season ON core_batting_season(season)",

    # ---------------------------------------------------------------
    # MLB pitching stats (one row per player per season)
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS core_pitching_season (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL REFERENCES core_player(id),
        season INTEGER NOT NULL,
        games INTEGER,
        wins INTEGER,
        losses INTEGER,
        era REAL,
        whip REAL,
        ip REAL,
        k_per_9 REAL,
        bb_per_9 REAL,
        fip REAL,
        war REAL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(player_id, season)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_core_pit_player ON core_pitching_season(player_id)",

    # ---------------------------------------------------------------
    # Statcast batter
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS core_statcast_batter (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL REFERENCES core_player(id),
        season INTEGER NOT NULL,
        barrel_pct REAL,
        hard_hit_pct REAL,
        avg_exit_velocity REAL,
        max_exit_velocity REAL,
        launch_angle REAL,
        sweet_spot_pct REAL,
        xslg REAL,
        xwoba REAL,
        sprint_speed REAL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(player_id, season)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_core_scb_player ON core_statcast_batter(player_id)",

    # ---------------------------------------------------------------
    # Statcast pitcher
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS core_statcast_pitcher (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL REFERENCES core_player(id),
        season INTEGER NOT NULL,
        avg_velocity REAL,
        max_velocity REAL,
        spin_rate REAL,
        whiff_pct REAL,
        chase_pct REAL,
        xera REAL,
        xwoba_against REAL,
        k_pct REAL,
        bb_pct REAL,
        pitch_mix_json TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(player_id, season)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_core_scp_player ON core_statcast_pitcher(player_id)",

    # ---------------------------------------------------------------
    # ESPN fantasy context
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS core_espn_league (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        league_id INTEGER NOT NULL,
        season INTEGER NOT NULL,
        name TEXT,
        num_teams INTEGER,
        scoring_type TEXT,
        league_type TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(league_id, season)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS core_espn_team (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        league_id INTEGER NOT NULL,
        season INTEGER NOT NULL,
        espn_team_id INTEGER NOT NULL,
        team_name TEXT,
        owner TEXT,
        wins INTEGER,
        losses INTEGER,
        standing INTEGER,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(league_id, season, espn_team_id)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS core_espn_roster (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL REFERENCES core_player(id),
        league_id INTEGER NOT NULL,
        season INTEGER NOT NULL,
        espn_team_id INTEGER,
        espn_team_name TEXT,
        ownership_pct REAL,
        adp REAL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(player_id, league_id, season)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_core_espn_rost_player ON core_espn_roster(player_id)",

    # ---------------------------------------------------------------
    # Fantrax fantasy context
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS core_fantrax_league (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        league_id TEXT NOT NULL,
        season INTEGER NOT NULL,
        name TEXT,
        num_teams INTEGER,
        scoring_type TEXT,
        league_type TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(league_id, season)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS core_fantrax_team (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        league_id TEXT NOT NULL,
        season INTEGER NOT NULL,
        fantrax_team_id TEXT NOT NULL,
        team_name TEXT,
        owner TEXT,
        wins INTEGER,
        losses INTEGER,
        standing INTEGER,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(league_id, season, fantrax_team_id)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS core_fantrax_roster (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL REFERENCES core_player(id),
        league_id TEXT NOT NULL,
        season INTEGER NOT NULL,
        fantrax_team_id TEXT,
        fantrax_team_name TEXT,
        roster_slot TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(player_id, league_id, season)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_core_ftx_rost_player ON core_fantrax_roster(player_id)",
    "CREATE INDEX IF NOT EXISTS idx_core_ftx_rost_slot ON core_fantrax_roster(roster_slot)",

    # ---------------------------------------------------------------
    # MiLB stats (one row per player per season per level)
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS core_milb_batting_season (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL REFERENCES core_player(id),
        season INTEGER NOT NULL,
        level TEXT NOT NULL,
        games INTEGER,
        pa INTEGER,
        ab INTEGER,
        hits INTEGER,
        hr INTEGER,
        rbi INTEGER,
        sb INTEGER,
        bb_pct REAL,
        k_pct REAL,
        avg REAL,
        obp REAL,
        slg REAL,
        ops REAL,
        wrc_plus REAL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(player_id, season, level)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_core_milb_bat_player ON core_milb_batting_season(player_id)",

    """
    CREATE TABLE IF NOT EXISTS core_milb_pitching_season (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL REFERENCES core_player(id),
        season INTEGER NOT NULL,
        level TEXT NOT NULL,
        games INTEGER,
        wins INTEGER,
        losses INTEGER,
        era REAL,
        whip REAL,
        ip REAL,
        k_per_9 REAL,
        bb_per_9 REAL,
        fip REAL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(player_id, season, level)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_core_milb_pit_player ON core_milb_pitching_season(player_id)",

    # ---------------------------------------------------------------
    # Prospect rankings and FV grades
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS core_prospect (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL REFERENCES core_player(id),
        overall_rank INTEGER,
        position_rank INTEGER,
        hit_fv INTEGER,
        power_fv INTEGER,
        speed_fv INTEGER,
        field_fv INTEGER,
        overall_fv INTEGER,
        eta TEXT,
        risk_level TEXT,
        ranking_source TEXT,
        ranking_date TIMESTAMP,
        hype_score REAL,
        notes TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(player_id, ranking_source)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_core_prosp_player ON core_prospect(player_id)",
    "CREATE INDEX IF NOT EXISTS idx_core_prosp_rank ON core_prospect(overall_rank)",
]


async def create_core_tables(db) -> None:
    """Execute all core DDL statements."""
    for ddl in CORE_TABLES:
        await db.execute(ddl)
    await db.commit()

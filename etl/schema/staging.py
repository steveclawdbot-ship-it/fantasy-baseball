"""DDL definitions for all staging (stg_*) tables.

Every staging table includes:
  - _id: auto-increment primary key
  - _extracted_at: timestamp of extraction
  - _batch_id: groups rows from the same ETL run
  - _raw_json: full source row for forward compatibility / audit
"""

STAGING_TABLES: list[str] = [
    # ---------------------------------------------------------------
    # pybaseball / FanGraphs
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS stg_pybaseball_batting (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        _season INTEGER NOT NULL,
        name TEXT,
        team TEXT,
        age INTEGER,
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
        idfg INTEGER,
        key_mlbam INTEGER,
        _raw_json TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_stg_pyb_bat_batch ON stg_pybaseball_batting(_batch_id)",
    "CREATE INDEX IF NOT EXISTS idx_stg_pyb_bat_idfg ON stg_pybaseball_batting(idfg)",
    "CREATE INDEX IF NOT EXISTS idx_stg_pyb_bat_mlbam ON stg_pybaseball_batting(key_mlbam)",

    """
    CREATE TABLE IF NOT EXISTS stg_pybaseball_pitching (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        _season INTEGER NOT NULL,
        name TEXT,
        team TEXT,
        age INTEGER,
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
        idfg INTEGER,
        key_mlbam INTEGER,
        _raw_json TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_stg_pyb_pit_batch ON stg_pybaseball_pitching(_batch_id)",
    "CREATE INDEX IF NOT EXISTS idx_stg_pyb_pit_idfg ON stg_pybaseball_pitching(idfg)",

    # ---------------------------------------------------------------
    # Statcast
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS stg_statcast_batter (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        _season INTEGER NOT NULL,
        mlbam_id INTEGER,
        player_name TEXT,
        barrel_pct REAL,
        hard_hit_pct REAL,
        avg_exit_velocity REAL,
        max_exit_velocity REAL,
        launch_angle REAL,
        sweet_spot_pct REAL,
        xslg REAL,
        xwoba REAL,
        sprint_speed REAL,
        _raw_json TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_stg_sc_bat_mlbam ON stg_statcast_batter(mlbam_id)",
    "CREATE INDEX IF NOT EXISTS idx_stg_sc_bat_batch ON stg_statcast_batter(_batch_id)",

    """
    CREATE TABLE IF NOT EXISTS stg_statcast_pitcher (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        _season INTEGER NOT NULL,
        mlbam_id INTEGER,
        player_name TEXT,
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
        _raw_json TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_stg_sc_pit_mlbam ON stg_statcast_pitcher(mlbam_id)",
    "CREATE INDEX IF NOT EXISTS idx_stg_sc_pit_batch ON stg_statcast_pitcher(_batch_id)",

    # ---------------------------------------------------------------
    # ESPN
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS stg_espn_leagues (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        league_id INTEGER NOT NULL,
        year INTEGER NOT NULL,
        name TEXT,
        num_teams INTEGER,
        scoring_type TEXT,
        roster_size INTEGER,
        _raw_json TEXT
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS stg_espn_teams (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        league_id INTEGER NOT NULL,
        year INTEGER NOT NULL,
        espn_team_id INTEGER NOT NULL,
        team_name TEXT,
        owner TEXT,
        wins INTEGER,
        losses INTEGER,
        ties INTEGER,
        standing INTEGER,
        _raw_json TEXT
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS stg_espn_players (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        league_id INTEGER NOT NULL,
        year INTEGER NOT NULL,
        espn_id INTEGER NOT NULL,
        name TEXT,
        position TEXT,
        pro_team TEXT,
        age INTEGER,
        ownership_pct REAL,
        adp REAL,
        roster_team_id INTEGER,
        roster_team_name TEXT,
        stats_json TEXT,
        projected_stats_json TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_stg_espn_pl_espn_id ON stg_espn_players(espn_id)",
    "CREATE INDEX IF NOT EXISTS idx_stg_espn_pl_name ON stg_espn_players(name)",
    "CREATE INDEX IF NOT EXISTS idx_stg_espn_pl_batch ON stg_espn_players(_batch_id)",

    """
    CREATE TABLE IF NOT EXISTS stg_espn_activity (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        league_id INTEGER NOT NULL,
        activity_date TEXT,
        activity_type TEXT,
        description TEXT,
        _raw_json TEXT
    )
    """,

    # ---------------------------------------------------------------
    # Fantrax
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS stg_fantrax_leagues (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        league_id TEXT NOT NULL,
        name TEXT,
        num_teams INTEGER,
        scoring_type TEXT,
        _raw_json TEXT
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS stg_fantrax_teams (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        league_id TEXT NOT NULL,
        fantrax_team_id TEXT NOT NULL,
        team_name TEXT,
        owner TEXT,
        wins INTEGER,
        losses INTEGER,
        standing INTEGER,
        _raw_json TEXT
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS stg_fantrax_rosters (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        league_id TEXT NOT NULL,
        fantrax_team_id TEXT,
        fantrax_team_name TEXT,
        player_id TEXT,
        player_name TEXT,
        position TEXT,
        pro_team TEXT,
        roster_slot TEXT,
        _raw_json TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_stg_ftx_rost_name ON stg_fantrax_rosters(player_name)",
    "CREATE INDEX IF NOT EXISTS idx_stg_ftx_rost_batch ON stg_fantrax_rosters(_batch_id)",

    # ---------------------------------------------------------------
    # MiLB stats
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS stg_milb_batting (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        _season INTEGER NOT NULL,
        mlbam_id INTEGER,
        player_name TEXT,
        level TEXT,
        team TEXT,
        age INTEGER,
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
        _raw_json TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_stg_milb_bat_mlbam ON stg_milb_batting(mlbam_id)",
    "CREATE INDEX IF NOT EXISTS idx_stg_milb_bat_batch ON stg_milb_batting(_batch_id)",

    """
    CREATE TABLE IF NOT EXISTS stg_milb_pitching (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        _season INTEGER NOT NULL,
        mlbam_id INTEGER,
        player_name TEXT,
        level TEXT,
        team TEXT,
        age INTEGER,
        games INTEGER,
        wins INTEGER,
        losses INTEGER,
        era REAL,
        whip REAL,
        ip REAL,
        k_per_9 REAL,
        bb_per_9 REAL,
        fip REAL,
        _raw_json TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_stg_milb_pit_mlbam ON stg_milb_pitching(mlbam_id)",
    "CREATE INDEX IF NOT EXISTS idx_stg_milb_pit_batch ON stg_milb_pitching(_batch_id)",

    # ---------------------------------------------------------------
    # Prospect rankings
    # ---------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS stg_prospect_rankings (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        _extracted_at TIMESTAMP NOT NULL,
        _batch_id TEXT NOT NULL,
        player_name TEXT,
        mlbam_id INTEGER,
        idfg INTEGER,
        org TEXT,
        position TEXT,
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
        _raw_json TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_stg_prosp_mlbam ON stg_prospect_rankings(mlbam_id)",
    "CREATE INDEX IF NOT EXISTS idx_stg_prosp_idfg ON stg_prospect_rankings(idfg)",
    "CREATE INDEX IF NOT EXISTS idx_stg_prosp_batch ON stg_prospect_rankings(_batch_id)",
]


async def create_staging_tables(db) -> None:
    """Execute all staging DDL statements."""
    for ddl in STAGING_TABLES:
        await db.execute(ddl)
    await db.commit()

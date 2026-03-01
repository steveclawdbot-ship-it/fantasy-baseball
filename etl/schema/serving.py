"""DDL definitions for serving views.

These views are named after the original tables (players, teams, etc.)
so the existing ORM models and API routes read from them transparently.

Before creating these views, the legacy tables must be renamed to _legacy_*.
"""

# Views to drop before recreation (order matters for dependencies)
SERVING_VIEW_NAMES = [
    "players",
    "teams",
    "player_stats",
    "player_statcast",
    "player_offense_advanced",
    "prospects",
    "pitching_stats",
    "pitcher_statcast",
]

SERVING_VIEWS: list[str] = [
    # ---------------------------------------------------------------
    # players — replaces the old players table
    # ---------------------------------------------------------------
    """
    CREATE VIEW IF NOT EXISTS players AS
    SELECT
        cp.id,
        (SELECT CAST(cps.source_player_id AS INTEGER)
         FROM core_player_source_id cps
         WHERE cps.player_id = cp.id AND cps.source = 'espn'
         LIMIT 1
        ) AS espn_id,
        cp.display_name AS name,
        cp.position,
        cp.mlb_team AS team,
        cp.player_level,
        NULL AS age,
        cbs.avg,
        cbs.hr,
        cbs.rbi,
        cbs.sb,
        cbs.ops,
        NULL AS projected_avg,
        NULL AS projected_hr,
        NULL AS projected_rbi,
        NULL AS projected_sb,
        NULL AS projected_ops,
        (SELECT AVG(cer.adp) FROM core_espn_roster cer WHERE cer.player_id = cp.id) AS adp,
        NULL AS adp_trend,
        (SELECT AVG(cer.ownership_pct) FROM core_espn_roster cer WHERE cer.player_id = cp.id) AS ownership,
        CASE WHEN EXISTS (SELECT 1 FROM core_pitching_season cps2 WHERE cps2.player_id = cp.id)
             THEN 'pitcher' ELSE 'hitter' END AS player_type,
        cp.created_at,
        cp.updated_at
    FROM core_player cp
    LEFT JOIN core_batting_season cbs
        ON cbs.player_id = cp.id
        AND cbs.season = (SELECT MAX(season) FROM core_batting_season WHERE player_id = cp.id)
    WHERE cp.is_active = 1
    """,

    # ---------------------------------------------------------------
    # teams — combines ESPN + Fantrax teams
    # ---------------------------------------------------------------
    """
    CREATE VIEW IF NOT EXISTS teams AS
    SELECT
        cet.id,
        cet.league_id AS espn_league_id,
        cet.espn_team_id,
        cet.team_name AS name,
        cet.owner,
        cel.name AS league_name,
        cel.league_type,
        cel.num_teams,
        cet.updated_at AS created_at,
        cet.updated_at
    FROM core_espn_team cet
    JOIN core_espn_league cel
        ON cel.league_id = cet.league_id AND cel.season = cet.season
    """,

    # ---------------------------------------------------------------
    # player_stats — used by stats.py query presets
    # ---------------------------------------------------------------
    """
    CREATE VIEW IF NOT EXISTS player_stats AS
    SELECT
        cbs.id,
        cbs.player_id,
        cbs.season,
        cbs.hr,
        cbs.rbi,
        cbs.sb,
        cbs.avg,
        cbs.ops,
        cbs.war,
        cbs.updated_at AS recorded_at
    FROM core_batting_season cbs
    """,

    # ---------------------------------------------------------------
    # player_statcast — used by stats.py query presets
    # ---------------------------------------------------------------
    """
    CREATE VIEW IF NOT EXISTS player_statcast AS
    SELECT
        csb.id,
        csb.player_id,
        csb.season,
        csb.barrel_pct,
        csb.hard_hit_pct,
        csb.avg_exit_velocity,
        csb.max_exit_velocity,
        csb.launch_angle,
        csb.sweet_spot_pct,
        csb.xslg,
        csb.sprint_speed,
        NULL AS rolling_7d,
        NULL AS rolling_14d,
        NULL AS rolling_30d,
        csb.updated_at AS extraction_timestamp,
        'statcast' AS source,
        csb.updated_at AS created_at,
        csb.updated_at AS updated_at
    FROM core_statcast_batter csb
    """,

    # ---------------------------------------------------------------
    # player_offense_advanced
    # ---------------------------------------------------------------
    """
    CREATE VIEW IF NOT EXISTS player_offense_advanced AS
    SELECT
        cbs.id,
        cbs.player_id,
        cbs.season,
        cbs.wrc_plus,
        cbs.iso,
        cbs.bb_pct,
        cbs.k_pct,
        cbs.obp,
        cbs.slg,
        cbs.woba,
        csb.xwoba,
        cbs.updated_at AS extraction_timestamp,
        'fangraphs' AS source,
        cbs.updated_at AS created_at,
        cbs.updated_at AS updated_at
    FROM core_batting_season cbs
    LEFT JOIN core_statcast_batter csb
        ON csb.player_id = cbs.player_id AND csb.season = cbs.season
    """,

    # ---------------------------------------------------------------
    # prospects — rankings + latest MiLB stats
    # ---------------------------------------------------------------
    """
    CREATE VIEW IF NOT EXISTS prospects AS
    SELECT
        cpros.id,
        cpros.player_id,
        cp.display_name AS name,
        cp.position,
        cp.mlb_team AS org,
        cp.player_level AS current_level,
        cpros.overall_rank,
        cpros.position_rank,
        cpros.hit_fv AS hit_future_value,
        cpros.power_fv AS power_future_value,
        cpros.speed_fv AS speed_future_value,
        cpros.field_fv AS field_future_value,
        cpros.overall_fv AS overall_future_value,
        cpros.eta,
        cpros.risk_level,
        cpros.ranking_source,
        cpros.ranking_date,
        cpros.hype_score,
        cpros.notes,
        milb.avg AS milb_avg,
        milb.ops AS milb_ops,
        milb.hr AS milb_hr,
        milb.sb AS milb_sb,
        milb.level AS milb_level,
        milb.season AS milb_season,
        cpros.updated_at AS created_at,
        cpros.updated_at AS updated_at
    FROM core_prospect cpros
    JOIN core_player cp ON cp.id = cpros.player_id
    LEFT JOIN core_milb_batting_season milb
        ON milb.player_id = cpros.player_id
        AND milb.id = (
            SELECT m2.id FROM core_milb_batting_season m2
            WHERE m2.player_id = cpros.player_id
            ORDER BY m2.season DESC, m2.level DESC
            LIMIT 1
        )
    """,

    # ---------------------------------------------------------------
    # pitching_stats — season pitching data (mirrors player_stats)
    # ---------------------------------------------------------------
    """
    CREATE VIEW IF NOT EXISTS pitching_stats AS
    SELECT
        cps.id,
        cps.player_id,
        cps.season,
        cps.games,
        cps.wins,
        cps.losses,
        cps.era,
        cps.whip,
        cps.ip,
        cps.k_per_9,
        cps.bb_per_9,
        cps.fip,
        cps.war,
        cps.updated_at AS recorded_at
    FROM core_pitching_season cps
    """,

    # ---------------------------------------------------------------
    # pitcher_statcast — pitcher Statcast metrics
    # ---------------------------------------------------------------
    """
    CREATE VIEW IF NOT EXISTS pitcher_statcast AS
    SELECT
        cscp.id,
        cscp.player_id,
        cscp.season,
        cscp.avg_velocity,
        cscp.max_velocity,
        cscp.spin_rate,
        cscp.whiff_pct,
        cscp.chase_pct,
        cscp.xera,
        cscp.xwoba_against,
        cscp.k_pct,
        cscp.bb_pct,
        cscp.pitch_mix_json,
        cscp.updated_at AS extraction_timestamp,
        'statcast' AS source,
        cscp.updated_at AS created_at,
        cscp.updated_at AS updated_at
    FROM core_statcast_pitcher cscp
    """,
]


async def create_serving_views(db) -> None:
    """Drop and recreate all serving views."""
    for view_name in SERVING_VIEW_NAMES:
        await db.execute(f"DROP VIEW IF EXISTS {view_name}")
    for ddl in SERVING_VIEWS:
        await db.execute(ddl)
    await db.commit()


# Legacy table renames (run once during migration)
LEGACY_RENAMES: list[tuple[str, str]] = [
    ("players", "_legacy_players"),
    ("teams", "_legacy_teams"),
    ("player_stats", "_legacy_player_stats"),
    ("player_offense_advanced", "_legacy_player_offense_advanced"),
    ("player_statcast", "_legacy_player_statcast"),
    ("prospects", "_legacy_prospects"),
]


async def rename_legacy_tables(db) -> None:
    """Rename existing tables to _legacy_* so serving views can take their names."""
    for old_name, new_name in LEGACY_RENAMES:
        # Check if the old table exists (and isn't already a view)
        cursor = await db.execute(
            "SELECT type FROM sqlite_master WHERE name = ?", (old_name,)
        )
        row = await cursor.fetchone()
        if row and row[0] == "table":
            await db.execute(f"ALTER TABLE [{old_name}] RENAME TO [{new_name}]")
    await db.commit()

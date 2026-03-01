# Plan: Get Data Flowing — Staging → Core → Serving Pipeline

## Context

The repo has a working FastAPI backend, ETL script, and frontend dashboard — but the DB is empty. The ESPN service is fully coded but never wired to routes. The ETL only pulls batting stats and has schema mismatches with the ORM. The user wants a proper 3-layer data architecture (raw/staging → core/conformed → serving) and has multiple ESPN leagues to connect. Runs on both Mac and Raspberry Pi.

## Architecture: 3-Layer Data Pipeline in SQLite

```
Sources                  Staging (stg_*)           Core (core_*)              Serving (views)
─────────────────────    ──────────────────────    ─────────────────────────  ──────────────────
pybaseball batting  ──→  stg_pybaseball_batting ─→ core_player              → players (view)
pybaseball pitching ──→  stg_pybaseball_pitching → core_player_source_id    → teams (view)
statcast batter     ──→  stg_statcast_batter ───→  core_batting_season      → player_stats (view)
statcast pitcher    ──→  stg_statcast_pitcher ──→  core_pitching_season     → player_statcast (view)
ESPN (N leagues)    ──→  stg_espn_players ──────→  core_statcast_batter     → player_offense_advanced (view)
                         stg_espn_teams ────────→  core_espn_roster
                         stg_espn_leagues ──────→  core_espn_league
                                                   core_espn_team
```

Serving layer uses SQL views named after the original tables (`players`, `teams`, `player_stats`, etc.) so existing API routes and ORM models work without changes to read paths. Write endpoints in `players.py` get updated to target `core_player` directly.

## Identity Resolution

Three sources use different IDs: pybaseball has `IDfg` (FanGraphs) + `key_mlbam`, statcast uses MLBAM IDs, ESPN uses `espn_id`.

- `core_player` — stable internal ID, canonical name/position/team
- `core_player_source_id` — bridge table mapping `(source, source_player_id)` to `core_player.id`
- Resolution order: pybaseball first (richest ID set), statcast second (match on MLBAM), ESPN third (normalized name + team match with confidence scoring)

## ESPN Multi-League Config

```bash
# .env
ESPN_LEAGUE_IDS=12345,67890,11111
ESPN_LEAGUE_TYPES=dynasty,redraft,best_ball
ESPN_S2=...
ESPN_SWID=...
```

ETL iterates all configured leagues. `core_espn_roster` stores per-league-per-player context.

## New File Structure

```
etl/
├── __init__.py
├── config.py                    # FANTASY_DB_PATH, ESPN_LEAGUES, batch ID gen
├── db.py                        # Shared aiosqlite connection (reads env var)
├── runner.py                    # Orchestrator: extract → transform → serve
├── identity.py                  # Player identity resolution logic
├── extractors/
│   ├── __init__.py
│   ├── pybaseball_batting.py    # batting_stats() → stg_pybaseball_batting
│   ├── pybaseball_pitching.py   # pitching_stats() → stg_pybaseball_pitching
│   ├── statcast_batter.py       # statcast aggregates → stg_statcast_batter
│   └── espn_leagues.py          # ESPN service → stg_espn_* (reuses existing espn_service.py)
├── transforms/
│   ├── __init__.py
│   ├── players.py               # Identity resolution → core_player + core_player_source_id
│   ├── batting.py               # stg → core_batting_season
│   ├── pitching.py              # stg → core_pitching_season
│   ├── statcast.py              # stg → core_statcast_batter
│   └── espn.py                  # stg → core_espn_roster/league/team
├── serving/
│   └── refresh.py               # Rebuild serving views from core tables
└── schema/
    ├── __init__.py
    ├── staging.py               # DDL for all stg_* tables
    ├── core.py                  # DDL for all core_* tables
    ├── serving.py               # DDL for serving views
    └── migrate.py               # Version-tracked migration runner (no Alembic)
```

## Implementation Steps

### Phase 1: Foundation (additive only, no breaking changes)

1. **Create `etl/config.py`** — centralize `FANTASY_DB_PATH` (env var, Pi default fallback), ESPN league list from env, `new_batch_id()` helper
2. **Create `etl/db.py`** — shared async connection using config, replacing all hardcoded paths
3. **Create `etl/schema/` package** — DDL for staging tables (all prefixed `stg_*`, each with `_extracted_at`, `_batch_id`, `_raw_json`), core tables, and serving views
4. **Create `etl/schema/migrate.py`** — lightweight migration runner with `_schema_version` tracking. Additive migrations first (staging + core tables alongside existing tables)
5. **Wire migration into backend startup** — call `run_migrations()` in [database.py](backend/app/db/database.py)'s `init_db()` before `Base.metadata.create_all`

### Phase 2: Extractors

6. **Create `etl/extractors/pybaseball_batting.py`** — `batting_stats(year)` → `stg_pybaseball_batting`. Preserve `IDfg` and `key_mlbam` for identity resolution. Store `_raw_json` per row.
7. **Create `etl/extractors/pybaseball_pitching.py`** — `pitching_stats(year)` → `stg_pybaseball_pitching`
8. **Create `etl/extractors/statcast_batter.py`** — statcast aggregates → `stg_statcast_batter` (barrel%, hard_hit%, exit velo, sprint speed, xSLG, xwOBA)
9. **Create `etl/extractors/espn_leagues.py`** — reuse existing `ESPNService` from [espn_service.py](backend/app/services/espn_service.py). For each configured league: `get_teams()`, `get_players()`, `get_free_agents()`, `get_league_settings()` → `stg_espn_*`

### Phase 3: Transforms

10. **Create `etl/identity.py`** — player identity resolution: pybaseball provides fangraphs+mlbam IDs, statcast matches on MLBAM, ESPN matches on normalized (name, team) with confidence scoring
11. **Create `etl/transforms/players.py`** — orchestrate identity resolution per batch → `core_player` + `core_player_source_id`
12. **Create `etl/transforms/batting.py`** — `stg_pybaseball_batting` → `core_batting_season` (UPSERT, one row per player per season)
13. **Create `etl/transforms/pitching.py`** — same for pitching
14. **Create `etl/transforms/statcast.py`** — `stg_statcast_batter` → `core_statcast_batter`
15. **Create `etl/transforms/espn.py`** — `stg_espn_*` → `core_espn_roster`, `core_espn_league`, `core_espn_team`

### Phase 4: Serving Layer Cutover

16. **Create `etl/serving/refresh.py`** — drops and recreates serving views:
    - `players` view ← `core_player` + `core_batting_season` + `core_espn_roster`
    - `teams` view ← `core_espn_team` + `core_espn_league`
    - `player_stats` view ← `core_batting_season`
    - `player_statcast` view ← `core_statcast_batter`
    - `player_offense_advanced` view ← `core_batting_season` + `core_statcast_batter`
17. **Migration: rename legacy tables** — `players` → `_legacy_players`, etc. Then create serving views with original names.
18. **Update [players.py](backend/app/api/players.py) write endpoints** — `create_player` (line 70), `update_player` (line 85), `delete_player` (line 105) write to `core_player` instead of through the view
19. **Create `backend/app/api/etl.py`** — `POST /api/etl/sync` to trigger pipeline from frontend. Accepts `source` param (full/batting/pitching/statcast/espn). Runs as `asyncio.create_task`.
20. **Wire ETL router into [main.py](backend/app/main.py)**

### Phase 5: Runner & Cleanup

21. **Create `etl/runner.py`** — full orchestrator with CLI: `--full` (all sources) and `--source <name>` (single). Sequence: extract → identity resolution → transforms → refresh serving views.
22. **Migrate legacy data** — one-time move from `_legacy_*` into core tables
23. **Fix existing bugs**: `stats_service.py` missing `import pandas as pd` in `get_league_leaders()`
24. **Delete `etl/daily_sync.py`** — replaced by modular pipeline
25. **Update `backend/.env.example`** — add `ESPN_LEAGUE_IDS`, `ESPN_LEAGUE_TYPES`

## Files Modified

| File | Change |
|------|--------|
| [database.py](backend/app/db/database.py) | Add migration runner call in `init_db()` |
| [players.py](backend/app/api/players.py) | Write endpoints target `core_player` |
| [main.py](backend/app/main.py) | Add ETL router |
| [.env.example](backend/.env.example) | Add ESPN multi-league env vars |
| [stats_service.py](backend/app/services/stats_service.py) | Fix missing pandas import |

## Files Created

All `etl/` files listed above (~20 files), plus [backend/app/api/etl.py](backend/app/api/etl.py).

## Files Deleted

| File | Reason |
|------|--------|
| [daily_sync.py](etl/daily_sync.py) | Replaced by modular extractors/transforms |

## Verification

1. **Schema**: Start backend → `_schema_version` table exists, all `stg_*` and `core_*` tables created
2. **Pybaseball extract**: `python -m etl.runner --source batting` → `stg_pybaseball_batting` has rows with `_batch_id` and `_raw_json`
3. **ESPN extract**: `python -m etl.runner --source espn` → `stg_espn_players` has rows per configured league
4. **Identity resolution**: After full run, `core_player` has entries with source ID mappings in `core_player_source_id`
5. **Serving views**: `SELECT * FROM players LIMIT 5` returns data with batting stats, ADP, ownership
6. **API works**: `curl http://localhost:8000/api/players/` returns player data
7. **Frontend works**: Dashboard shows players, stats, trends
8. **ETL trigger**: `curl -X POST http://localhost:8000/api/etl/sync?source=full` starts pipeline
9. **Tests pass**: `cd backend && .venv/bin/pytest`

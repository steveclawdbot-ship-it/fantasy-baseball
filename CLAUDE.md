# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Fantasy Baseball Research Assistant — a personal scouting and strategy tool for ESPN/Fantrax/Sleeper fantasy leagues. Monorepo with a FastAPI backend, static HTML frontend, and ETL pipeline. Currently in MVP phase targeting the 2026 fantasy draft.

## Commands

### Run locally (backend + frontend)
```bash
./run-local.sh              # starts both services, creates .venv if needed
./stop-local.sh             # kills both
```

### Development (via Make)
```bash
make dev                    # backend :8000 + frontend :8001 in parallel
make dev-backend            # backend only
make dev-frontend           # frontend only
make install                # install all deps (npm + pip)
make test                   # pytest in backend/
make clean                  # remove __pycache__, .pytest_cache, *.db
make status                 # health check both services
```

### Backend tests
```bash
cd backend && .venv/bin/pytest                    # all tests
cd backend && .venv/bin/pytest tests/test_vector_quality.py  # single test
cd backend && .venv/bin/pytest -k "test_name"     # by name
```

The venv lives at `.venv/` (root level, created by run-local.sh) or `backend/venv/` (used by Makefile targets). Use whichever exists.

### ETL
```bash
.venv/bin/python etl/daily_sync.py   # pull batting stats via pybaseball
```

## Architecture

### Backend (`backend/app/`)
- **FastAPI** async app in `main.py` with lifespan-managed DB init/close
- **SQLAlchemy** async ORM with `aiosqlite` driver — all DB access is async
- **SQLite** database; path controlled by `FANTASY_DB_PATH` env var (defaults to `/home/jesse/clawd-steve/data/fantasy_baseball.db`)
- API routers mounted under `/api/`: health, players, teams, stats, sentiment, leagues
- Models in `models/models.py`: Player, PlayerCard, Team, Scout, ADPData, Prospect, TradeValue, PlayerOffenseAdvanced, PlayerStatcast
- DB session dependency via `get_db()` in `db/database.py`
- ESPN integration via `espn-api` library in `services/espn_service.py`

### Frontend (`frontend/`)
- Single `index.html` — static HTML/CSS/JS, no build step
- Served by Python's `http.server` in dev

### ETL (`etl/`)
- `daily_sync.py` pulls batting stats via `pybaseball`, upserts into SQLite
- Uses direct `aiosqlite` (not the ORM) with its own table creation
- Season selection: before March defaults to previous year's stats

### Scripts (`scripts/`)
- Standalone test/exploration scripts for ESPN, CBS, Fantrax, Sleeper APIs

### Data (`data/`)
- `fantasy_youtube_sources.json` — curated content sources
- DB files are gitignored

## Key Patterns

- All API routes are async; use `async def` and `await` for DB operations
- Player lookup is by `espn_id` (unique) or internal `id`
- The ETL pipeline and the ORM models define overlapping but not identical schemas — the ETL uses raw SQL while the backend uses SQLAlchemy models
- Environment config via `.env` file in backend (see `backend/.env.example`); ESPN credentials (espn_s2, SWID) needed for private league access
- Migrations are in `backend/migrations/` as numbered Python files

## Ports

| Service  | Default | Fallback |
|----------|---------|----------|
| Backend  | 8000    | 8002     |
| Frontend | 4173    | 4174     |

`run-local.sh` auto-detects port conflicts and falls back.

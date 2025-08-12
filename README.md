# MLB Stats ETL (statsapi.mlb.com)

Full and daily ETL that pulls rich data from `https://statsapi.mlb.com/` into pandas DataFrames, with optional Parquet export and SingleStore/MySQL upsert.

- Reference data: sports, leagues, divisions, venues, teams
- Schedules and detailed game feeds: games, linescores, team summaries, player box, plays, pitches
- Optional: standings, leaderboards, player seasonal stats, team seasonal stats
- Daily incremental updates with `state.json` tracking
- Request caching with `requests-cache` and built-in retry + rate limiting

## Requirements

- Python 3.10+ (tested on 3.11)
- Install dependencies:
```bash
git clone <this-repo>
cd mlbstattracker
pip install -r requirements.txt
```

## Configuration (.env)
Copy `.env.example` to `.env` and adjust as needed.

Key settings:
- API base and versions
  - `MLB_STATS_BASE_URL=https://statsapi.mlb.com/api`
  - `MLB_STATS_VER=v1`
  - `MLB_STATS_GAME_VER=v1.1`
  - `MLB_SPORT_ID=1`
- Concurrency / throttling
  - `MLB_MAX_WORKERS=6`
  - `MLB_REQS_PER_SEC=5.0` (global polite rate limit)
  - `MLB_HTTP_TIMEOUT=30`
- Caching (SQLite via `requests-cache`)
  - `MLB_CACHE_ENABLED=true`
  - `MLB_CACHE_PATH=./mlb_cache.sqlite`
  - `MLB_CACHE_TTL_SECONDS=21600` (6 hours)
- State file
  - `MLB_STATE_PATH=./mlb_state.json`
- Database (SingleStore/MySQL)
  - `DB_HOST=...`
  - `DB_PORT=3306`
  - `DB_DATABASE=MLB`
  - `DB_USER=...`
  - `DB_PASSWORD=...` (change from example placeholder)
- Logging
  - `LOG_LEVEL=INFO` (set to DEBUG for verbose logs)
  - `LOG_JSON=false` (set to true for structured logs)

Note: All settings can also be overridden with CLI flags where available.

## Usage

### Full dump
Fetches reference frames, schedules and game feeds for a season range, and optional extras. Writes to Parquet and/or DB.

```bash
# Minimal full dump (data only)
python -m mlb_stats_etl.full_dump --start-season 2024 --end-season 2024 --out ./out_full

# With extras and DB upsert
python -m mlb_stats_etl.full_dump \
  --start-season 2022 --end-season 2025 \
  --include-standings --include-leaderboards --include-player-stats --include-team-stats \
  --out ./out_full --write-db

# Use a custom DB URL (overrides .env)
python -m mlb_stats_etl.full_dump --start-season 2024 --end-season 2024 --write-db \
  --db-url "mysql+pymysql://user:pass@host:3306/MLB?charset=utf8mb4"
```

### Daily incremental
Fetches a date window around today, skips game feeds for games already known final (tracked in `mlb_state.json`).

```bash
# Today only, Parquet only
python -m mlb_stats_etl.update_daily --lookback-days 0 --lookahead-days 0 --out ./out_daily

# Typical daily window with DB write
python -m mlb_stats_etl.update_daily --lookback-days 3 --lookahead-days 1 --write-db
```

### Logging
Human-readable (default):
```bash
LOG_LEVEL=DEBUG python -m mlb_stats_etl.update_daily --lookback-days 0 --lookahead-days 0 --out ./out_debug
```
JSON logs (great for ingestion):
```bash
LOG_LEVEL=DEBUG LOG_JSON=true python -m mlb_stats_etl.full_dump --start-season 2025 --end-season 2025 --out ./out_full
```
Logs include HTTP timings, per-step row counts, table concatenations, parquet and DB write summaries.

## What gets produced

Tables (written to Parquet when `--out` is set and/or upserted to DB when `--write-db` is used):
- `sports`
- `leagues`
- `divisions`
- `venues`
- `teams`
- `seasons` (all seasons metadata for the sport)
- `people` (roster people + hydrated details for requested seasons)
- `games` (one row per game)
- `linescores` (one row per game/inning)
- `game_teams` (team totals per game/side)
- `game_players` (player box per game/side)
- `plays` (play-by-play)
- `pitches` (pitch-by-pitch)
- Optional extras:
  - `standings` (by season)
  - `leaders_players` (leaderboards by group/category/season)
  - `player_stats_season` (seasonal player stats by group)
  - `team_stats_season` (seasonal team stats by group)

Database primary keys (enforced via unique indexes when possible):
- `games`: `gamePk`
- `linescores`: `gamePk, inning`
- `game_teams`: `gamePk, side`
- `game_players`: `gamePk, person_id, side`
- `plays`: `gamePk, playId`
- `pitches`: `gamePk, playId, eventIndex`
- See `mlb_stats_etl/db.py` for the complete mapping.

## Caching and rate limiting
- Caching is enabled by default via `requests-cache` using a local SQLite file. This speeds up reruns and reduces API load.
- If the cache backend encounters an error (e.g., corrupt SQLite), the program automatically disables caching for that run and retries the request.
- Use `MLB_REQS_PER_SEC` and `MLB_MAX_WORKERS` to tune API concurrency. Defaults are polite.

## State and incremental behavior
- Daily runs use `MLB_STATE_PATH` (default `./mlb_state.json`) to remember game IDs known to be final, so subsequent runs skip those game feeds.
- The file also records the last window and run timestamp.

## Troubleshooting
- SSL/HTTP errors: rerun; built-in retry/backoff is enabled.
- Cache corruption: the program will log a warning and fall back to non-cached requests automatically. You can also delete the cache file specified by `MLB_CACHE_PATH`.
- Parquet: requires `pyarrow` (already in `requirements.txt`).
- DB connectivity: verify `DB_HOST`, `DB_USER`, `DB_PASSWORD`, and that your IP is allowed by the DB server. You can also supply `--db-url` to bypass `.env`.
- Verbose diagnostics: set `LOG_LEVEL=DEBUG`.

## Notes
- The program targets MLB `sportId=1` by default; override via `.env` if needed.
- Some early historical seasons may have partial or no schedule data. The scripts handle empty schedules gracefully.

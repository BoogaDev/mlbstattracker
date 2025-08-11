# MLB Stats ETL (statsapi.mlb.com)

This package pulls data from MLB's **Stats API** and assembles it into pandas DataFrames
that are easy to load into a database. It provides two entry points:

- `python -m mlb_stats_etl.full_dump` — full (historical) load for selected seasons
- `python -m mlb_stats_etl.update_daily` — daily incremental update (with caching + state)

Both scripts return a Python `dict[str, pandas.DataFrame]` containing tables like:

- `seasons`
- `leagues`, `divisions`, `teams`, `venues`
- `people` (players/coaches front-loaded from rosters + game feeds)
- `games`, `linescores`
- `game_teams` (team boxscore summary per game)
- `game_players` (player boxscore stats per game)
- `plays` (one row per plate appearance / play)
- `pitches` (one row per pitch event)
- `transactions` (daily), `standings` (optional)
- **NEW:** `leaders_players`, `player_stats_season`, `team_stats_season`

You can iterate that dict and push each DataFrame to your database **or** use the built-in `--write-db`
to upsert into SingleStore/MySQL automatically.

> Note: MLB’s Stats API is unofficially documented. This project follows the endpoints cataloged by the
> MLB-StatsAPI community wiki (toddrob99).

## Quick start

```bash
pip install -r requirements.txt

# Full dump of 2022–2025, write to DB using env DB_*
python -m mlb_stats_etl.full_dump --start-season 2022 --end-season 2025 --include-standings --write-db

# Daily update (default last 3 days through tomorrow) + DB write + state.json
python -m mlb_stats_etl.update_daily --lookback-days 3 --lookahead-days 1 --write-db
```

Use `--out parquet_out` to also write local Parquet files.

## Configuration

All settings are in `mlb_stats_etl/config.py` or via env vars.

- API base: `BASE_URL` (default `https://statsapi.mlb.com/api`), `DEFAULT_VER=v1`, `GAME_FEED_VER=v1.1`
- Caching: `MLB_CACHE_ENABLED=true`, `MLB_CACHE_PATH=./mlb_cache.sqlite`, `MLB_CACHE_TTL_SECONDS=21600`
- State file: `MLB_STATE_PATH=./mlb_state.json`
- Concurrency/rate: `MLB_MAX_WORKERS=6`, `MLB_REQS_PER_SEC=5.0`
- DB (SingleStore/MySQL): `DB_HOST`, `DB_PORT`, `DB_DATABASE`, `DB_USER`, `DB_PASSWORD`

> ⚠️ **Security note**: Consider using a secret manager or a `.env` that’s git-ignored; avoid committing credentials.

## What’s new in this version

- **Local HTTP caching** via `requests-cache` to cut repeat GETs.
- **State tracking** in `state.json`:
  - Keeps a set of **final gamePks**; on daily runs we skip re-fetching those already-final games.
  - Stores last run/window metadata.
- **Stats endpoints**:
  - Player **leaderboards**: `/api/v1/stats/leaders` (e.g., HR, hits, SO, ERA, WHIP, saves).
  - **Player season stats**: `/api/v1/stats?stats=season&group=hitting,pitching,fielding`.
  - **Team season stats**: `/api/v1/teams/stats?group=hitting,pitching&stats=season`.
- **SingleStore writer**:
  - Auto-creates tables from DataFrames (first run) and adds a UNIQUE index for upsert keys.
  - Bulk **REPLACE INTO** upsert for fast merges.

## Primary keys used

- `sports(id)`, `leagues(id)`, `divisions(id)`, `venues(id)`, `teams(id)`, `seasons(seasonId)`, `people(person_id)`
- `games(gamePk)`, `linescores(gamePk, inning)`, `game_teams(gamePk, side)`, `game_players(gamePk, person_id, side)`
- `plays(gamePk, playId)`, `pitches(gamePk, playId, eventIndex)`
- `standings(season, league_id, division_id, team_id)`, `transactions(id)`
- `leaders_players(season, stat_group, category, rank, person_id)`
- `player_stats_season(season, group, person_id)`, `team_stats_season(season, group, team_id)`

## Pushing to your DB

Both scripts accept `--write-db`. They read connection settings from env vars (or `config.py` defaults),
create tables if needed, add a UNIQUE index on the primary key columns, and perform a **REPLACE INTO** upsert.

If you’d like pure `INSERT ... ON DUPLICATE KEY UPDATE` semantics, we can switch the writer easily.


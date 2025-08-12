# MLB Stats ETL (statsapi.mlb.com)

Full & daily ETL into pandas DataFrames with optional SingleStore upsert.
Now includes caching, state.json, and leaders/stats endpoints. Uses `.env` config.

## Quick start
```bash
pip install -r requirements.txt

# Full dump
python -m mlb_stats_etl.full_dump --start-season 2022 --end-season 2025 --include-standings --write-db

# Daily (state-aware, cached)
python -m mlb_stats_etl.update_daily --lookback-days 3 --lookahead-days 1 --write-db
```


## Logging & Progress

- Set `LOG_LEVEL` in `.env` (DEBUG, INFO, WARNING, ERROR). Default is INFO.
- Set `LOG_JSON=true` if you want structured JSON logs.
- `tqdm` progress bars are enabled by default; set `TQDM_DISABLE=true` to turn them off.
- Internally, functions accept a `on_progress(event: str, payload: dict)` callback. The CLI wires this to the logger, so you’ll see high-level steps as they happen (e.g., fetching rosters, game feed counts, stats/leaders totals).

from __future__ import annotations
import argparse
import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Set

import pandas as pd

from .logging_utils import setup_logging
from .http_client import MLBClient
from .extract import (
    fetch_reference_frames,
    fetch_schedule_by_dates,
    fetch_game_feeds,
)
from .state import load_state, save_state, get_final_game_pks, add_final_game_pks, mark_daily_run
from .db import get_engine, write_tables_to_db
from .config import STATE_PATH
from .utils import concat_into, write_tables_to_parquet

log = logging.getLogger("mlb_stats_etl.update_daily")

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Daily incremental MLB Stats update")
    ap.add_argument("--lookback-days", type=int, default=int(os.getenv("LOOKBACK_DAYS", "3")))
    ap.add_argument("--lookahead-days", type=int, default=int(os.getenv("LOOKAHEAD_DAYS", "1")))
    ap.add_argument("--out", type=str, default="")
    ap.add_argument("--write-db", action="store_true")
    ap.add_argument("--db-url", type=str, default="")
    ap.add_argument("--log-level", type=str, default=os.getenv("LOG_LEVEL", "INFO"))
    ap.add_argument("--log-json", action="store_true")
    return ap.parse_args()

def _detect_newly_final(games_df: pd.DataFrame) -> Set[int]:
    if games_df is None or games_df.empty or "gamePk" not in games_df.columns:
        return set()
    for col in ("status_code", "status", "detailedState"):
        if col in games_df.columns:
            return set(
                games_df.loc[
                    games_df[col].astype(str).str.lower().str.contains("final"),
                    "gamePk",
                ].dropna().astype(int).tolist()
            )
    return set()

def main() -> Dict[str, pd.DataFrame]:
    args = parse_args()
    setup_logging(args.log_level, args.log_json)

    client = MLBClient()
    today = date.today()
    start = today - timedelta(days=args.lookback_days)
    end = today + timedelta(days=args.lookahead_days)

    state = load_state(STATE_PATH)
    known_final = get_final_game_pks(state)

    tables: Dict[str, pd.DataFrame] = {}
    ref = fetch_reference_frames(client)
    if isinstance(ref, dict):
        tables.update({k: (v if isinstance(v, pd.DataFrame) else pd.DataFrame(v)) for k, v in ref.items()})

    games = fetch_schedule_by_dates(client, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    if isinstance(games, pd.DataFrame) and not games.empty:
        tables["games"] = games

    concat_into(tables, "game_feeds", fetch_game_feeds(client, gamepks=games, skip_final=known_final))

    newly_final = _detect_newly_final(tables.get("games", pd.DataFrame()))

    if args.out:
        count = write_tables_to_parquet(Path(args.out), tables)
        print(f"Wrote {count} tables to {args.out}/")

    if args.write_db:
        engine = get_engine(args.db_url or None)
        counts = write_tables_to_db(tables, engine=engine)
        print("DB upsert counts:", json.dumps(counts, indent=2))

    add_final_game_pks(state, newly_final)
    mark_daily_run(state, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    save_state(STATE_PATH, state)

    summary = {k: int(v.shape[0]) for k, v in tables.items()}
    print("Row counts:", json.dumps(summary, indent=2))
    return tables

if __name__ == "__main__":
    main()

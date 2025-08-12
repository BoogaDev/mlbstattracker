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
    for col in ("status_codedGameState", "status_detailedState", "status_abstractGameState"):
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
    log.info("Daily run window start=%s end=%s lookback_days=%s lookahead_days=%s", start, end, args.lookback_days, args.lookahead_days)

    state = load_state(STATE_PATH)
    known_final = get_final_game_pks(state)
    log.info("Loaded state path=%s known_final_count=%s", STATE_PATH, len(known_final))

    tables: Dict[str, pd.DataFrame] = {}
    ref = fetch_reference_frames(client)
    if isinstance(ref, dict):
        tables.update({k: (v if isinstance(v, pd.DataFrame) else pd.DataFrame(v)) for k, v in ref.items()})
    for name in ("sports", "leagues", "divisions", "venues", "teams"):
        df = tables.get(name)
        if isinstance(df, pd.DataFrame):
            log.info("Loaded reference %s rows=%s cols=%s", name, df.shape[0], df.shape[1])

    games = fetch_schedule_by_dates(client, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    if isinstance(games, pd.DataFrame) and not games.empty:
        tables["games"] = games
    log.info("Schedule window games rows=%s", 0 if games is None else games.shape[0])

    # Pull feeds for gamePks in the window, skip ones already known final
    game_pks = []
    if isinstance(games, pd.DataFrame) and not games.empty and "gamePk" in games.columns:
        game_pks = games["gamePk"].dropna().astype(int).tolist()
    to_fetch = [pk for pk in game_pks if pk not in known_final]
    log.info("GamePks total=%s to_fetch=%s skipped_known_final=%s", len(game_pks), len(to_fetch), len(game_pks) - len(to_fetch))
    if to_fetch:
        g_df, l_df, gt_df, gp_df, plays_df, pitches_df = fetch_game_feeds(client, to_fetch)
        log.info(
            "Feeds fetched: games=%s lines=%s game_teams=%s game_players=%s plays=%s pitches=%s",
            0 if g_df is None else g_df.shape[0],
            0 if l_df is None else l_df.shape[0],
            0 if gt_df is None else gt_df.shape[0],
            0 if gp_df is None else gp_df.shape[0],
            0 if plays_df is None else plays_df.shape[0],
            0 if pitches_df is None else pitches_df.shape[0],
        )
        concat_into(tables, "games", g_df)
        concat_into(tables, "linescores", l_df)
        concat_into(tables, "game_teams", gt_df)
        concat_into(tables, "game_players", gp_df)
        concat_into(tables, "plays", plays_df)
        concat_into(tables, "pitches", pitches_df)

    newly_final = _detect_newly_final(tables.get("games", pd.DataFrame()))
    log.info("Newly final detected count=%s", len(newly_final))

    if args.out:
        count = write_tables_to_parquet(Path(args.out), tables)
        log.info("Parquet write complete path=%s tables_written=%s", args.out, count)
        print(f"Wrote {count} tables to {args.out}/")

    if args.write_db:
        engine = get_engine(args.db_url or None)
        counts = write_tables_to_db(tables, engine=engine)
        log.info("DB upsert complete counts=%s", counts)
        print("DB upsert counts:", json.dumps(counts, indent=2))

    add_final_game_pks(state, newly_final)
    mark_daily_run(state, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    save_state(STATE_PATH, state)
    log.info("State saved path=%s final_game_pks=%s", STATE_PATH, len(get_final_game_pks(state)))

    summary = {k: int(v.shape[0]) for k, v in tables.items()}
    log.info("Run summary row_counts=%s", summary)
    print("Row counts:", json.dumps(summary, indent=2))
    return tables


if __name__ == "__main__":
    main()

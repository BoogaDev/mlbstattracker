from __future__ import annotations
import argparse, os, json
from .logging_utils import setup_logging
import logging
from datetime import date, timedelta
import pandas as pd
from .http_client import MLBClient
from .extract import (
    fetch_reference_frames,
    fetch_schedule_by_dates,
    fetch_game_feeds,
    fetch_transactions,
)
from .stats import fetch_leaderboards, fetch_player_stats_season, fetch_team_stats_season
from .state import load_state, save_state, get_final_game_pks, add_final_game_pks, mark_daily_run
from .progress import Progress
from .db import write_tables_to_db, get_engine
from .config import STATE_PATH

DEFAULT_LEADER_CATS_HITTING = ["homeRuns", "hits", "runsBattedIn", "stolenBases", "onBasePercentage", "sluggingPercentage", "ops"]
DEFAULT_LEADER_CATS_PITCHING = ["wins", "strikeOuts", "earnedRunAverage", "walksAndHitsPerInningPitched", "saves"]

def main():
    ap = argparse.ArgumentParser(description="Daily MLB Stats API updater (caching + state-aware).")
    ap.add_argument("--lookback-days", type=int, default=3)
    ap.add_argument("--lookahead-days", type=int, default=1)
    ap.add_argument("--out", type=str, default="")
    ap.add_argument("--write-db", action="store_true")
    ap.add_argument("--db-url", type=str, default="")
    
    ap.add_argument('--log-level', type=str, default=os.getenv('LOG_LEVEL','INFO'), help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    ap.add_argument('--log-json', action='store_true' if os.getenv('LOG_JSON','false').lower() in ('1','true','yes','y') else 'store_false', help='Emit logs as JSON')
    args = ap.parse_args()

    setup_logging(args.log_level, args.log_json)
    log = logging.getLogger('mlb_stats_etl.update_daily')
    client = MLBClient()
    progress = Progress()

    today = date.today()
    start = today - timedelta(days=args.lookback_days)
    end = today + timedelta(days=args.lookahead_days)

    state = load_state(STATE_PATH)
    known_final = get_final_game_pks(state)

    ref = fetch_reference_frames(client, season=today.year, on_progress=progress.emit)

    sched_df = fetch_schedule_by_dates(client, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'), on_progress=progress.emit)
    if not sched_df.empty and "status_codedGameState" in sched_df.columns:
        skip_pks = set(sched_df.loc[(sched_df["status_codedGameState"].isin(["F", "O"])) & (sched_df["gamePk"].isin(known_final)), "gamePk"].astype(int).tolist())
    else:
        skip_pks = set()
    all_pks = set(sched_df["gamePk"].dropna().astype(int).tolist())
    to_fetch = sorted(all_pks - skip_pks)

    g=l=t=p=plays=pitches=pd.DataFrame()
    if to_fetch:
        log.info('Fetching %d game feeds (skipping %d already-final in state)', len(to_fetch), len(skip_pks))
        g, l, t, p, plays, pitches = fetch_game_feeds(client, to_fetch, on_progress=progress.emit)

    newly_final = set()
    if not g.empty:
        newly_final = set(g.loc[g["status_codedGameState"] == "F", "gamePk"].dropna().astype(int).tolist())

    tx = fetch_transactions(client, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

    season = today.year
    lp_h = fetch_leaderboards(client, season, DEFAULT_LEADER_CATS_HITTING, stat_group="hitting", on_progress=progress.emit)
    lp_p = fetch_leaderboards(client, season, DEFAULT_LEADER_CATS_PITCHING, stat_group="pitching", on_progress=progress.emit)
    leaders_players = pd.concat([lp_h, lp_p], ignore_index=True)
    ps = fetch_player_stats_season(client, season, on_progress=progress.emit)
    ts = fetch_team_stats_season(client, season, on_progress=progress.emit)

    tables = {
        "sports": ref["sports"],
        "leagues": ref["leagues"],
        "divisions": ref["divisions"],
        "venues": ref["venues"],
        "teams": ref["teams"],
        "games": g,
        "linescores": l,
        "game_teams": t,
        "game_players": p,
        "plays": plays,
        "pitches": pitches,
        "transactions": tx,
        "leaders_players": leaders_players,
        "player_stats_season": ps,
        "team_stats_season": ts,
    }

    if args.out:
        os.makedirs(args.out, exist_ok=True)
        for name, df in tables.items():
            if not df.empty:
                df.to_parquet(os.path.join(args.out, f"{name}.parquet"), index=False)
        print(f"Wrote {len([1 for df in tables.values() if not df.empty])} tables to {args.out}/")

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

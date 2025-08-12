from __future__ import annotations
import argparse
import json
import logging
import os
from pathlib import Path
from typing import Dict, List

import pandas as pd

from .logging_utils import setup_logging
from .http_client import MLBClient
from .extract import (
    fetch_reference_frames,
    fetch_seasons,
    fetch_team_roster_people,
    fetch_schedule_gamepks,
    fetch_game_feeds,
    fetch_standings,
)
from .stats import (
    fetch_leaderboards,
    fetch_player_stats_season,
    fetch_team_stats_season,
)
from .db import get_engine, write_tables_to_db
from .config import SPORT_ID
from .utils import concat_into, write_tables_to_parquet, to_int_series

log = logging.getLogger("mlb_stats_etl.full_dump")

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Full MLB Stats API dump (teams, rosters, schedules, game feeds, standings, seasonal stats)."
    )
    ap.add_argument("--start-season", type=int, required=True)
    ap.add_argument("--end-season", type=int, required=True)
    ap.add_argument("--sport-id", type=int, default=SPORT_ID)
    ap.add_argument("--include-standings", action="store_true")
    ap.add_argument("--include-leaderboards", action="store_true")
    ap.add_argument("--include-player-stats", action="store_true")
    ap.add_argument("--include-team-stats", action="store_true")
    ap.add_argument("--out", type=str, default="")
    ap.add_argument("--write-db", action="store_true")
    ap.add_argument("--db-url", type=str, default="")
    ap.add_argument("--log-level", type=str, default=os.getenv("LOG_LEVEL", "INFO"))
    ap.add_argument("--log-json", action="store_true")
    return ap.parse_args()

def _team_ids_from(teams_df: pd.DataFrame) -> List[int]:
    if teams_df is None or teams_df.empty:
        return []
    for col in ("team_id", "id"):
        if col in teams_df.columns:
            return to_int_series(teams_df[col]).tolist()
    return []

def main() -> Dict[str, pd.DataFrame]:
    args = parse_args()
    setup_logging(args.log_level, args.log_json)

    client = MLBClient()
    tables: Dict[str, pd.DataFrame] = {}

    ref = fetch_reference_frames(client, sport_id=args.sport_id)
    if isinstance(ref, dict):
        tables.update({k: (v if isinstance(v, pd.DataFrame) else pd.DataFrame(v)) for k, v in ref.items()})
    tables["seasons"] = fetch_seasons(client, start_season=args.start_season, end_season=args.end_season)

    for season in range(args.start_season, args.end_season + 1):
        team_ids = _team_ids_from(tables.get("teams", pd.DataFrame()))
        if team_ids:
            concat_into(tables, "people", fetch_team_roster_people(client, team_ids=team_ids, season=season))

        gamepks = fetch_schedule_gamepks(client, season=season, sport_id=args.sport_id)
        if isinstance(gamepks, pd.DataFrame) and not gamepks.empty:
            concat_into(tables, "games", gamepks)

        concat_into(tables, "game_feeds", fetch_game_feeds(client, gamepks=gamepks))

        if args.include_standings:
            concat_into(tables, "standings", fetch_standings(client, season=season, sport_id=args.sport_id))

        if args.include_leaderboards:
            for group, cats in (
                ("hitting", ["homeRuns","hits","runsBattedIn","stolenBases","onBasePercentage","sluggingPercentage","ops"]),
                ("pitching", ["wins","strikeOuts","earnedRunAverage","walksAndHitsPerInningPitched","saves"]),
            ):
                concat_into(tables, f"leaders_{group}", fetch_leaderboards(client, season=season, categories=cats, stat_group=group))

        if args.include_player_stats:
            concat_into(tables, "player_stats", fetch_player_stats_season(client, season=season, group_type="hitting"))

        if args.include_team_stats:
            concat_into(tables, "team_stats", fetch_team_stats_season(client, season=season, group_type="hitting"))

    if args.out:
        count = write_tables_to_parquet(Path(args.out), tables)
        print(f"Wrote {count} tables to {args.out}/")

    if args.write_db:
        engine = get_engine(args.db_url or None)
        counts = write_tables_to_db(tables, engine=engine)
        print("DB upsert counts:", json.dumps(counts, indent=2))

    summary = {k: int(v.shape[0]) if isinstance(v, pd.DataFrame) else 0 for k, v in tables.items()}
    print("Row counts:", json.dumps(summary, indent=2))
    return tables

if __name__ == "__main__":
    main()

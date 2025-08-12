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
    log.info("Starting full dump: start_season=%s end_season=%s sport_id=%s include_standings=%s include_leaderboards=%s include_player_stats=%s include_team_stats=%s out=%s write_db=%s",
             args.start_season, args.end_season, args.sport_id, args.include_standings, args.include_leaderboards, args.include_player_stats, args.include_team_stats, args.out, args.write_db)

    client = MLBClient()
    tables: Dict[str, pd.DataFrame] = {}

    # Reference frames (sports, leagues, divisions, venues, teams)
    ref = fetch_reference_frames(client)
    if isinstance(ref, dict):
        tables.update({k: (v if isinstance(v, pd.DataFrame) else pd.DataFrame(v)) for k, v in ref.items()})
    for name in ("sports", "leagues", "divisions", "venues", "teams"):
        df = tables.get(name)
        if isinstance(df, pd.DataFrame):
            log.info("Loaded reference %s rows=%s cols=%s", name, df.shape[0], df.shape[1])

    # Seasons reference (all seasons for the sport)
    tables["seasons"] = fetch_seasons(client, sport_id=args.sport_id, all_seasons=True)
    log.info("Loaded seasons rows=%s", tables["seasons"].shape[0] if isinstance(tables.get("seasons"), pd.DataFrame) else 0)

    log.info("Processing seasons range %s-%s", args.start_season, args.end_season)
    for season in range(args.start_season, args.end_season + 1):
        log.info("Season %s: start", season)
        # People/rosters
        team_ids = _team_ids_from(tables.get("teams", pd.DataFrame()))
        log.info("Season %s: team_ids=%s", season, len(team_ids))
        if team_ids:
            people_df = fetch_team_roster_people(client, team_ids=team_ids, season=season)
            log.info("Season %s: people rows fetched=%s", season, 0 if people_df is None else people_df.shape[0])
            concat_into(tables, "people", people_df)

        # Schedule and game feeds
        gamepks = fetch_schedule_gamepks(client, season=season)
        log.info("Season %s: schedule gamePks=%s", season, len(gamepks))
        if gamepks:
            games_df, lines_df, gteams_df, gplayers_df, plays_df, pitches_df = fetch_game_feeds(client, gamepks)
            log.info(
                "Season %s: feeds games=%s lines=%s game_teams=%s game_players=%s plays=%s pitches=%s",
                season,
                0 if games_df is None else games_df.shape[0],
                0 if lines_df is None else lines_df.shape[0],
                0 if gteams_df is None else gteams_df.shape[0],
                0 if gplayers_df is None else gplayers_df.shape[0],
                0 if plays_df is None else plays_df.shape[0],
                0 if pitches_df is None else pitches_df.shape[0],
            )
            concat_into(tables, "games", games_df)
            concat_into(tables, "linescores", lines_df)
            concat_into(tables, "game_teams", gteams_df)
            concat_into(tables, "game_players", gplayers_df)
            concat_into(tables, "plays", plays_df)
            concat_into(tables, "pitches", pitches_df)

        if args.include_standings:
            st_df = fetch_standings(client, season=season)
            log.info("Season %s: standings rows=%s", season, 0 if st_df is None else st_df.shape[0])
            concat_into(tables, "standings", st_df)

        if args.include_leaderboards:
            for group, cats in (
                ("hitting", [
                    "homeRuns",
                    "hits",
                    "runsBattedIn",
                    "stolenBases",
                    "onBasePercentage",
                    "sluggingPercentage",
                    "ops",
                ]),
                ("pitching", [
                    "wins",
                    "strikeOuts",
                    "earnedRunAverage",
                    "walksAndHitsPerInningPitched",
                    "saves",
                ]),
            ):
                lb_df = fetch_leaderboards(client, season=season, categories=cats, stat_group=group)
                log.info("Season %s: leaders %s rows=%s", season, group, 0 if lb_df is None else lb_df.shape[0])
                concat_into(
                    tables,
                    "leaders_players",
                    lb_df,
                )

        if args.include_player_stats:
            ps_df = fetch_player_stats_season(client, season=season)
            log.info("Season %s: player_stats rows=%s", season, 0 if ps_df is None else ps_df.shape[0])
            concat_into(
                tables,
                "player_stats_season",
                ps_df,
            )

        if args.include_team_stats:
            ts_df = fetch_team_stats_season(client, season=season)
            log.info("Season %s: team_stats rows=%s", season, 0 if ts_df is None else ts_df.shape[0])
            concat_into(
                tables,
                "team_stats_season",
                ts_df,
            )

    if args.out:
        count = write_tables_to_parquet(Path(args.out), tables)
        log.info("Parquet write complete path=%s tables_written=%s", args.out, count)
        print(f"Wrote {count} tables to {args.out}/")

    if args.write_db:
        engine = get_engine(args.db_url or None)
        counts = write_tables_to_db(tables, engine=engine)
        log.info("DB upsert complete counts=%s", counts)
        print("DB upsert counts:", json.dumps(counts, indent=2))

    summary = {k: int(v.shape[0]) if isinstance(v, pd.DataFrame) else 0 for k, v in tables.items()}
    log.info("Run summary row_counts=%s", summary)
    print("Row counts:", json.dumps(summary, indent=2))
    return tables


if __name__ == "__main__":
    main()

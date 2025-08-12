from __future__ import annotations
import argparse, os, json
from .logging_utils import setup_logging
import logging
import pandas as pd
from .http_client import MLBClient
from .extract import (
    fetch_reference_frames,
    fetch_seasons,
    fetch_team_roster_people,
    fetch_schedule_gamepks,
    fetch_game_feeds,
    fetch_standings,
)
from .stats import fetch_leaderboards, fetch_player_stats_season, fetch_team_stats_season
from .config import SPORT_ID
from .progress import Progress
from .db import write_tables_to_db, get_engine

DEFAULT_LEADER_CATS_HITTING = ["homeRuns", "hits", "runsBattedIn", "stolenBases", "onBasePercentage", "sluggingPercentage", "ops"]
DEFAULT_LEADER_CATS_PITCHING = ["wins", "strikeOuts", "earnedRunAverage", "walksAndHitsPerInningPitched", "saves"]

def main():
    ap = argparse.ArgumentParser(description="Full MLB Stats API dump for given seasons.")
    ap.add_argument("--start-season", type=int, required=True)
    ap.add_argument("--end-season", type=int, required=True)
    ap.add_argument("--out", type=str, default="")
    ap.add_argument("--include-standings", action="store_true")
    ap.add_argument("--write-db", action="store_true")
    ap.add_argument("--db-url", type=str, default="")
    
    ap.add_argument('--log-level', type=str, default=os.getenv('LOG_LEVEL','INFO'), help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    ap.add_argument('--log-json', action='store_true' if os.getenv('LOG_JSON','false').lower() in ('1','true','yes','y') else 'store_false', help='Emit logs as JSON')
    args = ap.parse_args()

    setup_logging(args.log_level, args.log_json)
    log = logging.getLogger('mlb_stats_etl.full_dump')
    client = MLBClient()

    all_tables = {
        "sports": pd.DataFrame(), "leagues": pd.DataFrame(), "divisions": pd.DataFrame(),
        "venues": pd.DataFrame(), "teams": pd.DataFrame(), "seasons": pd.DataFrame(),
        "people": pd.DataFrame(),
        "games": pd.DataFrame(), "linescores": pd.DataFrame(), "game_teams": pd.DataFrame(),
        "game_players": pd.DataFrame(), "plays": pd.DataFrame(), "pitches": pd.DataFrame(),
        "standings": pd.DataFrame(),
        "leaders_players": pd.DataFrame(), "player_stats_season": pd.DataFrame(), "team_stats_season": pd.DataFrame(),
    }

    progress = Progress()
    ref = fetch_reference_frames(client, season=args.end_season, on_progress=progress.emit)
    all_tables.update(ref)

    seasons_df = fetch_seasons(client, sport_id=SPORT_ID, all_seasons=True, on_progress=progress.emit)
    seasons_df = seasons_df[seasons_df["seasonId"].astype(int).between(args.start_season, args.end_season)]
    all_tables["seasons"] = seasons_df

    for season in range(args.start_season, args.end_season + 1):
        print(f"=== Season {season} ===")
        teams_season = ref["teams"]
        if "season" in teams_season.columns:
            teams_season = teams_season[teams_season["season"].astype(str) == str(season)]
        if teams_season.empty:
            from .extract import fetch_reference_frames as _fetch_ref
            ref_season = _fetch_ref(client, season=season)
            tdf = ref_season["teams"]
        else:
            tdf = teams_season
        team_ids = sorted(set(tdf["id"].dropna().astype(int).tolist()))

        log.info('Season %s: fetching rosters for %d teams', season, len(team_ids))
        ppl = fetch_team_roster_people(client, team_ids, season=season, on_progress=progress.emit)
        all_tables["people"] = pd.concat([all_tables["people"], ppl], ignore_index=True)

        game_pks = fetch_schedule_gamepks(client, season=season, game_types="R,P", on_progress=progress.emit)
        log.info('Season %s: %d gamePks', season, len(game_pks))
        g, l, t, p, plays, pitches = fetch_game_feeds(client, game_pks, on_progress=progress.emit)
        all_tables["games"] = pd.concat([all_tables["games"], g], ignore_index=True)
        all_tables["linescores"] = pd.concat([all_tables["linescores"], l], ignore_index=True)
        all_tables["game_teams"] = pd.concat([all_tables["game_teams"], t], ignore_index=True)
        all_tables["game_players"] = pd.concat([all_tables["game_players"], p], ignore_index=True)
        all_tables["plays"] = pd.concat([all_tables["plays"], plays], ignore_index=True)
        all_tables["pitches"] = pd.concat([all_tables["pitches"], pitches], ignore_index=True)

        if args.include_standings:
            from .extract import fetch_standings as _fetch_standings
            std = _fetch_standings(client, season=season)
            all_tables["standings"] = pd.concat([all_tables["standings"], std], ignore_index=True)

        lp_h = fetch_leaderboards(client, season, DEFAULT_LEADER_CATS_HITTING, stat_group="hitting", on_progress=progress.emit)
        lp_p = fetch_leaderboards(client, season, DEFAULT_LEADER_CATS_PITCHING, stat_group="pitching", on_progress=progress.emit)
        leaders_players = pd.concat([lp_h, lp_p], ignore_index=True)
        all_tables["leaders_players"] = pd.concat([all_tables["leaders_players"], leaders_players], ignore_index=True)

        ps = fetch_player_stats_season(client, season, on_progress=progress.emit)
        all_tables["player_stats_season"] = pd.concat([all_tables["player_stats_season"], ps], ignore_index=True)

        ts = fetch_team_stats_season(client, season, on_progress=progress.emit)
        all_tables["team_stats_season"] = pd.concat([all_tables["team_stats_season"], ts], ignore_index=True)

    if not all_tables["people"].empty:
        all_tables["people"] = all_tables["people"].sort_values("season").drop_duplicates(subset=["person_id"], keep="last")

    if args.out:
        os.makedirs(args.out, exist_ok=True)
        for name, df in all_tables.items():
            if not df.empty:
                df.to_parquet(os.path.join(args.out, f"{name}.parquet"), index=False)
        print(f"Wrote {len([1 for df in all_tables.values() if not df.empty])} tables to {args.out}/")

    if args.write_db:
        engine = get_engine(args.db_url or None)
        counts = write_tables_to_db(all_tables, engine=engine)
        print("DB upsert counts:", json.dumps(counts, indent=2))

    summary = {k: int(v.shape[0]) for k, v in all_tables.items()}
    print("Row counts:", json.dumps(summary, indent=2))
    return all_tables

if __name__ == "__main__":
    main()

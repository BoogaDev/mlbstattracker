from __future__ import annotations
import concurrent.futures as cf
import logging
from typing import Any, Dict, Iterable, List, Tuple, Optional
from datetime import date, timedelta
import pandas as pd
from tqdm import tqdm

from .config import DEFAULT_VER, GAME_FEED_VER, SPORT_ID, MAX_WORKERS
from .http_client import MLBClient
from .parsers import parse_schedule_to_games, parse_game_feed
from .progress import ProgressCallback

log = logging.getLogger('mlb_stats_etl.extract')


def _concat(dfs: Iterable[pd.DataFrame]) -> pd.DataFrame:
    dfs = [df for df in dfs if df is not None and not df.empty]
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def fetch_reference_frames(client: MLBClient, season: Optional[int]=None, on_progress: Optional[ProgressCallback]=None) -> Dict[str, pd.DataFrame]:
    if on_progress: on_progress('fetch_reference_frames:start', {"season": season})
    sports = client.get(f"/{DEFAULT_VER}/sports")
    sports_df = pd.json_normalize(sports.get("sports", []))
    leagues = client.get(f"/{DEFAULT_VER}/league")
    leagues_df = pd.json_normalize(leagues.get("leagues", []) or leagues.get("league", []))
    divisions = client.get(f"/{DEFAULT_VER}/divisions")
    divisions_df = pd.json_normalize(divisions.get("divisions", []))
    venues = client.get(f"/{DEFAULT_VER}/venues")
    venues_df = pd.json_normalize(venues.get("venues", []))
    params = {"sportId": SPORT_ID}
    if season:
        params["season"] = season
    teams = client.get(f"/{DEFAULT_VER}/teams", params=params)
    teams_df = pd.json_normalize(teams.get("teams", []))
    out = {"sports": sports_df, "leagues": leagues_df, "divisions": divisions_df, "venues": venues_df, "teams": teams_df}
    log.info("Reference frames loaded sports=%s leagues=%s divisions=%s venues=%s teams=%s",
             sports_df.shape[0], leagues_df.shape[0], divisions_df.shape[0], venues_df.shape[0], teams_df.shape[0])
    return out


def fetch_seasons(client: MLBClient, sport_id: int=SPORT_ID, all_seasons: bool=True, on_progress: Optional[ProgressCallback]=None) -> pd.DataFrame:
    params = {"sportId": sport_id}
    if all_seasons:
        params["all"] = True
    if on_progress: on_progress('fetch_seasons:start', {"sport_id": sport_id, "all": all_seasons})
    seasons = client.get(f"/{DEFAULT_VER}/seasons", params=params)
    df = pd.json_normalize(seasons.get("seasons", []))
    if on_progress: on_progress('fetch_seasons:done', {"rows": int(df.shape[0])})
    log.info("Seasons fetched rows=%s", df.shape[0])
    return df


def fetch_team_roster_people(client: MLBClient, team_ids: List[int], season: int, on_progress: Optional[ProgressCallback]=None) -> pd.DataFrame:
    people_rows: List[Dict[str, Any]] = []

    if on_progress: on_progress('rosters:start', {"teams": len(team_ids), "season": season})
    log.info("Rosters fetch start season=%s teams=%s", season, len(team_ids))
    def pull_one(tid: int) -> List[Dict[str, Any]]:
        r = client.get(f"/{DEFAULT_VER}/teams/{tid}/roster", params={"season": season})
        rows = []
        for item in r.get("roster", []):
            person = item.get("person", {}) or {}
            pos = item.get("position", {}) or {}
            rows.append({
                "team_id": tid, "season": season, "person_id": person.get("id"), "fullName": person.get("fullName"),
                "link": person.get("link"), "position_code": pos.get("abbreviation"), "position_type": pos.get("type"),
                "rosterStatus": item.get("status", {}).get("code"),
            })
        return rows

    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for rows in tqdm(ex.map(pull_one, team_ids), total=len(team_ids), desc=f"Rosters {season}"):
            people_rows.extend(rows)

    if on_progress: on_progress('rosters:fetched', {"rows": len(people_rows)})
    log.info("Rosters fetched season=%s people_rows=%s", season, len(people_rows))
    df = pd.DataFrame.from_records(people_rows)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["person_id"]).reset_index(drop=True)
    person_ids = df["person_id"].dropna().astype(int).tolist()
    person_dfs = []
    CHUNK = 50
    for i in tqdm(range(0, len(person_ids), CHUNK), desc="Hydrate people"):
        chunk = person_ids[i:i+CHUNK]
        pdata = client.get(f"/{DEFAULT_VER}/people", params={"personIds": ",".join(map(str, chunk))})
        person_dfs.append(pd.json_normalize(pdata.get("people", [])))
    people_details_df = pd.concat(person_dfs, ignore_index=True) if person_dfs else pd.DataFrame()
    if on_progress: on_progress('rosters:hydrated', {"persons": len(person_ids), "details_rows": int(people_details_df.shape[0])})
    log.info("Rosters hydrated season=%s persons=%s details_rows=%s", season, len(person_ids), people_details_df.shape[0])
    out = df.merge(people_details_df.add_prefix("person_"), left_on="person_id", right_on="person_id", how="left")
    log.info("Rosters merged season=%s rows=%s cols=%s", season, out.shape[0], out.shape[1])
    return out


def fetch_schedule_gamepks(client: MLBClient, season: int, game_types: str="R,P", on_progress: Optional[ProgressCallback]=None) -> List[int]:
    params = {"sportId": 1, "season": season, "gameTypes": game_types}
    sched = client.get(f"/{DEFAULT_VER}/schedule", params=params)
    games_df = parse_schedule_to_games(sched)
    if on_progress: on_progress('schedule:season', {"season": season, "games": int(games_df.shape[0])})
    if games_df is None or games_df.empty or "gamePk" not in games_df.columns:
        log.info("Schedule season=%s games=0 (no gamePk)", season)
        return []
    out = games_df["gamePk"].dropna().astype(int).tolist()
    log.info("Schedule season=%s games=%s", season, len(out))
    return out


def fetch_schedule_by_dates(client: MLBClient, start_date: str, end_date: str, sport_id: int=1, on_progress: Optional[ProgressCallback]=None) -> pd.DataFrame:
    params = {"sportId": sport_id, "startDate": start_date, "endDate": end_date}
    sched = client.get(f"/{DEFAULT_VER}/schedule", params=params)
    df = parse_schedule_to_games(sched)
    if on_progress: on_progress('schedule:dates', {"start": start_date, "end": end_date, "games": int(df.shape[0])})
    log.info("Schedule dates start=%s end=%s games=%s", start_date, end_date, df.shape[0])
    return df


def fetch_game_feeds(client: MLBClient, game_pks: List[int], on_progress: Optional[ProgressCallback]=None):
    games_list=[]; line_list=[]; team_list=[]; player_list=[]; plays_list=[]; pitches_list=[]

    if on_progress: on_progress('game_feeds:start', {"count": len(game_pks)})
    log.info("Game feeds start count=%s", len(game_pks))
    def pull_one(gamePk: int):
        try:
            r = client.get(f"/{GAME_FEED_VER}/game/{gamePk}/feed/live")
        except Exception:
            r = client.get(f"/{DEFAULT_VER}/game/{gamePk}/feed/live")
        return r

    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for gj in tqdm(ex.map(pull_one, game_pks), total=len(game_pks), desc="Games"):
            g, l, t, p, plays, pitches = parse_game_feed(gj)
            games_list.append(g); line_list.append(l); team_list.append(t); player_list.append(p); plays_list.append(plays); pitches_list.append(pitches)

    def _concat_non_na(dfs: List[pd.DataFrame]) -> pd.DataFrame:
        filtered = []
        for d in dfs:
            if d is None or not isinstance(d, pd.DataFrame) or d.empty:
                continue
            # skip frames that are entirely NA to avoid FutureWarning
            if d.isna().all().all():
                continue
            filtered.append(d)
        return pd.concat(filtered, ignore_index=True) if filtered else pd.DataFrame()

    games_df = _concat_non_na(games_list)
    lines_df = _concat_non_na(line_list)
    gteams_df = _concat_non_na(team_list)
    gplayers_df = _concat_non_na(player_list)
    plays_df = _concat_non_na(plays_list)
    pitches_df = _concat_non_na(pitches_list)
    log.info(
        "Game feeds done games=%s lines=%s game_teams=%s game_players=%s plays=%s pitches=%s",
        games_df.shape[0], lines_df.shape[0], gteams_df.shape[0], gplayers_df.shape[0], plays_df.shape[0], pitches_df.shape[0]
    )
    return games_df, lines_df, gteams_df, gplayers_df, plays_df, pitches_df


def fetch_standings(client: MLBClient, season: int, league_ids: str="103,104", on_progress: Optional[ProgressCallback]=None) -> pd.DataFrame:
    params = {"leagueId": league_ids, "season": season}
    s = client.get(f"/{DEFAULT_VER}/standings", params=params)
    recs = []
    for rec in s.get("records", []):
        div = rec.get("division", {}) or {}
        for teamrec in rec.get("teamRecords", []):
            row = {
                "season": season, "league_id": rec.get("league", {}).get("id"), "division_id": div.get("id"),
                "team_id": teamrec.get("team", {}).get("id"), "wins": teamrec.get("wins"), "losses": teamrec.get("losses"),
                "pct": teamrec.get("winningPercentage"), "runsScored": teamrec.get("runsScored"),
                "runsAllowed": teamrec.get("runsAllowed"), "streak": (teamrec.get("streak", {}) or {}).get("streakCode"),
            }
            recs.append(row)
    df = pd.DataFrame.from_records(recs)
    if on_progress: on_progress('standings:season', {"season": season, "rows": int(df.shape[0])})
    log.info("Standings season=%s rows=%s", season, df.shape[0])
    return df


def fetch_transactions(client: MLBClient, start_date: str, end_date: str, team_id: Optional[int]=None) -> pd.DataFrame:
    params = {"startDate": start_date, "endDate": end_date}
    if team_id:
        params["teamId"] = team_id
    t = client.get(f"/{DEFAULT_VER}/transactions", params=params)
    rows = []
    for tr in t.get("transactions", []):
        rows.append({
            "id": tr.get("id"),
            "team_id": (tr.get("team", {}) or {}).get("id"),
            "date": tr.get("date"),
            "type": tr.get("typeCode"),
            "player_id": (tr.get("person", {}) or {}).get("id"),
            "player_name": (tr.get("person", {}) or {}).get("fullName"),
            "description": tr.get("description"),
            "effectiveDate": tr.get("effectiveDate"),
        })
    df = pd.DataFrame.from_records(rows)
    log.info("Transactions start=%s end=%s rows=%s", start_date, end_date, df.shape[0])
    return df

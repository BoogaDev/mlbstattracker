from __future__ import annotations

import logging
from typing import List, Optional
import pandas as pd
from .config import DEFAULT_VER, SPORT_ID
from .progress import ProgressCallback
from .http_client import MLBClient

log = logging.getLogger('mlb_stats_etl.stats')

def fetch_leaderboards(client: MLBClient, season: int, categories: List[str], stat_group: str="hitting", player_pool: str="ALL", limit: int=100, on_progress: Optional[ProgressCallback]=None) -> pd.DataFrame:
    if on_progress: on_progress('leaders:start', {"season": season, "group": stat_group, "categories": categories})
    rows = []
    for cat in categories:
        params = {
            "leaderCategories": cat,
            "season": season,
            "statGroup": stat_group,
            "playerPool": player_pool,
            "limit": limit,
            "sportId": SPORT_ID,
        }
        data = client.get(f"/{DEFAULT_VER}/stats/leaders", params=params)
        for cat_obj in data.get("leagueLeaders", []):
            for leader in cat_obj.get("leaders", []):
                person = (leader.get("person") or {})
                team = (leader.get("team") or {})
                rows.append({
                    "season": season,
                    "stat_group": stat_group,
                    "category": cat,
                    "rank": leader.get("rank"),
                    "value": leader.get("value"),
                    "person_id": person.get("id"),
                    "player_fullName": person.get("fullName"),
                    "team_id": team.get("id"),
                    "team_name": team.get("name"),
                })
    df = pd.DataFrame.from_records(rows)
    if on_progress: on_progress('leaders:done', {"season": season, "group": stat_group, "rows": int(df.shape[0])})
    return df

def fetch_player_stats_season(client: MLBClient, season: int, groups: List[str]=["hitting", "pitching", "fielding"], stats: str="season", game_type: str="R", on_progress: Optional[ProgressCallback]=None) -> pd.DataFrame:
    if on_progress: on_progress('player_stats:start', {"season": season, "groups": groups})
    if on_progress: on_progress('team_stats:start', {"season": season, "groups": groups})
    dfs = []
    for grp in groups:
        params = {"stats": stats, "group": grp, "season": season, "sportIds": SPORT_ID, "gameType": game_type, "playerPool": "ALL", "limit": 5000}
        data = client.get(f"/{DEFAULT_VER}/stats", params=params)
        recs = []
        for row in data.get("stats", []):
            for s in row.get("splits", []):
                player = (s.get("player") or {}); team = (s.get("team") or {}); stat = (s.get("stat") or {})
                rec = {"season": season, "group": grp, "person_id": player.get("id"), "player_fullName": player.get("fullName"),
                       "team_id": team.get("id"), "team_name": team.get("name")}
                for k, v in (stat or {}).items(): rec[k] = v
                recs.append(rec)
        dfs.append(pd.DataFrame.from_records(recs))
    out = pd.concat([df for df in dfs if not df.empty], ignore_index=True) if dfs else pd.DataFrame()
    if on_progress: on_progress('player_stats:done', {"season": season, "rows": int(out.shape[0])})
    return out

def fetch_team_stats_season(client: MLBClient, season: int, groups: List[str]=["hitting", "pitching"], stats: str="season", game_type: str="R", on_progress: Optional[ProgressCallback]=None) -> pd.DataFrame:
    if on_progress: on_progress('player_stats:start', {"season": season, "groups": groups})
    if on_progress: on_progress('team_stats:start', {"season": season, "groups": groups})
    dfs = []
    for grp in groups:
        params = {"season": season, "group": grp, "stats": stats, "sportIds": SPORT_ID, "gameType": game_type}
        data = client.get(f"/{DEFAULT_VER}/teams/stats", params=params)
        recs = []
        for row in data.get("stats", []):
            for split in row.get("splits", []):
                team = (split.get("team") or {}); stat = (split.get("stat") or {})
                rec = {"season": season, "group": grp, "team_id": team.get("id"), "team_name": team.get("name")}
                for k, v in (stat or {}).items(): rec[k] = v
                recs.append(rec)
        dfs.append(pd.DataFrame.from_records(recs))
    out = pd.concat([df for df in dfs if not df.empty], ignore_index=True) if dfs else pd.DataFrame()
    if on_progress: on_progress('player_stats:done', {"season": season, "rows": int(out.shape[0])})
    return out

from __future__ import annotations
from typing import Dict, List, Tuple
import pandas as pd
from .config import DEFAULT_VER, SPORT_ID
from .http_client import MLBClient

# Reference: stats, stats/leaders, teams/stats endpoints (community docs list parameters)
# https://github.com/toddrob99/MLB-StatsAPI/wiki/Endpoints

def fetch_leaderboards(client: MLBClient, season: int, categories: List[str], stat_group: str="hitting", player_pool: str="ALL", limit: int=100) -> pd.DataFrame:
    """
    Pull player leaderboards for given categories + season.
    categories: e.g., ["homeRuns", "hits", "strikeOuts", "wins", "earnedRunAverage"]
    stat_group: "hitting" | "pitching" | "fielding"
    """
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
    return pd.DataFrame.from_records(rows)

def fetch_player_stats_season(client: MLBClient, season: int, groups: List[str]=["hitting", "pitching", "fielding"], stats: str="season", game_type: str="R") -> pd.DataFrame:
    """
    Pull per-player season stats across groups using /stats.
    Note: /stats returns 50 records by default; set a high limit.
    """
    dfs = []
    for grp in groups:
        params = {
            "stats": stats,
            "group": grp,
            "season": season,
            "sportIds": SPORT_ID,
            "gameType": game_type,
            "playerPool": "ALL",
            "limit": 5000,  # plenty for MLB
        }
        data = client.get(f"/{DEFAULT_VER}/stats", params=params)
        recs = []
        for row in data.get("stats", []):
            splits = row.get("splits", [])
            for s in splits:
                player = (s.get("player") or {})
                team = (s.get("team") or {})
                stat = (s.get("stat") or {})
                rec = {"season": season, "group": grp, "person_id": player.get("id"), "player_fullName": player.get("fullName"),
                       "team_id": team.get("id"), "team_name": team.get("name")}
                # Flatten stat dict
                for k, v in (stat or {}).items():
                    rec[k] = v
                recs.append(rec)
        dfs.append(pd.DataFrame.from_records(recs))
    return pd.concat([df for df in dfs if not df.empty], ignore_index=True) if dfs else pd.DataFrame()

def fetch_team_stats_season(client: MLBClient, season: int, groups: List[str]=["hitting", "pitching"], stats: str="season", game_type: str="R") -> pd.DataFrame:
    """
    Pull per-team season stats using /teams/stats.
    """
    dfs = []
    for grp in groups:
        params = {
            "season": season,
            "group": grp,
            "stats": stats,
            "sportIds": SPORT_ID,
            "gameType": game_type,
        }
        data = client.get(f"/{DEFAULT_VER}/teams/stats", params=params)
        recs = []
        for row in data.get("stats", []):
            for split in row.get("splits", []):
                team = (split.get("team") or {})
                stat = (split.get("stat") or {})
                rec = {"season": season, "group": grp, "team_id": team.get("id"), "team_name": team.get("name")}
                for k, v in (stat or {}).items():
                    rec[k] = v
                recs.append(rec)
        dfs.append(pd.DataFrame.from_records(recs))
    return pd.concat([df for df in dfs if not df.empty], ignore_index=True) if dfs else pd.DataFrame()

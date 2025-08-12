from __future__ import annotations
from typing import Any, Dict, List, Tuple
import pandas as pd

def _norm(d: Dict[str, Any], prefix: str="") -> Dict[str, Any]:
    out = {}
    for k, v in (d or {}).items():
        out[f"{prefix}{k}"] = v
    return out

def _get(d: Dict[str, Any], path: str, default=None):
    cur = d
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur

def parse_schedule_to_games(schedule_json: Dict[str, Any]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for date_block in schedule_json.get("dates", []):
        for g in date_block.get("games", []):
            rows.append({
                "gamePk": g.get("gamePk"),
                "gameGuid": g.get("gameGuid"),
                "gameType": g.get("gameType"),
                "season": g.get("season"),
                "gameDate": g.get("gameDate"),
                "status_abstractGameState": _get(g, "status.abstractGameState"),
                "status_codedGameState": _get(g, "status.codedGameState"),
                "status_detailedState": _get(g, "status.detailedState"),
                "status_abstractGameCode": _get(g, "status.abstractGameCode"),
                "doubleHeader": g.get("doubleHeader"),
                "seriesDescription": g.get("seriesDescription"),
                "isTie": g.get("isTie"),
                "ifNecessary": g.get("ifNecessary"),
                "ifNecessaryDescription": g.get("ifNecessaryDescription"),
                "venue_id": _get(g, "venue.id"),
                "venue_name": _get(g, "venue.name"),
                "home_team_id": _get(g, "teams.home.team.id"),
                "home_team_name": _get(g, "teams.home.team.name"),
                "away_team_id": _get(g, "teams.away.team.id"),
                "away_team_name": _get(g, "teams.away.team.name"),
            })
    return pd.DataFrame.from_records(rows)

def parse_game_feed(game_json: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    gamePk = _get(game_json, "gameData.game.pk") or _get(game_json, "gamePk") or _get(game_json, "gameData.gamePk")
    game_row = {
        "gamePk": gamePk,
        "season": _get(game_json, "gameData.game.season"),
        "game_type": _get(game_json, "gameData.game.type"),
        "game_datetime": _get(game_json, "gameData.datetime.dateTime"),
        "game_startTimeTBD": _get(game_json, "gameData.datetime.startTimeTBD"),
        "status_detailedState": _get(game_json, "gameData.status.detailedState"),
        "status_codedGameState": _get(game_json, "gameData.status.codedGameState"),
        "venue_id": _get(game_json, "gameData.venue.id"),
        "venue_name": _get(game_json, "gameData.venue.name"),
        "home_team_id": _get(game_json, "gameData.teams.home.id"),
        "home_team_name": _get(game_json, "gameData.teams.home.name"),
        "away_team_id": _get(game_json, "gameData.teams.away.id"),
        "away_team_name": _get(game_json, "gameData.teams.away.name"),
        "weather_condition": _get(game_json, "gameData.weather.condition"),
        "weather_temp": _get(game_json, "gameData.weather.temp"),
        "officialScorer_id": _get(game_json, "gameData.officialScorer.id"),
        "officialScorer_name": _get(game_json, "gameData.officialScorer.fullName"),
    }
    games_df = pd.DataFrame([game_row]).drop_duplicates(subset=["gamePk"])

    lines = []
    ls = _get(game_json, "liveData.linescore")
    if ls:
        innings = ls.get("innings", [])
        for idx, inn in enumerate(innings, start=1):
            lines.append({
                "gamePk": gamePk,
                "inning": idx,
                "home_runs": _get(inn, "home.runs"),
                "home_hits": _get(inn, "home.hits"),
                "home_errors": _get(inn, "home.errors"),
                "away_runs": _get(inn, "away.runs"),
                "away_hits": _get(inn, "away.hits"),
                "away_errors": _get(inn, "away.errors"),
            })
    linescores_df = pd.DataFrame.from_records(lines)

    team_rows = []
    for side in ("home", "away"):
        team_rows.append({
            "gamePk": gamePk,
            "side": side,
            "team_id": _get(game_json, f"liveData.boxscore.teams.{side}.team.id"),
            "team_name": _get(game_json, f"liveData.boxscore.teams.{side}.team.name"),
            "batters_total_ab": _get(game_json, f"liveData.boxscore.teams.{side}.teamStats.batting.atBats"),
            "batters_total_r": _get(game_json, f"liveData.boxscore.teams.{side}.teamStats.batting.runs"),
            "batters_total_h": _get(game_json, f"liveData.boxscore.teams.{side}.teamStats.batting.hits"),
            "batters_total_hr": _get(game_json, f"liveData.boxscore.teams.{side}.teamStats.batting.homeRuns"),
            "batters_total_bb": _get(game_json, f"liveData.boxscore.teams.{side}.teamStats.batting.baseOnBalls"),
            "batters_total_so": _get(game_json, f"liveData.boxscore.teams.{side}.teamStats.batting.strikeOuts"),
            "pitch_total_ip": _get(game_json, f"liveData.boxscore.teams.{side}.teamStats.pitching.inningsPitched"),
            "pitch_total_r": _get(game_json, f"liveData.boxscore.teams.{side}.teamStats.pitching.runs"),
            "pitch_total_er": _get(game_json, f"liveData.boxscore.teams.{side}.teamStats.pitching.earnedRuns"),
            "pitch_total_so": _get(game_json, f"liveData.boxscore.teams.{side}.teamStats.pitching.strikeOuts"),
            "pitch_total_bb": _get(game_json, f"liveData.boxscore.teams.{side}.teamStats.pitching.baseOnBalls"),
        })
    game_teams_df = pd.DataFrame.from_records(team_rows)

    player_rows = []
    for side in ("home", "away"):
        players = _get(game_json, f"liveData.boxscore.teams.{side}.players") or {}
        for pid_str, pdata in players.items():
            try:
                pid = int(pid_str.replace("ID", ""))
            except Exception:
                pid = None
            batting = _get(pdata, "stats.batting") or {}
            pitching = _get(pdata, "stats.pitching") or {}
            fielding = _get(pdata, "stats.fielding") or {}
            row = {
                "gamePk": gamePk, "side": side,
                "person_id": _get(pdata, "person.id") or pid,
                "person_fullName": _get(pdata, "person.fullName"),
                "position_code": _get(pdata, "position.abbreviation"),
                "position_type": _get(pdata, "position.type"),
                "batting_order": _get(pdata, "battingOrder"),
                "batting_ab": batting.get("atBats"),
                "batting_r": batting.get("runs"),
                "batting_h": batting.get("hits"),
                "batting_hr": batting.get("homeRuns"),
                "batting_rbi": batting.get("rbi"),
                "batting_bb": batting.get("baseOnBalls"),
                "batting_so": batting.get("strikeOuts"),
                "batting_sb": batting.get("stolenBases"),
                "batting_avg": batting.get("avg"),
                "batting_obp": batting.get("obp"),
                "batting_slg": batting.get("slg"),
                "batting_ops": batting.get("ops"),
                "pitching_ip": pitching.get("inningsPitched"),
                "pitching_r": pitching.get("runs"),
                "pitching_er": pitching.get("earnedRuns"),
                "pitching_bb": pitching.get("baseOnBalls"),
                "pitching_so": pitching.get("strikeOuts"),
                "pitching_hr": pitching.get("homeRuns"),
                "pitching_era": pitching.get("era"),
                "fielding_po": fielding.get("putOuts"),
                "fielding_a": fielding.get("assists"),
                "fielding_e": fielding.get("errors"),
            }
            player_rows.append(row)
    game_players_df = pd.DataFrame.from_records(player_rows)

    play_rows, pitch_rows = [], []
    all_plays = _get(game_json, "liveData.plays.allPlays") or []
    for p in all_plays:
        play_id = p.get("playId")
        about = _get(p, "about") or {}
        matchup = _get(p, "matchup") or {}
        result = _get(p, "result") or {}
        counts = _get(p, "count") or {}
        play_rows.append({
            "gamePk": gamePk,
            "playId": play_id,
            "atBatIndex": about.get("atBatIndex"),
            "halfInning": about.get("halfInning"),
            "inning": about.get("inning"),
            "startTime": about.get("startTime"),
            "endTime": about.get("endTime"),
            "isOut": about.get("isOut"),
            "hasReview": about.get("hasReview"),
            "event": result.get("event"),
            "eventType": result.get("eventType"),
            "description": result.get("description"),
            "rbi": result.get("rbi"),
            "awayScore": result.get("awayScore"),
            "homeScore": result.get("homeScore"),
            "pitcher_id": matchup.get("pitcher", {}).get("id"),
            "batter_id": matchup.get("batter", {}).get("id"),
            "batSide": matchup.get("batSide", {}).get("code"),
            "pitchHand": matchup.get("pitchHand", {}).get("code"),
            "balls": counts.get("balls"),
            "strikes": counts.get("strikes"),
            "outs": counts.get("outs"),
        })
        for ev in p.get("playEvents", []):
            if ev.get("isPitch", False):
                det = ev.get("details", {}) or {}
                pd_ = ev.get("pitchData", {}) or {}
                hc = ev.get("hitData", {}) or {}
                pitch_rows.append({
                    "gamePk": gamePk, "playId": play_id,
                    "eventIndex": ev.get("index"),
                    "pitch_number": ev.get("pitchNumber"),
                    "call_code": det.get("call", {}).get("code"),
                    "call_description": det.get("call", {}).get("description"),
                    "type_code": det.get("type", {}).get("code"),
                    "type_description": det.get("type", {}).get("description"),
                    "description": det.get("description"),
                    "fromCatcher": det.get("fromCatcher"),
                    "startSpeed": pd_.get("startSpeed"),
                    "endSpeed": pd_.get("endSpeed"),
                    "strikeZoneTop": pd_.get("strikeZoneTop"),
                    "strikeZoneBottom": pd_.get("strikeZoneBottom"),
                    "coordinates_pX": pd_.get("coordinates", {}).get("pX"),
                    "coordinates_pZ": pd_.get("coordinates", {}).get("pZ"),
                    "plateTime": pd_.get("plateTime"),
                    "launchSpeed": hc.get("launchSpeed"),
                    "launchAngle": hc.get("launchAngle"),
                    "totalDistance": hc.get("totalDistance"),
                    "trajectory": hc.get("trajectory"),
                    "hardness": hc.get("hardness"),
                    "hitCoordinates_x": hc.get("coordinates", {}).get("coordX"),
                    "hitCoordinates_y": hc.get("coordinates", {}).get("coordY"),
                })
    plays_df = pd.DataFrame.from_records(play_rows)
    pitches_df = pd.DataFrame.from_records(pitch_rows)
    return games_df, linescores_df, game_teams_df, game_players_df, plays_df, pitches_df

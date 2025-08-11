from __future__ import annotations
import json, os, time
from typing import Dict, Any, Set

def load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {"final_game_pks": [], "last_daily_run": None, "last_window": None}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(path: str, state: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp, path)

def get_final_game_pks(state: Dict[str, Any]) -> Set[int]:
    return set(int(x) for x in state.get("final_game_pks", []) if x is not None)

def add_final_game_pks(state: Dict[str, Any], new_pks) -> None:
    cur = get_final_game_pks(state)
    cur.update(int(x) for x in new_pks if x is not None)
    state["final_game_pks"] = sorted(cur)

def mark_daily_run(state: Dict[str, Any], start_date: str, end_date: str) -> None:
    state["last_daily_run"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    state["last_window"] = {"start": start_date, "end": end_date}

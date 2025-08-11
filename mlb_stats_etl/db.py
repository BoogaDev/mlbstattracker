from __future__ import annotations
from typing import Dict, List, Sequence
import math
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from .config import build_db_url

def get_engine(db_url: str | None = None) -> Engine:
    url = db_url or build_db_url()
    engine = create_engine(url, pool_pre_ping=True, pool_recycle=3600)
    return engine

def ensure_table_exists(engine: Engine, table_name: str, df: pd.DataFrame) -> None:
    """
    Create table if it doesn't exist using pandas' DDL inference (first run).
    No-op if the table already exists.
    """
    exists_sql = text("SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :t LIMIT 1")
    with engine.begin() as conn:
        res = conn.execute(exists_sql, {"t": table_name}).fetchone()
        if res:
            return  # already exists
    # Create with 0 rows to only emit CREATE TABLE DDL
    df.iloc[0:0].to_sql(table_name, engine, index=False, if_exists="fail")

def create_unique_index(engine: Engine, table: str, index_name: str, cols: Sequence[str]) -> None:
    cols_sql = ", ".join(f"`{c}`" for c in cols)
    stmt = f"CREATE UNIQUE INDEX `{index_name}` ON `{table}` ({cols_sql})"
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(stmt)
    except Exception:
        # ignore if it already exists
        pass

def _chunk_iter(seq, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def replace_into(engine: Engine, table: str, df: pd.DataFrame, chunk_size: int = 1000) -> int:
    """Bulk upsert by REPLACE INTO (requires PK or UNIQUE index on target)."""
    if df.empty:
        return 0
    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_sql = ", ".join(f"`{c}`" for c in cols)
    sql = f"REPLACE INTO `{table}` ({col_sql}) VALUES ({placeholders})"
    total = 0
    with engine.begin() as conn:
        for chunk in _chunk_iter(list(df.itertuples(index=False, name=None)), chunk_size):
            conn.exec_driver_sql(sql, chunk)
            total += len(chunk)
    return total

# Main entry for writing many tables with default PKs/indices
DEFAULT_KEYS: Dict[str, List[str]] = {
    "sports": ["id"],
    "leagues": ["id"],
    "divisions": ["id"],
    "venues": ["id"],
    "teams": ["id"],
    "seasons": ["seasonId"],
    "people": ["person_id"],
    "games": ["gamePk"],
    "linescores": ["gamePk", "inning"],
    "game_teams": ["gamePk", "side"],
    "game_players": ["gamePk", "person_id", "side"],
    "plays": ["gamePk", "playId"],
    "pitches": ["gamePk", "playId", "eventIndex"],
    "standings": ["season", "league_id", "division_id", "team_id"],
    "transactions": ["id"],
    "leaders_players": ["season", "stat_group", "category", "rank", "person_id"],
    "leaders_teams": ["season", "stat_group", "category", "rank", "team_id"],
    "player_stats_season": ["season", "group", "person_id"],
    "team_stats_season": ["season", "group", "team_id"],
}

def write_tables_to_db(tables: Dict[str, pd.DataFrame], engine: Engine | None = None, keys: Dict[str, List[str]] | None = None) -> Dict[str, int]:
    engine = engine or get_engine()
    keys = keys or DEFAULT_KEYS
    counts: Dict[str, int] = {}
    for name, df in tables.items():
        if df is None or df.empty:
            continue
        # Ensure table exists
        try:
            ensure_table_exists(engine, name, df)
        except Exception:
            # Table probably exists; continue
            pass

        # Ensure we have a UNIQUE index on the PK set
        pk = keys.get(name)
        if pk:
            idx_name = f"uniq_{name}_{'_'.join(pk)}"
            create_unique_index(engine, name, idx_name, pk)

        # Write via REPLACE INTO
        written = replace_into(engine, name, df)
        counts[name] = written
    return counts

from __future__ import annotations
from typing import Dict, List, Sequence
import logging
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from .config import build_db_url

log = logging.getLogger("mlb_stats_etl.db")


def get_engine(db_url: str | None = None) -> Engine:
    url = db_url or build_db_url()
    engine = create_engine(url, pool_pre_ping=True, pool_recycle=3600)
    log.info("DB engine created url=%s", url)
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
            return
    log.info("Creating table (inferred) name=%s", table_name)
    df.iloc[0:0].to_sql(table_name, engine, index=False, if_exists="fail")


def create_unique_index(engine: Engine, table: str, index_name: str, cols: Sequence[str]) -> None:
    cols_sql = ", ".join(f"`{c}`" for c in cols)
    stmt = f"CREATE UNIQUE INDEX `{index_name}` ON `{table}` ({cols_sql})"
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(stmt)
        log.info("Created/ensured unique index table=%s index=%s cols=%s", table, index_name, list(cols))
    except Exception:
        # Index may already exist or be unsupported; ignore
        pass


def _chunk_iter(seq, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]


def _clean_value(val):
    # Normalize pandas/NumPy NA to None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    # Normalize booleans (including numpy types) to 0/1
    if isinstance(val, (np.bool_, bool)):
        return 1 if bool(val) else 0
    # Normalize floats (including numpy types)
    if isinstance(val, (np.floating, float)):
        f = float(val)
        if not np.isfinite(f):
            return None
        return f
    # Normalize ints (including numpy types)
    if isinstance(val, (np.integer, int)):
        return int(val)
    # Normalize strings that are textual NaNs
    if isinstance(val, str) and val.strip().lower() in ("nan", "inf", "-inf"):
        return None
    # Leave everything else (including dates/ISO strings) as-is
    return val


def replace_into(engine: Engine, table: str, df: pd.DataFrame, chunk_size: int = 1000) -> int:
    if df.empty:
        return 0
    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_sql = ", ".join(f"`{c}`" for c in cols)
    sql = f"REPLACE INTO `{table}` ({col_sql}) VALUES ({placeholders})"
    total = 0
    with engine.begin() as conn:
        for i in range(0, len(df), chunk_size):
            chunk_df = df.iloc[i:i+chunk_size]
            # Build SQL-safe rows
            rows = [tuple(_clean_value(x) for x in chunk_df.iloc[r].tolist()) for r in range(len(chunk_df))]
            if not rows:
                continue
            conn.exec_driver_sql(sql, rows)
            total += len(rows)
            log.debug("REPLACE chunk table=%s rows=%s total_written=%s", table, len(rows), total)
    log.info("REPLACE done table=%s rows_written=%s", table, total)
    return total


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
        log.info("DB writing table=%s rows=%s cols=%s", name, df.shape[0], df.shape[1])
        try:
            ensure_table_exists(engine, name, df)
        except Exception:
            pass
        pk = keys.get(name)
        if pk:
            idx_name = f"uniq_{name}_{'_'.join(pk)}"
            create_unique_index(engine, name, idx_name, pk)
        written = replace_into(engine, name, df)
        counts[name] = written
    return counts

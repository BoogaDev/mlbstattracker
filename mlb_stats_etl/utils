from __future__ import annotations
from pathlib import Path
from typing import Dict, Optional
import pandas as pd

def concat_into(tables: Dict[str, pd.DataFrame], key: str, df: Optional[pd.DataFrame]) -> None:
    """Append a DataFrame into tables[key], creating the key if needed; ignore None/empty."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return
    current = tables.get(key)
    tables[key] = pd.concat([current, df], ignore_index=True) if isinstance(current, pd.DataFrame) and not current.empty else df

def write_tables_to_parquet(root: Path, tables: Dict[str, pd.DataFrame]) -> int:
    """Write non-empty tables to Parquet under root, return count written."""
    root.mkdir(parents=True, exist_ok=True)
    written = 0
    for name, df in tables.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            (root / f"{name}.parquet").write_bytes(df.to_parquet(index=False))
            written += 1
    return written

def to_int_series(s: pd.Series) -> pd.Series:
    """Safely cast a series to int, dropping NA then returning int dtype."""
    if s is None:
        return pd.Series(dtype="int64")
    return s.dropna().astype(int)

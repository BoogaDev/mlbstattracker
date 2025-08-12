# %% [markdown]
# MLB EDA and Modeling Prep
# 
# Explore MLB datasets loaded by `mlb_stats_etl`, and prepare modeling-ready features to predict outcomes such as:
# - Home team win/loss
# - Total score
# - Player RBIs for an upcoming game

# %%
# Imports and config
import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from dotenv import load_dotenv

sns.set_theme(style="whitegrid")
pd.set_option("display.max_columns", 200)

# Load .env from project root if present and ensure local package import works
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")
from mlb_stats_etl.db import DEFAULT_KEYS

# Toggle sources
USE_DB = True  # set False to load from Parquet files in PARQUET_DIR
PARQUET_DIR = ROOT / "out_full"  # adjust as needed if you wrote elsewhere

DB_URL = os.getenv("DATABASE_URL") or (
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}?charset=utf8mb4"
)
ENGINE = create_engine(DB_URL) if USE_DB else None
print("DB_URL:", DB_URL if USE_DB else "<parquet mode>")

# %% [markdown]
# Schema overview and samples
# Inspect table structures and preview first 10 rows to understand available data.

# %%
MLB_TABLES = sorted(DEFAULT_KEYS.keys())

def describe_db_tables() -> None:
    if not USE_DB:
        print("DB not in use; skipping DB schema introspection.")
        return
    try:
        db_name = pd.read_sql("SELECT DATABASE() AS db", ENGINE)["db"].iloc[0]
        print(f"Using database: {db_name}")
    except Exception as exc:
        print("Could not query database name:", exc)
        return

    try:
        available = pd.read_sql(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name",
            ENGINE,
        )["table_name"].tolist()
    except Exception as exc:
        print("Could not list tables:", exc)
        return

    targets = [t for t in MLB_TABLES if t in available]
    print(f"Found {len(targets)} MLB tables in DB.")

    for t in targets:
        print("\n=== Table:", t, "===")
        try:
            schema_df = pd.read_sql(
                """
                SELECT column_name, data_type, is_nullable, column_key
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = %s
                ORDER BY ordinal_position
                """,
                ENGINE,
                params=(t,),
            )
            print(schema_df)
        except Exception as exc:
            print(f"Failed to describe columns for {t}:", exc)

        try:
            sample_df = pd.read_sql(f"SELECT * FROM `{t}` LIMIT 10", ENGINE)
            print("-- sample rows --")
            print(sample_df)
        except Exception as exc:
            print(f"Failed to sample rows for {t}:", exc)

def describe_parquet_tables() -> None:
    print(f"Inspecting Parquet directory: {PARQUET_DIR}")
    for t in MLB_TABLES:
        p = PARQUET_DIR / f"{t}.parquet"
        if not p.exists():
            continue
        print("\n=== Parquet:", t, "===")
        try:
            df = pd.read_parquet(p)
            print("dtypes:\n", df.dtypes)
            print("-- sample rows --")
            print(df.head(10))
        except Exception as exc:
            print(f"Failed to read {p}:", exc)

if USE_DB:
    describe_db_tables()
else:
    describe_parquet_tables()

# %% [markdown]
# Load core tables
# Adjust SEASON_START and SEASON_END to scope the data volume.

# %%
SEASON_START = 2023
SEASON_END = 2025


def read_table(name: str) -> pd.DataFrame:
    if USE_DB:
        # Apply season filter directly for games; join to games for related detail tables
        if name == "games":
            q = (
                "SELECT * FROM `games` WHERE season BETWEEN %s AND %s"
            )
            return pd.read_sql(q, ENGINE, params=(SEASON_START, SEASON_END))

        if name in {"game_players", "game_teams", "linescores", "plays", "pitches"}:
            q = (
                f"SELECT t.* FROM `{name}` t "
                "JOIN `games` g ON g.gamePk = t.gamePk "
                "WHERE g.season BETWEEN %s AND %s"
            )
            return pd.read_sql(q, ENGINE, params=(SEASON_START, SEASON_END))

        # All other small tables unfiltered
        return pd.read_sql(f"SELECT * FROM `{name}`", ENGINE)
    p = PARQUET_DIR / f"{name}.parquet"
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


teams = read_table("teams")
games = read_table("games")
game_teams = read_table("game_teams")
game_players = read_table("game_players")
linescores = read_table("linescores")
standings = read_table("standings")
print("Shapes:", games.shape, game_teams.shape, game_players.shape, linescores.shape)

# %% [markdown]
# Quick health checks

# %%
print(games.head())
print(game_teams.head())
print(games["status_detailedState"].value_counts(dropna=False).head(10))

# %% [markdown]
# Build game-level dataset
# Join home/away team totals into a single row per game and compute labels.

# %%
home = (
    game_teams.query('side == "home"')
    .rename(columns={"team_id": "home_team_id", "team_name": "home_team_name"})[
        ["gamePk", "home_team_id", "home_team_name", "batters_total_r"]
    ]
)
away = (
    game_teams.query('side == "away"')
    .rename(columns={"team_id": "away_team_id", "team_name": "away_team_name"})[
        ["gamePk", "away_team_id", "away_team_name", "batters_total_r"]
    ]
)

home = home.rename(columns={"batters_total_r": "home_runs"})
away = away.rename(columns={"batters_total_r": "away_runs"})

meta_cols = [
    "gamePk",
    "season",
    "game_type",
    "game_datetime",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
]

gm = games[meta_cols].drop_duplicates("gamePk").copy()
# Align dtype of season for merges
gm["season"] = pd.to_numeric(gm["season"], errors="coerce").astype("Int64")
gm = gm.merge(home[["gamePk", "home_runs"]], on="gamePk", how="left").merge(
    away[["gamePk", "away_runs"]], on="gamePk", how="left"
)

gm["total_runs"] = gm[["home_runs", "away_runs"]].sum(axis=1, skipna=True)
gm["home_win"] = (gm["home_runs"] > gm["away_runs"]).astype("Int64")

print(gm[["home_win", "total_runs"]].describe(include="all"))

# %% [markdown]
# Minimal features
# Use prior season team win percentage as a quick baseline signal.

# %%
stand = standings.rename(columns={"team_id": "stand_team_id", "season": "stand_season"}).copy()
stand["prev_season"] = stand["stand_season"].astype("Int64") - 1

home_feat = (
    stand[["stand_team_id", "prev_season", "pct"]]
    .rename(columns={"stand_team_id": "home_team_id", "prev_season": "season", "pct": "home_prev_pct"})
    .copy()
)
home_feat["season"] = pd.to_numeric(home_feat["season"], errors="coerce").astype("Int64")
away_feat = (
    stand[["stand_team_id", "prev_season", "pct"]]
    .rename(columns={"stand_team_id": "away_team_id", "prev_season": "season", "pct": "away_prev_pct"})
    .copy()
)
away_feat["season"] = pd.to_numeric(away_feat["season"], errors="coerce").astype("Int64")

Xy = (
    gm.merge(home_feat, on=["home_team_id", "season"], how="left")
    .merge(away_feat, on=["away_team_id", "season"], how="left")
    .copy()
)
print(Xy[["home_prev_pct", "away_prev_pct"]].describe())

# %% [markdown]
# Baseline models
# Logistic regression for home win; linear regression for total runs.

# %%
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.metrics import roc_auc_score, accuracy_score, mean_absolute_error

# Classification
cls = Xy.dropna(subset=["home_win", "home_prev_pct", "away_prev_pct"]).copy()
Xc = cls[["home_prev_pct", "away_prev_pct"]].astype(float)
yc = cls["home_win"].astype(int)
Xc_tr, Xc_te, yc_tr, yc_te = train_test_split(
    Xc, yc, test_size=0.2, random_state=42, stratify=yc
)
logr = LogisticRegression(max_iter=1000)
logr.fit(Xc_tr, yc_tr)
probs = logr.predict_proba(Xc_te)[:, 1]
preds = (probs >= 0.5).astype(int)
print("HomeWin AUC:", roc_auc_score(yc_te, probs))
print("HomeWin ACC:", accuracy_score(yc_te, preds))

# Regression
reg = Xy.dropna(subset=["total_runs", "home_prev_pct", "away_prev_pct"]).copy()
Xr = reg[["home_prev_pct", "away_prev_pct"]].astype(float)
yr = reg["total_runs"].astype(float)
Xr_tr, Xr_te, yr_tr, yr_te = train_test_split(Xr, yr, test_size=0.2, random_state=42)
linr = LinearRegression()
linr.fit(Xr_tr, yr_tr)
yhat = linr.predict(Xr_te)
print("TotalRuns MAE:", mean_absolute_error(yr_te, yhat))

# %% [markdown]
# Next steps
# - Rolling form features per team (last 5/10 games).
# - Starting pitcher features (from `game_players` and play-by-play).
# - Weather and venue effects.
# - Home/away splits and rest days.
# - Proper temporal validation (train/test split by date).
# - Player-level targets (e.g., per-player RBI) using `game_players`. 
from __future__ import annotations
import os
from pathlib import Path

# Load .env from project root if available
try:
    from dotenv import load_dotenv
    _ROOT = Path(__file__).resolve().parents[1]
    load_dotenv(_ROOT / ".env")
except Exception:
    pass

# ---- API base ----
BASE_URL: str = os.getenv("MLB_STATS_BASE_URL", "https://statsapi.mlb.com/api")
DEFAULT_VER: str = os.getenv("MLB_STATS_VER", "v1")
GAME_FEED_VER: str = os.getenv("MLB_STATS_GAME_VER", "v1.1")
SPORT_ID: int = int(os.getenv("MLB_SPORT_ID", "1"))

# ---- Concurrency / Throttling ----
MAX_WORKERS: int = int(os.getenv("MLB_MAX_WORKERS", "6"))
REQS_PER_SEC: float = float(os.getenv("MLB_REQS_PER_SEC", "5.0"))  # gentle default

# ---- HTTP timeouts ----
TIMEOUT_SECONDS: float = float(os.getenv("MLB_HTTP_TIMEOUT", "30"))

# ---- Caching (requests-cache) ----
CACHE_ENABLED: bool = os.getenv("MLB_CACHE_ENABLED", "true").lower() in ("1", "true", "yes", "y")
CACHE_PATH: str = os.getenv("MLB_CACHE_PATH", "./mlb_cache.sqlite")
CACHE_TTL_SECONDS: int = int(os.getenv("MLB_CACHE_TTL_SECONDS", str(6 * 60 * 60)))  # 6 hours default

# ---- State path ----
STATE_PATH: str = os.getenv("MLB_STATE_PATH", "./mlb_state.json")

# ---- DB (SingleStore/MySQL) ----
DB_HOST: str = os.getenv("DB_HOST", "108.195.104.154")
DB_PORT: int = int(os.getenv("DB_PORT", "3306"))
DB_DATABASE: str = os.getenv("DB_DATABASE", "MLB")
DB_USER: str = os.getenv("DB_USER", "root")
DB_PASSWORD: str = os.getenv("DB_PASSWORD", "!biA4z6JvBZafh2")

def build_db_url() -> str:
    return f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}?charset=utf8mb4"

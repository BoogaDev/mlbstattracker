from __future__ import annotations
import time, threading, logging
from typing import Any, Dict, Optional
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .config import BASE_URL, TIMEOUT_SECONDS, REQS_PER_SEC, CACHE_ENABLED, CACHE_PATH, CACHE_TTL_SECONDS

try:
    import requests_cache
except Exception:
    requests_cache = None

log = logging.getLogger("mlb_stats_etl.http")

class RateLimiter:
    """Simple token-bucket-like limiter: allow N reqs/second."""
    def __init__(self, reqs_per_sec: float):
        self.min_interval = 1.0 / max(reqs_per_sec, 0.1)
        self.last_time = 0.0

    def wait(self):
        now = time.perf_counter()
        delta = now - self.last_time
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self.last_time = time.perf_counter()

class MLBClient:
    def __init__(self, session: Optional[requests.Session]=None, reqs_per_sec: float=REQS_PER_SEC):
        if session is not None:
            self.sess = session
        else:
            if CACHE_ENABLED and requests_cache is not None:
                self.sess = requests_cache.CachedSession(
                    cache_name=CACHE_PATH,
                    backend="sqlite",
                    allowable_methods=("GET",),
                    stale_if_error=True,
                    expire_after=CACHE_TTL_SECONDS,
                )
            else:
                self.sess = requests.Session()
        self.limiter = RateLimiter(reqs_per_sec)

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
        retry=retry_if_exception_type((requests.RequestException,)),
    )
    def get(self, path: str, params: Optional[Dict[str, Any]]=None, *, timeout: float=TIMEOUT_SECONDS) -> Dict[str, Any]:
        """GET the MLB Stats API at the given path (must begin with '/')."""
        if not path.startswith("/"):
            path = "/" + path
        url = BASE_URL + path
        self.limiter.wait()
        resp = self.sess.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

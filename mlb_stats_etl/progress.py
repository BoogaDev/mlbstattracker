from __future__ import annotations
import logging
from typing import Callable, Dict, Any, Optional

ProgressCallback = Callable[[str, Dict[str, Any]], None]

class Progress:
    """Simple progress event emitter; logs if no callback provided."""
    def __init__(self, callback: Optional[ProgressCallback]=None, logger: Optional[logging.Logger]=None):
        self.callback = callback
        self.log = logger or logging.getLogger("mlb_stats_etl.progress")

    def emit(self, event: str, payload: Optional[Dict[str, Any]] = None, **payload_kwargs: Any) -> None:
        # Accept both positional dict payload and keyword args; merge into a single dict
        merged: Dict[str, Any] = {}
        if payload:
            merged.update(payload)
        if payload_kwargs:
            merged.update(payload_kwargs)
        if self.callback:
            try:
                self.callback(event, merged)
            except Exception:
                # ensure logging continues even if custom callback fails
                self.log.exception("Progress callback error for event=%s", event)
        # Always log
        self.log.info("%s | %s", event, merged)

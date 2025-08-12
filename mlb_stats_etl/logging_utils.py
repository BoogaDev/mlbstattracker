from __future__ import annotations
import logging, os, sys, json
from typing import Optional

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
            "time": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S"),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)

def setup_logging(level: Optional[str]=None, json_mode: Optional[bool]=None) -> None:
    level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    json_mode = json_mode if json_mode is not None else (os.getenv("LOG_JSON", "false").lower() in ("1","true","yes","y"))
    root = logging.getLogger()
    # Clear existing handlers to avoid duplication if reconfigured
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(getattr(logging, level, logging.INFO))
    handler = logging.StreamHandler(sys.stdout)
    if json_mode:
        fmt = JsonFormatter()
    else:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", "%H:%M:%S")
    handler.setFormatter(fmt)
    root.addHandler(handler)

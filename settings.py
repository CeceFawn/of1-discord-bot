# settings.py
from __future__ import annotations
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def _env_path(var_name: str, default_filename: str) -> str:
    raw = (os.getenv(var_name) or "").strip()
    if raw:
        return raw
    return os.path.join(BASE_DIR, default_filename)


CONFIG_PATH = _env_path("CONFIG_PATH", "config.json")
STATE_PATH = _env_path("STATE_PATH", "state.json")
ENV_PATH = _env_path("ENV_PATH", ".env")
LOG_PATH = _env_path("LOG_PATH", "bot.log")
RUNTIME_STATUS_PATH = _env_path("RUNTIME_STATUS_PATH", "runtime_status.json")
RUNTIME_DB_PATH = _env_path("RUNTIME_DB_PATH", "runtime.db")
DEPLOY_STATUS_PATH = _env_path("DEPLOY_STATUS_PATH", "deploy_status.json")
DRIVER_CACHE_PATH = _env_path("DRIVER_CACHE_PATH", "driver_cache.json")

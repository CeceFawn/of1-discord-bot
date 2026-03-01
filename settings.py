# settings.py
from __future__ import annotations
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
STATE_PATH = os.path.join(BASE_DIR, "state.json")
ENV_PATH = os.path.join(BASE_DIR, ".env")
LOG_PATH = os.path.join(BASE_DIR, "bot.log")
RUNTIME_STATUS_PATH = os.path.join(BASE_DIR, "runtime_status.json")
RUNTIME_DB_PATH = os.path.join(BASE_DIR, "runtime.db")
DEPLOY_STATUS_PATH = os.path.join(BASE_DIR, "deploy_status.json")

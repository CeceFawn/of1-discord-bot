# storage.py
from __future__ import annotations
import json
import os
from typing import Any, Dict
from settings import CONFIG_PATH, STATE_PATH, ENV_PATH

def load_json(path: str, fallback: Any) -> Any:
    if not os.path.exists(path):
        return fallback
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json_atomic(path: str, data: Any) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, path)

def load_config() -> Dict[str, Any]:
    return load_json(CONFIG_PATH, fallback={})

def save_config(cfg: Dict[str, Any]) -> None:
    save_json_atomic(CONFIG_PATH, cfg)

def load_state() -> Dict[str, Any]:
    return load_json(STATE_PATH, fallback={})

def save_state(state: Dict[str, Any]) -> None:
    save_json_atomic(STATE_PATH, state)

def set_env_value(key: str, value: str, env_path: str = ENV_PATH) -> None:
    """
    Upsert KEY=VALUE into .env while preserving other lines.
    Also updates os.environ for immediate use.
    """
    lines = []
    found = False

    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip().startswith(f"{key}="):
                    lines.append(f"{key}={value}\n")
                    found = True
                else:
                    lines.append(line)

    if not found:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] = lines[-1] + "\n"
        lines.append(f"{key}={value}\n")

    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(lines)

    os.environ[key] = str(value)

# storage.py
from __future__ import annotations
import json
import os
import threading
from contextlib import contextmanager
from typing import Any, Dict
from settings import CONFIG_PATH, STATE_PATH, ENV_PATH

_FILE_WRITE_LOCK = threading.RLock()

try:
    import fcntl  # type: ignore
except Exception:  # pragma: no cover
    fcntl = None  # type: ignore


@contextmanager
def _interprocess_lock(lock_name: str):
    """
    Best-effort cross-process lock (Linux flock). Falls back to process-local only.
    """
    lock_path = f"{lock_name}.lock"
    fh = None
    try:
        fh = open(lock_path, "a+", encoding="utf-8")
        if fcntl is not None:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            if fh is not None and fcntl is not None:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            if fh is not None:
                fh.close()
        except Exception:
            pass

def _env_quote(value: str) -> str:
    # Always quote to preserve spaces/comments/special characters in .env values.
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    return f'"{escaped}"'

def load_json(path: str, fallback: Any) -> Any:
    with _FILE_WRITE_LOCK, _interprocess_lock(path):
        if not os.path.exists(path):
            return fallback
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            # Try backup if primary is malformed/corrupt.
            bak = f"{path}.bak"
            if os.path.exists(bak):
                try:
                    with open(bak, "r", encoding="utf-8") as f:
                        return json.load(f)
                except Exception:
                    pass
            return fallback

def save_json_atomic(path: str, data: Any) -> None:
    with _FILE_WRITE_LOCK, _interprocess_lock(path):
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
    with _FILE_WRITE_LOCK, _interprocess_lock(env_path):
        lines = []
        found = False
        rendered = _env_quote(value)

        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip().startswith(f"{key}="):
                        lines.append(f"{key}={rendered}\n")
                        found = True
                    else:
                        lines.append(line)

        if not found:
            if lines and not lines[-1].endswith("\n"):
                lines[-1] = lines[-1] + "\n"
            lines.append(f"{key}={rendered}\n")

        with open(env_path, "w", encoding="utf-8") as f:
            f.writelines(lines)

    os.environ[key] = str(value)

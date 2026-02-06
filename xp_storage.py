# xp_storage.py
from __future__ import annotations

import json
import os
import time
import logging
from typing import Any, Dict, Optional, Tuple

# ----------------------------
# File location
# ----------------------------
# Priority:
# 1) env XP_STATE_FILE
# 2) xp_state.json next to bot.py (or wherever this file is)
DEFAULT_PATH = os.path.join(os.path.dirname(__file__), "xp_state.json")


def get_xp_state_path() -> str:
    path = (os.getenv("XP_STATE_FILE") or "").strip()
    return path or DEFAULT_PATH


# ----------------------------
# Shape helpers
# ----------------------------
def _ensure_root_shape(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        data = {}
    if "guilds" not in data or not isinstance(data.get("guilds"), dict):
        data["guilds"] = {}
    return data


def _ensure_guild_shape(state: Dict[str, Any], guild_id: int) -> Dict[str, Any]:
    state = _ensure_root_shape(state)
    gid = str(guild_id)
    g = state["guilds"].get(gid)
    if not isinstance(g, dict):
        g = {}
        state["guilds"][gid] = g
    if "users" not in g or not isinstance(g.get("users"), dict):
        g["users"] = {}
    return g


def _ensure_user_shape(guild_obj: Dict[str, Any], user_id: int) -> Dict[str, Any]:
    uid = str(user_id)
    u = guild_obj["users"].get(uid)
    if not isinstance(u, dict):
        u = {}
        guild_obj["users"][uid] = u

    # Defaults
    u.setdefault("xp", 0)
    u.setdefault("level", 0)
    u.setdefault("last_msg_ts", 0)   # unix seconds
    u.setdefault("messages", 0)

    # Per-user cosmetic settings for rank cards (optional)
    # You can expand later without breaking old data.
    if "card" not in u or not isinstance(u.get("card"), dict):
        u["card"] = {
            "bg_url": None,     # string or None
            "accent": None,     # "#RRGGBB" or None
            "tagline": None     # string or None
        }
    return u


# ----------------------------
# Load / Save (atomic)
# ----------------------------
def load_xp_state(path: Optional[str] = None) -> Dict[str, Any]:
    path = path or get_xp_state_path()
    try:
        if not os.path.exists(path):
            # Start fresh with correct shape
            return {"guilds": {}}

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return _ensure_root_shape(data)

    except Exception as e:
        logging.error(f"[XP] Failed to load xp state from {path}: {e}")
        # Fail-safe: return empty shaped state
        return {"guilds": {}}


def save_xp_state(state: Dict[str, Any], path: Optional[str] = None) -> None:
    path = path or get_xp_state_path()
    state = _ensure_root_shape(state)

    tmp_path = f"{path}.tmp"
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)

        # Atomic replace on most OSes
        os.replace(tmp_path, path)

    except Exception as e:
        logging.error(f"[XP] Failed to save xp state to {path}: {e}")
        # Best-effort cleanup
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


# ----------------------------
# Public API: getters/setters
# ----------------------------
def get_user_record(
    state: Dict[str, Any],
    guild_id: int,
    user_id: int
) -> Dict[str, Any]:
    """
    Returns the live dict record for the user (mutating it will mutate `state`).
    Ensures all shapes/defaults exist.
    """
    g = _ensure_guild_shape(state, guild_id)
    return _ensure_user_shape(g, user_id)


def set_user_xp_level(
    state: Dict[str, Any],
    guild_id: int,
    user_id: int,
    xp: int,
    level: int,
) -> None:
    u = get_user_record(state, guild_id, user_id)
    u["xp"] = int(max(0, xp))
    u["level"] = int(max(0, level))


def add_user_xp(
    state: Dict[str, Any],
    guild_id: int,
    user_id: int,
    delta_xp: int,
) -> int:
    """
    Adds xp and returns the new total xp.
    Does NOT compute level — keep that logic in your xp system module.
    """
    u = get_user_record(state, guild_id, user_id)
    u["xp"] = int(max(0, int(u.get("xp", 0)) + int(delta_xp)))
    return int(u["xp"])


def update_user_message_meta(
    state: Dict[str, Any],
    guild_id: int,
    user_id: int,
    now_ts: Optional[int] = None,
) -> None:
    u = get_user_record(state, guild_id, user_id)
    now_ts = int(now_ts if now_ts is not None else time.time())
    u["last_msg_ts"] = now_ts
    u["messages"] = int(u.get("messages", 0)) + 1


def set_user_card_prefs(
    state: Dict[str, Any],
    guild_id: int,
    user_id: int,
    *,
    bg_url: Optional[str] = None,
    accent: Optional[str] = None,
    tagline: Optional[str] = None,
) -> None:
    u = get_user_record(state, guild_id, user_id)
    card = u.get("card") or {}
    if bg_url is not None:
        card["bg_url"] = bg_url
    if accent is not None:
        card["accent"] = accent
    if tagline is not None:
        card["tagline"] = tagline
    u["card"] = card


def reset_user(
    state: Dict[str, Any],
    guild_id: int,
    user_id: int,
) -> None:
    g = _ensure_guild_shape(state, guild_id)
    uid = str(user_id)
    if uid in g["users"]:
        del g["users"][uid]


def get_guild_users(state: Dict[str, Any], guild_id: int) -> Dict[str, Any]:
    """
    Returns the dict of user_id(str) -> record for this guild.
    """
    g = _ensure_guild_shape(state, guild_id)
    return g["users"]


def get_top_users_by_xp(
    state: Dict[str, Any],
    guild_id: int,
    limit: int = 10
) -> list[Tuple[str, int, int]]:
    """
    Returns [(user_id_str, xp, level), ...] sorted by xp desc.
    """
    users = get_guild_users(state, guild_id)
    rows: list[Tuple[str, int, int]] = []

    for uid, rec in users.items():
        if not isinstance(rec, dict):
            continue
        xp = int(rec.get("xp", 0) or 0)
        lvl = int(rec.get("level", 0) or 0)
        rows.append((str(uid), xp, lvl))

    rows.sort(key=lambda x: x[1], reverse=True)
    return rows[: max(1, int(limit))]


# ----------------------------
# Cooldown helper (optional)
# ----------------------------
def is_on_cooldown(
    state: Dict[str, Any],
    guild_id: int,
    user_id: int,
    cooldown_seconds: int,
    now_ts: Optional[int] = None,
) -> bool:
    """
    True if user last_msg_ts is within cooldown window.
    """
    now_ts = int(now_ts if now_ts is not None else time.time())
    u = get_user_record(state, guild_id, user_id)
    last_ts = int(u.get("last_msg_ts", 0) or 0)
    return (now_ts - last_ts) < int(max(0, cooldown_seconds))
# ----------------------------
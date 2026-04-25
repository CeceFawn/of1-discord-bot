from __future__ import annotations

import os
import re
import json
import time
import logging
from logging.handlers import RotatingFileHandler
import asyncio
import random
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Optional, Any, List, Tuple
from collections import deque

import discord
from discord.ext import commands
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import aiohttp

# Load env early for runtime config.
load_dotenv()

from storage import load_config, save_config, load_state, save_state, set_env_value
from settings import LOG_PATH, RUNTIME_STATUS_PATH
from runtime_store import (
    init_runtime_db,
    upsert_runtime_status,
    insert_alert,
    migrate_alerts_from_state_json,
)

import io
from PIL import Image, ImageDraw, ImageFont # type: ignore

_RANK_FONT_CACHE: Optional[Tuple[ImageFont.FreeTypeFont | ImageFont.ImageFont, ImageFont.FreeTypeFont | ImageFont.ImageFont, ImageFont.FreeTypeFont | ImageFont.ImageFont]] = None
_RANK_TEMPLATE_CACHE: Dict[str, Image.Image] = {}
_RANK_COCKPIT_CACHE: Optional[Image.Image] = None

# Maps user-facing background key → asset path.
# Add new entries here as you create more background PNGs.
_RANK_BACKGROUNDS: Dict[str, str] = {
    "default": "assets/rank_template.png",
}

def rank_available_backgrounds() -> List[str]:
    """Return background keys that have an existing asset file."""
    return [k for k, path in _RANK_BACKGROUNDS.items() if os.path.exists(path)]

def _rank_fonts():
    global _RANK_FONT_CACHE
    if _RANK_FONT_CACHE is not None:
        return _RANK_FONT_CACHE
    try:
        _RANK_FONT_CACHE = (
            ImageFont.truetype("fonts/Inter-SemiBold.ttf", 36),
            ImageFont.truetype("fonts/Inter-Regular.ttf", 22),
            ImageFont.truetype("fonts/Inter-Regular.ttf", 18),
        )
    except Exception:
        _RANK_FONT_CACHE = (
            ImageFont.load_default(),
            ImageFont.load_default(),
            ImageFont.load_default(),
        )
    return _RANK_FONT_CACHE

def _rank_bg_image(bg_key: str | None, w: int, h: int) -> Image.Image:
    """Load and cache a background image by key. Falls back to solid color."""
    path = _RANK_BACKGROUNDS.get(bg_key or "default") if bg_key else None
    if not path:
        path = _RANK_BACKGROUNDS.get("default")
    if not path or not os.path.exists(path):
        return Image.new("RGBA", (w, h), (24, 26, 32, 255))
    cached = _RANK_TEMPLATE_CACHE.get(path)
    if cached is None:
        try:
            cached = Image.open(path).convert("RGBA").resize((w, h), Image.LANCZOS)
            _RANK_TEMPLATE_CACHE[path] = cached
        except Exception:
            return Image.new("RGBA", (w, h), (24, 26, 32, 255))
    return cached.copy()

def _rank_cockpit_overlay(w: int, h: int) -> Optional[Image.Image]:
    """Load and cache the cockpit overlay PNG. Returns None if file missing."""
    global _RANK_COCKPIT_CACHE
    if _RANK_COCKPIT_CACHE is not None:
        return _RANK_COCKPIT_CACHE.copy()
    path = "assets/rank_template_cockpit.png"
    if not os.path.exists(path):
        return None
    try:
        _RANK_COCKPIT_CACHE = Image.open(path).convert("RGBA").resize((w, h), Image.LANCZOS)
        return _RANK_COCKPIT_CACHE.copy()
    except Exception:
        return None

# ----------------------------
# Rank card builder
# ----------------------------
async def fetch_avatar_image(member: discord.Member, size: int = 256) -> Image.Image:
    """
    Returns a Pillow Image of the member's avatar.
    """
    asset = member.display_avatar.replace(size=size, static_format="png")
    data = await asset.read()
    im = Image.open(io.BytesIO(data)).convert("RGBA")
    return im

def circle_crop(im: Image.Image, out_size: int) -> Image.Image:
    """
    Resizes and circle-crops an RGBA image to out_size x out_size.
    """
    im = im.resize((out_size, out_size), Image.LANCZOS).convert("RGBA")
    mask = Image.new("L", (out_size, out_size), 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((0, 0, out_size - 1, out_size - 1), fill=255)

    out = Image.new("RGBA", (out_size, out_size), (0, 0, 0, 0))
    out.paste(im, (0, 0), mask)
    return out

def draw_progress_bar(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, pct: float):
    """
    Draws a rounded-ish bar (simple rectangle + fill).
    pct: 0..1
    """
    pct = max(0.0, min(1.0, pct))
    # background
    draw.rounded_rectangle((x, y, x+w, y+h), radius=h//2, fill=(60, 60, 70, 255))
    # fill
    fill_w = int(w * pct)
    if fill_w > 0:
        draw.rounded_rectangle((x, y, x+fill_w, y+h), radius=h//2, fill=(120, 200, 255, 255))

async def build_rank_card_png(
    member: discord.Member,
    level: int,
    xp: int,
    xp_next: int,
    title: str = "Rookie",
    template_path: str | None = None,  # kept for backwards compat, ignored if bg_key set
    bg_key: str | None = None,
    server_rank: int | None = None,
) -> bytes:
    """
    Returns PNG bytes for a rank card.
    Layer order: background → avatar (circle, left) → cockpit overlay → text/XP bar.
    """
    W, H = 900, 260

    # 1) Background
    base = _rank_bg_image(bg_key, W, H)

    # 2) Avatar — circle-cropped, positioned left (behind cockpit viewport)
    avatar = await fetch_avatar_image(member, size=256)
    avatar = circle_crop(avatar, 170)
    base.paste(avatar, (35, 45), avatar)

    # 3) Cockpit overlay — composited on top with full alpha transparency support
    cockpit = _rank_cockpit_overlay(W, H)
    if cockpit is not None:
        base.alpha_composite(cockpit)

    draw = ImageDraw.Draw(base)

    # 4) Fonts
    font_name, font_small, font_tiny = _rank_fonts()

    # 5) Text
    username = member.display_name
    draw.text((230, 45), username, font=font_name, fill=(240, 240, 245, 255))
    draw.text((230, 95), f"Title: {title}", font=font_small, fill=(180, 185, 195, 255))
    draw.text((730, 45), f"LVL {level}", font=font_name, fill=(120, 200, 255, 255))
    if server_rank is not None:
        draw.text((730, 95), f"#{server_rank}", font=font_small, fill=(180, 185, 195, 255))

    # 6) XP bar + numbers
    pct = 0.0 if xp_next <= 0 else (xp / xp_next)
    bar_x, bar_y, bar_w, bar_h = 230, 150, 635, 26
    draw_progress_bar(draw, bar_x, bar_y, bar_w, bar_h, pct)

    draw.text((230, 185), f"XP: {xp} / {xp_next}", font=font_tiny, fill=(180, 185, 195, 255))

    # 7) Export to bytes
    out = io.BytesIO()
    base.save(out, format="PNG")
    return out.getvalue()


# ✅ XP storage module (make sure xp_storage.py is next to bot.py)
from xp_storage import (
    get_xp_state_path,
    load_xp_state,
    save_xp_state,
    get_user_record,
    add_user_xp,
    set_user_xp_level,
    update_user_message_meta,
    is_on_cooldown,
    get_top_users_by_xp,
    set_user_card_prefs,
)

# ----------------------------
# Logging
# ----------------------------
class _JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def _configure_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Avoid duplicate handlers on reload/import quirks.
    for h in list(root.handlers):
        root.removeHandler(h)

    max_bytes = int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024)) or (5 * 1024 * 1024))
    backups = int(os.getenv("LOG_BACKUP_COUNT", "5") or 5)
    fh = RotatingFileHandler(LOG_PATH, maxBytes=max(1024 * 1024, max_bytes), backupCount=max(1, backups), encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(_JsonLineFormatter())
    root.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    root.addHandler(sh)


_configure_logging()

# ----------------------------
# Config + State (global in-memory)
# ----------------------------
CFG: Dict[str, Any] = {}
STATE: Dict[str, Any] = {}

ROLE_MAP_REACTION: Dict[str, str] = {}
ROLE_MAP_COLOR: Dict[str, str] = {}
ROLE_MAP_DRIVER: Dict[str, str] = {}
COLOR_ROLE_NAMES_CACHE: set[str] = set()

def _rebuild_role_caches() -> None:
    global ROLE_MAP_REACTION, ROLE_MAP_COLOR, ROLE_MAP_DRIVER, COLOR_ROLE_NAMES_CACHE
    rr = CFG.get("reaction_roles") or {}
    cr = CFG.get("color_roles") or {}
    driver = ((STATE.get("driver_roles") or {}).get("emoji_to_role")) or {}

    ROLE_MAP_REACTION = dict(rr) if isinstance(rr, dict) else {}
    ROLE_MAP_COLOR = dict(cr) if isinstance(cr, dict) else {}
    ROLE_MAP_DRIVER = dict(driver) if isinstance(driver, dict) else {}
    COLOR_ROLE_NAMES_CACHE = set(ROLE_MAP_COLOR.values())

def reload_config_state() -> None:
    global CFG, STATE
    CFG = load_config() or {}
    STATE = load_state() or {}
    _rebuild_role_caches()

# Load once at import time
reload_config_state()

# ----------------------------
# XP State (global in-memory)
# ----------------------------
XP_STATE: Dict[str, Any] = load_xp_state()
XP_DIRTY: bool = False

# ----------------------------
# Command Log (in-memory ring buffer)
# ----------------------------
import collections as _collections
_CMD_LOG: _collections.deque = _collections.deque(maxlen=300)
XP_SAVE_LOCK = asyncio.Lock()
XP_FLUSH_TASK: Optional[asyncio.Task] = None

PERIODIC_ROLE_RECOVERY_TASK: Optional[asyncio.Task] = None
RACE_SUPERVISOR_TASK: Optional[asyncio.Task] = None
RUNTIME_STATUS_TASK: Optional[asyncio.Task] = None
DRIVER_CACHE_VALIDATION_TASK: Optional[asyncio.Task] = None
LOOP_HEARTBEATS: Dict[str, str] = {}
LOOP_ERRORS: Dict[str, int] = {}


def _loop_tick(name: str) -> None:
    LOOP_HEARTBEATS[str(name)] = datetime.now(timezone.utc).isoformat()


def _loop_error(name: str) -> None:
    key = str(name)
    LOOP_ERRORS[key] = int(LOOP_ERRORS.get(key, 0) or 0) + 1

def _xp_mark_dirty() -> None:
    global XP_DIRTY
    XP_DIRTY = True

async def _send_staff_alert(msg: str) -> None:
    """Send an alert to the configured staff_alert_channel_id, if set."""
    channel_id = int(CFG.get("staff_alert_channel_id", 0) or 0)
    if not channel_id:
        return
    try:
        ch = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
        await ch.send(f"⚠️ **Bot Alert:** {msg}")
    except Exception as e:
        logging.warning(f"[Alert] Could not send staff alert: {e}")


async def xp_flush_loop():
    """Periodic XP flush so we don't write on every message."""
    global XP_DIRTY
    await bot.wait_until_ready()
    consecutive_failures = 0
    while not bot.is_closed():
        _loop_tick("xp_flush")
        try:
            await asyncio.sleep(int(os.getenv("XP_FLUSH_SECONDS", "30")))
            if not XP_DIRTY:
                continue
            async with XP_SAVE_LOCK:
                if XP_DIRTY:
                    await asyncio.to_thread(save_xp_state, XP_STATE)
                    XP_DIRTY = False
                    consecutive_failures = 0
        except Exception as e:
            _loop_error("xp_flush")
            logging.error(f"[XP] Flush loop error: {e}")
            consecutive_failures += 1
            if consecutive_failures >= 3:
                await _send_staff_alert(f"XP flush has failed {consecutive_failures} times in a row: `{e}`")

# ----------------------------
# Instagram scrape
# ----------------------------
_INSTAGRAM_FAIL_COUNT: Dict[str, int] = {}

def fetch_latest_instagram_post(username: str) -> Optional[str]:
    try:
        url = f"https://www.instagram.com/{username}/"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            _INSTAGRAM_FAIL_COUNT[username] = _INSTAGRAM_FAIL_COUNT.get(username, 0) + 1
            n = _INSTAGRAM_FAIL_COUNT[username]
            if n >= 3:
                logging.warning(f"[Instagram] Scraping @{username} failed {n} time(s) in a row (HTTP {response.status_code})")
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        scripts = soup.find_all("script", type="text/javascript")
        for script in scripts:
            if "window._sharedData" in script.text:
                shortcode = re.search(r'"shortcode":"(.*?)"', script.text)
                if shortcode:
                    _INSTAGRAM_FAIL_COUNT[username] = 0
                    return f"https://www.instagram.com/p/{shortcode.group(1)}/"
        _INSTAGRAM_FAIL_COUNT[username] = _INSTAGRAM_FAIL_COUNT.get(username, 0) + 1
        n = _INSTAGRAM_FAIL_COUNT[username]
        if n >= 3:
            logging.warning(f"[Instagram] Scraping @{username} found no post data {n} time(s) in a row")
        return None
    except Exception as e:
        _INSTAGRAM_FAIL_COUNT[username] = _INSTAGRAM_FAIL_COUNT.get(username, 0) + 1
        n = _INSTAGRAM_FAIL_COUNT[username]
        if n >= 3:
            logging.warning(f"[Instagram] Scraping @{username} raised exception {n} time(s) in a row: {e}")
        return None

# ----------------------------
# F1 data providers
# ----------------------------
OPENF1_BASE = "https://api.openf1.org/v1"
ERGAST_DRIVER_URL = "https://ergast.com/api/f1/current/driverStandings.json"
ERGAST_CONSTRUCTOR_URL = "https://ergast.com/api/f1/current/constructorStandings.json"
ERGAST_SCHEDULE_URL = "https://ergast.com/api/f1/current.json"
JOLPICA_DRIVER_URL = "https://api.jolpi.ca/ergast/f1/current/driverStandings.json"
JOLPICA_CONSTRUCTOR_URL = "https://api.jolpi.ca/ergast/f1/current/constructorStandings.json"
JOLPICA_SCHEDULE_URL = "https://api.jolpi.ca/ergast/f1/current.json"

F1_SCHEDULE_CACHE: Dict[str, Any] = {"ts": 0.0, "races": [], "fail_until": 0.0, "fail_count": 0}
F1_REMINDER_TASK: Optional[asyncio.Task] = None
F1_QUIZ_ACTIVE: Dict[int, Dict[str, Any]] = {}

SESSION_LABELS = {
    "FirstPractice": "FP1",
    "SecondPractice": "FP2",
    "ThirdPractice": "FP3",
    "Qualifying": "Qualifying",
    "Sprint": "Sprint",
    "SprintQualifying": "Sprint Qualifying",
    "SprintShootout": "Sprint Shootout",
    "Race": "Race",
}

F1_CIRCUITS_FILE = os.path.join(os.path.dirname(__file__), "f1_circuits.json")
F1_QUIZ_FILE = os.path.join(os.path.dirname(__file__), "f1_quiz.json")
CIRCUIT_INFO: Dict[str, Dict[str, Any]] = {}
CIRCUIT_ALIASES: Dict[str, str] = {}
F1_QUIZ_QUESTIONS: List[Dict[str, Any]] = []
QUIZ_DIFFICULTY_POINTS = {
    "easy": 1,
    "medium": 2,
    "hard": 3,
    "expert": 5,
}

def load_f1_static_data() -> None:
    global CIRCUIT_INFO, CIRCUIT_ALIASES, F1_QUIZ_QUESTIONS
    try:
        with open(F1_CIRCUITS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        circuits = data.get("circuits") if isinstance(data, dict) else None
        aliases = data.get("aliases") if isinstance(data, dict) else None
        CIRCUIT_INFO = dict(circuits) if isinstance(circuits, dict) else {}
        CIRCUIT_ALIASES = dict(aliases) if isinstance(aliases, dict) else {}
    except Exception as e:
        logging.error(f"[F1Data] Failed loading circuits file: {e}")
        CIRCUIT_INFO = {}
        CIRCUIT_ALIASES = {}

    try:
        with open(F1_QUIZ_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            F1_QUIZ_QUESTIONS = [x for x in data if isinstance(x, dict) and x.get("q")]
        elif isinstance(data, dict) and isinstance(data.get("questions"), list):
            F1_QUIZ_QUESTIONS = [x for x in data["questions"] if isinstance(x, dict) and x.get("q")]
        else:
            F1_QUIZ_QUESTIONS = []
    except Exception as e:
        logging.error(f"[F1Data] Failed loading quiz file: {e}")
        F1_QUIZ_QUESTIONS = []

def _get_json(url: str):
    r = requests.get(url, timeout=20, headers={"User-Agent": "OF1-Discord-Bot"})
    r.raise_for_status()
    return r.json()

def _get_json_any(urls: List[str], label: str = "api") -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for url in urls:
        try:
            return _get_json(url)
        except Exception as e:
            last_exc = e
            logging.warning(f"[F1] {label} fetch failed for {url}: {e}")
    if last_exc:
        raise last_exc
    raise RuntimeError(f"No {label} URLs configured.")

_OPENF1_TOKEN_CACHE: Dict[str, Any] = {"token": "", "expires_at": 0.0, "fetched_at": 0.0}
_OPENF1_TOKEN_LOCK = threading.RLock()
_OPENF1_TRACE_LOCK = threading.RLock()
_OPENF1_TRACE: Dict[str, Any] = {"window_start": time.time(), "rows": {}}
_OPENF1_AUTH_RETRY_AFTER_TS: float = 0.0
_OPENF1_ENDPOINT_COOLDOWN: Dict[str, float] = {}
_OPENF1_CANDIDATE_SESSIONS_CACHE: Dict[str, Any] = {"ts": 0.0, "keys": []}
# In-memory copy of driver_cache.json.  Loaded once on first access, then kept
# in sync with disk via _save_driver_cache().
_DRIVER_CACHE: Dict[str, Any] = {}
_DRIVER_CACHE_LOADED: bool = False


def _openf1_set_endpoint_cooldown(endpoint: str, seconds: int) -> None:
    sec = max(1, min(1800, int(seconds)))
    _OPENF1_ENDPOINT_COOLDOWN[str(endpoint or "").strip().lower()] = time.time() + sec


def _openf1_get_endpoint_cooldown_remaining(endpoint: str) -> int:
    until = float(_OPENF1_ENDPOINT_COOLDOWN.get(str(endpoint or "").strip().lower(), 0.0) or 0.0)
    remaining = int(until - time.time())
    return max(0, remaining)


def _openf1_trace_record(caller: str, endpoint: str, status_code: int, latency_ms: int) -> None:
    now_ts = time.time()
    with _OPENF1_TRACE_LOCK:
        rows = _OPENF1_TRACE.setdefault("rows", {})
        key = f"{caller}|{endpoint}|{status_code}"
        row = rows.get(key)
        if not isinstance(row, dict):
            row = {"count": 0, "lat_ms_sum": 0}
            rows[key] = row
        row["count"] = int(row.get("count", 0) or 0) + 1
        row["lat_ms_sum"] = int(row.get("lat_ms_sum", 0) or 0) + int(max(0, latency_ms))

        window_start = float(_OPENF1_TRACE.get("window_start", now_ts) or now_ts)
        if (now_ts - window_start) < 60.0:
            return

        total_calls = 0
        total_429 = 0
        summary_rows = []
        for raw_key, stats in rows.items():
            try:
                c, e, s = str(raw_key).split("|", 2)
            except Exception:
                continue
            cnt = int((stats or {}).get("count", 0) or 0)
            lat_sum = int((stats or {}).get("lat_ms_sum", 0) or 0)
            avg_ms = int(lat_sum / cnt) if cnt > 0 else 0
            total_calls += cnt
            if s == "429":
                total_429 += cnt
            summary_rows.append((cnt, c, e, s, avg_ms))

        summary_rows.sort(reverse=True)
        top = summary_rows[:8]
        top_txt = ", ".join([f"{c}:{e}:{s} x{cnt} avg{avg}ms" for cnt, c, e, s, avg in top]) or "none"
        level_fn = logging.warning if total_429 > 0 else logging.info
        level_fn(f"[OpenF1Trace] 60s total={total_calls} 429s={total_429} top={top_txt}")

        _OPENF1_TRACE["window_start"] = now_ts
        _OPENF1_TRACE["rows"] = {}


def _json_path_get(obj: Any, path: str) -> Any:
    cur = obj
    for part in [p for p in str(path or "").split(".") if p]:
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def _openf1_fetch_login_token() -> tuple[str, float]:
    auth_url = str(os.getenv("OPENF1_AUTH_URL") or "").strip()
    username = str(os.getenv("OPENF1_AUTH_USERNAME") or "").strip()
    password = str(os.getenv("OPENF1_AUTH_PASSWORD") or "").strip()
    if not auth_url or not username or not password:
        return "", 0.0

    user_field = str(os.getenv("OPENF1_AUTH_USERNAME_FIELD") or "username").strip() or "username"
    pass_field = str(os.getenv("OPENF1_AUTH_PASSWORD_FIELD") or "password").strip() or "password"
    token_key = str(os.getenv("OPENF1_AUTH_TOKEN_JSON_KEY") or "access_token").strip() or "access_token"
    expires_in_key = str(os.getenv("OPENF1_AUTH_EXPIRES_IN_JSON_KEY") or "expires_in").strip() or "expires_in"
    expires_at_key = str(os.getenv("OPENF1_AUTH_EXPIRES_AT_JSON_KEY") or "").strip()

    payload: Dict[str, Any] = {user_field: username, pass_field: password}
    extra_raw = str(os.getenv("OPENF1_AUTH_EXTRA_JSON") or "").strip()
    if extra_raw:
        try:
            extra = json.loads(extra_raw)
            if isinstance(extra, dict):
                payload.update(extra)
        except Exception:
            logging.warning("[OpenF1Auth] OPENF1_AUTH_EXTRA_JSON is not valid JSON.")

    req_headers: Dict[str, str] = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "User-Agent": "OF1-Discord-Bot",
    }
    hdr_raw = str(os.getenv("OPENF1_AUTH_HEADERS_JSON") or "").strip()
    if hdr_raw:
        try:
            h = json.loads(hdr_raw)
            if isinstance(h, dict):
                req_headers.update({str(k): str(v) for k, v in h.items()})
        except Exception:
            logging.warning("[OpenF1Auth] OPENF1_AUTH_HEADERS_JSON is not valid JSON.")

    r = requests.post(auth_url, data=payload, headers=req_headers, timeout=20)
    r.raise_for_status()
    body = r.json() if r.content else {}
    token = str(_json_path_get(body, token_key) or "").strip()
    if not token:
        raise RuntimeError("auth response missing token")

    now_ts = time.time()
    expires_at = 0.0
    if expires_at_key:
        raw_exp_at = _json_path_get(body, expires_at_key)
        if isinstance(raw_exp_at, (int, float)):
            expires_at = float(raw_exp_at)
        elif isinstance(raw_exp_at, str) and raw_exp_at.strip():
            try:
                expires_at = datetime.fromisoformat(raw_exp_at.replace("Z", "+00:00")).timestamp()
            except Exception:
                expires_at = 0.0
    if expires_at <= 0.0:
        raw_exp_in = _json_path_get(body, expires_in_key)
        try:
            exp_in = int(raw_exp_in)
        except Exception:
            exp_in = 3600
        exp_in = max(60, exp_in)
        expires_at = now_ts + exp_in

    return token, expires_at


def _openf1_get_bearer_token(force_refresh: bool = False) -> str:
    global _OPENF1_AUTH_RETRY_AFTER_TS
    static_bearer = str(os.getenv("OPENF1_BEARER_TOKEN") or "").strip()
    if static_bearer:
        return static_bearer

    with _OPENF1_TOKEN_LOCK:
        now_ts = time.time()
        if now_ts < float(_OPENF1_AUTH_RETRY_AFTER_TS or 0.0):
            token = str(_OPENF1_TOKEN_CACHE.get("token") or "")
            expires_at = float(_OPENF1_TOKEN_CACHE.get("expires_at") or 0.0)
            return token if token and now_ts < expires_at else ""
        token = str(_OPENF1_TOKEN_CACHE.get("token") or "")
        expires_at = float(_OPENF1_TOKEN_CACHE.get("expires_at") or 0.0)
        if (not force_refresh) and token and (now_ts < (expires_at - 120.0)):
            return token
        try:
            new_token, new_expires_at = _openf1_fetch_login_token()
        except Exception as e:
            logging.warning(f"[OpenF1Auth] token refresh failed: {e}")
            err = str(e)
            if "422" in err:
                _OPENF1_AUTH_RETRY_AFTER_TS = now_ts + 60.0
            elif "429" in err:
                _OPENF1_AUTH_RETRY_AFTER_TS = now_ts + 120.0
            elif "503" in err:
                _OPENF1_AUTH_RETRY_AFTER_TS = now_ts + 30.0
            else:
                _OPENF1_AUTH_RETRY_AFTER_TS = now_ts + 15.0
            return token if token and now_ts < expires_at else ""
        if not new_token:
            _OPENF1_AUTH_RETRY_AFTER_TS = now_ts + 30.0
            return token if token and now_ts < expires_at else ""
        _OPENF1_AUTH_RETRY_AFTER_TS = 0.0
        _OPENF1_TOKEN_CACHE["token"] = new_token
        _OPENF1_TOKEN_CACHE["expires_at"] = float(new_expires_at)
        _OPENF1_TOKEN_CACHE["fetched_at"] = now_ts
        return new_token


def _openf1_auth_headers(force_refresh: bool = False) -> Dict[str, str]:
    headers: Dict[str, str] = {"User-Agent": "OF1-Discord-Bot"}
    api_key = str(os.getenv("OPENF1_API_KEY") or "").strip()
    if api_key:
        headers["x-api-key"] = api_key

    bearer = _openf1_get_bearer_token(force_refresh=force_refresh)
    if bearer:
        auth_header_name = str(os.getenv("OPENF1_AUTH_HEADER_NAME") or "Authorization").strip() or "Authorization"
        auth_prefix = str(os.getenv("OPENF1_AUTH_HEADER_PREFIX") or "Bearer").strip()
        headers[auth_header_name] = f"{auth_prefix} {bearer}".strip()
    return headers


def _openf1_get_json(
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 20,
    caller: str = "unknown",
) -> Any:
    cooldown_s = _openf1_get_endpoint_cooldown_remaining(endpoint)
    if cooldown_s > 0:
        raise RuntimeError(f"OpenF1 endpoint cooldown active for {endpoint} ({cooldown_s}s)")

    url = f"{OPENF1_BASE}/{endpoint.lstrip('/')}"
    t0 = time.time()
    r = requests.get(url, params=params or {}, timeout=timeout, headers=_openf1_auth_headers())
    if r.status_code in (401, 403):
        r = requests.get(url, params=params or {}, timeout=timeout, headers=_openf1_auth_headers(force_refresh=True))
    latency_ms = int((time.time() - t0) * 1000)
    _openf1_trace_record(caller=str(caller or "unknown"), endpoint=str(endpoint or ""), status_code=int(r.status_code), latency_ms=latency_ms)
    if int(r.status_code) == 429:
        retry_after = int(r.headers.get("Retry-After", "60") or 60)
        _openf1_set_endpoint_cooldown(endpoint, retry_after)
    elif int(r.status_code) == 503:
        _openf1_set_endpoint_cooldown(endpoint, 15)
    r.raise_for_status()
    return r.json()

def _parse_openf1_dt(dt_raw: Any) -> Optional[datetime]:
    if not dt_raw:
        return None
    try:
        return datetime.fromisoformat(str(dt_raw).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def _dt_to_ergast_parts(dt: Optional[datetime]) -> Tuple[str, str]:
    if dt is None:
        return "", ""
    u = dt.astimezone(timezone.utc)
    return u.strftime("%Y-%m-%d"), u.strftime("%H:%M:%SZ")

load_f1_static_data()

def _state_bucket(key: str) -> Dict[str, Any]:
    global STATE
    bucket = STATE.get(key)
    if not isinstance(bucket, dict):
        bucket = {}
        STATE[key] = bucket
    return bucket

def _save_state_quiet() -> None:
    try:
        save_state(STATE)
    except Exception as e:
        logging.error(f"[State] save_state failed: {e}")

ALERT_RATE_LIMIT: Dict[str, float] = {}

def _record_alert(
    kind: str,
    message: str,
    guild_id: Optional[int] = None,
    user_id: Optional[int] = None,
    persist: bool = True,
) -> None:
    try:
        if not persist:
            return
        now_ts = time.time()
        dedupe_key = f"{str(kind)}|{int(guild_id or 0)}|{int(user_id or 0)}|{str(message or '')[:80]}"
        prev_ts = float(ALERT_RATE_LIMIT.get(dedupe_key, 0.0) or 0.0)
        if (now_ts - prev_ts) < 30.0:
            return
        ALERT_RATE_LIMIT[dedupe_key] = now_ts
        if len(ALERT_RATE_LIMIT) > 2000:
            cutoff = now_ts - 3600.0
            for k, v in list(ALERT_RATE_LIMIT.items()):
                if float(v) < cutoff:
                    ALERT_RATE_LIMIT.pop(k, None)
        insert_alert(
            ts=datetime.now(timezone.utc).isoformat(),
            kind=str(kind or "info"),
            message=str(message or "").strip()[:500],
            guild_id=int(guild_id or 0),
            user_id=int(user_id or 0),
        )
    except Exception as e:
        logging.warning(f"[Alert] Failed to insert alert: {e}")

def _clean_text_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()

# --- Driver name resolution ---
_DRIVER_ACRONYMS: Dict[str, str] = {
    "ver": "verstappen", "nor": "norris", "lec": "leclerc",
    "pia": "piastri", "sai": "sainz", "ham": "hamilton",
    "rus": "russell", "per": "perez", "alo": "alonso",
    "str": "stroll", "gas": "gasly", "oco": "ocon",
    "hul": "hulkenberg", "mag": "magnussen", "tsu": "tsunoda",
    "law": "lawson", "col": "colapinto", "bea": "bearman",
    "ant": "antonelli", "had": "hadjar", "bor": "bortoleto",
    "doo": "doohan",
}

_DRIVER_ALIASES: Dict[str, str] = {
    # full names
    "max verstappen": "verstappen", "lando norris": "norris",
    "charles leclerc": "leclerc", "oscar piastri": "piastri",
    "carlos sainz": "sainz", "lewis hamilton": "hamilton",
    "george russell": "russell", "sergio perez": "perez",
    "fernando alonso": "alonso", "lance stroll": "stroll",
    "pierre gasly": "gasly", "esteban ocon": "ocon",
    "nico hulkenberg": "hulkenberg", "kevin magnussen": "magnussen",
    "yuki tsunoda": "tsunoda", "liam lawson": "lawson",
    "franco colapinto": "colapinto", "oliver bearman": "bearman",
    "kimi antonelli": "antonelli", "isack hadjar": "hadjar",
    "gabriel bortoleto": "bortoleto", "jack doohan": "doohan",
    # broadcast format "F Lastname"
    "m verstappen": "verstappen", "l norris": "norris",
    "c leclerc": "leclerc", "o piastri": "piastri",
    "c sainz": "sainz", "l hamilton": "hamilton",
    "g russell": "russell", "s perez": "perez",
    "f alonso": "alonso", "l stroll": "stroll",
    "p gasly": "gasly", "e ocon": "ocon",
    "n hulkenberg": "hulkenberg", "k magnussen": "magnussen",
    "y tsunoda": "tsunoda", "l lawson": "lawson",
    "f colapinto": "colapinto", "o bearman": "bearman",
    "k antonelli": "antonelli", "i hadjar": "hadjar",
    "g bortoleto": "bortoleto", "j doohan": "doohan",
    # common nicknames / alternates
    "checo": "perez", "checo perez": "perez",
    "hulk": "hulkenberg", "kmag": "magnussen",
    "sainz jr": "sainz", "sainz jr.": "sainz",
}

def _resolve_driver_key(s: str) -> str:
    """Normalize any driver name input to a canonical last-name key for comparison."""
    if not s:
        return ""
    key = _clean_text_key(s)
    if not key:
        return ""
    # Exact acronym match (e.g. "ver")
    if key in _DRIVER_ACRONYMS:
        return _DRIVER_ACRONYMS[key]
    # Full alias match
    if key in _DRIVER_ALIASES:
        return _DRIVER_ALIASES[key]
    # Try extracting last word as last name (handles "Max Verstappen" → "verstappen")
    parts = key.split()
    if len(parts) > 1:
        last = parts[-1]
        if last in _DRIVER_ACRONYMS:
            return _DRIVER_ACRONYMS[last]
        if last in _DRIVER_ALIASES:
            return _DRIVER_ALIASES[last]
        return last
    return key

def _parse_dt_utc(date_s: str | None, time_s: str | None) -> Optional[datetime]:
    if not date_s:
        return None
    raw = f"{date_s}T{time_s or '00:00:00Z'}"
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def _f1_tz_name() -> str:
    return str(CFG.get("f1_timezone") or os.getenv("F1_TIMEZONE") or "UTC").strip() or "UTC"

def _f1_tz() -> timezone | ZoneInfo:
    name = _f1_tz_name()
    if name.upper() == "UTC":
        return timezone.utc
    try:
        return ZoneInfo(name)
    except Exception:
        return timezone.utc

def _fmt_dt_local(dt: datetime, tz: timezone | ZoneInfo | None = None) -> str:
    tz = tz or _f1_tz()
    local = dt.astimezone(tz)
    return local.strftime("%Y-%m-%d %H:%M %Z")

def _session_entries_for_race(race: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for key in ("FirstPractice", "SecondPractice", "ThirdPractice", "SprintQualifying", "SprintShootout", "Qualifying", "Sprint"):
        obj = race.get(key)
        if not isinstance(obj, dict):
            continue
        dt = _parse_dt_utc(obj.get("date"), obj.get("time"))
        if dt is None:
            continue
        entries.append({"type": key, "label": SESSION_LABELS.get(key, key), "dt": dt})

    race_dt = _parse_dt_utc(race.get("date"), race.get("time"))
    if race_dt is not None:
        entries.append({"type": "Race", "label": "Race", "dt": race_dt})

    entries.sort(key=lambda x: x["dt"])
    return entries

def _openf1_session_type(session: Dict[str, Any]) -> str:
    return str(session.get("session_type") or session.get("session_name") or "").strip()

def _openf1_is_f1_session(session: Dict[str, Any]) -> bool:
    # OpenF1 payloads can differ by endpoint/version, so inspect multiple fields.
    hay = " ".join(
        str(session.get(k) or "")
        for k in (
            "category",
            "series",
            "series_name",
            "meeting_name",
            "meeting_official_name",
            "session_name",
            "session_type",
        )
    ).lower()
    meta_hay = " ".join(
        str(session.get(k) or "")
        for k in ("category", "series", "series_name", "meeting_name", "meeting_official_name")
    ).lower()

    # Hard excludes with word boundaries to avoid false positives (e.g. "free practice" matching "fe").
    non_f1_pattern = re.compile(
        r"\b("
        r"formula\s*2|f2|gp2|"
        r"formula\s*3|f3|gp3|"
        r"formula\s*e|fe|"
        r"f1\s*academy|"
        r"porsche\s+supercup"
        r")\b"
    )
    if non_f1_pattern.search(hay):
        return False

    if re.search(r"\b(formula\s*1|f1)\b", hay):
        return True

    # Some endpoints can omit series/meeting labels; don't hard-fail unknown metadata.
    if not meta_hay.strip():
        return True

    return False

def _openf1_is_weekend_session(session: Dict[str, Any]) -> bool:
    st = _openf1_session_type(session).upper().strip()
    if not st:
        return False
    # Exclude testing sessions explicitly.
    if "TEST" in st:
        return False
    return st in {
        "PRACTICE",
        "QUALIFYING",
        "QUALI",
        "SPRINT",
        "SPRINT QUALIFYING",
        "SPRINT SHOOTOUT",
        "RACE",
    }

def _normalize_schedule_from_openf1(sessions: List[Dict[str, Any]], year: int) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for s in sessions:
        if not isinstance(s, dict):
            continue
        if not _openf1_is_f1_session(s):
            continue
        if not _openf1_is_weekend_session(s):
            continue
        mk = str(s.get("meeting_key") or "")
        if not mk:
            continue
        dt = _parse_openf1_dt(s.get("date_start"))
        if dt is None:
            continue
        slot = grouped.setdefault(mk, {"sessions": [], "base": s})
        slot["sessions"].append((dt, s))
        if len(str(s.get("meeting_official_name") or "")) > len(str(slot["base"].get("meeting_official_name") or "")):
            slot["base"] = s

    events: List[Dict[str, Any]] = []
    for _mk, obj in grouped.items():
        ss = sorted(obj["sessions"], key=lambda x: x[0])
        if not ss:
            continue
        base = obj["base"]
        season = str(base.get("year") or year)
        _cs = str(base.get("circuit_short_name") or "").strip()
        race_name = (
            str(base.get("meeting_name") or "").strip()
            or (f"{_cs} Grand Prix" if _cs else "")
            or (f"{str(base.get('location') or '').strip()} Grand Prix" if str(base.get("location") or "").strip() else "")
            or f"{str(base.get('country_name') or 'F1').strip()} Grand Prix"
        )
        circuit_name = str(base.get("circuit_short_name") or base.get("location") or "Unknown Circuit").strip()

        practice_sessions = [x for x in ss if _openf1_session_type(x[1]).upper().strip() == "PRACTICE"]
        quali_session = next((x for x in ss if "QUALIFYING" in _openf1_session_type(x[1]).upper() and "SPRINT" not in _openf1_session_type(x[1]).upper()), None)
        sprint_quali_session = next((x for x in ss if _openf1_session_type(x[1]).upper().strip() in {"SPRINT QUALIFYING", "SPRINT SHOOTOUT"}), None)
        sprint_session = next((x for x in ss if _openf1_session_type(x[1]).upper().strip() == "SPRINT"), None)
        race_session = next((x for x in ss if _openf1_session_type(x[1]).upper().strip() == "RACE"), None)
        race_dt = race_session[0] if race_session else ss[-1][0]

        race_date, race_time = _dt_to_ergast_parts(race_dt)
        race_obj: Dict[str, Any] = {
            "season": season,
            "round": "0",
            "raceName": race_name,
            "Circuit": {"circuitName": circuit_name},
            "date": race_date,
            "time": race_time,
            "_race_dt": race_dt,
        }

        if len(practice_sessions) >= 1:
            d, t = _dt_to_ergast_parts(practice_sessions[0][0])
            race_obj["FirstPractice"] = {"date": d, "time": t}
        if len(practice_sessions) >= 2:
            d, t = _dt_to_ergast_parts(practice_sessions[1][0])
            race_obj["SecondPractice"] = {"date": d, "time": t}
        if len(practice_sessions) >= 3:
            d, t = _dt_to_ergast_parts(practice_sessions[2][0])
            race_obj["ThirdPractice"] = {"date": d, "time": t}
        if quali_session:
            d, t = _dt_to_ergast_parts(quali_session[0])
            race_obj["Qualifying"] = {"date": d, "time": t}
        if sprint_quali_session:
            d, t = _dt_to_ergast_parts(sprint_quali_session[0])
            race_obj["SprintShootout"] = {"date": d, "time": t}
        if sprint_session:
            d, t = _dt_to_ergast_parts(sprint_session[0])
            race_obj["Sprint"] = {"date": d, "time": t}

        events.append(race_obj)

    events.sort(key=lambda r: r.get("_race_dt") or datetime.max.replace(tzinfo=timezone.utc))
    for i, race in enumerate(events, start=1):
        race["round"] = str(i)
        race.pop("_race_dt", None)
    return events

async def fetch_current_season_schedule(force: bool = False) -> List[Dict[str, Any]]:
    now_ts = time.time()
    fail_until = float(F1_SCHEDULE_CACHE.get("fail_until", 0.0) or 0.0)
    if (not force) and now_ts < fail_until:
        remaining = int(fail_until - now_ts)
        raise RuntimeError(f"OpenF1 schedule backoff active ({max(1, remaining)}s)")
    if (not force) and F1_SCHEDULE_CACHE["races"] and (now_ts - float(F1_SCHEDULE_CACHE["ts"])) < 300:
        return list(F1_SCHEDULE_CACHE["races"])

    year = datetime.now(timezone.utc).year
    races: List[Dict[str, Any]] = []
    try:
        sessions = await asyncio.to_thread(_openf1_get_json, "sessions", {"year": year}, 20, "schedule_fetch")
        if isinstance(sessions, list) and sessions:
            races = _normalize_schedule_from_openf1(sessions, year=year)
            F1_SCHEDULE_CACHE["fail_until"] = 0.0
            F1_SCHEDULE_CACHE["fail_count"] = 0
    except Exception as e:
        logging.warning(f"[F1] OpenF1 schedule fetch failed: {e}")
        fail_count = int(F1_SCHEDULE_CACHE.get("fail_count", 0) or 0) + 1
        F1_SCHEDULE_CACHE["fail_count"] = fail_count
        backoff = min(300, 15 * (2 ** min(5, fail_count - 1)))
        F1_SCHEDULE_CACHE["fail_until"] = now_ts + backoff

    if not races:
        raise RuntimeError("OpenF1 schedule unavailable or empty.")
    F1_SCHEDULE_CACHE["ts"] = now_ts
    F1_SCHEDULE_CACHE["races"] = list(races)
    return list(races)

async def upcoming_f1_sessions(limit: int = 8) -> List[Dict[str, Any]]:
    races = await fetch_current_season_schedule()
    now = datetime.now(timezone.utc)
    items: List[Dict[str, Any]] = []
    for race in races:
        for sess in _session_entries_for_race(race):
            if sess["dt"] >= now:
                items.append({
                    "round": race.get("round"),
                    "race_name": race.get("raceName") or "Grand Prix",
                    "circuit_name": ((race.get("Circuit") or {}).get("circuitName") or "").strip(),
                    "session_label": sess["label"],
                    "session_type": sess["type"],
                    "dt": sess["dt"],
                })
    items.sort(key=lambda x: x["dt"])
    return items[: max(1, int(limit))]

async def current_or_next_round_key() -> str:
    try:
        races = await fetch_current_season_schedule()
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y-round-unknown")
    now = datetime.now(timezone.utc)
    best = None
    for race in races:
        race_dt = _parse_dt_utc(race.get("date"), race.get("time"))
        if race_dt is None:
            continue
        if race_dt >= now - timedelta(days=2):
            best = race
            break
    if best is None and races:
        best = races[-1]
    if best is None:
        return datetime.now(timezone.utc).strftime("%Y-round-unknown")
    season = str((best.get("season") or datetime.now().year))
    rnd = str(best.get("round") or "unknown")
    return f"{season}-r{rnd}"

async def current_or_next_round_meta() -> Dict[str, Any]:
    key = await current_or_next_round_key()
    try:
        races = await fetch_current_season_schedule()
    except Exception:
        return {"key": key, "race_name": key, "race_dt": None, "sessions": []}
    for race in races:
        season = str(race.get("season") or "")
        rnd = str(race.get("round") or "")
        if key == f"{season}-r{rnd}":
            return {
                "key": key,
                "race_name": race.get("raceName") or key,
                "race_dt": _parse_dt_utc(race.get("date"), race.get("time")),
                "sessions": _session_entries_for_race(race),
            }
    return {"key": key, "race_name": key, "race_dt": None, "sessions": []}

def _predictions_root() -> Dict[str, Any]:
    root = _state_bucket("predictions")
    root.setdefault("rounds", {})
    root.setdefault("totals", {})
    return root

def _pred_round_obj(round_key: str) -> Dict[str, Any]:
    root = _predictions_root()
    rounds = root["rounds"]
    if round_key not in rounds or not isinstance(rounds.get(round_key), dict):
        rounds[round_key] = {"locked": False, "race_name": None, "actual": {}, "entries": {}, "scored": False}
    return rounds[round_key]

def _pred_user_entry(round_key: str, guild_id: int, user_id: int) -> Dict[str, Any]:
    rnd = _pred_round_obj(round_key)
    entries = rnd.setdefault("entries", {})
    gid = str(guild_id)
    if gid not in entries or not isinstance(entries.get(gid), dict):
        entries[gid] = {}
    g_entries = entries[gid]
    uid = str(user_id)
    if uid not in g_entries or not isinstance(g_entries.get(uid), dict):
        g_entries[uid] = {}
    return g_entries[uid]

def _pred_totals_for_guild(guild_id: int) -> Dict[str, int]:
    root = _predictions_root()
    totals = root.setdefault("totals", {})
    gid = str(guild_id)
    if gid not in totals or not isinstance(totals.get(gid), dict):
        totals[gid] = {}
    return totals[gid]

def _prediction_lock_dt(meta: Dict[str, Any], category: str) -> Optional[datetime]:
    category = (category or "").lower().strip()
    sessions = meta.get("sessions") or []
    if category == "pole":
        # GP pole locks when main Qualifying starts.
        for s in sessions:
            if str(s.get("type")) == "Qualifying":
                return s.get("dt")
        # Fallback if schedule data is incomplete.
        return meta.get("race_dt")
    if category in {"podium", "p10"}:
        for s in sessions:
            if str(s.get("type")) == "Race":
                return s.get("dt")
        return meta.get("race_dt")
    if category == "sprint_pole":
        for target in ("SprintShootout", "SprintQualifying"):
            for s in sessions:
                if str(s.get("type")) == target:
                    return s.get("dt")
        return meta.get("race_dt")
    if category in {"sprint_podium", "sprint_p8"}:
        for s in sessions:
            if str(s.get("type")) == "Sprint":
                return s.get("dt")
        return meta.get("race_dt")
    return meta.get("race_dt")

def _prediction_locked(round_key: str, race_dt: Optional[datetime]) -> bool:
    rnd = _pred_round_obj(round_key)
    if rnd.get("locked"):
        return True
    if race_dt and datetime.now(timezone.utc) >= race_dt:
        return True
    return False

def _prediction_category_locked(meta: Dict[str, Any], category: str) -> bool:
    lock_dt = _prediction_lock_dt(meta, category)
    return _prediction_locked(meta["key"], lock_dt)

def _prediction_category_lock_text(meta: Dict[str, Any], category: str) -> str:
    lock_dt = _prediction_lock_dt(meta, category)
    if lock_dt is None:
        return "unknown"
    return _fmt_dt_local(lock_dt)

def _prediction_session_requirements(meta: Dict[str, Any]) -> Dict[str, List[str]]:
    sessions = meta.get("sessions") or []
    session_types = {str(s.get("type")) for s in sessions}
    req: Dict[str, List[str]] = {}
    if "Qualifying" in session_types:
        req["quali"] = ["pole"]
    if "SprintShootout" in session_types or "SprintQualifying" in session_types:
        req["sprint_quali"] = ["sprint_pole"]
    if "Sprint" in session_types:
        req["sprint"] = ["sprint_podium", "sprint_p8"]
    if "Race" in session_types or meta.get("race_dt") is not None:
        req["race"] = ["podium", "p10"]
    return req

def _prediction_session_labels() -> Dict[str, str]:
    return {
        "quali": "Qualifying",
        "sprint_quali": "Sprint Qualifying",
        "sprint": "Sprint",
        "race": "Race",
    }

def _prediction_category_session(category: str) -> str:
    c = (category or "").lower().strip()
    if c == "pole":
        return "quali"
    if c == "sprint_pole":
        return "sprint_quali"
    if c in {"sprint_podium", "sprint_p8"}:
        return "sprint"
    if c in {"podium", "p10"}:
        return "race"
    return "race"

def _pred_scored_sessions_for_guild(round_obj: Dict[str, Any], guild_id: int) -> Dict[str, bool]:
    scored = round_obj.setdefault("scored_sessions", {})
    gid = str(guild_id)
    if gid not in scored or not isinstance(scored.get(gid), dict):
        scored[gid] = {}
    return scored[gid]

def _score_prediction_category(entry: Dict[str, Any], actual: Dict[str, Any], category: str) -> int:
    category = (category or "").lower().strip()
    if category in {"pole", "p10", "sprint_pole", "sprint_p8"}:
        pred_key = _resolve_driver_key(str(entry.get(category) or ""))
        actual_key = _resolve_driver_key(str(actual.get(category) or ""))
        if not pred_key or not actual_key:
            return 0
        if pred_key == actual_key:
            if category == "sprint_pole":
                return 2
            return 3  # pole, p10, sprint_p8 all worth 3
        return 0
    if category in {"podium", "sprint_podium"}:
        pred = entry.get(category) or []
        act = actual.get(category) or []
        if not (isinstance(pred, list) and isinstance(act, list) and len(act) >= 3):
            return 0
        act_keys = [_resolve_driver_key(str(x)) for x in act[:3]]
        exact_points = 5 if category == "podium" else 3
        in_points = 2 if category == "podium" else 1
        pts = 0
        for idx, p in enumerate(pred[:3]):
            pk = _resolve_driver_key(str(p))
            if not pk:
                continue
            if idx < len(act_keys) and pk == act_keys[idx]:
                pts += exact_points
            elif pk in act_keys:
                pts += in_points
        return pts
    return 0

def _score_prediction_session(entry: Dict[str, Any], actual: Dict[str, Any], session_key: str) -> int:
    pts = 0
    for cat in {"quali": ["pole"], "sprint_quali": ["sprint_pole"], "sprint": ["sprint_podium", "sprint_p8"], "race": ["podium", "p10"]}.get(session_key, []):
        pts += _score_prediction_category(entry, actual, cat)
    return pts

def _prediction_actuals_ready_for_session(meta: Dict[str, Any], round_obj: Dict[str, Any], session_key: str) -> bool:
    req = _prediction_session_requirements(meta).get(session_key, [])
    if not req:
        return False
    actual = round_obj.get("actual") or {}
    for cat in req:
        val = actual.get(cat)
        if cat in {"podium", "sprint_podium"}:
            if not (isinstance(val, list) and len(val) >= 3):
                return False
        else:
            if not str(val or "").strip():
                return False
    return True



async def _announce_prediction_scores_to_channel(
    channel,
    guild: discord.Guild,
    round_key: str,
    session_key: str,
) -> bool:
    """Core scoring function. Posts results to any channel/thread. No ctx needed."""
    meta = await _prediction_round_context()
    # If round_key doesn't match current round, load from state directly
    rnd = _pred_round_obj(round_key)
    race_name = rnd.get("race_name") or round_key
    scored_map = _pred_scored_sessions_for_guild(rnd, guild.id)
    if scored_map.get(session_key):
        return False
    if not _prediction_actuals_ready_for_session(meta if meta["key"] == round_key else {"key": round_key, "sessions": []}, rnd, session_key):
        return False

    guild_entries = ((rnd.get("entries") or {}).get(str(guild.id)) or {})
    totals = _pred_totals_for_guild(guild.id)
    # Also track per-round scores for !prstats
    round_scores = rnd.setdefault("round_scores", {}).setdefault(str(guild.id), {})
    rows: List[Tuple[int, str]] = []
    for uid, entry in guild_entries.items():
        pts = _score_prediction_session(entry, rnd.get("actual") or {}, session_key)
        totals[str(uid)] = int(totals.get(str(uid), 0) or 0) + pts
        # Accumulate per-round total
        prev_round = int(round_scores.get(str(uid), 0) or 0)
        round_scores[str(uid)] = prev_round + pts
        member = guild.get_member(int(uid))
        name = member.display_name if member else uid
        rows.append((pts, name))
    rows.sort(key=lambda x: (x[0], x[1].lower()), reverse=True)
    scored_map[session_key] = True
    _save_state_quiet()

    label = _prediction_session_labels().get(session_key, session_key)
    if rows:
        body = "\n".join(f"• {name} — **+{pts}** pts" for pts, name in rows)
    else:
        body = "No predictions were submitted."
    msg = f"🧮 **{race_name} — {label} Prediction Points**\n{body}"
    if len(msg) > 1900:
        msg = msg[:1850] + "\n… (truncated)"
    await channel.send(msg)
    return True

async def _announce_prediction_session_scores(ctx, meta: Dict[str, Any], session_key: str) -> bool:
    if ctx.guild is None:
        return False
    return await _announce_prediction_scores_to_channel(ctx.channel, ctx.guild, meta["key"], session_key)

async def _fetch_and_set_prediction_actuals_from_openf1(
    round_key: str,
    session_kind: str,
    openf1_session_key: int,
    driver_map: Dict[str, str],
) -> bool:
    """Fetch final positions from OpenF1 and set prediction actuals for the round. Returns True if actuals were set."""
    async with aiohttp.ClientSession(headers={"User-Agent": "OF1-Discord-Bot"}) as http:
        try:
            positions = await _openf1_latest_positions(http, openf1_session_key)
        except Exception as e:
            logging.warning(f"[Predict] Failed to fetch positions for autoscore: {e}")
            return False

    if not positions:
        return False

    ordered = sorted(positions.items(), key=lambda kv: kv[1])

    def _driver_last_name(num: str) -> str:
        label = driver_map.get(num, "")
        return _resolve_driver_key(label) if label else ""

    rnd = _pred_round_obj(round_key)
    actual = rnd.setdefault("actual", {})
    changed = False

    if session_kind in {"QUALI", "SPRINT_QUALI"}:
        p1 = next((num for num, pos in ordered if pos == 1), None)
        if p1:
            key = "sprint_pole" if session_kind == "SPRINT_QUALI" else "pole"
            name = _driver_last_name(p1)
            if name and not actual.get(key):
                actual[key] = name
                changed = True

    elif session_kind == "SPRINT":
        top3 = [num for num, pos in ordered if 1 <= pos <= 3]
        p8 = next((num for num, pos in ordered if pos == 8), None)
        if len(top3) == 3 and not actual.get("sprint_podium"):
            actual["sprint_podium"] = [_driver_last_name(n) for n in top3]
            changed = True
        if p8 and not actual.get("sprint_p8"):
            name = _driver_last_name(p8)
            if name:
                actual["sprint_p8"] = name
                changed = True

    elif session_kind == "RACE":
        top3 = [num for num, pos in ordered if 1 <= pos <= 3]
        p10 = next((num for num, pos in ordered if pos == 10), None)
        if len(top3) == 3 and not actual.get("podium"):
            actual["podium"] = [_driver_last_name(n) for n in top3]
            changed = True
        if p10 and not actual.get("p10"):
            name = _driver_last_name(p10)
            if name:
                actual["p10"] = name
                changed = True

    if changed:
        rnd["scored"] = False
        _save_state_quiet()
    return changed

async def _delayed_prediction_autoscore(
    guild: discord.Guild,
    thread: discord.Thread,
    round_key: str,
    session_kind: str,
    openf1_session_key: int,
    driver_map: Dict[str, str],
    delay_minutes: int = 30,
) -> None:
    """Wait delay_minutes after session end, fetch final results from OpenF1, then score predictions."""
    _racelog(guild.id, f"[Predict] Delayed autoscore scheduled in {delay_minutes}m for {session_kind} session {openf1_session_key}")
    await asyncio.sleep(delay_minutes * 60)
    try:
        changed = await _fetch_and_set_prediction_actuals_from_openf1(
            round_key, session_kind, openf1_session_key, driver_map
        )
        if not changed:
            _racelog(guild.id, "[Predict] Autoscore: no new actuals set (already set or fetch failed)")

        session_map = {"QUALI": "quali", "SPRINT_QUALI": "sprint_quali", "SPRINT": "sprint", "RACE": "race"}
        pred_session = session_map.get(session_kind)
        if pred_session:
            posted = await _announce_prediction_scores_to_channel(thread, guild, round_key, pred_session)
            _racelog(guild.id, f"[Predict] Autoscore posted={posted} for {pred_session}")
        else:
            _racelog(guild.id, f"[Predict] Autoscore: unknown session kind {session_kind}")
    except Exception as e:
        logging.error(f"[Predict] Delayed autoscore failed for guild {guild.id}: {e}")

def _normalize_driver_pick(s: str) -> str:
    s = " ".join((s or "").replace("|", " ").split())
    return s.strip()

def _split_podium_picks(raw: str) -> Optional[List[str]]:
    parts = [p.strip() for p in (raw or "").split("|")]
    if len(parts) != 3 or not all(parts):
        return None
    return [_normalize_driver_pick(p) for p in parts]


def _quiz_scores_root() -> Dict[str, Any]:
    root = _state_bucket("quiz_scores")
    return root

def _quiz_scores_for_guild(guild_id: int) -> Dict[str, int]:
    root = _quiz_scores_root()
    gid = str(guild_id)
    if gid not in root or not isinstance(root.get(gid), dict):
        root[gid] = {}
    return root[gid]

def _quiz_points_for_question(q: Dict[str, Any]) -> int:
    try:
        if "points" in q:
            return max(1, int(q.get("points")))
    except Exception:
        pass
    difficulty = str(q.get("difficulty") or "easy").lower().strip()
    return QUIZ_DIFFICULTY_POINTS.get(difficulty, 1)

def _quiz_category_for_question(q: Dict[str, Any]) -> str:
    return str(q.get("category") or "general").lower().strip() or "general"

def _quiz_history_state(guild_id: int) -> Dict[str, Any]:
    root = _state_bucket("quiz_history")
    gid = str(guild_id)
    if gid not in root or not isinstance(root.get(gid), dict):
        root[gid] = {"recent_questions": []}
    hist = root[gid]
    if not isinstance(hist.get("recent_questions"), list):
        hist["recent_questions"] = []
    return hist

def _quiz_question_key(q: Dict[str, Any]) -> str:
    return _clean_text_key(str(q.get("q") or ""))

def _quiz_pick_question(guild_id: int, difficulty_filters: set[str], category_filters: set[str]) -> Optional[Dict[str, Any]]:
    pool = []
    for q in F1_QUIZ_QUESTIONS:
        if not isinstance(q, dict) or not q.get("q"):
            continue
        q_diff = str(q.get("difficulty") or "easy").lower().strip()
        q_cat = _quiz_category_for_question(q)
        if difficulty_filters and q_diff not in difficulty_filters:
            continue
        if category_filters and q_cat not in category_filters:
            continue
        pool.append(q)
    if not pool:
        return None

    hist = _quiz_history_state(guild_id)
    recent = [str(x) for x in hist.get("recent_questions", []) if isinstance(x, str)]
    recent_set = set(recent)
    fresh = [q for q in pool if _quiz_question_key(q) not in recent_set]
    chosen = random.choice(fresh if fresh else pool)

    q_key = _quiz_question_key(chosen)
    recent.append(q_key)
    # Keep the anti-repeat window bounded and relative to question bank size.
    max_keep = max(20, min(120, len(F1_QUIZ_QUESTIONS) // 3))
    hist["recent_questions"] = recent[-max_keep:]
    _save_state_quiet()
    return chosen


def _circuit_lookup(query: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    q = _clean_text_key(query)
    if not q:
        return None, None
    key = CIRCUIT_ALIASES.get(q, q)
    if key in CIRCUIT_INFO:
        return key, CIRCUIT_INFO[key]
    for name in CIRCUIT_INFO.keys():
        if q in _clean_text_key(name):
            return name, CIRCUIT_INFO[name]
    return None, None

async def _openf1_candidate_race_session_keys() -> List[Any]:
    now_ts = time.time()
    cached_keys = _OPENF1_CANDIDATE_SESSIONS_CACHE.get("keys")
    cached_ts = float(_OPENF1_CANDIDATE_SESSIONS_CACHE.get("ts", 0.0) or 0.0)
    if isinstance(cached_keys, list) and cached_keys and (now_ts - cached_ts) < 300:
        return list(cached_keys)

    now = datetime.now(timezone.utc)
    # Include both Race and Sprint sessions — sprints award championship points
    # so the ↑/↓ delta should compare across whichever points-scoring event
    # came most recently, even if that was a sprint the day before the GP.
    POINTS_SESSION_TYPES = ("Race", "Sprint")
    parsed: List[tuple] = []
    years = [now.year, now.year - 1]
    for year in years:
        for session_type in POINTS_SESSION_TYPES:
            try:
                sessions = await asyncio.to_thread(
                    _openf1_get_json,
                    "sessions",
                    {"year": year, "session_type": session_type},
                    20,
                    "standings_candidate_sessions",
                )
            except Exception:
                continue
            if not isinstance(sessions, list):
                continue
            for s in sessions:
                if not isinstance(s, dict):
                    continue
                key = s.get("session_key")
                dt = _parse_openf1_dt(s.get("date_start"))
                if key is None or dt is None:
                    continue
                # Only include sessions that have already started — future races
                # won't have championship data and would waste API calls.
                if dt > now:
                    continue
                parsed.append((dt, key))

    parsed.sort(key=lambda x: x[0], reverse=True)
    candidates: List[Any] = []
    for _dt, key in parsed:
        if key not in candidates:
            candidates.append(key)
    # "latest" goes last — it often points to a non-race or in-progress session
    # with incomplete championship data (null team names, missing name fields).
    candidates.append("latest")
    _OPENF1_CANDIDATE_SESSIONS_CACHE["ts"] = now_ts
    _OPENF1_CANDIDATE_SESSIONS_CACHE["keys"] = list(candidates)
    return candidates

def _load_driver_cache() -> Dict[str, Any]:
    global _DRIVER_CACHE, _DRIVER_CACHE_LOADED
    if _DRIVER_CACHE_LOADED:
        return _DRIVER_CACHE
    from settings import DRIVER_CACHE_PATH
    try:
        with open(DRIVER_CACHE_PATH, "r", encoding="utf-8") as f:
            _DRIVER_CACHE = json.load(f)
    except Exception:
        _DRIVER_CACHE = {"drivers": {}, "last_session_key": None}
    _DRIVER_CACHE_LOADED = True
    return _DRIVER_CACHE


def _save_driver_cache() -> None:
    from settings import DRIVER_CACHE_PATH
    try:
        with open(DRIVER_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(_DRIVER_CACHE, f, indent=2)
    except Exception as e:
        logging.error(f"[DriverCache] save failed: {e}")


def _update_driver_cache(rows: List[Dict[str, Any]], session_key: Any) -> None:
    """Merge a fresh batch of API rows into the local driver cache.

    Rules:
    - Drivers are only ever added, never removed.
    - When a new session_key is seen, current points are snapshotted as
      prev_points before the new values are written in.  This gives us the
      "previous standing" for the ↑/↓ delta arrows without needing a second
      API call.
    - name/team/code are updated only when the API returns a non-empty value,
      so cached names survive API outages or incomplete responses.
    """
    cache = _load_driver_cache()
    drivers = cache.setdefault("drivers", {})
    last_key = str(cache.get("last_session_key") or "")
    current_key = str(session_key) if session_key is not None else ""
    is_new_session = bool(current_key and current_key != last_key)

    for r in rows:
        num = str(r.get("driver_number") or "")
        if not num:
            continue
        entry = drivers.setdefault(num, {})

        if is_new_session:
            # Snapshot before overwriting so we can compute position delta.
            entry["prev_points"] = entry.get("points", 0)

        entry["points"] = int(r.get("points", 0) or 0)

        # Only overwrite identity fields when the API actually has the data.
        name = str(r.get("name") or "").strip()
        if name:
            entry["name"] = name
        team = str(r.get("team") or "").strip()
        if team and team != "Unknown":
            entry["team"] = team
        code = str(r.get("code") or "").strip()
        if code:
            entry["code"] = code

    if is_new_session and current_key:
        cache["last_session_key"] = current_key
        cache["updated_at"] = datetime.now(timezone.utc).isoformat()

    _save_driver_cache()


def _standings_from_cache(use_previous: bool = False) -> List[Dict[str, Any]]:
    """Build a standings list from the local cache, sorted by points descending.
    Positions are assigned here so they're always consistent with the actual
    point totals rather than whatever the API returned.
    """
    cache = _load_driver_cache()
    drivers = cache.get("drivers") or {}
    rows = []
    for num_str, d in drivers.items():
        if not d.get("name"):
            continue
        pts = int(d.get("prev_points" if use_previous else "points", 0) or 0)
        rows.append({
            "driver_number": int(num_str) if num_str.isdigit() else 0,
            "name": d.get("name", f"#{num_str}"),
            "code": d.get("code", ""),
            "team": d.get("team", "Unknown"),
            "points": pts,
        })
    rows.sort(key=lambda x: x["points"], reverse=True)
    for i, r in enumerate(rows):
        r["position"] = i + 1
    return rows


async def _fetch_champ_driver_rows(session_key: Any) -> List[Dict[str, Any]]:
    """Fetch and process championship_drivers for a single session key. Returns [] if unavailable."""
    try:
        rows = await asyncio.to_thread(_openf1_get_json, "championship_drivers", {"session_key": session_key}, 20, "standings_drivers")
    except Exception:
        return []
    if not isinstance(rows, list) or not rows:
        return []
    meta_map: Dict[int, Dict[str, Any]] = {}
    try:
        drivers = await asyncio.to_thread(_openf1_get_json, "drivers", {"session_key": session_key}, 20, "standings_driver_meta")
        if isinstance(drivers, list):
            for d in drivers:
                if not isinstance(d, dict):
                    continue
                try:
                    n = int(d.get("driver_number"))
                except Exception:
                    continue
                meta_map[n] = d
    except Exception:
        pass
    out: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        try:
            num = int(r.get("driver_number"))
        except Exception:
            num = 0
        dmeta = meta_map.get(num, {})
        full_name = (
            str(r.get("full_name") or "").strip()
            or str(r.get("broadcast_name") or "").strip()
        )
        if not full_name:
            first = str(r.get("first_name") or "").strip()
            last = str(r.get("last_name") or "").strip()
            full_name = (f"{first} {last}").strip()
        if not full_name:
            full_name = (
                str(dmeta.get("full_name") or "").strip()
                or str(dmeta.get("broadcast_name") or "").strip()
            )
        if not full_name:
            first = str(dmeta.get("first_name") or "").strip()
            last = str(dmeta.get("last_name") or "").strip()
            full_name = (f"{first} {last}").strip()
        if not full_name:
            full_name = str(r.get("driver_name") or f"#{num}")
        team = (
            str(r.get("team_name") or "").strip()
            or str(dmeta.get("team_name") or "").strip()
            or "Unknown"
        )
        code = (
            str(r.get("name_acronym") or "").strip()
            or str(dmeta.get("name_acronym") or "").strip()
        )
        out.append({
            "position": int(r.get("position_current", r.get("position", 0)) or 0),
            "points": int(r.get("points_current", r.get("points", 0)) or 0),
            "driver_number": num,
            "code": code,
            "name": full_name,
            "team": team,
        })
    placed = [x for x in out if int(x.get("position", 0) or 0) > 0]
    # Drivers with position=0 but real points are still championship entrants —
    # the API just hasn't assigned them a standing yet (common at season start or
    # for the most-recent race before data is fully populated).
    unplaced = [
        x for x in out
        if int(x.get("position", 0) or 0) == 0 and int(x.get("points", 0) or 0) > 0
    ]
    if unplaced:
        next_pos = max((int(x.get("position", 0) or 0) for x in placed), default=0) + 1
        unplaced.sort(key=lambda x: int(x.get("points", 0) or 0), reverse=True)
        for i, x in enumerate(unplaced):
            x["position"] = next_pos + i
        out = placed + unplaced
    else:
        out = placed
    out.sort(key=lambda x: int(x.get("position", 999) or 999))
    return out


async def _refresh_driver_cache() -> None:
    """Try to pull the latest championship data from the API and merge it into
    the local driver cache.  Picks the candidate session that returns the most
    drivers (up to 3 attempts) so a partially-populated latest session doesn't
    overwrite good cached data with an incomplete set.
    """
    candidates = await _openf1_candidate_race_session_keys()
    best_rows: List[Dict[str, Any]] = []
    best_key: Any = None
    for i, session_key in enumerate(candidates):
        rows = await _fetch_champ_driver_rows(session_key)
        if len(rows) > len(best_rows):
            best_rows = rows
            best_key = session_key
        if len(best_rows) >= 20:
            break
        if i >= 2 and best_rows:
            break
    if best_rows:
        _update_driver_cache(best_rows, best_key)


async def _openf1_driver_standings_rows(limit: int = 0) -> List[Dict[str, Any]]:
    await _refresh_driver_cache()
    out = _standings_from_cache(use_previous=False)
    if not out:
        return []
    return out if not limit else out[:int(limit)]


async def _openf1_driver_standings_pair() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Returns (current_standings, previous_standings).

    Primary path: current = local cache sorted by points; previous = the
    pre-last-session snapshot stored in the cache (prev_points).  This means
    the ↑/↓ arrows show how each driver moved in the most recent race/sprint.

    Fallback (cache was just seeded, no real prev_points yet): query the API
    for the points-scoring session that came *before* the current one — i.e.
    skip the session we already have as current and use the next distinct one.
    This avoids comparing identical points (which would give all flat arrows).
    """
    await _refresh_driver_cache()
    current = _standings_from_cache(use_previous=False)
    previous = _standings_from_cache(use_previous=True)

    # Detect "no real snapshot": every prev_points is still 0 (fresh cache).
    if not previous or all(r["points"] == 0 for r in previous):
        cache = _load_driver_cache()
        current_key = str(cache.get("last_session_key") or "")
        candidates = await _openf1_candidate_race_session_keys()
        # Walk the list (most-recent-first) and take the first session that is
        # different from the one we already used for current standings.
        for session_key in candidates:
            if str(session_key) == current_key:
                continue
            rows = await _fetch_champ_driver_rows(session_key)
            if not rows:
                continue
            # Derive positions from points for consistency with the cache approach.
            rows.sort(key=lambda x: int(x.get("points", 0) or 0), reverse=True)
            for i, r in enumerate(rows):
                r["position"] = i + 1
            previous = rows
            break

    return current, previous


def _build_constructor_rows(driver_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    team_pts: Dict[str, int] = {}
    for r in driver_rows:
        team = str(r.get("team") or "").strip()
        # Only skip if completely blank — "Unknown" is still a valid aggregation
        # bucket and prevents real points (e.g. Ferrari with null team_name in the
        # API) from being silently dropped from the constructor standings.
        if not team:
            continue
        team_pts[team] = team_pts.get(team, 0) + int(r.get("points", 0) or 0)
    return [
        {"position": i + 1, "points": pts, "name": name}
        for i, (name, pts) in enumerate(sorted(team_pts.items(), key=lambda x: x[1], reverse=True))
    ]


async def _openf1_constructor_standings_rows(limit: int = 0) -> List[Dict[str, Any]]:
    driver_rows = await _openf1_driver_standings_rows(limit=0)
    out = _build_constructor_rows(driver_rows)
    return out if not limit else out[:int(limit)]


def _delta_str(current_pos: int, prev_pos: Optional[int]) -> str:
    if prev_pos is None:
        return "-"
    diff = prev_pos - current_pos  # positive = moved up (lower number is better)
    if diff > 0:
        return f"↑{diff}"
    if diff < 0:
        return f"↓{abs(diff)}"
    return "-"


async def fetch_driver_standings_text(limit: int = 0, _pair=None) -> str:
    current, previous = _pair if _pair is not None else await _openf1_driver_standings_pair()
    if not current:
        return "No standings available from OpenF1."
    prev_pos: Dict[int, int] = {r["driver_number"]: r["position"] for r in previous}
    lines = []
    for r in current:
        pos = int(r.get("position", 0))
        num = int(r.get("driver_number", 0))
        delta = _delta_str(pos, prev_pos.get(num))
        lines.append(f"{pos:>2}. {r.get('name', 'Unknown')} - {r.get('points', 0)} pts  {delta}")
    return "__**F1 Driver Standings**__\n```\n" + "\n".join(lines) + "\n```"


async def fetch_constructor_standings_text(limit: int = 0, _pair=None) -> str:
    current_drivers, prev_drivers = _pair if _pair is not None else await _openf1_driver_standings_pair()
    if not current_drivers:
        return "No standings available from OpenF1."
    current_rows = _build_constructor_rows(current_drivers)
    prev_pos: Dict[str, int] = {r["name"]: r["position"] for r in _build_constructor_rows(prev_drivers)}
    lines = []
    for r in current_rows:
        pos = int(r.get("position", 0))
        name = r.get("name", "Unknown")
        delta = _delta_str(pos, prev_pos.get(name))
        lines.append(f"{pos:>2}. {name} - {r.get('points', 0)} pts  {delta}")
    return "__**F1 Constructor Standings**__\n```\n" + "\n".join(lines) + "\n```"

# Discord setup
# ----------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.guilds = True
intents.members = True

def get_prefix(bot, message) -> str:
    return (CFG.get("prefix") or "!").strip() or "!"

bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)
APP_COMMANDS_SYNCED = False

# ----------------------------
# Helpers: role mapping
# ----------------------------
def cfg_reaction_roles() -> Dict[str, str]:
    return dict(ROLE_MAP_REACTION)

def cfg_color_roles() -> Dict[str, str]:
    return dict(ROLE_MAP_COLOR)

def cfg_driver_emoji_names() -> Dict[str, str]:
    """
    Mapping of custom emoji NAME -> role name.
    Example: {"Piastri":"Piastri"}
    """
    return dict(CFG.get("driver_emoji_names") or {})

def color_role_names() -> set[str]:
    return set(COLOR_ROLE_NAMES_CACHE)

def state_driver_map() -> Dict[str, str]:
    # emoji string (e.g. "<:Piastri:123>") -> role name
    return dict(ROLE_MAP_DRIVER)

def _ensure_reaction_panels_state() -> Dict[str, Any]:
    global STATE
    panels = STATE.get("reaction_panels")
    if not isinstance(panels, dict):
        panels = {}
        STATE["reaction_panels"] = panels
    return panels

def write_reaction_panel_state(panel: str, channel_id: int, message_id: int) -> None:
    panels = _ensure_reaction_panels_state()
    panels[panel] = {
        "channel_id": str(channel_id),
        "message_id": str(message_id),
    }
    save_state(STATE)

def _get_reaction_panel(panel: str) -> Optional[Tuple[int, int]]:
    panels = STATE.get("reaction_panels") or {}
    rec = panels.get(panel) if isinstance(panels, dict) else None
    if not isinstance(rec, dict):
        return None
    try:
        return int(rec["channel_id"]), int(rec["message_id"])
    except Exception:
        return None

def allowed_reaction_panel_message_ids() -> set[int]:
    ids: set[int] = set()
    for panel in ("notifications", "colors"):
        rec = _get_reaction_panel(panel)
        if rec:
            ids.add(rec[1])
    try:
        driver_msg_id = int(((STATE.get("driver_roles") or {}).get("message_id")) or 0)
        if driver_msg_id:
            ids.add(driver_msg_id)
    except Exception:
        pass
    return ids

def reaction_panel_targets_for_guild(guild: discord.Guild) -> List[Tuple[str, int, int]]:
    targets: List[Tuple[str, int, int]] = []
    for panel in ("notifications", "colors"):
        rec = _get_reaction_panel(panel)
        if not rec:
            continue
        channel_id, message_id = rec
        targets.append((panel, channel_id, message_id))

    try:
        drv = STATE.get("driver_roles") or {}
        channel_id = int(drv.get("channel_id") or 0)
        message_id = int(drv.get("message_id") or 0)
        if channel_id and message_id:
            targets.append(("drivers", channel_id, message_id))
    except Exception:
        pass
    return targets

def write_state_driver_map(channel_id: int, message_id: int, emoji_to_role: Dict[str, str]) -> None:
    global STATE
    if "driver_roles" not in STATE:
        STATE["driver_roles"] = {}
    STATE["driver_roles"]["channel_id"] = str(channel_id)
    STATE["driver_roles"]["message_id"] = str(message_id)
    STATE["driver_roles"]["emoji_to_role"] = dict(emoji_to_role)
    save_state(STATE)
    _rebuild_role_caches()

def resolve_role_name_from_emoji(emoji_str: str) -> Optional[str]:
    # order matters: notifications + colors + drivers(state)
    return (
        ROLE_MAP_REACTION.get(emoji_str)
        or ROLE_MAP_COLOR.get(emoji_str)
        or ROLE_MAP_DRIVER.get(emoji_str)
    )

# ============================================================
# XP SYSTEM (Mee6-style basic)
#   - awards XP per message with cooldown
#   - per-guild levels
#   - rank + leaderboard commands
#   - optional channel gate by minimum level (auto-delete)
#
# Config.json optional keys:
#   "xp_enabled": true
#   "xp_cooldown_seconds": 60
#   "xp_min_gain": 15
#   "xp_max_gain": 25
#   "xp_min_level_channels": { "123456789012345678": 5 }
#
# Level formula (simple + stable):
#   total_xp_needed_for_level(L) = 100 * L^2 + 50 * L
# ============================================================

def xp_enabled_for_guild(guild_id: Optional[int]) -> bool:
    if guild_id is None:
        return False
    return bool((CFG.get("xp_enabled", True)))

def xp_cooldown_seconds() -> int:
    try:
        v = int(CFG.get("xp_cooldown_seconds", os.getenv("XP_COOLDOWN_SECONDS", "60")))
    except Exception:
        v = 60
    return max(5, min(600, v))

def xp_gain_range() -> Tuple[int, int]:
    try:
        mn = int(CFG.get("xp_min_gain", os.getenv("XP_MIN_GAIN", "15")))
    except Exception:
        mn = 15
    try:
        mx = int(CFG.get("xp_max_gain", os.getenv("XP_MAX_GAIN", "25")))
    except Exception:
        mx = 25
    mn = max(1, min(1000, mn))
    mx = max(mn, min(2000, mx))
    return mn, mx

def xp_total_for_level(level: int) -> int:
    # 100*L^2 + 50*L
    L = max(0, int(level))
    return (100 * L * L) + (50 * L)

def xp_level_from_total(total_xp: int) -> int:
    # Find the highest L where xp_total_for_level(L) <= total_xp
    xp = max(0, int(total_xp))
    lo, hi = 0, 500  # sane cap
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if xp_total_for_level(mid) <= xp:
            lo = mid
        else:
            hi = mid - 1
    return lo

def xp_progress_to_next(total_xp: int) -> Tuple[int, int, int]:
    lvl = xp_level_from_total(total_xp)
    cur_req = xp_total_for_level(lvl)
    next_req = xp_total_for_level(lvl + 1)
    return lvl, max(0, total_xp - cur_req), max(1, next_req - cur_req)

def cfg_xp_min_level_channels() -> Dict[str, int]:
    raw = CFG.get("xp_min_level_channels") or {}
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, int] = {}
    for k, v in raw.items():
        try:
            out[str(k)] = int(v)
        except Exception:
            continue
    return out


async def maybe_gate_channel(message: discord.Message, user_level: int) -> bool:
    """
    Returns True if message was blocked (deleted).
    Only applies if xp_min_level_channels configured.
    """
    if message.guild is None:
        return False
    if message.author.bot:
        return False

    mapping = cfg_xp_min_level_channels()
    need = mapping.get(str(message.channel.id))
    if need is None:
        return False

    if user_level >= int(need):
        return False

    try:
        await message.delete()
    except Exception:
        pass

    try:
        await message.author.send(
            f"🚫 You need **level {need}** to talk in **#{message.channel.name}**.\n"
            f"You're currently **level {user_level}**."
        )
    except Exception:
        pass

    return True

# ----------------------------
# Commands: XP
# ----------------------------
@bot.hybrid_command(name="rank")
async def rank(ctx, member: discord.Member = None):
    member = member or ctx.author
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")

    rec = get_user_record(XP_STATE, ctx.guild.id, member.id)
    total_xp = int(rec.get("xp", 0) or 0)
    level, xp, xp_next = xp_progress_to_next(total_xp)
    title = "Rookie" if level < 5 else "Regular" if level < 15 else "Veteran"
    bg_key = (rec.get("card") or {}).get("bg_url") or "default"

    # Compute server rank by total XP
    all_rows = get_top_users_by_xp(XP_STATE, ctx.guild.id, limit=10000)
    uid_str = str(member.id)
    try:
        server_rank = next(i + 1 for i, (uid, _, _) in enumerate(all_rows) if uid == uid_str)
    except StopIteration:
        server_rank = None

    png_bytes = await build_rank_card_png(
        member=member,
        level=level,
        xp=xp,
        xp_next=xp_next,
        title=title,
        bg_key=bg_key,
        server_rank=server_rank,
    )

    file = discord.File(io.BytesIO(png_bytes), filename="rank.png")
    await ctx.send(file=file, ephemeral=True)

@bot.command(name="setbg")
async def setbg(ctx, name: str = None):
    """Set your rank card background. Use !cardbgs to see available options."""
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    available = rank_available_backgrounds()
    if not name:
        return await ctx.send(
            f"Available backgrounds: {', '.join(f'`{k}`' for k in available)}\n"
            f"Usage: `!setbg <name>`"
        )
    name = name.strip().lower()
    if name not in available:
        return await ctx.send(
            f"❌ `{name}` isn't available. Choose from: {', '.join(f'`{k}`' for k in available)}"
        )
    set_user_card_prefs(XP_STATE, ctx.guild.id, ctx.author.id, bg_url=name)
    _xp_mark_dirty()
    await ctx.send(f"✅ Background set to **{name}**. Use `!rank` to see your updated card.")

@bot.command(name="cardbgs", aliases=["backgrounds", "rankbgs"])
async def cardbgs(ctx):
    """List available rank card backgrounds."""
    available = rank_available_backgrounds()
    if not available:
        return await ctx.send("ℹ️ No backgrounds available yet.")
    await ctx.send("🎨 **Available rank card backgrounds:**\n" + "\n".join(f"• `{k}`" for k in available))


@bot.hybrid_command(name="xpleaderboard", aliases=["xptop"])
async def xpleaderboard(ctx, page: int = 1):
    """Top XP users in this server. Use page parameter to navigate."""
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    per_page = 10
    page = max(1, int(page))
    all_rows = get_top_users_by_xp(XP_STATE, ctx.guild.id, limit=10000)
    if not all_rows:
        return await ctx.send("No XP data yet.")
    total_pages = max(1, (len(all_rows) + per_page - 1) // per_page)
    page = min(page, total_pages)
    start = (page - 1) * per_page
    rows = all_rows[start:start + per_page]

    lines: List[str] = []
    for i, (uid, xp, lvl) in enumerate(rows, start=start + 1):
        member = ctx.guild.get_member(int(uid))
        name = member.display_name if member else f"<@{uid}>"
        real_lvl = xp_level_from_total(xp)
        lines.append(f"{i:>2}. {name} — **L{real_lvl}** ({xp} XP)")

    footer = f"Page {page}/{total_pages}" if total_pages > 1 else ""
    header = "🏆 **XP Leaderboard**"
    if footer:
        header += f" — {footer}"
    await ctx.send(header + "\n" + "\n".join(lines))

@bot.hybrid_command(name="xpset")
@commands.has_permissions(administrator=True)
async def xpset(ctx, member: discord.Member, xp: int):
    """Admin: set a user's XP."""
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    xp = max(0, int(xp))
    lvl = xp_level_from_total(xp)
    set_user_xp_level(XP_STATE, ctx.guild.id, member.id, xp=xp, level=lvl)
    _xp_mark_dirty()
    await ctx.send(f"✅ Set {member.display_name} to {xp} XP (L{lvl}).")

@bot.hybrid_command(name="xpreset")
@commands.has_permissions(administrator=True)
async def xpreset(ctx, member: discord.Member):
    """Admin: reset a user's XP."""
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    rec = get_user_record(XP_STATE, ctx.guild.id, member.id)
    rec["xp"] = 0
    rec["level"] = 0
    rec["last_msg_ts"] = 0
    rec["messages"] = 0
    _xp_mark_dirty()
    await ctx.send(f"✅ Reset XP for {member.display_name}.")

@bot.hybrid_command(name="xpaudit")
@commands.has_permissions(administrator=True)
async def xpaudit(ctx, member: discord.Member = None):
    """
    Admin: inspect a user's XP state and current tuning assumptions.
    """
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    member = member or ctx.author

    rec = get_user_record(XP_STATE, ctx.guild.id, member.id)
    total_xp = int(rec.get("xp", 0) or 0)
    level = xp_level_from_total(total_xp)
    lvl_progress, xp_in_level, xp_next = xp_progress_to_next(total_xp)
    messages_awarded = int(rec.get("messages", 0) or 0)
    last_ts = int(rec.get("last_msg_ts", 0) or 0)
    last_seen_txt = datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if last_ts > 0 else "never"

    cd = xp_cooldown_seconds()
    mn, mx = xp_gain_range()
    avg_gain = (mn + mx) / 2
    est_from_awards = int(round(messages_awarded * avg_gain))
    delta_vs_est = total_xp - est_from_awards

    body = (
        f"🔎 **XP Audit** for {member.mention}\n"
        f"- Level (stored/recomputed): **L{int(rec.get('level', 0) or 0)} / L{level}**\n"
        f"- Total XP: **{total_xp}**\n"
        f"- Progress in level: **{xp_in_level}/{xp_next}** (level calc = L{lvl_progress})\n"
        f"- Awarded message events (XP-state counter): **{messages_awarded}**\n"
        f"- Last XP-awarding message timestamp: `{last_seen_txt}`\n"
        f"- Current cooldown: **{cd}s**\n"
        f"- Current gain range: **{mn}–{mx} XP** (avg ~ {avg_gain:.1f})\n"
        f"- Estimated XP from awarded-message count at avg gain: **~{est_from_awards}**\n"
        f"- Delta vs estimate: **{delta_vs_est:+}**\n"
        "_Note: `messages` here is XP-awarding events after cooldown, not total Discord messages._"
    )
    await ctx.send(body)

@bot.hybrid_command(name="xpgate")
@commands.has_permissions(administrator=True)
async def xpgate(ctx, channel: discord.TextChannel, level: int):
    """Admin: require a minimum level to talk in a channel (auto-delete)."""
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    level = max(0, min(500, int(level)))

    reload_config_state()
    mapping = cfg_xp_min_level_channels()
    mapping[str(channel.id)] = level
    CFG["xp_min_level_channels"] = mapping
    save_config(CFG)

    await ctx.send(f"✅ Set **#{channel.name}** minimum level to **{level}**.")

@bot.hybrid_command(name="xpgateclear")
@commands.has_permissions(administrator=True)
async def xpgateclear(ctx, channel: discord.TextChannel):
    """Admin: remove channel min-level gate."""
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")

    reload_config_state()
    mapping = cfg_xp_min_level_channels()
    if str(channel.id) in mapping:
        del mapping[str(channel.id)]
    CFG["xp_min_level_channels"] = mapping
    save_config(CFG)

    await ctx.send(f"✅ Cleared min-level gate for **#{channel.name}**.")

# ----------------------------
# Commands: config tools
# ----------------------------
@bot.hybrid_command(name="configreload", aliases=["config_reload"])
@commands.has_permissions(administrator=True)
async def configreload(ctx):
    """Reload config.json + state.json without restarting the bot."""
    reload_config_state()
    load_f1_static_data()
    await ctx.send("✅ Reloaded config.json, state.json, and F1 data files.")

# ----------------------------
# Commands: reaction role setup
# ----------------------------
@bot.hybrid_command(name="setupnotifications", aliases=["setup_notifications"])
@commands.has_permissions(administrator=True)
async def setupnotifications(ctx):
    roles = cfg_reaction_roles()
    if not roles:
        await ctx.send("❌ No reaction_roles configured in config.json.")
        return

    description = "📰 **Get notified!**\nReact to opt in to pingable news roles."
    for emoji, role in roles.items():
        description += f"\n{emoji} → `{role}`"

    msg = await ctx.send(description)
    for emoji in roles.keys():
        await msg.add_reaction(emoji)
    write_reaction_panel_state("notifications", ctx.channel.id, msg.id)

    logging.info(f"[Notification Roles] Setup complete (Message ID: {msg.id})")
    await ctx.send(f"✅ Notifications setup message created: `{msg.id}`")

@bot.hybrid_command(name="setupcolors", aliases=["setup_colors"])
@commands.has_permissions(administrator=True)
async def setupcolors(ctx):
    roles = cfg_color_roles()
    if not roles:
        await ctx.send("❌ No color_roles configured in config.json.")
        return

    description = "🎨 **Choose your name color!**\nReact with an emoji to get a matching role. Only one color can be active at a time."
    for emoji, role in roles.items():
        description += f"\n{emoji} → `{role}`"

    msg = await ctx.send(description)
    for emoji in roles.keys():
        await msg.add_reaction(emoji)
    write_reaction_panel_state("colors", ctx.channel.id, msg.id)

    logging.info(f"[Color Roles] Setup complete (Message ID: {msg.id})")
    await ctx.send(f"✅ Colors setup message created: `{msg.id}`")

@bot.hybrid_command(name="setupdrivers", aliases=["setup_drivers"])
@commands.has_permissions(administrator=True)
async def setupdrivers(ctx):
    """
    Creates the driver-role reaction message and saves the mapping into state.json
    so it persists across restarts.
    """
    guild = ctx.guild
    if guild is None:
        await ctx.send("❌ This must be run in a server.")
        return

    emoji_name_map = cfg_driver_emoji_names()
    if not emoji_name_map:
        await ctx.send("❌ No driver_emoji_names configured in config.json.")
        return

    description = "\U0001F3CE\uFE0F **Choose your favorite F1 driver!**\nReact to get a fan role:"
    emoji_to_role: Dict[str, str] = {}
    missing = []

    for emoji_name, role_name in emoji_name_map.items():
        emoji_obj = discord.utils.get(guild.emojis, name=emoji_name)
        if emoji_obj:
            emoji_str = str(emoji_obj)  # "<:Name:123>"
            emoji_to_role[emoji_str] = role_name
            description += f"\n{emoji_obj} → `{role_name}`"
        else:
            missing.append(emoji_name)

    if missing:
        await ctx.send("\u26A0\uFE0F Missing custom emojis: " + ", ".join(missing))

    msg = await ctx.send(description)
    for emoji_str in emoji_to_role.keys():
        await msg.add_reaction(emoji_str)

    write_state_driver_map(channel_id=ctx.channel.id, message_id=msg.id, emoji_to_role=emoji_to_role)

    logging.info(f"[Driver Roles] Setup complete (Channel {ctx.channel.id}, Message {msg.id})")
    await ctx.send(f"✅ Driver roles message created and saved to state.json: `{msg.id}`")

# ----------------------------
# Commands: instagram quick check
# ----------------------------
@bot.command(name="instacheck", aliases=["insta_check"])
@commands.has_permissions(administrator=True)
async def instacheck(ctx, username: str = "of1.official"):
    post_url = await asyncio.to_thread(fetch_latest_instagram_post, username)
    if post_url:
        await ctx.send(f"📸 Latest Instagram post from `{username}`:\n{post_url}")
    else:
        await ctx.send("❌ Could not retrieve the latest Instagram post.")

# ----------------------------
# Utility commands
# ----------------------------
@bot.command(name="editmsg")
@commands.has_permissions(administrator=True)
async def editmsg(ctx, channel_id: int, message_id: int, *, new_text: str):
    channel = bot.get_channel(channel_id)
    if not channel:
        await ctx.send("❌ Could not find that channel.")
        return
    try:
        msg = await channel.fetch_message(message_id)
        if msg.author != bot.user:
            await ctx.send("\u26A0\uFE0F I can only edit my own messages.")
            return
        await msg.edit(content=new_text)
        await ctx.send("✅ Message updated.")
    except Exception as e:
        await ctx.send(f"❌ Failed to edit message: {e}")

@bot.command(name="botinfo")
@commands.has_permissions(administrator=True)
async def botinfo(ctx):
    uptime = datetime.now() - bot.launch_time
    await ctx.send(f"🛠 **Bot Uptime:** {uptime}")

@bot.command(name="serverlist")
@commands.has_permissions(administrator=True)
async def serverlist(ctx):
    guild_names = ", ".join(g.name for g in bot.guilds)
    await ctx.send(f"🤖 Connected to: {guild_names}")

@bot.command(name="logrecent")
@commands.has_permissions(administrator=True)
async def logrecent(ctx, lines: int = 10):
    try:
        lines = max(1, min(200, int(lines)))
        with open(LOG_PATH, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        text = "".join(all_lines[-lines:])
        await ctx.send(f"```\n{text[:1900]}\n```")
    except Exception as e:
        await ctx.send(f"❌ Could not read log: {e}")

@bot.hybrid_command(name="ping")
async def ping(ctx):
    await ctx.send("Pong!")

_HELP_CATEGORIES: Dict[str, Dict] = {
    "f1": {
        "label": "🏎 F1 Info",
        "commands": ["schedule", "nextsession", "circuit", "driverstats", "teamstats", "h2h", "standingssetup"],
    },
    "predictions": {
        "label": "🎯 Predictions",
        "commands": [
            "predictpole", "predictpodium", "predictp10",
            "predictsprintpole", "predictsprintpodium", "predictsprintp8",
            "mypredictions", "predictions", "predictionsboard",
            "predictionleaderboard", "prstats",
        ],
    },
    "quiz": {
        "label": "🧠 Quiz & XP",
        "commands": ["quiz", "quizscore", "rank", "xpleaderboard", "setbg", "cardbgs"],
    },
    "admin": {
        "label": "⚙️ Admin",
        "commands": [
            "f1reminders", "f1reminderleads", "setupnotifications", "setupcolors",
            "setupdrivers", "configreload", "standingssetup", "editmsg",
            "botinfo", "serverlist", "logrecent", "xpset", "xpreset", "xpaudit", "xpgate",
        ],
    },
}

class HelpView(discord.ui.View):
    def __init__(self, ctx, prefix: str):
        super().__init__(timeout=120)
        self.ctx = ctx
        self.prefix = prefix
        self.current = "f1"
        for key, meta in _HELP_CATEGORIES.items():
            btn = discord.ui.Button(label=meta["label"], style=discord.ButtonStyle.secondary, custom_id=f"help_{key}")
            btn.callback = self._make_callback(key)
            self.add_item(btn)

    def _make_callback(self, key: str):
        async def callback(interaction: discord.Interaction):
            if interaction.user.id != self.ctx.author.id:
                await interaction.response.send_message("This help menu isn't for you — run `!help` yourself!", ephemeral=True)
                return
            self.current = key
            await interaction.response.edit_message(content=self._page(key), view=self)
        return callback

    def _page(self, key: str) -> str:
        meta = _HELP_CATEGORIES[key]
        p = self.prefix
        descs = _command_descriptions()
        lines = [f"**{meta['label']}**\n"]
        for name in meta["commands"]:
            desc = descs.get(name, "")
            lines.append(f"`{p}{name}` — {desc}" if desc else f"`{p}{name}`")
        return "\n".join(lines)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        self.stop()


@bot.hybrid_command(name="help")
async def help(ctx):
    """Show categorized help for all bot commands."""
    prefix = getattr(ctx, "clean_prefix", None) or "!"
    view = HelpView(ctx, prefix)
    await ctx.send(view._page("f1"), view=view, ephemeral=True)

def _command_examples(prefix: str) -> Dict[str, str]:
    p = prefix or "!"
    return {
        "ping": f"{p}ping",
        "rank": f"{p}rank @DriverName",
        "xpleaderboard": f"{p}xpleaderboard 10",
        "xpset": f"{p}xpset @DriverName 2500",
        "xpreset": f"{p}xpreset @DriverName",
        "xpaudit": f"{p}xpaudit @DriverName",
        "schedule": f"{p}schedule 8",
        "nextsession": f"{p}nextsession",
        "f1reminders": f"{p}f1reminders on #admin-channel",
        "f1reminderleads": f"{p}f1reminderleads 1440 60 15",
        "circuit": f"{p}circuit spa",
        "driverstats": f"{p}driver verstappen",
        "teamstats": f"{p}team ferrari",
        "quiz": f"{p}quiz hard strategy",
        "quizscore": f"{p}quizscore",
        "predictpole": f"{p}predictpole Verstappen",
        "predictsprintpole": f"{p}predictsprintpole Norris",
        "predictpodium": f"{p}predictpodium Verstappen | Norris | Leclerc",
        "predictsprintpodium": f"{p}predictsprintpodium Norris | Piastri | Leclerc",
        "predictp10": f"{p}predictp10 Alonso",
        "mypredictions": f"{p}mypredictions",
        "predictions": f"{p}predictions",
        "predictionsboard": f"{p}predictionsboard",
        "predictionsetresult": f"{p}predictionsetresult podium Verstappen | Norris | Leclerc",
        "predictionscore": f"{p}predictionscore auto",
        "predictionleaderboard": f"{p}predictionleaderboard",
        "configreload": f"{p}configreload",
        "setupnotifications": f"{p}setupnotifications",
        "setupcolors": f"{p}setupcolors",
        "setupdrivers": f"{p}setupdrivers",
        "instacheck": f"{p}instacheck of1.official",
        "editmsg": f"{p}editmsg <channel_id> <message_id> New text",
        "logrecent": f"{p}logrecent 25",
        "standingssetup": f"{p}standingssetup both 5",
        "racelivestart": f"{p}racelivestart",
        "racelivestop": f"{p}racelivestop",
        "racelivetail": f"{p}racelivetail 20",
        "setdelay": f"{p}setdelay 2",
        "setpoll": f"{p}setpoll 3",
        "livesettings": f"{p}livesettings",
        "racelivekill": f"{p}racelivekill",
        "racetestlist": f"{p}racetestlist",
        "racetestinfo": f"{p}racetestinfo practice_short",
        "raceteststart": f"{p}raceteststart race_chaos 5",
        "raceteststop": f"{p}raceteststop",
        "openf1check": f"{p}openf1check 2026",
        "racereplay": f"{p}racereplay 2023 1 10",
    }

def _command_descriptions() -> Dict[str, str]:
    return {
        "ping": "Check whether the bot is responding.",
        "help": "List commands you can use in this channel.",
        "commands": "DM yourself a detailed command list with examples.",
        "rank": "Show your XP rank card or another user's.",
        "xpleaderboard": "Show the XP leaderboard.",
        "xpset": "Set a user's XP manually (admin).",
        "xpreset": "Reset a user's XP data (admin).",
        "xpaudit": "Audit XP stats and level math for a user.",
        "schedule": "Show upcoming F1 sessions from the current schedule.",
        "nextsession": "Show the next F1 session and countdown.",
        "f1reminders": "Configure or view automatic F1 session reminders (admin).",
        "f1reminderleads": "Set reminder lead times in minutes (admin).",
        "circuit": "Look up F1 circuit info by name or alias.",
        "driverstats": "Show current stats for a driver.",
        "teamstats": "Show current stats for a constructor/team.",
        "quiz": "Start an F1 quiz question (supports difficulty/category filters).",
        "quizscore": "Show the F1 quiz leaderboard.",
        "predictpole": "Submit your qualifying pole prediction.",
        "predictsprintpole": "Submit your sprint shootout pole prediction.",
        "predictpodium": "Submit your race podium prediction.",
        "predictsprintpodium": "Submit your sprint podium prediction.",
        "predictp10": "Submit your P10 race prediction.",
        "mypredictions": "Show your saved predictions and lock status.",
        "predictions": "Show prediction commands and usage overview.",
        "predictionsboard": "Show prediction entries for the current round.",
        "predictionsetresult": "Set actual results for prediction scoring (admin).",
        "predictionscore": "Post spoiler-safe prediction points for completed sessions (admin).",
        "predictionleaderboard": "Show season-long prediction points totals.",
        "configreload": "Reload config/state-backed bot settings and F1 data (admin).",
        "setupnotifications": "Post the notifications reaction-role panel (admin).",
        "setupcolors": "Post the color roles reaction-role panel (admin).",
        "setupdrivers": "Post the driver roles reaction-role panel (admin).",
        "instacheck": "Fetch the latest Instagram post URL for a username.",
        "editmsg": "Edit a bot-authored message by channel and message ID (admin).",
        "botinfo": "Show bot uptime and basic status (admin).",
        "serverlist": "List servers the bot is connected to (admin).",
        "logrecent": "Show recent lines from the bot log file (admin).",
        "standingssetup": "Create or refresh standings messages (admin).",
        "racelivestart": "Start race live tracking/supervision (admin).",
        "racelivestop": "Stop race live tracking loop (admin).",
        "racelivetail": "Show recent race-live event output (admin).",
        "setdelay": "Set race-live spoiler delay in seconds (admin).",
        "setpoll": "Set race-live OpenF1 poll interval in seconds (admin).",
        "livesettings": "Show race-live settings/status (admin).",
        "racelivekill": "Force-stop race-live worker tasks (admin).",
        "racetestlist": "List available race simulation scenarios.",
        "racetestinfo": "Show details for a race simulation scenario.",
        "raceteststart": "Start a race simulation scenario (admin).",
        "raceteststop": "Stop the active race simulation scenario (admin).",
        "openf1check": "Run OpenF1 API/auth/championship diagnostics (admin).",
        "racereplay": "Replay a historical race-control feed into a test thread (admin).",
    }

def _command_description_for(cmd: commands.Command) -> str:
    help_text = (cmd.help or "").strip() if getattr(cmd, "help", None) else ""
    if help_text:
        return help_text
    return _command_descriptions().get(cmd.name, "No description")

def _fallback_command_example(cmd: commands.Command, prefix: str) -> str:
    sig = (cmd.signature or "").strip()
    return f"{prefix}{cmd.name}" + (f" {sig}" if sig else "")

@bot.command(name="commands", aliases=["commandlist"])
async def commands_dm_list(ctx):
    """
    DM the caller a dynamic list of commands they can access, with examples.
    """
    prefix = getattr(ctx, "clean_prefix", None) or "!"
    examples = _command_examples(prefix)
    visible: List[str] = []
    for cmd in sorted(bot.commands, key=lambda c: c.name.lower()):
        try:
            if await cmd.can_run(ctx):
                ex = examples.get(cmd.name) or _fallback_command_example(cmd, prefix)
                desc = _command_description_for(cmd)
                visible.append(f"`{prefix}{cmd.name}` - {desc}\nExample: `{ex}`")
        except Exception:
            continue

    if not visible:
        return await ctx.send("❌ You don't have access to any commands.")

    chunks: List[str] = []
    current = "**Available Commands (You Have Access To)**\n"
    for entry in visible:
        candidate = current + ("\n\n" if current.strip() else "") + entry
        if len(candidate) > 1800:
            chunks.append(current)
            current = entry
        else:
            current = candidate
    if current:
        chunks.append(current)

    try:
        for chunk in chunks:
            await ctx.author.send(chunk)
        if ctx.guild is not None:
            await ctx.send("📬 Sent your command list to your DMs.")
    except Exception:
        await ctx.send("❌ I couldn't DM you. Check your privacy settings and try again.")

class QuizView(discord.ui.View):
    """Multiple-choice button quiz. One correct answer among 4 shuffled options."""

    def __init__(self, guild_id: int, correct_answer: str, options: List[str],
                 active_record: dict, timeout: float = 120.0):
        super().__init__(timeout=timeout)
        self.guild_id = guild_id
        self.correct_key = _clean_text_key(correct_answer)
        self.answered = False  # First correct answer wins; only locks once
        labels = ["A", "B", "C", "D"]
        styles = [
            discord.ButtonStyle.primary,
            discord.ButtonStyle.secondary,
            discord.ButtonStyle.success,
            discord.ButtonStyle.danger,
        ]
        for i, option in enumerate(options):
            btn = discord.ui.Button(
                label=f"{labels[i]}. {option[:50]}",
                style=styles[i],
                custom_id=f"quiz_{guild_id}_{i}",
            )
            btn.callback = self._make_callback(option)
            self.add_item(btn)

        # Store active record so on_timeout can expire it properly
        self._active = active_record

    def _make_callback(self, option: str):
        async def callback(interaction: discord.Interaction):
            if self.answered:
                await interaction.response.send_message("⌛ This question is already answered!", ephemeral=True)
                return
            active = F1_QUIZ_ACTIVE.get(self.guild_id)
            if not active or time.time() > float(active.get("expires_at", 0)):
                F1_QUIZ_ACTIVE.pop(self.guild_id, None)
                self.stop()
                await interaction.response.send_message("⌛ This quiz question expired.", ephemeral=True)
                return
            guess_key = _clean_text_key(option)
            all_valid = active.get("answers") or []
            if guess_key in all_valid or guess_key == self.correct_key:
                self.answered = True
                points = max(1, int(active.get("points", 1) or 1))
                difficulty = str(active.get("difficulty") or "easy").title()
                category = str(active.get("category") or "general").replace("_", " ").title()
                scores = _quiz_scores_for_guild(self.guild_id)
                uid = str(interaction.user.id)
                scores[uid] = int(scores.get(uid, 0) or 0) + points
                _save_state_quiet()
                F1_QUIZ_ACTIVE.pop(self.guild_id, None)
                # Disable all buttons
                for item in self.children:
                    item.disabled = True
                    if hasattr(item, "label") and _clean_text_key(item.label.split(". ", 1)[-1]) in all_valid:
                        item.style = discord.ButtonStyle.success
                self.stop()
                await interaction.response.edit_message(
                    content=interaction.message.content + f"\n\n✅ **{interaction.user.display_name}** got it! (**+{points}** pt{'s' if points != 1 else ''} · {category} · {difficulty})",
                    view=self,
                )
            else:
                await interaction.response.send_message("❌ Not quite — try again while the question is open!", ephemeral=True)
        return callback

    async def on_timeout(self):
        active = F1_QUIZ_ACTIVE.get(self.guild_id)
        if active and not self.answered:
            F1_QUIZ_ACTIVE.pop(self.guild_id, None)
        for item in self.children:
            item.disabled = True
        self.stop()


async def _quiz_difficulty_autocomplete(interaction: discord.Interaction, current: str):
    cur = current.lower()
    diffs = list(QUIZ_DIFFICULTY_POINTS.keys())
    return [discord.app_commands.Choice(name=d, value=d) for d in diffs if cur in d][:25]

async def _quiz_category_autocomplete(interaction: discord.Interaction, current: str):
    cur = current.lower()
    cats = sorted({_quiz_category_for_question(q) for q in F1_QUIZ_QUESTIONS})
    return [discord.app_commands.Choice(name=c.replace("_", " ").title(), value=c) for c in cats if cur in c][:25]

@bot.hybrid_command(name="quiz", aliases=["f1quiz"])
@commands.cooldown(1, 15, commands.BucketType.guild)
@discord.app_commands.autocomplete(difficulty=_quiz_difficulty_autocomplete, category=_quiz_category_autocomplete)
async def quiz(ctx, difficulty: str = "", category: str = ""):
    """Start a multiple-choice F1 quiz question. Optional: difficulty (easy/medium/hard) and category."""
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    if not F1_QUIZ_QUESTIONS:
        return await ctx.send("❌ No quiz questions configured.")

    difficulty_filters: set[str] = set()
    category_filters: set[str] = set()
    known_difficulties = set(QUIZ_DIFFICULTY_POINTS.keys())
    known_categories = {_quiz_category_for_question(q) for q in F1_QUIZ_QUESTIONS}

    for token_raw in [difficulty, category]:
        token = _clean_text_key(token_raw).replace(" ", "_")
        if not token:
            continue
        if token in known_difficulties:
            difficulty_filters.add(token)
        elif token in known_categories:
            category_filters.add(token)
        else:
            return await ctx.send(
                f"❌ Unknown filter `{token_raw}`. "
                "Use a difficulty (`easy`, `medium`, `hard`, `expert`) or a category like `circuits`, `strategy`, `rules`."
            )

    q = _quiz_pick_question(ctx.guild.id, difficulty_filters, category_filters)
    if q is None:
        return await ctx.send("❌ No quiz questions match those filters.")

    diff = str(q.get("difficulty") or "easy").lower().strip()
    cat = _quiz_category_for_question(q)
    points = _quiz_points_for_question(q)
    correct_display = (q.get("answers") or ["?"])[0]

    # Build distractors: mix curated wrong_answers bank with same-category real answers.
    # Numbers only match numbers; word answers only match word answers.
    correct_key = _clean_text_key(correct_display)
    correct_is_numeric = correct_display.strip().lstrip("-").isdigit()

    def _type_matches(candidate: str) -> bool:
        return candidate.strip().lstrip("-").isdigit() == correct_is_numeric

    curated = [str(w) for w in (q.get("wrong_answers") or [])
               if _clean_text_key(str(w)) != correct_key and _type_matches(str(w))]
    random.shuffle(curated)

    same_cat_pool: List[str] = []
    for oq in F1_QUIZ_QUESTIONS:
        if oq is q:
            continue
        if _quiz_category_for_question(oq) != cat:
            continue
        d = (oq.get("answers") or [None])[0]
        if d and _clean_text_key(d) != correct_key and _type_matches(str(d)):
            same_cat_pool.append(str(d))
    random.shuffle(same_cat_pool)

    # Interleave: one from curated, one from same-cat, repeat — keeps variety
    seen: set[str] = set()
    merged: List[str] = []
    for src in [curated, same_cat_pool]:
        for item in src:
            k = _clean_text_key(item)
            if k not in seen:
                seen.add(k)
                merged.append(item)

    # If still short, fall back across all categories (same type rule still applies)
    if len(merged) < 3:
        for oq in F1_QUIZ_QUESTIONS:
            if oq is q:
                continue
            d = (oq.get("answers") or [None])[0]
            if d and _type_matches(str(d)):
                k = _clean_text_key(d)
                if k != correct_key and k not in seen:
                    seen.add(k)
                    merged.append(str(d))
            if len(merged) >= 6:
                break

    random.shuffle(merged)
    distractors = merged[:3]

    options = [correct_display] + distractors
    random.shuffle(options)

    active_record = {
        "question": q["q"],
        "question_key": _quiz_question_key(q),
        "answers": [_clean_text_key(a) for a in q.get("answers", [])],
        "asked_at": time.time(),
        "expires_at": time.time() + 120,
        "asked_by": ctx.author.id,
        "difficulty": diff,
        "category": cat,
        "points": points,
    }
    F1_QUIZ_ACTIVE[ctx.guild.id] = active_record

    view = QuizView(ctx.guild.id, correct_display, options, active_record, timeout=120.0)
    await ctx.send(
        f"🧠 **F1 Quiz!** — {cat.replace('_', ' ').title()} · {diff.title()} · **{points}** pt{'s' if points != 1 else ''}\n"
        f"**{q['q']}**",
        view=view,
    )

@bot.hybrid_command(name="quizscore", aliases=["quizleaderboard"])
async def quizscore(ctx):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    scores = _quiz_scores_for_guild(ctx.guild.id)
    if not scores:
        return await ctx.send("\u2139\uFE0F No quiz scores yet.")
    rows = sorted(((int(v), uid) for uid, v in scores.items()), reverse=True)[:20]
    lines = []
    for i, (pts, uid) in enumerate(rows, start=1):
        member = ctx.guild.get_member(int(uid))
        name = member.display_name if member else uid
        lines.append(f"{i:>2}. {name} — **{pts}**")
    await ctx.send("🧠 **F1 Quiz Leaderboard**\n" + "\n".join(lines), ephemeral=True)

# ----------------------------
# Standings updater
# ----------------------------
STANDINGS_TASK: Optional[asyncio.Task] = None

def _refresh_seconds() -> int:
    try:
        minutes = int(os.getenv("STANDINGS_REFRESH_MINUTES", "5"))
    except ValueError:
        minutes = 5
    minutes = max(1, min(120, minutes))
    return minutes * 60

async def update_standings_once():
    channel_id = os.getenv("STANDINGS_CHANNEL_ID")
    if not channel_id:
        return

    channel = bot.get_channel(int(channel_id))
    if channel is None:
        try:
            channel = await bot.fetch_channel(int(channel_id))
        except Exception as e:
            logging.error(f"[Standings] Could not fetch channel {channel_id}: {e}")
            return

    driver_msg_id = os.getenv("DRIVER_STANDINGS_MESSAGE_ID")
    constructor_msg_id = os.getenv("CONSTRUCTOR_STANDINGS_MESSAGE_ID")

    driver_text = constructor_text = None
    if driver_msg_id or constructor_msg_id:
        try:
            pair = await _openf1_driver_standings_pair()
        except Exception as e:
            logging.error(f"[Standings] Failed to fetch standings pair: {e}")
            pair = ([], [])
        if driver_msg_id:
            try:
                driver_text = await fetch_driver_standings_text(_pair=pair)
            except Exception as e:
                driver_text = e
        if constructor_msg_id:
            try:
                constructor_text = await fetch_constructor_standings_text(_pair=pair)
            except Exception as e:
                constructor_text = e

    if driver_msg_id:
        try:
            if isinstance(driver_text, Exception):
                raise driver_text
            msg = await channel.fetch_message(int(driver_msg_id))
            await msg.edit(content=driver_text or "No standings available.")
        except Exception as e:
            logging.error(f"[Standings] Driver update failed: {e}")

    if constructor_msg_id:
        try:
            if isinstance(constructor_text, Exception):
                raise constructor_text
            msg = await channel.fetch_message(int(constructor_msg_id))
            await msg.edit(content=constructor_text or "No standings available.")
        except Exception as e:
            logging.error(f"[Standings] Constructor update failed: {e}")

async def standings_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        _loop_tick("standings")
        try:
            await update_standings_once()
        except Exception as e:
            _loop_error("standings")
            logging.error(f"[Standings] loop error: {e}")
        await asyncio.sleep(_refresh_seconds())

def ensure_standings_task_running():
    global STANDINGS_TASK
    if STANDINGS_TASK is None or STANDINGS_TASK.done():
        STANDINGS_TASK = asyncio.create_task(standings_loop())
        logging.info("[Standings] Loop started.")

def _ensure_background_task(task_ref_name: str, coro_factory, label: str) -> None:
    task = globals().get(task_ref_name)
    if task is None or task.done():
        globals()[task_ref_name] = asyncio.create_task(coro_factory())
        logging.info(f"[{label}] Loop started.")

def _task_running(task: Optional[asyncio.Task]) -> bool:
    return bool(task is not None and not task.done())

def _runtime_status_snapshot() -> Dict[str, Any]:
    running_live_guilds = []
    for gid, task in RACE_LIVE_TASKS.items():
        if _task_running(task):
            running_live_guilds.append(int(gid))
    bot_started_at = getattr(bot, "launch_time", None)
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "bot_started_at": bot_started_at.astimezone(timezone.utc).isoformat() if bot_started_at else None,
        "guild_count": len(bot.guilds),
        "loops": {
            "standings": _task_running(STANDINGS_TASK),
            "f1_reminders": _task_running(F1_REMINDER_TASK),
            "race_supervisor": _task_running(RACE_SUPERVISOR_TASK),
            "xp_flush": _task_running(XP_FLUSH_TASK),
            "periodic_role_recovery": _task_running(PERIODIC_ROLE_RECOVERY_TASK),
        },
        "race_live": {
            "enabled_guild_ids": sorted(int(g) for g, enabled in RACE_LIVE_ENABLED.items() if enabled),
            "running_guild_ids": sorted(running_live_guilds),
            "tracked_round_keys": dict(RACE_LIVE_ROUND_KEYS),
            "session_kinds": {str(g): str(k) for g, k in RACE_LIVE_SESSION_KINDS.items()},
            "last_event_ts": {str(g): str(ts) for g, ts in RACE_LIVE_LAST_EVENT_TS.items()},
            "manual_hold_guild_ids": sorted(int(g) for g, v in _race_live_hold_map().items() if v and str(g).isdigit()),
            "delay_seconds": _race_live_delay_seconds(),
            "poll_seconds": _race_live_poll_seconds(),
        },
        "standings": {
            "channel_id": int(os.getenv("STANDINGS_CHANNEL_ID", "0") or 0),
            "driver_message_id": int(os.getenv("DRIVER_STANDINGS_MESSAGE_ID", "0") or 0),
            "constructor_message_id": int(os.getenv("CONSTRUCTOR_STANDINGS_MESSAGE_ID", "0") or 0),
            "refresh_minutes": int(os.getenv("STANDINGS_REFRESH_MINUTES", "5") or 5),
        },
        "openf1_window": {
            "pre_buffer_hours": int(os.getenv("OPENF1_PRE_WEEKEND_BUFFER_HOURS", os.getenv("RACE_WINDOW_PADDING_HOURS", "24")) or 24),
            "post_buffer_hours": int(os.getenv("OPENF1_POST_WEEKEND_BUFFER_HOURS", "12") or 12),
        },
        "loop_health": {
            "heartbeats": dict(LOOP_HEARTBEATS),
            "errors": dict(LOOP_ERRORS),
        },
    }

def _write_runtime_status_file(payload: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(RUNTIME_STATUS_PATH) or ".", exist_ok=True)
        tmp = f"{RUNTIME_STATUS_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
            f.write("\n")
        os.replace(tmp, RUNTIME_STATUS_PATH)
    except Exception as e:
        logging.warning(f"[RuntimeStatus] write failed: {e}")


def _json_safe_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    def _default(o):
        if isinstance(o, datetime):
            return o.astimezone(timezone.utc).isoformat()
        return str(o)
    try:
        return json.loads(json.dumps(payload or {}, default=_default, ensure_ascii=False))
    except Exception:
        return {"ts": datetime.now(timezone.utc).isoformat(), "runtime_error": "runtime_status_payload_serialize_failed"}

async def runtime_status_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        _loop_tick("runtime_status")
        try:
            runtime = _runtime_status_snapshot()
            try:
                round_meta = await current_or_next_round_meta()
            except Exception:
                round_meta = {}
            payload = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "runtime": runtime,
                "round_meta": round_meta if isinstance(round_meta, dict) else {},
            }
            payload = _json_safe_payload(payload)
            await asyncio.to_thread(_write_runtime_status_file, payload)
            await asyncio.to_thread(upsert_runtime_status, payload)
        except Exception as e:
            _loop_error("runtime_status")
            logging.warning(f"[RuntimeStatus] loop error: {e}")
        interval = int(os.getenv("RUNTIME_STATUS_PUBLISH_SECONDS", "10") or 10)
        await asyncio.sleep(max(5, min(120, interval)))

@bot.hybrid_command(name="standingssetup", aliases=["standings_setup"])
@commands.has_permissions(administrator=True)
async def standingssetup(ctx, which: str = "both", refresh_minutes: int = 5):
    which = which.lower().strip()
    if which not in ("drivers", "constructors", "both"):
        await ctx.send("❌ Use: `drivers`, `constructors`, or `both`.")
        return

    refresh_minutes = max(1, min(120, int(refresh_minutes)))
    set_env_value("STANDINGS_REFRESH_MINUTES", str(refresh_minutes))
    set_env_value("STANDINGS_CHANNEL_ID", str(ctx.channel.id))

    created = []

    if which in ("drivers", "both"):
        msg = await ctx.send("\U0001F3C1 **F1 Driver Standings (Current Season)**\nLoading...")
        set_env_value("DRIVER_STANDINGS_MESSAGE_ID", str(msg.id))
        created.append(f"✅ Drivers message: `{msg.id}`")

    if which in ("constructors", "both"):
        msg = await ctx.send("\U0001F3C1 **F1 Constructor Standings (Current Season)**\nLoading...")
        set_env_value("CONSTRUCTOR_STANDINGS_MESSAGE_ID", str(msg.id))
        created.append(f"✅ Constructors message: `{msg.id}`")

    reload_config_state()
    if "standings" not in STATE:
        STATE["standings"] = {}
    STATE["standings"]["channel_id"] = str(ctx.channel.id)
    STATE["standings"]["driver_message_id"] = os.getenv("DRIVER_STANDINGS_MESSAGE_ID")
    STATE["standings"]["constructor_message_id"] = os.getenv("CONSTRUCTOR_STANDINGS_MESSAGE_ID")
    save_state(STATE)

    await update_standings_once()
    ensure_standings_task_running()

    await ctx.send(
        "📌 Standings configured.\n"
        + "\n".join(created)
        + f"\n⏱ Refresh: {refresh_minutes} min\n"
        "\u2139\uFE0F IDs saved to `.env` so it continues after restart."
    )

# ----------------------------
# Commands: F1 schedule / reminders / circuit info
# ----------------------------
@bot.hybrid_command(name="schedule")
async def schedule(ctx, count: int = 5):
    count = max(1, min(10, int(count)))
    try:
        items = await upcoming_f1_sessions(limit=count * 10)
    except Exception as e:
        logging.error(f"[F1] schedule failed: {e}")
        return await ctx.send("❌ Could not fetch the F1 schedule right now.")

    if not items:
        return await ctx.send("ℹ️ No upcoming sessions found.")

    # Group sessions by race weekend
    weekends: dict = {}
    for item in items:
        key = (item.get("round"), item["race_name"])
        if key not in weekends:
            weekends[key] = []
        weekends[key].append(item)

    blocks = list(weekends.items())[:count]
    first_session = True
    lines = ["📅 **F1 Schedule**"]
    for (_round, race_name), sessions in blocks:
        lines.append(f"\n**{race_name}**")
        for sess in sessions:
            unix_ts = int(sess["dt"].timestamp())
            label = sess["session_label"]
            if first_session:
                lines.append(f"› **{label}** — <t:{unix_ts}:F> ← next")
                first_session = False
            else:
                lines.append(f"· {label} — <t:{unix_ts}:R>")

    await ctx.send("\n".join(lines))

@bot.hybrid_command(name="nextsession", aliases=["nextf1"])
async def nextsession(ctx):
    try:
        items = await upcoming_f1_sessions(limit=1)
    except Exception as e:
        logging.error(f"[F1] nextsession failed: {e}")
        return await ctx.send("❌ Could not fetch the next session right now.")
    if not items:
        return await ctx.send("\u2139\uFE0F No upcoming sessions found.")
    item = items[0]
    unix_ts = int(item["dt"].timestamp())
    await ctx.send(
        f"⏭️ **Next F1 session:** **{item['race_name']} — {item['session_label']}**\n"
        f"🕒 <t:{unix_ts}:F> — <t:{unix_ts}:R>"
    )

def _f1_reminder_cfg() -> Dict[str, Any]:
    raw_leads = CFG.get("f1_reminder_leads_minutes") or [1440, 60, 15]
    leads: List[int] = []
    for x in raw_leads:
        try:
            leads.append(int(x))
        except Exception:
            continue
    if not leads:
        leads = [1440, 60, 15]
    return {
        "enabled": bool(CFG.get("f1_reminders_enabled", False)),
        "channel_id": int(CFG.get("f1_reminders_channel_id", 0) or 0),
        "leads": leads,
    }

def _f1_reminder_state() -> Dict[str, Any]:
    root = _state_bucket("f1_reminders")
    sent = root.get("sent")
    if not isinstance(sent, dict):
        sent = {}
        root["sent"] = sent
    return root

async def f1_reminder_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        _loop_tick("f1_reminders")
        try:
            reload_config_state()
            cfg = _f1_reminder_cfg()
            if not cfg["enabled"] or not cfg["channel_id"]:
                await asyncio.sleep(60)
                continue

            channel = bot.get_channel(cfg["channel_id"])
            if channel is None:
                try:
                    channel = await bot.fetch_channel(cfg["channel_id"])
                except Exception as e:
                    logging.warning(f"[F1Reminder] fetch channel failed: {e}")
                    await asyncio.sleep(120)
                    continue
            if not isinstance(channel, discord.TextChannel):
                await asyncio.sleep(120)
                continue

            upcoming = await upcoming_f1_sessions(limit=20)
            now = datetime.now(timezone.utc)
            st = _f1_reminder_state()
            sent = st["sent"]

            # prune old reminder keys
            cutoff = now - timedelta(days=7)
            for k, iso in list(sent.items()):
                try:
                    dt = datetime.fromisoformat(str(iso))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if dt < cutoff:
                        sent.pop(k, None)
                except Exception:
                    sent.pop(k, None)

            changed = False
            for item in upcoming:
                dt = item["dt"]
                if dt < now:
                    continue
                for lead in sorted(set(max(1, min(7 * 24 * 60, int(x))) for x in cfg["leads"]), reverse=True):
                    key = f"{item['round']}|{item['session_type']}|{lead}"
                    if key in sent:
                        continue
                    delta_m = int((dt - now).total_seconds() // 60)
                    if delta_m < 0:
                        continue
                    if delta_m <= lead:
                        msg = (
                            f"⏰ **F1 Reminder**: **{item['race_name']} — {item['session_label']}** starts "
                            f"{'now' if delta_m == 0 else f'in about {delta_m}m'}.\n"
                            f"🕒 `{_fmt_dt_local(dt)}` ({_f1_tz_name()})"
                        )
                        try:
                            await channel.send(msg)
                            sent[key] = now.isoformat()
                            changed = True
                            logging.info(f"[F1Reminder] sent {key}")
                        except Exception as e:
                            logging.warning(f"[F1Reminder] send failed: {e}")
            if changed:
                _save_state_quiet()
        except Exception as e:
            _loop_error("f1_reminders")
            logging.error(f"[F1Reminder] loop error: {e}")
        await asyncio.sleep(60)

@bot.hybrid_command(name="f1reminders")
@commands.has_permissions(administrator=True)
async def f1reminders(ctx, mode: str = "status", channel: discord.TextChannel = None):
    mode = (mode or "status").lower().strip()
    if mode in {"on", "enable"}:
        if channel is None:
            channel = ctx.channel if isinstance(ctx.channel, discord.TextChannel) else None
        if channel is None:
            return await ctx.send("❌ Provide a text channel, e.g. `!f1reminders on #channel`.")
        reload_config_state()
        CFG["f1_reminders_enabled"] = True
        CFG["f1_reminders_channel_id"] = int(channel.id)
        save_config(CFG)
        return await ctx.send(f"✅ F1 reminders enabled in {channel.mention}.")
    if mode in {"off", "disable"}:
        reload_config_state()
        CFG["f1_reminders_enabled"] = False
        save_config(CFG)
        return await ctx.send("✅ F1 reminders disabled.")

    cfg = _f1_reminder_cfg()
    ch_txt = f"<#{cfg['channel_id']}>" if cfg["channel_id"] else "(not set)"
    await ctx.send(
        "\u2139\uFE0F **F1 reminders status**\n"
        f"- Enabled: `{cfg['enabled']}`\n"
        f"- Channel: {ch_txt}\n"
        f"- Lead minutes: `{', '.join(str(x) for x in cfg['leads'])}`"
    )

@bot.hybrid_command(name="f1reminderleads")
@commands.has_permissions(administrator=True)
async def f1reminderleads(ctx, *, minutes: str):
    raw = str(minutes or "").strip()
    if not raw:
        return await ctx.send("❌ Usage: `!f1reminderleads 1440 60 15` or `/f1reminderleads minutes:\"1440 60 15\"`")
    parts = [p for p in re.split(r"[,\s]+", raw) if p]
    parsed: List[int] = []
    for p in parts:
        try:
            parsed.append(int(p))
        except Exception:
            continue
    if not parsed:
        return await ctx.send("❌ No valid minute values found. Example: `1440 60 15`")
    leads = sorted({max(1, min(10080, int(m))) for m in parsed}, reverse=True)
    reload_config_state()
    CFG["f1_reminder_leads_minutes"] = leads
    save_config(CFG)
    await ctx.send(f"✅ F1 reminder leads set to: `{', '.join(str(x) for x in leads)}` minutes.")

async def _circuit_autocomplete(interaction: discord.Interaction, current: str):
    cur = current.lower()
    seen: set = set()
    choices = []
    # Prefer canonical circuit names, then aliases
    for canonical in sorted(CIRCUIT_INFO.keys()):
        if cur in canonical.lower() and canonical not in seen:
            choices.append(discord.app_commands.Choice(name=canonical, value=canonical))
            seen.add(canonical)
    for alias, canonical in sorted(CIRCUIT_ALIASES.items()):
        if cur in alias.lower() and alias not in seen:
            choices.append(discord.app_commands.Choice(name=alias, value=alias))
            seen.add(alias)
    return choices[:25]

@bot.hybrid_command(name="circuit")
@discord.app_commands.autocomplete(name=_circuit_autocomplete)
async def circuit(ctx, *, name: str):
    key, info = _circuit_lookup(name)
    if not info:
        return await ctx.send("❌ Circuit not found in local data. Try a GP/city/circuit name (e.g. `!circuit spa`).")
    race_distance = info.get("race_distance_km")
    if race_distance is None and info.get("length_km") and info.get("laps"):
        race_distance = round(float(info["length_km"]) * int(info["laps"]), 3)
    await ctx.send(
        f"\U0001F3CE\uFE0F **{key.title()}**\n"
        f"- Location: {info.get('location', 'Unknown')}, {info.get('country', 'Unknown')}\n"
        f"- Corners: **{info.get('corners', '?')}**\n"
        f"- Lap length: **{info.get('length_km', '?')} km**\n"
        f"- Race laps: **{info.get('laps', '?')}**\n"
        f"- Race distance: **{race_distance if race_distance is not None else '?'} km**"
    )

async def _driver_name_autocomplete(interaction: discord.Interaction, current: str):
    try:
        rows = await _openf1_driver_standings_rows(limit=30)
        cur = current.lower()
        choices = []
        for r in rows:
            name = str(r.get("name") or "")
            if cur in name.lower() or cur in str(r.get("code") or "").lower():
                choices.append(discord.app_commands.Choice(name=name, value=name))
        return choices[:25]
    except Exception:
        return []

async def _team_name_autocomplete(interaction: discord.Interaction, current: str):
    try:
        rows = await _openf1_constructor_standings_rows(limit=20)
        cur = current.lower()
        return [
            discord.app_commands.Choice(name=str(r.get("name") or ""), value=str(r.get("name") or ""))
            for r in rows if cur in str(r.get("name") or "").lower()
        ][:25]
    except Exception:
        return []

@bot.hybrid_command(name="driverstats", aliases=["driver"])
@commands.cooldown(1, 10, commands.BucketType.user)
@discord.app_commands.autocomplete(query=_driver_name_autocomplete)
async def driverstats(ctx, *, query: str):
    try:
        rows = await _openf1_driver_standings_rows(limit=30)
    except Exception as e:
        logging.error(f"[F1] driverstats failed: {e}")
        return await ctx.send("⚠️ Could not fetch driver standings right now. Try again shortly.")

    if not rows:
        return await ctx.send("ℹ️ No driver standings available yet.")

    q = _clean_text_key(query)
    match = None
    for row in rows:
        hay = " ".join(str(x) for x in [row.get("name"), row.get("code"), row.get("driver_number"), row.get("team")] if x)
        if q in _clean_text_key(hay):
            match = row
            break
    if not match:
        return await ctx.send(f"❌ Driver `{query}` not found in current standings.")

    await ctx.send(
        f"**{match.get('name', 'Unknown')}** ({match.get('code') or match.get('driver_number') or '?'})\n"
        f"- Position: **P{match.get('position', '?')}**\n"
        f"- Points: **{match.get('points', '0')}**\n"
        f"- Wins: **{match.get('wins', 'N/A')}**\n"
        f"- Team: **{match.get('team', 'Unknown')}**"
    )

@bot.hybrid_command(name="teamstats", aliases=["team"])
@commands.cooldown(1, 10, commands.BucketType.user)
@discord.app_commands.autocomplete(query=_team_name_autocomplete)
async def teamstats(ctx, *, query: str):
    try:
        rows = await _openf1_constructor_standings_rows(limit=20)
    except Exception as e:
        logging.error(f"[F1] teamstats failed: {e}")
        return await ctx.send("⚠️ Could not fetch constructor standings right now. Try again shortly.")

    if not rows:
        return await ctx.send("ℹ️ No constructor standings available yet.")

    q = _clean_text_key(query)
    match = None
    for row in rows:
        hay = " ".join(str(x) for x in [row.get("name"), row.get("team_name"), row.get("nationality")] if x)
        if q in _clean_text_key(hay):
            match = row
            break
    if not match:
        return await ctx.send(f"❌ Team `{query}` not found in current standings.")

    await ctx.send(
        f"**{match.get('name', 'Unknown Team')}**\n"
        f"- Position: **P{match.get('position', '?')}**\n"
        f"- Points: **{match.get('points', '0')}**\n"
        f"- Wins: **{match.get('wins', 'N/A')}**"
    )
@bot.hybrid_command(name="h2h", aliases=["headtohead", "versus"])
@commands.cooldown(1, 30, commands.BucketType.user)
async def h2h(ctx, *, query: str):
    """Compare two F1 drivers head-to-head. E.g. !h2h verstappen norris"""
    import re as _re
    parts = _re.split(r'\s+vs\.?\s+|\s+versus\s+', query, flags=_re.IGNORECASE)
    if len(parts) == 2:
        q1, q2 = parts[0].strip(), parts[1].strip()
    else:
        words = query.split()
        if len(words) < 2:
            return await ctx.send("ℹ️ Usage: `!h2h <driver1> <driver2>` — e.g. `!h2h verstappen norris`")
        mid = max(1, len(words) // 2)
        q1, q2 = " ".join(words[:mid]), " ".join(words[mid:])

    try:
        rows = await _openf1_driver_standings_rows(limit=0)
    except Exception as e:
        logging.error(f"[F1] h2h standings failed: {e}")
        return await ctx.send("⚠️ Could not fetch standings. Try again shortly.")

    if not rows:
        return await ctx.send("ℹ️ No driver standings available yet.")

    def _find(q: str):
        qk = _clean_text_key(q)
        for r in rows:
            hay = " ".join(str(x) for x in [r.get("name"), r.get("code"), r.get("driver_number"), r.get("team")] if x)
            if qk in _clean_text_key(hay):
                return r
        return None

    d1, d2 = _find(q1), _find(q2)
    if not d1 and not d2:
        return await ctx.send(f"Neither `{q1}` nor `{q2}` found in current standings.")
    if not d1:
        return await ctx.send(f"Driver `{q1}` not found in current standings.")
    if not d2:
        return await ctx.send(f"Driver `{q2}` not found in current standings.")

    name1 = d1["name"].split()[-1] if d1.get("name") else d1["name"]
    name2 = d2["name"].split()[-1] if d2.get("name") else d2["name"]
    pos1  = int(d1.get("position", 99) or 99)
    pos2  = int(d2.get("position", 99) or 99)
    pts1  = int(d1.get("points",  0)  or 0)
    pts2  = int(d2.get("points",  0)  or 0)
    team1 = str(d1.get("team", "?") or "?")
    team2 = str(d2.get("team", "?") or "?")
    code1 = str(d1.get("code") or d1.get("driver_number", ""))
    code2 = str(d2.get("code") or d2.get("driver_number", ""))
    num1  = str(d1.get("driver_number") or "")
    num2  = str(d2.get("driver_number") or "")

    # ── Season head-to-head via position endpoint ──────────────
    race_d1 = race_d2 = race_total = 0
    sprint_d1 = sprint_d2 = sprint_total = 0
    quali_d1 = quali_d2 = quali_total = 0
    sq_d1 = sq_d2 = sq_total = 0

    if num1 and num2:
        async with ctx.typing():
            now = datetime.now(timezone.utc)

            _NON_F1 = re.compile(
                r"\b(formula\s*[23e]|f[23]|gp[23]|f1\s*academy|porsche)\b", re.I
            )

            async def _completed_session_keys(session_type: str, buffer_h: int) -> List[int]:
                """Fetch session keys for completed F1 sessions of a given type this year.
                Uses date_start + buffer to determine completion — date_end is unreliable.
                Deny-lists known non-F1 series without requiring an explicit F1 label."""
                try:
                    raw = await asyncio.to_thread(
                        _openf1_get_json, "sessions",
                        {"year": now.year, "session_type": session_type},
                        30, f"h2h_sess_{session_type.lower().replace(' ', '_')}",
                    )
                except Exception:
                    return []
                keys: List[int] = []
                if not isinstance(raw, list):
                    return keys
                for s in raw:
                    if not isinstance(s, dict):
                        continue
                    hay = " ".join(str(s.get(k) or "") for k in (
                        "meeting_name", "meeting_official_name",
                    ))
                    if _NON_F1.search(hay):
                        continue
                    sk = s.get("session_key")
                    if not sk:
                        continue
                    dt = _parse_openf1_dt(s.get("date_start"))
                    if dt is None or dt + timedelta(hours=buffer_h) > now:
                        continue
                    keys.append(int(sk))
                return keys

            # Fetch all four session types sequentially to avoid rate limiting
            race_keys  = await _completed_session_keys("Race", 3)
            sprint_keys = await _completed_session_keys("Sprint", 2)
            quali_keys = await _completed_session_keys("Qualifying", 2)
            # OpenF1 uses either "Sprint Qualifying" or "Sprint Shootout" depending on season
            sq_keys    = await _completed_session_keys("Sprint Qualifying", 1)
            if not sq_keys:
                sq_keys = await _completed_session_keys("Sprint Shootout", 1)

            async def _session_final_positions(session_key: int) -> dict:
                """Fetch all drivers' final positions for a session in one API call.
                Called sequentially — the position endpoint silently rate-limits
                concurrent requests by returning empty lists."""
                try:
                    data = await asyncio.to_thread(
                        _openf1_get_json, "position",
                        {"session_key": session_key},
                        20, "h2h_pos",
                    )
                    if not isinstance(data, list):
                        return {}
                    latest: dict = {}
                    for r in data:
                        if not isinstance(r, dict):
                            continue
                        drv = str(r.get("driver_number") or "")
                        dt = str(r.get("date") or "")
                        try:
                            pos = int(r["position"])
                        except Exception:
                            continue
                        if drv not in latest or dt >= latest[drv][0]:
                            latest[drv] = (dt, pos)
                    return {drv: p for drv, (_, p) in latest.items()}
                except Exception:
                    return {}

            segments = [
                (race_keys,   "race"),
                (sprint_keys, "sprint"),
                (quali_keys,  "quali"),
                (sq_keys,     "sq"),
            ]
            counters: Dict[str, List[int]] = {
                "race":   [0, 0, 0],
                "sprint": [0, 0, 0],
                "quali":  [0, 0, 0],
                "sq":     [0, 0, 0],
            }
            for keys, label in segments:
                for sk in keys:
                    positions = await _session_final_positions(sk)
                    p1 = positions.get(num1)
                    p2 = positions.get(num2)
                    if p1 is None or p2 is None:
                        continue
                    counters[label][2] += 1
                    if p1 < p2:
                        counters[label][0] += 1
                    elif p2 < p1:
                        counters[label][1] += 1

            race_d1,   race_d2,   race_total   = counters["race"]
            sprint_d1, sprint_d2, sprint_total = counters["sprint"]
            quali_d1,  quali_d2,  quali_total  = counters["quali"]
            sq_d1,     sq_d2,     sq_total     = counters["sq"]

    # ── Build message ──────────────────────────────────────────
    pts_diff = pts1 - pts2
    if pts_diff > 0:
        pts_line = f"{name1} leads by **{pts_diff}** pts"
    elif pts_diff < 0:
        pts_line = f"{name2} leads by **{abs(pts_diff)}** pts"
    else:
        pts_line = "Both drivers level on points"

    champ_ahead = name1 if pos1 < pos2 else (name2 if pos2 < pos1 else None)
    champ_line = (f"{champ_ahead} is ahead in the championship" if champ_ahead
                  else "Equal championship position")

    def _bar(a: int, b: int, name_a: str, name_b: str) -> str:
        if a == 0 and b == 0:
            return "  No data yet"
        winner = name_a if a > b else (name_b if b > a else None)
        w_str = f" — **{winner}** leads" if winner else " — level"
        return f"  {name_a} **{a}** — **{b}** {name_b}{w_str}"

    def _section(emoji: str, label: str, total: int, a: int, b: int) -> str:
        unit = "session" if total == 1 else "sessions"
        return f"{emoji} **{label}** ({total} {unit})\n{_bar(a, b, name1, name2)}"

    sections = []
    if num1 and num2:
        sections.append(_section("🏁", "Races", race_total, race_d1, race_d2))
        if sprint_total > 0:
            sections.append(_section("⚡", "Sprint Races", sprint_total, sprint_d1, sprint_d2))
        sections.append(_section("🅿", "Qualifying", quali_total, quali_d1, quali_d2))
        if sq_total > 0:
            sections.append(_section("⚡🅿", "Sprint Qualifying", sq_total, sq_d1, sq_d2))

    msg = (
        f"**{name1}** ({code1}) vs **{name2}** ({code2})\n"
        f"──────────────────────────────────\n"
        f"🏆 **Championship**\n"
        f"  {name1}: **P{pos1}** — {pts1} pts — {team1}\n"
        f"  {name2}: **P{pos2}** — {pts2} pts — {team2}\n"
        f"  {pts_line}\n"
        f"  {champ_line}\n"
        + ("\n" + "\n\n".join(sections) if sections else "")
    )
    await ctx.send(msg)


async def _prediction_round_context() -> Dict[str, Any]:
    try:
        meta = await current_or_next_round_meta()
    except Exception as e:
        logging.error(f"[Predict] round meta fetch failed: {e}")
        key = datetime.now(timezone.utc).strftime("%Y-rfallback")
        meta = {"key": key, "race_name": key, "race_dt": None, "sessions": []}
    rnd = _pred_round_obj(meta["key"])
    if not rnd.get("race_name"):
        rnd["race_name"] = meta.get("race_name")
    return meta

def _pred_entry_summary(entry: Dict[str, Any], req: Optional[Dict[str, Any]] = None) -> str:
    req = req or {}
    podium = entry.get("podium") or []
    sprint_podium = entry.get("sprint_podium") or []
    podium_txt = " | ".join(str(x) for x in podium) if isinstance(podium, list) and podium else "—"
    sprint_podium_txt = " | ".join(str(x) for x in sprint_podium) if isinstance(sprint_podium, list) and sprint_podium else "—"
    lines = [
        f"- Pole: `{entry.get('pole') or '—'}`",
        f"- Podium: `{podium_txt}`",
        f"- P10: `{entry.get('p10') or '—'}`",
    ]
    if "sprint_quali" in req:
        lines.append(f"- Sprint Pole: `{entry.get('sprint_pole') or '—'}`")
    if "sprint" in req:
        lines.append(f"- Sprint Podium: `{sprint_podium_txt}`")
        lines.append(f"- Sprint P8: `{entry.get('sprint_p8') or '—'}`")
    return "\n".join(lines)

@bot.hybrid_command(name="predictpole")
@discord.app_commands.autocomplete(driver=_driver_name_autocomplete)
async def predictpole(ctx, *, driver: str):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    meta = await _prediction_round_context()
    if _prediction_category_locked(meta, "pole"):
        return await ctx.send(
            f"🔒 Pole predictions are locked for **{meta['race_name']}** "
            f"(locked at Qualifying start: `{_prediction_category_lock_text(meta, 'pole')}`)."
        )
    entry = _pred_user_entry(meta["key"], ctx.guild.id, ctx.author.id)
    entry["pole"] = _normalize_driver_pick(driver)
    _save_state_quiet()
    await _try_update_predictions_board(ctx.guild, meta)
    lock_text = _prediction_category_lock_text(meta, "pole")
    await ctx.send(f"✅ Pole pick saved for **{meta['race_name']}**: `{entry['pole']}` — locks at `{lock_text}`")

@bot.hybrid_command(name="predictsprintpole", aliases=["predictsppole"])
@discord.app_commands.autocomplete(driver=_driver_name_autocomplete)
async def predictsprintpole(ctx, *, driver: str):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    meta = await _prediction_round_context()
    req = _prediction_session_requirements(meta)
    if "sprint_quali" not in req:
        return await ctx.send(f"\u2139\uFE0F **{meta['race_name']}** does not appear to have a sprint qualifying/shootout session scheduled.")
    if _prediction_category_locked(meta, "sprint_pole"):
        return await ctx.send(
            f"🔒 Sprint pole predictions are locked for **{meta['race_name']}** "
            f"(locked at Sprint Qualifying/Shootout start: `{_prediction_category_lock_text(meta, 'sprint_pole')}`)."
        )
    entry = _pred_user_entry(meta["key"], ctx.guild.id, ctx.author.id)
    entry["sprint_pole"] = _normalize_driver_pick(driver)
    _save_state_quiet()
    await _try_update_predictions_board(ctx.guild, meta)
    lock_text = _prediction_category_lock_text(meta, "sprint_pole")
    await ctx.send(f"✅ Sprint pole pick saved for **{meta['race_name']}**: `{entry['sprint_pole']}` — locks at `{lock_text}`")

@bot.hybrid_command(name="predictpodium")
async def predictpodium(ctx, *, picks: str):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    meta = await _prediction_round_context()
    if _prediction_category_locked(meta, "podium"):
        return await ctx.send(
            f"🔒 Podium predictions are locked for **{meta['race_name']}** "
            f"(locked at Race start: `{_prediction_category_lock_text(meta, 'podium')}`)."
        )
    podium = _split_podium_picks(picks)
    if not podium:
        return await ctx.send("❌ Use format: `!predictpodium Driver 1 | Driver 2 | Driver 3`")
    entry = _pred_user_entry(meta["key"], ctx.guild.id, ctx.author.id)
    entry["podium"] = podium
    _save_state_quiet()
    await _try_update_predictions_board(ctx.guild, meta)
    lock_text = _prediction_category_lock_text(meta, "podium")
    await ctx.send(f"✅ Podium pick saved for **{meta['race_name']}**: `{ ' | '.join(podium) }` — locks at `{lock_text}`")

@bot.hybrid_command(name="predictsprintpodium", aliases=["predictsppodium"])
async def predictsprintpodium(ctx, *, picks: str):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    meta = await _prediction_round_context()
    req = _prediction_session_requirements(meta)
    if "sprint" not in req:
        return await ctx.send(f"\u2139\uFE0F **{meta['race_name']}** does not appear to have a sprint race scheduled.")
    if _prediction_category_locked(meta, "sprint_podium"):
        return await ctx.send(
            f"🔒 Sprint podium predictions are locked for **{meta['race_name']}** "
            f"(locked at Sprint start: `{_prediction_category_lock_text(meta, 'sprint_podium')}`)."
        )
    podium = _split_podium_picks(picks)
    if not podium:
        return await ctx.send("❌ Use format: `!predictsprintpodium Driver 1 | Driver 2 | Driver 3`")
    entry = _pred_user_entry(meta["key"], ctx.guild.id, ctx.author.id)
    entry["sprint_podium"] = podium
    _save_state_quiet()
    await _try_update_predictions_board(ctx.guild, meta)
    lock_text = _prediction_category_lock_text(meta, "sprint_podium")
    await ctx.send(f"✅ Sprint podium pick saved for **{meta['race_name']}**: `{ ' | '.join(podium) }` — locks at `{lock_text}`")

@bot.hybrid_command(name="predictp10")
@discord.app_commands.autocomplete(driver=_driver_name_autocomplete)
async def predictp10(ctx, *, driver: str):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    meta = await _prediction_round_context()
    if _prediction_category_locked(meta, "p10"):
        return await ctx.send(
            f"🔒 P10 predictions are locked for **{meta['race_name']}** "
            f"(locked at Race start: `{_prediction_category_lock_text(meta, 'p10')}`)."
        )
    entry = _pred_user_entry(meta["key"], ctx.guild.id, ctx.author.id)
    entry["p10"] = _normalize_driver_pick(driver)
    _save_state_quiet()
    await _try_update_predictions_board(ctx.guild, meta)
    lock_text = _prediction_category_lock_text(meta, "p10")
    await ctx.send(f"✅ P10 pick saved for **{meta['race_name']}**: `{entry['p10']}` — locks at `{lock_text}`")

@bot.hybrid_command(name="predictsprintp8", aliases=["predictspp8"])
@discord.app_commands.autocomplete(driver=_driver_name_autocomplete)
async def predictsprintp8(ctx, *, driver: str):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    meta = await _prediction_round_context()
    req = _prediction_session_requirements(meta)
    if "sprint" not in req:
        return await ctx.send(f"ℹ️ **{meta['race_name']}** does not appear to have a sprint race scheduled.")
    if _prediction_category_locked(meta, "sprint_p8"):
        return await ctx.send(
            f"🔒 Sprint P8 predictions are locked for **{meta['race_name']}** "
            f"(locked at Sprint start: `{_prediction_category_lock_text(meta, 'sprint_p8')}`)."
        )
    entry = _pred_user_entry(meta["key"], ctx.guild.id, ctx.author.id)
    entry["sprint_p8"] = _normalize_driver_pick(driver)
    _save_state_quiet()
    await _try_update_predictions_board(ctx.guild, meta)
    lock_text = _prediction_category_lock_text(meta, "sprint_p8")
    await ctx.send(f"✅ Sprint P8 pick saved for **{meta['race_name']}**: `{entry['sprint_p8']}` — locks at `{lock_text}`")

def _lock_status_text(meta: Dict[str, Any], category: str) -> str:
    """Return a human-friendly lock status string for a prediction category."""
    if _prediction_category_locked(meta, category):
        return "🔒 Locked"
    lock_dt = _prediction_lock_dt(meta, category)
    if lock_dt is None:
        return "⏳ Open"
    now = datetime.now(timezone.utc)
    if lock_dt <= now:
        return "🔒 Locked"
    ts = int(lock_dt.timestamp())
    return f"⏳ Open — locks <t:{ts}:R>"

@bot.hybrid_command(name="mypredictions")
async def mypredictions(ctx):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    meta = await _prediction_round_context()
    req = _prediction_session_requirements(meta)
    entry = _pred_user_entry(meta["key"], ctx.guild.id, ctx.author.id)
    lines = [f"📝 **Your predictions** for **{meta['race_name']}** (`{meta['key']}`)"]
    lines.append(f"**Pole** {_lock_status_text(meta, 'pole')}: `{entry.get('pole') or '—'}`")
    podium = entry.get("podium") or []
    podium_txt = " | ".join(str(x) for x in podium) if isinstance(podium, list) and podium else "—"
    lines.append(f"**Podium** {_lock_status_text(meta, 'podium')}: `{podium_txt}`")
    lines.append(f"**P10** {_lock_status_text(meta, 'p10')}: `{entry.get('p10') or '—'}`")
    if "sprint_quali" in req:
        lines.append(f"**Sprint Pole** {_lock_status_text(meta, 'sprint_pole')}: `{entry.get('sprint_pole') or '—'}`")
    if "sprint" in req:
        sprint_podium = entry.get("sprint_podium") or []
        sp_txt = " | ".join(str(x) for x in sprint_podium) if isinstance(sprint_podium, list) and sprint_podium else "—"
        lines.append(f"**Sprint Podium** {_lock_status_text(meta, 'sprint_podium')}: `{sp_txt}`")
        lines.append(f"**Sprint P8** {_lock_status_text(meta, 'sprint_p8')}: `{entry.get('sprint_p8') or '—'}`")
    await ctx.send("\n".join(lines), ephemeral=True)

@bot.hybrid_command(name="predictions", aliases=["predicthelp"])
async def predictions(ctx):
    meta = await _prediction_round_context()
    req = _prediction_session_requirements(meta)
    lines = [
        f"📋 **Predictions** for **{meta['race_name']}** (`{meta['key']}`)",
        f"- `!predictpole <driver>` ({_lock_status_text(meta, 'pole')})",
        f"- `!predictpodium A | B | C` ({_lock_status_text(meta, 'podium')})",
        f"- `!predictp10 <driver>` ({_lock_status_text(meta, 'p10')})",
    ]
    if "sprint_quali" in req:
        lines.append(f"- `!predictsprintpole <driver>` ({_lock_status_text(meta, 'sprint_pole')})")
    if "sprint" in req:
        lines.append(f"- `!predictsprintpodium A | B | C` ({_lock_status_text(meta, 'sprint_podium')})")
        lines.append(f"- `!predictsprintp8 <driver>` ({_lock_status_text(meta, 'sprint_p8')})")
    lines.append("- `!mypredictions` — view your picks")
    lines.append("- `!predictionsboard` — see all entries")
    lines.append("- `!prstats` — your prediction standings")
    await ctx.send("\n".join(lines))

def _build_predictions_board_text(meta: Dict[str, Any], guild: discord.Guild, page: int = 1, per_page: int = 10) -> str:
    rnd = _pred_round_obj(meta["key"])
    req = _prediction_session_requirements(meta)
    cat_defs = [("pole", "Pole"), ("podium", "Podium"), ("p10", "P10")]
    if "sprint_quali" in req:
        cat_defs.append(("sprint_pole", "SP Pole"))
    if "sprint" in req:
        cat_defs.append(("sprint_podium", "SP Podium"))
        cat_defs.append(("sprint_p8", "SP P8"))
    locked_cats = {cat for cat, _ in cat_defs if _prediction_category_locked(meta, cat)}
    guild_entries = ((rnd.get("entries") or {}).get(str(guild.id)) or {})
    if not guild_entries:
        return f"ℹ️ No predictions submitted yet for **{meta['race_name']}**."
    all_items = list(guild_entries.items())
    total_pages = max(1, (len(all_items) + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    page_items = all_items[start:start + per_page]
    lines = []
    for uid, entry in page_items:
        member = guild.get_member(int(uid))
        name = member.display_name if member else uid
        filled = sum(1 for cat, _ in cat_defs if entry.get(cat))
        header = f"**{name}** — {filled}/{len(cat_defs)} picks"
        pick_parts = []
        for cat, label in cat_defs:
            val = entry.get(cat)
            if not val:
                continue
            if cat in locked_cats:
                if isinstance(val, list):
                    pick_parts.append(f"{label}: {' | '.join(str(x) for x in val)}")
                else:
                    pick_parts.append(f"{label}: {val}")
        if pick_parts:
            lines.append(f"{header}\n  {', '.join(pick_parts)}")
        else:
            lines.append(header)
    header_line = f"📋 **Predictions board** for **{meta['race_name']}**"
    if total_pages > 1:
        header_line += f" — Page {page}/{total_pages}"
    text = header_line + "\n" + "\n".join(lines)
    if len(text) > 1900:
        text = text[:1850] + "\n… (truncated)"
    return text


async def _try_update_predictions_board(guild: discord.Guild, meta: Dict[str, Any]) -> None:
    """Silently try to edit the pinned predictions board message for this guild+round."""
    board_key = f"{guild.id}:{meta['key']}"
    ref = (_state_bucket("predictions").get("board_messages") or {}).get(board_key)
    if not ref:
        return
    channel = bot.get_channel(int(ref.get("channel_id", 0)))
    if channel is None:
        return
    try:
        msg = await channel.fetch_message(int(ref["msg_id"]))
        await msg.edit(content=_build_predictions_board_text(meta, guild))
    except Exception:
        pass


@bot.hybrid_command(name="predictionsboard", aliases=["predboard"])
async def predictionsboard(ctx, page: int = 1):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    meta = await _prediction_round_context()
    text = _build_predictions_board_text(meta, ctx.guild, page=page)
    board_key = f"{ctx.guild.id}:{meta['key']}"
    boards = _state_bucket("predictions").setdefault("board_messages", {})
    ref = boards.get(board_key)
    # Try to edit an existing board message first
    if ref:
        try:
            ch = bot.get_channel(int(ref.get("channel_id", 0))) or await bot.fetch_channel(int(ref["channel_id"]))
            existing = await ch.fetch_message(int(ref["msg_id"]))
            await existing.edit(content=text)
            return
        except Exception:
            pass
    # Send fresh and remember the message
    sent = await ctx.send(text)
    boards[board_key] = {"msg_id": sent.id, "channel_id": sent.channel.id}
    _save_state_quiet()

@bot.hybrid_command(name="predictionslock")
@commands.has_permissions(administrator=True)
async def predictionslock(ctx):
    meta = await _prediction_round_context()
    rnd = _pred_round_obj(meta["key"])
    rnd["locked"] = True
    _save_state_quiet()
    await ctx.send(f"🔒 Predictions locked for **{meta['race_name']}** (`{meta['key']}`).")

@bot.hybrid_command(name="predictionsunlock")
@commands.has_permissions(administrator=True)
async def predictionsunlock(ctx):
    meta = await _prediction_round_context()
    rnd = _pred_round_obj(meta["key"])
    rnd["locked"] = False
    _save_state_quiet()
    await ctx.send(f"🔓 Predictions unlocked for **{meta['race_name']}** (`{meta['key']}`).")

_PRED_CATEGORIES = ["pole", "podium", "p10", "sprint_pole", "sprint_podium", "sprint_p8"]

async def _pred_category_autocomplete(interaction: discord.Interaction, current: str):
    cur = current.lower()
    return [
        discord.app_commands.Choice(name=c, value=c)
        for c in _PRED_CATEGORIES if cur in c
    ][:25]

@bot.hybrid_command(name="predictionsetresult")
@commands.has_permissions(administrator=True)
@discord.app_commands.autocomplete(category=_pred_category_autocomplete)
async def predictionsetresult(ctx, category: str, *, value: str):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    category = (category or "").lower().strip()
    category = {
        "sprintpole": "sprint_pole",
        "sprint_pole": "sprint_pole",
        "sppole": "sprint_pole",
        "sprintpodium": "sprint_podium",
        "sprint_podium": "sprint_podium",
        "sppodium": "sprint_podium",
        "sprintp8": "sprint_p8",
        "sprint_p8": "sprint_p8",
        "spp8": "sprint_p8",
    }.get(category, category)
    meta = await _prediction_round_context()
    rnd = _pred_round_obj(meta["key"])
    actual = rnd.setdefault("actual", {})
    if category == "pole":
        actual["pole"] = _normalize_driver_pick(value)
    elif category == "p10":
        actual["p10"] = _normalize_driver_pick(value)
    elif category == "podium":
        podium = _split_podium_picks(value)
        if not podium:
            return await ctx.send("❌ Podium format: `!predictionsetresult podium Driver 1 | Driver 2 | Driver 3`")
        actual["podium"] = podium
    elif category == "sprint_pole":
        actual["sprint_pole"] = _normalize_driver_pick(value)
    elif category == "sprint_podium":
        podium = _split_podium_picks(value)
        if not podium:
            return await ctx.send("❌ Sprint podium format: `!predictionsetresult sprint_podium Driver 1 | Driver 2 | Driver 3`")
        actual["sprint_podium"] = podium
    elif category == "sprint_p8":
        actual["sprint_p8"] = _normalize_driver_pick(value)
    else:
        return await ctx.send("❌ Category must be `pole`, `podium`, `p10`, `sprint_pole`, `sprint_podium`, or `sprint_p8`.")
    rnd["scored"] = False  # legacy field, kept for compatibility
    scored_map = _pred_scored_sessions_for_guild(rnd, ctx.guild.id)
    scored_map.pop(_prediction_category_session(category), None)
    _save_state_quiet()
    auto_posted = False
    session_key = _prediction_category_session(category)
    try:
        auto_posted = await _announce_prediction_session_scores(ctx, meta, session_key)
    except Exception as e:
        logging.error(f"[Predict] auto score announce failed for {session_key}: {e}")
    if not auto_posted:
        await ctx.send(f"✅ Saved `{category}` result for **{meta['race_name']}**.")

@bot.hybrid_command(name="predictionscore")
@commands.has_permissions(administrator=True)
async def predictionscore(ctx, session: str = "auto"):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    meta = await _prediction_round_context()
    rnd = _pred_round_obj(meta["key"])
    req = _prediction_session_requirements(meta)
    wanted = (session or "auto").lower().strip()
    alias_map = {
        "auto": "auto",
        "all": "all",
        "quali": "quali",
        "qualifying": "quali",
        "sprintquali": "sprint_quali",
        "sprint_quali": "sprint_quali",
        "sprintshootout": "sprint_quali",
        "sprint": "sprint",
        "race": "race",
    }
    wanted = alias_map.get(wanted, wanted)
    if wanted not in {"auto", "all", "quali", "sprint_quali", "sprint", "race"}:
        return await ctx.send("❌ Use `!predictionscore [auto|all|quali|sprint_quali|sprint|race]`")

    session_keys = list(req.keys()) if wanted in {"auto", "all"} else [wanted]
    did_any = False
    for sk in session_keys:
        if sk not in req:
            continue
        if wanted == "auto":
            scored_map = _pred_scored_sessions_for_guild(rnd, ctx.guild.id)
            if scored_map.get(sk):
                continue
            if not _prediction_actuals_ready_for_session(meta, rnd, sk):
                continue
        posted = await _announce_prediction_session_scores(ctx, meta, sk)
        did_any = did_any or posted
    if not did_any:
        await ctx.send("\u2139\uFE0F No scoreable prediction sessions yet (missing actuals or already scored).")

@bot.hybrid_command(name="predictionleaderboard", aliases=["fantasypoints", "predictlb"])
async def predictionleaderboard(ctx):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    totals = _pred_totals_for_guild(ctx.guild.id)
    if not totals:
        return await ctx.send("\u2139\uFE0F No prediction points yet.")
    rows = sorted(((int(v), uid) for uid, v in totals.items()), reverse=True)[:20]
    lines = []
    for rank_i, (pts, uid) in enumerate(rows, start=1):
        member = ctx.guild.get_member(int(uid))
        name = member.display_name if member else uid
        lines.append(f"{rank_i:>2}. {name} — **{pts} pts**")
    await ctx.send("\U0001F3C6 **Prediction Leaderboard (Fantasy Points)**\n" + "\n".join(lines))

async def _pred_round_key_autocomplete(interaction: discord.Interaction, current: str):
    root = _predictions_root()
    keys = sorted((root.get("rounds") or {}).keys(), reverse=True)
    cur = current.lower()
    return [
        discord.app_commands.Choice(name=k, value=k)
        for k in keys if cur in k
    ][:25]

@bot.hybrid_command(name="predresults", aliases=["roundresults", "predhistory"])
@discord.app_commands.autocomplete(round_key=_pred_round_key_autocomplete)
async def predresults(ctx, *, round_key: str = None):
    """Show prediction scores and actuals for a specific past round.
    Usage: !predresults            (current/most recent round)
           !predresults 2026-r3   (specific round key)
    """
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")

    if round_key:
        round_key = round_key.strip().lower()
    else:
        meta = await _prediction_round_context()
        round_key = meta["key"]

    root = _predictions_root()
    rnd = (root.get("rounds") or {}).get(round_key)
    if not rnd or not isinstance(rnd, dict):
        # Try to find a close match
        all_keys = sorted((root.get("rounds") or {}).keys())
        available = ", ".join(f"`{k}`" for k in all_keys[-10:]) or "none"
        return await ctx.send(
            f"ℹ️ No prediction data found for round `{round_key}`.\n"
            f"Available rounds (most recent): {available}"
        )

    race_name = rnd.get("race_name") or round_key
    actual = rnd.get("actual") or {}
    round_scores = (rnd.get("round_scores") or {}).get(str(ctx.guild.id)) or {}
    guild_entries = (rnd.get("entries") or {}).get(str(ctx.guild.id)) or {}
    scored_sessions = (rnd.get("scored_sessions") or {}).get(str(ctx.guild.id)) or {}

    if not guild_entries:
        return await ctx.send(f"ℹ️ No prediction entries found for **{race_name}** (`{round_key}`).")

    lines = [f"📊 **{race_name}** (`{round_key}`) — Prediction Results"]

    # Show actuals if available
    actual_parts = []
    if actual.get("pole"):
        actual_parts.append(f"Pole: **{actual['pole']}**")
    if actual.get("podium") and isinstance(actual["podium"], list):
        actual_parts.append("Podium: **" + " | ".join(actual["podium"][:3]) + "**")
    if actual.get("p10"):
        actual_parts.append(f"P10: **{actual['p10']}**")
    if actual.get("sprint_pole"):
        actual_parts.append(f"Sprint Pole: **{actual['sprint_pole']}**")
    if actual.get("sprint_podium") and isinstance(actual["sprint_podium"], list):
        actual_parts.append("Sprint Podium: **" + " | ".join(actual["sprint_podium"][:3]) + "**")
    if actual.get("sprint_p8"):
        actual_parts.append(f"Sprint P8: **{actual['sprint_p8']}**")

    if actual_parts:
        lines.append("**Results:** " + " · ".join(actual_parts))
    else:
        lines.append("_Results not set yet._")

    scored_label = ", ".join(scored_sessions.keys()) if scored_sessions else "none"
    lines.append(f"_Scored sessions: {scored_label}_")
    lines.append("")

    # Show per-user scores sorted by points
    rows = []
    for uid, entry in guild_entries.items():
        pts = int(round_scores.get(uid, 0) or 0)
        member = ctx.guild.get_member(int(uid))
        name = member.display_name if member else uid
        rows.append((pts, name))
    rows.sort(key=lambda x: (x[0], x[1].lower()), reverse=True)

    if rows:
        for pts, name in rows:
            lines.append(f"• {name} — **{pts} pts**")
    else:
        lines.append("No scores recorded.")

    msg = "\n".join(lines)
    if len(msg) > 1900:
        msg = msg[:1850] + "\n… (truncated)"
    await ctx.send(msg)

@bot.hybrid_command(name="prstats", aliases=["predstats", "myprstats"])
async def prstats(ctx, member: Optional[discord.Member] = None):
    """Show a user's prediction standings position and their best single-round score."""
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    target = member or ctx.author
    uid = str(target.id)
    totals = _pred_totals_for_guild(ctx.guild.id)
    if not totals or uid not in totals:
        name = target.display_name
        return await ctx.send(f"ℹ️ **{name}** has no prediction points yet.")
    # Build leaderboard to find rank
    ranked = sorted(totals.items(), key=lambda kv: int(kv[1] or 0), reverse=True)
    rank = next((i + 1 for i, (u, _) in enumerate(ranked) if u == uid), None)
    total_pts = int(totals.get(uid, 0) or 0)
    # Find best single-round score
    root = _predictions_root()
    rounds = root.get("rounds") or {}
    best_round_name = None
    best_round_pts = 0
    for rk, rnd in rounds.items():
        if not isinstance(rnd, dict):
            continue
        round_scores = (rnd.get("round_scores") or {}).get(str(ctx.guild.id)) or {}
        pts = int(round_scores.get(uid, 0) or 0)
        if pts > best_round_pts:
            best_round_pts = pts
            best_round_name = rnd.get("race_name") or rk
    name = target.display_name
    lines = [f"📊 **Prediction Stats — {name}**"]
    lines.append(f"🏆 Season rank: **#{rank}** of {len(ranked)} — **{total_pts} pts** total")
    if best_round_name:
        lines.append(f"⭐ Best round: **{best_round_name}** — **{best_round_pts} pts**")
    else:
        lines.append("⭐ Best round: no round scores recorded yet")
    await ctx.send("\n".join(lines), ephemeral=True)

@bot.command(name="predictionadjust", aliases=["predadjust"])
@commands.has_permissions(administrator=True)
async def predictionadjust(ctx, target: discord.Member, points: int, *, round_key: str = None):
    """Manually adjust a user's prediction score. Use positive or negative points.
    Optionally specify a round key (e.g. 2026-r3) to also update that round's record.
    Usage: !predictionadjust @user +5
           !predictionadjust @user -3 2026-r3
    """
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    uid = str(target.id)
    totals = _pred_totals_for_guild(ctx.guild.id)
    old_total = int(totals.get(uid, 0) or 0)
    new_total = old_total + points
    totals[uid] = new_total
    # If round_key provided, also update that round's per-round score record
    if round_key:
        rnd = _pred_round_obj(round_key.strip())
        round_scores = rnd.setdefault("round_scores", {}).setdefault(str(ctx.guild.id), {})
        old_round = int(round_scores.get(uid, 0) or 0)
        round_scores[uid] = old_round + points
    _save_state_quiet()
    sign = "+" if points >= 0 else ""
    rk_note = f" (round `{round_key.strip()}`)" if round_key else ""
    await ctx.send(
        f"✅ Adjusted **{target.display_name}**'s prediction score by **{sign}{points}**{rk_note}.\n"
        f"New season total: **{new_total} pts** (was {old_total})"
    )

# ----------------------------
# Reaction role handlers + periodic recovery
# ----------------------------
@bot.event
async def on_ready():
    global APP_COMMANDS_SYNCED
    logging.info(f"Bot is online as {bot.user}")
    if not hasattr(bot, "launch_time"):
        bot.launch_time = datetime.now()

    reload_config_state()
    try:
        init_runtime_db()
        migrated = migrate_alerts_from_state_json()
        if migrated:
            logging.info(f"[RuntimeDB] Migrated {migrated} alert rows from state.json")
    except Exception as e:
        logging.warning(f"[RuntimeDB] init/migration failed: {e}")
    setattr(bot, "of1_runtime_status_snapshot", _runtime_status_snapshot)
    setattr(bot, "of1_current_or_next_round_meta_coro", current_or_next_round_meta)
    setattr(bot, "of1_race_live_snapshot", of1_race_live_snapshot)
    setattr(bot, "of1_apply_race_setting", of1_apply_race_setting)
    setattr(bot, "of1_dashboard_send_to_thread", of1_dashboard_send_to_thread)
    setattr(bot, "of1_dashboard_kill_race_live", of1_dashboard_kill_race_live)
    setattr(bot, "of1_dashboard_start_race_live", of1_dashboard_start_race_live)
    setattr(bot, "of1_pred_snapshot", of1_pred_snapshot)
    setattr(bot, "of1_pred_set_result", of1_pred_set_result)

    ensure_standings_task_running()
    _ensure_background_task("PERIODIC_ROLE_RECOVERY_TASK", periodic_reaction_role_check, "Recovery")

    # XP flushing loop
    _ensure_background_task("XP_FLUSH_TASK", xp_flush_loop, "XP")

    # Race supervisor loop (your existing module)
    _ensure_background_task("RACE_SUPERVISOR_TASK", race_supervisor_loop, "RaceLive")

    # Race thread pre-creation loop (creates thread ~6 days before race)
    _ensure_background_task("RACE_PRECREATE_TASK", race_thread_precreate_loop, "RaceLive")

    # F1 reminders loop
    _ensure_background_task("F1_REMINDER_TASK", f1_reminder_loop, "F1Reminder")
    _ensure_background_task("RUNTIME_STATUS_TASK", runtime_status_loop, "RuntimeStatus")
    _ensure_background_task("DRIVER_CACHE_VALIDATION_TASK", driver_cache_validation_loop, "DriverCache")

    if not APP_COMMANDS_SYNCED:
        try:
            _guild_id_str = os.getenv("DISCORD_GUILD_ID", "").strip()
            if _guild_id_str:
                _guild_obj = discord.Object(id=int(_guild_id_str))
                bot.tree.copy_global_to(guild=_guild_obj)
                await bot.tree.sync(guild=_guild_obj)
                logging.info(f"[Slash] Command tree synced to guild {_guild_id_str}")
            # Clear global commands so nothing shows up twice
            bot.tree.clear_commands(guild=None)
            await bot.tree.sync()
            APP_COMMANDS_SYNCED = True
            logging.info("[Slash] Global commands cleared (guild-only mode)")
        except Exception as e:
            logging.error(f"[Slash] Command tree sync failed: {e}")

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id:
        return
    if payload.message_id not in allowed_reaction_panel_message_ids():
        return
    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return

    emoji_str = str(payload.emoji)
    role_name = resolve_role_name_from_emoji(emoji_str)
    if not role_name:
        return

    role = discord.utils.get(guild.roles, name=role_name)
    if role is None:
        logging.warning(f"[Roles] Role '{role_name}' not found in guild '{guild.name}'")
        return

    try:
        member = await guild.fetch_member(payload.user_id)
    except Exception as e:
        logging.warning(f"[Roles] Could not fetch member {payload.user_id}: {e}")
        return

    try:
        if role_name in color_role_names():
            roles_to_remove = [
                discord.utils.get(guild.roles, name=rname)
                for rname in color_role_names()
                if rname != role_name
            ]
            remove_list = [r for r in roles_to_remove if r and r in member.roles]
            if remove_list:
                await member.remove_roles(*remove_list)

        await member.add_roles(role)
        logging.info(f"[Roles] Assigned '{role_name}' to {member.name}")
    except Exception as e:
        logging.warning(f"[Roles] Failed assigning '{role_name}' to {member}: {e}")

@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if payload.message_id not in allowed_reaction_panel_message_ids():
        return
    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return

    emoji_str = str(payload.emoji)
    role_name = resolve_role_name_from_emoji(emoji_str)
    if not role_name:
        return

    role = discord.utils.get(guild.roles, name=role_name)
    if role is None:
        return

    try:
        member = await guild.fetch_member(payload.user_id)
    except Exception:
        return

    try:
        await member.remove_roles(role)
        logging.info(f"[Roles] Removed '{role_name}' from {member.name}")
    except Exception as e:
        logging.warning(f"[Roles] Failed removing '{role_name}' from {member}: {e}")

async def periodic_reaction_role_check():
    await bot.wait_until_ready()
    interval_min = 60

    while not bot.is_closed():
        _loop_tick("periodic_role_recovery")
        try:
            reload_config_state()
            try:
                interval_min = int(CFG.get("periodic_role_recovery_minutes", 60))
            except Exception:
                interval_min = 60
            interval_min = max(5, min(240, interval_min))

            for guild in bot.guilds:
                me = guild.me
                if me is None:
                    continue

                for panel_name, channel_id, message_id in reaction_panel_targets_for_guild(guild):
                    channel = guild.get_channel(channel_id)
                    if channel is None:
                        try:
                            channel = await guild.fetch_channel(channel_id)
                        except Exception as e:
                            logging.warning(f"[Recovery] Could not fetch channel {channel_id} for {panel_name} panel in {guild.name}: {e}")
                            continue
                    if not isinstance(channel, discord.TextChannel):
                        continue
                    perms = channel.permissions_for(me)
                    if not (perms.view_channel and perms.read_message_history):
                        continue
                    try:
                        message = await channel.fetch_message(message_id)
                    except Exception as e:
                        logging.warning(f"[Recovery] Could not fetch {panel_name} panel message {message_id} in {guild.name}: {e}")
                        continue
                    if message.author != bot.user:
                        continue

                    for reaction in message.reactions:
                        emoji = str(reaction.emoji).strip()
                        role_name = resolve_role_name_from_emoji(emoji)
                        if not role_name:
                            continue

                        role = discord.utils.get(guild.roles, name=role_name)
                        if not role:
                            continue

                        async for user in reaction.users():
                            if user.bot:
                                continue
                            try:
                                member = await guild.fetch_member(user.id)
                                if member and role not in member.roles:
                                    if role_name in color_role_names():
                                        roles_to_remove = [
                                            discord.utils.get(guild.roles, name=rname)
                                            for rname in color_role_names()
                                            if rname != role_name
                                        ]
                                        remove_list = [r for r in roles_to_remove if r and r in member.roles]
                                        if remove_list:
                                            await member.remove_roles(*remove_list)
                                    await member.add_roles(role)
                                    logging.info(f"[Recovery] Reassigned '{role_name}' to {member.name}")
                            except discord.Forbidden:
                                logging.warning(f"[Recovery] Forbidden fetching member {user.id} in {guild.name}")
                            except Exception as e:
                                logging.warning(f"[Recovery] Error user {user.id}: {e}")

        except Exception as e:
            _loop_error("periodic_role_recovery")
            logging.error(f"[Recovery] Loop error: {e}")

        await asyncio.sleep(interval_min * 60)

# ----------------------------
# XP awarding: on_message
# ----------------------------
@bot.event
async def on_message(message: discord.Message):
    # Always allow commands + ignore bots
    if message.author.bot:
        return

    # Award XP only in guild text channels
    if message.guild is not None and xp_enabled_for_guild(message.guild.id):
        try:
            gid = message.guild.id
            uid = message.author.id

            rec = get_user_record(XP_STATE, gid, uid)
            current_xp = int(rec.get("xp", 0) or 0)
            current_level = xp_level_from_total(current_xp)

            # Optional channel gate by min level
            blocked = await maybe_gate_channel(message, current_level)
            if blocked:
                # still process commands? (deleted message can't be a command anyway)
                return

            cd = xp_cooldown_seconds()
            if not is_on_cooldown(XP_STATE, gid, uid, cd):
                mn, mx = xp_gain_range()
                gain = random.randint(mn, mx)

                async with XP_SAVE_LOCK:
                    new_xp = add_user_xp(XP_STATE, gid, uid, gain)
                    new_level = xp_level_from_total(new_xp)

                    # store message meta + level
                    update_user_message_meta(XP_STATE, gid, uid)
                    set_user_xp_level(XP_STATE, gid, uid, xp=new_xp, level=new_level)
                    _xp_mark_dirty()

                if new_level > current_level:
                    # lightweight level-up ping
                    try:
                        await message.channel.send(f"✨ {message.author.mention} leveled up to **Level {new_level}**!")
                    except Exception:
                        pass

        except Exception as e:
            logging.error(f"[XP] on_message error: {e}")

    await bot.process_commands(message)

@bot.event
async def on_command_error(ctx, error):
    # Keep behavior simple: log + alert state. Individual commands can still catch and handle their own exceptions.
    cmd_name = getattr(getattr(ctx, "command", None), "qualified_name", None) or "(unknown)"
    err_name = type(error).__name__
    err_text = f"{err_name}: {error}"
    gid = getattr(getattr(ctx, "guild", None), "id", None)
    uid = getattr(getattr(ctx, "author", None), "id", None)

    if isinstance(error, commands.CommandNotFound):
        logging.warning(f"[CmdError] {cmd_name} by user={uid} guild={gid}: {err_text}")
        _record_alert("command_not_found", f"{cmd_name} -> {err_text}", guild_id=gid, user_id=uid, persist=False)
        return

    if isinstance(error, commands.CommandOnCooldown):
        retry = round(error.retry_after)
        await ctx.send(f"⏳ You're doing that too fast. Try again in **{retry}s**.", ephemeral=True)
        return

    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"⚠️ Missing argument: `{error.param.name}`. Use `/help` or `!help` for usage.", ephemeral=True)
        return

    if isinstance(error, commands.CheckFailure):
        logging.warning(f"[CmdError] check failure {cmd_name} by user={uid} guild={gid}: {err_text}")
        _record_alert("permission_error", f"{cmd_name} -> {err_text}", guild_id=gid, user_id=uid, persist=True)
        return

    logging.error(f"[CmdError] {cmd_name} by user={uid} guild={gid}: {err_text}")
    _record_alert("command_error", f"{cmd_name} -> {err_text}", guild_id=gid, user_id=uid, persist=True)

# ============================================================
# Race Live (OpenF1) + Kill Switch + Debug Tail (NO underscores)
# ============================================================

RACE_LIVE_TASKS: Dict[int, asyncio.Task] = {}
RACE_LIVE_ENABLED: Dict[int, bool] = {}
RACE_LIVE_ROUND_KEYS: Dict[int, str] = {}
RACE_LIVE_SESSION_KINDS: Dict[int, str] = {}
RACE_LIVE_LAST_EVENT_TS: Dict[int, str] = {}
RACE_LIVE_DEBUG: Dict[int, deque] = {}
RACE_LIVE_POSTED_SIGS: Dict[int, set] = {}
RACE_LIVE_POSTED_SIGS_ORDER: Dict[int, deque] = {}
# Dashboard-accessible state
RACE_LIVE_THREADS: Dict[int, Any] = {}          # gid -> discord.Thread
RACE_LIVE_SESSION_KEYS: Dict[int, int] = {}      # gid -> session_key
RACE_LIVE_DRIVER_MAPS: Dict[int, Dict[str, str]] = {}  # gid -> driver_map
RACE_CONTROL_FEED: Dict[int, deque] = {}         # gid -> rolling msg feed (maxlen 300)

def _race_feed_append(gid: int, ts: str, msg: str, status: str, emoji: str = "") -> None:
    """Append a race control message to the per-guild dashboard feed buffer."""
    RACE_CONTROL_FEED.setdefault(gid, deque(maxlen=300)).append({
        "ts": ts[11:19] if len(ts) >= 19 else ts,
        "msg": msg,
        "status": status,   # "posted" | "skipped" | "track_deletion" | "boundary"
        "emoji": emoji,
    })

def _race_live_delay_seconds() -> float:
    bucket = _state_bucket("race_live")
    try:
        v = float(bucket.get("delay_seconds", 0.0) or 0.0)
    except Exception:
        v = 0.0
    return max(0.0, min(60.0, v))

def _set_race_live_delay_seconds(seconds: float) -> float:
    v = max(0.0, min(60.0, float(seconds)))
    bucket = _state_bucket("race_live")
    bucket["delay_seconds"] = v
    _save_state_quiet()
    return v

def _race_live_poll_seconds() -> float:
    bucket = _state_bucket("race_live")
    raw = bucket.get("poll_seconds", None)
    if raw is None:
        raw = os.getenv("OPENF1_ACTIVE_POLL_SECONDS", "3")
    try:
        v = float(raw)
    except Exception:
        v = 3.0
    return max(1.0, min(15.0, v))

def _set_race_live_poll_seconds(seconds: float) -> float:
    v = max(1.0, min(15.0, float(seconds)))
    bucket = _state_bucket("race_live")
    bucket["poll_seconds"] = v
    _save_state_quiet()
    return v

def _race_control_emoji_for_message(msg: str) -> str:
    t = str(msg or "").lower()
    if "double yellow" in t:
        return "🟨"
    if "yellow" in t:
        return "🟨"
    if "purple" in t:
        return "🟣"
    if "virtual safety car" in t or " vsc" in t or t.startswith("vsc"):
        return "🟠"
    if t.startswith("safety car"):
        return "🟡"
    if "red flag" in t or t.startswith("red "):
        return "🔴"
    if "green flag" in t or "green light" in t or "lights out" in t or t.startswith("green"):
        return "🟢"
    if "CHEQUERED FLAG" in t or "checkered flag" in t or "session ended" in t:
        return "🏁"
    return "ℹ️"

def _race_control_should_post(msg: str) -> bool:
    t = str(msg or "").lower().strip()
    if not t:
        return False

    noisy_markers = (
        "medical car",
        "track clear",
        "track is clear",
        "track surface slippery",
        "yellow flag clear",
        "yellow flags clear",
        "maximum delta time",
        "failing to follow",
    )
    if any(x in t for x in noisy_markers):
        return False

    allow_markers = (
        "red flag",
        "green flag",
        "green light",
        "lights out",
        "chequered flag",
        "checkered flag",
        "session ended",
        "safety car",
        "virtual safety car",
        "vsc",
        "incident",
        "collision",
        "crash",
    )
    return any(x in t for x in allow_markers)

def _normalize_session_kind(session_type: str) -> str:
    st = str(session_type or "").upper().strip()
    # Check sprint variants first — OpenF1 returns session_type="Qualifying"/"Race"
    # for both regular and sprint sessions; session_name is the reliable discriminator.
    if "SPRINT" in st and ("QUALI" in st or "SHOOTOUT" in st):
        return "SPRINT_QUALI"
    if "SPRINT" in st:
        return "SPRINT"
    if st in {"QUALI", "QUALIFYING"}:
        return "QUALI"
    if st == "RACE":
        return "RACE"
    return st

def _race_live_ops_channel_id() -> int:
    bucket = _state_bucket("race_live")
    try:
        return int(bucket.get("ops_channel_id") or 0)
    except Exception:
        return 0

def _race_live_hold_map() -> Dict[str, bool]:
    bucket = _state_bucket("race_live")
    raw = bucket.get("manual_hold_guilds")
    if not isinstance(raw, dict):
        raw = {}
        bucket["manual_hold_guilds"] = raw
    return {str(k): bool(v) for k, v in raw.items()}

def _race_live_is_held(guild_id: int) -> bool:
    return bool(_race_live_hold_map().get(str(guild_id), False))

def _set_race_live_hold(guild_id: int, held: bool) -> None:
    bucket = _state_bucket("race_live")
    raw = bucket.get("manual_hold_guilds")
    if not isinstance(raw, dict):
        raw = {}
        bucket["manual_hold_guilds"] = raw
    raw[str(int(guild_id))] = bool(held)
    _save_state_quiet()

def _set_race_live_ops_channel_id(channel_id: int) -> int:
    bucket = _state_bucket("race_live")
    cid = int(channel_id or 0)
    bucket["ops_channel_id"] = cid
    _save_state_quiet()
    return cid

async def _send_race_live_ops_notice(guild: discord.Guild, message: str) -> None:
    cid = _race_live_ops_channel_id()
    if not cid:
        return
    ch = guild.get_channel(cid)
    if ch is None:
        try:
            ch = await guild.fetch_channel(cid)
        except Exception:
            return
    if isinstance(ch, (discord.TextChannel, discord.Thread)):
        try:
            await ch.send(message)
        except Exception:
            pass

def _extract_quali_segment(msg: str) -> Optional[str]:
    m = re.search(r"\b(SQ[123]|Q[123])\b", str(msg or "").upper())
    return m.group(1) if m else None

def _racelog(gid: int, msg: str) -> None:
    buf = RACE_LIVE_DEBUG.setdefault(gid, deque(maxlen=200))
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {msg}"
    buf.append(line)
    logging.info(f"[RaceLive][{gid}] {msg}")

def _racetail(gid: int, n: int = 20) -> str:
    buf = RACE_LIVE_DEBUG.get(gid) or deque()
    tail = list(buf)[-n:]
    return "\n".join(tail) if tail else "(no debug lines captured)"

def _race_sig_seen_or_add(gid: int, sig: str) -> bool:
    sigs = RACE_LIVE_POSTED_SIGS.setdefault(gid, set())
    if sig in sigs:
        return True
    order = RACE_LIVE_POSTED_SIGS_ORDER.setdefault(gid, deque())
    sigs.add(sig)
    order.append(sig)
    max_keep = 5000
    while len(order) > max_keep:
        old = order.popleft()
        sigs.discard(old)
    return False

async def _openf1_driver_name_map(http: aiohttp.ClientSession, session_key: int) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        rows = await _openf1_get(http, "drivers", {"session_key": session_key}, caller="race_live_drivers")
    except Exception:
        return out
    if not isinstance(rows, list):
        return out
    for r in rows:
        if not isinstance(r, dict):
            continue
        num = str(r.get("driver_number") or "").strip()
        if not num:
            continue
        label = (
            str(r.get("broadcast_name") or "").strip()
            or str(r.get("name_acronym") or "").strip()
            or str(r.get("full_name") or "").strip()
            or num
        )
        out[num] = label
    return out

async def _openf1_latest_positions(http: aiohttp.ClientSession, session_key: int) -> Dict[str, int]:
    try:
        rows = await _openf1_get(http, "position", {"session_key": session_key}, caller="race_live_positions")
    except Exception:
        return {}
    if not isinstance(rows, list):
        return {}

    latest: Dict[str, Tuple[str, int]] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        num = str(r.get("driver_number") or "").strip()
        if not num:
            continue
        try:
            pos = int(r.get("position"))
        except Exception:
            continue
        dt = str(r.get("date") or "")
        prev = latest.get(num)
        if prev is None or dt >= prev[0]:
            latest[num] = (dt, pos)
    return {num: pos for num, (_dt, pos) in latest.items()}

async def _post_quali_boundary_summary(
    thread: discord.Thread,
    http: aiohttp.ClientSession,
    session_key: int,
    session_kind: str,
    segment: str,
    driver_map: Dict[str, str],
) -> None:
    seg = str(segment or "").upper().strip()
    if seg not in {"Q1", "Q2", "Q3", "SQ1", "SQ2", "SQ3"}:
        return

    is_sprint_quali = session_kind == "SPRINT_QUALI"
    cutoff_title = "Sprint Qualifying" if is_sprint_quali else "Qualifying"

    def _name(num: str) -> str:
        return driver_map.get(num, f"#{num}")

    # Poll until positions stop changing for 20 consecutive polls (~60s).
    # Don't gate on an expected driver count — F1 grids vary and checking
    # count caused the function to silently time out and post nothing.
    ordered: List[Tuple[str, int]] = []
    prev_snapshot: Optional[List[Tuple[str, int]]] = None
    stable_count = 0
    for _ in range(100):  # up to ~300s at 3s intervals
        positions = await _openf1_latest_positions(http, session_key)
        if positions:
            current = sorted(positions.items(), key=lambda kv: kv[1])
            ordered = current  # always keep latest snapshot
            if current == prev_snapshot:
                stable_count += 1
                if stable_count >= 20:  # unchanged for 20 consecutive polls (~60s)
                    break
            else:
                stable_count = 0
                prev_snapshot = current
        await asyncio.sleep(3)

    if not ordered:
        return

    # With up to 22 cars: Q1/SQ1 knocks out P17-P22 (6 cars),
    # Q2/SQ2 knocks out P11-P16 (6 cars), Q3/SQ3 sets top 10.
    if seg in {"Q1", "SQ1"}:
        knocked = [(num, pos) for num, pos in ordered if pos >= 17]
        if not knocked:
            return
        body = "\n".join(f"P{pos} {_name(num)}" for num, pos in knocked[:6])
        await thread.send(_wrap_spoiler(f"🚫 {cutoff_title} {seg} Knockouts\n{body}"))
        return

    if seg in {"Q2", "SQ2"}:
        knocked = [(num, pos) for num, pos in ordered if 11 <= pos <= 16]
        if not knocked:
            return
        body = "\n".join(f"P{pos} {_name(num)}" for num, pos in knocked[:6])
        await thread.send(_wrap_spoiler(f"🚫 {cutoff_title} {seg} Knockouts\n{body}"))
        return

    # Q3 / SQ3
    top10 = [(num, pos) for num, pos in ordered if 1 <= pos <= 10]
    if not top10:
        return
    body = "\n".join(f"P{pos} {_name(num)}" for num, pos in top10[:10])
    await thread.send(_wrap_spoiler(f"📊 {cutoff_title} Top 10\n{body}"))

async def _post_race_or_sprint_final_summary(
    thread: discord.Thread,
    http: aiohttp.ClientSession,
    session_key: int,
    session_kind: str,
    driver_map: Dict[str, str],
) -> bool:
    if session_kind == "SPRINT":
        top_n = 8
        title = "Sprint Final Classification (Top 8)"
    else:
        top_n = 10
        title = "Race Final Classification (Top 10)"

    rows: List[Tuple[str, int]] = []
    prev_snapshot: Optional[List[Tuple[str, int]]] = None
    stable_count = 0
    # Wait until all top-N positions are filled AND the order has stopped changing
    # for 20 consecutive polls (~60s), so late finishers are accounted for.
    for _ in range(100):  # up to ~300s at 3s intervals
        positions = await _openf1_latest_positions(http, session_key)
        if positions:
            ordered = sorted(positions.items(), key=lambda kv: kv[1])
            snapshot = [(num, pos) for num, pos in ordered if 1 <= pos <= top_n]
            pos_set = {pos for _num, pos in snapshot}
            if len(pos_set) >= top_n:
                rows = snapshot
                if snapshot == prev_snapshot:
                    stable_count += 1
                    if stable_count >= 20:  # unchanged for 20 consecutive polls (~60s)
                        break
                else:
                    stable_count = 0
                    prev_snapshot = snapshot
        await asyncio.sleep(3)

    if not rows:
        return False

    body = "\n".join(f"P{pos} {driver_map.get(num, f'#{num}')}" for num, pos in rows[:top_n])
    await thread.send(_wrap_spoiler(f"📊 {title}\n{body}"))
    return True

async def _openf1_get(
    http: aiohttp.ClientSession,
    endpoint: str,
    params: Dict[str, Any],
    caller: str = "race_live",
) -> Any:
    cooldown_s = _openf1_get_endpoint_cooldown_remaining(endpoint)
    if cooldown_s > 0:
        raise RuntimeError(f"OpenF1 endpoint cooldown active for {endpoint} ({cooldown_s}s)")

    url = f"{OPENF1_BASE}/{endpoint.lstrip('/')}"
    for attempt in range(2):
        force_refresh = bool(attempt == 1)
        t0 = time.time()
        async with http.get(
            url,
            params=params,
            headers=_openf1_auth_headers(force_refresh=force_refresh),
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            latency_ms = int((time.time() - t0) * 1000)
            _openf1_trace_record(
                caller=str(caller or "race_live"),
                endpoint=str(endpoint or ""),
                status_code=int(r.status),
                latency_ms=latency_ms,
            )
            if r.status in (401, 403):
                if attempt == 0:
                    await r.read()
                    continue
                text = await r.text()
                raise RuntimeError(f"OpenF1 auth error {r.status}: {text[:200]}")
            if int(r.status) == 429:
                retry_after = int(r.headers.get("Retry-After", "60") or 60)
                _openf1_set_endpoint_cooldown(endpoint, retry_after)
            elif int(r.status) == 503:
                _openf1_set_endpoint_cooldown(endpoint, 15)
            r.raise_for_status()
            return await r.json()
    raise RuntimeError("OpenF1 auth retry exhausted.")

def _session_type_upper(s: Dict[str, Any]) -> str:
    return str(s.get("session_type") or s.get("session_name") or "").upper().strip()

FOLLOW_SESSION_TYPES = {
    "RACE",
    "QUALIFYING",
    "QUALI",
    "SPRINT",
    "SPRINT QUALIFYING",
    "SPRINT SHOOTOUT",
}

def _parse_iso(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str)

def _race_threads_root() -> Dict[str, Any]:
    root = _state_bucket("race_threads")
    rounds = root.get("rounds")
    if not isinstance(rounds, dict):
        rounds = {}
        root["rounds"] = rounds
    return root

def _race_thread_round_obj(round_key: str) -> Dict[str, Any]:
    root = _race_threads_root()
    rounds = root["rounds"]
    if round_key not in rounds or not isinstance(rounds.get(round_key), dict):
        rounds[round_key] = {"guilds": {}}
    obj = rounds[round_key]
    guilds = obj.get("guilds")
    if not isinstance(guilds, dict):
        guilds = {}
        obj["guilds"] = guilds
    return obj

def _race_thread_record(round_key: str, guild_id: int) -> Optional[Dict[str, Any]]:
    obj = _race_thread_round_obj(round_key)
    rec = (obj.get("guilds") or {}).get(str(guild_id))
    return rec if isinstance(rec, dict) else None

def _save_race_thread_record(
    round_key: str,
    race_name: str,
    guild_id: int,
    thread: discord.Thread,
    source: str,
) -> None:
    obj = _race_thread_round_obj(round_key)
    guilds = obj["guilds"]
    guilds[str(guild_id)] = {
        "thread_id": int(thread.id),
        "parent_channel_id": int(thread.parent_id or 0),
        "thread_name": thread.name,
        "source": (source or "auto"),
        "weekend_state": "queued" if (source or "auto") == "manual" else "active",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if race_name:
        obj["race_name"] = race_name
    _save_state_quiet()

def _set_race_thread_weekend_state(round_key: str, guild_id: int, weekend_state: str) -> None:
    rec = _race_thread_record(round_key, guild_id)
    if not rec:
        return
    new_state = str(weekend_state or "").strip().lower()
    if new_state not in {"queued", "active", "past"}:
        return
    if str(rec.get("weekend_state") or "") == new_state:
        return
    rec["weekend_state"] = new_state
    now_iso = datetime.now(timezone.utc).isoformat()
    if new_state == "active":
        rec["activated_at"] = now_iso
    elif new_state == "past":
        rec["past_at"] = now_iso
    _save_state_quiet()

def _clear_race_thread_record(round_key: str, guild_id: int) -> None:
    obj = _race_thread_round_obj(round_key)
    guilds = obj.get("guilds") or {}
    if str(guild_id) in guilds:
        del guilds[str(guild_id)]
        _save_state_quiet()

async def _fetch_saved_race_thread(guild: discord.Guild, round_key: str) -> Optional[discord.Thread]:
    rec = _race_thread_record(round_key, guild.id)
    if not rec:
        return None
    thread_id = int(rec.get("thread_id") or 0)
    if not thread_id:
        return None

    th = guild.get_thread(thread_id)
    if isinstance(th, discord.Thread):
        return th

    try:
        fetched = await guild.fetch_channel(thread_id)
        if isinstance(fetched, discord.Thread):
            return fetched
    except discord.NotFound:
        # Thread was genuinely deleted — safe to forget it.
        _clear_race_thread_record(round_key, guild.id)
    except Exception:
        # Transient error (rate-limit, network, permissions issue) — keep the
        # saved record so we can try again on the next supervisor tick rather
        # than silently nuking it and creating a duplicate thread.
        pass

    return None

def _normalize_race_name_key(value: str) -> str:
    s = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()
    return re.sub(r"\s+", " ", s)

async def _fetch_fallback_race_thread_for_guild(
    guild: discord.Guild,
    preferred_race_name: str,
) -> Optional[Tuple[str, discord.Thread]]:
    root = _race_threads_root()
    rounds = root.get("rounds") or {}
    if not isinstance(rounds, dict):
        return None

    gid = str(guild.id)
    preferred_key = _normalize_race_name_key(preferred_race_name)
    candidates: List[Tuple[int, str, Dict[str, Any]]] = []

    for rk, robj in rounds.items():
        if not isinstance(robj, dict):
            continue
        guilds = robj.get("guilds") or {}
        if not isinstance(guilds, dict):
            continue
        rec = guilds.get(gid)
        if not isinstance(rec, dict):
            continue
        weekend_state = str(rec.get("weekend_state") or "").lower()
        if weekend_state == "past":
            continue

        race_name = str(robj.get("race_name") or "")
        race_key = _normalize_race_name_key(race_name)
        score = 0
        if preferred_key and race_key:
            if preferred_key == race_key:
                score += 200
            elif preferred_key in race_key or race_key in preferred_key:
                score += 120
        if weekend_state == "queued":
            score += 40
        elif weekend_state == "active":
            score += 20
        try:
            created_at = str(rec.get("created_at") or "")
            if created_at:
                score += int(_parse_iso(created_at).timestamp() // 86400)
        except Exception:
            pass
        candidates.append((score, str(rk), rec))

    for _, round_key, rec in sorted(candidates, key=lambda x: x[0], reverse=True):
        thread_id = int(rec.get("thread_id") or 0)
        if not thread_id:
            continue
        th = guild.get_thread(thread_id)
        if isinstance(th, discord.Thread):
            return round_key, th
        try:
            fetched = await guild.fetch_channel(thread_id)
            if isinstance(fetched, discord.Thread):
                return round_key, fetched
        except discord.NotFound:
            _clear_race_thread_record(round_key, guild.id)
            continue
        except Exception:
            # Transient error — don't delete the record, just skip this candidate.
            continue
    return None

async def _get_forum_channel_live(guild: discord.Guild) -> Optional[discord.abc.GuildChannel]:
    forum_id = os.getenv("RACE_FORUM_CHANNEL_ID")
    if not forum_id:
        return None
    try:
        ch = guild.get_channel(int(forum_id))
        if ch is None:
            ch = await guild.fetch_channel(int(forum_id))
        return ch
    except Exception as e:
        logging.error(f"[RaceLive] Could not fetch forum channel {forum_id}: {e}")
        return None

async def _create_race_thread(
    guild: discord.Guild,
    title: str,
    opener_text: str,
    opener_file: Optional[discord.File] = None,
) -> discord.Thread:
    ch = await _get_forum_channel_live(guild)
    if ch is None:
        raise RuntimeError("RACE_FORUM_CHANNEL_ID not set or not accessible.")

    if isinstance(ch, discord.ForumChannel):
        create_kwargs: Dict[str, Any] = {
            "name": title,
            "content": opener_text,
            "auto_archive_duration": 1440,
        }
        if opener_file is not None:
            create_kwargs["file"] = opener_file
        created = await ch.create_thread(**create_kwargs)
        if isinstance(created, tuple) and len(created) >= 1:
            return created[0]
        return created

    if isinstance(ch, discord.TextChannel):
        msg = await ch.send(f"Race Thread: **{title}**")
        thread = await msg.create_thread(name=title, auto_archive_duration=1440)
        if opener_file is not None:
            await thread.send(opener_text, file=opener_file)
        else:
            await thread.send(opener_text)
        return thread

    raise RuntimeError("RACE_FORUM_CHANNEL_ID must point to a ForumChannel or TextChannel.")

async def _ensure_live_thread(
    guild: discord.Guild,
    round_key: str,
    race_name: str,
    title: str,
) -> discord.Thread:
    has_saved_record = bool(_race_thread_record(round_key, guild.id))
    existing = await _fetch_saved_race_thread(guild, round_key)
    if existing is not None:
        _set_race_thread_weekend_state(round_key, guild.id, "active")
        return existing

    # If a record existed for this round_key but we couldn't reach the thread,
    # it's a transient error (rate-limit, network hiccup).  Raise so the
    # supervisor retries on the next tick rather than creating a duplicate thread.
    if has_saved_record:
        raise RuntimeError(
            f"Saved thread record exists for {round_key} but thread is temporarily "
            "unreachable (rate-limit or network). Will retry on the next supervisor tick."
        )

    fallback = await _fetch_fallback_race_thread_for_guild(guild, race_name)
    if fallback is not None:
        fallback_round_key, fallback_thread = fallback
        if fallback_round_key != round_key:
            fallback_rec = _race_thread_record(fallback_round_key, guild.id) or {}
            source = str(fallback_rec.get("source") or "manual")
            _clear_race_thread_record(fallback_round_key, guild.id)
            _save_race_thread_record(
                round_key=round_key,
                race_name=race_name,
                guild_id=guild.id,
                thread=fallback_thread,
                source=source,
            )
        _set_race_thread_weekend_state(round_key, guild.id, "active")
        return fallback_thread

    # Guard: if the round_key looks like a fallback value it means the OpenF1
    # schedule API was unavailable when the supervisor ran.  Creating a thread
    # now would produce a garbage title like "2025-round-unknown - Live Weekend"
    # and prevent the real pre-created thread from ever being found.  Raise so
    # the supervisor skips this tick and retries once the API recovers.
    if "unknown" in round_key.lower():
        raise RuntimeError(
            f"Refusing to create race thread: round key is '{round_key}' "
            "(OpenF1 schedule API may be temporarily unavailable). "
            "Will retry on the next supervisor tick."
        )

    created = await _create_race_thread(
        guild=guild,
        title=title,
        opener_text=f"Live thread created by {bot.user.mention}",
        opener_file=None,
    )
    _save_race_thread_record(
        round_key=round_key,
        race_name=race_name,
        guild_id=guild.id,
        thread=created,
        source="auto",
    )
    _set_race_thread_weekend_state(round_key, guild.id, "active")
    return created

async def _pick_current_meeting_and_window(http: aiohttp.ClientSession) -> Optional[tuple[datetime, datetime, Dict[str, Any], list, Dict[str, Any]]]:
    latest = await _openf1_get(http, "sessions", {"session_key": "latest"}, caller="race_supervisor_latest")
    if not latest:
        return None

    if not _openf1_is_f1_session(latest[0]):
        return None

    latest_session = latest[0]
    meeting_key = latest_session.get("meeting_key")
    if not meeting_key:
        return None

    all_sessions = await _openf1_get(http, "sessions", {"meeting_key": meeting_key}, caller="race_supervisor_meeting_sessions")
    if not all_sessions:
        return None

    relevant = [s for s in all_sessions if _openf1_is_f1_session(s) and _session_type_upper(s) in FOLLOW_SESSION_TYPES]
    if not relevant:
        return None

    starts = [_parse_iso(s["date_start"]) for s in relevant if s.get("date_start")]
    ends = [_parse_iso(s["date_end"]) for s in relevant if s.get("date_end")]
    if not starts or not ends:
        return None

    legacy_pad = int(os.getenv("RACE_WINDOW_PADDING_HOURS", "24"))
    pre_hours = int(os.getenv("OPENF1_PRE_WEEKEND_BUFFER_HOURS", str(legacy_pad)))
    post_hours = int(os.getenv("OPENF1_POST_WEEKEND_BUFFER_HOURS", "12"))
    pre_pad = timedelta(hours=max(0, min(72, pre_hours)))
    post_pad = timedelta(hours=max(0, min(72, post_hours)))

    window_start = min(starts) - pre_pad
    window_end = max(ends) + post_pad

    meta = relevant[0]
    return window_start, window_end, meta, relevant, latest_session

async def _post_deferred_quali_boundary(
    gid: int,
    thread: discord.Thread,
    http: aiohttp.ClientSession,
    session_key: int,
    session_kind: str,
    current_quali_seg: str,
    posted_segment_summaries: set,
    seen_session_end: bool,
    driver_map: dict,
) -> None:
    """Post a qualifying boundary summary that was waiting for SESSION STARTED but never fired."""
    if not (session_kind in {"QUALI", "SPRINT_QUALI"} and seen_session_end and current_quali_seg not in {"Q3", "SQ3"}):
        return
    key = f"{current_quali_seg}:end"
    if key in posted_segment_summaries:
        return
    posted_segment_summaries.add(key)
    _racelog(gid, f"posting deferred boundary for {current_quali_seg}")
    try:
        await _post_quali_boundary_summary(
            thread=thread,
            http=http,
            session_key=session_key,
            session_kind=session_kind,
            segment=current_quali_seg,
            driver_map=driver_map,
        )
    except Exception as e:
        _racelog(gid, f"deferred boundary failed for {current_quali_seg}: {e}")


async def race_live_loop(guild: discord.Guild, thread: discord.Thread, session_key: int, session_type: str = ""):
    gid = guild.id
    RACE_LIVE_ENABLED[gid] = True
    RACE_LIVE_POSTED_SIGS.setdefault(gid, set())
    RACE_LIVE_POSTED_SIGS_ORDER.setdefault(gid, deque())
    RACE_LIVE_THREADS[gid] = thread
    RACE_LIVE_SESSION_KEYS[gid] = session_key

    poll_s = _race_live_poll_seconds()
    session_kind = _normalize_session_kind(session_type)

    _racelog(
        gid,
        f"race_live_loop started (session_key={session_key}, session={session_kind or session_type or 'UNKNOWN'}, poll={poll_s}s)",
    )
    await _send_race_live_ops_notice(
        guild,
        f"Live follower attached: thread={thread.mention} session_key={session_key} session={session_kind or session_type or 'UNKNOWN'}",
    )

    # Only process race_control messages that arrive after this loop starts,
    # so that reconnects or late-starts don't replay the entire session history.
    loop_start_dt = datetime.now(timezone.utc).isoformat()

    async with aiohttp.ClientSession(headers={"User-Agent": "OF1-Discord-Bot"}) as http:
        driver_map = await _openf1_driver_name_map(http, session_key)
        RACE_LIVE_DRIVER_MAPS[gid] = driver_map
        posted_segment_summaries: set[str] = set()
        posted_final_summary = False
        current_quali_seg = "SQ1" if session_kind == "SPRINT_QUALI" else "Q1"
        # Set when a segment ends; cleared when SESSION STARTED fires the boundary post.
        # This lets us post the boundary on SESSION STARTED (next segment beginning)
        # instead of on CHEQUERED FLAG, because OpenF1 sends SESSION FINISHED *before*
        # CHEQUERED FLAG at the same timestamp — the old approach stopped the loop on
        # SESSION FINISHED before CHEQUERED FLAG was ever processed.
        _seen_session_end_since_last_boundary: bool = False
        pred_reminders_posted: set[str] = set()  # tracks which lock reminders we've sent

        # For qualifying sessions, scan historical race_control once at loop start
        # to initialize segment state.  This handles late-start cases where the loop
        # begins mid-session and some segment transitions already occurred with
        # timestamps that would be filtered by loop_start_dt in the main loop.
        # We don't post anything from this scan — state init only.
        if session_kind in {"QUALI", "SPRINT_QUALI"}:
            try:
                _hist_rc = await _openf1_get(http, "race_control", {"session_key": session_key}, caller="race_live_quali_init")
                for _hi in _hist_rc:
                    _hm = str(_hi.get("message") or "").strip()
                    if not _hm:
                        continue
                    _hu = _hm.upper()
                    _hs = _extract_quali_segment(_hm)
                    if _hs:
                        current_quali_seg = _hs
                    if "SESSION STARTED" in _hu:
                        _seen_session_end_since_last_boundary = False
                    if any(_k in _hu for _k in ("CHECKERED", "CHEQUERED", "SESSION END", "SESSION FINISHED")):
                        if current_quali_seg not in {"Q3", "SQ3"}:
                            _seen_session_end_since_last_boundary = True
                        else:
                            _seen_session_end_since_last_boundary = False
                _racelog(gid, f"quali init: seg={current_quali_seg}, pending_boundary={_seen_session_end_since_last_boundary}")
            except Exception as _e:
                _racelog(gid, f"quali init scan failed (non-fatal): {_e}")

        while RACE_LIVE_ENABLED.get(gid, False):
            _loop_tick("race_live")
            try:
                poll_s = _race_live_poll_seconds()
                _racelog(gid, "poll race_control")
                rc = await _openf1_get(http, "race_control", {"session_key": session_key}, caller="race_live_race_control")
                _racelog(gid, f"race_control items={len(rc)}")

                stop_requested = False
                for item in rc:
                    msg = str(item.get("message") or "").strip()
                    if not msg:
                        continue
                    dt = str(item.get("date") or "")
                    # Skip messages that predate this loop session (prevents
                    # replaying history on reconnect and catches all new messages,
                    # not just the last 30).
                    if dt and dt < loop_start_dt:
                        continue
                    sig = f"{dt}|{msg}"
                    if _race_sig_seen_or_add(gid, sig):
                        continue

                    upper_msg = msg.upper()
                    session_end = (
                        ("CHECKERED" in upper_msg)
                        or ("CHEQUERED" in upper_msg)
                        or ("SESSION END" in upper_msg)
                        or ("SESSION FINISHED" in upper_msg)
                    )

                    will_post = _race_control_should_post(msg)
                    feed_handled = False  # ensures exactly one _race_feed_append per message

                    if will_post:
                        delay_s = _race_live_delay_seconds()
                        if delay_s > 0:
                            await asyncio.sleep(delay_s)
                        emoji = _race_control_emoji_for_message(msg)
                        await thread.send(f"{emoji} {msg}")
                        RACE_LIVE_LAST_EVENT_TS[gid] = datetime.now(timezone.utc).isoformat()
                        _race_feed_append(gid, dt, msg, "posted", emoji)
                        feed_handled = True

                    if session_kind in {"QUALI", "SPRINT_QUALI"}:
                        seg = _extract_quali_segment(msg)
                        # Keep current_quali_seg up to date whenever a segment is mentioned
                        if seg:
                            current_quali_seg = seg

                        # SESSION STARTED = the previous segment has fully ended.
                        # Post that segment's boundary here rather than on CHEQUERED FLAG,
                        # because OpenF1 delivers SESSION FINISHED *before* CHEQUERED FLAG
                        # at identical timestamps — waiting for CHEQUERED FLAG meant the
                        # old explicit_end check stopped the loop first.
                        if "SESSION STARTED" in upper_msg and _seen_session_end_since_last_boundary:
                            closing_seg = current_quali_seg
                            key = f"{closing_seg}:end"
                            if key not in posted_segment_summaries:
                                posted_segment_summaries.add(key)
                                _seg_next = {"Q1": "Q2", "Q2": "Q3", "Q3": "Q3", "SQ1": "SQ2", "SQ2": "SQ3", "SQ3": "SQ3"}
                                current_quali_seg = _seg_next.get(closing_seg, closing_seg)
                                try:
                                    await _post_quali_boundary_summary(
                                        thread=thread,
                                        http=http,
                                        session_key=session_key,
                                        session_kind=session_kind,
                                        segment=closing_seg,
                                        driver_map=driver_map,
                                    )
                                except Exception as e:
                                    _racelog(gid, f"quali summary failed for {closing_seg}: {e}")
                            _seen_session_end_since_last_boundary = False

                        # Post track limit / lap time deletion messages during qualifying.
                        # These are filtered by _race_control_should_post but we still
                        # want them surfaced in the thread with a dedicated emoji.
                        if not feed_handled:
                            lower_msg = msg.lower()
                            is_track_deletion = any(p in lower_msg for p in ("track limits", "lap time deleted", "time deleted", "lap deleted"))
                            if is_track_deletion:
                                delay_s = _race_live_delay_seconds()
                                if delay_s > 0:
                                    await asyncio.sleep(delay_s)
                                await thread.send(f"🚫 {msg}")
                                RACE_LIVE_LAST_EVENT_TS[gid] = datetime.now(timezone.utc).isoformat()
                                _race_feed_append(gid, dt, msg, "track_deletion", "🚫")
                                feed_handled = True

                        if session_end:
                            if current_quali_seg in {"Q3", "SQ3"}:
                                # Final segment ended — post its boundary and stop
                                key = f"{current_quali_seg}:end"
                                if key not in posted_segment_summaries:
                                    posted_segment_summaries.add(key)
                                    try:
                                        await _post_quali_boundary_summary(
                                            thread=thread,
                                            http=http,
                                            session_key=session_key,
                                            session_kind=session_kind,
                                            segment=current_quali_seg,
                                            driver_map=driver_map,
                                        )
                                    except Exception as e:
                                        _racelog(gid, f"quali summary failed for {current_quali_seg}: {e}")
                                stop_requested = True
                                _racelog(gid, "session end detected in quali (final segment); stopping live loop")
                                round_key = str(RACE_LIVE_ROUND_KEYS.get(gid) or "")
                                if round_key:
                                    delay_min = int(os.getenv("PRED_AUTOSCORE_DELAY_MINUTES", "30"))
                                    asyncio.create_task(_delayed_prediction_autoscore(
                                        guild, thread, round_key, session_kind, session_key, dict(driver_map), delay_min
                                    ))
                                break
                            else:
                                # Intermediate segment ended — SESSION STARTED will trigger boundary
                                _seen_session_end_since_last_boundary = True
                                _racelog(gid, f"segment end ({current_quali_seg}), waiting for SESSION STARTED to post boundary")

                    elif session_kind in {"RACE", "SPRINT"}:
                        if session_end and not posted_final_summary:
                            posted_final_summary = True
                            try:
                                ok = await _post_race_or_sprint_final_summary(
                                    thread=thread,
                                    http=http,
                                    session_key=session_key,
                                    session_kind=session_kind,
                                    driver_map=driver_map,
                                )
                                _racelog(gid, f"final summary posted={ok}")
                            except Exception as e:
                                _racelog(gid, f"final summary failed: {e}")
                            # Schedule delayed auto-scoring (30 min default to catch penalty/lap deletions)
                            round_key = str(RACE_LIVE_ROUND_KEYS.get(gid) or "")
                            if round_key:
                                delay_min = int(os.getenv("PRED_AUTOSCORE_DELAY_MINUTES", "30"))
                                asyncio.create_task(_delayed_prediction_autoscore(
                                    guild, thread, round_key, session_kind, session_key, dict(driver_map), delay_min
                                ))
                            stop_requested = True
                            _racelog(gid, "session end detected in race/sprint; stopping live loop")
                            break

                    # Catch-all: if no session-kind handler claimed this message, mark skipped
                    if not feed_handled:
                        _race_feed_append(gid, dt, msg, "skipped")

                if stop_requested:
                    RACE_LIVE_ENABLED[gid] = False
                    break

                # --- Pre-lock prediction reminders ---
                try:
                    _pred_meta = await _prediction_round_context()
                    _pred_req = _prediction_session_requirements(_pred_meta)
                    now_utc = datetime.now(timezone.utc)
                    remind_window = timedelta(minutes=35)  # post reminder within 35min of lock
                    remind_cats = [
                        ("pole", "🏎️ **Pole prediction** (`!predictpole`) locks in ~30 min!"),
                        ("podium", "🏆 **Podium prediction** (`!predictpodium`) locks in ~30 min!"),
                        ("p10", "🔟 **P10 prediction** (`!predictp10`) locks in ~30 min!"),
                    ]
                    if "sprint_quali" in _pred_req:
                        remind_cats.append(("sprint_pole", "🏎️ **Sprint Pole prediction** (`!predictsprintpole`) locks in ~30 min!"))
                    if "sprint" in _pred_req:
                        remind_cats.append(("sprint_podium", "🏆 **Sprint Podium prediction** (`!predictsprintpodium`) locks in ~30 min!"))
                        remind_cats.append(("sprint_p8", "8️⃣ **Sprint P8 prediction** (`!predictsprintp8`) locks in ~30 min!"))
                    for cat, remind_msg in remind_cats:
                        if cat in pred_reminders_posted:
                            continue
                        if _prediction_category_locked(_pred_meta, cat):
                            pred_reminders_posted.add(cat)
                            continue
                        lock_dt = _prediction_lock_dt(_pred_meta, cat)
                        if lock_dt and timedelta(0) < (lock_dt - now_utc) <= remind_window:
                            await thread.send(f"⏰ {remind_msg}")
                            pred_reminders_posted.add(cat)
                except Exception:
                    pass  # never let reminder logic crash the live loop

                await asyncio.sleep(poll_s)

            except asyncio.CancelledError:
                _racelog(gid, "race_live_loop cancelled")
                await _post_deferred_quali_boundary(
                    gid, thread, http, session_key, session_kind, current_quali_seg,
                    posted_segment_summaries, _seen_session_end_since_last_boundary, driver_map,
                )
                raise
            except Exception as e:
                _loop_error("race_live")
                _racelog(gid, f"ERROR {type(e).__name__}: {e}")
                await asyncio.sleep(5)

        # Natural exit (RACE_LIVE_ENABLED set to False by supervisor).
        # Post any boundary that was waiting for SESSION STARTED.
        await _post_deferred_quali_boundary(
            gid, thread, http, session_key, session_kind, current_quali_seg,
            posted_segment_summaries, _seen_session_end_since_last_boundary, driver_map,
        )

    _racelog(gid, "race_live_loop exited")

async def race_thread_precreate_loop():
    """
    Runs every 6 hours. When a race is within RACE_THREAD_CREATE_DAYS (default 6)
    days, automatically creates the race thread for each guild if one doesn't exist yet.
    This means the thread is up by Monday of race week for a Sunday race.
    Manual pre-creation via /racethread is always respected and won't be overwritten.
    """
    await bot.wait_until_ready()
    logging.info("[RaceLive] Thread pre-creation loop started")

    while not bot.is_closed():
        try:
            pre_create_days = max(1, int(os.getenv("RACE_THREAD_CREATE_DAYS", "6")))
            now = datetime.now(timezone.utc)
            round_meta = await current_or_next_round_meta()
            race_dt = round_meta.get("race_dt")
            round_key = str(round_meta.get("key") or "unknown-round")
            race_name = str(round_meta.get("race_name") or "").strip()

            if race_dt is not None:
                days_until = (race_dt - now).total_seconds() / 86400
                if 0 < days_until <= pre_create_days:
                    title = f"{race_name} - Race Weekend" if race_name else "F1 Race Weekend"
                    for guild in bot.guilds:
                        try:
                            existing_rec = _race_thread_record(round_key, guild.id) or {}
                            if existing_rec.get("thread_id"):
                                continue  # thread already exists, skip
                            thread = await _ensure_live_thread(guild, round_key, race_name or "F1", title)
                            logging.info(f"[RaceLive] Pre-created race thread for guild {guild.id}: {round_key} ({days_until:.1f} days before race)")
                            await _send_race_live_ops_notice(guild, f"Race thread pre-created: {thread.mention} ({days_until:.0f} days until race)")
                        except Exception as e:
                            logging.warning(f"[RaceLive] Thread pre-creation failed for guild {guild.id}: {e}")
        except Exception as e:
            logging.error(f"[RaceLive] Thread pre-creation loop error: {e}")

        await asyncio.sleep(6 * 3600)  # check every 6 hours

async def driver_cache_validation_loop():
    """Runs every 6 hours and sanity-checks the local driver cache.

    Checks:
    - At least 18 drivers present (full F1 grid).
    - No driver is missing a name.
    - No driver has negative points.
    - At least one driver has points > 0 (catches a fully-zeroed cache).

    If any check fails an ops notice is sent to every guild so you know
    something has gone wrong without having to spot it yourself.
    """
    await bot.wait_until_ready()
    logging.info("[DriverCache] Validation loop started")

    while not bot.is_closed():
        await asyncio.sleep(6 * 3600)
        try:
            cache = _load_driver_cache()
            drivers = cache.get("drivers") or {}
            issues: List[str] = []

            driver_count = len(drivers)
            if driver_count < 18:
                issues.append(f"Only {driver_count} drivers in cache (expected ≥ 18).")

            missing_names = [num for num, d in drivers.items() if not d.get("name")]
            if missing_names:
                issues.append(f"{len(missing_names)} driver(s) have no name: {', '.join(missing_names)}.")

            negative_pts = [
                d.get("name", f"#{num}")
                for num, d in drivers.items()
                if int(d.get("points", 0) or 0) < 0
            ]
            if negative_pts:
                issues.append(f"Negative points detected for: {', '.join(negative_pts)}.")

            all_zero = all(int(d.get("points", 0) or 0) == 0 for d in drivers.values())
            if drivers and all_zero:
                issues.append("All drivers have 0 points — cache may not have been populated yet.")

            if issues:
                msg = "⚠️ **Driver cache validation failed:**\n" + "\n".join(f"- {i}" for i in issues)
                logging.warning(f"[DriverCache] Validation issues: {issues}")
                for guild in bot.guilds:
                    await _send_race_live_ops_notice(guild, msg)
            else:
                logging.info(f"[DriverCache] Validation OK ({driver_count} drivers).")
        except Exception as e:
            logging.error(f"[DriverCache] Validation loop error: {e}")

async def race_supervisor_loop():
    await bot.wait_until_ready()
    logging.info("[RaceLive] Supervisor started")

    idle_s = int(os.getenv("OPENF1_IDLE_CHECK_SECONDS", str(60 * 30)))
    idle_s = max(60, min(60 * 180, idle_s))

    async with aiohttp.ClientSession(headers={"User-Agent": "OF1-Discord-Bot"}) as http:
        while not bot.is_closed():
            _loop_tick("race_supervisor")
            try:
                info = await _pick_current_meeting_and_window(http)
                if not info:
                    await asyncio.sleep(idle_s)
                    continue

                window_start, window_end, meta, relevant, latest_live = info
                now = datetime.now(timezone.utc)
                in_window = window_start <= now <= window_end

                latest_type = _session_type_upper(latest_live)
                # Prefer session_name — it's more specific (e.g. "Sprint Qualifying" vs generic "Qualifying")
                session_type = str(latest_live.get("session_name") or latest_live.get("session_type") or "")
                session_key = int(latest_live.get("session_key") or 0)
                if not session_key:
                    await asyncio.sleep(60)
                    continue
                if not _openf1_is_f1_session(latest_live):
                    await asyncio.sleep(idle_s)
                    continue

                start_raw = str(latest_live.get("date_start") or "")
                end_raw = str(latest_live.get("date_end") or "")
                try:
                    start_dt = _parse_iso(start_raw) if start_raw else None
                except Exception:
                    start_dt = None
                try:
                    end_dt = _parse_iso(end_raw) if end_raw else None
                except Exception:
                    end_dt = None

                session_active = (
                    (start_dt is not None)
                    and (end_dt is not None)
                    and (start_dt <= now <= (end_dt + timedelta(minutes=2)))
                )
                should_follow = bool(in_window and (latest_type in FOLLOW_SESSION_TYPES) and session_active)

                round_meta = await current_or_next_round_meta()
                round_key = str(round_meta.get("key") or "unknown-round")
                race_name = str(round_meta.get("race_name") or "").strip()

                for guild in bot.guilds:
                    try:
                        gid = guild.id
                        task = RACE_LIVE_TASKS.get(gid)
                        running = task is not None and not task.done()
                        held = _race_live_is_held(gid)

                        if should_follow and held and running:
                            _racelog(gid, "Supervisor stopping live loop (manual hold active)")
                            RACE_LIVE_ENABLED[gid] = False
                            task.cancel()
                            try:
                                await task
                            except (asyncio.CancelledError, Exception):
                                pass
                            continue

                        if should_follow and (not held) and (not running):
                            location = str(meta.get("location") or meta.get("meeting_name") or "F1").strip()
                            title_base = race_name or location
                            title = f"{title_base} - Live Weekend"
                            thread = await _ensure_live_thread(guild, round_key, race_name or title_base, title)

                            _racelog(gid, f"Supervisor starting live loop (session_key={session_key}, session={session_type or 'unknown'})")
                            await _send_race_live_ops_notice(
                                guild,
                                f"Race-live start: thread={thread.mention} session={session_type or 'unknown'} session_key={session_key}",
                            )
                            RACE_LIVE_ENABLED[gid] = True
                            RACE_LIVE_ROUND_KEYS[gid] = round_key
                            RACE_LIVE_SESSION_KINDS[gid] = _normalize_session_kind(session_type)

                            async def runner(g=guild, th=thread, sk=session_key, st=session_type):
                                try:
                                    await race_live_loop(g, th, sk, st)
                                except asyncio.CancelledError:
                                    pass
                                except Exception as e:
                                    _racelog(g.id, f"FATAL {type(e).__name__}: {e}")

                            RACE_LIVE_TASKS[gid] = asyncio.create_task(runner())

                        if (not should_follow) and running:
                            _racelog(gid, "Supervisor stopping live loop (session inactive/out-of-scope)")
                            RACE_LIVE_ENABLED[gid] = False
                            task.cancel()
                            try:
                                await task
                            except (asyncio.CancelledError, Exception):
                                pass
                            stopped_round = str(RACE_LIVE_ROUND_KEYS.get(gid) or "")
                            if stopped_round:
                                _set_race_thread_weekend_state(stopped_round, gid, "past")
                                RACE_LIVE_ROUND_KEYS.pop(gid, None)
                                RACE_LIVE_SESSION_KINDS.pop(gid, None)
                                RACE_LIVE_LAST_EVENT_TS.pop(gid, None)
                            await _send_race_live_ops_notice(
                                guild,
                                f"Race-live stop: session={session_type or latest_type or 'unknown'} session_key={session_key}",
                            )
                    except Exception as e:
                        logging.error(f"[RaceLive] Guild {guild.id} supervisor step failed: {e}")

                await asyncio.sleep(60 if should_follow else idle_s)

            except Exception as e:
                _loop_error("race_supervisor")
                logging.error(f"[RaceLive] Supervisor error: {e}")
                await asyncio.sleep(60)

@bot.hybrid_command(name="racelivekill", aliases=["race_live_kill"])
@commands.has_permissions(administrator=True)
async def racelivekill(ctx):
    """Emergency kill switch: stop race-live module only + show tail."""
    guild = ctx.guild
    if not guild:
        return
    gid = guild.id

    RACE_LIVE_ENABLED[gid] = False
    _set_race_live_hold(gid, True)
    t = RACE_LIVE_TASKS.get(gid)
    if t and not t.done():
        t.cancel()
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass
    stopped_round = str(RACE_LIVE_ROUND_KEYS.get(gid) or "")
    if stopped_round:
        _set_race_thread_weekend_state(stopped_round, gid, "past")
        RACE_LIVE_ROUND_KEYS.pop(gid, None)
    RACE_LIVE_SESSION_KINDS.pop(gid, None)
    RACE_LIVE_LAST_EVENT_TS.pop(gid, None)

    tail = _racetail(gid, 20)
    logging.warning(f"[RaceLive][{gid}] KILL SWITCH. Tail:\n{tail}")
    await _send_race_live_ops_notice(guild, f"Race-live killed by {ctx.author.mention}.")
    await ctx.send("🛑 **Race live killed.** Auto-restart is now on hold until `racelivestart`.\n```text\n" + tail[:1800] + "\n```")

@bot.hybrid_command(name="racelivetail", aliases=["race_live_tail"])
@commands.has_permissions(administrator=True)
async def racelivetail(ctx, lines: int = 20):
    """Show last N debug lines for race-live module."""
    guild = ctx.guild
    if not guild:
        return
    lines = max(1, min(50, int(lines)))
    tail = _racetail(guild.id, lines)
    await ctx.send("```text\n" + tail[:1900] + "\n```")

@bot.hybrid_command(name="setdelay")
@commands.has_permissions(administrator=True)
async def setdelay(ctx, seconds: float):
    """Set race-live spoiler delay in seconds for current and future sessions."""
    value = _set_race_live_delay_seconds(seconds)
    await ctx.send(f"✅ Race-live delay set to `{value:.1f}` second(s).")

@bot.hybrid_command(name="setpoll")
@commands.has_permissions(administrator=True)
async def setpoll(ctx, seconds: float):
    """Set race-live OpenF1 poll interval in seconds for current and future sessions."""
    value = _set_race_live_poll_seconds(seconds)
    await ctx.send(f"✅ Race-live poll interval set to `{value:.1f}` second(s).")

@bot.hybrid_command(name="livesettings")
@commands.has_permissions(administrator=True)
async def livesettings(ctx):
    """Show race-live runtime settings and active session types."""
    delay_s = _race_live_delay_seconds()
    poll_s = _race_live_poll_seconds()
    active = sorted(int(g) for g, v in RACE_LIVE_ENABLED.items() if v)
    running = sorted(int(g) for g, t in RACE_LIVE_TASKS.items() if _task_running(t))
    kinds = {str(g): str(k) for g, k in (RACE_LIVE_SESSION_KINDS or {}).items()}
    ops_channel = _race_live_ops_channel_id()
    hold_map = _race_live_hold_map()
    held_guild_ids = sorted(int(g) for g, v in hold_map.items() if v and str(g).isdigit())
    await ctx.send(
        "ℹ️ **Race Live Settings**\n"
        f"- Delay: `{delay_s:.1f}s`\n"
        f"- Poll interval: `{poll_s:.1f}s`\n"
        f"- Active guild IDs: `{active}`\n"
        f"- Running guild IDs: `{running}`\n"
        f"- Session kinds: `{kinds}`\n"
        f"- Held guild IDs: `{held_guild_ids}`\n"
        f"- Ops channel ID: `{ops_channel or 'not set'}`"
    )

@bot.hybrid_command(name="raceliveopschannel", aliases=["race_live_ops_channel"])
@commands.has_permissions(administrator=True)
async def raceliveopschannel(ctx, channel: Optional[discord.TextChannel] = None):
    """Set/show the channel used for race-live ops notices (start/stop/attach)."""
    guild = ctx.guild
    if guild is None:
        return

    if channel is None:
        cid = _race_live_ops_channel_id()
        if not cid:
            return await ctx.send("ℹ️ Race-live ops channel is not set.")
        ch = guild.get_channel(cid)
        if ch is None:
            try:
                ch = await guild.fetch_channel(cid)
            except Exception:
                ch = None
        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            return await ctx.send(f"ℹ️ Race-live ops channel: {ch.mention}")
        return await ctx.send(f"ℹ️ Race-live ops channel is set to `{cid}` but not accessible right now.")

    _set_race_live_ops_channel_id(channel.id)
    await ctx.send(f"✅ Race-live ops channel set to {channel.mention}.")

@bot.hybrid_command(name="raceliveopsclear", aliases=["race_live_ops_clear"])
@commands.has_permissions(administrator=True)
async def raceliveopsclear(ctx):
    """Clear the dedicated race-live ops notice channel."""
    _set_race_live_ops_channel_id(0)
    await ctx.send("✅ Race-live ops channel cleared.")

@bot.tree.command(name="racethreadcheck", description="Check which thread the bot will post live race updates to.")
@discord.app_commands.checks.has_permissions(administrator=True)
async def racethreadcheck_slash(interaction: discord.Interaction):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        round_meta = await current_or_next_round_meta()
        round_key  = str(round_meta.get("key") or "unknown")
        race_name  = str(round_meta.get("race_name") or "Next Race").strip()

        thread = await _fetch_saved_race_thread(guild, round_key)
        if thread is not None:
            await interaction.followup.send(
                f"For **{race_name}** (`{round_key}`), live updates will post to: {thread.mention}",
                ephemeral=True,
            )
        else:
            await interaction.followup.send(
                f"No thread saved yet for **{race_name}** (`{round_key}`). "
                f"One will be created automatically before the race, or use `/racethread` to create it now.",
                ephemeral=True,
            )
    except Exception as e:
        logging.error(f"[RaceThreadCheck] failed: {e}")
        await interaction.followup.send(f"Check failed: {e}", ephemeral=True)


@bot.tree.command(name="racethreadset", description="Manually set which thread the bot will post live race updates to.")
@discord.app_commands.checks.has_permissions(administrator=True)
@discord.app_commands.describe(thread="The thread to redirect live race updates to.")
async def racethreadset_slash(interaction: discord.Interaction, thread: discord.Thread):
    guild = interaction.guild
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if guild is None or member is None:
        await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
        return

    perms = member.guild_permissions
    if not (perms.administrator or perms.manage_threads or perms.manage_channels):
        await interaction.response.send_message(
            "You need Manage Threads, Manage Channels, or Administrator permissions to use this command.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        round_meta = await current_or_next_round_meta()
        round_key  = str(round_meta.get("key") or "unknown")
        race_name  = str(round_meta.get("race_name") or "Next Race").strip()

        _save_race_thread_record(
            round_key=round_key,
            race_name=race_name,
            guild_id=guild.id,
            thread=thread,
            source="manual",
        )
        await interaction.followup.send(
            f"Done. Live updates for **{race_name}** (`{round_key}`) will now post to {thread.mention}.",
            ephemeral=True,
        )
    except Exception as e:
        logging.error(f"[RaceThreadSet] failed: {e}")
        await interaction.followup.send(f"Could not update race thread: {e}", ephemeral=True)


@bot.tree.command(name="racethread", description="Create the next race thread early with custom watchalong info.")
@discord.app_commands.describe(
    message="Message text for the race thread",
    image="Optional image attachment",
)
async def racethread_slash(
    interaction: discord.Interaction,
    message: str = "",
    image: Optional[discord.Attachment] = None,
):
    guild = interaction.guild
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if guild is None or member is None:
        await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
        return

    perms = member.guild_permissions
    if not (perms.administrator or perms.manage_threads or perms.manage_channels):
        await interaction.response.send_message(
            "You need Manage Threads, Manage Channels, or Administrator permissions to use this command.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        round_meta = await current_or_next_round_meta()
        round_key = str(round_meta.get("key") or "unknown-round")
        race_name = str(round_meta.get("race_name") or "F1").strip() or "F1"
        title = f"{race_name}"
        post_text = (message or "").strip() or f"Race thread for **{race_name}**."

        upload_file: Optional[discord.File] = None
        if image is not None:
            data = await image.read()
            upload_file = discord.File(io.BytesIO(data), filename=(image.filename or "race-thread-image.png"))

        existing = await _fetch_saved_race_thread(guild, round_key)
        if existing is not None:
            await interaction.followup.send(
                f"Race thread already exists for **{race_name}**: {existing.mention}\nNo new thread was created.",
                ephemeral=True,
            )
            return

        created = await _create_race_thread(
            guild=guild,
            title=title,
            opener_text=post_text,
            opener_file=upload_file,
        )
        _save_race_thread_record(
            round_key=round_key,
            race_name=race_name,
            guild_id=guild.id,
            thread=created,
            source="manual",
        )
        await interaction.followup.send(
            f"Created race thread for **{race_name}**: {created.mention}",
            ephemeral=True,
        )
    except Exception as e:
        logging.error(f"[RaceThread] slash command failed: {e}")
        await interaction.followup.send(f"Could not create race thread: {e}", ephemeral=True)

@bot.hybrid_command(name="racelivestart", aliases=["race_live_start"])
@commands.has_permissions(administrator=True)
async def racelivestart(ctx):
    """Manually start race-live right now (ignores weekend window)."""
    guild = ctx.guild
    if not guild:
        return
    gid = guild.id
    _set_race_live_hold(gid, False)

    t = RACE_LIVE_TASKS.get(gid)
    if t and not t.done():
        RACE_LIVE_ENABLED[gid] = False
        t.cancel()
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass

    async with aiohttp.ClientSession(headers={"User-Agent": "OF1-Discord-Bot"}) as http:
        latest = await _openf1_get(http, "sessions", {"session_key": "latest"}, caller="racelivestart_latest")
        if not latest:
            return await ctx.send("❌ No OpenF1 sessions available right now.")
        if not _openf1_is_f1_session(latest[0]):
            return await ctx.send("❌ Latest OpenF1 session is not Formula 1. Not starting race-live.")
        session_key = int(latest[0].get("session_key"))
        session_type = str(latest[0].get("session_type") or latest[0].get("session_name") or "")

    round_meta = await current_or_next_round_meta()
    round_key = str(round_meta.get("key") or "manual-round")
    race_name = str(round_meta.get("race_name") or "F1").strip() or "F1"
    title = f"{race_name} - Live (Manual)"
    thread = await _ensure_live_thread(guild, round_key, race_name, title)

    RACE_LIVE_ENABLED[gid] = True
    RACE_LIVE_ROUND_KEYS[gid] = round_key
    RACE_LIVE_SESSION_KINDS[gid] = _normalize_session_kind(session_type)

    async def runner():
        try:
            await race_live_loop(guild, thread, session_key, session_type)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            _racelog(gid, f"FATAL {type(e).__name__}: {e}")

    RACE_LIVE_TASKS[gid] = asyncio.create_task(runner())
    await _send_race_live_ops_notice(
        guild,
        f"Manual race-live start by {ctx.author.mention}: thread={thread.mention} session={session_type or 'unknown'} session_key={session_key}",
    )
    await ctx.send(f"✅ Started race live manually (session_key={session_key}, session={session_type or 'unknown'}).")

@bot.hybrid_command(name="racelivestop", aliases=["race_live_stop"])
@commands.has_permissions(administrator=True)
async def racelivestop(ctx):
    """Gracefully stop race-live for this guild."""
    guild = ctx.guild
    if not guild:
        return
    gid = guild.id

    RACE_LIVE_ENABLED[gid] = False
    _set_race_live_hold(gid, True)
    t = RACE_LIVE_TASKS.get(gid)
    if t and not t.done():
        t.cancel()
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass
    stopped_round = str(RACE_LIVE_ROUND_KEYS.get(gid) or "")
    if stopped_round:
        _set_race_thread_weekend_state(stopped_round, gid, "past")
        RACE_LIVE_ROUND_KEYS.pop(gid, None)
    RACE_LIVE_SESSION_KINDS.pop(gid, None)
    RACE_LIVE_LAST_EVENT_TS.pop(gid, None)

    await _send_race_live_ops_notice(guild, f"Manual race-live stop by {ctx.author.mention}.")
    await ctx.send("Race live stopped. Auto-restart is now on hold until `racelivestart`.")

# ============================================================
# Race Test Harness (Fake Scenarios)
#   - kept for testing
#   - commands renamed to NO underscores
# ============================================================

# In-memory tasks so you can stop a running test
RACE_TEST_TASKS: Dict[int, asyncio.Task] = {}  # key: guild_id

# Built-in default scenarios (config-friendly shape)
DEFAULT_RACE_SCENARIOS: Dict[str, Dict[str, Any]] = {
    "practice_short": {
        "title": "Bahrain GP - FP1 (TEST)",
        "events": [
            {"t": 0,  "type": "SESSION_START", "session": "FP1"},
            {"t": 2,  "type": "GREEN",         "detail": "Session green"},
            {"t": 10, "type": "VSC",           "detail": "Virtual Safety Car deployed"},
            {"t": 20, "type": "GREEN",         "detail": "VSC ended, green"},
            {"t": 30, "type": "RED",           "detail": "Red flag - debris on track"},
            {"t": 45, "type": "GREEN",         "detail": "Session resumes"},
            {"t": 60, "type": "SESSION_END",   "detail": "FP1 complete"},
        ],
    },
    "race_chaos": {
        "title": "Bahrain GP - RACE (TEST)",
        "events": [
            {"t": 0,   "type": "SESSION_START", "session": "RACE"},
            {"t": 5,   "type": "GREEN",         "detail": "Lights out - race start"},
            {"t": 60,  "type": "SC",            "detail": "Safety Car deployed"},
            {"t": 120, "type": "GREEN",         "detail": "Safety Car in - green"},
            {"t": 180, "type": "RED",           "detail": "Red flag - major incident"},
            {"t": 240, "type": "GREEN",         "detail": "Restart underway"},
            {"t": 420, "type": "SC",            "detail": "Late Safety Car"},
            {"t": 480, "type": "GREEN",         "detail": "Final sprint - green"},
            {"t": 600, "type": "SESSION_END",   "detail": "Chequered flag"},
        ],
    },
}

EVENT_STYLE = {
    "SESSION_START": ("🟦", "**Session started**"),
    "SESSION_END":   ("\U0001F3C1", "**Session ended**"),
    "GREEN":         ("🟢", "**GREEN**"),
    "SC":            ("🟡", "**SAFETY CAR**"),
    "VSC":           ("🟠", "**VSC**"),
    "RED":           ("🔴", "**RED FLAG**"),
    "YELLOW":         ("🟡", "**YELLOW**"),
    "SEGMENT_START":  ("🟦", "**Segment started**"),
    "SEGMENT_END":    ("⬛", "**Segment ended**"),
    "PURPLE_SECTOR":  ("🟣", "**Purple sector**"),
    "CHECKERED_FLAG": ("🏁", "**CHEQUERED FLAG**"),
    "CLASSIFICATION_READY": ("📊", "**Classification ready**"),
    "RESULTS_READY":  ("📊", "**Results ready**"),
    "INFO":          ("\u2139\uFE0F", "**Info**"),
}

def _scenario_meta(scenario: Dict[str, Any]) -> Dict[str, Any]:
    return dict(scenario.get("meta") or {})

def _scenario_title(scenario: Dict[str, Any], fallback: str) -> str:
    meta = _scenario_meta(scenario)
    return (meta.get("title") or scenario.get("title") or fallback).strip()

def _scenario_session(scenario: Dict[str, Any]) -> str:
    meta = _scenario_meta(scenario)
    return str(meta.get("session") or "").upper().strip()

def _scenario_grid_map(scenario: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for d in (scenario.get("grid") or []):
        if not isinstance(d, dict):
            continue
        did = str(d.get("id") or "").strip()
        name = str(d.get("name") or "").strip()
        if did:
            out[did] = name or did
    return out

def _format_race_classification(scenario: Dict[str, Any]) -> str:
    cls = scenario.get("classification") or {}
    results = cls.get("results") or []
    grid = _scenario_grid_map(scenario)
    lines: List[str] = []
    for r in results:
        if not isinstance(r, dict):
            continue
        pos = r.get("pos")
        did = str(r.get("driver_id") or "").strip()
        name = grid.get(did, did or "Unknown")
        status = str(r.get("status") or "").upper().strip()
        gap = r.get("gap")
        reason = str(r.get("reason") or "").strip()

        if status == "DNF":
            tail = f"DNF" + (f" — {reason}" if reason else "")
        else:
            tail = str(gap) if gap is not None else (status or "")

        lines.append(f"{int(pos):>2}. {name} — {tail}" if pos is not None else f"- {name} — {tail}")

    if not lines:
        return "No classification data."
    return "\n".join(lines)

def _format_quali_classification(scenario: Dict[str, Any]) -> str:
    cls = scenario.get("classification") or {}
    results = cls.get("results") or []
    grid = _scenario_grid_map(scenario)
    lines: List[str] = []
    for r in results:
        if not isinstance(r, dict):
            continue
        pos = r.get("pos")
        did = str(r.get("driver_id") or "").strip()
        name = grid.get(did, did or "Unknown")
        best = str(r.get("best") or "").strip()
        gap = str(r.get("gap") or "").strip()
        status = str(r.get("status") or "").upper().strip()

        tail = best if best else "—"
        if gap and gap != "0.000":
            tail += f" ({gap})"
        if status in ("POLE", "OUT"):
            tail += f" — {status}"
        note = str(r.get("note") or "").strip()
        if note:
            tail += f" — {note}"

        lines.append(f"{int(pos):>2}. {name} — {tail}" if pos is not None else f"- {name} — {tail}")

    if not lines:
        return "No qualifying results data."
    return "\n".join(lines)

def _wrap_spoiler(text: str) -> str:
    return "\n".join(f"||{line}||" for line in text.splitlines())

def _race_event_recap(events: List[Dict[str, Any]]) -> str:
    counts: Dict[str, int] = {}
    for ev in events:
        etype = str((ev or {}).get("type") or "INFO").upper().strip()
        counts[etype] = counts.get(etype, 0) + 1
    if not counts:
        return "No events recorded."
    keys = ["SESSION_START", "GREEN", "YELLOW", "VSC", "SC", "RED", "SEGMENT_START", "SEGMENT_END", "CLASSIFICATION_READY", "RESULTS_READY", "SESSION_END"]
    lines = []
    for k in keys:
        if counts.get(k):
            lines.append(f"- {k}: **{counts[k]}**")
    for k in sorted(counts.keys()):
        if k not in keys:
            lines.append(f"- {k}: **{counts[k]}**")
    return "\n".join(lines)

def _load_race_scenarios() -> Dict[str, Dict[str, Any]]:
    path = (os.getenv("RACE_SCENARIOS_FILE") or "").strip()
    if not path:
        path = os.path.join(os.path.dirname(__file__), "scenario.json")

    logging.info(f"[RaceTest] Loading scenarios from: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        merged = dict(DEFAULT_RACE_SCENARIOS)
        merged.update(data or {})

        logging.info(f"[RaceTest] Loaded scenarios OK: {list(data.keys())}")
        return merged

    except Exception as e:
        logging.error(f"[RaceTest] Failed to load scenario.json, using defaults: {e}")
        return DEFAULT_RACE_SCENARIOS

def _format_quali_knockouts(scenario: Dict[str, Any], knocked_in: str) -> str:
    cls = scenario.get("classification") or {}
    results = cls.get("results") or []
    grid = _scenario_grid_map(scenario)

    knocked_in = (knocked_in or "").upper().strip()
    knocked: List[Tuple[int, str, str]] = []

    for r in results:
        if not isinstance(r, dict):
            continue
        status = str(r.get("status") or "").upper().strip()
        seg = str(r.get("segment") or "").upper().strip()
        if status != "OUT" or seg != knocked_in:
            continue

        pos = r.get("pos")
        did = str(r.get("driver_id") or "").strip()
        name = grid.get(did, did or "Unknown")
        note = str(r.get("note") or "").strip()
        knocked.append((int(pos) if pos is not None else 999, name, note))

    if not knocked:
        return "No knockouts listed."

    knocked.sort(key=lambda x: x[0])
    lines = []
    for pos, name, note in knocked:
        tail = f" — {note}" if note else ""
        lines.append(f"P{pos} {name}{tail}")
    return "\n".join(lines)

async def _get_forum_channel(guild: discord.Guild) -> Optional[discord.abc.GuildChannel]:
    forum_id = os.getenv("RACE_FORUM_CHANNEL_ID")
    if not forum_id:
        return None
    try:
        ch = guild.get_channel(int(forum_id))
        if ch is None:
            ch = await guild.fetch_channel(int(forum_id))
        return ch
    except Exception as e:
        logging.error(f"[RaceTest] Could not fetch forum channel {forum_id}: {e}")
        return None

async def _ensure_test_thread(guild: discord.Guild, title: str) -> Optional[discord.Thread]:
    ch = await _get_forum_channel(guild)
    if ch is None:
        return None

    try:
        if isinstance(ch, discord.ForumChannel):
            created = await ch.create_thread(
                name=title,
                content=f"🧪 Race test thread created by {bot.user.mention}",
                auto_archive_duration=1440,
            )
            if isinstance(created, tuple) and len(created) >= 1:
                return created[0]
            return created
    except Exception as e:
        logging.error(f"[RaceTest] Forum create_thread failed: {e}")

    try:
        if isinstance(ch, discord.TextChannel):
            msg = await ch.send(f"🧪 Race test thread: **{title}**")
            th = await msg.create_thread(name=title, auto_archive_duration=1440)
            return th
    except Exception as e:
        logging.error(f"[RaceTest] Text thread creation failed: {e}")

    return None

async def _emit_race_event(thread: discord.Thread, scenario: Dict[str, Any], event: Dict[str, Any], grid_map: Dict[str, str]) -> None:
    etype = (event.get("type") or "INFO").upper().strip()
    emoji, label = EVENT_STYLE.get(etype, ("\u2139\uFE0F", "**Info**"))

    scenario_session = _scenario_session(scenario)
    ev_session = str(event.get("session") or "").strip()
    segment = str(event.get("segment") or "").strip().upper()

    if etype == "PURPLE_SECTOR":
        did = str(event.get("driver_id") or "").strip()
        name = grid_map.get(did, did or "Unknown")
        sector = event.get("sector")
        lap = str(event.get("lap") or "").strip()
        seg_txt = f" ({segment})" if segment else ""
        sec_txt = f"S{sector}" if sector is not None else "sector"
        text = f"{emoji} {name} sets purple {sec_txt}{seg_txt}"
        if lap:
            text += f" — {lap}"
        await thread.send(text)
        return

    if etype in ("SEGMENT_START", "SEGMENT_END") and segment:
        label = f"**{segment} {'started' if etype == 'SEGMENT_START' else 'ended'}**"

    detail = (event.get("detail") or "").strip()

    if etype == "SESSION_START":
        use_session = ev_session or scenario_session
        suffix = f" ({use_session})" if use_session else ""
    else:
        suffix = ""

    text = f"{emoji} {label}{suffix}"
    if detail:
        text += f"\n{detail}"

    await thread.send(text)

    if etype in ("CLASSIFICATION_READY", "RESULTS_READY"):
        session_type = scenario_session
        if session_type == "RACE" and etype == "CLASSIFICATION_READY":
            body = _format_race_classification(scenario)
            await thread.send(_wrap_spoiler("📊 Race Classification\n" + body))
        elif session_type in ("QUALI", "QUALIFYING") and etype == "RESULTS_READY":
            body = _format_quali_classification(scenario)
            await thread.send(_wrap_spoiler("📊 Qualifying Results\n" + body))

    if etype == "SEGMENT_END" and scenario_session in ("QUALI", "QUALIFYING") and segment in ("Q1", "Q2"):
        body = _format_quali_knockouts(scenario, segment)
        await thread.send(_wrap_spoiler(f"🚫 {segment} Knockouts\n{body}"))

async def _run_race_test_scenario(guild: discord.Guild, scenario_name: str, speed: float = 1.0) -> None:
    scenarios = _load_race_scenarios()
    scenario = scenarios.get(scenario_name)

    if not scenario:
        for k, v in scenarios.items():
            if k.lower() == scenario_name.lower():
                scenario_name = k
                scenario = v
                break

    if not scenario:
        raise RuntimeError(f"Scenario '{scenario_name}' not found.")

    title = _scenario_title(scenario, fallback=f"Race Test - {scenario_name}")
    events = scenario.get("events") or []
    if not isinstance(events, list) or not events:
        raise RuntimeError(f"Scenario '{scenario_name}' has no events.")

    grid_map = _scenario_grid_map(scenario)
    thread = await _ensure_test_thread(guild, title)
    if thread is None:
        raise RuntimeError("Could not create or access the race forum/thread. Check RACE_FORUM_CHANNEL_ID and bot perms.")

    events_sorted = sorted(events, key=lambda e: float(e.get("t", 0)))
    await thread.send(f"🧪 Starting scenario: **{scenario_name}**\nSpeed: **x{speed}**")

    last_t = float(events_sorted[0].get("t", 0))
    for ev in events_sorted:
        cur_t = float(ev.get("t", last_t))
        dt = max(0.0, cur_t - last_t)
        last_t = cur_t
        sleep_for = dt / max(0.01, float(speed))
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)
        await _emit_race_event(thread, scenario, ev, grid_map)

    recap = _race_event_recap(events_sorted)
    await thread.send("✅ Scenario complete.")
    await thread.send("📦 **Session Recap**\n" + recap)

def _resolve_scenario(scenario_name: str) -> Tuple[str, Dict[str, Any]]:
    scenarios = _load_race_scenarios()
    name = (scenario_name or "").strip()
    if not name:
        raise RuntimeError("Scenario name is required.")
    scenario = scenarios.get(name)
    if scenario:
        return name, scenario
    for k, v in scenarios.items():
        if k.lower() == name.lower():
            return k, v
    raise RuntimeError(f"Scenario '{name}' not found.")

@bot.command(name="racetestlist", aliases=["race_test_list"])
@commands.has_permissions(administrator=True)
async def racetestlist(ctx):
    scenarios = _load_race_scenarios()
    names = sorted(scenarios.keys())
    await ctx.send("\U0001F9EA **Race test scenarios:**\n" + "\n".join(f"- `{n}`" for n in names))

@bot.command(name="racetestinfo", aliases=["race_test_info"])
@commands.has_permissions(administrator=True)
async def racetestinfo(ctx, scenario: str):
    try:
        name, sc = _resolve_scenario(scenario)
    except Exception as e:
        await ctx.send(f"\u274C {e}")
        return

    title = _scenario_title(sc, fallback=name)
    session_type = _scenario_session(sc) or "(none)"
    events = sc.get("events") or []
    grid = sc.get("grid") or []
    segments = sc.get("segments") or []
    has_cls = bool((sc.get("classification") or {}).get("results"))

    await ctx.send(
        "\U0001F9EA **Scenario info**\n"
        f"- **Key:** `{name}`\n"
        f"- **Title:** {title}\n"
        f"- **Session:** `{session_type}`\n"
        f"- **Events:** {len(events) if isinstance(events, list) else 0}\n"
        f"- **Grid drivers:** {len(grid) if isinstance(grid, list) else 0}\n"
        f"- **Segments:** {len(segments) if isinstance(segments, list) else 0}\n"
        f"- **Has classification:** {'yes' if has_cls else 'no'}"
    )

@bot.command(name="racetestresults", aliases=["race_test_results"])
@commands.has_permissions(administrator=True)
async def racetestresults(ctx, scenario: str):
    try:
        name, sc = _resolve_scenario(scenario)
    except Exception as e:
        await ctx.send(f"\u274C {e}")
        return

    session_type = _scenario_session(sc)
    if session_type == "RACE":
        body = _format_race_classification(sc)
        await ctx.send(_wrap_spoiler("\U0001F4CA Race Classification\n" + body))
    elif session_type in ("QUALI", "QUALIFYING"):
        body = _format_quali_classification(sc)
        await ctx.send(_wrap_spoiler("\U0001F4CA Qualifying Results\n" + body))
    else:
        await ctx.send(f"\u2139\uFE0F Scenario `{name}` has unknown session type `{session_type}`; no formatter yet.")

@bot.command(name="raceteststart", aliases=["race_test_start"])
@commands.has_permissions(administrator=True)
async def raceteststart(ctx, scenario: str = None, speed: float = None):
    guild = ctx.guild
    if not guild:
        await ctx.send("❌ Must be run in a server.")
        return

    scenario = (scenario or os.getenv("RACE_TEST_DEFAULT_SCENARIO") or "practice_short").strip()
    try:
        if speed is None:
            speed = float(os.getenv("RACE_TEST_SPEED", "1.0"))
        speed = float(speed)
        speed = max(0.1, min(50.0, speed))
    except Exception:
        speed = 1.0

    existing = RACE_TEST_TASKS.get(guild.id)
    if existing and not existing.done():
        existing.cancel()
        try:
            await existing
        except (asyncio.CancelledError, Exception):
            pass

    async def runner():
        try:
            await _run_race_test_scenario(guild, scenario, speed=speed)
        except asyncio.CancelledError:
            logging.info(f"[RaceTest] Cancelled scenario '{scenario}'")
        except Exception as e:
            logging.error(f"[RaceTest] Scenario '{scenario}' failed: {e}")
            try:
                await ctx.send(f"\u274C Race test failed: {e}")
            except Exception:
                pass

    task = asyncio.create_task(runner())
    RACE_TEST_TASKS[guild.id] = task

    await ctx.send(f"\U0001F9EA Starting race test: `{scenario}` (speed x{speed})")

@bot.command(name="raceteststop", aliases=["race_test_stop"])
@commands.has_permissions(administrator=True)
async def raceteststop(ctx):
    guild = ctx.guild
    if not guild:
        return
    t = RACE_TEST_TASKS.get(guild.id)
    if t and not t.done():
        t.cancel()
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass
        await ctx.send("🛑 Race test stopped.")
    else:
        await ctx.send("ℹ️ No race test running.")


# ---------------------------------------------------------------------------
# TEMPORARY: Session replay harness — run !replaytest to observe what the bot
# would post for the current weekend's sprint + qualifying sessions.
# Remove this block once no longer needed.
# ---------------------------------------------------------------------------

async def _run_replay_test(channel: discord.TextChannel, meeting_key_override: Optional[int] = None) -> None:
    lines: List[str] = []

    async with aiohttp.ClientSession(headers={"User-Agent": "OF1-Discord-Bot"}) as http:
        # --- Discover sessions for the current (or specified) meeting ---
        if meeting_key_override:
            all_sessions = await _openf1_get(http, "sessions", {"meeting_key": meeting_key_override}, caller="replay")
        else:
            latest = await _openf1_get(http, "sessions", {"session_key": "latest"}, caller="replay")
            if not latest or not isinstance(latest, list):
                await channel.send("❌ Could not determine current session from OpenF1.")
                return
            meeting_key_override = latest[0].get("meeting_key")
            all_sessions = await _openf1_get(http, "sessions", {"meeting_key": meeting_key_override}, caller="replay")

        if not all_sessions or not isinstance(all_sessions, list):
            await channel.send("❌ No sessions found for this meeting.")
            return

        meeting_name = all_sessions[0].get("meeting_name") or "Unknown Meeting"
        lines.append(f"REPLAY — {meeting_name} (meeting_key={meeting_key_override})")
        lines.append(f"All sessions in meeting:")
        for s in all_sessions:
            lines.append(f"  [{s.get('session_key')}] {s.get('session_name')} ({s.get('session_type')})")
        lines.append("")

        # --- Pick sprint + qualifying sessions to replay ---
        REPLAY_TARGETS = {"sprint", "qualifying", "sprint qualifying", "sprint shootout"}
        target_sessions = [
            s for s in all_sessions
            if str(s.get("session_name") or s.get("session_type") or "").lower().strip() in REPLAY_TARGETS
            or any(kw in str(s.get("session_name") or "").lower() for kw in ("sprint", "qualif", "shootout"))
        ]
        if not target_sessions:
            lines.append("⚠️  No sprint/qualifying sessions found — replaying ALL sessions instead.")
            target_sessions = all_sessions

        # --- Replay each target session ---
        for session in target_sessions:
            session_key = session.get("session_key")
            session_type = str(session.get("session_name") or session.get("session_type") or "")
            session_kind = _normalize_session_kind(session_type)

            lines.append("=" * 70)
            lines.append(f"SESSION : {session.get('session_name')} | type={session_type} | kind={session_kind}")
            lines.append(f"KEY     : {session_key}")
            lines.append("=" * 70)

            rc = await _openf1_get(http, "race_control", {"session_key": session_key}, caller="replay")
            if not isinstance(rc, list):
                lines.append("  ⚠️  race_control returned non-list — API may be restricted.")
                lines.append("")
                continue

            driver_map = await _openf1_driver_name_map(http, session_key)
            # Fetch positions once — historical session is already stable
            positions = await _openf1_latest_positions(http, session_key)
            ordered_all = sorted(positions.items(), key=lambda kv: kv[1]) if positions else []

            lines.append(f"race_control msgs : {len(rc)}")
            lines.append(f"positions fetched : {len(positions)}")
            lines.append(f"drivers in map    : {len(driver_map)}")
            lines.append("")

            def _name(num: str) -> str:
                return driver_map.get(str(num), f"#{num}")

            current_quali_seg = "SQ1" if session_kind == "SPRINT_QUALI" else "Q1"
            posted_segment_summaries: set = set()
            posted_final_summary = False
            loop_stopped_at: Optional[str] = None
            _replay_seen_session_end: bool = False

            for item in rc:
                msg = str(item.get("message") or "").strip()
                dt = str(item.get("date") or "")
                ts = dt[11:19] if len(dt) >= 19 else dt

                # Show every raw item, even empty ones
                if not msg:
                    lines.append(f"[{ts}] (empty message — skipped in live loop)")
                    continue

                upper_msg = msg.upper()
                lower_msg = msg.lower()

                session_end = (
                    ("CHECKERED" in upper_msg)
                    or ("CHEQUERED" in upper_msg)
                    or ("SESSION END" in upper_msg)
                    or ("SESSION FINISHED" in upper_msg)
                )

                will_post = _race_control_should_post(msg)
                is_track_deletion = session_kind in {"QUALI", "SPRINT_QUALI"} and any(
                    p in lower_msg for p in ("track limits", "lap time deleted", "time deleted", "lap deleted")
                )

                # Mark messages that arrive after the loop would have stopped
                after_stop = f"  [AFTER LOOP STOP @ {loop_stopped_at}]" if loop_stopped_at else ""

                if will_post:
                    emoji = _race_control_emoji_for_message(msg)
                    lines.append(f"[{ts}] POST : {emoji} {msg}{after_stop}")
                elif is_track_deletion:
                    lines.append(f"[{ts}] POST : 🚫 {msg}{after_stop}")
                else:
                    lines.append(f"[{ts}] SKIP : {msg}{after_stop}")

                # --- Qualifying boundary logic (mirrors fixed live loop) ---
                if session_kind in {"QUALI", "SPRINT_QUALI"}:
                    seg = _extract_quali_segment(msg)
                    if seg:
                        current_quali_seg = seg

                    if "SESSION STARTED" in upper_msg and _replay_seen_session_end:
                        closing_seg = current_quali_seg
                        key = f"{closing_seg}:end"
                        if key not in posted_segment_summaries:
                            posted_segment_summaries.add(key)
                            _seg_next = {"Q1": "Q2", "Q2": "Q3", "Q3": "Q3", "SQ1": "SQ2", "SQ2": "SQ3", "SQ3": "SQ3"}
                            current_quali_seg = _seg_next.get(closing_seg, closing_seg)
                            cutoff = "Sprint Qualifying" if session_kind == "SPRINT_QUALI" else "Qualifying"
                            lines.append(f"         ↳ BOUNDARY on SESSION STARTED for {closing_seg} (next seg={current_quali_seg}){after_stop}")
                            if closing_seg in {"Q1", "SQ1"}:
                                knocked = [(n, p) for n, p in ordered_all if p >= 17]
                                if knocked:
                                    body = "\n".join(f"           P{p} {_name(n)}" for n, p in knocked[:6])
                                    lines.append(f"         ↳ SPOILER POST: 🚫 {cutoff} {closing_seg} Knockouts")
                                    lines.append(body)
                                else:
                                    lines.append(f"         ↳ No knockouts found (got {len(ordered_all)} positions)")
                            elif closing_seg in {"Q2", "SQ2"}:
                                knocked = [(n, p) for n, p in ordered_all if 11 <= p <= 16]
                                if knocked:
                                    body = "\n".join(f"           P{p} {_name(n)}" for n, p in knocked[:6])
                                    lines.append(f"         ↳ SPOILER POST: 🚫 {cutoff} {closing_seg} Knockouts")
                                    lines.append(body)
                                else:
                                    lines.append(f"         ↳ No P11-P16 found (got {len(ordered_all)} positions)")
                            else:
                                top10 = [(n, p) for n, p in ordered_all if 1 <= p <= 10]
                                if top10:
                                    body = "\n".join(f"           P{p} {_name(n)}" for n, p in top10[:10])
                                    lines.append(f"         ↳ SPOILER POST: 📊 {cutoff} Top 10")
                                    lines.append(body)
                                else:
                                    lines.append(f"         ↳ No top-10 positions found")
                        _replay_seen_session_end = False

                    if session_end and not loop_stopped_at:
                        if current_quali_seg in {"Q3", "SQ3"}:
                            cutoff = "Sprint Qualifying" if session_kind == "SPRINT_QUALI" else "Qualifying"
                            key = f"{current_quali_seg}:end"
                            if key not in posted_segment_summaries:
                                posted_segment_summaries.add(key)
                                top10 = [(n, p) for n, p in ordered_all if 1 <= p <= 10]
                                if top10:
                                    body = "\n".join(f"           P{p} {_name(n)}" for n, p in top10[:10])
                                    lines.append(f"         ↳ SPOILER POST: 📊 {cutoff} Top 10")
                                    lines.append(body)
                            loop_stopped_at = ts
                            lines.append(f"         ↳ [*** LOOP WOULD STOP HERE *** — final segment {current_quali_seg} done]")
                        else:
                            _replay_seen_session_end = True
                            lines.append(f"         ↳ [segment end — waiting for SESSION STARTED to post {current_quali_seg} boundary]")

                # --- Race / Sprint final summary logic ---
                elif session_kind in {"RACE", "SPRINT"}:
                    if session_end and not posted_final_summary:
                        posted_final_summary = True
                        top_n = 8 if session_kind == "SPRINT" else 10
                        label = "Sprint Final Classification (Top 8)" if session_kind == "SPRINT" else "Race Final Classification (Top 10)"
                        top_rows = [(n, p) for n, p in ordered_all if 1 <= p <= top_n]
                        if top_rows:
                            body = "\n".join(f"           P{p} {_name(n)}" for n, p in top_rows[:top_n])
                            lines.append(f"         ↳ SPOILER POST: 📊 {label}")
                            lines.append(body)
                        else:
                            lines.append(f"         ↳ [final summary: no positions P1-P{top_n} found]")
                        if not loop_stopped_at:
                            loop_stopped_at = ts
                            lines.append(f"         ↳ [*** LOOP WOULD STOP HERE ***]")

            lines.append("")

    # Write and upload
    outpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "replay_output.txt")
    with open(outpath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    total = len(lines)
    try:
        await channel.send(
            f"✅ Replay complete — {total} lines. See attached file.",
            file=discord.File(outpath, filename="replay_output.txt"),
        )
    except Exception:
        await channel.send(f"✅ Replay written to `replay_output.txt` ({total} lines) — file too large to attach, check repo.")


@bot.command(name="replaytest")
@commands.has_permissions(administrator=True)
async def cmd_replaytest(ctx, meeting_key: Optional[int] = None):
    """[TEMP] Replay sprint + qualifying race_control messages and log what the bot would post."""
    await ctx.send("🔄 Fetching and replaying sessions — this may take 30–60 seconds...")
    try:
        await _run_replay_test(ctx.channel, meeting_key)
    except Exception as e:
        await ctx.send(f"❌ Replay error: {type(e).__name__}: {e}")


def _openf1_meeting_groups_for_year(year: int) -> List[Dict[str, Any]]:
    y = int(year)
    sessions = _openf1_get_json("sessions", {"year": y}, 30, "racereplay_sessions")
    if not isinstance(sessions, list) or not sessions:
        # Fallback: fetch without year filter and then filter locally.
        sessions = _openf1_get_json("sessions", {}, 30, "racereplay_sessions_fallback")
    if not isinstance(sessions, list):
        return []

    grouped: Dict[str, Dict[str, Any]] = {}
    for s in sessions:
        if not isinstance(s, dict):
            continue
        if not _openf1_is_weekend_session(s):
            continue

        mk = str(s.get("meeting_key") or "").strip()
        if not mk:
            continue

        dt = _parse_openf1_dt(s.get("date_start"))
        if dt is None:
            continue
        if int(dt.year) != y:
            continue

        slot = grouped.setdefault(mk, {"meeting_key": mk, "sessions": [], "base": s})
        slot["sessions"].append(s)
        if len(str(s.get("meeting_official_name") or "")) > len(str(slot["base"].get("meeting_official_name") or "")):
            slot["base"] = s

    rows: List[Dict[str, Any]] = []
    for mk, obj in grouped.items():
        ss = [x for x in obj.get("sessions", []) if isinstance(x, dict)]
        if not ss:
            continue
        race_s = next((x for x in ss if _session_type_upper(x) == "RACE"), None)
        race_dt = _parse_openf1_dt((race_s or ss[-1]).get("date_start"))
        if race_dt is None:
            continue
        base = obj.get("base") or {}
        _cs2 = str(base.get("circuit_short_name") or "").strip()
        race_name = (
            str(base.get("meeting_name") or "").strip()
            or (f"{_cs2} Grand Prix" if _cs2 else "")
            or (f"{str(base.get('location') or '').strip()} Grand Prix" if str(base.get("location") or "").strip() else "")
            or f"{str(base.get('country_name') or 'F1').strip()} Grand Prix"
        )
        rows.append(
            {
                "meeting_key": mk,
                "race_name": race_name,
                "race_dt": race_dt,
                "sessions": sorted(
                    ss,
                    key=lambda x: _parse_openf1_dt(x.get("date_start")) or datetime.min.replace(tzinfo=timezone.utc),
                ),
            }
        )

    rows.sort(key=lambda x: x["race_dt"])
    for i2, r in enumerate(rows, start=1):
        r["round"] = i2
    return rows

@bot.command(name="openf1check")
@commands.has_permissions(administrator=True)
async def openf1check(ctx, year: int = None):
    target_year = int(year or datetime.now(timezone.utc).year)

    def _probe(endpoint: str, params: Dict[str, Any], caller: str) -> Tuple[str, int, str]:
        t0 = time.time()
        try:
            data = _openf1_get_json(endpoint, params=params, timeout=20, caller=caller)
            elapsed = int((time.time() - t0) * 1000)
            if isinstance(data, list):
                extra = f"rows={len(data)}"
            elif isinstance(data, dict):
                extra = f"keys={len(data.keys())}"
            else:
                extra = f"type={type(data).__name__}"
            return ("OK", elapsed, extra)
        except Exception as e:
            elapsed = int((time.time() - t0) * 1000)
            return ("FAIL", elapsed, str(e)[:140])

    def _run() -> Dict[str, Any]:
        out: Dict[str, Any] = {"year": target_year, "checks": []}
        api_key_present = bool(str(os.getenv("OPENF1_API_KEY") or "").strip())
        auth_url = bool(str(os.getenv("OPENF1_AUTH_URL") or "").strip())
        auth_user = bool(str(os.getenv("OPENF1_AUTH_USERNAME") or "").strip())
        auth_pass = bool(str(os.getenv("OPENF1_AUTH_PASSWORD") or "").strip())
        auth_ready = auth_url and auth_user and auth_pass
        out["auth_mode"] = f"api_key={api_key_present} auth_creds={auth_ready}"

        t0 = time.time()
        try:
            token = _openf1_get_bearer_token(force_refresh=True)
            elapsed = int((time.time() - t0) * 1000)
            out["checks"].append(("auth_token", "OK" if token else "WARN", elapsed, "token present" if token else "no token"))
        except Exception as e:
            elapsed = int((time.time() - t0) * 1000)
            out["checks"].append(("auth_token", "FAIL", elapsed, str(e)[:140]))

        latest_status, latest_ms, latest_extra = _probe("sessions", {"session_key": "latest"}, "openf1check_latest")
        out["checks"].append(("sessions_latest", latest_status, latest_ms, latest_extra))

        year_status, year_ms, year_extra = _probe("sessions", {"year": target_year}, "openf1check_year")
        out["checks"].append((f"sessions_year_{target_year}", year_status, year_ms, year_extra))

        candidate_key = None
        try:
            meetings = _openf1_meeting_groups_for_year(target_year)
            if meetings:
                last = meetings[-1]
                race_session = next((s for s in last["sessions"] if _session_type_upper(s) == "RACE"), None)
                if race_session:
                    candidate_key = race_session.get("session_key")
            out["checks"].append(("meeting_map", "OK", 0, f"meetings={len(meetings)}"))
        except Exception as e:
            out["checks"].append(("meeting_map", "FAIL", 0, str(e)[:140]))

        if candidate_key:
            ds, dms, de = _probe("championship_drivers", {"session_key": candidate_key}, "openf1check_drivers")
            cs, cms, ce = _probe("championship_teams", {"session_key": candidate_key}, "openf1check_teams")
            out["checks"].append(("championship_drivers", ds, dms, de))
            out["checks"].append(("championship_teams", cs, cms, ce))
        else:
            out["checks"].append(("championship_drivers", "WARN", 0, "no candidate race session"))
            out["checks"].append(("championship_teams", "WARN", 0, "no candidate race session"))
        return out

    report = await asyncio.to_thread(_run)
    lines = [
        "\U0001F9EA **OpenF1 Health Check**",
        f"- Year: `{report['year']}`",
        f"- Auth mode: `{report['auth_mode']}`",
    ]
    for name, status, ms, extra in report.get("checks", []):
        badge = "\u2705" if status == "OK" else ("\u26A0\uFE0F" if status == "WARN" else "\u274C")
        lines.append(f"- {badge} `{name}`: **{status}** ({ms}ms) - {extra}")
    await ctx.send("\n".join(lines))

@bot.command(name="racereplay")
@commands.has_permissions(administrator=True)
async def racereplay(ctx, year: int, round_num: int, speed: float = 10.0):
    guild = ctx.guild
    if not guild:
        return await ctx.send("\u274C Must be run in a server.")

    try:
        speed = float(speed)
    except Exception:
        speed = 10.0
    speed = max(0.1, min(50.0, speed))

    max_events = int(os.getenv("RACE_REPLAY_MAX_EVENTS", "350") or 350)
    max_events = max(50, min(2000, max_events))

    existing = RACE_TEST_TASKS.get(guild.id)
    if existing and not existing.done():
        existing.cancel()
        try:
            await existing
        except (asyncio.CancelledError, Exception):
            pass

    async def runner():
        try:
            meetings = await asyncio.to_thread(_openf1_meeting_groups_for_year, int(year))
            if not meetings:
                await ctx.send(f"\u274C No OpenF1 meetings found for `{year}`.")
                return
            race_meetings = [
                m for m in meetings
                if any(_session_type_upper(s) == "RACE" for s in (m.get("sessions") or []))
            ]
            if not race_meetings:
                await ctx.send(f"\u274C No race weekends with a `RACE` session were found for `{year}`.")
                return
            if round_num < 1 or round_num > len(race_meetings):
                await ctx.send(f"\u274C Round must be between `1` and `{len(race_meetings)}` for `{year}`.")
                return

            target = race_meetings[round_num - 1]
            sessions = target.get("sessions") or []
            race_session = next((s for s in sessions if _session_type_upper(s) == "RACE"), None)
            if not race_session:
                await ctx.send(f"\u274C No race session found for `{target['race_name']}`.")
                return

            session_key = race_session.get("session_key")
            if not session_key:
                await ctx.send("\u274C OpenF1 race session is missing `session_key`.")
                return

            rc = await asyncio.to_thread(
                _openf1_get_json,
                "race_control",
                {"session_key": session_key},
                30,
                "racereplay_race_control",
            )
            if not isinstance(rc, list) or not rc:
                await ctx.send("\u274C No race-control events returned for that session.")
                return

            cleaned: List[Tuple[datetime, str]] = []
            for item in rc:
                if not isinstance(item, dict):
                    continue
                msg = str(item.get("message") or "").strip()
                dt = _parse_openf1_dt(item.get("date"))
                if not msg or dt is None:
                    continue
                cleaned.append((dt, msg))
            cleaned.sort(key=lambda x: x[0])
            if not cleaned:
                await ctx.send("\u274C No usable replay events were found.")
                return

            truncated = False
            if len(cleaned) > max_events:
                cleaned = cleaned[:max_events]
                truncated = True

            title = f"{target['race_name']} {year} Replay R{round_num}"
            thread = await _ensure_test_thread(guild, title)
            if not thread:
                await ctx.send("\u274C Could not create/find test thread for replay.")
                return

            await thread.send(
                f"\U0001F9EA Starting replay: **{target['race_name']}** (`{year}` round `{round_num}`)\n"
                f"Session key: `{session_key}` | Speed: `x{speed}` | Events: `{len(cleaned)}`"
                + ("\n\u26A0\uFE0F Replay truncated by `RACE_REPLAY_MAX_EVENTS`." if truncated else "")
            )

            prev_dt: Optional[datetime] = None
            for dt, msg in cleaned:
                if prev_dt is not None:
                    raw_wait = max(0.0, (dt - prev_dt).total_seconds()) / speed
                    await asyncio.sleep(min(3.0, raw_wait))
                await thread.send(f"\U0001F3C1 {msg}")
                prev_dt = dt

            await thread.send(f"\u2705 Replay complete. Posted `{len(cleaned)}` events at `x{speed}`.")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f"[RaceReplay] failed: {e}")
            try:
                await ctx.send(f"\u274C Replay failed: {e}")
            except Exception:
                pass

    RACE_TEST_TASKS[guild.id] = asyncio.create_task(runner())
    await ctx.send(f"\U0001F9EA Queued replay for `{year}` round `{round_num}` at `x{speed}`.")

# ─────────────────────────────────────────────────────────────
# Dashboard integration helpers
# These are called from dashboard.py via bot_reference.
# Sync functions can be called directly; async ones must be
# dispatched via asyncio.run_coroutine_threadsafe().
# ─────────────────────────────────────────────────────────────

def of1_race_live_snapshot() -> dict:
    """Return a JSON-serialisable snapshot of race-live state for the dashboard."""
    guilds: dict = {}
    for gid_raw in set(list(RACE_LIVE_ENABLED.keys()) + list(RACE_LIVE_TASKS.keys())):
        gid_int = int(gid_raw)
        task   = RACE_LIVE_TASKS.get(gid_int)
        feed_raw = RACE_CONTROL_FEED.get(gid_int, [])
        thread = RACE_LIVE_THREADS.get(gid_int)
        guilds[str(gid_int)] = {
            "enabled":      bool(RACE_LIVE_ENABLED.get(gid_int, False)),
            "running":      _task_running(task) if task else False,
            "session_kind": str(RACE_LIVE_SESSION_KINDS.get(gid_int, "")),
            "session_key":  RACE_LIVE_SESSION_KEYS.get(gid_int),
            "last_event_ts": str(RACE_LIVE_LAST_EVENT_TS.get(gid_int) or ""),
            "hold":         _race_live_is_held(gid_int),
            "thread_id":    thread.id   if thread else None,
            "thread_name":  thread.name if thread else None,
            "feed":         list(feed_raw)[-100:],
        }
    return {
        "delay_seconds":  _race_live_delay_seconds(),
        "poll_seconds":   _race_live_poll_seconds(),
        "ops_channel_id": _race_live_ops_channel_id(),
        "guilds": guilds,
    }


def of1_apply_race_setting(key: str, value, guild_id: int = 0) -> tuple:
    """Apply a race-live setting by key. Returns (ok: bool, message: str)."""
    try:
        if key == "delay_seconds":
            v = _set_race_live_delay_seconds(float(value))
            return True, f"delay_seconds set to {v:.1f}s"
        if key == "poll_seconds":
            v = _set_race_live_poll_seconds(float(value))
            return True, f"poll_seconds set to {v:.1f}s"
        if key == "ops_channel_id":
            v = _set_race_live_ops_channel_id(int(value))
            return True, f"ops_channel_id set to {v}"
        if key == "session_key":
            sk = int(value)
            if guild_id:
                RACE_LIVE_SESSION_KEYS[int(guild_id)] = sk
            return True, f"session_key set to {sk}" + (f" for guild {guild_id}" if guild_id else "")
        return False, f"Unknown setting key: '{key}'"
    except Exception as e:
        return False, str(e)


async def of1_dashboard_send_to_thread(guild_id: int, message: str) -> tuple:
    """Send a message to the active race thread for the guild. Returns (ok, msg)."""
    thread = RACE_LIVE_THREADS.get(int(guild_id))
    if thread is None:
        return False, "No active race thread for that guild"
    try:
        await thread.send(message)
        return True, "Sent"
    except Exception as e:
        return False, str(e)


async def of1_dashboard_kill_race_live(guild_id: int) -> tuple:
    """Kill race live for a guild (sets hold so supervisor won't restart)."""
    gid = int(guild_id)
    RACE_LIVE_ENABLED[gid] = False
    _set_race_live_hold(gid, True)
    task = RACE_LIVE_TASKS.get(gid)
    if task and _task_running(task):
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
    RACE_LIVE_TASKS.pop(gid, None)
    RACE_LIVE_SESSION_KINDS.pop(gid, None)
    RACE_LIVE_LAST_EVENT_TS.pop(gid, None)
    RACE_LIVE_THREADS.pop(gid, None)
    RACE_LIVE_SESSION_KEYS.pop(gid, None)
    return True, "Race live killed and hold set"


async def of1_dashboard_start_race_live(guild_id: int) -> tuple:
    """Clear the manual hold so the supervisor will restart race live."""
    _set_race_live_hold(int(guild_id), False)
    return True, "Hold cleared — supervisor will pick up on next cycle"


@bot.listen("on_command")
async def _cmd_log_listener(ctx):
    _log_ctx_command(ctx)

@bot.after_invoke
async def _after_invoke_log(ctx):
    # Catches hybrid commands invoked as slash commands (on_command doesn't fire for those)
    if getattr(ctx, 'interaction', None):
        _log_ctx_command(ctx)

def _log_ctx_command(ctx) -> None:
    try:
        from runtime_store import insert_cmd_log
        insert_cmd_log(
            ts=datetime.now(timezone.utc).isoformat(),
            user=str(ctx.author),
            user_id=str(ctx.author.id),
            guild=str(ctx.guild) if ctx.guild else "DM",
            guild_id=str(ctx.guild.id) if ctx.guild else "",
            command=ctx.command.name if ctx.command else "?",
            full=(getattr(ctx.message, 'content', None) or f"/{ctx.command.name if ctx.command else '?'}")[:300],
        )
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# Dashboard bridge helpers (additions)
# ─────────────────────────────────────────────────────────────

def of1_cmd_log_snapshot() -> list:
    return list(_CMD_LOG)


def of1_openf1_health_snapshot() -> dict:
    with _OPENF1_TRACE_LOCK:
        import copy
        raw = copy.deepcopy(_OPENF1_TRACE)

    window_start = float(raw.get("window_start") or 0)
    by_endpoint: Dict[str, Any] = {}
    for raw_key, stats in (raw.get("rows") or {}).items():
        try:
            _caller, endpoint, status_str = str(raw_key).split("|", 2)
            status = int(status_str)
        except Exception:
            continue
        if not isinstance(stats, dict):
            continue
        cnt = int(stats.get("count", 0) or 0)
        lat_sum = int(stats.get("lat_ms_sum", 0) or 0)
        ep = by_endpoint.setdefault(endpoint, {"calls": 0, "errors": 0, "lat_sum": 0, "last_status": 0})
        ep["calls"] += cnt
        ep["lat_sum"] += lat_sum
        ep["last_status"] = status
        if status >= 400:
            ep["errors"] += cnt

    endpoints = {
        ep: {
            "calls": d["calls"],
            "errors": d["errors"],
            "avg_ms": int(d["lat_sum"] / d["calls"]) if d["calls"] > 0 else 0,
            "last_status": d["last_status"],
        }
        for ep, d in by_endpoint.items()
    }
    return {"window_start": window_start, "endpoints": endpoints}


def of1_member_name_map(guild_id: int) -> dict:
    """Return {str(user_id): display_name} for all in-cache members of a guild."""
    result: Dict[str, str] = {}
    try:
        guild = bot.get_guild(int(guild_id))
        if guild:
            for member in guild.members:
                result[str(member.id)] = member.display_name
    except Exception:
        pass
    return result


def of1_xp_snapshot() -> dict:
    import copy
    return copy.deepcopy(XP_STATE)


def of1_xp_adjust(guild_id: int, user_id: int, delta: int) -> tuple:
    try:
        from xp_storage import get_user_record
        u = get_user_record(XP_STATE, int(guild_id), int(user_id))
        old_xp = int(u.get("xp", 0) or 0)
        new_xp = max(0, old_xp + int(delta))
        u["xp"] = new_xp
        _xp_mark_dirty()
        return True, f"XP adjusted: {old_xp} → {new_xp} (Δ{delta:+d})"
    except Exception as e:
        return False, str(e)


def of1_quiz_snapshot() -> list:
    import copy
    return copy.deepcopy(F1_QUIZ_QUESTIONS)


def of1_quiz_save(questions: list) -> tuple:
    global F1_QUIZ_QUESTIONS
    try:
        clean = [q for q in questions if isinstance(q, dict) and q.get("q")]
        tmp = F1_QUIZ_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(clean, f, indent=2, ensure_ascii=False)
        os.replace(tmp, F1_QUIZ_FILE)
        F1_QUIZ_QUESTIONS = clean
        return True, f"Saved {len(clean)} questions"
    except Exception as e:
        return False, str(e)


def of1_pred_snapshot() -> dict:
    """Return current predictions data for the dashboard."""
    import copy
    root = _predictions_root()
    return copy.deepcopy({"rounds": root.get("rounds", {}), "totals": root.get("totals", {})})


def of1_pred_set_result(round_key: str, category: str, value) -> tuple:
    """Set an actual result for a prediction round. value may be a str or list."""
    try:
        rnd = _pred_round_obj(round_key)
        actual = rnd.setdefault("actual", {})
        valid_cats = {"pole", "p10", "sprint_pole", "sprint_p8", "podium", "sprint_podium"}
        if category not in valid_cats:
            return False, f"Invalid category '{category}'"
        actual[category] = value
        rnd["scored"] = False
        _save_state_quiet()
        return True, f"Set {category} = {value!r} for {round_key}"
    except Exception as e:
        return False, str(e)


bot_token = os.getenv("DISCORD_BOT_TOKEN")
if not bot_token:
    raise RuntimeError("DISCORD_BOT_TOKEN is missing. Put it in your .env file.")

bot.run(bot_token)

from __future__ import annotations

import os
import re
import json
import time
import logging
import asyncio
import random
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

# Load env before importing modules that read env at import time (dashboard.py)
load_dotenv()

from dashboard import start_dashboard_thread, set_bot_reference
from storage import load_config, save_config, load_state, save_state, set_env_value
from settings import LOG_PATH

import io
from PIL import Image, ImageDraw, ImageFont

_RANK_FONT_CACHE: Optional[Tuple[ImageFont.FreeTypeFont | ImageFont.ImageFont, ImageFont.FreeTypeFont | ImageFont.ImageFont, ImageFont.FreeTypeFont | ImageFont.ImageFont]] = None
_RANK_TEMPLATE_CACHE: Dict[str, Image.Image] = {}

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

def _rank_template_base(template_path: str | None, w: int, h: int) -> Image.Image:
    if not template_path:
        return Image.new("RGBA", (w, h), (24, 26, 32, 255))
    cached = _RANK_TEMPLATE_CACHE.get(template_path)
    if cached is None:
        try:
            cached = Image.open(template_path).convert("RGBA").resize((w, h), Image.LANCZOS)
            _RANK_TEMPLATE_CACHE[template_path] = cached
        except Exception:
            return Image.new("RGBA", (w, h), (24, 26, 32, 255))
    return cached.copy()

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
    template_path: str | None = None,
) -> bytes:
    """
    Returns PNG bytes for a rank card with the user's avatar embedded.
    """
    W, H = 900, 260

    # 1) Background (template image OR solid color)
    base = _rank_template_base(template_path, W, H)

    draw = ImageDraw.Draw(base)

    # 2) Avatar
    avatar = await fetch_avatar_image(member, size=256)
    avatar = circle_crop(avatar, 170)
    base.paste(avatar, (35, 45), avatar)

    # Optional: avatar ring
    ring = Image.new("RGBA", (170, 170), (0, 0, 0, 0))
    rd = ImageDraw.Draw(ring)
    rd.ellipse((0, 0, 169, 169), outline=(120, 200, 255, 255), width=6)
    base.paste(ring, (35, 45), ring)

    # 3) Fonts
    # Put a .ttf in your project folder (recommended) like fonts/Inter-SemiBold.ttf
    # Fallback: PIL default (looks meh)
    font_name, font_small, font_tiny = _rank_fonts()

    # 4) Text
    username = member.display_name
    draw.text((230, 45), username, font=font_name, fill=(240, 240, 245, 255))
    draw.text((230, 95), f"Title: {title}", font=font_small, fill=(180, 185, 195, 255))
    draw.text((730, 45), f"LVL {level}", font=font_name, fill=(120, 200, 255, 255))

    # 5) XP bar + numbers
    pct = 0.0 if xp_next <= 0 else (xp / xp_next)
    bar_x, bar_y, bar_w, bar_h = 230, 150, 635, 26
    draw_progress_bar(draw, bar_x, bar_y, bar_w, bar_h, pct)

    draw.text((230, 185), f"XP: {xp} / {xp_next}", font=font_tiny, fill=(180, 185, 195, 255))

    # 6) Export to bytes
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
)

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

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
XP_SAVE_LOCK = asyncio.Lock()
XP_FLUSH_TASK: Optional[asyncio.Task] = None

PERIODIC_ROLE_RECOVERY_TASK: Optional[asyncio.Task] = None
RACE_SUPERVISOR_TASK: Optional[asyncio.Task] = None

def _xp_mark_dirty() -> None:
    global XP_DIRTY
    XP_DIRTY = True

async def xp_flush_loop():
    """Periodic XP flush so we don't write on every message."""
    global XP_DIRTY
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            await asyncio.sleep(int(os.getenv("XP_FLUSH_SECONDS", "30")))
            if not XP_DIRTY:
                continue
            async with XP_SAVE_LOCK:
                if XP_DIRTY:
                    await asyncio.to_thread(save_xp_state, XP_STATE)
                    XP_DIRTY = False
        except Exception as e:
            logging.error(f"[XP] Flush loop error: {e}")

# ----------------------------
# Instagram scrape
# ----------------------------
def fetch_latest_instagram_post(username: str) -> Optional[str]:
    try:
        url = f"https://www.instagram.com/{username}/"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        scripts = soup.find_all("script", type="text/javascript")
        for script in scripts:
            if "window._sharedData" in script.text:
                shortcode = re.search(r'"shortcode":"(.*?)"', script.text)
                if shortcode:
                    return f"https://www.instagram.com/p/{shortcode.group(1)}/"
        return None
    except Exception:
        return None

# ----------------------------
# Standings (Ergast)
# ----------------------------
ERGAST_DRIVER_URL = "https://ergast.com/api/f1/current/driverStandings.json"
ERGAST_CONSTRUCTOR_URL = "https://ergast.com/api/f1/current/constructorStandings.json"
ERGAST_SCHEDULE_URL = "https://ergast.com/api/f1/current.json"

F1_SCHEDULE_CACHE: Dict[str, Any] = {"ts": 0.0, "races": []}
F1_REMINDER_TASK: Optional[asyncio.Task] = None
F1_QUIZ_ACTIVE: Dict[int, Dict[str, Any]] = {}
F1_QUIZ_PENDING_OVERRIDES: Dict[int, Dict[str, Any]] = {}

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

def _clean_text_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()

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

async def fetch_current_season_schedule(force: bool = False) -> List[Dict[str, Any]]:
    now_ts = time.time()
    if (not force) and F1_SCHEDULE_CACHE["races"] and (now_ts - float(F1_SCHEDULE_CACHE["ts"])) < 300:
        return list(F1_SCHEDULE_CACHE["races"])

    data = await asyncio.to_thread(_get_json, ERGAST_SCHEDULE_URL)
    races = data.get("MRData", {}).get("RaceTable", {}).get("Races", []) or []
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
    races = await fetch_current_season_schedule()
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
    if category == "sprint_podium":
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
        req["sprint"] = ["sprint_podium"]
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
    if c == "sprint_podium":
        return "sprint"
    if c in {"podium", "p10"}:
        return "race"
    return "race"

def _prediction_category_display(category: str) -> str:
    return {
        "pole": "Pole",
        "sprint_pole": "Sprint Pole",
        "podium": "Podium",
        "sprint_podium": "Sprint Podium",
        "p10": "P10",
    }.get(category, category)

def _pred_scored_sessions_for_guild(round_obj: Dict[str, Any], guild_id: int) -> Dict[str, bool]:
    scored = round_obj.setdefault("scored_sessions", {})
    gid = str(guild_id)
    if gid not in scored or not isinstance(scored.get(gid), dict):
        scored[gid] = {}
    return scored[gid]

def _score_prediction_category(entry: Dict[str, Any], actual: Dict[str, Any], category: str) -> int:
    category = (category or "").lower().strip()
    if category in {"pole", "p10", "sprint_pole"}:
        pred_key = _clean_text_key(str(entry.get(category) or ""))
        actual_key = _clean_text_key(str(actual.get(category) or ""))
        if not pred_key or not actual_key:
            return 0
        if pred_key == actual_key:
            return 3 if category != "sprint_pole" else 2
        return 0
    if category in {"podium", "sprint_podium"}:
        pred = entry.get(category) or []
        act = actual.get(category) or []
        if not (isinstance(pred, list) and isinstance(act, list) and len(act) >= 3):
            return 0
        act_keys = [_clean_text_key(str(x)) for x in act[:3]]
        exact_points = 5 if category == "podium" else 3
        in_points = 2 if category == "podium" else 1
        pts = 0
        for idx, p in enumerate(pred[:3]):
            pk = _clean_text_key(str(p))
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
    for cat in {"quali": ["pole"], "sprint_quali": ["sprint_pole"], "sprint": ["sprint_podium"], "race": ["podium", "p10"]}.get(session_key, []):
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

async def _announce_prediction_session_scores(ctx, meta: Dict[str, Any], session_key: str) -> bool:
    if ctx.guild is None:
        return False
    rnd = _pred_round_obj(meta["key"])
    scored_map = _pred_scored_sessions_for_guild(rnd, ctx.guild.id)
    if scored_map.get(session_key):
        return False
    if not _prediction_actuals_ready_for_session(meta, rnd, session_key):
        return False

    guild_entries = ((rnd.get("entries") or {}).get(str(ctx.guild.id)) or {})
    totals = _pred_totals_for_guild(ctx.guild.id)
    rows: List[Tuple[int, str]] = []
    for uid, entry in guild_entries.items():
        pts = _score_prediction_session(entry, rnd.get("actual") or {}, session_key)
        totals[str(uid)] = int(totals.get(str(uid), 0) or 0) + pts
        member = ctx.guild.get_member(int(uid))
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
    msg = f"🧮 **{meta['race_name']} — {label} Prediction Points** (spoiler-safe)\n{body}"
    if len(msg) > 1900:
        msg = msg[:1850] + "\n… (truncated)"
    await ctx.send(msg)
    return True

def _normalize_driver_pick(s: str) -> str:
    s = " ".join((s or "").replace("|", " ").split())
    return s.strip()

def _split_podium_picks(raw: str) -> Optional[List[str]]:
    parts = [p.strip() for p in (raw or "").split("|")]
    if len(parts) != 3 or not all(parts):
        return None
    return [_normalize_driver_pick(p) for p in parts]

def _score_prediction(entry: Dict[str, Any], actual: Dict[str, Any]) -> int:
    points = 0
    pred_pole = _clean_text_key(str(entry.get("pole") or ""))
    actual_pole = _clean_text_key(str(actual.get("pole") or ""))
    if pred_pole and actual_pole and pred_pole == actual_pole:
        points += 3

    pred_podium = entry.get("podium") or []
    actual_podium = actual.get("podium") or []
    if isinstance(pred_podium, list) and isinstance(actual_podium, list) and len(actual_podium) >= 3:
        actual_keys = [_clean_text_key(str(x)) for x in actual_podium[:3]]
        for idx, pred in enumerate(pred_podium[:3]):
            pk = _clean_text_key(str(pred))
            if not pk:
                continue
            if idx < len(actual_keys) and pk == actual_keys[idx]:
                points += 5
            elif pk in actual_keys:
                points += 2

    pred_p10 = _clean_text_key(str(entry.get("p10") or ""))
    actual_p10 = _clean_text_key(str(actual.get("p10") or ""))
    if pred_p10 and actual_p10 and pred_p10 == actual_p10:
        points += 3
    return points

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

def _quiz_remove_pending_overrides(guild_id: int, question_key: Optional[str] = None) -> None:
    to_remove: List[int] = []
    for msg_id, meta in F1_QUIZ_PENDING_OVERRIDES.items():
        if int(meta.get("guild_id", 0) or 0) != int(guild_id):
            continue
        if question_key and str(meta.get("question_key") or "") != str(question_key):
            continue
        to_remove.append(msg_id)
    for msg_id in to_remove:
        F1_QUIZ_PENDING_OVERRIDES.pop(msg_id, None)

def _quiz_learn_answer_variant(question_key: str, user_answer: str) -> bool:
    normalized = _clean_text_key(user_answer)
    if not normalized:
        return False
    changed = False
    for q in F1_QUIZ_QUESTIONS:
        if _quiz_question_key(q) != question_key:
            continue
        answers = q.get("answers")
        if not isinstance(answers, list):
            answers = []
            q["answers"] = answers
        existing_norm = {_clean_text_key(str(a)) for a in answers}
        if normalized not in existing_norm:
            answers.append(user_answer.strip())
            changed = True
        break
    if not changed:
        return False
    try:
        with open(F1_QUIZ_FILE, "w", encoding="utf-8") as f:
            json.dump(F1_QUIZ_QUESTIONS, f, indent=2)
            f.write("\n")
        return True
    except Exception as e:
        logging.error(f"[Quiz] Failed to save learned answer variant: {e}")
        return False

async def _quiz_process_reaction_override(payload: discord.RawReactionActionEvent) -> bool:
    if str(payload.emoji) != "🟢":
        return False
    pending = F1_QUIZ_PENDING_OVERRIDES.get(payload.message_id)
    if not pending:
        return False
    if payload.guild_id is None or payload.channel_id is None:
        return False
    if int(pending.get("guild_id", 0) or 0) != int(payload.guild_id):
        return False
    if int(pending.get("channel_id", 0) or 0) != int(payload.channel_id):
        return False
    if bool(pending.get("resolved")):
        return True

    guild = bot.get_guild(payload.guild_id)
    if guild is None:
        return True

    member = getattr(payload, "member", None)
    if member is None:
        try:
            member = await guild.fetch_member(payload.user_id)
        except Exception:
            return True
    if member is None or not getattr(member.guild_permissions, "administrator", False):
        return True

    active = F1_QUIZ_ACTIVE.get(guild.id)
    question_key = str(pending.get("question_key") or "")
    if not active or str(active.get("question_key") or "") != question_key:
        return True
    if time.time() > float(active.get("expires_at", 0) or 0):
        F1_QUIZ_ACTIVE.pop(guild.id, None)
        _quiz_remove_pending_overrides(guild.id, question_key)
        return True

    pending["resolved"] = True
    user_id = int(pending.get("user_id", 0) or 0)
    points = max(1, int(pending.get("points", 1) or 1))
    difficulty = str(pending.get("difficulty") or "easy").lower().strip()
    category = str(pending.get("category") or "general").lower().strip()
    raw_guess = str(pending.get("guess_raw") or "").strip()
    guess_key = _clean_text_key(raw_guess)

    if guess_key:
        answers = active.setdefault("answers", [])
        if guess_key not in answers:
            answers.append(guess_key)
        _quiz_learn_answer_variant(question_key, raw_guess)

    scores = _quiz_scores_for_guild(guild.id)
    uid = str(user_id)
    scores[uid] = int(scores.get(uid, 0) or 0) + points
    _save_state_quiet()

    F1_QUIZ_ACTIVE.pop(guild.id, None)
    _quiz_remove_pending_overrides(guild.id, question_key)

    channel = bot.get_channel(payload.channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(payload.channel_id)
        except Exception:
            channel = None
    if channel is not None:
        try:
            reply_id = int(pending.get("bot_reply_message_id", 0) or 0)
            if reply_id:
                bot_reply = await channel.fetch_message(reply_id)
                await bot_reply.edit(
                    content=(
                        f"✅ Correct, <@{user_id}>! (fixed) "
                        f"({category.replace('_', ' ').title()} · {difficulty.title()}) "
                        f"You earned **{points}** quiz point{'s' if points != 1 else ''}."
                    )
                )
            else:
                await channel.send(
                    f"✅ Correct, <@{user_id}>! (fixed) "
                    f"({category.replace('_', ' ').title()} · {difficulty.title()}) "
                    f"You earned **{points}** quiz point{'s' if points != 1 else ''}."
                )
        except Exception as e:
            logging.warning(f"[Quiz] Failed to edit override response: {e}")
    logging.info(
        f"[Quiz] Override accepted by {member.id} for user {user_id} on question '{question_key[:60]}'"
    )
    return True

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

async def fetch_driver_standings_text(limit: int = 20) -> str:
    data = await asyncio.to_thread(_get_json, ERGAST_DRIVER_URL)
    lists = data.get("MRData", {}).get("StandingsTable", {}).get("StandingsLists", [])
    if not lists:
        return "No standings available."
    standings = lists[0].get("DriverStandings", [])[:limit]

    lines = []
    for s in standings:
        pos = s.get("position", "?")
        pts = s.get("points", "0")
        drv = s.get("Driver", {})
        given = drv.get("givenName", "")
        family = drv.get("familyName", "")
        constructor = (s.get("Constructors") or [{}])[0].get("name", "")
        lines.append(f"{pos:>2}. {given} {family} — {pts} pts ({constructor})")

    updated = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return "🏁 **F1 Driver Standings (Current Season)**\n" + "\n".join(lines) + f"\n\n_Last updated: {updated}_"

async def fetch_constructor_standings_text(limit: int = 10) -> str:
    data = await asyncio.to_thread(_get_json, ERGAST_CONSTRUCTOR_URL)
    lists = data.get("MRData", {}).get("StandingsTable", {}).get("StandingsLists", [])
    if not lists:
        return "No standings available."
    standings = lists[0].get("ConstructorStandings", [])[:limit]

    lines = []
    for s in standings:
        pos = s.get("position", "?")
        pts = s.get("points", "0")
        name = s.get("Constructor", {}).get("name", "")
        lines.append(f"{pos:>2}. {name} — {pts} pts")

    updated = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return "🏁 **F1 Constructor Standings (Current Season)**\n" + "\n".join(lines) + f"\n\n_Last updated: {updated}_"

# ----------------------------
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
set_bot_reference(bot)

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

def _xp_backfill_gain_for_message(msg_id: int, mn: int, mx: int) -> int:
    # Deterministic gain so reruns are stable and don't drift due to random().
    if mx <= mn:
        return mn
    span = (mx - mn) + 1
    return mn + (int(msg_id) % span)

async def _iter_xp_backfill_channels(guild: discord.Guild) -> List[discord.abc.Messageable]:
    channels: List[discord.abc.Messageable] = []
    channels.extend(guild.text_channels)
    # Include active threads (private/public/news/forum) that are currently visible.
    try:
        channels.extend(list(guild.threads))
    except Exception:
        pass
    # De-duplicate by channel id while preserving order.
    seen: set[int] = set()
    out: List[discord.abc.Messageable] = []
    for ch in channels:
        cid = getattr(ch, "id", None)
        if cid is None or cid in seen:
            continue
        seen.add(cid)
        out.append(ch)
    return out

async def _xp_rebuild_guild_from_history(guild: discord.Guild, *, skip_message_id: Optional[int] = None) -> Dict[str, int]:
    """
    Rebuild this guild's XP state from message history using current cooldown/gain settings.
    Silent: no level-up announcements, direct state mutation only.
    """
    gid = str(guild.id)
    mn, mx = xp_gain_range()
    cooldown = xp_cooldown_seconds()

    # Collect all accessible messages first in chronological order across channels.
    history_rows: List[Tuple[int, int]] = []  # (unix_ts, user_id)
    scanned_messages = 0
    scanned_channels = 0
    skipped_channels = 0

    me = guild.me
    channels = await _iter_xp_backfill_channels(guild)
    for ch in channels:
        try:
            if me is not None:
                perms = ch.permissions_for(me)  # type: ignore[attr-defined]
                if not (getattr(perms, "view_channel", False) and getattr(perms, "read_message_history", False)):
                    skipped_channels += 1
                    continue
        except Exception:
            pass

        try:
            async for message in ch.history(limit=None, oldest_first=True):  # type: ignore[attr-defined]
                scanned_messages += 1
                if skip_message_id and int(message.id) == int(skip_message_id):
                    continue
                if message.guild is None or message.guild.id != guild.id:
                    continue
                if getattr(message.author, "bot", False):
                    continue
                if not isinstance(message.author, discord.Member):
                    # fetches may return User in some thread/history contexts; still usable if it has id
                    if not hasattr(message.author, "id"):
                        continue
                ts = int(message.created_at.replace(tzinfo=timezone.utc).timestamp())
                history_rows.append((ts, int(message.author.id)))
        except discord.Forbidden:
            skipped_channels += 1
            continue
        except Exception as e:
            logging.warning(f"[XP] Backfill channel {getattr(ch, 'id', '?')} scan error: {e}")
            skipped_channels += 1
            continue
        scanned_channels += 1

    history_rows.sort(key=lambda x: x[0])

    # Build fresh guild XP users map.
    guild_users: Dict[str, Dict[str, Any]] = {}
    awarded_messages = 0
    for ts, uid in history_rows:
        uid_s = str(uid)
        rec = guild_users.get(uid_s)
        if not isinstance(rec, dict):
            rec = {
                "xp": 0,
                "level": 0,
                "last_msg_ts": 0,
                "messages": 0,
                "card": {"bg_url": None, "accent": None, "tagline": None},
            }
            guild_users[uid_s] = rec

        last_ts = int(rec.get("last_msg_ts", 0) or 0)
        if (ts - last_ts) < max(0, cooldown):
            continue

        gain = _xp_backfill_gain_for_message(uid ^ ts, mn, mx)
        new_xp = int(rec.get("xp", 0) or 0) + gain
        rec["xp"] = new_xp
        rec["level"] = xp_level_from_total(new_xp)
        rec["last_msg_ts"] = ts
        rec["messages"] = int(rec.get("messages", 0) or 0) + 1
        awarded_messages += 1

    XP_STATE.setdefault("guilds", {})
    XP_STATE["guilds"][gid] = {"users": guild_users}
    _xp_mark_dirty()
    # Flush immediately so a restart won't lose the rebuild.
    await asyncio.to_thread(save_xp_state, XP_STATE)
    global XP_DIRTY
    XP_DIRTY = False

    return {
        "channels_scanned": scanned_channels,
        "channels_skipped": skipped_channels,
        "messages_scanned": scanned_messages,
        "eligible_awards": awarded_messages,
        "users_updated": len(guild_users),
        "cooldown_seconds": cooldown,
        "xp_min_gain": mn,
        "xp_max_gain": mx,
    }

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
@bot.command(name="rank")
async def rank(ctx, member: discord.Member = None):
    member = member or ctx.author
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")

    rec = get_user_record(XP_STATE, ctx.guild.id, member.id)
    total_xp = int(rec.get("xp", 0) or 0)
    level, xp, xp_next = xp_progress_to_next(total_xp)
    title = "Rookie" if level < 5 else "Regular" if level < 15 else "Veteran"
    template_path = "assets/rank_template.png" if os.path.exists("assets/rank_template.png") else None

    png_bytes = await build_rank_card_png(
        member=member,
        level=level,
        xp=xp,
        xp_next=xp_next,
        title=title,
        template_path=template_path,
    )

    file = discord.File(io.BytesIO(png_bytes), filename="rank.png")
    await ctx.send(file=file)


@bot.command(name="xpleaderboard", aliases=["xptop"])
async def xpleaderboard(ctx, limit: int = 10):
    """Top XP users in this server."""
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    limit = max(3, min(20, int(limit)))

    rows = get_top_users_by_xp(XP_STATE, ctx.guild.id, limit=limit)
    if not rows:
        return await ctx.send("No XP data yet.")

    lines: List[str] = []
    for i, (uid, xp, lvl) in enumerate(rows, start=1):
        member = ctx.guild.get_member(int(uid))
        name = member.display_name if member else f"<@{uid}>"
        # Recompute level in case someone edited XP manually
        real_lvl = xp_level_from_total(xp)
        lines.append(f"{i:>2}. {name} — **L{real_lvl}** ({xp} XP)")

    await ctx.send("🏆 **XP Leaderboard**\n" + "\n".join(lines))

@bot.command(name="xpset")
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

@bot.command(name="xpreset")
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

@bot.command(name="xpaudit")
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

@bot.command(name="xpbackfillhistory")
@commands.has_permissions(administrator=True)
async def xpbackfillhistory(ctx, mode: str = "rebuild", confirm: str = ""):
    """
    TEMP ADMIN TOOL: rebuild guild XP from existing message history without level-up spam.
    Usage: !xpbackfillhistory rebuild CONFIRM
    """
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    mode = (mode or "").lower().strip()
    if mode != "rebuild":
        return await ctx.send("❌ Only `rebuild` mode is supported right now. Use: `!xpbackfillhistory rebuild CONFIRM`")
    if (confirm or "").strip().upper() != "CONFIRM":
        return await ctx.send(
            "⚠️ This rebuilds XP for the **entire server** from message history and replaces current XP records for this guild.\n"
            "Run: `!xpbackfillhistory rebuild CONFIRM`"
        )

    status_msg = await ctx.send("⏳ Starting silent XP backfill (history rebuild). This may take a while...")
    try:
        summary = await _xp_rebuild_guild_from_history(ctx.guild, skip_message_id=ctx.message.id)
        await status_msg.edit(
            content=(
                "✅ Silent XP backfill complete.\n"
                f"- Users updated: **{summary['users_updated']}**\n"
                f"- Messages scanned: **{summary['messages_scanned']}**\n"
                f"- Awarded XP events (after cooldown): **{summary['eligible_awards']}**\n"
                f"- Channels scanned: **{summary['channels_scanned']}**\n"
                f"- Channels skipped: **{summary['channels_skipped']}**\n"
                f"- Cooldown used: **{summary['cooldown_seconds']}s**\n"
                f"- XP gain range used: **{summary['xp_min_gain']}–{summary['xp_max_gain']}**\n"
                "_No level-up messages were posted during this rebuild._"
            )
        )
    except Exception as e:
        logging.error(f"[XP] Backfill failed for guild {ctx.guild.id}: {e}")
        await status_msg.edit(content=f"❌ XP backfill failed: {e}")

@bot.command(name="xpgate")
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

@bot.command(name="xpgateclear")
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
@bot.command(name="configreload", aliases=["config_reload"])
@commands.has_permissions(administrator=True)
async def configreload(ctx):
    """Reload config.json + state.json without restarting the bot."""
    reload_config_state()
    load_f1_static_data()
    await ctx.send("✅ Reloaded config.json, state.json, and F1 data files.")

# ----------------------------
# Commands: reaction role setup
# ----------------------------
@bot.command(name="setupnotifications", aliases=["setup_notifications"])
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

@bot.command(name="setupcolors", aliases=["setup_colors"])
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

@bot.command(name="setupdrivers", aliases=["setup_drivers"])
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

    description = "🏎 **Choose your favorite F1 driver!**\nReact to get a fan role:"
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
        await ctx.send("⚠️ Missing custom emojis: " + ", ".join(missing))

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
            await ctx.send("⚠️ I can only edit my own messages.")
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

@bot.command(name="ping")
async def ping(ctx):
    await ctx.send("Pong!")

@bot.command(name="help")
async def help(ctx):
    visible = []
    prefix = getattr(ctx, "clean_prefix", None) or "!"
    for cmd in bot.commands:
        try:
            if await cmd.can_run(ctx):
                visible.append(f"{prefix}{cmd.name} - {cmd.help or 'No description'}")
        except Exception:
            continue
    if visible:
        await ctx.send("**Available Commands:**\n" + "\n".join(visible))
    else:
        await ctx.send("❌ You don't have access to any commands.")

def _command_examples(prefix: str) -> Dict[str, str]:
    p = prefix or "!"
    return {
        "ping": f"{p}ping",
        "rank": f"{p}rank @DriverName",
        "xpleaderboard": f"{p}xpleaderboard 10",
        "xpset": f"{p}xpset @DriverName 2500",
        "xpreset": f"{p}xpreset @DriverName",
        "xpaudit": f"{p}xpaudit @DriverName",
        "xpbackfillhistory": f"{p}xpbackfillhistory rebuild CONFIRM",
        "schedule": f"{p}schedule 8",
        "nextsession": f"{p}nextsession",
        "f1reminders": f"{p}f1reminders on #admin-channel",
        "f1reminderleads": f"{p}f1reminderleads 1440 60 15",
        "circuit": f"{p}circuit spa",
        "driverstats": f"{p}driver verstappen",
        "teamstats": f"{p}team ferrari",
        "quiz": f"{p}quiz hard strategy",
        "quizanswer": f"{p}quizanswer virtual safety car",
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
        "racelivekill": f"{p}racelivekill",
        "racetestlist": f"{p}racetestlist",
        "racetestinfo": f"{p}racetestinfo practice_short",
        "raceteststart": f"{p}raceteststart race_chaos 5",
        "raceteststop": f"{p}raceteststop",
    }

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
                desc = cmd.help or "No description"
                visible.append(f"`{prefix}{cmd.name}` - {desc}\nExample: `{ex}`")
        except Exception:
            continue

    if not visible:
        return await ctx.send("❌ You don't have access to any commands.")

    chunks: List[str] = []
    current = "**Available Commands (You Have Access To)**\\n"
    for entry in visible:
        candidate = current + ("\\n\\n" if current.strip() else "") + entry
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

@bot.command(name="quiz", aliases=["f1quiz"])
async def quiz(ctx, *filters: str):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    if not F1_QUIZ_QUESTIONS:
        return await ctx.send("❌ No quiz questions configured.")
    difficulty_filters: set[str] = set()
    category_filters: set[str] = set()
    known_difficulties = set(QUIZ_DIFFICULTY_POINTS.keys())
    known_categories = {_quiz_category_for_question(q) for q in F1_QUIZ_QUESTIONS}
    unknown_filters: List[str] = []

    for f in filters:
        token = _clean_text_key(f).replace(" ", "_")
        if not token:
            continue
        if token in known_difficulties:
            difficulty_filters.add(token)
            continue
        if token in known_categories:
            category_filters.add(token)
            continue
        unknown_filters.append(f)

    if unknown_filters:
        return await ctx.send(
            "❌ Unknown quiz filter(s): "
            + ", ".join(f"`{x}`" for x in unknown_filters)
            + "\\nUse difficulty (`easy`, `medium`, `hard`, `expert`) and/or categories like `rules`, `circuits`, `strategy`, `bot_xp`, `weekend_format`."
        )

    q = _quiz_pick_question(ctx.guild.id, difficulty_filters, category_filters)
    if q is None:
        return await ctx.send("❌ No quiz questions match those filters.")
    difficulty = str(q.get("difficulty") or "easy").lower().strip()
    category = _quiz_category_for_question(q)
    points = _quiz_points_for_question(q)
    F1_QUIZ_ACTIVE[ctx.guild.id] = {
        "question": q["q"],
        "question_key": _quiz_question_key(q),
        "answers": [_clean_text_key(a) for a in q.get("answers", [])],
        "asked_at": time.time(),
        "expires_at": time.time() + 120,
        "asked_by": ctx.author.id,
        "difficulty": difficulty,
        "category": category,
        "points": points,
    }
    await ctx.send(
        "🧠 **F1 Quiz Time!**\n"
        f"{q['q']}\n"
        f"Category: **{category.replace('_', ' ').title()}** · Difficulty: **{difficulty.title()}** · Worth: **{points}** point{'s' if points != 1 else ''}\n"
        "Reply with `!quizanswer <answer>` within 2 minutes."
    )

@bot.command(name="quizanswer")
async def quizanswer(ctx, *, answer: str):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    active = F1_QUIZ_ACTIVE.get(ctx.guild.id)
    if not active:
        return await ctx.send("ℹ️ No active quiz question. Start one with `!quiz`.")
    if time.time() > float(active.get("expires_at", 0)):
        F1_QUIZ_ACTIVE.pop(ctx.guild.id, None)
        return await ctx.send("⌛ That quiz question expired. Start a new one with `!quiz`.")

    guess = _clean_text_key(answer)
    answers = active.get("answers") or []
    if guess in answers:
        scores = _quiz_scores_for_guild(ctx.guild.id)
        uid = str(ctx.author.id)
        points = max(1, int(active.get("points", 1) or 1))
        difficulty = str(active.get("difficulty") or "easy").lower().strip()
        category = str(active.get("category") or "general").lower().strip()
        scores[uid] = int(scores.get(uid, 0) or 0) + points
        _save_state_quiet()
        _quiz_remove_pending_overrides(ctx.guild.id, str(active.get("question_key") or ""))
        F1_QUIZ_ACTIVE.pop(ctx.guild.id, None)
        return await ctx.send(
            f"✅ Correct, {ctx.author.mention}! "
            f"({category.replace('_', ' ').title()} · {difficulty.title()}) "
            f"You earned **{points}** quiz point{'s' if points != 1 else ''}."
        )

    wrong_reply = await ctx.send(
        "❌ Not quite. Try again while the question is still open. "
        "(Admins can react 🟢 to your `!quizanswer` message to mark a valid alternate wording as correct.)"
    )
    try:
        F1_QUIZ_PENDING_OVERRIDES[ctx.message.id] = {
            "guild_id": ctx.guild.id,
            "channel_id": ctx.channel.id,
            "user_id": ctx.author.id,
            "question_key": str(active.get("question_key") or _clean_text_key(str(active.get("question") or ""))),
            "guess_raw": answer.strip(),
            "guess_key": guess,
            "bot_reply_message_id": wrong_reply.id,
            "difficulty": str(active.get("difficulty") or "easy"),
            "category": str(active.get("category") or "general"),
            "points": int(active.get("points", 1) or 1),
            "resolved": False,
            "created_at": time.time(),
        }
    except Exception:
        pass

@bot.command(name="quizscore", aliases=["quizleaderboard"])
async def quizscore(ctx):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    scores = _quiz_scores_for_guild(ctx.guild.id)
    if not scores:
        return await ctx.send("ℹ️ No quiz scores yet.")
    rows = sorted(((int(v), uid) for uid, v in scores.items()), reverse=True)[:20]
    lines = []
    for i, (pts, uid) in enumerate(rows, start=1):
        member = ctx.guild.get_member(int(uid))
        name = member.display_name if member else uid
        lines.append(f"{i:>2}. {name} — **{pts}**")
    await ctx.send("🧠 **F1 Quiz Leaderboard**\n" + "\n".join(lines))

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
    driver_text_task = fetch_driver_standings_text() if driver_msg_id else None
    constructor_text_task = fetch_constructor_standings_text() if constructor_msg_id else None

    driver_text = constructor_text = None
    if driver_text_task or constructor_text_task:
        results = await asyncio.gather(
            driver_text_task if driver_text_task else asyncio.sleep(0, result=None),
            constructor_text_task if constructor_text_task else asyncio.sleep(0, result=None),
            return_exceptions=True,
        )
        driver_text, constructor_text = results

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
        await update_standings_once()
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

@bot.command(name="standingssetup", aliases=["standings_setup"])
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
        msg = await ctx.send("🏁 **F1 Driver Standings (Current Season)**\nLoading...")
        set_env_value("DRIVER_STANDINGS_MESSAGE_ID", str(msg.id))
        created.append(f"✅ Drivers message: `{msg.id}`")

    if which in ("constructors", "both"):
        msg = await ctx.send("🏁 **F1 Constructor Standings (Current Season)**\nLoading...")
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
        "ℹ️ IDs saved to `.env` so it continues after restart."
    )

# ----------------------------
# Commands: F1 schedule / reminders / circuit info
# ----------------------------
@bot.command(name="schedule")
async def schedule(ctx, count: int = 8):
    count = max(1, min(20, int(count)))
    try:
        items = await upcoming_f1_sessions(limit=count)
    except Exception as e:
        logging.error(f"[F1] schedule failed: {e}")
        return await ctx.send("❌ Could not fetch the F1 schedule right now.")

    if not items:
        return await ctx.send("ℹ️ No upcoming sessions found.")

    tz = _f1_tz()
    tz_name = _f1_tz_name() if tz != timezone.utc else "UTC"
    lines = []
    for item in items:
        delta = item["dt"] - datetime.now(timezone.utc)
        hrs = int(delta.total_seconds() // 3600)
        mins = int((delta.total_seconds() % 3600) // 60)
        countdown = f"in {hrs}h {mins}m" if delta.total_seconds() > 0 else "started"
        lines.append(
            f"• **{item['race_name']}** — {item['session_label']} at `{_fmt_dt_local(item['dt'], tz)}` ({countdown})"
        )
    await ctx.send(f"📅 **Upcoming F1 Sessions** ({tz_name})\n" + "\n".join(lines))

@bot.command(name="nextsession", aliases=["nextf1"])
async def nextsession(ctx):
    try:
        items = await upcoming_f1_sessions(limit=1)
    except Exception as e:
        logging.error(f"[F1] nextsession failed: {e}")
        return await ctx.send("❌ Could not fetch the next session right now.")
    if not items:
        return await ctx.send("ℹ️ No upcoming sessions found.")
    item = items[0]
    delta = item["dt"] - datetime.now(timezone.utc)
    total_minutes = max(0, int(delta.total_seconds() // 60))
    days, rem = divmod(total_minutes, 1440)
    hrs, mins = divmod(rem, 60)
    await ctx.send(
        f"⏭️ **Next F1 session:** **{item['race_name']} — {item['session_label']}**\n"
        f"🕒 `{_fmt_dt_local(item['dt'])}` ({_f1_tz_name()})\n"
        f"⏳ In **{days}d {hrs}h {mins}m**"
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
                    if delta_m <= lead and delta_m >= max(0, lead - 1):
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
            logging.error(f"[F1Reminder] loop error: {e}")
        await asyncio.sleep(60)

@bot.command(name="f1reminders")
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
        "ℹ️ **F1 reminders status**\n"
        f"- Enabled: `{cfg['enabled']}`\n"
        f"- Channel: {ch_txt}\n"
        f"- Lead minutes: `{', '.join(str(x) for x in cfg['leads'])}`"
    )

@bot.command(name="f1reminderleads")
@commands.has_permissions(administrator=True)
async def f1reminderleads(ctx, *minutes: int):
    if not minutes:
        return await ctx.send("❌ Usage: `!f1reminderleads 1440 60 15`")
    leads = sorted({max(1, min(10080, int(m))) for m in minutes}, reverse=True)
    reload_config_state()
    CFG["f1_reminder_leads_minutes"] = leads
    save_config(CFG)
    await ctx.send(f"✅ F1 reminder leads set to: `{', '.join(str(x) for x in leads)}` minutes.")

@bot.command(name="circuit")
async def circuit(ctx, *, name: str):
    key, info = _circuit_lookup(name)
    if not info:
        return await ctx.send("❌ Circuit not found in local data. Try a GP/city/circuit name (e.g. `!circuit spa`).")
    race_distance = info.get("race_distance_km")
    if race_distance is None and info.get("length_km") and info.get("laps"):
        race_distance = round(float(info["length_km"]) * int(info["laps"]), 3)
    await ctx.send(
        f"🏎️ **{key.title()}**\n"
        f"- Location: {info.get('location', 'Unknown')}, {info.get('country', 'Unknown')}\n"
        f"- Corners: **{info.get('corners', '?')}**\n"
        f"- Lap length: **{info.get('length_km', '?')} km**\n"
        f"- Race laps: **{info.get('laps', '?')}**\n"
        f"- Race distance: **{race_distance if race_distance is not None else '?'} km**"
    )

@bot.command(name="driverstats", aliases=["driver"])
async def driverstats(ctx, *, query: str):
    try:
        data = await asyncio.to_thread(_get_json, ERGAST_DRIVER_URL)
    except Exception as e:
        logging.error(f"[F1] driverstats failed: {e}")
        return await ctx.send("❌ Could not fetch driver standings.")

    standings_lists = data.get("MRData", {}).get("StandingsTable", {}).get("StandingsLists", [])
    if not standings_lists:
        return await ctx.send("ℹ️ No driver standings available.")
    rows = standings_lists[0].get("DriverStandings", [])
    q = _clean_text_key(query)
    match = None
    for row in rows:
        drv = row.get("Driver", {})
        hay = " ".join(
            str(x) for x in [
                drv.get("driverId"), drv.get("code"), drv.get("givenName"), drv.get("familyName")
            ] if x
        )
        if q in _clean_text_key(hay):
            match = row
            break
    if not match:
        return await ctx.send("❌ Driver not found in current standings.")

    drv = match.get("Driver", {})
    constructor = (match.get("Constructors") or [{}])[0].get("name", "Unknown")
    await ctx.send(
        f"👤 **{drv.get('givenName','')} {drv.get('familyName','')}** ({drv.get('code') or drv.get('driverId') or '?'})\n"
        f"- Position: **P{match.get('position','?')}**\n"
        f"- Points: **{match.get('points','0')}**\n"
        f"- Wins: **{match.get('wins','0')}**\n"
        f"- Team: **{constructor}**"
    )

@bot.command(name="teamstats", aliases=["team"])
async def teamstats(ctx, *, query: str):
    try:
        data = await asyncio.to_thread(_get_json, ERGAST_CONSTRUCTOR_URL)
    except Exception as e:
        logging.error(f"[F1] teamstats failed: {e}")
        return await ctx.send("❌ Could not fetch constructor standings.")

    standings_lists = data.get("MRData", {}).get("StandingsTable", {}).get("StandingsLists", [])
    if not standings_lists:
        return await ctx.send("ℹ️ No constructor standings available.")
    rows = standings_lists[0].get("ConstructorStandings", [])
    q = _clean_text_key(query)
    match = None
    for row in rows:
        c = row.get("Constructor", {})
        hay = " ".join(str(x) for x in [c.get("constructorId"), c.get("name"), c.get("nationality")] if x)
        if q in _clean_text_key(hay):
            match = row
            break
    if not match:
        return await ctx.send("❌ Team not found in current standings.")

    c = match.get("Constructor", {})
    await ctx.send(
        f"🏁 **{c.get('name','Unknown Team')}**\n"
        f"- Position: **P{match.get('position','?')}**\n"
        f"- Points: **{match.get('points','0')}**\n"
        f"- Wins: **{match.get('wins','0')}**\n"
        f"- Nationality: **{c.get('nationality','?')}**"
    )

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

def _pred_entry_summary(entry: Dict[str, Any]) -> str:
    podium = entry.get("podium") or []
    sprint_podium = entry.get("sprint_podium") or []
    podium_txt = " | ".join(str(x) for x in podium) if isinstance(podium, list) and podium else "—"
    sprint_podium_txt = " | ".join(str(x) for x in sprint_podium) if isinstance(sprint_podium, list) and sprint_podium else "—"
    return (
        f"- Pole: `{entry.get('pole') or '—'}`\n"
        f"- Sprint Pole: `{entry.get('sprint_pole') or '—'}`\n"
        f"- Podium: `{podium_txt}`\n"
        f"- Sprint Podium: `{sprint_podium_txt}`\n"
        f"- P10: `{entry.get('p10') or '—'}`"
    )

@bot.command(name="predictpole")
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
    await ctx.send(f"✅ Pole pick saved for **{meta['race_name']}**: `{entry['pole']}`")

@bot.command(name="predictsprintpole", aliases=["predictsppole"])
async def predictsprintpole(ctx, *, driver: str):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    meta = await _prediction_round_context()
    req = _prediction_session_requirements(meta)
    if "sprint_quali" not in req:
        return await ctx.send(f"ℹ️ **{meta['race_name']}** does not appear to have a sprint qualifying/shootout session scheduled.")
    if _prediction_category_locked(meta, "sprint_pole"):
        return await ctx.send(
            f"🔒 Sprint pole predictions are locked for **{meta['race_name']}** "
            f"(locked at Sprint Qualifying/Shootout start: `{_prediction_category_lock_text(meta, 'sprint_pole')}`)."
        )
    entry = _pred_user_entry(meta["key"], ctx.guild.id, ctx.author.id)
    entry["sprint_pole"] = _normalize_driver_pick(driver)
    _save_state_quiet()
    await ctx.send(f"✅ Sprint pole pick saved for **{meta['race_name']}**: `{entry['sprint_pole']}`")

@bot.command(name="predictpodium")
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
    await ctx.send(f"✅ Podium pick saved for **{meta['race_name']}**: `{ ' | '.join(podium) }`")

@bot.command(name="predictsprintpodium", aliases=["predictsppodium"])
async def predictsprintpodium(ctx, *, picks: str):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    meta = await _prediction_round_context()
    req = _prediction_session_requirements(meta)
    if "sprint" not in req:
        return await ctx.send(f"ℹ️ **{meta['race_name']}** does not appear to have a sprint race scheduled.")
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
    await ctx.send(f"✅ Sprint podium pick saved for **{meta['race_name']}**: `{ ' | '.join(podium) }`")

@bot.command(name="predictp10")
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
    await ctx.send(f"✅ P10 pick saved for **{meta['race_name']}**: `{entry['p10']}`")

@bot.command(name="mypredictions")
async def mypredictions(ctx):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    meta = await _prediction_round_context()
    req = _prediction_session_requirements(meta)
    entry = _pred_user_entry(meta["key"], ctx.guild.id, ctx.author.id)
    pole_locked = _prediction_category_locked(meta, "pole")
    sprint_pole_locked = _prediction_category_locked(meta, "sprint_pole") if "sprint_quali" in req else None
    podium_locked = _prediction_category_locked(meta, "podium")
    sprint_podium_locked = _prediction_category_locked(meta, "sprint_podium") if "sprint" in req else None
    p10_locked = _prediction_category_locked(meta, "p10")
    extra_lock_lines = ""
    if sprint_pole_locked is not None:
        extra_lock_lines += f"- Sprint pole locked: `{sprint_pole_locked}`\n"
    if sprint_podium_locked is not None:
        extra_lock_lines += f"- Sprint podium locked: `{sprint_podium_locked}`\n"
    await ctx.send(
        f"📝 **Your predictions** for **{meta['race_name']}** (`{meta['key']}`)\n"
        f"- Pole locked: `{pole_locked}`\n"
        + extra_lock_lines +
        f"- Podium locked: `{podium_locked}`\n"
        f"- P10 locked: `{p10_locked}`\n"
        + _pred_entry_summary(entry)
    )

@bot.command(name="predictions", aliases=["predicthelp"])
async def predictions(ctx):
    meta = await _prediction_round_context()
    req = _prediction_session_requirements(meta)
    lines = [
        f"📋 **Predictions** for **{meta['race_name']}** (`{meta['key']}`)",
        "- `!predictpole <driver>` (locks at Qualifying start)",
        "- `!predictpodium A | B | C` (locks at Race start)",
        "- `!predictp10 <driver>` (locks at Race start)",
    ]
    if "sprint_quali" in req:
        lines.append("- `!predictsprintpole <driver>` (locks at Sprint Qualifying/Shootout start)")
    if "sprint" in req:
        lines.append("- `!predictsprintpodium A | B | C` (locks at Sprint start)")
    lines.append("- `!mypredictions` to view your picks")
    await ctx.send("\n".join(lines))

@bot.command(name="predictionsboard", aliases=["predboard"])
async def predictionsboard(ctx):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    meta = await _prediction_round_context()
    rnd = _pred_round_obj(meta["key"])
    req = _prediction_session_requirements(meta)
    categories = ["pole", "podium", "p10"]
    if "sprint_quali" in req:
        categories.append("sprint_pole")
    if "sprint" in req:
        categories.append("sprint_podium")
    guild_entries = ((rnd.get("entries") or {}).get(str(ctx.guild.id)) or {})
    if not guild_entries:
        return await ctx.send(f"ℹ️ No predictions submitted yet for **{meta['race_name']}**.")
    lines = []
    for uid, entry in list(guild_entries.items())[:20]:
        member = ctx.guild.get_member(int(uid))
        name = member.display_name if member else uid
        filled = sum(1 for k in categories if entry.get(k))
        lines.append(f"• {name} — {filled}/{len(categories)} picks")
    await ctx.send(f"📋 **Predictions board** for **{meta['race_name']}**\n" + "\n".join(lines))

@bot.command(name="predictionslock")
@commands.has_permissions(administrator=True)
async def predictionslock(ctx):
    meta = await _prediction_round_context()
    rnd = _pred_round_obj(meta["key"])
    rnd["locked"] = True
    _save_state_quiet()
    await ctx.send(f"🔒 Predictions locked for **{meta['race_name']}** (`{meta['key']}`).")

@bot.command(name="predictionsunlock")
@commands.has_permissions(administrator=True)
async def predictionsunlock(ctx):
    meta = await _prediction_round_context()
    rnd = _pred_round_obj(meta["key"])
    rnd["locked"] = False
    _save_state_quiet()
    await ctx.send(f"🔓 Predictions unlocked for **{meta['race_name']}** (`{meta['key']}`).")

@bot.command(name="predictionsetresult")
@commands.has_permissions(administrator=True)
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
    else:
        return await ctx.send("❌ Category must be `pole`, `podium`, `p10`, `sprint_pole`, or `sprint_podium`.")
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

@bot.command(name="predictionscore")
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
        await ctx.send("ℹ️ No scoreable prediction sessions yet (missing actuals or already scored).")

@bot.command(name="predictionleaderboard", aliases=["fantasypoints", "predictlb"])
async def predictionleaderboard(ctx):
    if ctx.guild is None:
        return await ctx.send("❌ This must be used in a server.")
    totals = _pred_totals_for_guild(ctx.guild.id)
    if not totals:
        return await ctx.send("ℹ️ No prediction points yet.")
    rows = sorted(((int(v), uid) for uid, v in totals.items()), reverse=True)[:20]
    lines = []
    for rank_i, (pts, uid) in enumerate(rows, start=1):
        member = ctx.guild.get_member(int(uid))
        name = member.display_name if member else uid
        lines.append(f"{rank_i:>2}. {name} — **{pts} pts**")
    await ctx.send("🏆 **Prediction Leaderboard (Fantasy Points)**\n" + "\n".join(lines))

# ----------------------------
# Reaction role handlers + periodic recovery
# ----------------------------
@bot.event
async def on_ready():
    logging.info(f"Bot is online as {bot.user}")
    if not hasattr(bot, "launch_time"):
        bot.launch_time = datetime.now()

    reload_config_state()

    ensure_standings_task_running()
    _ensure_background_task("PERIODIC_ROLE_RECOVERY_TASK", periodic_reaction_role_check, "Recovery")

    # XP flushing loop
    _ensure_background_task("XP_FLUSH_TASK", xp_flush_loop, "XP")

    # Race supervisor loop (your existing module)
    _ensure_background_task("RACE_SUPERVISOR_TASK", race_supervisor_loop, "RaceLive")

    # F1 reminders loop
    _ensure_background_task("F1_REMINDER_TASK", f1_reminder_loop, "F1Reminder")

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id:
        return
    if await _quiz_process_reaction_override(payload):
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

# ============================================================
# Race Live (OpenF1) + Kill Switch + Debug Tail (NO underscores)
# ============================================================

RACE_LIVE_TASKS: Dict[int, asyncio.Task] = {}
RACE_LIVE_ENABLED: Dict[int, bool] = {}
RACE_LIVE_DEBUG: Dict[int, deque] = {}
RACE_LIVE_POSTED_SIGS: Dict[int, set] = {}
RACE_LIVE_POSTED_SIGS_ORDER: Dict[int, deque] = {}

OPENF1_BASE = "https://api.openf1.org/v1"

def _racelog(gid: int, msg: str) -> None:
    buf = RACE_LIVE_DEBUG.setdefault(gid, deque(maxlen=200))
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
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

async def _openf1_get(http: aiohttp.ClientSession, endpoint: str, params: Dict[str, Any]) -> Any:
    url = f"{OPENF1_BASE}/{endpoint.lstrip('/')}"
    async with http.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as r:
        if r.status in (401, 403):
            text = await r.text()
            raise RuntimeError(f"OpenF1 auth error {r.status}: {text[:200]}")
        r.raise_for_status()
        return await r.json()

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

async def _ensure_live_thread(guild: discord.Guild, title: str) -> discord.Thread:
    ch = await _get_forum_channel_live(guild)
    if ch is None:
        raise RuntimeError("RACE_FORUM_CHANNEL_ID not set or not accessible.")

    if isinstance(ch, discord.ForumChannel):
        created = await ch.create_thread(
            name=title,
            content=f"📡 Live thread created by {bot.user.mention}",
            auto_archive_duration=1440,
        )
        if isinstance(created, tuple) and len(created) >= 1:
            return created[0]
        return created

    if isinstance(ch, discord.TextChannel):
        msg = await ch.send(f"📡 **{title}**")
        return await msg.create_thread(name=title, auto_archive_duration=1440)

    raise RuntimeError("RACE_FORUM_CHANNEL_ID must point to a ForumChannel or TextChannel.")

async def _pick_current_meeting_and_window(http: aiohttp.ClientSession) -> Optional[tuple[datetime, datetime, Dict[str, Any], list]]:
    latest = await _openf1_get(http, "sessions", {"session_key": "latest"})
    if not latest:
        return None

    meeting_key = latest[0].get("meeting_key")
    if not meeting_key:
        return None

    all_sessions = await _openf1_get(http, "sessions", {"meeting_key": meeting_key})
    if not all_sessions:
        return None

    relevant = [s for s in all_sessions if _session_type_upper(s) in FOLLOW_SESSION_TYPES]
    if not relevant:
        return None

    starts = [_parse_iso(s["date_start"]) for s in relevant if s.get("date_start")]
    ends = [_parse_iso(s["date_end"]) for s in relevant if s.get("date_end")]
    if not starts or not ends:
        return None

    pad_hours = int(os.getenv("RACE_WINDOW_PADDING_HOURS", "24"))
    pad = timedelta(hours=max(0, min(72, pad_hours)))

    window_start = min(starts) - pad
    window_end = max(ends) + pad

    meta = relevant[0]
    return window_start, window_end, meta, relevant

async def race_live_loop(guild: discord.Guild, thread: discord.Thread, session_key: int):
    gid = guild.id
    RACE_LIVE_ENABLED[gid] = True
    RACE_LIVE_POSTED_SIGS.setdefault(gid, set())
    RACE_LIVE_POSTED_SIGS_ORDER.setdefault(gid, deque())

    poll_s = float(os.getenv("OPENF1_ACTIVE_POLL_SECONDS", "3"))
    poll_s = max(1.0, min(15.0, poll_s))

    _racelog(gid, f"race_live_loop started (session_key={session_key}, poll={poll_s}s)")
    await thread.send(f"📡 Live follower attached. `session_key={session_key}`")

    async with aiohttp.ClientSession(headers={"User-Agent": "OF1-Discord-Bot"}) as http:
        while RACE_LIVE_ENABLED.get(gid, False):
            try:
                _racelog(gid, "poll race_control")
                rc = await _openf1_get(http, "race_control", {"session_key": session_key})
                _racelog(gid, f"race_control items={len(rc)}")

                for item in rc[-30:]:
                    msg = str(item.get("message") or "").strip()
                    if not msg:
                        continue
                    dt = str(item.get("date") or "")
                    sig = f"{dt}|{msg}"
                    if _race_sig_seen_or_add(gid, sig):
                        continue
                    await thread.send(f"🏁 {msg}")

                await asyncio.sleep(poll_s)

            except asyncio.CancelledError:
                _racelog(gid, "race_live_loop cancelled")
                raise
            except Exception as e:
                _racelog(gid, f"ERROR {type(e).__name__}: {e}")
                await asyncio.sleep(5)

    _racelog(gid, "race_live_loop exited")

async def race_supervisor_loop():
    await bot.wait_until_ready()
    logging.info("[RaceLive] Supervisor started")

    idle_s = int(os.getenv("OPENF1_IDLE_CHECK_SECONDS", str(60 * 30)))
    idle_s = max(60, min(60 * 180, idle_s))

    async with aiohttp.ClientSession(headers={"User-Agent": "OF1-Discord-Bot"}) as http:
        while not bot.is_closed():
            try:
                info = await _pick_current_meeting_and_window(http)
                if not info:
                    await asyncio.sleep(idle_s)
                    continue

                window_start, window_end, meta, relevant = info
                now = datetime.now(timezone.utc)
                in_window = window_start <= now <= window_end

                def _key(s):
                    ds = s.get("date_start")
                    return _parse_iso(ds) if ds else datetime.min.replace(tzinfo=timezone.utc)

                latest_relevant = sorted(relevant, key=_key)[-1]
                session_key = int(latest_relevant.get("session_key"))

                for guild in bot.guilds:
                    try:
                        gid = guild.id
                        task = RACE_LIVE_TASKS.get(gid)
                        running = task is not None and not task.done()

                        if in_window and not running:
                            location = str(meta.get("location") or meta.get("meeting_name") or "F1").strip()
                            title = f"{location} — Live Weekend"
                            thread = await _ensure_live_thread(guild, title)

                            _racelog(gid, f"Supervisor starting live loop (session_key={session_key})")
                            RACE_LIVE_ENABLED[gid] = True

                            async def runner(g=guild, th=thread, sk=session_key):
                                try:
                                    await race_live_loop(g, th, sk)
                                except asyncio.CancelledError:
                                    pass
                                except Exception as e:
                                    _racelog(g.id, f"FATAL {type(e).__name__}: {e}")

                            RACE_LIVE_TASKS[gid] = asyncio.create_task(runner())

                        if (not in_window) and running:
                            _racelog(gid, "Supervisor stopping live loop (out of window)")
                            RACE_LIVE_ENABLED[gid] = False
                            task.cancel()
                    except Exception as e:
                        logging.error(f"[RaceLive] Guild {guild.id} supervisor step failed: {e}")

                await asyncio.sleep(60 if in_window else idle_s)

            except Exception as e:
                logging.error(f"[RaceLive] Supervisor error: {e}")
                await asyncio.sleep(60)

@bot.command(name="racelivekill", aliases=["race_live_kill"])
@commands.has_permissions(administrator=True)
async def racelivekill(ctx):
    """Emergency kill switch: stop race-live module only + show tail."""
    guild = ctx.guild
    if not guild:
        return
    gid = guild.id

    RACE_LIVE_ENABLED[gid] = False
    t = RACE_LIVE_TASKS.get(gid)
    if t and not t.done():
        t.cancel()

    tail = _racetail(gid, 20)
    logging.warning(f"[RaceLive][{gid}] KILL SWITCH. Tail:\n{tail}")
    await ctx.send("🛑 **Race live killed.**\n```text\n" + tail[:1800] + "\n```")

@bot.command(name="racelivetail", aliases=["race_live_tail"])
@commands.has_permissions(administrator=True)
async def racelivetail(ctx, lines: int = 20):
    """Show last N debug lines for race-live module."""
    guild = ctx.guild
    if not guild:
        return
    lines = max(1, min(50, int(lines)))
    tail = _racetail(guild.id, lines)
    await ctx.send("```text\n" + tail[:1900] + "\n```")

@bot.command(name="racelivestart", aliases=["race_live_start"])
@commands.has_permissions(administrator=True)
async def racelivestart(ctx):
    """Manually start race-live right now (ignores weekend window)."""
    guild = ctx.guild
    if not guild:
        return
    gid = guild.id

    t = RACE_LIVE_TASKS.get(gid)
    if t and not t.done():
        RACE_LIVE_ENABLED[gid] = False
        t.cancel()

    async with aiohttp.ClientSession(headers={"User-Agent": "OF1-Discord-Bot"}) as http:
        latest = await _openf1_get(http, "sessions", {"session_key": "latest"})
        if not latest:
            return await ctx.send("❌ No OpenF1 sessions available right now.")
        session_key = int(latest[0].get("session_key"))

    title = "F1 — Live (Manual)"
    thread = await _ensure_live_thread(guild, title)

    RACE_LIVE_ENABLED[gid] = True

    async def runner():
        try:
            await race_live_loop(guild, thread, session_key)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            _racelog(gid, f"FATAL {type(e).__name__}: {e}")

    RACE_LIVE_TASKS[gid] = asyncio.create_task(runner())
    await ctx.send(f"✅ Started race live manually (session_key={session_key}).")

@bot.command(name="racelivestop", aliases=["race_live_stop"])
@commands.has_permissions(administrator=True)
async def racelivestop(ctx):
    """Gracefully stop race-live for this guild."""
    guild = ctx.guild
    if not guild:
        return
    gid = guild.id

    RACE_LIVE_ENABLED[gid] = False
    t = RACE_LIVE_TASKS.get(gid)
    if t and not t.done():
        t.cancel()

    await ctx.send("🛑 Race live stopped.")

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
    "SESSION_END":   ("🏁", "**Session ended**"),
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
    "INFO":          ("ℹ️", "**Info**"),
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
        import json
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
    emoji, label = EVENT_STYLE.get(etype, ("ℹ️", "**Info**"))

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
        await asyncio.sleep(0)
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
    await ctx.send("🧪 **Race test scenarios:**\n" + "\n".join(f"- `{n}`" for n in names))

@bot.command(name="racetestinfo", aliases=["race_test_info"])
@commands.has_permissions(administrator=True)
async def racetestinfo(ctx, scenario: str):
    try:
        name, sc = _resolve_scenario(scenario)
    except Exception as e:
        await ctx.send(f"❌ {e}")
        return

    title = _scenario_title(sc, fallback=name)
    session_type = _scenario_session(sc) or "(none)"
    events = sc.get("events") or []
    grid = sc.get("grid") or []
    segments = sc.get("segments") or []
    has_cls = bool((sc.get("classification") or {}).get("results"))

    await ctx.send(
        "🧪 **Scenario info**\n"
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
        await ctx.send(f"❌ {e}")
        return

    session_type = _scenario_session(sc)
    if session_type == "RACE":
        body = _format_race_classification(sc)
        await ctx.send(_wrap_spoiler("📊 Race Classification\n" + body))
    elif session_type in ("QUALI", "QUALIFYING"):
        body = _format_quali_classification(sc)
        await ctx.send(_wrap_spoiler("📊 Qualifying Results\n" + body))
    else:
        await ctx.send(f"ℹ️ Scenario `{name}` has unknown session type `{session_type}`; no formatter yet.")

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

    async def runner():
        try:
            await _run_race_test_scenario(guild, scenario, speed=speed)
        except asyncio.CancelledError:
            logging.info(f"[RaceTest] Cancelled scenario '{scenario}'")
        except Exception as e:
            logging.error(f"[RaceTest] Scenario '{scenario}' failed: {e}")
            try:
                await ctx.send(f"❌ Race test failed: {e}")
            except Exception:
                pass

    task = asyncio.create_task(runner())
    RACE_TEST_TASKS[guild.id] = task

    await ctx.send(f"🧪 Starting race test: `{scenario}` (speed x{speed})")

@bot.command(name="raceteststop", aliases=["race_test_stop"])
@commands.has_permissions(administrator=True)
async def raceteststop(ctx):
    guild = ctx.guild
    if not guild:
        return
    t = RACE_TEST_TASKS.get(guild.id)
    if t and not t.done():
        t.cancel()
        await ctx.send("🛑 Race test stopped.")
    else:
        await ctx.send("ℹ️ No race test running.")

# ----------------------------
# Start dashboard + run bot
# ----------------------------
start_dashboard_thread()

bot_token = os.getenv("DISCORD_BOT_TOKEN")
if not bot_token:
    raise RuntimeError("DISCORD_BOT_TOKEN is missing. Put it in your .env file.")

bot.run(bot_token)

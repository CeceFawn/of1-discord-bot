from __future__ import annotations

import base64
import json
import os
import time
import asyncio
import threading
import subprocess
import secrets
import hmac
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from functools import wraps
from urllib.parse import urlencode

import bcrypt
import psutil
import requests
import queue
from dotenv import load_dotenv
from flask import Flask, request, redirect, url_for, render_template_string, session, abort, jsonify, Response, stream_with_context

load_dotenv()

from settings import LOG_PATH, STATE_PATH, RUNTIME_STATUS_PATH, RUNTIME_DB_PATH, DEPLOY_STATUS_PATH, WATCH_PARTY_PATH

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
GALLERY_DIR = os.path.join(BASE_DIR, "static", "gallery")
F1_QUIZ_PATH = os.path.join(BASE_DIR, "f1_quiz.json")
AUDIT_LOG_PATH = os.path.join(BASE_DIR, "audit.log")
XP_STATE_PATH = os.path.join(BASE_DIR, "xp_state.json")
SCHEDULED_MSGS_PATH = os.path.join(BASE_DIR, "scheduled_messages.json")
_DISCORD_BOT_TOKEN_LOCAL = (os.getenv("DISCORD_BOT_TOKEN") or "").strip()
_SCHEDULED_MSGS_LOCK = threading.Lock()
from storage import load_config, load_state
from runtime_store import get_runtime_status, list_alerts, init_runtime_db

app = Flask(__name__)

# ----------------------------
# Optional bot reference
# ----------------------------
bot_reference = None

def set_bot_reference(bot):
    global bot_reference
    bot_reference = bot

DASHBOARD_STARTED_AT = time.time()

# ----------------------------
# Bot control config (ENV)
# ----------------------------
BOT_SYSTEMD_SERVICE = (os.getenv("BOT_SYSTEMD_SERVICE") or "discordbot.service").strip()
DASHBOARD_SYSTEMD_SERVICE = (os.getenv("DASHBOARD_SYSTEMD_SERVICE") or "of1-dashboard.service").strip()
WEBSITE_SYSTEMD_SERVICE = (os.getenv("WEBSITE_SYSTEMD_SERVICE") or "of1-website.service").strip()
BOT_REPO_DIR = (os.getenv("BOT_REPO_DIR") or "").strip()
if not BOT_REPO_DIR:
    # Fallback: assume dashboard.py is inside the repo
    BOT_REPO_DIR = os.path.abspath(os.path.dirname(__file__))

BOT_VENV_PIP = (os.getenv("BOT_VENV_PIP") or os.path.join(BOT_REPO_DIR, "venv", "bin", "pip")).strip()

# Store last action output for quick debugging
_LAST_ACTION = {"ts": None, "action": None, "ok": None, "output": ""}
_LAST_ACTION_LOCK = threading.Lock()
_DEPLOY_LOCK = threading.Lock()
_DEPLOY_IN_PROGRESS = False
_RUNTIME_STATUS_CACHE = {"ts": 0.0, "data": {}}
_ROUND_META_CACHE = {"ts": 0.0, "data": {}}
_RUNTIME_FILE_CACHE = {"ts": 0.0, "data": {}, "source": "none", "error": ""}

# ----------------------------
# SSE subscriber registry
# ----------------------------
_SSE_SUBSCRIBERS: list[queue.Queue] = []
_SSE_LOCK = threading.Lock()

def _sse_subscribe() -> queue.Queue:
    q: queue.Queue = queue.Queue(maxsize=50)
    with _SSE_LOCK:
        _SSE_SUBSCRIBERS.append(q)
    return q

def _sse_unsubscribe(q: queue.Queue) -> None:
    with _SSE_LOCK:
        try:
            _SSE_SUBSCRIBERS.remove(q)
        except ValueError:
            pass

def _sse_broadcast(event: str, data: dict) -> None:
    """Push an SSE event to all connected subscribers (non-blocking, drops if full)."""
    payload = f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"
    with _SSE_LOCK:
        dead = []
        for q in _SSE_SUBSCRIBERS:
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(q)
        for q in dead:
            try:
                _SSE_SUBSCRIBERS.remove(q)
            except ValueError:
                pass


def _write_deploy_status(payload: dict) -> None:
    try:
        tmp = f"{DEPLOY_STATUS_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload or {}, f, indent=2, ensure_ascii=False)
            f.write("\n")
        os.replace(tmp, DEPLOY_STATUS_PATH)
    except Exception:
        pass


def _read_deploy_status() -> dict:
    try:
        if not os.path.exists(DEPLOY_STATUS_PATH):
            return {}
        with open(DEPLOY_STATUS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _set_last_action(action: str, ok: bool, output: str):
    with _LAST_ACTION_LOCK:
        _LAST_ACTION["ts"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        _LAST_ACTION["action"] = action
        _LAST_ACTION["ok"] = ok
        _LAST_ACTION["output"] = output[-8000:]  # cap output size
    if str(action).startswith("deploy"):
        _write_deploy_status(
            {
                "ts": _LAST_ACTION["ts"],
                "action": action,
                "ok": bool(ok),
                "output": str(output or "")[-8000:],
            }
        )

def _get_last_action():
    with _LAST_ACTION_LOCK:
        return dict(_LAST_ACTION)


def _audit_log(action: str, detail: str = "") -> None:
    try:
        user = session.get("dash_user", "unknown")
    except Exception:
        user = "system"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {user} | {action}"
    if detail:
        line += f" | {detail}"
    try:
        with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ----------------------------
# Auth config
# ----------------------------
SECRET_KEY = (os.getenv("DASHBOARD_SECRET_KEY") or "").strip()
if not SECRET_KEY:
    raise RuntimeError("DASHBOARD_SECRET_KEY missing in .env")
app.secret_key = SECRET_KEY
_https = (os.getenv("DASHBOARD_HTTPS", "false").strip().lower() in ("1", "true", "yes"))
app.config["SESSION_COOKIE_SECURE"]   = _https
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=24)

RAW_USERS = (os.getenv("DASHBOARD_USERS_JSON") or "{}").strip()
try:
    DASH_USERS: dict[str, str] = json.loads(RAW_USERS)
    if not isinstance(DASH_USERS, dict):
        raise ValueError("must be a JSON object")
    if not all(isinstance(k, str) and isinstance(v, str) for k, v in DASH_USERS.items()):
        raise ValueError("must map usernames to bcrypt hash strings")
except Exception as e:
    raise RuntimeError(f"DASHBOARD_USERS_JSON is not valid JSON: {e}")
PASSWORD_LOGIN_ENABLED = False  # Discord OAuth only

DISCORD_CLIENT_ID = (os.getenv("DASHBOARD_DISCORD_CLIENT_ID") or "").strip()
DISCORD_CLIENT_SECRET = (os.getenv("DASHBOARD_DISCORD_CLIENT_SECRET") or "").strip()
DISCORD_REDIRECT_URI = (os.getenv("DASHBOARD_DISCORD_REDIRECT_URI") or "").strip()
DISCORD_ALLOWED_USER_IDS = {
    x.strip() for x in (os.getenv("DASHBOARD_DISCORD_ALLOWED_USER_IDS") or "").split(",") if x.strip()
}
DISCORD_OAUTH_ENABLED = bool(DISCORD_CLIENT_ID and DISCORD_CLIENT_SECRET and DISCORD_REDIRECT_URI and DISCORD_ALLOWED_USER_IDS)
DISCORD_API_BASE = "https://discord.com/api/v10"

ALLOWED_IPS = [x.strip() for x in (os.getenv("DASHBOARD_ALLOWED_IPS") or "").split(",") if x.strip()]

LOGIN_ATTEMPTS: dict[str, list[float]] = {}
MAX_ATTEMPTS = 8
WINDOW_SECONDS = 10 * 60  # 10 minutes

def _client_ip() -> str:
    # X-Forwarded-For is set by nginx/Caddy; take only the first (original client) IP.
    forwarded = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
    return forwarded or request.remote_addr or "unknown"

def _ip_allowed() -> bool:
    if not ALLOWED_IPS:
        return True
    return _client_ip() in ALLOWED_IPS

def _rate_limited() -> bool:
    ip = _client_ip()
    now = time.time()
    arr = LOGIN_ATTEMPTS.get(ip, [])
    arr = [t for t in arr if now - t < WINDOW_SECONDS]
    LOGIN_ATTEMPTS[ip] = arr
    return len(arr) >= MAX_ATTEMPTS

def _record_attempt():
    ip = _client_ip()
    LOGIN_ATTEMPTS.setdefault(ip, []).append(time.time())

def _clear_attempts():
    LOGIN_ATTEMPTS.pop(_client_ip(), None)

def _discord_exchange_code(code: str) -> tuple[bool, dict | str]:
    try:
        r = requests.post(
            f"{DISCORD_API_BASE}/oauth2/token",
            data={
                "client_id": DISCORD_CLIENT_ID,
                "client_secret": DISCORD_CLIENT_SECRET,
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": DISCORD_REDIRECT_URI,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        if r.status_code != 200:
            return False, f"Token exchange failed ({r.status_code})"
        return True, (r.json() or {})
    except Exception as e:
        return False, f"Token exchange error: {e}"

def _discord_fetch_user(access_token: str) -> tuple[bool, dict | str]:
    try:
        r = requests.get(
            f"{DISCORD_API_BASE}/users/@me",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        if r.status_code != 200:
            return False, f"User fetch failed ({r.status_code})"
        return True, (r.json() or {})
    except Exception as e:
        return False, f"User fetch error: {e}"

def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _ip_allowed():
            abort(403)
        if not session.get("dash_user"):
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper

def _csrf_token() -> str:
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token

def _csrf_input() -> str:
    return f'<input type="hidden" name="_csrf" value="{_csrf_token()}">'

@app.after_request
def _security_headers(response):
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.tailwindcss.com; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "img-src 'self' data: https://cdn.discordapp.com; "
        "connect-src 'self';"
    )
    response.headers["Server"] = "OF1"
    return response


@app.before_request
def _csrf_protect():
    if request.method != "POST":
        return
    # Accept token from form body (normal form submits) or X-CSRFToken header (JSON API)
    token = (
        request.form.get("_csrf", "")
        or request.headers.get("X-CSRFToken", "")
    )
    expected = session.get("_csrf_token", "")
    if not token or not expected or not hmac.compare_digest(token, expected):
        abort(400)


_QUIZ_UNDO_ENDPOINTS = {
    "quiz_mgr", "quiz_mgr_delete", "quiz_mgr_delete_bulk",
    "quiz_mgr_undo", "quiz_mgr_add", "quiz_mgr_edit",
    "quiz_mgr_clear_filter", "quiz_mgr_import", "static",
}

@app.before_request
def _clear_quiz_undo_on_navigate():
    if session.get("quiz_undo_data") and request.endpoint not in _QUIZ_UNDO_ENDPOINTS:
        session.pop("quiz_undo_data", None)
        session.pop("quiz_undo_type", None)
        session.pop("quiz_undo_idx", None)
        session.pop("quiz_undo_indices", None)

# ----------------------------
# UI helpers
# ----------------------------
BASE_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>{{ page_title }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <script src="https://cdn.tailwindcss.com"></script>
  <meta name="csrf-token" content="{{ csrf_token }}">
  <style>
    [x-cloak] { display: none; }
    .nav-link.active { background: #1f1f1f; color: #fff; }
  </style>
</head>
<body class="bg-[#0a0a0a] text-gray-200 min-h-screen">

  <!-- Mobile top bar -->
  <header class="lg:hidden fixed top-0 inset-x-0 z-40 bg-[#111] border-b border-[#222] flex items-center gap-3 px-4 h-14">
    <button id="menuBtn" class="text-gray-400 hover:text-white p-1 rounded">
      <svg id="menuIcon" class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" d="M4 6h16M4 12h16M4 18h16"/>
      </svg>
    </button>
    <span class="font-bold text-white tracking-wide">OF1 Dashboard</span>
    {% if bot_name %}
      <span class="ml-auto text-xs text-gray-500 truncate max-w-[140px]">{{ bot_name }}</span>
    {% endif %}
  </header>

  <!-- Sidebar overlay -->
  <div id="sidebarOverlay" class="lg:hidden fixed inset-0 z-30 bg-black/60 hidden"></div>

  <!-- Sidebar -->
  <aside id="sidebar"
    class="fixed top-0 left-0 h-full w-56 bg-[#111] border-r border-[#222] z-40 flex flex-col
           transition-transform duration-200 -translate-x-full lg:translate-x-0">

    <!-- Sidebar header -->
    <div class="flex items-center gap-2 px-5 py-4 border-b border-[#222]">
      <div>
        <div class="font-extrabold text-white text-lg leading-none">OF1</div>
        <div class="text-xs text-gray-500">Dashboard</div>
      </div>
      <div class="ml-auto flex items-center gap-1.5">
        {% if bot_ok is not none %}
          <span class="inline-block w-2 h-2 rounded-full {% if bot_ok %}bg-green-500{% else %}bg-red-500{% endif %}" title="Bot {% if bot_ok %}running{% else %}stopped{% endif %}"></span>
        {% endif %}
        {% if bot_name %}
          <span class="text-xs text-gray-600 truncate max-w-[80px]" title="{{ bot_name }}">{{ bot_name }}</span>
        {% endif %}
      </div>
    </div>

    <!-- Nav links -->
    <nav class="flex-1 overflow-y-auto p-2 space-y-0.5">
      <p class="px-3 pt-2 pb-0.5 text-[10px] uppercase tracking-widest text-gray-600">System</p>
      <a href="{{ url_for('logs') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M4 6h16M4 10h16M4 14h10"/>
        </svg>
        Logs
      </a>
      <a href="{{ url_for('status') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/>
        </svg>
        Status
      </a>
      <a href="{{ url_for('cmd_log') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/>
        </svg>
        Cmd Log
      </a>
      <a href="{{ url_for('openf1_health') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M9 3H5a2 2 0 00-2 2v4m6-6h10a2 2 0 012 2v4M9 3v18m0 0h10a2 2 0 002-2V9M9 21H5a2 2 0 01-2-2V9m0 0h18"/>
        </svg>
        OpenF1 Health
      </a>
      <a href="{{ url_for('audit_log') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/>
        </svg>
        Audit Log
      </a>

      <p class="px-3 pt-3 pb-0.5 text-[10px] uppercase tracking-widest text-gray-600">Community</p>
      <a href="{{ url_for('watch_party_editor') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/>
        </svg>
        Watch Party
      </a>
      <a href="{{ url_for('discord_events') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/>
        </svg>
        Discord Events
      </a>
      <a href="{{ url_for('gallery_mgr') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"/>
        </svg>
        Gallery
      </a>
      <a href="{{ url_for('member_stats') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z"/>
        </svg>
        Member Stats
      </a>
      <a href="{{ url_for('predictions_view') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"/>
        </svg>
        Predictions
      </a>

      <p class="px-3 pt-3 pb-0.5 text-[10px] uppercase tracking-widest text-gray-600">Bot Tools</p>
      <a href="{{ url_for('quiz_mgr') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
        </svg>
        Quiz
      </a>
      <a href="{{ url_for('xp_mgr') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M11.049 2.927c.3-.921 1.603-.921 1.902 0l1.519 4.674a1 1 0 00.95.69h4.915c.969 0 1.371 1.24.588 1.81l-3.976 2.888a1 1 0 00-.363 1.118l1.518 4.674c.3.922-.755 1.688-1.538 1.118l-3.976-2.888a1 1 0 00-1.176 0l-3.976 2.888c-.783.57-1.838-.197-1.538-1.118l1.518-4.674a1 1 0 00-.363-1.118l-3.976-2.888c-.784-.57-.38-1.81.588-1.81h4.914a1 1 0 00.951-.69l1.519-4.674z"/>
        </svg>
        XP Manager
      </a>
      <a href="{{ url_for('announce') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M11 5.882V19.24a1.76 1.76 0 01-3.417.592l-2.147-6.15M18 13a3 3 0 100-6M5.436 13.683A4.001 4.001 0 017 6h1.832c4.1 0 7.625-1.234 9.168-3v14c-1.543-1.766-5.067-3-9.168-3H7a3.988 3.988 0 01-1.564-.317z"/>
        </svg>
        Announce
      </a>
      <a href="{{ url_for('schedule_msgs') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/>
        </svg>
        Schedule
      </a>

      <p class="px-3 pt-3 pb-0.5 text-[10px] uppercase tracking-widest text-gray-600">Race</p>
      <a href="{{ url_for('race_live') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-yellow-500 hover:bg-[#1a1a1a] hover:text-yellow-300 text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M3 21l9-18 9 18M5.5 16.5h13"/>
        </svg>
        Race Live
      </a>
    </nav>

    <!-- Bot controls -->
    <div class="p-2 border-t border-[#222] space-y-0.5">
      <p class="px-3 pt-1 pb-0.5 text-[10px] uppercase tracking-widest text-gray-600">Bot Controls</p>

      <form data-async-refresh="1" action="{{ url_for('bot_action', action='restart') }}" method="post"
            onsubmit="return confirm('Restart the bot? It will be offline for a few seconds.')">
        {{ csrf_input|safe }}
        <button class="w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-red-400 hover:bg-[#1a1a1a] text-sm transition-colors cursor-pointer">
          <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
          </svg>
          Restart Bot
        </button>
      </form>

      <!-- Deploy dropdown -->
      <div class="relative" id="deployWrap">
        <button id="deployBtn" type="button"
          class="w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
          <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12"/>
          </svg>
          Deploy
          <svg class="w-3 h-3 ml-auto" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" d="M19 9l-7 7-7-7"/>
          </svg>
        </button>
        <div id="deployMenu" class="hidden absolute bottom-full left-0 right-0 mb-1 bg-[#181818] border border-[#2a2a2a] rounded-xl shadow-2xl overflow-hidden z-50 py-1">
          <form data-async-refresh="1" action="{{ url_for('bot_action', action='deployall') }}" method="post"
                onsubmit="return confirm('Deploy all? Bot + website + dashboard will reload.')">
            {{ csrf_input|safe }}
            <button class="w-full text-left px-4 py-2 text-gray-300 hover:bg-[#222] text-sm">Deploy All</button>
          </form>
          <div class="my-1 border-t border-[#2a2a2a]"></div>
          <form data-async-refresh="1" action="{{ url_for('bot_action', action='deploybot') }}" method="post"
                onsubmit="return confirm('Deploy bot only?')">
            {{ csrf_input|safe }}
            <button class="w-full text-left px-4 py-2 text-gray-400 hover:bg-[#222] text-sm">Bot only</button>
          </form>
          <form data-async-refresh="1" action="{{ url_for('bot_action', action='deploywebsite') }}" method="post"
                onsubmit="return confirm('Deploy website only?')">
            {{ csrf_input|safe }}
            <button class="w-full text-left px-4 py-2 text-gray-400 hover:bg-[#222] text-sm">Website only</button>
          </form>
          <form data-async-refresh="1" action="{{ url_for('bot_action', action='deploydashboard') }}" method="post"
                onsubmit="return confirm('Deploy dashboard only? Page will reload.')">
            {{ csrf_input|safe }}
            <button class="w-full text-left px-4 py-2 text-gray-400 hover:bg-[#222] text-sm">Dashboard only</button>
          </form>
        </div>
      </div>

      <!-- Start / Stop -->
      <div class="relative" id="moreWrap">
        <button id="moreBtn" type="button"
          class="w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-500 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
          <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" d="M5 12h.01M12 12h.01M19 12h.01"/>
          </svg>
          More
          <svg class="w-3 h-3 ml-auto" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" d="M19 9l-7 7-7-7"/>
          </svg>
        </button>
        <div id="moreMenu" class="hidden absolute bottom-full left-0 right-0 mb-1 bg-[#181818] border border-[#2a2a2a] rounded-xl shadow-2xl overflow-hidden z-50 py-1">
          <form data-async-refresh="1" action="{{ url_for('bot_action', action='start') }}" method="post"
                onsubmit="return confirm('Start the bot process?')">
            {{ csrf_input|safe }}
            <button class="w-full text-left px-4 py-2 text-gray-300 hover:bg-[#222] text-sm">Start Bot</button>
          </form>
          <form data-async-refresh="1" action="{{ url_for('bot_action', action='stop') }}" method="post"
                onsubmit="return confirm('Stop the bot? It will go offline until restarted.')">
            {{ csrf_input|safe }}
            <button class="w-full text-left px-4 py-2 text-gray-300 hover:bg-[#222] text-sm">Stop Bot</button>
          </form>
        </div>
      </div>
    </div>

    <!-- Footer: time + sign out -->
    <div class="p-2 border-t border-[#222]">
      <div class="px-3 py-1 text-[11px] text-gray-600">{{ now }}</div>
      <form action="{{ url_for('logout') }}" method="post">
        {{ csrf_input|safe }}
        <button class="w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-600 hover:bg-[#1a1a1a] hover:text-gray-300 text-sm transition-colors">
          <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"/>
          </svg>
          Sign out
        </button>
      </form>
    </div>
  </aside>

  <!-- Main content -->
  <main class="lg:ml-56 pt-14 lg:pt-0 min-h-screen">
    {% if flash %}
      <div class="mx-4 mt-4 p-3 bg-[#1a1a1a] border border-[#2a2a2a] rounded-xl text-sm">{{ flash|safe }}</div>
    {% endif %}
    <div class="p-4 lg:p-6">
      {{ body|safe }}
    </div>
  </main>

  <script>
  (function(){
    // --- Sidebar toggle (mobile) ---
    const menuBtn = document.getElementById('menuBtn');
    const sidebar = document.getElementById('sidebar');
    const overlay = document.getElementById('sidebarOverlay');

    function openSidebar() {
      sidebar.classList.remove('-translate-x-full');
      overlay.classList.remove('hidden');
    }
    function closeSidebar() {
      sidebar.classList.add('-translate-x-full');
      overlay.classList.add('hidden');
    }
    menuBtn && menuBtn.addEventListener('click', function(){ sidebar.classList.contains('-translate-x-full') ? openSidebar() : closeSidebar(); });
    overlay && overlay.addEventListener('click', closeSidebar);

    // --- Dropdown menus ---
    function makeDropdown(btnId, menuId) {
      const btn = document.getElementById(btnId);
      const menu = document.getElementById(menuId);
      if (!btn || !menu) return;
      btn.addEventListener('click', function(e){ e.stopPropagation(); menu.classList.toggle('hidden'); });
      document.addEventListener('click', function(){ menu.classList.add('hidden'); });
      menu.addEventListener('click', function(e){ e.stopPropagation(); });
    }
    makeDropdown('deployBtn', 'deployMenu');
    makeDropdown('moreBtn', 'moreMenu');

    // --- Async form submit ---
    document.querySelectorAll('form[data-async-refresh="1"]').forEach(function(form){
      form.addEventListener('submit', async function(e){
        e.preventDefault();
        const btn = form.querySelector('button');
        if (btn) btn.disabled = true;
        try {
          await fetch(form.action, { method:'POST', body: new FormData(form), credentials:'same-origin' });
        } catch(_) { form.submit(); return; }
        window.location.reload();
      });
    });

    // --- Active nav link ---
    var path = window.location.pathname;
    document.querySelectorAll('.nav-link').forEach(function(a){
      if (a.getAttribute('href') === path) a.classList.add('active');
    });
  })();
  </script>
</body>
</html>
"""

def _escape(s: str) -> str:
    return s.replace("<", "&lt;").replace(">", "&gt;")

def _empty_row(ncols: int, msg: str = "No data") -> str:
    return f'<tr><td colspan="{ncols}" class="px-3 py-4 text-gray-500 text-sm">{msg}</td></tr>'

_SECTION_TITLES = {
    "logs": "Logs",
    "status": "Status",
    "cmd_log": "Command Log",
    "openf1_health": "OpenF1 Health",
    "watch_party_editor": "Watch Party",
    "discord_events": "Discord Events",
    "gallery_mgr": "Gallery",
    "member_stats": "Member Stats",
    "quiz_mgr": "Quiz Manager",
    "xp_mgr": "XP Manager",
    "announce": "Announce",
    "schedule_msgs": "Schedule",
    "race_live": "Race Live",
    "audit_log": "Audit Log",
}

def _render(body: str, flash: str = ""):
    bot_name = None
    try:
        if bot_reference and getattr(bot_reference, "user", None):
            bot_name = str(bot_reference.user)
    except Exception:
        bot_name = None

    try:
        section = _SECTION_TITLES.get(request.endpoint or "", "")
    except Exception:
        section = ""
    page_title = f"OF1 Dashboard — {section}" if section else "OF1 Dashboard"

    try:
        bot_ok, _ = _service_is_active(BOT_SYSTEMD_SERVICE)
    except Exception:
        bot_ok = None

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return render_template_string(
        BASE_TEMPLATE,
        body=body,
        flash=flash,
        bot_name=bot_name,
        bot_ok=bot_ok,
        now=now,
        csrf_input=_csrf_input(),
        csrf_token=_csrf_token(),
        page_title=page_title,
    )

def _build_logs_view_data(tail_n: int, show_filtered: bool) -> dict:
    cfg = load_config() or {}
    filters = cfg.get("log_filters", []) or []

    try:
        with open(LOG_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()[-tail_n:]
    except FileNotFoundError:
        lines = ["Log file not found.\n"]
    except Exception as e:
        lines = [f"Unable to read log file: {e}\n"]

    if show_filtered and filters:
        lines = [line for line in lines if not any(x in line for x in filters)]

    safe_logs = _escape("".join(lines))

    last = _get_last_action()
    deploy_status = _read_deploy_status()
    last_html = ""
    if last.get("ts"):
        ok = last.get("ok")
        badge = '<span class="text-green-400 font-semibold">OK</span>' if ok else '<span class="text-red-400 font-semibold">FAILED</span>'
        last_html = (
            f'<div class="bg-[#111] border border-[#222] rounded-xl p-4 mb-3">'
            f'<div class="flex items-center gap-2 mb-2 text-xs text-gray-500">'
            f'<span class="text-gray-300 font-medium">{_escape(str(last.get("action")))}</span>'
            f'<span>·</span>{badge}<span>·</span>'
            f'<span>{_escape(str(last.get("ts")))}</span>'
            f'</div>'
            f'<pre class="text-xs text-gray-400 whitespace-pre-wrap bg-[#0a0a0a] rounded-lg p-3 overflow-x-auto">{_escape(last.get("output") or "")}</pre>'
            f'</div>'
        )

    deploy_html = ""
    if deploy_status.get("ts"):
        ok = deploy_status.get("ok")
        badge = '<span class="text-green-400 font-semibold">OK</span>' if ok else '<span class="text-red-400 font-semibold">FAILED</span>'
        deploy_html = (
            f'<div class="bg-[#111] border border-[#222] rounded-xl p-4 mb-3">'
            f'<div class="flex items-center gap-2 mb-2 text-xs text-gray-500">'
            f'<span class="text-gray-400">Last deploy:</span>'
            f'<span class="text-gray-300 font-medium">{_escape(str(deploy_status.get("action")))}</span>'
            f'<span>·</span>{badge}<span>·</span>'
            f'<span>{_escape(str(deploy_status.get("ts")))}</span>'
            f'</div>'
            f'<pre class="text-xs text-gray-400 whitespace-pre-wrap bg-[#0a0a0a] rounded-lg p-3 overflow-x-auto">{_escape(str(deploy_status.get("output") or ""))}</pre>'
            f'</div>'
        )

    return {
        "safe_logs": safe_logs,
        "last_html": last_html,
        "deploy_html": deploy_html,
        "last_ts": str(last.get("ts") or ""),
    }

def _parse_iso_utc(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def _fmt_ts_utc(raw: str | None) -> str:
    dt = _parse_iso_utc(raw)
    if not dt:
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

def _fmt_relative(raw: str | None) -> str:
    dt = _parse_iso_utc(raw)
    if not dt:
        return "-"
    delta = datetime.now(timezone.utc) - dt
    sec = int(abs(delta.total_seconds()))
    if sec < 60:
        amount, unit = sec, "s"
    elif sec < 3600:
        amount, unit = sec // 60, "m"
    elif sec < 86400:
        amount, unit = sec // 3600, "h"
    else:
        amount, unit = sec // 86400, "d"
    if delta.total_seconds() >= 0:
        return f"{amount}{unit} ago"
    return f"in {amount}{unit}"

def _runtime_file_snapshot() -> dict:
    now_ts = time.time()
    if (now_ts - float(_RUNTIME_FILE_CACHE.get("ts", 0.0))) < 5.0:
        cached = _RUNTIME_FILE_CACHE.get("data")
        return dict(cached) if isinstance(cached, dict) else {}
    read_error = ""
    try:
        db_data = get_runtime_status()
        if isinstance(db_data, dict) and db_data:
            _RUNTIME_FILE_CACHE["ts"] = now_ts
            _RUNTIME_FILE_CACHE["data"] = dict(db_data)
            _RUNTIME_FILE_CACHE["source"] = "db"
            _RUNTIME_FILE_CACHE["error"] = ""
            return db_data
    except Exception as e:
        read_error = f"db read failed: {e}"
    try:
        with open(RUNTIME_STATUS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _RUNTIME_FILE_CACHE["ts"] = now_ts
            _RUNTIME_FILE_CACHE["data"] = dict(data)
            _RUNTIME_FILE_CACHE["source"] = "file"
            _RUNTIME_FILE_CACHE["error"] = ""
            return data
    except Exception as e:
        if read_error:
            read_error += f" | file read failed: {e}"
        else:
            read_error = f"file read failed: {e}"
    _RUNTIME_FILE_CACHE["ts"] = now_ts
    _RUNTIME_FILE_CACHE["data"] = {}
    _RUNTIME_FILE_CACHE["source"] = "none"
    _RUNTIME_FILE_CACHE["error"] = read_error
    return {}

def _bot_runtime_status() -> dict:
    now_ts = time.time()
    if (now_ts - float(_RUNTIME_STATUS_CACHE.get("ts", 0.0))) < 5.0:
        cached = _RUNTIME_STATUS_CACHE.get("data")
        return dict(cached) if isinstance(cached, dict) else {}
    try:
        if not bot_reference:
            snap = _runtime_file_snapshot()
            runtime = snap.get("runtime")
            return dict(runtime) if isinstance(runtime, dict) else {}
        fn = getattr(bot_reference, "of1_runtime_status_snapshot", None)
        if callable(fn):
            data = fn()
            if isinstance(data, dict):
                _RUNTIME_STATUS_CACHE["ts"] = now_ts
                _RUNTIME_STATUS_CACHE["data"] = dict(data)
                return data
    except Exception:
        pass
    snap = _runtime_file_snapshot()
    runtime = snap.get("runtime")
    return dict(runtime) if isinstance(runtime, dict) else {}

def _bot_round_meta(timeout_s: float = 4.0) -> dict:
    now_ts = time.time()
    if (now_ts - float(_ROUND_META_CACHE.get("ts", 0.0))) < 10.0:
        cached = _ROUND_META_CACHE.get("data")
        return dict(cached) if isinstance(cached, dict) else {}
    try:
        if not bot_reference:
            snap = _runtime_file_snapshot()
            meta = snap.get("round_meta")
            return dict(meta) if isinstance(meta, dict) else {}
        coro_fn = getattr(bot_reference, "of1_current_or_next_round_meta_coro", None)
        loop = getattr(bot_reference, "loop", None)
        if not callable(coro_fn) or loop is None:
            snap = _runtime_file_snapshot()
            meta = snap.get("round_meta")
            return dict(meta) if isinstance(meta, dict) else {}
        fut = asyncio.run_coroutine_threadsafe(coro_fn(), loop)
        data = fut.result(timeout=max(0.5, float(timeout_s)))
        if isinstance(data, dict):
            _ROUND_META_CACHE["ts"] = now_ts
            _ROUND_META_CACHE["data"] = dict(data)
            return data
        return {}
    except Exception:
        snap = _runtime_file_snapshot()
        meta = snap.get("round_meta")
        return dict(meta) if isinstance(meta, dict) else {}

def _recent_log_alerts(limit: int = 20, tail_n: int = 1500) -> list[str]:
    try:
        with open(LOG_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()[-max(200, int(tail_n)):]
    except Exception:
        return []
    keys = (" - ERROR - ", "Traceback", "[CmdError]", "FAILED", "Exception")
    hits = [ln.rstrip("\n") for ln in lines if any(k in ln for k in keys)]
    return hits[-max(1, int(limit)):]

def _status_view_data() -> dict:
    st = load_state() or {}
    runtime = _bot_runtime_status()
    round_meta = _bot_round_meta()
    now = datetime.now(timezone.utc)

    race_root = (st.get("race_threads") or {})
    rounds = (race_root.get("rounds") or {}) if isinstance(race_root, dict) else {}
    flat_threads = []
    for round_key, robj in (rounds.items() if isinstance(rounds, dict) else []):
        if not isinstance(robj, dict):
            continue
        race_name = str(robj.get("race_name") or round_key)
        guilds = robj.get("guilds") or {}
        if not isinstance(guilds, dict):
            continue
        for gid, rec in guilds.items():
            if not isinstance(rec, dict):
                continue
            item = dict(rec)
            item["round_key"] = str(round_key)
            item["race_name"] = race_name
            item["guild_id"] = str(gid)
            flat_threads.append(item)

    flat_threads.sort(key=lambda x: (_parse_iso_utc(x.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
    active_threads = [x for x in flat_threads if str(x.get("weekend_state") or "").lower() == "active"]
    past_threads = [x for x in flat_threads if str(x.get("weekend_state") or "").lower() == "past"]
    queued_threads = [x for x in flat_threads if str(x.get("weekend_state") or "").lower() == "queued"]

    current_round_key = str(round_meta.get("key") or "")
    current_round_name = str(round_meta.get("race_name") or current_round_key or "Next round")
    round_sessions = round_meta.get("sessions") or []
    first_session = None
    if isinstance(round_sessions, list):
        dts = []
        for s in round_sessions:
            if not isinstance(s, dict):
                continue
            dt = _parse_iso_utc(str(s.get("dt") or ""))
            if dt:
                dts.append(dt)
        if dts:
            first_session = min(dts)
    race_dt = _parse_iso_utc(str(round_meta.get("race_dt") or ""))
    runtime_window = runtime.get("openf1_window") if isinstance(runtime, dict) else {}
    pre_h = int((runtime_window or {}).get("pre_buffer_hours", 24) or 24)
    queued_eta = (first_session or race_dt)
    if queued_eta:
        queued_eta = queued_eta - timedelta(hours=max(0, min(72, pre_h)))

    current_round_record = None
    if current_round_key:
        for item in flat_threads:
            if str(item.get("round_key")) == current_round_key:
                current_round_record = item
                break

    recent_alerts = list_alerts(limit=20)
    log_alert_lines = _recent_log_alerts(limit=20)

    runtime_ts = _parse_iso_utc(str((runtime or {}).get("ts") or ""))
    heartbeat_age_s = int((now - runtime_ts).total_seconds()) if runtime_ts else None
    runtime_stale = bool(heartbeat_age_s is None or heartbeat_age_s > 30)
    runtime_source = str(_RUNTIME_FILE_CACHE.get("source") or "none")
    runtime_read_error = str(_RUNTIME_FILE_CACHE.get("error") or "")

    return {
        "runtime": runtime,
        "round_meta": round_meta,
        "runtime_stale": runtime_stale,
        "runtime_heartbeat_age_s": heartbeat_age_s,
        "runtime_source": runtime_source,
        "runtime_read_error": runtime_read_error,
        "current_round_key": current_round_key,
        "current_round_name": current_round_name,
        "current_round_record": current_round_record,
        "queued_eta": queued_eta.isoformat() if queued_eta else "",
        "threads_flat": flat_threads,
        "threads_active": active_threads,
        "threads_past": past_threads,
        "threads_queued": queued_threads,
        "recent_alerts": recent_alerts,
        "recent_log_alerts": log_alert_lines,
        "now_iso": now.isoformat(),
    }

# ----------------------------
# Bot control helpers
# ----------------------------
def _run_cmd(cmd: list[str], cwd: str | None = None, timeout_s: int = 180) -> tuple[int, str]:
    """Run a command safely (no shell=True). Return (rc, combined_output)."""
    try:
        p = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
            timeout=max(1, int(timeout_s)),
        )
        out = (p.stdout or "") + (p.stderr or "")
        return p.returncode, out.strip()
    except subprocess.TimeoutExpired as e:
        out = ((e.stdout or "") if isinstance(e.stdout, str) else "") + ((e.stderr or "") if isinstance(e.stderr, str) else "")
        return 124, f"Timeout after {timeout_s}s while running {cmd}\n{out}".strip()
    except Exception as e:
        return 99, f"Exception while running {cmd}: {e}"

def _sudo_systemctl(action: str, service_name: str = BOT_SYSTEMD_SERVICE) -> tuple[bool, str]:
    # Try direct systemctl first (dashboard often runs as root). Fallback to sudo -n.
    rc, out = _run_cmd(["systemctl", action, service_name], timeout_s=30)
    if rc == 0:
        return True, out
    rc2, out2 = _run_cmd(["sudo", "-n", "systemctl", action, service_name], timeout_s=30)
    merged = "\n".join(x for x in [out, out2] if x).strip()
    return (rc2 == 0), merged


def _service_is_active(service_name: str = BOT_SYSTEMD_SERVICE) -> tuple[bool, str]:
    rc, out = _run_cmd(["systemctl", "is-active", service_name], timeout_s=15)
    if rc != 0:
        rc2, out2 = _run_cmd(["sudo", "-n", "systemctl", "is-active", service_name], timeout_s=15)
        return (rc2 == 0 and (out2 or "").strip() == "active"), (out2 or "").strip()
    return ((out or "").strip() == "active"), (out or "").strip()

def _deploy_worker(target: str = "bot"):
    global _DEPLOY_IN_PROGRESS
    chunks = []
    ok_all = True
    target = (target or "bot").strip().lower()
    if target not in {"bot", "dashboard", "website", "both", "all"}:
        target = "bot"
    try:
        def checkpoint(step: str, ok: bool | None = None, detail: str = "") -> None:
            payload = {
                "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "action": f"deploy_{target}",
                "step": str(step),
                "ok": ok,
                "detail": str(detail or "")[-4000:],
            }
            _write_deploy_status(payload)

        checkpoint("started", ok=None, detail=f"target={target}")
        chunks.append(f"Repo dir: {BOT_REPO_DIR}")
        chunks.append(f"Bot service: {BOT_SYSTEMD_SERVICE}")
        chunks.append(f"Dashboard service: {DASHBOARD_SYSTEMD_SERVICE}")
        chunks.append(f"Website service: {WEBSITE_SYSTEMD_SERVICE}")
        chunks.append(f"Deploy worker started (target={target}).")

        # git pull (fast-forward only to avoid surprise merges)
        rc, out = _run_cmd(["git", "pull", "--ff-only"], cwd=BOT_REPO_DIR, timeout_s=120)
        chunks.append("---- git pull --ff-only ----")
        chunks.append(out or f"(exit {rc})")
        if rc != 0:
            ok_all = False
        checkpoint("git_pull", ok=(rc == 0), detail=out or f"exit={rc}")

        # pip install -r requirements.txt (if pip exists and requirements exists)
        req_path = os.path.join(BOT_REPO_DIR, "requirements.txt")
        if os.path.exists(req_path) and os.path.exists(BOT_VENV_PIP):
            rc, out = _run_cmd([BOT_VENV_PIP, "install", "-r", req_path], cwd=BOT_REPO_DIR, timeout_s=600)
            chunks.append("---- pip install -r requirements.txt ----")
            chunks.append(out or f"(exit {rc})")
            if rc != 0:
                ok_all = False
            checkpoint("pip_install", ok=(rc == 0), detail=out or f"exit={rc}")
        else:
            chunks.append("---- pip install skipped ----")
            chunks.append(f"requirements.txt exists={os.path.exists(req_path)}, venv pip exists={os.path.exists(BOT_VENV_PIP)}")
            checkpoint("pip_install_skipped", ok=True, detail="requirements or venv pip missing")

        # restart service(s) only if earlier steps succeeded
        chunks.append("---- systemctl restart ----")
        if ok_all:
            services: list[tuple[str, str]] = []
            if target == "bot":
                services.append(("bot", BOT_SYSTEMD_SERVICE))
            elif target == "dashboard":
                services.append(("dashboard", DASHBOARD_SYSTEMD_SERVICE))
            elif target == "website":
                services.append(("website", WEBSITE_SYSTEMD_SERVICE))
            elif target == "both":
                services.append(("bot", BOT_SYSTEMD_SERVICE))
                services.append(("dashboard", DASHBOARD_SYSTEMD_SERVICE))  # dashboard last so it doesn't kill the worker
            else:  # all
                services.append(("bot", BOT_SYSTEMD_SERVICE))
                services.append(("website", WEBSITE_SYSTEMD_SERVICE))
                services.append(("dashboard", DASHBOARD_SYSTEMD_SERVICE))

            for label, svc in services:
                ok, out = _sudo_systemctl("restart", svc)
                chunks.append(f"[{label}] {svc}")
                chunks.append(out or ("OK" if ok else "FAILED"))
                if not ok:
                    ok_all = False
                checkpoint(f"restart_{label}", ok=ok, detail=out or ("OK" if ok else "FAILED"))
        else:
            chunks.append("Skipped because deploy steps failed.")
            checkpoint("restart_skipped", ok=False, detail="earlier deploy step failed")

        _set_last_action(f"deploy_{target}", ok_all, "\n".join(chunks))
        checkpoint("finished", ok=ok_all, detail="completed")
    except Exception as e:
        chunks.append("---- deploy worker exception ----")
        chunks.append(f"{type(e).__name__}: {e}")
        _set_last_action(f"deploy_{target}", False, "\n".join(chunks))
        checkpoint("exception", ok=False, detail=f"{type(e).__name__}: {e}")
    finally:
        with _DEPLOY_LOCK:
            _DEPLOY_IN_PROGRESS = False

# ----------------------------
# Auth routes
# ----------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if not _ip_allowed():
        abort(403)

    err = ""
    if request.method == "POST":
        if not PASSWORD_LOGIN_ENABLED:
            err = "Password login is disabled. Use Discord login."
        elif _rate_limited():
            err = "Too many attempts. Try again later."
        else:
            username = (request.form.get("username") or "").strip()
            password = (request.form.get("password") or "").encode("utf-8")

            stored = DASH_USERS.get(username)
            valid_login = False
            if stored:
                try:
                    valid_login = bcrypt.checkpw(password, stored.encode("utf-8"))
                except Exception:
                    valid_login = False

            if valid_login:
                _clear_attempts()
                session.permanent = True
                session["dash_user"] = username
                return redirect(url_for("logs"))
            _record_attempt()
            err = "Invalid username or password."

    discord_login_url = url_for("login_discord") if DISCORD_OAUTH_ENABLED else None
    return render_template_string("""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>OF1 Dashboard — Login</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-[#0a0a0a] min-h-screen flex items-center justify-center px-4">
  <div class="w-full max-w-sm">

    <!-- Logo -->
    <div class="text-center mb-8">
      <div class="text-4xl font-extrabold text-white tracking-tight">OF1</div>
      <div class="text-sm text-gray-500 mt-1">Dashboard</div>
    </div>

    <!-- Card -->
    <div class="bg-[#111] border border-[#222] rounded-2xl p-7 shadow-2xl space-y-5">

      {% if err %}
        <div class="bg-red-950/50 border border-red-800/60 text-red-300 text-sm rounded-xl px-4 py-3">
          {{ err }}
        </div>
      {% endif %}

      {% if password_login_enabled %}
      <form method="post" class="space-y-3">
        {{ csrf_input|safe }}
        <div>
          <label class="block text-xs text-gray-500 mb-1.5 font-medium">Username</label>
          <input name="username" type="text" autocomplete="username" placeholder="your username"
            class="w-full bg-[#0a0a0a] border border-[#2a2a2a] text-gray-100 placeholder-gray-600
                   rounded-xl px-4 py-2.5 text-sm focus:outline-none focus:border-gray-500 transition-colors" />
        </div>
        <div>
          <label class="block text-xs text-gray-500 mb-1.5 font-medium">Password</label>
          <input name="password" type="password" autocomplete="current-password" placeholder="••••••••"
            class="w-full bg-[#0a0a0a] border border-[#2a2a2a] text-gray-100 placeholder-gray-600
                   rounded-xl px-4 py-2.5 text-sm focus:outline-none focus:border-gray-500 transition-colors" />
        </div>
        <button type="submit"
          class="w-full bg-white text-black font-semibold rounded-xl py-2.5 text-sm hover:bg-gray-100 transition-colors mt-1">
          Sign in
        </button>
      </form>
      {% elif not discord_login_url %}
        <div class="text-red-400 text-sm text-center">No login method is configured.</div>
      {% endif %}

      {% if discord_login_url %}
        {% if password_login_enabled %}
          <div class="flex items-center gap-3">
            <div class="flex-1 h-px bg-[#222]"></div>
            <span class="text-xs text-gray-600">or</span>
            <div class="flex-1 h-px bg-[#222]"></div>
          </div>
        {% endif %}
        <a href="{{ discord_login_url }}"
           class="flex items-center justify-center gap-2.5 w-full bg-[#5865F2] hover:bg-[#4752c4]
                  text-white font-semibold rounded-xl py-2.5 text-sm transition-colors">
          <svg class="w-4 h-4" viewBox="0 0 24 24" fill="currentColor">
            <path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 0 0 .031.057 19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028c.462-.63.874-1.295 1.226-1.994a.076.076 0 0 0-.041-.106 13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.892.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03z"/>
          </svg>
          Continue with Discord
        </a>
      {% endif %}

    </div>
  </div>
</body>
</html>
    """, err=err, csrf_input=_csrf_input(), discord_login_url=discord_login_url, password_login_enabled=PASSWORD_LOGIN_ENABLED)

@app.route("/login/discord")
def login_discord():
    if not _ip_allowed():
        abort(403)
    if not DISCORD_OAUTH_ENABLED:
        abort(404)
    state = secrets.token_urlsafe(24)
    session["_discord_oauth_state"] = state
    params = {
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": DISCORD_REDIRECT_URI,
        "response_type": "code",
        "scope": "identify",
        "state": state,
        "prompt": "consent",
    }
    return redirect(f"{DISCORD_API_BASE}/oauth2/authorize?{urlencode(params)}")

@app.route("/oauth/discord/callback")
def discord_oauth_callback():
    if not _ip_allowed():
        abort(403)
    if not DISCORD_OAUTH_ENABLED:
        abort(404)

    state = (request.args.get("state") or "").strip()
    expected = (session.get("_discord_oauth_state") or "").strip()
    session.pop("_discord_oauth_state", None)
    if not state or not expected or not hmac.compare_digest(state, expected):
        return render_template_string("<html><body style='background:#111;color:#eee;font-family:system-ui;padding:30px;'>Invalid OAuth state. <a style='color:#9cf;' href='{{ url_for(\"login\") }}'>Back to login</a></body></html>"), 400

    if request.args.get("error"):
        err = _escape(request.args.get("error_description") or request.args.get("error") or "OAuth denied")
        return render_template_string("<html><body style='background:#111;color:#eee;font-family:system-ui;padding:30px;'>Discord login failed: {{ err }}<br><a style='color:#9cf;' href='{{ url_for(\"login\") }}'>Back to login</a></body></html>", err=err), 400

    code = (request.args.get("code") or "").strip()
    if not code:
        return redirect(url_for("login"))

    ok, token_resp = _discord_exchange_code(code)
    if not ok:
        return render_template_string("<html><body style='background:#111;color:#eee;font-family:system-ui;padding:30px;'>{{ msg }}<br><a style='color:#9cf;' href='{{ url_for(\"login\") }}'>Back to login</a></body></html>", msg=_escape(str(token_resp))), 400

    access_token = str((token_resp or {}).get("access_token") or "")
    if not access_token:
        return render_template_string("<html><body style='background:#111;color:#eee;font-family:system-ui;padding:30px;'>Missing access token from Discord.<br><a style='color:#9cf;' href='{{ url_for(\"login\") }}'>Back to login</a></body></html>"), 400

    ok, user_resp = _discord_fetch_user(access_token)
    if not ok:
        return render_template_string("<html><body style='background:#111;color:#eee;font-family:system-ui;padding:30px;'>{{ msg }}<br><a style='color:#9cf;' href='{{ url_for(\"login\") }}'>Back to login</a></body></html>", msg=_escape(str(user_resp))), 400

    discord_user_id = str((user_resp or {}).get("id") or "").strip()
    username = str((user_resp or {}).get("username") or "discord-user").strip()
    global_name = str((user_resp or {}).get("global_name") or "").strip()
    display = global_name or username or "discord-user"

    if discord_user_id not in DISCORD_ALLOWED_USER_IDS:
        return render_template_string("<html><body style='background:#111;color:#eee;font-family:system-ui;padding:30px;'>Discord account not allowlisted for dashboard access.<br><a style='color:#9cf;' href='{{ url_for(\"login\") }}'>Back to login</a></body></html>"), 403

    session.permanent = True
    session["dash_user"] = f"{display} (Discord)"
    session["dash_auth_method"] = "discord"
    session["discord_user_id"] = discord_user_id
    return redirect(url_for("logs"))

@app.route("/logout", methods=["POST"])
def logout():
    session.pop("dash_user", None)
    session.pop("dash_auth_method", None)
    session.pop("discord_user_id", None)
    session.pop("_discord_oauth_state", None)
    session.pop("quiz_filter_q", None)
    session.pop("quiz_filter_cat", None)
    session.pop("quiz_filter_diff", None)
    return redirect(url_for("login"))

# ----------------------------
# App routes
# ----------------------------
@app.route("/")
@login_required
def index():
    return redirect(url_for("logs"))

@app.route("/logs")
@login_required
def logs():
    tail = request.args.get("tail", "100")
    try:
        tail_n = max(50, min(4000, int(tail)))
    except Exception:
        tail_n = 100

    show_filtered = (request.args.get("filtered", "1").strip() != "0")
    data = _build_logs_view_data(tail_n, show_filtered)

    if request.args.get("ajax") == "1":
        return jsonify(data)

    controls = f"""
      <form method="get" class="flex items-center gap-3 flex-wrap">
        <div class="flex items-center gap-2">
          <label class="text-xs text-gray-500">Lines</label>
          <input name="tail" value="{tail_n}"
            class="w-20 bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-1.5 focus:outline-none focus:border-gray-500" />
        </div>
        <div class="flex items-center gap-2">
          <label class="text-xs text-gray-500">Filter</label>
          <select name="filtered"
            class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-1.5 focus:outline-none focus:border-gray-500">
            <option value="1" {"selected" if show_filtered else ""}>On</option>
            <option value="0" {"selected" if not show_filtered else ""}>Off</option>
          </select>
        </div>
        <button type="submit"
          class="bg-[#1a1a1a] border border-[#2a2a2a] text-gray-300 text-sm rounded-lg px-4 py-1.5 hover:bg-[#222] transition-colors">
          Apply
        </button>
      </form>
    """

    body = (
        '<div class="space-y-4 max-w-5xl">'
        + '<div class="flex items-center justify-between flex-wrap gap-3">'
        + '<h1 class="text-xl font-bold text-white">Logs</h1>'
        + controls
        + '</div>'
        + f"<div id='lastActionBox'>{data['last_html']}</div>"
        + f"<div id='deployStatusBox'>{data.get('deploy_html','')}</div>"
        + """<div class="flex flex-wrap items-center gap-2">
          <input id="logSearch" type="text" placeholder="Filter log lines…"
            class="w-48 bg-[#0a0a0a] border border-[#2a2a2a] text-gray-300 text-xs rounded-lg px-3 py-1.5 focus:outline-none focus:border-gray-500" />
          <button id="logSearchClear" type="button" class="text-xs text-gray-600 hover:text-gray-300 hidden">&#x2715; Clear</button>
          <div class="flex items-center gap-1 ml-1">
            <button class="log-sev-btn active text-xs px-2 py-1 rounded border" data-sev="DEBUG"    style="background:#1a1a2a;color:#88a;border-color:#336">DEBUG</button>
            <button class="log-sev-btn active text-xs px-2 py-1 rounded border" data-sev="INFO"     style="background:#0a1a0a;color:#8c8;border-color:#363">INFO</button>
            <button class="log-sev-btn active text-xs px-2 py-1 rounded border" data-sev="WARNING"  style="background:#1a1400;color:#cc8;border-color:#553">WARNING</button>
            <button class="log-sev-btn active text-xs px-2 py-1 rounded border" data-sev="ERROR"    style="background:#1a0a0a;color:#f88;border-color:#633">ERROR</button>
            <button class="log-sev-btn active text-xs px-2 py-1 rounded border" data-sev="CRITICAL" style="background:#2a0a0a;color:#f44;border-color:#844">CRITICAL</button>
          </div>
          <span id="logMatchCount" class="text-xs text-gray-600"></span>
        </div>"""
        + f"<pre id='liveLogsPre' class='text-xs text-gray-400 whitespace-pre-wrap bg-[#0a0a0a] border border-[#222] rounded-xl p-4 overflow-x-auto max-h-[70vh] overflow-y-auto'>{data['safe_logs']}</pre>"
        + "</div>"
        + """
        <script>
          (function(){
            const pre = document.getElementById('liveLogsPre');
            const lastBox = document.getElementById('lastActionBox');
            const deployBox = document.getElementById('deployStatusBox');
            const searchInput = document.getElementById('logSearch');
            const matchCount = document.getElementById('logMatchCount');
            const clearBtn = document.getElementById('logSearchClear');
            if (!pre) return;

            // --- Log severity + keyword filter ---
            let rawLines = null;
            const sevBtns = document.querySelectorAll('.log-sev-btn');
            const hiddenSev = new Set();

            function sevLineLevel(line) {
              const u = line.toUpperCase();
              for (const sev of ['CRITICAL','ERROR','WARNING','INFO','DEBUG']) {
                if (u.includes(sev)) return sev;
              }
              return null;
            }

            function applyFilter() {
              if (!rawLines) rawLines = pre.textContent.split('\\n');
              const kw = (searchInput ? searchInput.value : '').toLowerCase();
              if (clearBtn) kw ? clearBtn.classList.remove('hidden') : clearBtn.classList.add('hidden');
              const filtered = rawLines.filter(l => {
                if (kw && !l.toLowerCase().includes(kw)) return false;
                const lev = sevLineLevel(l);
                if (lev && hiddenSev.has(lev)) return false;
                return true;
              });
              pre.textContent = filtered.join('\\n');
              if (matchCount) {
                const total = rawLines.filter(l => l.trim()).length;
                matchCount.textContent = (filtered.length < total) ? filtered.length + '/' + total + ' lines' : '';
              }
            }

            sevBtns.forEach(function(btn) {
              btn.addEventListener('click', function() {
                const sev = btn.dataset.sev;
                if (hiddenSev.has(sev)) {
                  hiddenSev.delete(sev);
                  btn.classList.add('active');
                  btn.style.opacity = '1';
                } else {
                  hiddenSev.add(sev);
                  btn.classList.remove('active');
                  btn.style.opacity = '0.35';
                }
                applyFilter();
              });
            });

            if (searchInput) {
              searchInput.addEventListener('input', applyFilter);
              searchInput.addEventListener('keydown', function(e){ if(e.key==='Escape'){searchInput.value='';applyFilter();} });
            }
            if (clearBtn) clearBtn.addEventListener('click', function(){ searchInput.value=''; applyFilter(); });

            // --- Live polling ---
            if (!lastBox || !deployBox) return;
            const url = new URL(window.location.href);
            url.searchParams.set('ajax', '1');
            let inFlight = false;
            async function tick(){
              if (inFlight || document.hidden) return;
              inFlight = true;
              try {
                const wasNearBottom = (pre.scrollHeight - pre.scrollTop - pre.clientHeight) < 32;
                const res = await fetch(url.toString(), { credentials: 'same-origin', cache: 'no-store' });
                if (!res.ok) return;
                const data = await res.json();
                if (typeof data.safe_logs === 'string') {
                  rawLines = null; // reset cached lines on new data
                  pre.innerHTML = data.safe_logs;
                  applyFilter();
                  if (wasNearBottom && !(searchInput && searchInput.value)) pre.scrollTop = pre.scrollHeight;
                }
                if (typeof data.last_html === 'string') lastBox.innerHTML = data.last_html;
                if (typeof data.deploy_html === 'string') deployBox.innerHTML = data.deploy_html;
              } catch (_err) {
              } finally {
                inFlight = false;
              }
            }
            setInterval(tick, 2500);
          })();
        </script>
        """
    )
    return _render(body)

@app.route("/status")
@login_required
def status():
    proc_uptime_s = max(0, int(time.time() - DASHBOARD_STARTED_AT))
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent
    data = _status_view_data()
    runtime = data.get("runtime") if isinstance(data.get("runtime"), dict) else {}
    loops = runtime.get("loops") if isinstance(runtime.get("loops"), dict) else {}
    loop_health = runtime.get("loop_health") if isinstance(runtime.get("loop_health"), dict) else {}
    hb = loop_health.get("heartbeats") if isinstance(loop_health.get("heartbeats"), dict) else {}
    errs = loop_health.get("errors") if isinstance(loop_health.get("errors"), dict) else {}
    standings = runtime.get("standings") if isinstance(runtime.get("standings"), dict) else {}
    openf1_window = runtime.get("openf1_window") if isinstance(runtime.get("openf1_window"), dict) else {}
    runtime_stale = bool(data.get("runtime_stale"))
    runtime_age = data.get("runtime_heartbeat_age_s")
    runtime_source = str(data.get("runtime_source") or "none")
    runtime_read_error = str(data.get("runtime_read_error") or "")

    def _dot_badge(ok: bool, label_ok: str = "Running", label_no: str = "Stopped") -> str:
        dot = "bg-green-500" if ok else "bg-red-500"
        txt = "text-green-400" if ok else "text-red-400"
        label = label_ok if ok else label_no
        return (
            f'<span class="inline-flex items-center gap-1.5">'
            f'<span class="inline-block w-1.5 h-1.5 rounded-full {dot}"></span>'
            f'<span class="text-xs {txt}">{label}</span>'
            f'</span>'
        )

    def _kv(label: str, value: str, mono: bool = False) -> str:
        val_cls = "font-mono text-xs" if mono else "text-sm"
        return (
            f'<div class="flex items-start justify-between gap-4 py-1.5 border-b border-[#1a1a1a] last:border-0">'
            f'<span class="text-gray-500 text-sm shrink-0">{label}</span>'
            f'<span class="text-gray-300 {val_cls} text-right truncate max-w-[240px]">{value}</span>'
            f'</div>'
        )

    current_round_record = data.get("current_round_record") if isinstance(data.get("current_round_record"), dict) else None
    current_round_key = str(data.get("current_round_key") or "")
    current_round_name = str(data.get("current_round_name") or "Next round")
    queued_eta = _fmt_ts_utc(str(data.get("queued_eta") or ""))
    has_current = current_round_record is not None
    current_state = str((current_round_record or {}).get("weekend_state") or "queued").lower()
    active_threads = data.get("threads_active") if isinstance(data.get("threads_active"), list) else []
    past_threads = data.get("threads_past") if isinstance(data.get("threads_past"), list) else []
    recent_alerts = data.get("recent_alerts") if isinstance(data.get("recent_alerts"), list) else []
    log_alerts = data.get("recent_log_alerts") if isinstance(data.get("recent_log_alerts"), list) else []

    # heartbeat indicator
    hb_fresh = not runtime_stale
    hb_dot = "bg-green-500 shadow-[0_0_6px_#22c55e]" if hb_fresh else "bg-red-500"
    hb_label = "Live" if hb_fresh else "Stale"
    hb_txt = "text-green-400" if hb_fresh else "text-red-400"
    hb_age = str(runtime_age) + "s" if runtime_age is not None else "—"

    # unix timestamps for client-side live tickers
    _rt_dt = _parse_iso_utc(str(runtime.get("ts") or ""))
    runtime_ts_unix = int(_rt_dt.timestamp()) if _rt_dt else 0
    dashboard_start_unix = int(DASHBOARD_STARTED_AT)
    _bot_start_dt = _parse_iso_utc(str(runtime.get("bot_started_at") or ""))
    bot_start_unix = int(_bot_start_dt.timestamp()) if _bot_start_dt else 0

    # race thread card
    if has_current:
        state_color = "text-green-400" if current_state == "active" else "text-yellow-400"
        thread_card_rows = (
            _kv("Round", f"{_escape(current_round_name)} <span class='text-gray-600'>({_escape(current_round_key or '-')})</span>")
            + _kv("Status", f'<span class="{state_color}">{_escape(current_state.title())}</span>')
            + _kv("Thread", _escape(str(current_round_record.get("thread_name") or current_round_record.get("thread_id") or "-")))
            + _kv("Created", _escape(_fmt_ts_utc(current_round_record.get("created_at"))) + f' <span class="text-gray-600">({_escape(_fmt_relative(current_round_record.get("created_at")))})</span>')
            + _kv("Source", _escape(str(current_round_record.get("source") or "-")))
        )
        thread_card_title = "Current Round Thread"
    else:
        thread_card_rows = (
            _kv("Round", f"{_escape(current_round_name)} <span class='text-gray-600'>({_escape(current_round_key or '-')})</span>")
            + _kv("Status", '<span class="text-yellow-400">Queued</span>')
            + _kv("Auto-create window", _escape(queued_eta))
        )
        thread_card_title = "Next Round"

    # active / past thread summary
    if active_threads:
        t = active_threads[0]
        active_line = (
            f"#{_escape(str(t.get('thread_name') or t.get('thread_id') or 'thread'))} &mdash; "
            f"round {_escape(str(t.get('round_key') or '-'))}, "
            f"created {_escape(_fmt_ts_utc(t.get('created_at')))}"
        )
    else:
        active_line = "<span class='text-gray-600'>None</span>"

    if past_threads:
        t = past_threads[0]
        prior_line = (
            f"#{_escape(str(t.get('thread_name') or t.get('thread_id') or 'thread'))} &mdash; "
            f"round {_escape(str(t.get('round_key') or '-'))}, "
            f"past since {_escape(_fmt_ts_utc(t.get('past_at') or t.get('created_at')))}"
        )
    else:
        prior_line = "<span class='text-gray-600'>None</span>"

    # alerts
    if recent_alerts:
        alert_rows = ""
        for a in reversed(recent_alerts[-10:]):
            ts = _fmt_ts_utc(str(a.get("ts") or ""))
            kind = _escape(str(a.get("kind") or "alert"))
            msg = _escape(str(a.get("message") or ""))
            alert_rows += (
                f'<div class="py-2 border-b border-[#1a1a1a] last:border-0">'
                f'<div class="flex items-center gap-2">'
                f'<span class="text-xs font-medium text-yellow-400">{kind}</span>'
                f'<span class="text-xs text-gray-600">{_escape(ts)}</span>'
                f'</div>'
                f'<div class="text-sm text-gray-300 mt-0.5">{msg}</div>'
                f'</div>'
            )
        alert_items_html = alert_rows
    else:
        alert_items_html = "<div class='text-sm text-gray-600 py-2'>No recorded state alerts yet.</div>"

    if log_alerts:
        log_alerts_html = (
            "<pre class='text-xs text-gray-400 whitespace-pre-wrap bg-[#0a0a0a] rounded-lg p-3 mt-1 overflow-x-auto'>"
            + _escape("\n".join(log_alerts[-10:]))
            + "</pre>"
        )
    else:
        log_alerts_html = "<div class='text-sm text-gray-600 py-2'>No recent error-like log lines.</div>"

    # loop health modules — (display_name, running_key, heartbeat_key)
    modules = [
        ("Race Supervisor",  "race_supervisor",         "RACE_SUPERVISOR_TASK"),
        ("F1 Reminders",     "f1_reminders",            "F1_REMINDER_TASK"),
        ("Standings",        "standings",               "STANDINGS_TASK"),
        ("XP Flush",         "xp_flush",                "XP_FLUSH_TASK"),
        ("Role Recovery",    "periodic_role_recovery",  "PERIODIC_ROLE_RECOVERY_TASK"),
        ("Runtime Status",   "runtime_status",          "RUNTIME_STATUS_TASK"),
        ("Driver Cache",     "driver_cache_validation", "DRIVER_CACHE_VALIDATION_TASK"),
    ]
    now_ts = time.time()
    module_grid = ""
    for name, loop_key, hb_key in modules:
        ok = bool(loops.get(loop_key))
        hb_iso = hb.get(hb_key) or hb.get(loop_key) or ""
        hb_age_str = ""
        if hb_iso:
            try:
                hb_dt = datetime.fromisoformat(hb_iso.replace("Z", "+00:00"))
                age_s = int(now_ts - hb_dt.timestamp())
                if age_s < 120:
                    hb_age_str = f"{age_s}s ago"
                elif age_s < 3600:
                    hb_age_str = f"{age_s // 60}m ago"
                else:
                    hb_age_str = f"{age_s // 3600}h ago"
            except Exception:
                hb_age_str = ""
        bg = "bg-[#0f1f0f]" if ok else "bg-[#1f0f0f]"
        border = "border-green-900/40" if ok else "border-red-900/40"
        hb_line = (f'<span class="text-xs text-gray-600 ml-auto mr-2">{_escape(hb_age_str)}</span>' if hb_age_str else '')
        module_grid += (
            f'<div class="{bg} border {border} rounded-xl px-4 py-3 flex items-center gap-2">'
            f'<span class="text-sm text-gray-300">{name}</span>'
            f'{hb_line}'
            f'{_dot_badge(ok)}'
            f'</div>'
        )

    race_live = runtime.get("race_live") or {}

    body = f"""
<div class="space-y-6 max-w-5xl">

  <!-- Page header + heartbeat -->
  <div class="flex items-center justify-between flex-wrap gap-3">
    <h1 class="text-xl font-bold text-white">Status</h1>
    <div class="flex items-center gap-2.5 px-3 py-1.5 rounded-lg bg-[#111] border border-[#222] text-sm">
      <span class="inline-block w-2 h-2 rounded-full {hb_dot}"></span>
      <span class="{hb_txt} font-medium">{hb_label}</span>
      <span class="text-gray-700">·</span>
      <span class="text-gray-500"><span id="stat-hb-age" data-ts="{runtime_ts_unix}">{hb_age}</span> &middot; {_escape(runtime_source)}</span>
    </div>
  </div>

  {"<div class='bg-yellow-950/40 border border-yellow-800/40 text-yellow-300 text-sm rounded-xl px-4 py-3'><b>Runtime read warning:</b> " + _escape(runtime_read_error) + "</div>" if runtime_read_error else ""}

  <!-- Metric cards -->
  <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
    <div class="bg-[#111] border border-[#222] rounded-xl p-4">
      <div class="text-xs text-gray-500 mb-1 uppercase tracking-wider">CPU</div>
      <div class="text-3xl font-bold text-white">{cpu}<span class="text-lg text-gray-500">%</span></div>
    </div>
    <div class="bg-[#111] border border-[#222] rounded-xl p-4">
      <div class="text-xs text-gray-500 mb-1 uppercase tracking-wider">RAM</div>
      <div class="text-3xl font-bold text-white">{ram}<span class="text-lg text-gray-500">%</span></div>
    </div>
    <div class="bg-[#111] border border-[#222] rounded-xl p-4">
      <div class="text-xs text-gray-500 mb-1 uppercase tracking-wider">Bot Uptime</div>
      <div class="text-2xl font-bold text-white" id="stat-bot-uptime" data-start="{bot_start_unix}">—</div>
    </div>
    <div class="bg-[#111] border border-[#222] rounded-xl p-4">
      <div class="text-xs text-gray-500 mb-1 uppercase tracking-wider">Dashboard Uptime</div>
      <div class="text-2xl font-bold text-white" id="stat-uptime" data-start="{dashboard_start_unix}">—</div>
    </div>
  </div>

  <!-- Host + Service info -->
  <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
    <div class="bg-[#111] border border-[#222] rounded-xl p-4">
      <div class="text-xs text-gray-500 font-semibold uppercase tracking-widest mb-3">Host</div>
      {_kv("Guilds", _escape(str(runtime.get("guild_count", "—"))))}
      {_kv("Snapshot", _escape(_fmt_ts_utc(str(runtime.get("ts") or ""))))}
      {_kv("Log path", _escape(LOG_PATH), mono=True)}
    </div>
    <div class="bg-[#111] border border-[#222] rounded-xl p-4">
      <div class="text-xs text-gray-500 font-semibold uppercase tracking-widest mb-3">Services</div>
      {_kv("Bot", _escape(BOT_SYSTEMD_SERVICE), mono=True)}
      {_kv("Dashboard", _escape(DASHBOARD_SYSTEMD_SERVICE), mono=True)}
      {_kv("Repo", _escape(BOT_REPO_DIR), mono=True)}
    </div>
  </div>

  <!-- Race Thread Lifecycle -->
  <div>
    <div class="text-xs text-gray-500 font-semibold uppercase tracking-widest mb-3">Race Thread Lifecycle</div>
    <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
      <div class="bg-[#111] border border-[#222] rounded-xl p-4">
        <div class="text-sm font-semibold text-gray-300 mb-3">{thread_card_title}</div>
        {thread_card_rows}
      </div>
      <div class="bg-[#111] border border-[#222] rounded-xl p-4 space-y-3">
        <div>
          <div class="text-xs text-gray-500 mb-1">Active thread</div>
          <div class="text-sm text-gray-300">{active_line}</div>
        </div>
        <div class="border-t border-[#1a1a1a] pt-3">
          <div class="text-xs text-gray-500 mb-1">Prior thread</div>
          <div class="text-sm text-gray-300">{prior_line}</div>
        </div>
        <div class="border-t border-[#1a1a1a] pt-3">
          <div class="text-xs text-gray-500 mb-1">Standings</div>
          <div class="text-sm text-gray-300">
            Refresh every {_escape(str(standings.get("refresh_minutes", 5)))}m &middot;
            Channel {_escape(str(standings.get("channel_id", "—")))}
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- Module Health -->
  <div>
    <div class="text-xs text-gray-500 font-semibold uppercase tracking-widest mb-3">Module Health</div>
    <div class="grid grid-cols-2 sm:grid-cols-3 gap-2">
      {module_grid}
    </div>
  </div>

  <!-- Live Race Details -->
  <div class="bg-[#111] border border-[#222] rounded-xl p-4">
    <div class="text-xs text-gray-500 font-semibold uppercase tracking-widest mb-3">Live Race Config</div>
    <div class="grid grid-cols-1 sm:grid-cols-2 gap-x-8">
      {_kv("Spoiler delay", _escape(str(race_live.get("delay_seconds", 0))) + "s")}
      {_kv("Poll interval", _escape(str(race_live.get("poll_seconds", 3))) + "s")}
      {_kv("Pre-weekend buffer", _escape(str(openf1_window.get("pre_buffer_hours", 24))) + "h")}
      {_kv("Post-weekend buffer", _escape(str(openf1_window.get("post_buffer_hours", 12))) + "h")}
    </div>
  </div>

  <!-- Alerts -->
  <div>
    <div class="text-xs text-gray-500 font-semibold uppercase tracking-widest mb-3">Alerts</div>
    <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
      <div class="bg-[#111] border border-[#222] rounded-xl p-4">
        <div class="text-sm font-semibold text-gray-300 mb-2">State Alerts</div>
        {alert_items_html}
      </div>
      <div class="bg-[#111] border border-[#222] rounded-xl p-4">
        <div class="text-sm font-semibold text-gray-300 mb-2">Recent Errors in Logs</div>
        {log_alerts_html}
      </div>
    </div>
  </div>

</div>
    """
    if request.args.get("ajax") == "1":
        return jsonify({"status_html": body})

    page = (
        "<div id='statusRoot'>"
        + body
        + "</div>"
        + """
        <script>
          (function(){
            const root = document.getElementById('statusRoot');
            if (!root) return;
            const url = new URL(window.location.href);
            url.searchParams.set('ajax', '1');
            let inFlight = false;
            async function tick(){
              if (inFlight || document.hidden) return;
              inFlight = true;
              try {
                const res = await fetch(url.toString(), { credentials: 'same-origin', cache: 'no-store' });
                if (!res.ok) return;
                const data = await res.json();
                if (data && typeof data.status_html === 'string') {
                  root.innerHTML = data.status_html;
                }
              } catch (_err) {
                // ignore transient polling errors
              } finally {
                inFlight = false;
              }
            }
            setInterval(tick, 15000);
          })();
        </script>
        <script>
          (function(){
            function fmtDuration(secs) {
              secs = Math.max(0, Math.floor(secs));
              if (secs < 60)   return secs + 's';
              if (secs < 3600) return Math.floor(secs / 60) + 'm ' + (secs % 60) + 's';
              return Math.floor(secs / 3600) + 'h ' + Math.floor((secs % 3600) / 60) + 'm';
            }
            function tickLive() {
              var now = Date.now() / 1000;
              var upEl = document.getElementById('stat-uptime');
              if (upEl && upEl.dataset.start) {
                upEl.textContent = fmtDuration(now - parseFloat(upEl.dataset.start));
              }
              var botUpEl = document.getElementById('stat-bot-uptime');
              if (botUpEl && botUpEl.dataset.start && parseFloat(botUpEl.dataset.start) > 0) {
                botUpEl.textContent = fmtDuration(now - parseFloat(botUpEl.dataset.start));
              } else if (botUpEl) {
                botUpEl.textContent = '—';
              }
              var hbEl = document.getElementById('stat-hb-age');
              if (hbEl && hbEl.dataset.ts && parseFloat(hbEl.dataset.ts) > 0) {
                hbEl.textContent = fmtDuration(now - parseFloat(hbEl.dataset.ts));
              }
            }
            tickLive();
            setInterval(tickLive, 1000);
          })();
        </script>
        """
    )
    return _render(page)

# ----------------------------
# Bot action routes
# ----------------------------
@app.route("/bot_action/<action>", methods=["POST"])
@login_required
def bot_action(action: str):
    global _DEPLOY_IN_PROGRESS
    action = (action or "").strip().lower()
    allowed = {"start", "stop", "restart", "deploy", "deploybot", "deploydashboard", "deploywebsite", "deployboth", "deployall"}
    if action not in allowed:
        abort(404)

    if action in {"start", "stop", "restart"}:
        ok, out = _sudo_systemctl(action)
        expected_active = action in {"start", "restart"}
        is_active, state_txt = _service_is_active(BOT_SYSTEMD_SERVICE)
        state_ok = (is_active == expected_active)
        final_ok = bool(ok and state_ok)
        combined = (out or "").strip()
        if state_txt:
            combined = (combined + "\n" if combined else "") + f"is-active: {state_txt}"
        if not state_ok:
            combined = (combined + "\n" if combined else "") + f"Expected active={expected_active}, got active={is_active}"
        _set_last_action(action, final_ok, combined or ("OK" if final_ok else "FAILED"))
        _audit_log(f"bot_{action}", f"ok={final_ok}")
        return redirect(url_for("logs"))

    # deploy actions: run in background so the request returns quickly
    target = {
        "deploy": "bot",  # backwards-compat
        "deploybot": "bot",
        "deploydashboard": "dashboard",
        "deploywebsite": "website",
        "deployboth": "both",
        "deployall": "all",
    }.get(action, "bot")
    with _DEPLOY_LOCK:
        if _DEPLOY_IN_PROGRESS:
            _set_last_action(f"deploy_{target}", False, "Deploy already running. Wait for it to finish, then refresh Logs.")
            return redirect(url_for("logs"))
        _DEPLOY_IN_PROGRESS = True
    t = threading.Thread(target=_deploy_worker, kwargs={"target": target}, daemon=True, name=f"dashboard-deploy-{target}-worker")
    t.start()
    _set_last_action(f"deploy_{target}", True, f"Deploy started in background (thread={t.name}). Refresh Logs in a few seconds.")
    _audit_log(f"deploy_{target}")
    return redirect(url_for("logs"))

# Backwards-compat route name (your old template called /restart)
# ----------------------------
# Watch Party editor
# ----------------------------
_WATCH_PARTY_LOCK = threading.Lock()

_WP_DEFAULT: dict = {
    "active": True,
    "override": False,
    "title": "",
    "date": "",
    "time": "",
    "location": "",
    "details": "Join us live as we watch the race together! React in real time, make predictions, and enjoy the chaos.",
    "_venues": [
        {"name": "", "address": ""},
        {"name": "", "address": ""},
    ],
    "_active_venues": [],
}

def _load_wp() -> dict:
    try:
        with open(WATCH_PARTY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return dict(_WP_DEFAULT)

def _save_wp(data: dict) -> None:
    tmp = WATCH_PARTY_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, WATCH_PARTY_PATH)

# Auto-filled fields that have a Reset button (clears to "" to let auto-detect take over)
_AUTO_FIELDS = {"title", "date", "time", "datetime_utc"}
# Fields that are always manual (no reset)
_MANUAL_FIELDS = {"details", "location"}


@app.route("/watch_party")
@login_required
def watch_party_editor():
    wp = _load_wp()
    venues = wp.get("_venues") or [{"name": "", "address": ""}, {"name": "", "address": ""}]
    # Ensure always exactly 2 venue slots
    while len(venues) < 2:
        venues.append({"name": "", "address": ""})
    active_venues = set(wp.get("_active_venues") or [])
    override = bool(wp.get("override"))
    wp_active = bool(wp.get("active", True))

    # Build datetime-local input value from stored UTC string (YYYY-MM-DDTHH:MM)
    raw_dt_utc = str(wp.get("datetime_utc") or "")
    wp_dt_utc_input = raw_dt_utc[:16].replace(" ", "T") if raw_dt_utc and "T" in raw_dt_utc else ""

    def _val(k):
        return _escape(str(wp.get(k) or ""))

    def _field_row(label, key, auto=True):
        reset_btn = ""
        if auto:
            reset_btn = f"""
              <button type="button" onclick="wpReset('{key}')"
                style="background:#222;color:#aaa;border:1px solid #444;padding:5px 10px;border-radius:8px;cursor:pointer;white-space:nowrap;">
                Reset
              </button>"""
        return f"""
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px;">
            <label style="width:70px;color:#aaa;font-size:13px;">{label}</label>
            <input id="wp_{key}" name="{key}" value="{_val(key)}"
              style="flex:1;min-width:200px;background:#000;color:#eee;border:1px solid #333;padding:7px 10px;border-radius:8px;font-size:14px;" />
            <button type="button" onclick="wpSave('{key}')"
              style="background:#1f6f3f;color:#fff;border:1px solid #2a8f52;padding:5px 10px;border-radius:8px;cursor:pointer;white-space:nowrap;">
              Save
            </button>
            {reset_btn}
          </div>"""

    venue_rows = ""
    for i, v in enumerate(venues[:2]):
        checked = "checked" if v.get("name") in active_venues else ""
        vname = _escape(str(v.get("name") or ""))
        vaddr = _escape(str(v.get("address") or ""))
        venue_rows += f"""
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px;">
            <input type="checkbox" id="venue_check_{i}" name="venue_active_{i}" {checked}
              style="width:16px;height:16px;accent-color:#2a8f52;cursor:pointer;" />
            <input id="venue_name_{i}" placeholder="Venue name"
              value="{vname}"
              style="width:160px;background:#000;color:#eee;border:1px solid #333;padding:7px 10px;border-radius:8px;font-size:14px;" />
            <input id="venue_addr_{i}" placeholder="Full address"
              value="{vaddr}"
              style="flex:1;min-width:200px;background:#000;color:#eee;border:1px solid #333;padding:7px 10px;border-radius:8px;font-size:14px;" />
          </div>"""

    override_color = "#8f2a2a" if override else "#1f6f3f"
    override_border = "#c44" if override else "#2a8f52"
    override_label = "ON — using manual values" if override else "OFF — auto-detecting from schedule"

    active_btn_bg = "#1f6f3f" if wp_active else "#8f2a2a"
    active_btn_border = "#2a8f52" if wp_active else "#c44"
    active_btn_label = "● ACTIVE — Card showing on website" if wp_active else "○ INACTIVE — Card hidden from website"

    page = f"""
      <h2 style="margin:0 0 16px 0;">Watch Party</h2>
      <meta name="wp-csrf" content="{_csrf_token()}" />

      <!-- Active toggle (prominent) -->
      <div style="margin-bottom:20px;padding:16px;background:#111;border:2px solid {'#2a8f52' if wp_active else '#8f2a2a'};border-radius:12px;">
        <div style="color:#aaa;font-size:12px;text-transform:uppercase;letter-spacing:.05em;margin-bottom:10px;">Watch Party Visibility</div>
        <div style="display:flex;align-items:center;gap:12px;">
          <button id="activeBtn" type="button" onclick="wpToggleActive()"
            style="background:{active_btn_bg};color:#fff;border:1px solid {active_btn_border};padding:10px 20px;border-radius:10px;cursor:pointer;font-weight:700;font-size:15px;">
            {active_btn_label}
          </button>
          <span style="color:#666;font-size:13px;">
            Toggle whether the watch party card appears on the public website.
          </span>
        </div>
      </div>

      <!-- Override toggle -->
      <div style="margin-bottom:20px;padding:14px;background:#1a1a1a;border:1px solid #333;border-radius:12px;">
        <div style="color:#aaa;font-size:12px;text-transform:uppercase;letter-spacing:.05em;margin-bottom:10px;">Override</div>
        <div style="display:flex;align-items:center;gap:12px;">
          <button id="overrideBtn" type="button" onclick="wpToggleOverride()"
            style="background:{override_color};color:#fff;border:1px solid {override_border};padding:8px 16px;border-radius:10px;cursor:pointer;font-weight:600;">
            {override_label}
          </button>
          <span style="color:#666;font-size:13px;">
            When OFF, title/date/time are filled from the F1 schedule. When ON, all fields below are used as-is.
          </span>
        </div>
      </div>

      <!-- Auto-filled fields -->
      <div style="margin-bottom:20px;padding:14px;background:#1a1a1a;border:1px solid #333;border-radius:12px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;flex-wrap:wrap;gap:8px;">
          <div style="color:#aaa;font-size:12px;text-transform:uppercase;letter-spacing:.05em;">
            Auto-filled fields
            <span style="color:#555;font-weight:normal;text-transform:none;"> — leave blank to auto-detect, or fill in to override just this field</span>
          </div>
          <button type="button" onclick="wpAutofillF1()"
            style="background:#1a3a6f;color:#7bb3ff;border:1px solid #2a5aaf;padding:5px 12px;border-radius:8px;cursor:pointer;font-size:12px;white-space:nowrap;">
            📅 Auto-fill from F1 Calendar
          </button>
        </div>
        {_field_row("Title", "title", auto=True)}
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px;">
          <label style="width:70px;color:#aaa;font-size:13px;">Date &amp; Time</label>
          <input id="wp_datetime_utc" type="datetime-local" value="{wp_dt_utc_input}"
            style="flex:1;min-width:200px;background:#000;color:#eee;border:1px solid #333;padding:7px 10px;border-radius:8px;font-size:14px;" />
          <span style="color:#555;font-size:11px;white-space:nowrap;">UTC</span>
          <button type="button" onclick="wpSaveDatetime()"
            style="background:#1f6f3f;color:#fff;border:1px solid #2a8f52;padding:5px 10px;border-radius:8px;cursor:pointer;white-space:nowrap;">
            Save
          </button>
          <button type="button" onclick="wpResetDatetime()"
            style="background:#222;color:#aaa;border:1px solid #444;padding:5px 10px;border-radius:8px;cursor:pointer;white-space:nowrap;">
            Reset
          </button>
        </div>
      </div>

      <!-- Manual fields -->
      <div style="margin-bottom:20px;padding:14px;background:#1a1a1a;border:1px solid #333;border-radius:12px;">
        <div style="color:#aaa;font-size:12px;text-transform:uppercase;letter-spacing:.05em;margin-bottom:12px;">Always manual</div>
        {_field_row("Details", "details", auto=False)}
      </div>

      <!-- Venues -->
      <div style="margin-bottom:20px;padding:14px;background:#1a1a1a;border:1px solid #333;border-radius:12px;">
        <div style="color:#aaa;font-size:12px;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px;">Venues</div>
        <div style="color:#555;font-size:12px;margin-bottom:12px;">
          Check the venues that are active for this watch party, then click Save Location.
          The address(es) of the checked venue(s) will be shown on the website.
        </div>
        {venue_rows}
        <div style="margin-top:10px;display:flex;gap:8px;">
          <button type="button" onclick="wpSaveVenues()"
            style="background:#1f6f3f;color:#fff;border:1px solid #2a8f52;padding:7px 14px;border-radius:8px;cursor:pointer;font-weight:600;">
            Save Venues &amp; Location
          </button>
        </div>
      </div>

      <!-- Discord channel (fixed) -->
      <div style="padding:14px;background:#1a1a1a;border:1px solid #333;border-radius:12px;">
        <div style="color:#aaa;font-size:12px;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px;">Discord Channel</div>
        <span style="color:#eee;font-size:14px;">#race-thread</span>
        <span style="color:#555;font-size:12px;margin-left:8px;">(fixed — always shown on the website)</span>
      </div>

      <!-- Schedule flyer uploads -->
      <div style="margin-top:16px;padding:14px;background:#1a1a1a;border:1px solid #333;border-radius:12px;">
        <div style="color:#aaa;font-size:12px;text-transform:uppercase;letter-spacing:.05em;margin-bottom:12px;">Schedule Flyers</div>
        <div style="display:flex;flex-direction:column;gap:16px;">
          <div>
            <div style="color:#eee;font-size:13px;font-weight:600;margin-bottom:6px;">Half Barrel Beer Project</div>
            <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
              <label style="background:#1f6f3f;color:#fff;border:1px solid #2a8f52;padding:6px 12px;border-radius:8px;cursor:pointer;font-size:13px;">
                Choose PNG
                <input type="file" accept="image/png" style="display:none;"
                  onchange="wpUploadFlyer(this, 'halfbarrel')" />
              </label>
              <span id="flyer-status-halfbarrel" style="font-size:12px;color:#555;">No file chosen</span>
            </div>
          </div>
          <div>
            <div style="color:#eee;font-size:13px;font-weight:600;margin-bottom:6px;">Hourglass Brewing Longwood</div>
            <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
              <label style="background:#1f6f3f;color:#fff;border:1px solid #2a8f52;padding:6px 12px;border-radius:8px;cursor:pointer;font-size:13px;">
                Choose PNG
                <input type="file" accept="image/png" style="display:none;"
                  onchange="wpUploadFlyer(this, 'hourglass')" />
              </label>
              <span id="flyer-status-hourglass" style="font-size:12px;color:#555;">No file chosen</span>
            </div>
          </div>
        </div>
      </div>

      <!-- Status toast -->
      <div id="wp-toast" style="display:none;margin-top:14px;padding:10px 14px;border-radius:10px;font-size:14px;"></div>

      <script>
      (function(){{
        function csrf() {{
          return document.querySelector('meta[name="wp-csrf"]').content;
        }}

        function toast(msg, ok) {{
          const el = document.getElementById('wp-toast');
          el.textContent = msg;
          el.style.display = 'block';
          el.style.background = ok ? '#0d3320' : '#3a0d0d';
          el.style.border = '1px solid ' + (ok ? '#2a8f52' : '#8f2a2a');
          el.style.color = ok ? '#6f6' : '#f88';
          clearTimeout(el._t);
          el._t = setTimeout(() => {{ el.style.display = 'none'; }}, 3500);
        }}

        async function post(url, body) {{
          const fd = new FormData();
          fd.append('_csrf', csrf());
          for (const [k, v] of Object.entries(body)) fd.append(k, v);
          const r = await fetch(url, {{ method: 'POST', body: fd, credentials: 'same-origin' }});
          return r.json();
        }}

        window.wpSave = async function(field) {{
          const val = document.getElementById('wp_' + field).value;
          try {{
            const d = await post('/watch_party/field', {{ field, value: val }});
            toast(d.ok ? field + ' saved.' : ('Error: ' + d.error), d.ok);
          }} catch(e) {{ toast('Request failed.', false); }}
        }};

        window.wpSaveDatetime = async function() {{
          const val = document.getElementById('wp_datetime_utc').value;
          try {{
            const d = await post('/watch_party/field', {{ field: 'datetime_utc', value: val }});
            toast(d.ok ? 'Date & time saved (UTC).' : ('Error: ' + d.error), d.ok);
          }} catch(e) {{ toast('Request failed.', false); }}
        }};

        window.wpResetDatetime = async function() {{
          document.getElementById('wp_datetime_utc').value = '';
          try {{
            const d = await post('/watch_party/field', {{ field: 'datetime_utc', value: '' }});
            toast(d.ok ? 'Date & time reset to auto-detect.' : ('Error: ' + d.error), d.ok);
          }} catch(e) {{ toast('Request failed.', false); }}
        }};

        window.wpReset = async function(field) {{
          document.getElementById('wp_' + field).value = '';
          try {{
            const d = await post('/watch_party/field', {{ field, value: '' }});
            toast(d.ok ? field + ' reset to auto.' : ('Error: ' + d.error), d.ok);
          }} catch(e) {{ toast('Request failed.', false); }}
        }};

        window.wpToggleActive = async function() {{
          try {{
            const d = await post('/watch_party/toggle_active', {{}});
            if (d.ok) window.location.reload();
            else toast('Error: ' + d.error, false);
          }} catch(e) {{ toast('Request failed.', false); }}
        }};

        window.wpAutofillF1 = async function() {{
          try {{
            const r = await fetch('/watch_party/autofill_f1', {{credentials: 'same-origin'}});
            const d = await r.json();
            if (!d.ok) {{ toast('Error: ' + d.error, false); return; }}
            ['title'].forEach(k => {{
              const el = document.getElementById('wp_' + k);
              if (el && d[k]) el.value = d[k];
            }});
            if (d.datetime_utc) {{
              const el = document.getElementById('wp_datetime_utc');
              if (el) el.value = d.datetime_utc;
            }}
            toast('Fields filled from F1 calendar — click Save on each field to apply.', true);
          }} catch(e) {{ toast('Request failed.', false); }}
        }};

        window.wpToggleOverride = async function() {{
          try {{
            const d = await post('/watch_party/toggle_override', {{}});
            if (d.ok) window.location.reload();
            else toast('Error: ' + d.error, false);
          }} catch(e) {{ toast('Request failed.', false); }}
        }};

        window.wpSaveVenues = async function() {{
          const venues = [];
          const active = [];
          for (let i = 0; i < 2; i++) {{
            const name = document.getElementById('venue_name_' + i).value.trim();
            const addr = document.getElementById('venue_addr_' + i).value.trim();
            venues.push(JSON.stringify({{ name, address: addr }}));
            if (document.getElementById('venue_check_' + i).checked && name) {{
              active.push(name);
            }}
          }}
          try {{
            const fd = new FormData();
            fd.append('_csrf', csrf());
            venues.forEach(v => fd.append('venues[]', v));
            active.forEach(a => fd.append('active[]', a));
            const r = await fetch('/watch_party/venues', {{ method: 'POST', body: fd, credentials: 'same-origin' }});
            const d = await r.json();
            toast(d.ok ? 'Venues & location saved.' : ('Error: ' + d.error), d.ok);
          }} catch(e) {{ toast('Request failed.', false); }}
        }};

        window.wpUploadFlyer = async function(input, key) {{
          const statusEl = document.getElementById('flyer-status-' + key);
          const file = input.files[0];
          if (!file) return;
          if (!file.name.toLowerCase().endsWith('.png')) {{
            statusEl.style.color = '#f88';
            statusEl.textContent = 'Must be a PNG file.';
            return;
          }}
          if (file.size > 10 * 1024 * 1024) {{
            statusEl.style.color = '#f88';
            statusEl.textContent = 'File too large (max 10 MB).';
            return;
          }}
          statusEl.style.color = '#aaa';
          statusEl.textContent = 'Uploading...';
          const fd = new FormData();
          fd.append('_csrf', csrf());
          fd.append('file', file);
          fd.append('key', key);
          try {{
            const r = await fetch('/watch_party/upload_flyer', {{ method: 'POST', body: fd, credentials: 'same-origin' }});
            const d = await r.json();
            if (d.ok) {{
              statusEl.style.color = '#6f6';
              statusEl.textContent = 'Uploaded successfully.';
              toast('Flyer updated. Reload the website to see it.', true);
            }} else {{
              statusEl.style.color = '#f88';
              statusEl.textContent = 'Error: ' + d.error;
            }}
          }} catch(e) {{
            statusEl.style.color = '#f88';
            statusEl.textContent = 'Upload failed.';
          }}
          input.value = '';
        }};
      }})();
      </script>
    """
    return _render(page)


_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
_ALLOWED_FLYER_KEYS = {"halfbarrel", "hourglass"}

@app.route("/watch_party/upload_flyer", methods=["POST"])
@login_required
def wp_upload_flyer():
    key = (request.form.get("key") or "").strip()
    if key not in _ALLOWED_FLYER_KEYS:
        return jsonify({"ok": False, "error": "Invalid flyer key"}), 400
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"ok": False, "error": "No file provided"}), 400
    if not f.filename.lower().endswith(".png"):
        return jsonify({"ok": False, "error": "Only PNG files are allowed"}), 400
    dest = os.path.join(_STATIC_DIR, f"schedule_{key}.png")
    try:
        os.makedirs(_STATIC_DIR, exist_ok=True)
        tmp = dest + ".tmp"
        f.save(tmp)
        os.replace(tmp, dest)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/watch_party/autofill_f1")
@login_required
def wp_autofill_f1():
    """Fetch the next race from Jolpica and return pre-fill data."""
    import requests as _req
    from datetime import datetime, timezone
    try:
        url = "https://api.jolpi.ca/ergast/f1/current.json?limit=50"
        r = _req.get(url, timeout=10)
        r.raise_for_status()
        races = r.json()["MRData"]["RaceTable"]["Races"]
        now = datetime.now(timezone.utc)
        next_race = None
        for race in races:
            date_str = race.get("date", "")
            time_str = race.get("time", "00:00:00Z")
            try:
                dt = datetime.fromisoformat(f"{date_str}T{time_str.rstrip('Z')}+00:00")
            except Exception:
                continue
            if dt >= now:
                next_race = race
                break
        if not next_race:
            return jsonify({"ok": False, "error": "No upcoming races found."})
        race_name = next_race.get("raceName", "")
        date_str = next_race.get("date", "")
        time_str = (next_race.get("time") or "").rstrip("Z")
        # Format date/time for display — watch party starts 1 hour before race
        try:
            from datetime import datetime as _dt, timedelta
            dt = _dt.fromisoformat(f"{date_str}T{time_str}+00:00") - timedelta(hours=1)
            date_display = dt.strftime("%-m/%-d/%Y")
            time_display = dt.strftime("%-I:%M %p UTC")
            datetime_utc = dt.strftime("%Y-%m-%dT%H:%M")
        except Exception:
            date_display = date_str
            time_display = time_str
            datetime_utc = ""
        return jsonify({"ok": True, "title": race_name, "date": date_display, "time": time_display, "datetime_utc": datetime_utc})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/watch_party/toggle_active", methods=["POST"])
@login_required
def wp_toggle_active():
    with _WATCH_PARTY_LOCK:
        wp = _load_wp()
        wp["active"] = not bool(wp.get("active", True))
        _save_wp(wp)
    return jsonify({"ok": True, "active": wp["active"]})


@app.route("/watch_party/toggle_override", methods=["POST"])
@login_required
def wp_toggle_override():
    with _WATCH_PARTY_LOCK:
        wp = _load_wp()
        wp["override"] = not bool(wp.get("override"))
        _save_wp(wp)
    return jsonify({"ok": True, "override": wp["override"]})


@app.route("/watch_party/field", methods=["POST"])
@login_required
def wp_save_field():
    field = (request.form.get("field") or "").strip()
    value = request.form.get("value", "")
    allowed = _AUTO_FIELDS | _MANUAL_FIELDS
    if field not in allowed:
        return jsonify({"ok": False, "error": f"Unknown field '{field}'"}), 400
    with _WATCH_PARTY_LOCK:
        wp = _load_wp()
        wp[field] = value
        _save_wp(wp)
    return jsonify({"ok": True})


@app.route("/watch_party/venues", methods=["POST"])
@login_required
def wp_save_venues():
    try:
        raw_venues = request.form.getlist("venues[]")
        active_names = request.form.getlist("active[]")
        venues = []
        for rv in raw_venues:
            v = json.loads(rv)
            if isinstance(v, dict):
                venues.append({"name": str(v.get("name") or ""), "address": str(v.get("address") or "")})
        with _WATCH_PARTY_LOCK:
            wp = _load_wp()
            wp["_venues"] = venues
            wp["_active_venues"] = active_names
            # Build location lines as "Name — Address" per active venue
            lines = []
            for v in venues:
                if v["name"] in active_names and v["address"]:
                    lines.append(f"{v['name']} — {v['address']}")
            wp["location"] = "\n".join(lines)
            _save_wp(wp)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ----------------------------
# Discord Events
# ----------------------------

_DISCORD_BOT_TOKEN      = (os.getenv("DISCORD_BOT_TOKEN") or "").strip()
_DISCORD_GUILD_ID        = (os.getenv("DISCORD_GUILD_ID") or "").strip()
_WATCH_PARTY_VOICE_CH_ID = (os.getenv("WATCH_PARTY_VOICE_CHANNEL_ID") or "1028490296482344971").strip()
_EASTERN                 = ZoneInfo("America/New_York")

_DISCORD_EVENT_LOCATIONS = {
    "halfbarrel": "Half Barrel Brewing — 9650 Universal Blvd Ste 143, Orlando, FL 32819",
    "hourglass":  "Hourglass Brewing Longwood — 480 South Ronald Reagan Blvd Ste 1020, Longwood, FL 32750",
}


@app.route("/discord_events")
@login_required
def discord_events():
    page = f"""
      <meta name="de-csrf" content="{_csrf_token()}" />

      <!-- Existing Events -->
      <div style="margin-bottom:32px;">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
          <h2 style="margin:0;font-size:16px;">Upcoming Discord Events</h2>
          <button onclick="deLoadEvents()" id="de-refresh-btn"
            style="background:#1a1a1a;color:#aaa;border:1px solid #333;padding:4px 12px;border-radius:7px;font-size:12px;cursor:pointer;">
            Refresh
          </button>
        </div>
        <div id="de-events-list" style="display:flex;flex-direction:column;gap:8px;max-width:560px;">
          <div style="color:#555;font-size:13px;">Loading…</div>
        </div>
      </div>

      <h2 style="margin:0 0 4px 0;">Create Discord Event</h2>
      <p style="color:#666;font-size:13px;margin:0 0 20px 0;">
        Creates a scheduled event in the Discord server. Times are Eastern.
      </p>

      <div style="display:flex;flex-direction:column;gap:16px;max-width:560px;">

        <!-- Name -->
        <div style="padding:16px;background:#1a1a1a;border:1px solid #333;border-radius:12px;">
          <label style="display:block;color:#aaa;font-size:11px;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;">
            Event Name
          </label>
          <input id="de-name" type="text" placeholder="Japan GP Watch Party"
            style="width:100%;box-sizing:border-box;background:#000;color:#eee;border:1px solid #333;padding:9px 12px;border-radius:8px;font-size:14px;outline:none;" />
        </div>

        <!-- Location -->
        <div style="padding:16px;background:#1a1a1a;border:1px solid #333;border-radius:12px;">
          <div style="color:#aaa;font-size:11px;text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;">Location</div>
          <div style="color:#555;font-size:11px;margin-bottom:12px;">Select all that apply</div>
          <div style="display:flex;flex-direction:column;gap:8px;" id="de-locations">

            <label id="loc-voice-wrap" style="display:flex;align-items:flex-start;gap:10px;padding:10px 12px;border-radius:9px;border:1px solid #333;cursor:pointer;transition:border-color .15s;">
              <input type="checkbox" class="de-loc-cb" value="voice" style="margin-top:2px;accent-color:#5865F2;" />
              <div>
                <div style="font-size:13px;font-weight:600;color:#eee;">🎙️ Watchalong Voice Channel</div>
                <div style="font-size:12px;color:#555;margin-top:2px;">Discord voice channel event</div>
              </div>
            </label>

            <label id="loc-hb-wrap" style="display:flex;align-items:flex-start;gap:10px;padding:10px 12px;border-radius:9px;border:1px solid #333;cursor:pointer;transition:border-color .15s;">
              <input type="checkbox" class="de-loc-cb" value="halfbarrel" style="margin-top:2px;accent-color:#5865F2;" />
              <div>
                <div style="font-size:13px;font-weight:600;color:#eee;">🍺 Half Barrel Brewing</div>
                <div style="font-size:12px;color:#555;margin-top:2px;">9650 Universal Blvd Ste 143, Orlando, FL 32819</div>
              </div>
            </label>

            <label id="loc-hg-wrap" style="display:flex;align-items:flex-start;gap:10px;padding:10px 12px;border-radius:9px;border:1px solid #333;cursor:pointer;transition:border-color .15s;">
              <input type="checkbox" class="de-loc-cb" value="hourglass" style="margin-top:2px;accent-color:#5865F2;" />
              <div>
                <div style="font-size:13px;font-weight:600;color:#eee;">⏳ Hourglass Brewing Longwood</div>
                <div style="font-size:12px;color:#555;margin-top:2px;">480 S Ronald Reagan Blvd Ste 1020, Longwood, FL 32750</div>
              </div>
            </label>

          </div>
        </div>

        <!-- Date -->
        <div style="padding:16px;background:#1a1a1a;border:1px solid #333;border-radius:12px;">
          <label style="display:block;color:#aaa;font-size:11px;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;">
            Date
          </label>
          <input id="de-date" type="date"
            style="width:100%;box-sizing:border-box;background:#000;color:#eee;border:1px solid #333;padding:9px 12px;border-radius:8px;font-size:14px;outline:none;color-scheme:dark;" />
        </div>

        <!-- Times -->
        <div style="padding:16px;background:#1a1a1a;border:1px solid #333;border-radius:12px;">
          <div style="color:#aaa;font-size:11px;text-transform:uppercase;letter-spacing:.08em;margin-bottom:12px;">
            Time <span style="color:#555;text-transform:none;font-size:11px;">(Eastern)</span>
          </div>
          <div style="display:flex;gap:12px;">
            <div style="flex:1;">
              <label style="display:block;color:#666;font-size:11px;margin-bottom:6px;">Start</label>
              <input id="de-start" type="time"
                style="width:100%;box-sizing:border-box;background:#000;color:#eee;border:1px solid #333;padding:9px 12px;border-radius:8px;font-size:14px;outline:none;color-scheme:dark;" />
            </div>
            <div style="flex:1;">
              <label style="display:block;color:#666;font-size:11px;margin-bottom:6px;">End</label>
              <input id="de-end" type="time"
                style="width:100%;box-sizing:border-box;background:#000;color:#eee;border:1px solid #333;padding:9px 12px;border-radius:8px;font-size:14px;outline:none;color-scheme:dark;" />
            </div>
          </div>
        </div>

        <!-- Banner -->
        <div style="padding:16px;background:#1a1a1a;border:1px solid #333;border-radius:12px;">
          <div style="color:#aaa;font-size:11px;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;">
            Banner Image <span style="color:#555;text-transform:none;">(optional)</span>
          </div>
          <div id="de-drop"
            style="border:2px dashed #333;border-radius:9px;padding:20px;text-align:center;cursor:pointer;transition:border-color .2s;"
            onclick="document.getElementById('de-file').click()">
            <img id="de-preview" src="" alt=""
              style="display:none;width:100%;max-height:160px;object-fit:cover;border-radius:6px;margin-bottom:10px;" />
            <div id="de-drop-label" style="color:#555;font-size:13px;">Click or drag &amp; drop an image</div>
            <input id="de-file" type="file" accept="image/*" style="display:none;" />
          </div>
          <button id="de-clear-banner" onclick="deClearBanner()"
            style="display:none;margin-top:8px;background:none;border:none;color:#555;font-size:12px;cursor:pointer;text-decoration:underline;padding:0;">
            Remove image
          </button>
        </div>

        <!-- Submit -->
        <button id="de-submit" onclick="deSubmit()"
          style="background:#2a8f52;color:#fff;border:1px solid #38b567;padding:12px 20px;border-radius:10px;font-size:14px;font-weight:600;cursor:pointer;transition:background .15s;">
          Create Discord Event
        </button>

        <!-- Status -->
        <div id="de-toast" style="display:none;padding:10px 14px;border-radius:10px;font-size:14px;"></div>

      </div>

      <script>
      (function(){{
        // Set default date to today
        const d = new Date();
        const pad = n => String(n).padStart(2,'0');
        document.getElementById('de-date').value = `${{d.getFullYear()}}-${{pad(d.getMonth()+1)}}-${{pad(d.getDate())}}`;

        // Checkbox card highlight
        document.querySelectorAll('.de-loc-cb').forEach(function(cb) {{
          cb.addEventListener('change', function() {{
            cb.closest('label').style.borderColor = cb.checked ? '#5865F2' : '#333';
          }});
        }});

        // Banner drag/drop
        const drop = document.getElementById('de-drop');
        const fileIn = document.getElementById('de-file');
        const preview = document.getElementById('de-preview');
        const dropLabel = document.getElementById('de-drop-label');
        const clearBtn = document.getElementById('de-clear-banner');

        drop.addEventListener('dragover', e => {{ e.preventDefault(); drop.style.borderColor='#5865F2'; }});
        drop.addEventListener('dragleave', () => {{ drop.style.borderColor='#333'; }});
        drop.addEventListener('drop', e => {{
          e.preventDefault(); drop.style.borderColor='#333';
          const f = e.dataTransfer.files[0];
          if (f && f.type.startsWith('image/')) deSetBanner(f);
        }});
        fileIn.addEventListener('change', () => {{ if (fileIn.files[0]) deSetBanner(fileIn.files[0]); }});

        window.deSetBanner = function(file) {{
          const reader = new FileReader();
          reader.onload = ev => {{
            preview.src = ev.target.result;
            preview.style.display = 'block';
            dropLabel.style.display = 'none';
            clearBtn.style.display = 'block';
            if (!fileIn.files[0]) {{
              const dt = new DataTransfer(); dt.items.add(file); fileIn.files = dt.files;
            }}
          }};
          reader.readAsDataURL(file);
        }};

        window.deClearBanner = function() {{
          fileIn.value = '';
          preview.style.display = 'none';
          dropLabel.style.display = '';
          clearBtn.style.display = 'none';
        }};

        // Toast
        window.deToast = function(msg, ok) {{
          const el = document.getElementById('de-toast');
          el.textContent = msg;
          el.style.display = 'block';
          el.style.background = ok ? '#0d3320' : '#3a0d0d';
          el.style.border = '1px solid ' + (ok ? '#2a8f52' : '#8f2a2a');
          el.style.color = ok ? '#6f6' : '#f88';
          clearTimeout(el._t);
          el._t = setTimeout(() => {{ el.style.display = 'none'; }}, 5000);
        }};

        // Submit
        window.deSubmit = async function() {{
          const name  = document.getElementById('de-name').value.trim();
          const locs  = Array.from(document.querySelectorAll('.de-loc-cb:checked')).map(c => c.value);
          const date  = document.getElementById('de-date').value;
          const start = document.getElementById('de-start').value;
          const end   = document.getElementById('de-end').value;

          if (!name)        return deToast('Please enter an event name.', false);
          if (!locs.length) return deToast('Please select at least one location.', false);
          if (!date)        return deToast('Please choose a date.', false);
          if (!start)       return deToast('Please set a start time.', false);
          if (!end)         return deToast('Please set an end time.', false);

          const btn = document.getElementById('de-submit');
          btn.disabled = true; btn.textContent = 'Creating…';

          try {{
            const csrf = document.querySelector('meta[name="de-csrf"]').content;
            const fd = new FormData();
            fd.append('_csrf', csrf);
            fd.append('name', name);
            locs.forEach(l => fd.append('locations', l));
            fd.append('date', date);
            fd.append('start_time', start);
            fd.append('end_time', end);
            const bannerFile = document.getElementById('de-file').files[0];
            if (bannerFile) fd.append('banner', bannerFile);

            const r = await fetch('/discord_events/create', {{method:'POST', body:fd, credentials:'same-origin'}});
            const ct = r.headers.get('content-type') || '';
            if (!ct.includes('application/json')) {{
              if (r.status === 302 || r.redirected) {{
                deToast('❌ Session expired — reload the page and log in again.', false);
              }} else {{
                deToast(`❌ Server error (HTTP ${{r.status}}) — check that you're still logged in.`, false);
              }}
              return;
            }}
            const data = await r.json();
            if (data.ok) {{
              deToast('✅ Event created: ' + data.name, true);
              document.getElementById('de-name').value = '';
              deClearBanner();
            }} else {{
              deToast('❌ ' + data.error, false);
            }}
          }} catch(e) {{
            deToast('❌ Request failed: ' + e.message, false);
          }} finally {{
            btn.disabled = false; btn.textContent = 'Create Discord Event';
          }}
        }};

        // Load existing events on page load
        window.deLoadEvents = async function() {{
          const list = document.getElementById('de-events-list');
          const btn  = document.getElementById('de-refresh-btn');
          btn.textContent = 'Loading…'; btn.disabled = true;
          try {{
            const r = await fetch('/discord_events/list', {{credentials:'same-origin'}});
            const data = await r.json();
            if (!data.ok) {{
              list.innerHTML = `<div style="color:#f88;font-size:13px;">Error: ${{data.error}}</div>`;
              return;
            }}
            if (!data.events.length) {{
              list.innerHTML = '<div style="color:#555;font-size:13px;">No upcoming events.</div>';
              return;
            }}
            list.innerHTML = data.events.map(function(ev) {{
              const start = ev.start ? new Date(ev.start) : null;
              const end   = ev.end   ? new Date(ev.end)   : null;
              const fmt = dt => dt ? dt.toLocaleString('en-US',{{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',hour12:true,timeZone:'America/New_York'}}) + ' ET' : '';
              const statusLabel = [,'SCHEDULED','ACTIVE','COMPLETED','CANCELLED'][ev.status] || '';
              const statusColor = ev.status === 2 ? '#6f6' : ev.status >= 3 ? '#555' : '#aaa';
              return `<div style="display:flex;align-items:flex-start;justify-content:space-between;gap:12px;padding:12px 14px;background:#1a1a1a;border:1px solid #333;border-radius:10px;">
                <div style="min-width:0;">
                  <div style="font-size:13px;font-weight:600;color:#eee;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${{ev.name}}</div>
                  <div style="font-size:12px;color:#888;margin-top:3px;">${{fmt(start)}}${{end?' → '+fmt(end):''}}</div>
                  ${{ev.location ? `<div style="font-size:11px;color:#555;margin-top:2px;">${{ev.location}}</div>` : ''}}
                  <div style="font-size:11px;color:${{statusColor}};margin-top:3px;">${{statusLabel}}</div>
                </div>
                ${{ev.status <= 2 ? `<button onclick="deCancel('${{ev.id}}',this)"
                  style="flex-shrink:0;background:#3a0d0d;color:#f88;border:1px solid #8f2a2a;padding:5px 11px;border-radius:7px;font-size:12px;cursor:pointer;white-space:nowrap;">
                  Cancel
                </button>` : ''}}
              </div>`;
            }}).join('');
          }} catch(e) {{
            list.innerHTML = `<div style="color:#f88;font-size:13px;">Failed to load: ${{e.message}}</div>`;
          }} finally {{
            btn.textContent = 'Refresh'; btn.disabled = false;
          }}
        }};

        window.deCancel = async function(eventId, btn) {{
          if (!confirm('Cancel this Discord event? This cannot be undone.')) return;
          btn.disabled = true; btn.textContent = '…';
          try {{
            const csrf = document.querySelector('meta[name="de-csrf"]').content;
            const fd = new FormData();
            fd.append('_csrf', csrf);
            fd.append('event_id', eventId);
            const r = await fetch('/discord_events/cancel', {{method:'POST', body:fd, credentials:'same-origin'}});
            const data = await r.json();
            if (data.ok) {{
              deToast('Event cancelled.', true);
              deLoadEvents();
            }} else {{
              deToast('❌ ' + data.error, false);
              btn.disabled = false; btn.textContent = 'Cancel';
            }}
          }} catch(e) {{
            deToast('❌ ' + e.message, false);
            btn.disabled = false; btn.textContent = 'Cancel';
          }}
        }};

        deLoadEvents();

      }})();
      </script>
    """
    return _render(page)


@app.route("/discord_events/list")
@login_required
def discord_events_list():
    if not _DISCORD_BOT_TOKEN or not _DISCORD_GUILD_ID:
        return jsonify({"ok": False, "error": "Bot token or guild ID not configured."}), 500
    try:
        resp = requests.get(
            f"https://discord.com/api/v10/guilds/{_DISCORD_GUILD_ID}/scheduled-events",
            headers={"Authorization": f"Bot {_DISCORD_BOT_TOKEN}"},
            timeout=10,
        )
    except requests.RequestException as e:
        return jsonify({"ok": False, "error": f"Network error: {e}"}), 502
    if resp.status_code != 200:
        return jsonify({"ok": False, "error": f"Discord API {resp.status_code}: {resp.text}"}), 502
    events = resp.json()
    out = []
    for ev in events:
        start = ev.get("scheduled_start_time", "")
        end   = ev.get("scheduled_end_time", "")
        loc   = (ev.get("entity_metadata") or {}).get("location") or ""
        if not loc and ev.get("channel_id"):
            loc = "Voice Channel"
        out.append({
            "id":    ev.get("id"),
            "name":  ev.get("name"),
            "start": start,
            "end":   end,
            "location": loc,
            "status": ev.get("status", 1),
        })
    out.sort(key=lambda e: e["start"])
    return jsonify({"ok": True, "events": out})


@app.route("/discord_events/cancel", methods=["POST"])
@login_required
def discord_events_cancel():
    _csrf_protect()
    event_id = (request.form.get("event_id") or "").strip()
    if not event_id:
        return jsonify({"ok": False, "error": "Missing event_id."}), 400
    if not _DISCORD_BOT_TOKEN or not _DISCORD_GUILD_ID:
        return jsonify({"ok": False, "error": "Bot token or guild ID not configured."}), 500
    try:
        resp = requests.delete(
            f"https://discord.com/api/v10/guilds/{_DISCORD_GUILD_ID}/scheduled-events/{event_id}",
            headers={"Authorization": f"Bot {_DISCORD_BOT_TOKEN}"},
            timeout=10,
        )
    except requests.RequestException as e:
        return jsonify({"ok": False, "error": f"Network error: {e}"}), 502
    if resp.status_code in (200, 204):
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": f"Discord API {resp.status_code}: {resp.text}"}), 502


@app.route("/discord_events/create", methods=["POST"])
@login_required
def discord_events_create():
    _csrf_protect()

    name      = (request.form.get("name") or "").strip()
    locations = request.form.getlist("locations")
    date_str  = (request.form.get("date") or "").strip()
    start_str = (request.form.get("start_time") or "").strip()
    end_str   = (request.form.get("end_time") or "").strip()
    banner    = request.files.get("banner")

    if not all([name, date_str, start_str, end_str]):
        return jsonify({"ok": False, "error": "All fields are required."}), 400
    if not locations:
        return jsonify({"ok": False, "error": "Please select at least one location."}), 400

    try:
        start_dt = datetime.fromisoformat(f"{date_str}T{start_str}:00").replace(tzinfo=_EASTERN)
        end_dt   = datetime.fromisoformat(f"{date_str}T{end_str}:00").replace(tzinfo=_EASTERN)
    except ValueError as e:
        return jsonify({"ok": False, "error": f"Invalid date/time: {e}"}), 400

    if end_dt <= start_dt:
        return jsonify({"ok": False, "error": "End time must be after start time."}), 400

    payload: dict = {
        "name":                 name,
        "privacy_level":        2,
        "scheduled_start_time": start_dt.isoformat(),
        "scheduled_end_time":   end_dt.isoformat(),
    }

    venue_keys   = [l for l in locations if l in _DISCORD_EVENT_LOCATIONS]
    voice_chosen = "voice" in locations
    invalid      = [l for l in locations if l not in _DISCORD_EVENT_LOCATIONS and l != "voice"]
    if invalid:
        return jsonify({"ok": False, "error": f"Invalid location(s): {', '.join(invalid)}"}), 400

    if venue_keys:
        # External event — combine all selected venue strings
        combined = " & ".join(_DISCORD_EVENT_LOCATIONS[k] for k in venue_keys)
        payload["entity_type"]     = 3
        payload["entity_metadata"] = {"location": combined}
    elif voice_chosen:
        # Voice-only event
        payload["entity_type"] = 2
        payload["channel_id"]  = _WATCH_PARTY_VOICE_CH_ID
    else:
        return jsonify({"ok": False, "error": "Invalid location."}), 400

    if banner and banner.filename:
        try:
            b64 = base64.b64encode(banner.read()).decode("utf-8")
            payload["image"] = f"data:{banner.mimetype or 'image/jpeg'};base64,{b64}"
        except Exception as e:
            return jsonify({"ok": False, "error": f"Banner error: {e}"}), 400

    if not _DISCORD_BOT_TOKEN or not _DISCORD_GUILD_ID:
        return jsonify({"ok": False, "error": "Bot token or guild ID not configured."}), 500

    try:
        resp = requests.post(
            f"https://discord.com/api/v10/guilds/{_DISCORD_GUILD_ID}/scheduled-events",
            headers={"Authorization": f"Bot {_DISCORD_BOT_TOKEN}", "Content-Type": "application/json"},
            json=payload,
            timeout=15,
        )
    except requests.RequestException as e:
        return jsonify({"ok": False, "error": f"Network error: {e}"}), 502

    if resp.status_code in (200, 201):
        ev = resp.json()
        return jsonify({"ok": True, "event_id": ev.get("id"), "name": ev.get("name")})
    return jsonify({"ok": False, "error": f"Discord API {resp.status_code}: {resp.text}"}), 502



# ─────────────────────────────────────────────────────────────
# SSE endpoint — real-time push to browser
# ─────────────────────────────────────────────────────────────

@app.route("/api/sse")
@login_required
def api_sse():
    """Server-Sent Events stream. Pushes race_state events every 2 s."""
    def _generate():
        q = _sse_subscribe()
        # Send initial snapshot immediately
        snap = _race_snapshot_safe()
        yield f"event: race_state\ndata: {json.dumps(snap, default=str)}\n\n"
        last_push = time.time()
        try:
            while True:
                # Drain any queued pushes first
                try:
                    msg = q.get(timeout=2.0)
                    yield msg
                    last_push = time.time()
                except queue.Empty:
                    pass
                # Heartbeat / periodic poll regardless
                if time.time() - last_push >= 2.0:
                    snap = _race_snapshot_safe()
                    yield f"event: race_state\ndata: {json.dumps(snap, default=str)}\n\n"
                    last_push = time.time()
        except GeneratorExit:
            pass
        finally:
            _sse_unsubscribe(q)

    return Response(
        stream_with_context(_generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


def _race_snapshot_safe() -> dict:
    """Grab race-live snapshot from the bot reference, safely."""
    try:
        if bot_reference and hasattr(bot_reference, "of1_race_live_snapshot"):
            return bot_reference.of1_race_live_snapshot()
    except Exception:
        pass
    return {}


# ─────────────────────────────────────────────────────────────
# Race Live page
# ─────────────────────────────────────────────────────────────

_RACE_LIVE_PAGE = """
<style>
  .rl-card{background:#111;border:1px solid #222;border-radius:12px;padding:16px;margin-bottom:16px;}
  .rl-label{font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.08em;margin-bottom:10px;font-weight:600;}
  .badge{display:inline-flex;align-items:center;padding:2px 9px;border-radius:20px;font-size:11px;font-weight:700;letter-spacing:.03em;}
  .badge-green{background:#0f2a0f;color:#4ade80;border:1px solid #166534;}
  .badge-red{background:#2a0f0f;color:#f87171;border:1px solid #7f1d1d;}
  .badge-yellow{background:#2a2a0f;color:#facc15;border:1px solid #713f12;}
  .badge-grey{background:#1a1a1a;color:#666;border:1px solid #2a2a2a;}
  /* Feed rows */
  .feed-row{position:relative;display:flex;gap:8px;align-items:center;padding:6px 4px;border-bottom:1px solid #1a1a1a;font-size:13px;border-radius:6px;transition:background .1s;}
  .feed-row:hover{background:rgba(255,255,255,.04);}
  .feed-ts{color:#555;min-width:58px;font-family:monospace;font-size:12px;flex-shrink:0;}
  .feed-emoji{min-width:20px;text-align:center;flex-shrink:0;}
  .feed-msg{color:#ccc;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
  .feed-row:hover .feed-msg{white-space:normal;}
  /* Hover reveal: status pill + override button */
  .feed-hover{opacity:0;display:flex;align-items:center;gap:6px;flex-shrink:0;transition:opacity .15s;pointer-events:none;}
  .feed-row:hover .feed-hover{opacity:1;pointer-events:auto;}
  .feed-pill{font-size:11px;font-weight:700;padding:2px 7px;border-radius:10px;white-space:nowrap;}
  .pill-posted{background:#0f2a0f;color:#4ade80;border:1px solid #166534;}
  .pill-skipped{background:#1a1a1a;color:#666;border:1px solid #2a2a2a;}
  .pill-track_deletion{background:#2a1a00;color:#fb923c;border:1px solid #7c2d12;}
  .pill-boundary{background:#0f1a2a;color:#60a5fa;border:1px solid #1e3a5f;}
  .feed-override-btn{background:#1a2a1a;color:#86efac;border:1px solid #166534;padding:2px 8px;border-radius:6px;cursor:pointer;font-size:11px;font-weight:600;}
  .feed-override-btn:hover{background:#14532d;}
  /* Process rows */
  .proc-row{display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid #1a1a1a;}
  .proc-row:last-child{border-bottom:none;}
  /* Settings */
  .settings-row{display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid #1a1a1a;}
  .settings-row:last-child{border-bottom:none;}
  .settings-key{color:#888;font-size:13px;min-width:170px;font-family:monospace;}
  input.edit-field{background:#0a0a0a;border:1px solid #2a2a2a;color:#eee;padding:5px 10px;border-radius:6px;font-size:13px;width:110px;font-family:monospace;}
  input.edit-field:focus{outline:none;border-color:#444;}
  button.rl-btn{background:#1a1a1a;color:#ccc;border:1px solid #2a2a2a;padding:5px 12px;border-radius:7px;cursor:pointer;font-size:12px;font-weight:600;}
  button.rl-btn:hover{background:#222;color:#eee;}
  button.rl-btn-red{background:#2a0f0f;color:#f87171;border-color:#7f1d1d;}
  button.rl-btn-red:hover{background:#3a1010;}
  button.rl-btn-green{background:#0f2a0f;color:#86efac;border-color:#166534;}
  button.rl-btn-green:hover{background:#14532d;}
  /* Modal */
  #send-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:200;align-items:center;justify-content:center;}
  #send-overlay.open{display:flex;}
  .modal{background:#111;border:1px solid #2a2a2a;border-radius:14px;padding:24px;width:500px;max-width:95vw;box-shadow:0 20px 60px rgba(0,0,0,.6);}
  .modal h3{margin:0 0 6px;color:#eee;font-size:16px;}
  .modal-sub{font-size:12px;color:#666;margin-bottom:14px;}
  .modal-preview{background:#0a0a0a;border:1px solid #1a1a1a;border-radius:8px;padding:10px 12px;font-size:13px;color:#aaa;margin-bottom:14px;font-family:monospace;white-space:pre-wrap;word-break:break-all;}
  .modal textarea{width:100%;box-sizing:border-box;background:#0a0a0a;border:1px solid #2a2a2a;color:#eee;padding:10px;border-radius:8px;font-size:13px;resize:vertical;min-height:80px;font-family:monospace;}
  .modal textarea:focus{outline:none;border-color:#444;}
  .modal .actions{margin-top:14px;display:flex;gap:10px;justify-content:flex-end;}
</style>

<h1 class="text-xl font-bold text-white mb-5">Race Live</h1>

<!-- Process Tags -->
<div class="rl-card">
  <div class="rl-label">Active Guilds</div>
  <div id="proc-list">Loading…</div>
</div>

<!-- Active Session -->
<div class="rl-card">
  <div class="rl-label" style="display:flex;justify-content:space-between;align-items:center;">
    <span>Current Session</span>
    <span id="session-ts" style="color:#444;font-size:11px;font-weight:400;"></span>
  </div>
  <div id="session-list">Loading…</div>
  <div style="margin-top:12px;border-top:1px solid #1a1a1a;padding-top:12px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
    <span style="font-size:11px;color:#555;">Manual session key:</span>
    <select id="session-key-select"
      style="background:#0a0a0a;color:#ccc;border:1px solid #2a2a2a;border-radius:6px;padding:3px 8px;font-size:12px;font-family:monospace;">
      <option value="">— loading —</option>
    </select>
    <input id="session-key-gid" type="text" placeholder="Guild ID"
      style="background:#0a0a0a;color:#ccc;border:1px solid #2a2a2a;border-radius:6px;padding:4px 8px;font-size:12px;font-family:monospace;width:130px;" />
    <button class="rl-btn" onclick="applySessionKey()">Set Session Key</button>
  </div>
</div>

<!-- Message Feed -->
<div class="rl-card">
  <div class="rl-label" style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
    <span>Race Control Feed</span>
    <div style="display:flex;gap:12px;align-items:center;">
      <label style="font-size:12px;color:#666;display:flex;align-items:center;gap:4px;">
        <input type="checkbox" id="feed-skipped" checked onchange="renderFeed()">
        <span>Show skipped</span>
      </label>
      <label style="font-size:12px;color:#666;">
        Guild:
        <select id="feed-guild" style="background:#0a0a0a;color:#ccc;border:1px solid #2a2a2a;border-radius:6px;padding:2px 6px;font-size:12px;margin-left:4px;" onchange="renderFeed()">
          <option value="">All</option>
        </select>
      </label>
    </div>
  </div>
  <div style="font-size:11px;color:#555;margin-bottom:8px;">Hover any row to see status and send controls.</div>
  <div id="feed-list" style="max-height:420px;overflow-y:auto;">Loading…</div>
</div>

<!-- Settings -->
<div class="rl-card">
  <div class="rl-label">Settings</div>
  <div id="settings-panel">
    <div class="settings-row">
      <span class="settings-key">delay_seconds</span>
      <input class="edit-field" id="set-delay" type="number" step="0.5" min="0" placeholder="0">
      <button class="rl-btn" onclick="applySetting('delay_seconds','set-delay')">Apply</button>
      <span style="color:#555;font-size:12px;">Spoiler delay before posting messages</span>
    </div>
    <div class="settings-row">
      <span class="settings-key">poll_seconds</span>
      <input class="edit-field" id="set-poll" type="number" step="0.5" min="1" placeholder="3">
      <button class="rl-btn" onclick="applySetting('poll_seconds','set-poll')">Apply</button>
      <span style="color:#555;font-size:12px;">OpenF1 poll interval</span>
    </div>
    <div class="settings-row">
      <span class="settings-key">ops_channel_id</span>
      <input class="edit-field" id="set-ops" type="text" placeholder="channel ID">
      <button class="rl-btn" onclick="applySetting('ops_channel_id','set-ops')">Apply</button>
      <span style="color:#555;font-size:12px;">Ops notice channel</span>
    </div>
  </div>
</div>

<!-- Send / override modal -->
<div id="send-overlay">
  <div class="modal">
    <h3>Send to Race Thread</h3>
    <div class="modal-sub">Guild: <span id="send-guild-label" style="color:#aaa;"></span></div>
    <div id="send-preview" class="modal-preview" style="display:none;"></div>
    <textarea id="send-text" placeholder="Message text…"></textarea>
    <div class="actions">
      <button class="rl-btn" onclick="closeSendModal()">Cancel</button>
      <button class="rl-btn rl-btn-green" onclick="confirmSend()">Send to Thread</button>
    </div>
  </div>
</div>

<script>
let _state = {};
let _sendGuildId = null;
let _settingsReady = false;

const evtSource = new EventSource('/api/sse');
evtSource.addEventListener('race_state', e => {
  _state = JSON.parse(e.data);
  renderProcs();
  renderSessions();
  renderFeed();
  syncSettings();
});

// ── Process tags ──────────────────────────────────────────
function renderProcs() {
  const guilds = _state.guilds || {};
  const rows = [];
  for (const [gid, g] of Object.entries(guilds)) {
    const running = g.running;
    const kind = g.session_kind || '—';
    const kindColor = {RACE:'#f87171',SPRINT:'#fb923c',QUALI:'#60a5fa',SPRINT_QUALI:'#a78bfa'}[kind] || '#888';
    const statusBadge = running
      ? `<span class="badge badge-green">RUNNING</span>`
      : `<span class="badge badge-grey">STOPPED</span>`;
    const holdBadge = g.hold ? `<span class="badge badge-yellow">HOLD</span>` : '';
    const ts = g.last_event_ts ? `<span style="color:#555;font-size:12px;font-family:monospace;">${g.last_event_ts.slice(11,19)} UTC</span>` : '';
    const thread = g.thread_name ? `<span style="color:#666;font-size:12px;">#${escHtml(g.thread_name)}</span>` : '';
    const actionBtns = running
      ? `<button class="rl-btn" onclick="openSendModal('${gid}')" style="padding:3px 10px;">Send Msg</button>
         <button class="rl-btn rl-btn-red" onclick="killSession('${gid}')" style="padding:3px 10px;">Kill</button>`
      : `<button class="rl-btn rl-btn-green" onclick="startSession('${gid}')" style="padding:3px 10px;">Clear Hold</button>`;
    rows.push(`<div class="proc-row">
      <div style="min-width:130px;font-family:monospace;font-size:13px;color:#ccc;">Guild ${gid}</div>
      <div style="min-width:110px;font-size:13px;font-weight:700;color:${kindColor};">${kind}</div>
      ${statusBadge}
      ${holdBadge}
      ${thread}
      ${ts}
      <div style="margin-left:auto;display:flex;gap:6px;">${actionBtns}</div>
    </div>`);
  }
  document.getElementById('proc-list').innerHTML = rows.length
    ? rows.join('')
    : '<div style="color:#555;font-size:13px;padding:4px 0;">No guilds configured or bot not connected.</div>';
}

// ── Session ───────────────────────────────────────────────
function renderSessions() {
  const guilds = _state.guilds || {};
  const rows = [];
  for (const [gid, g] of Object.entries(guilds)) {
    if (!g.running && !g.session_key) continue;
    const kindColor = {RACE:'#f87171',SPRINT:'#fb923c',QUALI:'#60a5fa',SPRINT_QUALI:'#a78bfa'}[g.session_kind] || '#888';
    rows.push(`<div style="display:flex;gap:16px;align-items:center;padding:6px 0;border-bottom:1px solid #1a1a1a;font-size:13px;">
      <span style="color:#aaa;min-width:130px;font-family:monospace;">Guild ${gid}</span>
      <span style="color:${kindColor};font-weight:700;min-width:100px;">${g.session_kind || '—'}</span>
      <span style="color:#555;">key: ${g.session_key || '—'}</span>
      ${g.thread_name ? `<span style="color:#555;">#${escHtml(g.thread_name)}</span>` : ''}
    </div>`);
  }
  document.getElementById('session-list').innerHTML = rows.length
    ? rows.join('')
    : '<div style="color:#555;font-size:13px;padding:4px 0;">No active sessions.</div>';
  document.getElementById('session-ts').textContent = new Date().toLocaleTimeString();
}

// ── Feed ─────────────────────────────────────────────────
function renderFeed() {
  const guilds = _state.guilds || {};
  const sel = document.getElementById('feed-guild');
  const existing = Array.from(sel.options).map(o => o.value);
  for (const gid of Object.keys(guilds)) {
    if (!existing.includes(gid)) {
      const opt = document.createElement('option');
      opt.value = gid; opt.textContent = 'Guild ' + gid;
      sel.appendChild(opt);
    }
  }

  const showSkipped = document.getElementById('feed-skipped').checked;
  const filterGuild = sel.value;

  let allRows = [];
  for (const [gid, g] of Object.entries(guilds)) {
    if (filterGuild && gid !== filterGuild) continue;
    for (const item of (g.feed || [])) allRows.push({gid, ...item});
  }
  allRows.sort((a,b) => b.ts > a.ts ? 1 : -1);

  const html = [];
  for (const item of allRows) {
    if (!showSkipped && item.status === 'skipped') continue;
    const pillCls = 'pill-' + item.status;
    const pillLabel = {posted:'✓ posted', skipped:'skipped', track_deletion:'🚫 deleted', boundary:'boundary'}[item.status] || item.status;
    const msgJson = JSON.stringify(item.msg || '');
    const gidJs = JSON.stringify(item.gid);
    const overrideBtn = `<button class="feed-override-btn" onclick="sendOverride(${gidJs},${msgJson})">Override →</button>`;
    html.push(`<div class="feed-row">
      <span class="feed-ts">${item.ts || ''}</span>
      <span class="feed-emoji">${item.emoji || ''}</span>
      <span class="feed-msg">${escHtml(item.msg || '')}</span>
      <span class="feed-hover">
        <span class="feed-pill ${pillCls}">${pillLabel}</span>
        ${overrideBtn}
      </span>
    </div>`);
  }
  document.getElementById('feed-list').innerHTML = html.length
    ? html.join('')
    : '<div style="color:#555;font-size:13px;padding:8px 0;">No messages yet — feed populates during an active race session.</div>';
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ── Settings (pre-fill inputs; don't clobber while user is typing) ─────────
function syncSettings() {
  const fields = [
    ['set-delay', _state.delay_seconds],
    ['set-poll',  _state.poll_seconds],
    ['set-ops',   _state.ops_channel_id],
  ];
  for (const [id, val] of fields) {
    const el = document.getElementById(id);
    if (!el || document.activeElement === el) continue;
    if (val !== undefined && val !== null) el.value = val;
  }
}

// ── Actions ───────────────────────────────────────────────
async function _post(url, body) {
  const r = await fetch(url, {
    method: 'POST',
    headers: {'Content-Type':'application/json','X-CSRFToken':getCsrf()},
    body: JSON.stringify(body),
  });
  return r.json();
}

function getCsrf() {
  const m = document.querySelector('meta[name="csrf-token"]');
  return m ? m.getAttribute('content') : '';
}

async function killSession(gid) {
  if (!confirm('Kill race live for guild ' + gid + '? This sets a hold — use Clear Hold to re-enable.')) return;
  const r = await _post('/api/race/kill', {guild_id: gid});
  alert(r.message || (r.ok ? 'Done' : 'Error'));
}

async function startSession(gid) {
  const r = await _post('/api/race/start', {guild_id: gid});
  alert(r.message || (r.ok ? 'Done' : 'Error'));
}

function openSendModal(gid) {
  _sendGuildId = gid;
  document.getElementById('send-guild-label').textContent = 'Guild ' + gid;
  document.getElementById('send-text').value = '';
  document.getElementById('send-preview').style.display = 'none';
  document.getElementById('send-overlay').classList.add('open');
  setTimeout(() => document.getElementById('send-text').focus(), 50);
}

function sendOverride(gid, msg) {
  _sendGuildId = gid;
  document.getElementById('send-guild-label').textContent = 'Guild ' + gid;
  document.getElementById('send-text').value = msg;
  const prev = document.getElementById('send-preview');
  prev.textContent = msg;
  prev.style.display = 'block';
  document.getElementById('send-overlay').classList.add('open');
}

function closeSendModal() {
  document.getElementById('send-overlay').classList.remove('open');
  _sendGuildId = null;
}

async function confirmSend() {
  const msg = document.getElementById('send-text').value.trim();
  if (!msg || !_sendGuildId) return;
  closeSendModal();
  const r = await _post('/api/race/send', {guild_id: _sendGuildId, message: msg});
  if (!r.ok) alert('Send failed: ' + (r.message || 'unknown error'));
}

async function applySetting(key, inputId) {
  const value = document.getElementById(inputId).value;
  const r = await _post('/api/race/settings', {key, value});
  if (!r.ok) alert('Error: ' + (r.message || 'unknown'));
  else {
    const el = document.getElementById(inputId);
    el.style.borderColor = '#166534';
    setTimeout(() => el.style.borderColor = '', 1200);
  }
}

// close modal on overlay click
document.getElementById('send-overlay').addEventListener('click', function(e){
  if (e.target === this) closeSendModal();
});

// ── Session key dropdown ──────────────────────────────────
(async function loadSessionKeys() {
  try {
    const r = await fetch('/api/race/sessions', {credentials:'same-origin'});
    const d = await r.json();
    if (!d.ok || !d.sessions) return;
    const sel = document.getElementById('session-key-select');
    sel.innerHTML = '<option value="">— select —</option>' +
      d.sessions.map(s =>
        `<option value="${s.session_key}">${s.session_key} — ${escHtml(s.circuit_short_name)} ${escHtml(s.session_name)} (${s.date_start})</option>`
      ).join('');
    // Pre-fill guild ID from first guild
    const gids = Object.keys(_state.guilds || {});
    if (gids.length === 1) {
      document.getElementById('session-key-gid').value = gids[0];
    }
  } catch(e) {}
})();

// Auto-fill guild from state once loaded
evtSource.addEventListener('race_state', function() {
  const gids = Object.keys(_state.guilds || {});
  const gidEl = document.getElementById('session-key-gid');
  if (gids.length === 1 && !gidEl.value) gidEl.value = gids[0];
}, {once: true});

async function applySessionKey() {
  const sk = document.getElementById('session-key-select').value;
  const gid = document.getElementById('session-key-gid').value.trim();
  if (!sk) return alert('Select a session key.');
  if (!gid) return alert('Enter a guild ID.');
  // Use the set-session-key setting
  const r = await _post('/api/race/settings', {key: 'session_key', value: parseInt(sk), guild_id: parseInt(gid)});
  alert(r.message || (r.ok ? 'Done' : 'Error'));
}
</script>
"""

@app.route("/race")
@login_required
def race_live():
    return _render(_RACE_LIVE_PAGE)


# ─────────────────────────────────────────────────────────────
# Race Live API endpoints
# ─────────────────────────────────────────────────────────────

@app.route("/api/race/send", methods=["POST"])
@login_required
def api_race_send():
    data = request.get_json(silent=True) or {}
    gid = data.get("guild_id")
    msg = str(data.get("message") or "").strip()
    if not gid or not msg:
        return jsonify({"ok": False, "message": "guild_id and message required"})
    try:
        bot_loop = bot_reference.loop if bot_reference else None
        if not bot_loop or not bot_loop.is_running():
            return jsonify({"ok": False, "message": "Bot not running"})
        coro = bot_reference.of1_dashboard_send_to_thread(int(gid), msg)
        fut = asyncio.run_coroutine_threadsafe(coro, bot_loop)
        ok, message = fut.result(timeout=10)
        return jsonify({"ok": ok, "message": message})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


@app.route("/api/race/kill", methods=["POST"])
@login_required
def api_race_kill():
    data = request.get_json(silent=True) or {}
    gid = data.get("guild_id")
    if not gid:
        return jsonify({"ok": False, "message": "guild_id required"})
    try:
        bot_loop = bot_reference.loop if bot_reference else None
        if not bot_loop or not bot_loop.is_running():
            return jsonify({"ok": False, "message": "Bot not running"})
        coro = bot_reference.of1_dashboard_kill_race_live(int(gid))
        fut = asyncio.run_coroutine_threadsafe(coro, bot_loop)
        ok, message = fut.result(timeout=10)
        return jsonify({"ok": ok, "message": message})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


@app.route("/api/race/start", methods=["POST"])
@login_required
def api_race_start():
    data = request.get_json(silent=True) or {}
    gid = data.get("guild_id")
    if not gid:
        return jsonify({"ok": False, "message": "guild_id required"})
    try:
        bot_loop = bot_reference.loop if bot_reference else None
        if not bot_loop or not bot_loop.is_running():
            return jsonify({"ok": False, "message": "Bot not running"})
        coro = bot_reference.of1_dashboard_start_race_live(int(gid))
        fut = asyncio.run_coroutine_threadsafe(coro, bot_loop)
        ok, message = fut.result(timeout=10)
        return jsonify({"ok": ok, "message": message})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


@app.route("/api/race/sessions")
@login_required
def api_race_sessions():
    """Fetch recent sessions from OpenF1 for session-key dropdown."""
    try:
        r = requests.get(
            "https://api.openf1.org/v1/sessions",
            params={"year": datetime.now(timezone.utc).year},
            timeout=10,
            headers={"User-Agent": "OF1-Dashboard"},
        )
        if r.status_code != 200:
            return jsonify({"ok": False, "error": f"OpenF1 {r.status_code}"}), 502
        sessions = r.json() or []
        # Filter to the most recent 20, sorted descending
        sorted_sess = sorted(sessions, key=lambda s: s.get("session_key") or 0, reverse=True)[:20]
        out = [
            {
                "session_key": s.get("session_key"),
                "session_name": s.get("session_name") or "",
                "session_type": s.get("session_type") or "",
                "circuit_short_name": s.get("circuit_short_name") or "",
                "date_start": (s.get("date_start") or "")[:10],
            }
            for s in sorted_sess
        ]
        return jsonify({"ok": True, "sessions": out})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 502


@app.route("/api/race/settings", methods=["POST"])
@login_required
def api_race_settings():
    data = request.get_json(silent=True) or {}
    key      = str(data.get("key")      or "").strip()
    value    = data.get("value")
    guild_id = int(data.get("guild_id") or 0)
    if not key:
        return jsonify({"ok": False, "message": "key required"})
    try:
        if bot_reference and hasattr(bot_reference, "of1_apply_race_setting"):
            ok, message = bot_reference.of1_apply_race_setting(key, value, guild_id=guild_id)
        else:
            return jsonify({"ok": False, "message": "Bot not connected"})
        return jsonify({"ok": ok, "message": message})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


def run_dashboard():
    try:
        init_runtime_db()
    except Exception:
        pass
    _sched_thread = threading.Thread(target=_scheduled_msgs_worker, daemon=True, name="scheduled-msgs")
    _sched_thread.start()
    port = int(os.getenv("DASHBOARD_PORT", "5000"))
    app.run(host="0.0.0.0", port=port, threaded=True)

# ─────────────────────────────────────────────────────────────
# Gallery Manager
# ─────────────────────────────────────────────────────────────
_GALLERY_ALLOWED_EXT = {".jpg", ".jpeg", ".png", ".gif", ".webp"}


@app.route("/gallery_mgr")
@login_required
def gallery_mgr():
    os.makedirs(GALLERY_DIR, exist_ok=True)
    files = []
    for fname in sorted(os.listdir(GALLERY_DIR)):
        ext = os.path.splitext(fname)[1].lower()
        if ext in _GALLERY_ALLOWED_EXT:
            size = os.path.getsize(os.path.join(GALLERY_DIR, fname))
            files.append({"name": fname, "size": size, "url": f"/static/gallery/{fname}"})

    def _gallery_row(photo):
        ename = _escape(photo["name"])
        eurl  = _escape(photo["url"])
        kb    = photo["size"] // 1024
        csrf  = _csrf_input()
        return (
            f'<div class="flex items-center gap-3 p-3 bg-[#111] border border-[#222] rounded-xl">'
            f'<img src="{eurl}" class="w-16 h-16 object-cover rounded-lg shrink-0" loading="lazy" />'
            f'<div class="flex-1 min-w-0">'
            f'<div class="text-sm text-gray-200 truncate">{ename}</div>'
            f'<div class="text-xs text-gray-500">{kb} KB</div>'
            f'</div>'
            f'<form method="post" action="/gallery_mgr/delete" onsubmit="return confirm(\'Delete this photo?\')">'
            f'{csrf}'
            f'<input type="hidden" name="filename" value="{ename}" />'
            f'<button class="text-xs text-red-400 hover:text-red-300 border border-red-900 px-3 py-1.5 rounded-lg transition-colors">Delete</button>'
            f'</form>'
            f'</div>'
        )

    rows = "".join(_gallery_row(f) for f in files) or '<p class="text-gray-500 text-sm">No photos in gallery yet.</p>'

    body = f"""
    <div class="space-y-4 max-w-3xl">
      <div class="flex items-center justify-between">
        <h1 class="text-xl font-bold text-white">Gallery Manager</h1>
        <span class="text-xs text-gray-500">{len(files)} photo(s)</span>
      </div>
      <form method="post" action="/gallery_mgr/upload" enctype="multipart/form-data"
            class="bg-[#111] border border-[#222] rounded-xl p-4 space-y-3">
        {_csrf_input()}
        <div class="text-xs text-gray-500 uppercase tracking-widest">Upload Photos</div>
        <input type="file" name="photos" multiple accept=".jpg,.jpeg,.png,.gif,.webp"
               class="block w-full text-sm text-gray-400 file:mr-3 file:py-1.5 file:px-4 file:rounded-lg file:border-0 file:bg-[#222] file:text-gray-300 file:cursor-pointer" />
        <button class="bg-[#1f6f3f] hover:bg-[#2a8f52] text-white text-sm px-4 py-2 rounded-lg transition-colors">Upload</button>
      </form>
      <div class="space-y-2">{rows}</div>
    </div>
    """
    return _render(body)


def _compress_gallery_image(path: str, max_width: int = 1920, quality: int = 82) -> None:
    """Compress/resize a gallery image in-place using Pillow."""
    try:
        from PIL import Image
        ext = os.path.splitext(path)[1].lower()
        with Image.open(path) as img:
            w, h = img.size
            if w <= max_width and os.path.getsize(path) < 150_000:
                return
            if w > max_width:
                img = img.resize((max_width, int(h * max_width / w)), Image.LANCZOS)
            exif = img.info.get("exif", b"")
            kwargs: dict = {"optimize": True}
            if ext in {".jpg", ".jpeg"}:
                kwargs["quality"] = quality
                kwargs["format"] = "JPEG"
                if exif:
                    kwargs["exif"] = exif
            elif ext == ".webp":
                kwargs["quality"] = quality
                kwargs["format"] = "WebP"
            elif ext == ".png":
                kwargs["compress_level"] = 7
                kwargs["format"] = "PNG"
            else:
                return
            img.save(path, **kwargs)
    except Exception as e:
        import logging
        logging.warning(f"[Gallery] Auto-compress failed for {path}: {e}")


@app.route("/gallery_mgr/upload", methods=["POST"])
@login_required
def gallery_mgr_upload():
    os.makedirs(GALLERY_DIR, exist_ok=True)
    files = request.files.getlist("photos")
    saved = 0
    for f in files:
        if not f or not f.filename:
            continue
        ext = os.path.splitext(f.filename)[1].lower()
        if ext not in _GALLERY_ALLOWED_EXT:
            continue
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in f.filename)
        dest = os.path.join(GALLERY_DIR, safe)
        f.save(dest)
        _compress_gallery_image(dest)
        saved += 1
    _audit_log("gallery_upload", f"count={saved}")
    return redirect(url_for("gallery_mgr"))


@app.route("/gallery_mgr/delete", methods=["POST"])
@login_required
def gallery_mgr_delete():
    fname = (request.form.get("filename") or "").strip()
    if fname and "/" not in fname and "\\" not in fname:
        target = os.path.join(GALLERY_DIR, fname)
        ext = os.path.splitext(fname)[1].lower()
        if ext in _GALLERY_ALLOWED_EXT and os.path.isfile(target):
            os.remove(target)
            _audit_log("gallery_delete", fname)
    return redirect(url_for("gallery_mgr"))


# ─────────────────────────────────────────────────────────────
# Quiz Manager
# ─────────────────────────────────────────────────────────────
def _load_quiz() -> list:
    try:
        with open(F1_QUIZ_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [q for q in data if isinstance(q, dict) and q.get("q")]
        if isinstance(data, dict) and isinstance(data.get("questions"), list):
            return [q for q in data["questions"] if isinstance(q, dict) and q.get("q")]
    except Exception:
        pass
    return []


def _save_quiz(questions: list) -> None:
    tmp = F1_QUIZ_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(questions, f, indent=2, ensure_ascii=False)
    os.replace(tmp, F1_QUIZ_PATH)
    if bot_reference and hasattr(bot_reference, "of1_quiz_save"):
        bot_reference.of1_quiz_save(questions)


@app.route("/quiz_mgr")
@login_required
def quiz_mgr():
    questions = _load_quiz()

    # Persist filters in session: use URL args when the filter form is submitted,
    # fall back to session values on redirects (after add/edit/delete).
    has_args = any(k in request.args for k in ("q", "cat", "diff"))
    if has_args:
        q_filter = (request.args.get("q") or "").lower().strip()
        cat_filter = (request.args.get("cat") or "").lower().strip()
        diff_filter = (request.args.get("diff") or "").lower().strip()
        session["quiz_filter_q"] = q_filter
        session["quiz_filter_cat"] = cat_filter
        session["quiz_filter_diff"] = diff_filter
    else:
        q_filter = session.get("quiz_filter_q", "")
        cat_filter = session.get("quiz_filter_cat", "")
        diff_filter = session.get("quiz_filter_diff", "")

    cats = sorted({str(q.get("category") or "").strip() for q in questions if q.get("category")})
    diffs = sorted({str(q.get("difficulty") or "").strip() for q in questions if q.get("difficulty")})

    shown = questions
    if q_filter:
        shown = [q for q in shown if q_filter in q.get("q", "").lower()]
    if cat_filter:
        shown = [q for q in shown if (q.get("category") or "").lower() == cat_filter]
    if diff_filter:
        shown = [q for q in shown if (q.get("difficulty") or "").lower() == diff_filter]

    cat_opts = "".join(f'<option value="{_escape(c)}" {"selected" if cat_filter == c else ""}>{_escape(c)}</option>' for c in cats)
    diff_opts = "".join(f'<option value="{_escape(d)}" {"selected" if diff_filter == d else ""}>{_escape(d)}</option>' for d in diffs)

    any_filter = bool(q_filter or cat_filter or diff_filter)
    clear_btn = (
        '<a href="/quiz_mgr/clear_filter" class="bg-[#1a1a1a] border border-[#2a2a2a] text-gray-400 text-sm '
        'rounded-lg px-3 py-1.5 hover:bg-[#222] hover:text-gray-200">&#x2715; Clear Filter</a>'
    ) if any_filter else ""

    # Undo bar
    undo_data = session.get("quiz_undo_data")
    undo_type = session.get("quiz_undo_type", "single")
    if undo_data:
        if undo_type == "bulk":
            n = len(undo_data) if isinstance(undo_data, list) else 1
            undo_label = f"{n} question{'s' if n != 1 else ''}"
        else:
            q_preview = (undo_data.get("q", "") if isinstance(undo_data, dict) else "question")[:50]
            undo_label = f"“{q_preview}”"
        undo_bar = (
            f'<div class="flex items-center gap-3 bg-[#0d0d1a] border border-[#2a2a50] rounded-xl px-4 py-2.5">'
            f'<span class="text-sm text-gray-300">Deleted {_escape(undo_label)}</span>'
            f'<form method="post" action="/quiz_mgr/undo" class="inline">{_csrf_input()}'
            f'<button class="text-sm text-blue-400 hover:text-blue-300 font-medium ml-2 underline underline-offset-2">Undo</button>'
            f'</form>'
            f'</div>'
        )
    else:
        undo_bar = ""

    # Import flash message
    import_flash = session.pop("quiz_import_flash", "")
    import_flash_html = (
        f'<div class="bg-[#111] border border-[#222] rounded-xl px-4 py-2.5 text-sm">{import_flash}</div>'
    ) if import_flash else ""

    rows = ""
    csrf = _csrf_input()
    for i, q in enumerate(questions):
        if q not in shown:
            continue
        idx = questions.index(q)
        ans = ", ".join(str(a) for a in (q.get("answers") or []))
        diff_val = _escape(q.get("difficulty") or "easy")
        diff_opts_edit = "".join(
            f'<option value="{d}" {"selected" if diff_val == d else ""}>{d.capitalize()}</option>'
            for d in ("easy", "medium", "hard")
        )
        wrong_ans_raw = q.get("wrong_answers") or []
        wrong_ans_str = _escape(", ".join(str(w) for w in wrong_ans_raw))
        has_wrong = len(wrong_ans_raw) >= 3
        wrong_badge = (
            f'<span class="ml-1 text-[10px] text-emerald-400 border border-emerald-700 rounded px-1">✓ bank</span>'
            if has_wrong else
            f'<span class="ml-1 text-[10px] text-gray-600 border border-gray-700 rounded px-1">no bank</span>'
        )
        rows += (
            f'<tr class="border-b border-[#1a1a1a] hover:bg-[#0d0d0d]" id="qrow_{idx}">'
            f'<td class="px-3 py-2"><input type="checkbox" class="q-select accent-blue-500 w-4 h-4 cursor-pointer" value="{idx}" /></td>'
            f'<td class="px-3 py-2 text-sm text-gray-200">{_escape(q.get("q",""))}{wrong_badge}</td>'
            f'<td class="hidden md:table-cell px-3 py-2 text-xs text-gray-400">{_escape(ans[:80])}</td>'
            f'<td class="hidden sm:table-cell px-3 py-2 text-xs text-gray-500">{_escape(q.get("category",""))}</td>'
            f'<td class="hidden sm:table-cell px-3 py-2 text-xs text-gray-500">{_escape(q.get("difficulty",""))}</td>'
            f'<td class="px-3 py-2 whitespace-nowrap">'
            f'<button type="button" onclick="toggleEdit({idx})" class="text-xs text-blue-400 hover:text-blue-300 mr-2">Edit</button>'
            f'<form method="post" action="/quiz_mgr/delete" class="inline">'
            f'{csrf}<input type="hidden" name="idx" value="{idx}" />'
            f'<button type="button" onclick="confirmSingleDelete(this)" class="text-xs text-red-400 hover:text-red-300">Delete</button>'
            f'</form>'
            f'</td>'
            f'</tr>'
            f'<tr id="qedit_{idx}" class="hidden bg-[#0a0a12] border-b border-[#1a1a2a]">'
            f'<td colspan="6" class="px-4 py-3">'
            f'<form method="post" action="/quiz_mgr/edit" class="grid grid-cols-1 gap-2">'
            f'{csrf}<input type="hidden" name="idx" value="{idx}" />'
            f'<textarea name="question" rows="2" class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-blue-600 resize-none w-full">{_escape(q.get("q",""))}</textarea>'
            f'<div>'
            f'<div class="text-[10px] text-gray-500 mb-1">Correct answers (comma-separated — first is the display value)</div>'
            f'<input name="answers" value="{_escape(ans)}" placeholder="e.g. 25, twenty-five"'
            f' class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-blue-600 w-full" />'
            f'</div>'
            f'<div>'
            f'<div class="text-[10px] text-gray-500 mb-1">Wrong answer bank (comma-separated — mixed with same-category answers as distractors)</div>'
            f'<input name="wrong_answers" value="{wrong_ans_str}" placeholder="e.g. 18, 15, 12"'
            f' class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-blue-600 w-full" />'
            f'</div>'
            f'<div class="flex gap-2 flex-wrap">'
            f'<input name="category" value="{_escape(q.get("category",""))}" placeholder="Category"'
            f' class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-blue-600 flex-1 min-w-[120px]" />'
            f'<select name="difficulty" class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-blue-600">{diff_opts_edit}</select>'
            f'<button class="bg-blue-700 hover:bg-blue-600 text-white text-sm px-4 py-2 rounded-lg transition-colors">Save</button>'
            f'<button type="button" onclick="toggleEdit({idx})" class="text-sm text-gray-500 hover:text-gray-300 px-3 py-2">Cancel</button>'
            f'</div>'
            f'</form>'
            f'</td>'
            f'</tr>'
        )

    body = f"""
    <!-- Confirm modal -->
    <div id="confirm-modal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4">
      <div class="bg-[#111] border border-[#333] rounded-2xl p-6 max-w-sm w-full shadow-2xl">
        <h2 class="text-white font-semibold text-base mb-2" id="modal-title">Confirm delete</h2>
        <p class="text-gray-400 text-sm mb-5" id="modal-body">Are you sure?</p>
        <div class="flex gap-3 justify-end">
          <button id="modal-cancel" class="text-sm text-gray-400 hover:text-white px-4 py-2 rounded-lg hover:bg-[#1a1a1a]">Cancel</button>
          <button id="modal-confirm" class="text-sm bg-red-700 hover:bg-red-600 text-white px-4 py-2 rounded-lg transition-colors">Delete</button>
        </div>
      </div>
    </div>

    <div class="space-y-4 max-w-5xl">
      <div class="flex items-center justify-between flex-wrap gap-3">
        <h1 class="text-xl font-bold text-white">Quiz Manager</h1>
        <span class="text-xs text-gray-500">{len(questions)} questions · {len(shown)} shown</span>
      </div>

      {undo_bar}
      {import_flash_html}

      <!-- Filters -->
      <form method="get" class="flex gap-2 flex-wrap items-end">
        <input name="q" value="{_escape(q_filter)}" placeholder="Search question…"
               class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-1.5 w-48 sm:w-56 focus:outline-none focus:border-gray-500" />
        <select name="cat" class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-1.5 focus:outline-none focus:border-gray-500">
          <option value="">All categories</option>{cat_opts}
        </select>
        <select name="diff" class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-1.5 focus:outline-none focus:border-gray-500">
          <option value="">All difficulties</option>{diff_opts}
        </select>
        <button class="bg-[#1a1a1a] border border-[#2a2a2a] text-gray-300 text-sm rounded-lg px-4 py-1.5 hover:bg-[#222]">Filter</button>
        {clear_btn}
      </form>

      <!-- Bulk action bar (shown when checkboxes are selected) -->
      <div id="bulk-bar" class="hidden items-center gap-3 bg-[#111] border border-[#333] rounded-xl px-4 py-2.5 flex-wrap">
        <span id="bulk-count" class="text-sm text-gray-300 font-medium">0 selected</span>
        <form id="bulk-delete-form" method="post" action="/quiz_mgr/delete_bulk">
          {_csrf_input()}
          <div id="bulk-indices"></div>
          <button type="button" id="bulk-delete-btn" class="bg-red-800 hover:bg-red-700 text-white text-sm px-4 py-1.5 rounded-lg transition-colors">Delete Selected</button>
        </form>
        <button type="button" onclick="clearSelection()" class="text-sm text-gray-500 hover:text-gray-300">Deselect All</button>
      </div>

      <!-- Add question -->
      <details class="bg-[#111] border border-[#222] rounded-xl">
        <summary class="px-4 py-3 text-sm text-gray-300 cursor-pointer select-none font-medium">+ Add Question</summary>
        <form method="post" action="/quiz_mgr/add" class="p-4 space-y-3 border-t border-[#222]">
          {_csrf_input()}
          <div class="grid grid-cols-1 gap-3">
            <textarea name="question" placeholder="Question text" rows="2" required
                      class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-gray-500 resize-none w-full"></textarea>
            <div>
              <div class="text-[10px] text-gray-500 mb-1">Correct answers (comma-separated — first is the display value)</div>
              <input name="answers" placeholder="e.g. 25, twenty-five" required
                     class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-gray-500 w-full" />
            </div>
            <div>
              <div class="text-[10px] text-gray-500 mb-1">Wrong answer bank (comma-separated — mixed with same-category answers as distractors)</div>
              <input name="wrong_answers" placeholder="e.g. 18, 15, 12"
                     class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-gray-500 w-full" />
            </div>
            <div class="flex gap-2 flex-wrap">
              <input name="category" placeholder="Category (e.g. circuits)"
                     class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-gray-500 flex-1 min-w-[120px]" />
              <select name="difficulty" class="bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-gray-500">
                <option value="easy">Easy</option>
                <option value="medium">Medium</option>
                <option value="hard">Hard</option>
              </select>
            </div>
          </div>
          <button class="bg-[#1f6f3f] hover:bg-[#2a8f52] text-white text-sm px-4 py-2 rounded-lg transition-colors">Add Question</button>
        </form>
      </details>

      <!-- Import questions -->
      <details class="bg-[#111] border border-[#222] rounded-xl">
        <summary class="px-4 py-3 text-sm text-gray-300 cursor-pointer select-none font-medium">⬆ Import Questions (JSON)</summary>
        <form method="post" action="/quiz_mgr/import" enctype="multipart/form-data" class="p-4 space-y-3 border-t border-[#222]">
          {_csrf_input()}
          <p class="text-xs text-gray-500">Upload a JSON file — an array of objects with <code class="font-mono bg-[#1a1a1a] px-1 rounded">q</code>, <code class="font-mono bg-[#1a1a1a] px-1 rounded">answers</code>, and optionally <code class="font-mono bg-[#1a1a1a] px-1 rounded">category</code> and <code class="font-mono bg-[#1a1a1a] px-1 rounded">difficulty</code> keys.</p>
          <input type="file" name="quiz_file" accept=".json"
                 class="text-sm text-gray-400 file:bg-[#1a1a1a] file:border file:border-[#333] file:text-gray-300 file:text-xs file:px-3 file:py-1.5 file:rounded-lg file:mr-3 file:cursor-pointer" />
          <button class="bg-[#1a3a5c] hover:bg-[#1e4a7a] text-white text-sm px-4 py-2 rounded-lg transition-colors">Import</button>
        </form>
      </details>

      <!-- Table -->
      <div class="overflow-x-auto bg-[#0a0a0a] border border-[#222] rounded-xl">
        <table class="w-full text-left">
          <thead><tr class="border-b border-[#222] text-xs text-gray-500 uppercase tracking-widest">
            <th class="px-3 py-2"><input type="checkbox" id="select-all" class="accent-blue-500 w-4 h-4 cursor-pointer" title="Select all visible" /></th>
            <th class="px-3 py-2">Question</th>
            <th class="hidden md:table-cell px-3 py-2">Answers</th>
            <th class="hidden sm:table-cell px-3 py-2">Category</th>
            <th class="hidden sm:table-cell px-3 py-2">Difficulty</th>
            <th class="px-3 py-2"></th>
          </tr></thead>
          <tbody>{rows or '<tr><td colspan="6" class="px-3 py-4 text-gray-500 text-sm">No questions match.</td></tr>'}</tbody>
        </table>
      </div>
    </div>
    <script>
    function toggleEdit(idx) {{
      const editRow = document.getElementById('qedit_' + idx);
      if (editRow) editRow.classList.toggle('hidden');
    }}

    function updateBulkBar() {{
      const checked = document.querySelectorAll('.q-select:checked');
      const bar = document.getElementById('bulk-bar');
      const count = document.getElementById('bulk-count');
      const indicesDiv = document.getElementById('bulk-indices');
      count.textContent = checked.length + ' selected';
      if (checked.length > 0) {{
        bar.classList.remove('hidden');
        bar.classList.add('flex');
        indicesDiv.innerHTML = '';
        checked.forEach(cb => {{
          const inp = document.createElement('input');
          inp.type = 'hidden';
          inp.name = 'idx';
          inp.value = cb.value;
          indicesDiv.appendChild(inp);
        }});
      }} else {{
        bar.classList.add('hidden');
        bar.classList.remove('flex');
      }}
    }}

    function clearSelection() {{
      document.querySelectorAll('.q-select').forEach(cb => cb.checked = false);
      const all = document.getElementById('select-all');
      if (all) {{ all.checked = false; all.indeterminate = false; }}
      updateBulkBar();
    }}

    // --- Confirm modal ---
    const modal = document.getElementById('confirm-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalBody = document.getElementById('modal-body');
    const modalConfirm = document.getElementById('modal-confirm');
    const modalCancel = document.getElementById('modal-cancel');
    let _pendingAction = null;

    function showModal(title, body, onConfirm) {{
      modalTitle.textContent = title;
      modalBody.textContent = body;
      _pendingAction = onConfirm;
      modal.classList.remove('hidden');
    }}
    if (modalCancel) modalCancel.addEventListener('click', function() {{ modal.classList.add('hidden'); _pendingAction = null; }});
    if (modal) modal.addEventListener('click', function(e) {{ if (e.target === modal) {{ modal.classList.add('hidden'); _pendingAction = null; }} }});
    if (modalConfirm) modalConfirm.addEventListener('click', function() {{
      modal.classList.add('hidden');
      if (_pendingAction) _pendingAction();
      _pendingAction = null;
    }});
    document.addEventListener('keydown', function(e) {{ if (e.key === 'Escape') {{ modal.classList.add('hidden'); _pendingAction = null; }} }});

    function confirmSingleDelete(btn) {{
      const form = btn.closest('form');
      showModal('Delete question?', 'This will be deleted — you can undo immediately after.', function() {{ form.submit(); }});
    }}

    const bulkDeleteBtn = document.getElementById('bulk-delete-btn');
    if (bulkDeleteBtn) {{
      bulkDeleteBtn.addEventListener('click', function() {{
        const n = document.querySelectorAll('.q-select:checked').length;
        showModal(
          'Delete ' + n + ' question' + (n !== 1 ? 's' : '') + '?',
          'These will be deleted — you can undo immediately after.',
          function() {{ document.getElementById('bulk-delete-form').submit(); }}
        );
      }});
    }}

    const selectAll = document.getElementById('select-all');
    if (selectAll) {{
      selectAll.addEventListener('change', function() {{
        document.querySelectorAll('.q-select').forEach(cb => cb.checked = this.checked);
        updateBulkBar();
      }});
    }}

    document.querySelectorAll('.q-select').forEach(cb => {{
      cb.addEventListener('change', function() {{
        const total = document.querySelectorAll('.q-select').length;
        const totalChecked = document.querySelectorAll('.q-select:checked').length;
        if (selectAll) {{
          selectAll.checked = total === totalChecked;
          selectAll.indeterminate = totalChecked > 0 && totalChecked < total;
        }}
        updateBulkBar();
      }});
    }});
    </script>
    """
    return _render(body)


@app.route("/quiz_mgr/add", methods=["POST"])
@login_required
def quiz_mgr_add():
    question = (request.form.get("question") or "").strip()
    raw_answers = (request.form.get("answers") or "").strip()
    raw_wrong = (request.form.get("wrong_answers") or "").strip()
    category = (request.form.get("category") or "").strip()
    difficulty = (request.form.get("difficulty") or "easy").strip()
    if question and raw_answers:
        answers = [a.strip() for a in raw_answers.split(",") if a.strip()]
        entry: dict = {"q": question, "answers": answers, "category": category, "difficulty": difficulty}
        if raw_wrong:
            entry["wrong_answers"] = [a.strip() for a in raw_wrong.split(",") if a.strip()]
        qs = _load_quiz()
        qs.append(entry)
        _save_quiz(qs)
        _audit_log("quiz_add", f"q={question[:60]!r}")
    return redirect(url_for("quiz_mgr"))


@app.route("/quiz_mgr/edit", methods=["POST"])
@login_required
def quiz_mgr_edit():
    try:
        idx = int(request.form.get("idx", -1))
        question = (request.form.get("question") or "").strip()
        raw_answers = (request.form.get("answers") or "").strip()
        raw_wrong = (request.form.get("wrong_answers") or "").strip()
        category = (request.form.get("category") or "").strip()
        difficulty = (request.form.get("difficulty") or "easy").strip()
        if question and raw_answers:
            answers = [a.strip() for a in raw_answers.split(",") if a.strip()]
            entry: dict = {"q": question, "answers": answers, "category": category, "difficulty": difficulty}
            if raw_wrong:
                entry["wrong_answers"] = [a.strip() for a in raw_wrong.split(",") if a.strip()]
            qs = _load_quiz()
            if 0 <= idx < len(qs):
                qs[idx] = entry
                _save_quiz(qs)
                _audit_log("quiz_edit", f"idx={idx} q={question[:60]!r}")
    except Exception:
        pass
    return redirect(url_for("quiz_mgr"))


@app.route("/quiz_mgr/delete", methods=["POST"])
@login_required
def quiz_mgr_delete():
    _csrf_protect()
    try:
        idx = int(request.form.get("idx", -1))
        qs = _load_quiz()
        if 0 <= idx < len(qs):
            removed = qs.pop(idx)
            _save_quiz(qs)
            _audit_log("quiz_delete", f"idx={idx} q={str(removed.get('q',''))[:60]!r}")
            session["quiz_undo_type"] = "single"
            session["quiz_undo_data"] = removed
            session["quiz_undo_idx"] = idx
    except Exception:
        pass
    return redirect(url_for("quiz_mgr"))


@app.route("/quiz_mgr/undo", methods=["POST"])
@login_required
def quiz_mgr_undo():
    _csrf_protect()
    undo_type = session.pop("quiz_undo_type", None)
    undo_data = session.pop("quiz_undo_data", None)
    session.pop("quiz_undo_idx", None)
    session.pop("quiz_undo_indices", None)
    try:
        if undo_type == "single" and isinstance(undo_data, dict) and undo_data.get("q"):
            qs = _load_quiz()
            qs.append(undo_data)
            _save_quiz(qs)
            _audit_log("quiz_undo_single")
        elif undo_type == "bulk" and isinstance(undo_data, list):
            qs = _load_quiz()
            qs.extend(q for q in undo_data if isinstance(q, dict) and q.get("q"))
            _save_quiz(qs)
            _audit_log("quiz_undo_bulk", f"count={len(undo_data)}")
    except Exception:
        pass
    return redirect(url_for("quiz_mgr"))


@app.route("/quiz_mgr/clear_filter")
@login_required
def quiz_mgr_clear_filter():
    session.pop("quiz_filter_q", None)
    session.pop("quiz_filter_cat", None)
    session.pop("quiz_filter_diff", None)
    return redirect(url_for("quiz_mgr"))


@app.route("/quiz_mgr/delete_bulk", methods=["POST"])
@login_required
def quiz_mgr_delete_bulk():
    _csrf_protect()
    try:
        raw = request.form.getlist("idx")
        indices = sorted({int(x) for x in raw if x.isdigit()}, reverse=True)
        if indices:
            qs = _load_quiz()
            removed = []
            for idx in indices:
                if 0 <= idx < len(qs):
                    removed.append(qs.pop(idx))
            _save_quiz(qs)
            _audit_log("quiz_delete_bulk", f"count={len(removed)}")
            session["quiz_undo_type"] = "bulk"
            session["quiz_undo_data"] = list(reversed(removed))
    except Exception:
        pass
    return redirect(url_for("quiz_mgr"))


@app.route("/quiz_mgr/import", methods=["POST"])
@login_required
def quiz_mgr_import():
    _csrf_protect()
    flash_msg = ""
    f = request.files.get("quiz_file")
    if not f or not f.filename:
        flash_msg = "No file selected."
    else:
        try:
            raw = json.load(f)
            if isinstance(raw, dict) and isinstance(raw.get("questions"), list):
                raw = raw["questions"]
            if not isinstance(raw, list):
                raise ValueError("Expected a JSON array")
            valid = [q for q in raw if isinstance(q, dict) and q.get("q") and q.get("answers")]
            if not valid:
                raise ValueError("No valid questions found (each needs 'q' and 'answers')")
            qs = _load_quiz()
            qs.extend(valid)
            _save_quiz(qs)
            _audit_log("quiz_import", f"count={len(valid)}")
            flash_msg = f"✅ Imported {len(valid)} question{'s' if len(valid) != 1 else ''}."
        except Exception as e:
            flash_msg = f"❌ Import failed: {_escape(str(e))}"
    session["quiz_import_flash"] = flash_msg
    return redirect(url_for("quiz_mgr"))


# ─────────────────────────────────────────────────────────────
# XP Manager
# ─────────────────────────────────────────────────────────────
_XP_AUDIT_LOG: list = []


def _load_xp_state_direct() -> dict:
    try:
        with open(XP_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"guilds": {}}


def _get_name_map(guild_id: str) -> dict:
    """Return {uid_str: display_name} for the guild, preferring bot cache then REST API."""
    if bot_reference and hasattr(bot_reference, "of1_member_name_map"):
        try:
            result = bot_reference.of1_member_name_map(int(guild_id))
            if result:
                return result
        except Exception:
            pass
    # Fallback: Discord REST API (handles empty member cache)
    token = _DISCORD_BOT_TOKEN_LOCAL or _DISCORD_BOT_TOKEN
    if not token or not guild_id:
        return {}
    try:
        name_map: dict = {}
        after = "0"
        while True:
            r = requests.get(
                f"https://discord.com/api/v10/guilds/{guild_id}/members",
                params={"limit": 1000, "after": after},
                headers={"Authorization": f"Bot {token}", "User-Agent": "OF1-Dashboard"},
                timeout=10,
            )
            if r.status_code != 200:
                break
            batch = r.json() or []
            if not batch:
                break
            for m in batch:
                uid = str((m.get("user") or {}).get("id") or "")
                nick = m.get("nick") or (m.get("user") or {}).get("global_name") or (m.get("user") or {}).get("username") or uid
                if uid:
                    name_map[uid] = nick
            if len(batch) < 1000:
                break
            after = str((batch[-1].get("user") or {}).get("id") or "0")
        return name_map
    except Exception:
        return {}


def _user_cell(uid: str, name_map: dict) -> str:
    """Render a user cell: display name with user ID shown on hover."""
    name = _escape(name_map.get(uid) or uid)
    title = f"User ID: {_escape(uid)}"
    cls = "cursor-help border-b border-dotted border-gray-600" if name != _escape(uid) else "font-mono"
    return f'<span class="{cls}" title="{title}">{name}</span>'


@app.route("/xp_mgr")
@login_required
def xp_mgr():
    if bot_reference and hasattr(bot_reference, "of1_xp_snapshot"):
        xp_state = bot_reference.of1_xp_snapshot()
    else:
        xp_state = _load_xp_state_direct()

    guilds = xp_state.get("guilds") or {}
    guild_tabs = ""
    tables = ""
    guild_ids = sorted(guilds.keys())

    for gid in guild_ids:
        name_map = _get_name_map(gid)
        g = guilds[gid]
        users = g.get("users") or {}
        sorted_users = sorted(users.items(), key=lambda kv: int((kv[1] or {}).get("xp", 0) or 0), reverse=True)
        guild_tabs += f'<button onclick="showGuild(\'{gid}\')" id="tab_{gid}" class="px-3 py-1.5 text-sm rounded-lg border border-[#2a2a2a] text-gray-400 hover:text-white hover:bg-[#1a1a1a]">{gid}</button>'

        def _row(uid, rec, gid=gid, name_map=name_map):
            disp = name_map.get(uid) or uid
            cell = _user_cell(uid, name_map)
            safe_gid = _escape(gid)
            safe_uid = _escape(uid)
            xp  = int((rec or {}).get("xp", 0) or 0)
            lvl = int((rec or {}).get("level", 0) or 0)
            msgs = int((rec or {}).get("messages", 0) or 0)
            search_key = _escape(f"{disp} {uid}").lower()
            return (
                f'<tr class="xp-row border-b border-[#1a1a1a] hover:bg-[#111]" data-search="{search_key}">'
                f'<td class="px-3 py-2 text-sm text-gray-300">{cell}</td>'
                f'<td class="px-3 py-2 text-sm text-gray-200">{xp:,}</td>'
                f'<td class="px-3 py-2 text-sm text-gray-400">{lvl}</td>'
                f'<td class="px-3 py-2 text-sm text-gray-500">{msgs:,}</td>'
                f'<td class="px-3 py-2"><div class="flex gap-1">'
                f'<input id="xp_amt_{safe_gid}_{safe_uid}" type="number" placeholder="±XP"'
                f' class="w-20 bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-xs rounded px-2 py-1 focus:outline-none" />'
                f'<button onclick="adjustXp(\'{safe_gid}\',\'{safe_uid}\')"'
                f' class="text-xs bg-[#1f6f3f] hover:bg-[#2a8f52] text-white px-2 py-1 rounded transition-colors">Apply</button>'
                f'</div></td>'
                f'</tr>'
            )

        user_rows = "".join(_row(uid, rec) for uid, rec in sorted_users) \
            or '<tr><td colspan="5" class="px-3 py-3 text-gray-500 text-sm">No users.</td></tr>'

        tables += (
            f'<div id="guild_{gid}" class="guild-panel hidden">'
            f'<div class="flex items-center gap-2 mb-2">'
            f'<input type="text" placeholder="Search member…" oninput="xpSearch(this)"'
            f' class="w-48 bg-[#0a0a0a] border border-[#2a2a2a] text-gray-300 text-xs rounded-lg px-3 py-1.5 focus:outline-none focus:border-gray-500" />'
            f'<span class="xp-count text-xs text-gray-600"></span>'
            f'<a href="/xp_mgr/export_csv?guild_id={gid}" download="xp_{gid}.csv"'
            f' class="ml-auto text-xs bg-[#1a1a1a] border border-[#2a2a2a] text-gray-300 px-3 py-1.5 rounded-lg hover:bg-[#222]">Export CSV</a>'
            f'</div>'
            f'<div class="overflow-x-auto bg-[#0a0a0a] border border-[#222] rounded-xl">'
            f'<table class="w-full text-left">'
            f'<thead><tr class="border-b border-[#222] text-xs text-gray-500 uppercase tracking-widest">'
            f'<th class="px-3 py-2">Member</th><th class="px-3 py-2">XP</th>'
            f'<th class="px-3 py-2">Level</th><th class="px-3 py-2">Messages</th><th class="px-3 py-2">Adjust</th>'
            f'</tr></thead>'
            f'<tbody>{user_rows}</tbody></table></div>'
            f'<p class="text-xs text-gray-600 mt-2">Hover a name to see user ID.</p>'
            f'</div>'
        )

    audit_rows = "".join(
        f'<div class="flex gap-3 text-xs py-1 border-b border-[#1a1a1a]">'
        f'<span class="text-gray-600 shrink-0">{_escape(e.get("ts",""))}</span>'
        f'<span class="text-gray-400">{_escape(e.get("user_id",""))}</span>'
        f'<span class="text-green-400">{_escape(e.get("result",""))}</span>'
        f'</div>'
        for e in reversed(_XP_AUDIT_LOG[-50:])
    ) or '<p class="text-gray-600 text-xs">No adjustments this session.</p>'

    body = f"""
    <div class="space-y-4 max-w-4xl">
      <h1 class="text-xl font-bold text-white">XP Manager</h1>
      <div class="flex gap-2 flex-wrap" id="guildTabs">{guild_tabs or '<span class="text-gray-500 text-sm">No guild data found.</span>'}</div>
      <div id="guildPanels">{tables}</div>
      <details class="bg-[#111] border border-[#222] rounded-xl">
        <summary class="px-4 py-3 text-sm text-gray-400 cursor-pointer">Audit Log (this session)</summary>
        <div class="p-4 border-t border-[#222] space-y-0.5">{audit_rows}</div>
      </details>
    </div>
    <script>
    function showGuild(gid) {{
      document.querySelectorAll('.guild-panel').forEach(el => el.classList.add('hidden'));
      document.getElementById('guild_' + gid)?.classList.remove('hidden');
    }}
    const firstGuild = document.querySelector('.guild-panel');
    if (firstGuild) firstGuild.classList.remove('hidden');

    function xpSearch(input) {{
      const kw = input.value.toLowerCase();
      const panel = input.closest('.guild-panel');
      const rows = panel ? panel.querySelectorAll('.xp-row') : [];
      let shown = 0;
      rows.forEach(function(r) {{
        const match = !kw || (r.dataset.search || '').includes(kw);
        r.style.display = match ? '' : 'none';
        if (match) shown++;
      }});
      const countEl = panel ? panel.querySelector('.xp-count') : null;
      if (countEl) countEl.textContent = kw ? shown + '/' + rows.length + ' shown' : '';
    }}

    const csrfToken = document.querySelector('meta[name="csrf-token"]').content;
    async function adjustXp(gid, uid) {{
      const amt = parseInt(document.getElementById('xp_amt_' + gid + '_' + uid)?.value || '0');
      if (!amt || isNaN(amt)) return alert('Enter a non-zero XP amount.');
      const r = await fetch('/xp_mgr/adjust', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json', 'X-CSRFToken': csrfToken}},
        body: JSON.stringify({{guild_id: gid, user_id: uid, delta: amt}})
      }});
      const d = await r.json();
      alert(d.message || (d.ok ? 'Done' : 'Error'));
      if (d.ok) location.reload();
    }}
    </script>
    """
    return _render(body)


@app.route("/xp_mgr/export_csv")
@login_required
def xp_mgr_export_csv():
    guild_id = (request.args.get("guild_id") or "").strip()
    if not guild_id:
        return "guild_id required", 400
    if bot_reference and hasattr(bot_reference, "of1_xp_snapshot"):
        xp_state = bot_reference.of1_xp_snapshot()
    else:
        xp_state = _load_xp_state_direct()
    name_map = _get_name_map(guild_id)
    guilds = xp_state.get("guilds") or {}
    users = (guilds.get(guild_id) or {}).get("users") or {}
    sorted_users = sorted(users.items(), key=lambda kv: int((kv[1] or {}).get("xp", 0) or 0), reverse=True)
    import io, csv as _csv
    buf = io.StringIO()
    w = _csv.writer(buf)
    w.writerow(["user_id", "display_name", "xp", "level", "messages"])
    for uid, rec in sorted_users:
        disp = name_map.get(uid) or uid
        xp   = int((rec or {}).get("xp", 0) or 0)
        lvl  = int((rec or {}).get("level", 0) or 0)
        msgs = int((rec or {}).get("messages", 0) or 0)
        w.writerow([uid, disp, xp, lvl, msgs])
    csv_bytes = buf.getvalue().encode("utf-8")
    from flask import Response
    return Response(
        csv_bytes,
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="xp_{guild_id}.csv"'},
    )


@app.route("/xp_mgr/adjust", methods=["POST"])
@login_required
def xp_mgr_adjust():
    data = request.get_json(silent=True) or {}
    gid = str(data.get("guild_id") or "").strip()
    uid = str(data.get("user_id") or "").strip()
    try:
        delta = int(data.get("delta") or 0)
    except Exception:
        return jsonify({"ok": False, "message": "Invalid delta"})

    if not gid or not uid or delta == 0:
        return jsonify({"ok": False, "message": "guild_id, user_id, and non-zero delta required"})

    if bot_reference and hasattr(bot_reference, "of1_xp_adjust"):
        ok, msg = bot_reference.of1_xp_adjust(int(gid), int(uid), delta)
    else:
        return jsonify({"ok": False, "message": "Bot not connected"})

    _XP_AUDIT_LOG.append({
        "ts": datetime.now(timezone.utc).strftime("%H:%M:%S"),
        "guild_id": gid,
        "user_id": uid,
        "delta": delta,
        "result": msg,
    })
    return jsonify({"ok": ok, "message": msg})


# ─────────────────────────────────────────────────────────────
# Announcement Broadcaster
# ─────────────────────────────────────────────────────────────
def _discord_list_channels(guild_id: str) -> list:
    token = _DISCORD_BOT_TOKEN_LOCAL or _DISCORD_BOT_TOKEN
    if not token or not guild_id:
        return []
    try:
        r = requests.get(
            f"https://discord.com/api/v10/guilds/{guild_id}/channels",
            headers={"Authorization": f"Bot {token}", "User-Agent": "OF1-Dashboard"},
            timeout=8,
        )
        if r.status_code == 200:
            chans = r.json() or []
            return sorted(
                [c for c in chans if isinstance(c, dict) and c.get("type") in (0, 5)],
                key=lambda c: str(c.get("name") or ""),
            )
    except Exception:
        pass
    return []


def _discord_list_roles(guild_id: str) -> list:
    token = _DISCORD_BOT_TOKEN_LOCAL or _DISCORD_BOT_TOKEN
    if not token or not guild_id:
        return []
    try:
        r = requests.get(
            f"https://discord.com/api/v10/guilds/{guild_id}/roles",
            headers={"Authorization": f"Bot {token}", "User-Agent": "OF1-Dashboard"},
            timeout=10,
        )
        if r.status_code == 200:
            roles = r.json() or []
            return sorted(
                [ro for ro in roles if isinstance(ro, dict) and not ro.get("managed") and ro.get("name") != "@everyone"],
                key=lambda ro: int(ro.get("position") or 0),
                reverse=True,
            )
    except Exception:
        pass
    return []


def _discord_send_message(channel_id: str, content: str) -> tuple:
    token = _DISCORD_BOT_TOKEN_LOCAL or _DISCORD_BOT_TOKEN
    if not token:
        return False, "No bot token configured"
    try:
        r = requests.post(
            f"https://discord.com/api/v10/channels/{channel_id}/messages",
            json={"content": content},
            headers={"Authorization": f"Bot {token}", "User-Agent": "OF1-Dashboard"},
            timeout=10,
        )
        if r.status_code in (200, 201):
            return True, "Message sent"
        return False, f"Discord returned {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, str(e)


@app.route("/announce", methods=["GET", "POST"])
@login_required
def announce():
    guild_id = _DISCORD_GUILD_ID or (os.getenv("DISCORD_GUILD_ID") or "").strip()
    channels = _discord_list_channels(guild_id)
    roles = _discord_list_roles(guild_id)
    flash_msg = ""

    if request.method == "POST":
        channel_id = (request.form.get("channel_id") or "").strip()
        content = (request.form.get("content") or "").strip()
        role_id = (request.form.get("role_id") or "").strip()
        if channel_id and content:
            if role_id:
                content = f"<@&{role_id}>\n{content}"
            ok, msg = _discord_send_message(channel_id, content)
            flash_msg = f'{"✅" if ok else "❌"} {_escape(msg)}'
            _audit_log("announce", f"channel={channel_id} ok={ok} len={len(content)} role={role_id or 'none'}")
        else:
            flash_msg = "❌ Channel and message are required."

    chan_opts = "".join(
        f'<option value="{_escape(str(c.get("id","")))}">#{_escape(str(c.get("name","")))}</option>'
        for c in channels
    ) or '<option value="">No channels found — check bot token/guild ID</option>'

    role_opts = '<option value="">None (no ping)</option>' + "".join(
        f'<option value="{_escape(str(ro.get("id","")))}">@{_escape(str(ro.get("name","")))}</option>'
        for ro in roles
    )

    body = f"""
    <div class="space-y-4 max-w-2xl">
      <h1 class="text-xl font-bold text-white">Announcement Broadcaster</h1>
      {f'<div class="bg-[#111] border border-[#222] rounded-xl px-4 py-3 text-sm">{flash_msg}</div>' if flash_msg else ''}
      <form method="post" class="bg-[#111] border border-[#222] rounded-xl p-4 space-y-3">
        {_csrf_input()}
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <div>
            <label class="block text-xs text-gray-500 uppercase tracking-widest mb-1">Channel</label>
            <select name="channel_id" class="w-full bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-gray-500">
              {chan_opts}
            </select>
          </div>
          <div>
            <label class="block text-xs text-gray-500 uppercase tracking-widest mb-1">Role Ping <span class="text-gray-600 normal-case">(optional)</span></label>
            <select name="role_id" class="w-full bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-gray-500">
              {role_opts}
            </select>
          </div>
        </div>
        <div>
          <label class="block text-xs text-gray-500 uppercase tracking-widest mb-1">Message</label>
          <textarea name="content" rows="5" placeholder="Message content (Markdown supported)…" required
                    class="w-full bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-gray-500 resize-y font-mono"></textarea>
        </div>
        <button class="bg-[#1f6f3f] hover:bg-[#2a8f52] text-white text-sm px-5 py-2 rounded-lg transition-colors font-medium">Send to Discord</button>
      </form>
      <p class="text-xs text-gray-600">Message is sent immediately as the bot to the selected channel. Supports Discord markdown. Role ping is prepended if selected.</p>
    </div>
    """
    return _render(body)


# ─────────────────────────────────────────────────────────────
# Scheduled Messages Queue
# ─────────────────────────────────────────────────────────────
def _load_scheduled_msgs() -> list:
    with _SCHEDULED_MSGS_LOCK:
        try:
            if os.path.exists(SCHEDULED_MSGS_PATH):
                with open(SCHEDULED_MSGS_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:
            pass
        return []


def _save_scheduled_msgs(msgs: list) -> None:
    with _SCHEDULED_MSGS_LOCK:
        tmp = SCHEDULED_MSGS_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(msgs, f, indent=2, ensure_ascii=False)
        os.replace(tmp, SCHEDULED_MSGS_PATH)


def _scheduled_msgs_worker() -> None:
    while True:
        try:
            time.sleep(30)
            now_ts = time.time()
            msgs = _load_scheduled_msgs()
            changed = False
            for m in msgs:
                if m.get("sent") or not m.get("channel_id") or not m.get("content"):
                    continue
                send_at = float(m.get("send_at_ts") or 0)
                if now_ts >= send_at:
                    ok, _ = _discord_send_message(str(m["channel_id"]), str(m["content"]))
                    m["sent"] = True
                    m["sent_ok"] = ok
                    m["sent_at"] = datetime.now(timezone.utc).isoformat()
                    changed = True
            if changed:
                _save_scheduled_msgs(msgs)
        except Exception:
            pass


@app.route("/schedule_msgs")
@login_required
def schedule_msgs():
    guild_id = _DISCORD_GUILD_ID or (os.getenv("DISCORD_GUILD_ID") or "").strip()
    channels = _discord_list_channels(guild_id)
    msgs = _load_scheduled_msgs()

    chan_opts = "".join(
        f'<option value="{_escape(str(c.get("id","")))}">#{_escape(str(c.get("name","")))}</option>'
        for c in channels
    ) or '<option value="">No channels found</option>'

    msg_rows = ""
    for m in reversed(msgs[-100:]):
        sent = m.get("sent", False)
        ok_badge = '<span class="text-green-400 text-xs">Sent</span>' if (sent and m.get("sent_ok")) else \
                   ('<span class="text-red-400 text-xs">Failed</span>' if (sent and not m.get("sent_ok")) else \
                    '<span class="text-yellow-400 text-xs">Pending</span>')
        send_ts = float(m.get("send_at_ts") or 0)
        send_dt = datetime.fromtimestamp(send_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        msg_rows += (
            f'<tr class="border-b border-[#1a1a1a] hover:bg-[#111]">'
            f'<td class="px-3 py-2 text-xs text-gray-500">'
            f'{_escape(send_dt)}<br>'
            f'<span class="rel-time text-gray-600" data-ts="{int(send_ts)}"></span>'
            f'</td>'
            f'<td class="px-3 py-2 text-xs text-gray-400">{_escape(str(m.get("channel_id",""))[:20])}</td>'
            f'<td class="px-3 py-2 text-sm text-gray-300 max-w-xs truncate">{_escape((m.get("content") or "")[:80])}</td>'
            f'<td class="px-3 py-2">{ok_badge}</td>'
            f'<td class="px-3 py-2">'
            + ('' if sent else
               f'<form method="post" action="/schedule_msgs/cancel" class="inline">'
               f'{_csrf_input()}<input type="hidden" name="msg_id" value="{_escape(str(m.get("id","")))}"/>'
               f'<button class="text-xs text-red-400 hover:text-red-300">Cancel</button></form>')
            + '</td></tr>'
        )

    body = f"""
    <div class="space-y-4 max-w-4xl">
      <h1 class="text-xl font-bold text-white">Scheduled Messages</h1>
      <form method="post" action="/schedule_msgs/add" class="bg-[#111] border border-[#222] rounded-xl p-4 space-y-3">
        {_csrf_input()}
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <div>
            <label class="block text-xs text-gray-500 uppercase tracking-widest mb-1">Channel</label>
            <select name="channel_id" class="w-full bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-gray-500">{chan_opts}</select>
          </div>
          <div>
            <label class="block text-xs text-gray-500 uppercase tracking-widest mb-1">Send At (UTC)</label>
            <input name="send_at" type="datetime-local" required
                   class="w-full bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-gray-500" />
          </div>
        </div>
        <textarea name="content" rows="3" placeholder="Message content…" required
                  class="w-full bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-gray-500 resize-y font-mono"></textarea>
        <button class="bg-[#1f6f3f] hover:bg-[#2a8f52] text-white text-sm px-5 py-2 rounded-lg transition-colors">Schedule</button>
      </form>
      <div class="overflow-x-auto bg-[#0a0a0a] border border-[#222] rounded-xl">
        <table class="w-full text-left">
          <thead><tr class="border-b border-[#222] text-xs text-gray-500 uppercase tracking-widest">
            <th class="px-3 py-2">Scheduled For</th><th class="px-3 py-2">Channel</th>
            <th class="px-3 py-2">Message</th><th class="px-3 py-2">Status</th><th class="px-3 py-2"></th>
          </tr></thead>
          <tbody>{msg_rows or '<tr><td colspan="5" class="px-3 py-4 text-gray-500 text-sm">No scheduled messages.</td></tr>'}</tbody>
        </table>
      </div>
      <p class="text-xs text-gray-600">Worker checks every 30 seconds and sends any due messages.</p>
    </div>
    <script>
    (function(){{
      function relTime(ts) {{
        const diff = Math.round((ts * 1000 - Date.now()) / 1000);
        const abs = Math.abs(diff);
        const past = diff < 0;
        if (abs < 60)   return past ? 'just now' : 'in <1 min';
        if (abs < 3600) return (past?'':'in ') + Math.round(abs/60) + ' min' + (past?' ago':'');
        if (abs < 86400)return (past?'':'in ') + Math.round(abs/3600) + ' hr' + (past?' ago':'');
        return (past?'':'in ') + Math.round(abs/86400) + ' day' + (Math.round(abs/86400)!==1?'s':'') + (past?' ago':'');
      }}
      function updateRelTimes() {{
        document.querySelectorAll('.rel-time[data-ts]').forEach(function(el) {{
          el.textContent = relTime(parseInt(el.dataset.ts));
        }});
      }}
      updateRelTimes();
      setInterval(updateRelTimes, 30000);
    }})();
    </script>
    """
    return _render(body)


@app.route("/schedule_msgs/add", methods=["POST"])
@login_required
def schedule_msgs_add():
    channel_id = (request.form.get("channel_id") or "").strip()
    content = (request.form.get("content") or "").strip()
    send_at_raw = (request.form.get("send_at") or "").strip()
    if channel_id and content and send_at_raw:
        try:
            dt = datetime.fromisoformat(send_at_raw).replace(tzinfo=timezone.utc)
            msgs = _load_scheduled_msgs()
            import uuid
            msgs.append({
                "id": str(uuid.uuid4())[:8],
                "channel_id": channel_id,
                "content": content,
                "send_at_ts": dt.timestamp(),
                "send_at_iso": dt.isoformat(),
                "sent": False,
                "created_at": datetime.now(timezone.utc).isoformat(),
            })
            _save_scheduled_msgs(msgs)
        except Exception:
            pass
    return redirect(url_for("schedule_msgs"))


@app.route("/schedule_msgs/cancel", methods=["POST"])
@login_required
def schedule_msgs_cancel():
    msg_id = (request.form.get("msg_id") or "").strip()
    if msg_id:
        msgs = _load_scheduled_msgs()
        msgs = [m for m in msgs if str(m.get("id") or "") != msg_id]
        _save_scheduled_msgs(msgs)
    return redirect(url_for("schedule_msgs"))


# ─────────────────────────────────────────────────────────────
# Predictions Read-Only View
# ─────────────────────────────────────────────────────────────
@app.route("/predictions")
@login_required
def predictions_view():
    state = load_state() or {}
    pred_data = state.get("predictions") or {}
    rounds = pred_data.get("rounds") or {}
    totals_root = pred_data.get("totals") or {}

    # Find the most recent/active round
    guild_id = None
    for rnd_key, rnd_obj in rounds.items():
        entries = rnd_obj.get("entries") or {}
        for gid in entries:
            guild_id = gid
            break
        if guild_id:
            break
    if not guild_id:
        # Try totals
        if totals_root:
            guild_id = next(iter(totals_root))

    name_map = _get_name_map(guild_id) if guild_id else {}

    CAT_LABELS = [
        ("pole",           "Pole"),
        ("podium",         "Podium"),
        ("p10",            "P10"),
        ("sprint_pole",    "SP Pole"),
        ("sprint_podium",  "SP Podium"),
        ("sprint_p8",      "SP P8"),
    ]

    round_sections = []
    for rnd_key in sorted(rounds.keys(), reverse=True):
        rnd_obj = rounds[rnd_key]
        race_name = rnd_obj.get("race_name") or rnd_key
        actual = rnd_obj.get("actual") or {}
        scored = bool(rnd_obj.get("scored"))
        guild_entries = (rnd_obj.get("entries") or {}).get(guild_id or "", {})
        if not guild_entries:
            continue

        # Which cats have actual results set
        cats_with_actuals = {cat for cat, _ in CAT_LABELS if actual.get(cat)}

        user_rows = ""
        for uid, entry in sorted(guild_entries.items(), key=lambda kv: kv[0]):
            display = name_map.get(uid) or f"<{uid}>"
            cells = ""
            for cat, label in CAT_LABELS:
                val = entry.get(cat)
                if val is None:
                    cells += f'<td class="px-3 py-2 text-sm text-gray-600">—</td>'
                    continue
                if isinstance(val, list):
                    pick_str = " / ".join(str(x) for x in val)
                else:
                    pick_str = str(val)
                # Check correctness if actuals available
                style = "text-gray-300"
                badge = ""
                if cat in cats_with_actuals:
                    actual_val = actual.get(cat)
                    if isinstance(actual_val, list):
                        correct = any(str(v).lower() in pick_str.lower() for v in actual_val)
                    else:
                        correct = str(actual_val or "").lower() in pick_str.lower()
                    style = "text-green-400" if correct else "text-red-400"
                cells += f'<td class="px-3 py-2 text-sm {style} font-medium">{_escape(pick_str)}</td>'

            user_rows += (
                f'<tr class="border-b border-[#1a1a1a] hover:bg-[#0f0f0f]">'
                f'<td class="px-3 py-2 text-sm text-gray-300">{_escape(display)}</td>'
                f'{cells}'
                f'</tr>'
            )

        # Actual row if scored
        actual_row = ""
        if cats_with_actuals:
            actual_cells = ""
            for cat, label in CAT_LABELS:
                av = actual.get(cat)
                if av is None:
                    actual_cells += f'<td class="px-3 py-2 text-sm text-gray-600">—</td>'
                elif isinstance(av, list):
                    actual_cells += f'<td class="px-3 py-2 text-sm text-yellow-300 font-semibold">{_escape(" / ".join(str(x) for x in av))}</td>'
                else:
                    actual_cells += f'<td class="px-3 py-2 text-sm text-yellow-300 font-semibold">{_escape(str(av))}</td>'
            actual_row = (
                f'<tr class="border-b border-[#333] bg-[#0d0d00]">'
                f'<td class="px-3 py-2 text-xs text-yellow-500 font-bold uppercase tracking-wider">Actual</td>'
                f'{actual_cells}'
                f'</tr>'
            )

        col_headers = "".join(
            f'<th class="px-3 py-2 text-left text-xs text-gray-500 uppercase tracking-wider">{label}</th>'
            for _, label in CAT_LABELS
        )

        scored_badge = '<span class="text-xs text-green-400 ml-2">● Scored</span>' if scored else ""
        round_sections.append(
            f'<div class="bg-[#0a0a0a] border border-[#222] rounded-xl overflow-hidden">'
            f'<div class="px-4 py-3 border-b border-[#222] text-sm font-semibold text-gray-200">'
            f'{_escape(race_name)}{scored_badge}'
            f'</div>'
            f'<div class="overflow-x-auto">'
            f'<table class="w-full text-left">'
            f'<thead><tr class="border-b border-[#1a1a1a]">'
            f'<th class="px-3 py-2 text-left text-xs text-gray-500 uppercase tracking-wider">User</th>'
            f'{col_headers}'
            f'</tr></thead>'
            f'<tbody>'
            f'{actual_row}'
            f'{user_rows}'
            f'</tbody></table></div></div>'
        )

    # Totals leaderboard
    totals_rows = ""
    totals_gid_data = totals_root.get(guild_id or "", {}) if guild_id else {}
    if totals_gid_data:
        sorted_totals = sorted(totals_gid_data.items(), key=lambda kv: int(kv[1] or 0), reverse=True)
        for rank, (uid, pts) in enumerate(sorted_totals, 1):
            display = name_map.get(uid) or f"<{uid}>"
            totals_rows += (
                f'<tr class="border-b border-[#1a1a1a] hover:bg-[#111]">'
                f'<td class="px-3 py-2 text-sm text-gray-500">#{rank}</td>'
                f'<td class="px-3 py-2 text-sm text-gray-300">{_escape(display)}</td>'
                f'<td class="px-3 py-2 text-sm text-green-400 font-semibold">{int(pts or 0)} pts</td>'
                f'</tr>'
            )

    totals_table = (
        f'<div class="bg-[#0a0a0a] border border-[#222] rounded-xl overflow-hidden">'
        f'<div class="px-4 py-3 border-b border-[#222] text-sm font-semibold text-gray-200">Overall Leaderboard</div>'
        f'<table class="w-full text-left"><thead>'
        f'<tr class="border-b border-[#222] text-xs text-gray-500 uppercase tracking-widest">'
        f'<th class="px-3 py-2">#</th><th class="px-3 py-2">User</th><th class="px-3 py-2">Points</th>'
        f'</tr></thead><tbody>'
        f'{totals_rows or _empty_row(3, "No scores yet.")}'
        f'</tbody></table></div>'
    )

    # Build round key options for the set-result form
    round_options = "".join(
        f'<option value="{_escape(rk)}">{_escape((rounds[rk].get("race_name") or rk))}</option>'
        for rk in sorted(rounds.keys(), reverse=True)
        if rounds[rk].get("entries")
    )

    csrf = _csrf_token()
    set_result_form = f"""
    <div class="bg-[#0a0a0a] border border-[#222] rounded-xl overflow-hidden">
      <div class="px-4 py-3 border-b border-[#222] text-sm font-semibold text-gray-200">Set Actual Results</div>
      <div class="p-4 space-y-3">
        <meta name="pred-csrf" content="{csrf}" />
        <div>
          <label class="block text-xs text-gray-500 uppercase tracking-wider mb-1">Round</label>
          <select id="pr-round" class="w-full bg-[#111] border border-[#333] text-gray-200 text-sm rounded-lg px-3 py-2">
            {round_options or '<option value="">No rounds</option>'}
          </select>
        </div>
        <div>
          <label class="block text-xs text-gray-500 uppercase tracking-wider mb-1">Category</label>
          <select id="pr-cat" class="w-full bg-[#111] border border-[#333] text-gray-200 text-sm rounded-lg px-3 py-2">
            <option value="pole">Pole</option>
            <option value="p10">P10</option>
            <option value="podium">Podium (P1 | P2 | P3)</option>
            <option value="sprint_pole">Sprint Pole</option>
            <option value="sprint_p8">Sprint P8</option>
            <option value="sprint_podium">Sprint Podium (P1 | P2 | P3)</option>
          </select>
        </div>
        <div>
          <label class="block text-xs text-gray-500 uppercase tracking-wider mb-1">Value</label>
          <input id="pr-value" type="text" placeholder="Verstappen  or  Verstappen | Norris | Leclerc"
            class="w-full bg-[#111] border border-[#333] text-gray-200 text-sm rounded-lg px-3 py-2 outline-none" />
          <div class="text-xs text-gray-600 mt-1">For podium categories use | to separate 3 names.</div>
        </div>
        <button onclick="prSubmit()"
          class="w-full bg-[#1a3a1a] hover:bg-[#2a5a2a] border border-[#2a8f52] text-green-300 text-sm font-semibold py-2 rounded-lg transition-colors">
          Save Result
        </button>
        <div id="pr-msg" class="hidden text-sm px-3 py-2 rounded-lg"></div>
      </div>
    </div>
    <script>
    (function(){{
      window.prSubmit = async function() {{
        const round = document.getElementById('pr-round').value;
        const cat   = document.getElementById('pr-cat').value;
        const val   = document.getElementById('pr-value').value.trim();
        const msg   = document.getElementById('pr-msg');
        if (!round) return prMsg('No round selected.', false);
        if (!val)   return prMsg('Enter a value.', false);
        const csrf  = document.querySelector('meta[name="pred-csrf"]').content;
        const fd = new FormData();
        fd.append('_csrf', csrf);
        fd.append('round_key', round);
        fd.append('category', cat);
        fd.append('value', val);
        try {{
          const r = await fetch('/predictions/set_result', {{method:'POST', body:fd, credentials:'same-origin'}});
          const d = await r.json();
          prMsg(d.ok ? ('✅ ' + d.message) : ('❌ ' + d.error), d.ok);
          if (d.ok) setTimeout(() => location.reload(), 1200);
        }} catch(e) {{
          prMsg('❌ ' + e.message, false);
        }}
      }};
      function prMsg(text, ok) {{
        const el = document.getElementById('pr-msg');
        el.textContent = text;
        el.className = 'text-sm px-3 py-2 rounded-lg ' + (ok ? 'bg-[#0d3320] text-green-300 border border-[#2a8f52]' : 'bg-[#3a0d0d] text-red-300 border border-[#8f2a2a]');
        el.classList.remove('hidden');
      }}
    }})();
    </script>
    """

    content = f"""
    <div class="space-y-4 max-w-5xl">
      <h1 class="text-xl font-bold text-white">Predictions</h1>
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-4 items-start">
        <div class="lg:col-span-2 space-y-4">
          {''.join(round_sections) or '<div class="text-gray-500 text-sm">No prediction rounds found.</div>'}
        </div>
        <div class="space-y-4">
          {totals_table}
          {set_result_form}
        </div>
      </div>
    </div>
    """
    return _render(content)


@app.route("/predictions/set_result", methods=["POST"])
@login_required
def predictions_set_result():
    _csrf_protect()
    round_key = (request.form.get("round_key") or "").strip()
    category  = (request.form.get("category") or "").strip().lower()
    raw_value = (request.form.get("value") or "").strip()

    valid_cats = {"pole", "p10", "sprint_pole", "sprint_p8", "podium", "sprint_podium"}
    if not round_key:
        return jsonify({"ok": False, "error": "round_key is required"}), 400
    if category not in valid_cats:
        return jsonify({"ok": False, "error": f"Invalid category '{category}'"}), 400
    if not raw_value:
        return jsonify({"ok": False, "error": "value is required"}), 400

    # Podium categories: parse "A | B | C"
    if category in {"podium", "sprint_podium"}:
        parts = [p.strip() for p in raw_value.split("|")]
        parts = [p for p in parts if p]
        if len(parts) != 3:
            return jsonify({"ok": False, "error": "Podium requires exactly 3 names separated by |"}), 400
        value = parts
    else:
        value = raw_value

    if bot_reference and hasattr(bot_reference, "of1_pred_set_result"):
        ok, msg = bot_reference.of1_pred_set_result(round_key, category, value)
    else:
        # Fallback: write directly to state file
        try:
            from storage import load_state, save_state
            state = load_state() or {}
            rounds = (state.setdefault("predictions", {})).setdefault("rounds", {})
            rnd = rounds.setdefault(round_key, {"locked": False, "race_name": None, "actual": {}, "entries": {}, "scored": False})
            rnd.setdefault("actual", {})[category] = value
            rnd["scored"] = False
            save_state(state)
            ok, msg = True, f"Set {category} = {value!r}"
        except Exception as e:
            ok, msg = False, str(e)

    if ok:
        return jsonify({"ok": True, "message": msg})
    return jsonify({"ok": False, "error": msg}), 400


# ─────────────────────────────────────────────────────────────
# Member Stats Panel
# ─────────────────────────────────────────────────────────────
@app.route("/stats")
@login_required
def member_stats():
    if bot_reference and hasattr(bot_reference, "of1_xp_snapshot"):
        xp_state = bot_reference.of1_xp_snapshot()
    else:
        xp_state = _load_xp_state_direct()

    state = load_state() or {}
    quiz_root = state.get("quiz_scores") or {}
    pred_root = (state.get("predictions") or {}).get("totals") or {}

    guilds = xp_state.get("guilds") or {}
    guild_id = list(guilds.keys())[0] if guilds else None
    name_map = _get_name_map(guild_id) if guild_id else {}

    # XP leaderboard
    xp_rows = ""
    if guild_id:
        users = (guilds.get(guild_id) or {}).get("users") or {}
        top_xp = sorted(users.items(), key=lambda kv: int((kv[1] or {}).get("xp", 0) or 0), reverse=True)[:15]
        for rank, (uid, rec) in enumerate(top_xp, 1):
            xp = int((rec or {}).get("xp", 0) or 0)
            lvl = int((rec or {}).get("level", 0) or 0)
            msgs = int((rec or {}).get("messages", 0) or 0)
            xp_rows += (
                f'<tr class="border-b border-[#1a1a1a] hover:bg-[#111]">'
                f'<td class="px-3 py-2 text-sm text-gray-500">#{rank}</td>'
                f'<td class="px-3 py-2 text-sm text-gray-300">{_user_cell(uid, name_map)}</td>'
                f'<td class="px-3 py-2 text-sm text-yellow-400 font-semibold">{xp:,}</td>'
                f'<td class="px-3 py-2 text-sm text-gray-400">Lv {lvl}</td>'
                f'<td class="px-3 py-2 text-sm text-gray-500">{msgs:,} msgs</td>'
                f'</tr>'
            )

    # Quiz leaderboard
    quiz_gid = list(quiz_root.keys())[0] if quiz_root else None
    quiz_name_map = _get_name_map(quiz_gid) if quiz_gid else name_map
    quiz_rows = ""
    if quiz_gid:
        quiz_scores = quiz_root.get(quiz_gid) or {}
        top_quiz = sorted(quiz_scores.items(), key=lambda kv: int(kv[1] or 0), reverse=True)[:10]
        for rank, (uid, pts) in enumerate(top_quiz, 1):
            quiz_rows += (
                f'<tr class="border-b border-[#1a1a1a] hover:bg-[#111]">'
                f'<td class="px-3 py-2 text-sm text-gray-500">#{rank}</td>'
                f'<td class="px-3 py-2 text-sm text-gray-300">{_user_cell(uid, quiz_name_map)}</td>'
                f'<td class="px-3 py-2 text-sm text-blue-400 font-semibold">{int(pts or 0):,} pts</td>'
                f'</tr>'
            )

    # Prediction leaderboard
    pred_gid = list(pred_root.keys())[0] if pred_root else None
    pred_name_map = _get_name_map(pred_gid) if pred_gid else name_map
    pred_rows = ""
    if pred_gid:
        pred_scores = pred_root.get(pred_gid) or {}
        top_pred = sorted(pred_scores.items(), key=lambda kv: int(kv[1] or 0), reverse=True)[:10]
        for rank, (uid, pts) in enumerate(top_pred, 1):
            pred_rows += (
                f'<tr class="border-b border-[#1a1a1a] hover:bg-[#111]">'
                f'<td class="px-3 py-2 text-sm text-gray-500">#{rank}</td>'
                f'<td class="px-3 py-2 text-sm text-gray-300">{_user_cell(uid, pred_name_map)}</td>'
                f'<td class="px-3 py-2 text-sm text-green-400 font-semibold">{int(pts or 0):,} pts</td>'
                f'</tr>'
            )

    def _table(title: str, headers: list, rows: str, empty: str = "No data") -> str:
        ths = "".join(f'<th class="px-3 py-2">{h}</th>' for h in headers)
        ncols = len(headers)
        empty_row = f'<tr><td colspan="{ncols}" class="px-3 py-4 text-gray-500 text-sm">{empty}</td></tr>'
        return (
            f'<div class="bg-[#0a0a0a] border border-[#222] rounded-xl overflow-hidden">'
            f'<div class="px-4 py-3 border-b border-[#222] text-sm font-semibold text-gray-300">{title}</div>'
            f'<table class="w-full text-left"><thead>'
            f'<tr class="border-b border-[#222] text-xs text-gray-500 uppercase tracking-widest">{ths}</tr>'
            f'</thead><tbody>'
            f'{rows or empty_row}'
            f'</tbody></table></div>'
        )

    body = f"""
    <div class="space-y-4 max-w-5xl">
      <h1 class="text-xl font-bold text-white">Member Stats</h1>
      <p class="text-xs text-gray-500">Guild: {_escape(guild_id or "none")} — hover a name to see their user ID</p>
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {_table("XP Leaderboard", ["#", "User ID", "XP", "Level", "Messages"], xp_rows)}
        {_table("Quiz Leaderboard", ["#", "User ID", "Score"], quiz_rows)}
        {_table("Predictions Leaderboard", ["#", "User ID", "Points"], pred_rows)}
      </div>
    </div>
    """
    return _render(body)


# ─────────────────────────────────────────────────────────────
# Bot Command Log
# ─────────────────────────────────────────────────────────────
@app.route("/cmd_log")
@login_required
def cmd_log():
    from runtime_store import list_cmd_log
    entries = list_cmd_log(200)

    rows = "".join(
        f'<tr class="border-b border-[#1a1a1a] hover:bg-[#111]">'
        f'<td class="px-3 py-1.5 text-xs text-gray-600 whitespace-nowrap">{_escape(str(e.get("ts",""))[:19].replace("T"," "))}</td>'
        f'<td class="px-3 py-1.5 text-xs text-yellow-400 font-mono">{_escape(str(e.get("command","?")))}</td>'
        f'<td class="px-3 py-1.5 text-xs text-gray-300">{_escape(str(e.get("user","")))}</td>'
        f'<td class="px-3 py-1.5 text-xs text-gray-500">{_escape(str(e.get("guild","")))}</td>'
        f'<td class="px-3 py-1.5 text-xs text-gray-600 font-mono max-w-xs truncate">{_escape(str(e.get("full",""))[:100])}</td>'
        f'</tr>'
        for e in entries
    ) or '<tr><td colspan="5" class="px-3 py-4 text-gray-500 text-sm">No commands logged yet. Commands are logged while the bot is running.</td></tr>'

    body = f"""
    <div class="space-y-4 max-w-5xl">
      <div class="flex items-center justify-between">
        <h1 class="text-xl font-bold text-white">Command Log</h1>
        <div class="flex gap-2">
          <span class="text-xs text-gray-500">{len(entries)} entries</span>
          <button onclick="location.reload()" class="text-xs bg-[#1a1a1a] border border-[#222] text-gray-300 px-3 py-1 rounded-lg hover:bg-[#222]">Refresh</button>
        </div>
      </div>
      <div class="overflow-x-auto bg-[#0a0a0a] border border-[#222] rounded-xl">
        <table class="w-full text-left">
          <thead><tr class="border-b border-[#222] text-xs text-gray-500 uppercase tracking-widest">
            <th class="px-3 py-2">Time (UTC)</th>
            <th class="px-3 py-2">Command</th>
            <th class="px-3 py-2">User</th>
            <th class="px-3 py-2">Server</th>
            <th class="px-3 py-2">Full Input</th>
          </tr></thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
      <p class="text-xs text-gray-600">In-memory only — cleared on bot restart. Shows last 200 commands.</p>
    </div>
    <script>setTimeout(() => location.reload(), 15000);</script>
    """
    return _render(body)


# ─────────────────────────────────────────────────────────────
# OpenF1 API Health
# ─────────────────────────────────────────────────────────────
def _render_website_cache_section(data: dict) -> str:
    if not data.get("ok"):
        return '<p class="text-xs text-gray-600">Website cache unavailable — is the website process running?</p>'
    func_entries = data.get("func_cache") or []
    raw_count = int(data.get("raw_cache_count") or 0)
    func_rows = ""
    for e in func_entries:
        age = int(e.get("age_s") or 0)
        ttl = int(e.get("ttl_s") or 300)
        pct = min(100, int(age * 100 / ttl)) if ttl else 0
        bar_color = "bg-green-700" if pct < 60 else ("bg-yellow-600" if pct < 90 else "bg-red-700")
        has_data = bool(e.get("has_data"))
        func_rows += (
            f'<tr class="border-b border-[#1a1a1a]">'
            f'<td class="px-3 py-2 text-xs font-mono text-gray-300">{_escape(str(e.get("key","")))}</td>'
            f'<td class="px-3 py-2 text-xs text-gray-500">{age}s / {ttl}s</td>'
            f'<td class="px-3 py-2 w-24"><div class="h-1.5 rounded-full bg-[#222]"><div class="{bar_color} h-1.5 rounded-full" style="width:{pct}%"></div></div></td>'
            f'<td class="px-3 py-2 text-xs {"text-green-400" if has_data else "text-gray-600"}">'
            f'{"cached" if has_data else "empty"}</td>'
            f'</tr>'
        )
    return (
        f'<div class="overflow-x-auto bg-[#0a0a0a] border border-[#222] rounded-xl">'
        f'<table class="w-full text-left"><thead>'
        f'<tr class="border-b border-[#222] text-xs text-gray-500 uppercase tracking-widest">'
        f'<th class="px-3 py-2">Key</th><th class="px-3 py-2">Age / TTL</th>'
        f'<th class="px-3 py-2">Freshness</th><th class="px-3 py-2">State</th>'
        f'</tr></thead><tbody>'
        f'{func_rows or _empty_row(4, "No entries")}'
        f'</tbody></table></div>'
        f'<p class="text-xs text-gray-600">High-level cache entries. Raw OpenF1 response cache: {raw_count} entries.</p>'
    )


def _fetch_website_cache_stats() -> dict:
    """Fetch cache stats from the website process (localhost only)."""
    website_port = int(os.getenv("WEBSITE_PORT", "5001"))
    try:
        r = requests.get(f"http://127.0.0.1:{website_port}/api/cache_stats", timeout=3)
        if r.status_code == 200:
            return r.json() or {}
    except Exception:
        pass
    return {}


@app.route("/openf1_health")
@login_required
def openf1_health():
    if bot_reference and hasattr(bot_reference, "of1_openf1_health_snapshot"):
        trace = bot_reference.of1_openf1_health_snapshot()
    else:
        trace = {}

    website_cache = _fetch_website_cache_stats()

    window_start = float(trace.get("window_start") or 0)
    window_age_s = int(time.time() - window_start) if window_start else 0
    endpoints = trace.get("endpoints") or {}
    total_calls = sum(int((v or {}).get("calls", 0)) for v in endpoints.values() if isinstance(v, dict))

    endpoint_rows = ""
    for ep, stats in sorted(endpoints.items()):
        if not isinstance(stats, dict):
            continue
        calls = int(stats.get("calls", 0) or 0)
        errors = int(stats.get("errors", 0) or 0)
        avg_ms = int(stats.get("avg_ms", 0) or 0)
        last_status = int(stats.get("last_status", 0) or 0)
        status_cls = "text-green-400" if 200 <= last_status < 300 else ("text-yellow-400" if last_status == 0 else "text-red-400")
        err_cls = "text-red-400" if errors else "text-gray-500"
        endpoint_rows += (
            f'<tr class="border-b border-[#1a1a1a] hover:bg-[#111]">'
            f'<td class="px-3 py-2 text-sm font-mono text-gray-300">{_escape(ep)}</td>'
            f'<td class="px-3 py-2 text-sm text-gray-200">{calls}</td>'
            f'<td class="px-3 py-2 text-sm {err_cls}">{errors}</td>'
            f'<td class="px-3 py-2 text-sm text-gray-400">{avg_ms} ms</td>'
            f'<td class="px-3 py-2 text-sm {status_cls}">{last_status or "—"}</td>'
            f'</tr>'
        )

    no_data_note = (
        'No API calls recorded in this 60-second window yet — '
        'trigger any bot F1 command, then refresh.'
    ) if window_start else 'Bot not connected or no data yet.'

    body = f"""
    <div class="space-y-4 max-w-3xl">
      <div class="flex items-center justify-between">
        <h1 class="text-xl font-bold text-white">OpenF1 API Health</h1>
        <button onclick="location.reload()" class="text-xs bg-[#1a1a1a] border border-[#222] text-gray-300 px-3 py-1 rounded-lg hover:bg-[#222]">Refresh</button>
      </div>
      <div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
        <div class="bg-[#111] border border-[#222] rounded-xl p-4">
          <div class="text-xs text-gray-500 uppercase tracking-widest mb-1">Window Age</div>
          <div class="text-2xl font-bold text-white">{window_age_s}s <span class="text-sm text-gray-500">/ 60s</span></div>
        </div>
        <div class="bg-[#111] border border-[#222] rounded-xl p-4">
          <div class="text-xs text-gray-500 uppercase tracking-widest mb-1">Endpoints Hit</div>
          <div class="text-2xl font-bold text-white">{len(endpoints)}</div>
        </div>
        <div class="bg-[#111] border border-[#222] rounded-xl p-4">
          <div class="text-xs text-gray-500 uppercase tracking-widest mb-1">Total Calls</div>
          <div class="text-2xl font-bold text-white">{total_calls}</div>
        </div>
      </div>
      <div class="overflow-x-auto bg-[#0a0a0a] border border-[#222] rounded-xl">
        <table class="w-full text-left">
          <thead><tr class="border-b border-[#222] text-xs text-gray-500 uppercase tracking-widest">
            <th class="px-3 py-2">Endpoint</th>
            <th class="px-3 py-2">Calls</th>
            <th class="px-3 py-2">Errors</th>
            <th class="px-3 py-2">Avg Latency</th>
            <th class="px-3 py-2">Last Status</th>
          </tr></thead>
          <tbody>{endpoint_rows or f'<tr><td colspan="5" class="px-3 py-4 text-gray-500 text-sm">{_escape(no_data_note)}</td></tr>'}</tbody>
        </table>
      </div>
      <p class="text-xs text-gray-600">Trace window resets every 60 seconds — counters reflect the current window only.</p>

      <!-- Website cache stats -->
      <h2 class="text-base font-semibold text-white pt-2">Website Cache</h2>
      {_render_website_cache_section(website_cache)}
    </div>
    <script>setTimeout(() => location.reload(), 20000);</script>
    """
    return _render(body)


# ─────────────────────────────────────────────────────────────
# Audit Log viewer
# ─────────────────────────────────────────────────────────────
@app.route("/audit_log")
@login_required
def audit_log():
    tail = request.args.get("tail", "200")
    try:
        tail_n = max(50, min(2000, int(tail)))
    except Exception:
        tail_n = 200
    try:
        with open(AUDIT_LOG_PATH, "r", encoding="utf-8") as f:
            raw_lines = f.readlines()[-tail_n:]
    except FileNotFoundError:
        raw_lines = []
    except Exception:
        raw_lines = []

    # Extract unique action types from lines like: [ts] user | action | detail
    action_types: list = []
    seen: set = set()
    for line in raw_lines:
        parts = line.split("|")
        if len(parts) >= 2:
            action = parts[1].strip()
            if action and action not in seen:
                seen.add(action)
                action_types.append(action)

    safe = _escape("".join(raw_lines)) if raw_lines else "No audit log yet — actions will appear here after the first admin action."

    action_btns = "".join(
        f'<button class="al-type-btn active text-xs px-2 py-1 rounded border border-[#333] bg-[#1a1a1a] text-gray-300 hover:bg-[#222]" data-action="{_escape(a)}">{_escape(a)}</button>'
        for a in action_types
    ) if action_types else ""

    filter_row = f"""
    <div class="flex flex-wrap items-center gap-2">
      <input id="alSearch" type="text" placeholder="Search…"
        class="w-44 bg-[#0a0a0a] border border-[#2a2a2a] text-gray-300 text-xs rounded-lg px-3 py-1.5 focus:outline-none focus:border-gray-500" />
      <button id="alClear" class="text-xs text-gray-600 hover:text-gray-300 hidden">&#x2715; Clear</button>
      {('<span class="text-xs text-gray-600 mr-1">Action:</span>' + action_btns) if action_btns else ''}
      <span id="alCount" class="text-xs text-gray-600 ml-auto"></span>
    </div>""" if action_btns else ""

    body = f"""
    <div class="space-y-4 max-w-4xl">
      <div class="flex items-center justify-between flex-wrap gap-3">
        <h1 class="text-xl font-bold text-white">Audit Log</h1>
        <form method="get" class="flex items-center gap-2">
          <label class="text-xs text-gray-500">Lines</label>
          <input name="tail" value="{tail_n}"
            class="w-20 bg-[#0a0a0a] border border-[#2a2a2a] text-gray-200 text-sm rounded-lg px-3 py-1.5 focus:outline-none focus:border-gray-500" />
          <button class="bg-[#1a1a1a] border border-[#2a2a2a] text-gray-300 text-sm rounded-lg px-4 py-1.5 hover:bg-[#222]">Apply</button>
        </form>
      </div>
      <p class="text-xs text-gray-600">Records every admin action: quiz edits, deploys, gallery changes, announcements.</p>
      {filter_row}
      <pre id="alPre" class="text-xs text-gray-400 whitespace-pre-wrap bg-[#0a0a0a] border border-[#222] rounded-xl p-4 overflow-x-auto max-h-[75vh] overflow-y-auto">{safe}</pre>
    </div>
    <script>
    (function(){{
      const pre = document.getElementById('alPre');
      if (!pre) return;
      const searchInput = document.getElementById('alSearch');
      const clearBtn = document.getElementById('alClear');
      const countEl  = document.getElementById('alCount');
      const typeBtns = document.querySelectorAll('.al-type-btn');
      let rawLines = null;
      const hiddenActions = new Set();

      function applyFilter() {{
        if (!rawLines) rawLines = pre.textContent.split('\\n');
        const kw = searchInput ? searchInput.value.toLowerCase() : '';
        if (clearBtn) kw ? clearBtn.classList.remove('hidden') : clearBtn.classList.add('hidden');
        const filtered = rawLines.filter(function(l) {{
          if (kw && !l.toLowerCase().includes(kw)) return false;
          if (hiddenActions.size > 0) {{
            const parts = l.split('|');
            if (parts.length >= 2) {{
              const action = parts[1].trim();
              if (hiddenActions.has(action)) return false;
            }}
          }}
          return true;
        }});
        pre.textContent = filtered.join('\\n');
        if (countEl) {{
          const total = rawLines.filter(l => l.trim()).length;
          countEl.textContent = filtered.filter(l => l.trim()).length < total ? filtered.filter(l=>l.trim()).length + '/' + total + ' lines' : '';
        }}
      }}

      typeBtns.forEach(function(btn) {{
        btn.addEventListener('click', function() {{
          const action = btn.dataset.action;
          if (hiddenActions.has(action)) {{
            hiddenActions.delete(action);
            btn.style.opacity = '1';
          }} else {{
            hiddenActions.add(action);
            btn.style.opacity = '0.35';
          }}
          applyFilter();
        }});
      }});

      if (searchInput) {{
        searchInput.addEventListener('input', applyFilter);
        searchInput.addEventListener('keydown', function(e) {{ if(e.key==='Escape'){{searchInput.value='';applyFilter();}} }});
      }}
      if (clearBtn) clearBtn.addEventListener('click', function() {{ searchInput.value=''; applyFilter(); }});
    }})();
    </script>
    """
    return _render(body)


def start_dashboard_thread():
    thread = threading.Thread(target=run_dashboard, daemon=True)
    thread.start()

if __name__ == "__main__":
    run_dashboard()

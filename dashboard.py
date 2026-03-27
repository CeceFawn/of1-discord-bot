from __future__ import annotations

import json
import os
import time
import asyncio
import threading
import subprocess
import secrets
import hmac
from datetime import datetime, timezone, timedelta
from functools import wraps
from urllib.parse import urlencode

import bcrypt
import psutil
import requests
from flask import Flask, request, redirect, url_for, render_template_string, session, abort, jsonify

from settings import LOG_PATH, CONFIG_PATH, STATE_PATH, RUNTIME_STATUS_PATH, RUNTIME_DB_PATH, DEPLOY_STATUS_PATH, WATCH_PARTY_PATH
from storage import load_config, save_config, load_state, save_state
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

RAW_USERS = (os.getenv("DASHBOARD_USERS_JSON") or "{}").strip()
try:
    DASH_USERS: dict[str, str] = json.loads(RAW_USERS)
    if not isinstance(DASH_USERS, dict):
        raise ValueError("must be a JSON object")
    if not all(isinstance(k, str) and isinstance(v, str) for k, v in DASH_USERS.items()):
        raise ValueError("must map usernames to bcrypt hash strings")
except Exception as e:
    raise RuntimeError(f"DASHBOARD_USERS_JSON is not valid JSON: {e}")
PASSWORD_LOGIN_ENABLED = bool(DASH_USERS)

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
    # NOTE: if you later put this behind a reverse proxy, handle X-Forwarded-For carefully.
    return request.remote_addr or "unknown"

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

def _discord_authorize_url() -> str:
    params = {
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": DISCORD_REDIRECT_URI,
        "response_type": "code",
        "scope": "identify",
        "prompt": "consent",
    }
    return f"{DISCORD_API_BASE}/oauth2/authorize?{urlencode(params)}"

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

@app.before_request
def _csrf_protect():
    if request.method != "POST":
        return
    token = request.form.get("_csrf", "")
    expected = session.get("_csrf_token", "")
    if not token or not expected or not hmac.compare_digest(token, expected):
        abort(400)

# ----------------------------
# UI helpers
# ----------------------------
BASE_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>OF1 Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <script src="https://cdn.tailwindcss.com"></script>
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
      {% if bot_name %}
        <div class="ml-auto text-xs text-gray-600 truncate max-w-[80px]" title="{{ bot_name }}">{{ bot_name }}</div>
      {% endif %}
    </div>

    <!-- Nav links -->
    <nav class="flex-1 overflow-y-auto p-2 space-y-0.5">
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
      <a href="{{ url_for('watch_party_editor') }}"
         class="nav-link flex items-center gap-2.5 px-3 py-2 rounded-lg text-gray-400 hover:bg-[#1a1a1a] hover:text-white text-sm transition-colors">
        <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/>
        </svg>
        Watch Party
      </a>
    </nav>

    <!-- Bot controls -->
    <div class="p-2 border-t border-[#222] space-y-0.5">
      <p class="px-3 pt-1 pb-0.5 text-[10px] uppercase tracking-widest text-gray-600">Bot Controls</p>

      <form data-async-refresh="1" action="{{ url_for('bot_action', action='restart') }}" method="post">
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
          <form data-async-refresh="1" action="{{ url_for('bot_action', action='deployall') }}" method="post">
            {{ csrf_input|safe }}
            <button class="w-full text-left px-4 py-2 text-gray-300 hover:bg-[#222] text-sm">Deploy All</button>
          </form>
          <div class="my-1 border-t border-[#2a2a2a]"></div>
          <form data-async-refresh="1" action="{{ url_for('bot_action', action='deploybot') }}" method="post">
            {{ csrf_input|safe }}
            <button class="w-full text-left px-4 py-2 text-gray-400 hover:bg-[#222] text-sm">Bot only</button>
          </form>
          <form data-async-refresh="1" action="{{ url_for('bot_action', action='deploywebsite') }}" method="post">
            {{ csrf_input|safe }}
            <button class="w-full text-left px-4 py-2 text-gray-400 hover:bg-[#222] text-sm">Website only</button>
          </form>
          <form data-async-refresh="1" action="{{ url_for('bot_action', action='deploydashboard') }}" method="post">
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
          <form data-async-refresh="1" action="{{ url_for('bot_action', action='start') }}" method="post">
            {{ csrf_input|safe }}
            <button class="w-full text-left px-4 py-2 text-gray-300 hover:bg-[#222] text-sm">Start Bot</button>
          </form>
          <form data-async-refresh="1" action="{{ url_for('bot_action', action='stop') }}" method="post">
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

def _render(body: str, flash: str = ""):
    bot_name = None
    try:
        if bot_reference and getattr(bot_reference, "user", None):
            bot_name = str(bot_reference.user)
    except Exception:
        bot_name = None

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return render_template_string(
        BASE_TEMPLATE,
        body=body,
        flash=flash,
        bot_name=bot_name,
        now=now,
        csrf_input=_csrf_input(),
    )

def _backup_file(path: str) -> None:
    try:
        if os.path.exists(path):
            bak = path + ".bak"
            with open(path, "rb") as src, open(bak, "wb") as dst:
                dst.write(src.read())
    except Exception:
        pass

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
        status = "OK" if last.get("ok") else "FAILED"
        color = "#6f6" if last.get("ok") else "#f66"
        last_html = f"""
          <div style="margin:10px 0;padding:10px;border-radius:10px;background:#0b0b0b;border:1px solid #333;">
            <div style="color:#aaa;font-size:12px;margin-bottom:6px;">Last action: <b style="color:{color};">{_escape(str(last.get("action")))} · {status}</b> · {_escape(str(last.get("ts")))}</div>
            <pre style="white-space:pre-wrap;margin:0;color:#ddd;">{_escape(last.get("output") or "")}</pre>
          </div>
        """

    deploy_html = ""
    if deploy_status.get("ts"):
        status = "OK" if deploy_status.get("ok") else "FAILED"
        color = "#6f6" if deploy_status.get("ok") else "#f66"
        deploy_html = f"""
          <div style="margin:10px 0;padding:10px;border-radius:10px;background:#0b0b0b;border:1px solid #333;">
            <div style="color:#aaa;font-size:12px;margin-bottom:6px;">Last deploy checkpoint: <b style="color:{color};">{_escape(str(deploy_status.get("action")))} · {status}</b> · {_escape(str(deploy_status.get("ts")))}</div>
            <pre style="white-space:pre-wrap;margin:0;color:#ddd;">{_escape(str(deploy_status.get("output") or ""))}</pre>
          </div>
        """

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
      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:10px;">
        <form method="get" style="display:flex;gap:8px;align-items:center;">
          <label style="color:#aaa;font-size:13px;">Tail</label>
          <input name="tail" value="{tail_n}" style="width:90px;background:#000;color:#eee;border:1px solid #333;border-radius:8px;padding:6px;" />

          <label style="color:#aaa;font-size:13px;">Filtered</label>
          <select name="filtered" style="background:#000;color:#eee;border:1px solid #333;border-radius:8px;padding:6px;">
            <option value="1" {"selected" if show_filtered else ""}>On</option>
            <option value="0" {"selected" if not show_filtered else ""}>Off</option>
          </select>

          <button type="submit"
            style="background:#222;color:#eee;border:1px solid #333;padding:7px 10px;border-radius:10px;cursor:pointer;">
            Refresh
          </button>
        </form>
      </div>
    """

    body = (
        "<h2 style='margin:0 0 10px 0;'>Logs</h2>"
        + controls
        + f"<div id='lastActionBox'>{data['last_html']}</div>"
        + f"<div id='deployStatusBox'>{data.get('deploy_html','')}</div>"
        + f"<pre id='liveLogsPre' style='white-space:pre-wrap;background:#000;padding:12px;border-radius:10px;border:1px solid #333;max-width:1200px;'>{data['safe_logs']}</pre>"
        + """
        <script>
          (function(){
            const pre = document.getElementById('liveLogsPre');
            const lastBox = document.getElementById('lastActionBox');
            const deployBox = document.getElementById('deployStatusBox');
            if (!pre || !lastBox || !deployBox) return;
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
                  pre.innerHTML = data.safe_logs;
                  if (wasNearBottom) pre.scrollTop = pre.scrollHeight;
                }
                if (typeof data.last_html === 'string') {
                  lastBox.innerHTML = data.last_html;
                }
                if (typeof data.deploy_html === 'string') {
                  deployBox.innerHTML = data.deploy_html;
                }
              } catch (_err) {
                // ignore transient polling errors
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

    # loop health modules
    modules = [
        ("Race Supervisor",   bool(loops.get("race_supervisor"))),
        ("F1 Reminders",      bool(loops.get("f1_reminders"))),
        ("Standings",         bool(loops.get("standings"))),
        ("XP Flush",          bool(loops.get("xp_flush"))),
        ("Role Recovery",     bool(loops.get("periodic_role_recovery"))),
    ]
    module_grid = ""
    for name, ok in modules:
        bg = "bg-[#0f1f0f]" if ok else "bg-[#1f0f0f]"
        border = "border-green-900/40" if ok else "border-red-900/40"
        module_grid += (
            f'<div class="{bg} border {border} rounded-xl px-4 py-3 flex items-center justify-between">'
            f'<span class="text-sm text-gray-300">{name}</span>'
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
        # Validate expected state so button outcomes are obvious in Logs.
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
    return redirect(url_for("logs"))

# Backwards-compat route name (your old template called /restart)
@app.route("/restart", methods=["POST"])
@login_required
def restart():
    # Now restarts the bot service, not the dashboard process
    ok, out = _sudo_systemctl("restart")
    _set_last_action("restart", ok, out or ("OK" if ok else "FAILED"))
    return redirect(url_for("logs"))

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
_AUTO_FIELDS = {"title", "date", "time"}
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

    page = f"""
      <h2 style="margin:0 0 16px 0;">Watch Party</h2>
      <meta name="wp-csrf" content="{_csrf_token()}" />

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
        <div style="color:#aaa;font-size:12px;text-transform:uppercase;letter-spacing:.05em;margin-bottom:12px;">
          Auto-filled fields
          <span style="color:#555;font-weight:normal;text-transform:none;"> — leave blank to auto-detect, or fill in to override just this field</span>
        </div>
        {_field_row("Title", "title", auto=True)}
        {_field_row("Date", "date", auto=True)}
        {_field_row("Time", "time", auto=True)}
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

        window.wpReset = async function(field) {{
          document.getElementById('wp_' + field).value = '';
          try {{
            const d = await post('/watch_party/field', {{ field, value: '' }});
            toast(d.ok ? field + ' reset to auto.' : ('Error: ' + d.error), d.ok);
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


def run_dashboard():
    try:
        init_runtime_db()
    except Exception:
        pass
    port = int(os.getenv("DASHBOARD_PORT", "5000"))
    app.run(host="0.0.0.0", port=port)

def start_dashboard_thread():
    thread = threading.Thread(target=run_dashboard, daemon=True)
    thread.start()

if __name__ == "__main__":
    run_dashboard()

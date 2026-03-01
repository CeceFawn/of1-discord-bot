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

from settings import LOG_PATH, CONFIG_PATH, STATE_PATH
from storage import load_config, save_config, load_state, save_state

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

def _set_last_action(action: str, ok: bool, output: str):
    with _LAST_ACTION_LOCK:
        _LAST_ACTION["ts"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        _LAST_ACTION["action"] = action
        _LAST_ACTION["ok"] = ok
        _LAST_ACTION["output"] = output[-8000:]  # cap output size

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
<html>
  <head>
    <meta charset="utf-8" />
    <title>OF1 Bot Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body style="background:#111;color:#eee;padding:16px;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;">
    <div style="display:flex;gap:14px;align-items:center;flex-wrap:wrap;margin-bottom:14px;">
      <a style="color:#9cf;" href="{{ url_for('logs') }}">Logs</a>
      <a style="color:#9cf;" href="{{ url_for('status') }}">Status</a>
      <a style="color:#9cf;" href="{{ url_for('config') }}">Config</a>
      <a style="color:#9cf;" href="{{ url_for('state') }}">State</a>

      <!-- Split button: Restart + dropdown -->
      <div style="display:inline-flex;align-items:stretch;gap:0;margin-left:12px;">
        <form data-async-refresh="1" action="{{ url_for('bot_action', action='restart') }}" method="post" style="display:inline;margin:0;">
          {{ csrf_input|safe }}
          <button style="background:#300;color:#f88;border:1px solid #822;padding:6px 10px;border-radius:8px 0 0 8px;cursor:pointer;">
            Restart
          </button>
        </form>

        <div style="position:relative;display:inline-block;">
          <button id="actionsBtn"
            type="button"
            style="background:#300;color:#f88;border:1px solid #822;border-left:none;padding:6px 10px;border-radius:0 8px 8px 0;cursor:pointer;">
            ▼
          </button>

          <div id="actionsMenu"
            style="display:none;position:absolute;z-index:50;right:0;top:110%;background:#1a1a1a;border:1px solid #333;border-radius:10px;min-width:220px;box-shadow:0 12px 40px rgba(0,0,0,.35);padding:6px;">
            <form data-async-refresh="1" action="{{ url_for('bot_action', action='start') }}" method="post" style="margin:0;">
              {{ csrf_input|safe }}
              <button style="width:100%;text-align:left;background:transparent;color:#eee;border:none;padding:10px;border-radius:8px;cursor:pointer;">
                Start bot
              </button>
            </form>
            <form data-async-refresh="1" action="{{ url_for('bot_action', action='stop') }}" method="post" style="margin:0;">
              {{ csrf_input|safe }}
              <button style="width:100%;text-align:left;background:transparent;color:#eee;border:none;padding:10px;border-radius:8px;cursor:pointer;">
                Stop bot
              </button>
            </form>
            <div style="height:1px;background:#2a2a2a;margin:6px 0;"></div>
            <form data-async-refresh="1" action="{{ url_for('bot_action', action='deploy') }}" method="post" style="margin:0;">
              {{ csrf_input|safe }}
              <button style="width:100%;text-align:left;background:transparent;color:#eee;border:none;padding:10px;border-radius:8px;cursor:pointer;">
                Deploy update (git pull + restart)
              </button>
            </form>
          </div>
        </div>
      </div>

      <script>
        (function(){
          const btn = document.getElementById('actionsBtn');
          const menu = document.getElementById('actionsMenu');
          function close() { menu.style.display = 'none'; }
          function toggle() { menu.style.display = (menu.style.display === 'none' || !menu.style.display) ? 'block' : 'none'; }

          btn.addEventListener('click', function(e){ e.preventDefault(); e.stopPropagation(); toggle(); });
          document.addEventListener('click', function(){ close(); });
          menu.addEventListener('click', function(e){ e.stopPropagation(); });

          document.querySelectorAll('form[data-async-refresh="1"]').forEach(function(form){
            form.addEventListener('submit', async function(e){
              e.preventDefault();
              const btnEl = form.querySelector('button');
              if (btnEl) btnEl.disabled = true;
              try {
                await fetch(form.action, {
                  method: 'POST',
                  body: new FormData(form),
                  credentials: 'same-origin',
                });
              } catch (_err) {
                // Fall back to normal navigation if fetch fails.
                form.submit();
                return;
              }
              window.location.reload();
            });
          });
        })();
      </script>

      <form action="{{ url_for('logout') }}" method="post" style="display:inline;margin-left:6px;">
        {{ csrf_input|safe }}
        <button style="background:#222;color:#eee;border:1px solid #333;padding:6px 10px;border-radius:8px;cursor:pointer;">
          Logout
        </button>
      </form>

      <div style="margin-left:auto;color:#aaa;font-size:13px;">
        {% if bot_name %}Bot: <b style="color:#eee;">{{ bot_name }}</b> · {% endif %}
        {{ now }}
      </div>
    </div>

    {% if flash %}
      <div style="margin-bottom:12px;padding:10px;border-radius:10px;background:#1a1a1a;border:1px solid #333;">
        {{ flash|safe }}
      </div>
    {% endif %}

    {{ body|safe }}
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

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    return render_template_string(
        BASE_TEMPLATE,
        body=body,
        flash=flash,
        bot_name=bot_name,
        now=now,
        csrf_input=_csrf_input(),
    )

def _json_editor(title: str, obj) -> str:
    pretty = json.dumps(obj, indent=2, ensure_ascii=False)
    return render_template_string(
        """
        <h2 style="margin:0 0 10px 0;">{{ title }}</h2>

        <form method="post" style="display:flex;flex-direction:column;gap:10px;">
          {{ csrf_input|safe }}
          <textarea name="json" rows="28"
            style="width:100%;max-width:1200px;background:#000;color:#0f0;padding:12px;border-radius:10px;border:1px solid #333;white-space:pre;"
          >{{ pretty }}</textarea>

          <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
            <button type="submit"
              style="background:#1f6f3f;color:#fff;border:1px solid #2a8f52;padding:8px 12px;border-radius:10px;cursor:pointer;">
              Save
            </button>

            <span style="color:#aaa;font-size:13px;">
              Tip: If you break JSON, it won’t save.
            </span>
          </div>
        </form>
        """,
        title=title,
        pretty=pretty,
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

    return {
        "safe_logs": safe_logs,
        "last_html": last_html,
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

def _bot_runtime_status() -> dict:
    try:
        if not bot_reference:
            return {}
        fn = getattr(bot_reference, "of1_runtime_status_snapshot", None)
        if callable(fn):
            data = fn()
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}

def _bot_round_meta(timeout_s: float = 4.0) -> dict:
    try:
        if not bot_reference:
            return {}
        coro_fn = getattr(bot_reference, "of1_current_or_next_round_meta_coro", None)
        loop = getattr(bot_reference, "loop", None)
        if not callable(coro_fn) or loop is None:
            return {}
        fut = asyncio.run_coroutine_threadsafe(coro_fn(), loop)
        data = fut.result(timeout=max(0.5, float(timeout_s)))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

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

    recent_alerts = []
    alerts_root = st.get("alerts") if isinstance(st.get("alerts"), dict) else {}
    for a in (alerts_root.get("items") or [])[-20:]:
        if isinstance(a, dict):
            recent_alerts.append(a)
    log_alert_lines = _recent_log_alerts(limit=20)

    return {
        "runtime": runtime,
        "round_meta": round_meta,
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

def _sudo_systemctl(action: str) -> tuple[bool, str]:
    # Requires sudoers NOPASSWD for systemctl on this service.
    # Use -n so it fails immediately instead of hanging waiting for a password prompt.
    rc, out = _run_cmd(["sudo", "-n", "systemctl", action, BOT_SYSTEMD_SERVICE], timeout_s=30)
    return (rc == 0), out

def _deploy_worker():
    global _DEPLOY_IN_PROGRESS
    chunks = []
    ok_all = True
    try:
        chunks.append(f"Repo dir: {BOT_REPO_DIR}")
        chunks.append(f"Service: {BOT_SYSTEMD_SERVICE}")
        chunks.append("Deploy worker started.")

        # git pull (fast-forward only to avoid surprise merges)
        rc, out = _run_cmd(["git", "pull", "--ff-only"], cwd=BOT_REPO_DIR, timeout_s=120)
        chunks.append("---- git pull --ff-only ----")
        chunks.append(out or f"(exit {rc})")
        if rc != 0:
            ok_all = False

        # pip install -r requirements.txt (if pip exists and requirements exists)
        req_path = os.path.join(BOT_REPO_DIR, "requirements.txt")
        if os.path.exists(req_path) and os.path.exists(BOT_VENV_PIP):
            rc, out = _run_cmd([BOT_VENV_PIP, "install", "-r", req_path], cwd=BOT_REPO_DIR, timeout_s=600)
            chunks.append("---- pip install -r requirements.txt ----")
            chunks.append(out or f"(exit {rc})")
            if rc != 0:
                ok_all = False
        else:
            chunks.append("---- pip install skipped ----")
            chunks.append(f"requirements.txt exists={os.path.exists(req_path)}, venv pip exists={os.path.exists(BOT_VENV_PIP)}")

        # restart service only if earlier steps succeeded
        chunks.append("---- systemctl restart ----")
        if ok_all:
            ok, out = _sudo_systemctl("restart")
            chunks.append(out or ("OK" if ok else "FAILED"))
            if not ok:
                ok_all = False
        else:
            chunks.append("Skipped because deploy steps failed.")

        _set_last_action("deploy", ok_all, "\n".join(chunks))
    except Exception as e:
        chunks.append("---- deploy worker exception ----")
        chunks.append(f"{type(e).__name__}: {e}")
        _set_last_action("deploy", False, "\n".join(chunks))
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
    <html><body style="background:#111;color:#eee;font-family:system-ui;padding:30px;">
      <h2>Dashboard Login</h2>
      {% if err %}<div style="color:#f66;margin:10px 0;">{{ err }}</div>{% endif %}
      {% if discord_login_url %}
        <a href="{{ discord_login_url }}"
           style="display:inline-block;margin:8px 0 14px 0;padding:10px 14px;border-radius:10px;background:#5865F2;color:#fff;text-decoration:none;font-weight:600;">
          Login with Discord
        </a>
      {% endif %}
      {% if password_login_enabled %}
      <form method="post" style="display:flex;flex-direction:column;gap:10px;max-width:320px;">
        {{ csrf_input|safe }}
        <input name="username" placeholder="Username" style="padding:10px;border-radius:10px;border:1px solid #333;background:#000;color:#eee;" />
        <input name="password" type="password" placeholder="Password" style="padding:10px;border-radius:10px;border:1px solid #333;background:#000;color:#eee;" />
        <button type="submit" style="padding:10px;border-radius:10px;border:1px solid #333;background:#222;color:#eee;cursor:pointer;">Login</button>
      </form>
      {% elif not discord_login_url %}
        <div style="color:#f66;margin-top:12px;">No login method is configured.</div>
      {% endif %}
    </body></html>
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
        + f"<pre id='liveLogsPre' style='white-space:pre-wrap;background:#000;padding:12px;border-radius:10px;border:1px solid #333;max-width:1200px;'>{data['safe_logs']}</pre>"
        + """
        <script>
          (function(){
            const pre = document.getElementById('liveLogsPre');
            const lastBox = document.getElementById('lastActionBox');
            if (!pre || !lastBox) return;
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
    standings = runtime.get("standings") if isinstance(runtime.get("standings"), dict) else {}
    openf1_window = runtime.get("openf1_window") if isinstance(runtime.get("openf1_window"), dict) else {}

    def _badge(ok: bool, txt_ok: str = "Running", txt_no: str = "Stopped") -> str:
        color = "#6f6" if ok else "#f66"
        label = txt_ok if ok else txt_no
        return f"<span style='display:inline-block;padding:2px 8px;border-radius:999px;border:1px solid {color};color:{color};font-size:12px;'>{label}</span>"

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

    active_line = "No active race thread."
    if active_threads:
        t = active_threads[0]
        active_line = (
            f"#{_escape(str(t.get('thread_name') or t.get('thread_id') or 'thread'))} "
            f"(round {_escape(str(t.get('round_key') or '-'))}, created {_escape(_fmt_ts_utc(t.get('created_at')))})."
        )

    prior_line = "No prior race thread marked as past yet."
    if past_threads:
        t = past_threads[0]
        prior_line = (
            f"#{_escape(str(t.get('thread_name') or t.get('thread_id') or 'thread'))} "
            f"(round {_escape(str(t.get('round_key') or '-'))}, past since {_escape(_fmt_ts_utc(t.get('past_at') or t.get('created_at')))})."
        )

    if has_current:
        current_card = f"""
          <div style="padding:10px;border:1px solid #333;border-radius:10px;background:#141414;">
            <div style="font-weight:700;">Current Round Thread</div>
            <div style="margin-top:6px;"><b>Round:</b> {_escape(current_round_name)} ({_escape(current_round_key or '-')})</div>
            <div><b>Status:</b> {_escape(current_state.title())}</div>
            <div><b>Thread:</b> {_escape(str(current_round_record.get("thread_name") or current_round_record.get("thread_id") or "-"))}</div>
            <div><b>Created:</b> {_escape(_fmt_ts_utc(current_round_record.get("created_at")))} ({_escape(_fmt_relative(current_round_record.get("created_at")))})</div>
            <div><b>Source:</b> {_escape(str(current_round_record.get("source") or "-"))}</div>
          </div>
        """
    else:
        current_card = f"""
          <div style="padding:10px;border:1px solid #333;border-radius:10px;background:#141414;">
            <div style="font-weight:700;">Next Round Queue</div>
            <div style="margin-top:6px;"><b>Round:</b> {_escape(current_round_name)} ({_escape(current_round_key or '-')})</div>
            <div><b>Status:</b> Queued (not created yet)</div>
            <div><b>Expected auto-create window starts:</b> {_escape(queued_eta)}</div>
          </div>
        """

    alert_items_html = ""
    if recent_alerts:
        rows = []
        for a in reversed(recent_alerts[-10:]):
            ts = _fmt_ts_utc(str(a.get("ts") or ""))
            kind = str(a.get("kind") or "alert")
            msg = str(a.get("message") or "")
            rows.append(f"<li><b>{_escape(kind)}</b> @ {_escape(ts)} - {_escape(msg)}</li>")
        alert_items_html = "<ul>" + "".join(rows) + "</ul>"
    else:
        alert_items_html = "<div style='color:#aaa;'>No recorded state alerts yet.</div>"

    log_alerts_html = ""
    if log_alerts:
        log_alerts_html = "<pre style='white-space:pre-wrap;background:#000;padding:10px;border-radius:8px;border:1px solid #333;'>" + _escape("\n".join(log_alerts[-10:])) + "</pre>"
    else:
        log_alerts_html = "<div style='color:#aaa;'>No recent error-like log lines detected.</div>"

    body = f"""
      <h2 style="margin:0 0 10px 0;">Status</h2>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:10px;margin-bottom:12px;">
        <div style="padding:10px;border:1px solid #333;border-radius:10px;background:#141414;">
          <div style="font-weight:700;">Host</div>
          <div style="margin-top:6px;"><b>CPU:</b> {cpu}%</div>
          <div><b>RAM:</b> {ram}%</div>
          <div><b>Dashboard uptime:</b> {proc_uptime_s}s</div>
          <div><b>Log path:</b> {_escape(LOG_PATH)}</div>
        </div>
        <div style="padding:10px;border:1px solid #333;border-radius:10px;background:#141414;">
          <div style="font-weight:700;">Service</div>
          <div style="margin-top:6px;"><b>Bot service:</b> {_escape(BOT_SYSTEMD_SERVICE)}</div>
          <div><b>Repo dir:</b> {_escape(BOT_REPO_DIR)}</div>
          <div><b>Bot connected guilds:</b> {_escape(str(runtime.get("guild_count", "-")))}</div>
          <div><b>Snapshot time:</b> {_escape(_fmt_ts_utc(str(runtime.get("ts") or "")))}</div>
        </div>
      </div>

      <h3 style="margin:14px 0 8px 0;">Race Thread Lifecycle</h3>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:10px;margin-bottom:12px;">
        {current_card}
        <div style="padding:10px;border:1px solid #333;border-radius:10px;background:#141414;">
          <div style="font-weight:700;">Active Race Thread</div>
          <div style="margin-top:6px;">{active_line}</div>
          <div style="margin-top:8px;font-weight:700;">Prior Race Weekend</div>
          <div style="margin-top:6px;">{prior_line}</div>
        </div>
      </div>

      <h3 style="margin:14px 0 8px 0;">Live Module Health</h3>
      <ul>
        <li><b>Race supervisor:</b> {_badge(bool(loops.get("race_supervisor")))} </li>
        <li><b>F1 reminders loop:</b> {_badge(bool(loops.get("f1_reminders")))} </li>
        <li><b>Standings loop:</b> {_badge(bool(loops.get("standings")))} </li>
        <li><b>XP flush loop:</b> {_badge(bool(loops.get("xp_flush")))} </li>
        <li><b>Role recovery loop:</b> {_badge(bool(loops.get("periodic_role_recovery")))} </li>
      </ul>
      <ul>
        <li><b>Active race-live guild IDs:</b> {_escape(str((runtime.get("race_live") or {}).get("running_guild_ids", [])))}</li>
        <li><b>Tracked race round keys:</b> {_escape(str((runtime.get("race_live") or {}).get("tracked_round_keys", {})))}</li>
        <li><b>OpenF1 pre-weekend buffer:</b> {_escape(str(openf1_window.get("pre_buffer_hours", 24)))}h</li>
        <li><b>OpenF1 post-weekend buffer (auto-kill):</b> {_escape(str(openf1_window.get("post_buffer_hours", 12)))}h</li>
      </ul>

      <h3 style="margin:14px 0 8px 0;">Standings Health</h3>
      <ul>
        <li><b>Channel ID configured:</b> {_escape(str(standings.get("channel_id", 0)))}</li>
        <li><b>Driver message ID:</b> {_escape(str(standings.get("driver_message_id", 0)))}</li>
        <li><b>Constructor message ID:</b> {_escape(str(standings.get("constructor_message_id", 0)))}</li>
        <li><b>Refresh every:</b> {_escape(str(standings.get("refresh_minutes", 5)))} minute(s)</li>
      </ul>

      <h3 style="margin:14px 0 8px 0;">Alerts</h3>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:10px;">
        <div style="padding:10px;border:1px solid #333;border-radius:10px;background:#141414;">
          <div style="font-weight:700;margin-bottom:6px;">Command/State Alerts</div>
          {alert_items_html}
        </div>
        <div style="padding:10px;border:1px solid #333;border-radius:10px;background:#141414;">
          <div style="font-weight:700;margin-bottom:6px;">Recent Error-like Logs</div>
          {log_alerts_html}
        </div>
      </div>

      <script>
        setTimeout(function() {{ window.location.reload(); }}, 15000);
      </script>
    """
    return _render(body)

@app.route("/config", methods=["GET", "POST"])
@login_required
def config():
    cfg = load_config() or {}
    if request.method == "POST":
        raw = request.form.get("json", "")
        try:
            parsed = json.loads(raw)
            _backup_file(CONFIG_PATH)
            save_config(parsed)
            return redirect(url_for("config"))
        except Exception as e:
            flash = f"<div style='color:#f66;'><b>Invalid JSON:</b> {_escape(str(e))}</div>"
            return _render(_json_editor("config.json", cfg), flash=flash)

    return _render(_json_editor("config.json", cfg))

@app.route("/state", methods=["GET", "POST"])
@login_required
def state():
    st = load_state() or {}
    if request.method == "POST":
        raw = request.form.get("json", "")
        try:
            parsed = json.loads(raw)
            _backup_file(STATE_PATH)
            save_state(parsed)
            return redirect(url_for("state"))
        except Exception as e:
            flash = f"<div style='color:#f66;'><b>Invalid JSON:</b> {_escape(str(e))}</div>"
            return _render(_json_editor("state.json", st), flash=flash)

    return _render(_json_editor("state.json", st))

# ----------------------------
# Bot action routes
# ----------------------------
@app.route("/bot_action/<action>", methods=["POST"])
@login_required
def bot_action(action: str):
    global _DEPLOY_IN_PROGRESS
    action = (action or "").strip().lower()
    allowed = {"start", "stop", "restart", "deploy"}
    if action not in allowed:
        abort(404)

    if action in {"start", "stop", "restart"}:
        ok, out = _sudo_systemctl(action)
        _set_last_action(action, ok, out or ("OK" if ok else "FAILED"))
        return redirect(url_for("logs"))

    # deploy: run in background so the request returns quickly
    with _DEPLOY_LOCK:
        if _DEPLOY_IN_PROGRESS:
            _set_last_action("deploy", False, "Deploy already running. Wait for it to finish, then refresh Logs.")
            return redirect(url_for("logs"))
        _DEPLOY_IN_PROGRESS = True
    t = threading.Thread(target=_deploy_worker, daemon=True, name="dashboard-deploy-worker")
    t.start()
    _set_last_action("deploy", True, f"Deploy started in background (thread={t.name}). Refresh Logs in a few seconds.")
    return redirect(url_for("logs"))

# Backwards-compat route name (your old template called /restart)
@app.route("/restart", methods=["POST"])
@login_required
def restart():
    # Now restarts the bot service, not the dashboard process
    ok, out = _sudo_systemctl("restart")
    _set_last_action("restart", ok, out or ("OK" if ok else "FAILED"))
    return redirect(url_for("logs"))

def run_dashboard():
    port = int(os.getenv("DASHBOARD_PORT", "5000"))
    app.run(host="0.0.0.0", port=port)

def start_dashboard_thread():
    thread = threading.Thread(target=run_dashboard, daemon=True)
    thread.start()

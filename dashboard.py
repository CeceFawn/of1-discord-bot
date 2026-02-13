from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime
from functools import wraps
from typing import Any

import bcrypt
import psutil
from flask import Flask, abort, redirect, render_template_string, request, session, url_for

from settings import LOG_PATH
from storage import load_config, save_config, load_state, save_state

# ------------------------------------------------------------
# Flask app + bot reference
# ------------------------------------------------------------
app = Flask(__name__)

bot_reference = None
def set_bot_reference(bot):
    global bot_reference
    bot_reference = bot

DASHBOARD_STARTED_AT = time.time()

# ------------------------------------------------------------
# Auth config (username/password login)
# ------------------------------------------------------------
SECRET_KEY = (os.getenv("DASHBOARD_SECRET_KEY") or "").strip()
if not SECRET_KEY:
    raise RuntimeError("DASHBOARD_SECRET_KEY missing in .env")
app.secret_key = SECRET_KEY

# Session cookie hardening
# If you put this behind HTTPS (recommended), set DASHBOARD_HTTPS=1
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=(os.getenv("DASHBOARD_HTTPS", "").strip() == "1"),
)

# Users: {"username": "bcrypt_hash_string"}
RAW_USERS = (os.getenv("DASHBOARD_USERS_JSON") or "{}").strip()
try:
    DASH_USERS: dict[str, str] = json.loads(RAW_USERS)
    if not isinstance(DASH_USERS, dict):
        raise ValueError("must be a JSON object")
except Exception as e:
    raise RuntimeError(f"DASHBOARD_USERS_JSON is not valid JSON: {e}")

# Optional IP allowlist (comma separated)
ALLOWED_IPS = [x.strip() for x in (os.getenv("DASHBOARD_ALLOWED_IPS") or "").split(",") if x.strip()]

# Simple rate limit per IP for login attempts
LOGIN_ATTEMPTS: dict[str, list[float]] = {}
MAX_ATTEMPTS = 8
WINDOW_SECONDS = 10 * 60  # 10 minutes


def _client_ip() -> str:
    # NOTE: if you're behind a reverse proxy later, you'll want to trust X-Forwarded-For carefully.
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


def _record_attempt() -> None:
    ip = _client_ip()
    LOGIN_ATTEMPTS.setdefault(ip, []).append(time.time())


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _ip_allowed():
            abort(403)
        if not session.get("dash_user"):
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper


# ------------------------------------------------------------
# UI helpers
# ------------------------------------------------------------
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

      <form action="{{ url_for('restart') }}" method="post" style="display:inline;margin-left:12px;">
        <button style="background:#300;color:#f88;border:1px solid #822;padding:6px 10px;border-radius:8px;cursor:pointer;">
          Restart
        </button>
      </form>

      <form action="{{ url_for('logout') }}" method="post" style="display:inline;">
        <button style="background:#222;color:#ddd;border:1px solid #333;padding:6px 10px;border-radius:8px;cursor:pointer;">
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
    )


def _json_editor(title: str, obj: Any) -> str:
    pretty = json.dumps(obj, indent=2, ensure_ascii=False)
    return render_template_string(
        """
        <h2 style="margin:0 0 10px 0;">{{ title }}</h2>

        <form method="post" style="display:flex;flex-direction:column;gap:10px;">
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
    )


def _backup_file(path: str) -> None:
    try:
        if os.path.exists(path):
            bak = path + ".bak"
            with open(path, "rb") as src, open(bak, "wb") as dst:
                dst.write(src.read())
    except Exception:
        pass


# ------------------------------------------------------------
# Auth routes
# ------------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if not _ip_allowed():
        abort(403)

    err = ""
    if request.method == "POST":
        if _rate_limited():
            err = "Too many attempts. Try again later."
        else:
            username = (request.form.get("username") or "").strip()
            password = (request.form.get("password") or "").encode("utf-8")

            stored = DASH_USERS.get(username)
            _record_attempt()

            if stored and bcrypt.checkpw(password, stored.encode("utf-8")):
                session["dash_user"] = username
                return redirect(url_for("logs"))
            err = "Invalid username or password."

    return render_template_string(
        """
        <html><body style="background:#111;color:#eee;font-family:system-ui;padding:30px;">
          <h2>Dashboard Login</h2>
          {% if err %}<div style="color:#f66;margin:10px 0;">{{ err }}</div>{% endif %}
          <form method="post" style="display:flex;flex-direction:column;gap:10px;max-width:320px;">
            <input name="username" placeholder="Username"
              style="padding:10px;border-radius:10px;border:1px solid #333;background:#000;color:#eee;" />
            <input name="password" type="password" placeholder="Password"
              style="padding:10px;border-radius:10px;border:1px solid #333;background:#000;color:#eee;" />
            <button type="submit"
              style="padding:10px;border-radius:10px;border:1px solid #333;background:#222;color:#eee;cursor:pointer;">
              Login
            </button>
          </form>
        </body></html>
        """,
        err=err,
    )


@app.route("/logout", methods=["POST"])
@login_required
def logout():
    session.pop("dash_user", None)
    return redirect(url_for("login"))


# ------------------------------------------------------------
# App routes
# ------------------------------------------------------------
@app.route("/")
@login_required
def index():
    return redirect(url_for("logs"))


@app.route("/logs")
@login_required
def logs():
    cfg = load_config() or {}
    filters = cfg.get("log_filters", []) or []

    tail = request.args.get("tail", "400")
    try:
        tail_n = max(50, min(4000, int(tail)))
    except Exception:
        tail_n = 400

    show_filtered = (request.args.get("filtered", "1").strip() != "0")

    try:
        with open(LOG_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()[-tail_n:]
    except FileNotFoundError:
        lines = ["Log file not found.\n"]

    if show_filtered and filters:
        lines = [line for line in lines if not any(x in line for x in filters)]

    safe = _escape("".join(lines))

    controls = f"""
      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:10px;">
        <form method="get" style="display:flex;gap:8px;align-items:center;">
          <label style="color:#aaa;font-size:13px;">Tail</label>
          <input name="tail" value="{tail_n}"
            style="width:90px;background:#000;color:#eee;border:1px solid #333;border-radius:8px;padding:6px;" />

          <label style="color:#aaa;font-size:13px;">Filtered</label>
          <select name="filtered"
            style="background:#000;color:#eee;border:1px solid #333;border-radius:8px;padding:6px;">
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
        + f"<pre style='white-space:pre-wrap;background:#000;padding:12px;border-radius:10px;border:1px solid #333;max-width:1200px;'>{safe}</pre>"
    )
    return _render(body)


@app.route("/status")
@login_required
def status():
    proc_uptime_s = max(0, int(time.time() - DASHBOARD_STARTED_AT))
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent

    body = f"""
      <h2 style="margin:0 0 10px 0;">Status</h2>
      <ul>
        <li><b>CPU:</b> {cpu}%</li>
        <li><b>RAM:</b> {ram}%</li>
        <li><b>Dashboard uptime:</b> {proc_uptime_s}s</li>
        <li><b>Log path:</b> {_escape(LOG_PATH)}</li>
        <li><b>IP allowlist:</b> {"(disabled)" if not ALLOWED_IPS else _escape(", ".join(ALLOWED_IPS))}</li>
      </ul>
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
            _backup_file("config.json")
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
            _backup_file("state.json")
            save_state(parsed)
            return redirect(url_for("state"))
        except Exception as e:
            flash = f"<div style='color:#f66;'><b>Invalid JSON:</b> {_escape(str(e))}</div>"
            return _render(_json_editor("state.json", st), flash=flash)

    return _render(_json_editor("state.json", st))


@app.route("/restart", methods=["POST"])
@login_required
def restart():
    os._exit(1)


# ------------------------------------------------------------
# Runner (threaded)
# ------------------------------------------------------------
def run_dashboard():
    port = int(os.getenv("DASHBOARD_PORT", "5000"))
    app.run(host="0.0.0.0", port=port)


def start_dashboard_thread():
    thread = threading.Thread(target=run_dashboard, daemon=True)
    thread.start()

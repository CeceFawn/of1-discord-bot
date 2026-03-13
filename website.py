from __future__ import annotations

import json
import os
import threading
import time
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from flask import Flask, render_template, jsonify
from dotenv import load_dotenv

EASTERN = ZoneInfo("America/New_York")

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

app = Flask(__name__)

# ----------------------------
# Social / site config from env
# ----------------------------
DISCORD_INVITE  = "https://discord.gg/WxUBRYVUmF"
INSTAGRAM_URL   = os.getenv("INSTAGRAM_URL", "")
TWITTER_URL     = os.getenv("TWITTER_URL", "")
TIKTOK_URL      = os.getenv("TIKTOK_URL", "")

WATCH_PARTY_PATH = os.path.join(os.path.dirname(__file__), "watch_party.json")

OPENF1_BASE = "https://api.openf1.org/v1"
PRE_HOURS  = 24
POST_HOURS = 12

_cache: dict = {}  # key -> (fetched_at, data)

# ----------------------------
# OAuth token cache (mirrors bot's logic)
# ----------------------------
_TOKEN_LOCK = threading.Lock()
_TOKEN_CACHE: dict = {}   # token, expires_at, fetched_at
_TOKEN_RETRY_AFTER = 0.0  # epoch seconds — don't attempt before this


def _fetch_login_token() -> tuple[str, float]:
    """POST credentials to OPENF1_AUTH_URL and return (token, expires_at_epoch)."""
    auth_url = os.getenv("OPENF1_AUTH_URL", "").strip()
    username  = os.getenv("OPENF1_AUTH_USERNAME", "").strip()
    password  = os.getenv("OPENF1_AUTH_PASSWORD", "").strip()
    if not auth_url or not username or not password:
        return "", 0.0

    user_field   = os.getenv("OPENF1_AUTH_USERNAME_FIELD", "username").strip() or "username"
    pass_field   = os.getenv("OPENF1_AUTH_PASSWORD_FIELD", "password").strip() or "password"
    token_key    = os.getenv("OPENF1_AUTH_TOKEN_JSON_KEY", "access_token").strip() or "access_token"
    exp_in_key   = os.getenv("OPENF1_AUTH_EXPIRES_IN_JSON_KEY", "expires_in").strip() or "expires_in"
    exp_at_key   = os.getenv("OPENF1_AUTH_EXPIRES_AT_JSON_KEY", "").strip()

    payload: dict = {user_field: username, pass_field: password}
    extra_raw = os.getenv("OPENF1_AUTH_EXTRA_JSON", "").strip()
    if extra_raw:
        try:
            extra = json.loads(extra_raw)
            if isinstance(extra, dict):
                payload.update(extra)
        except Exception:
            pass

    req_headers: dict = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "User-Agent": "OF1-Website",
    }
    hdr_raw = os.getenv("OPENF1_AUTH_HEADERS_JSON", "").strip()
    if hdr_raw:
        try:
            h = json.loads(hdr_raw)
            if isinstance(h, dict):
                req_headers.update({str(k): str(v) for k, v in h.items()})
        except Exception:
            pass

    r = requests.post(auth_url, data=payload, headers=req_headers, timeout=20)
    r.raise_for_status()
    body = r.json() if r.content else {}

    # Traverse dot-notation key path
    def _path_get(obj: dict, key: str):
        for part in key.split("."):
            if isinstance(obj, dict):
                obj = obj.get(part)
            else:
                return None
        return obj

    token = str(_path_get(body, token_key) or "").strip()
    if not token:
        raise RuntimeError("auth response missing token")

    now_ts = time.time()
    expires_at = 0.0
    if exp_at_key:
        raw = _path_get(body, exp_at_key)
        if isinstance(raw, (int, float)):
            expires_at = float(raw)
        elif isinstance(raw, str) and raw.strip():
            try:
                expires_at = datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
            except Exception:
                pass
    if expires_at <= 0.0:
        raw_in = _path_get(body, exp_in_key)
        try:
            exp_in = max(60, int(raw_in))
        except Exception:
            exp_in = 3600
        expires_at = now_ts + exp_in

    return token, expires_at


def _get_bearer_token() -> str:
    """Return a valid bearer token, refreshing via OAuth if needed."""
    global _TOKEN_RETRY_AFTER

    static = os.getenv("OPENF1_BEARER_TOKEN", "").strip()
    if static:
        return static

    with _TOKEN_LOCK:
        now_ts = time.time()
        token      = str(_TOKEN_CACHE.get("token") or "")
        expires_at = float(_TOKEN_CACHE.get("expires_at") or 0.0)

        # Still in back-off period — return cached token if valid
        if now_ts < _TOKEN_RETRY_AFTER:
            return token if (token and now_ts < expires_at) else ""

        # Token still fresh (120 s buffer)
        if token and now_ts < (expires_at - 120.0):
            return token

        try:
            new_token, new_expires_at = _fetch_login_token()
        except Exception as e:
            err = str(e)
            if "422" in err:
                _TOKEN_RETRY_AFTER = now_ts + 60.0
            elif "429" in err:
                _TOKEN_RETRY_AFTER = now_ts + 120.0
            elif "503" in err:
                _TOKEN_RETRY_AFTER = now_ts + 30.0
            else:
                _TOKEN_RETRY_AFTER = now_ts + 15.0
            return token if (token and now_ts < expires_at) else ""

        if not new_token:
            _TOKEN_RETRY_AFTER = now_ts + 30.0
            return token if (token and now_ts < expires_at) else ""

        _TOKEN_RETRY_AFTER = 0.0
        _TOKEN_CACHE["token"]      = new_token
        _TOKEN_CACHE["expires_at"] = float(new_expires_at)
        _TOKEN_CACHE["fetched_at"] = now_ts
        return new_token


# ----------------------------
# Helpers
# ----------------------------
def load_watch_party() -> dict:
    if os.path.exists(WATCH_PARTY_PATH):
        try:
            with open(WATCH_PARTY_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _openf1_headers() -> dict:
    headers = {"User-Agent": "OF1-Website"}
    api_key = os.getenv("OPENF1_API_KEY", "").strip()
    if api_key:
        headers["x-api-key"] = api_key
    bearer = _get_bearer_token()
    if bearer:
        header_name   = os.getenv("OPENF1_AUTH_HEADER_NAME", "Authorization").strip() or "Authorization"
        header_prefix = os.getenv("OPENF1_AUTH_HEADER_PREFIX", "Bearer").strip()
        headers[header_name] = f"{header_prefix} {bearer}".strip()
    return headers


def _openf1_get(endpoint: str, params: dict) -> list:
    cache_key = endpoint + str(sorted(params.items()))
    cached = _cache.get(cache_key)
    now_ts = time.time()

    # Try a fresh fetch first
    try:
        resp = requests.get(f"{OPENF1_BASE}/{endpoint}", params=params,
                            headers=_openf1_headers(), timeout=10)
        data = resp.json()
        # OpenF1 returns a dict with "detail" when restricted during live sessions
        if isinstance(data, list):
            _cache[cache_key] = (now_ts, data)
            return data
    except Exception:
        pass

    # Fall back to cached data if available
    if cached:
        return cached[1]
    return []


def get_current_race_weekend() -> dict | None:
    """Return race info if we're currently within a race weekend window, else None.
    Uses the same logic as the bot: fetch latest session, get all sessions for that
    meeting, check if now falls within the padded window."""
    try:
        now = datetime.now(timezone.utc)

        latest = _openf1_get("sessions", {"session_key": "latest"})
        if not latest or not isinstance(latest, list):
            return None

        meeting_key = latest[0].get("meeting_key")
        lat = latest[0]
        meeting_name = (
            lat.get("meeting_name")
            or lat.get("country_name")
            or lat.get("circuit_short_name")
            or "Race Weekend"
        )
        if not meeting_key:
            return None

        all_sessions = _openf1_get("sessions", {"meeting_key": meeting_key})
        if not all_sessions:
            return None

        starts, ends = [], []
        for s in all_sessions:
            ds = s.get("date_start")
            de = s.get("date_end")
            if ds:
                try:
                    starts.append(datetime.fromisoformat(str(ds).replace("Z", "+00:00")).astimezone(timezone.utc))
                except Exception:
                    pass
            if de:
                try:
                    ends.append(datetime.fromisoformat(str(de).replace("Z", "+00:00")).astimezone(timezone.utc))
                except Exception:
                    pass

        if not starts or not ends:
            return None

        window_start = min(starts) - timedelta(hours=PRE_HOURS)
        window_end   = max(ends)   + timedelta(hours=POST_HOURS)

        if window_start <= now <= window_end:
            # Find the race session for date/time display; fall back to last session
            race_session = None
            for s in all_sessions:
                st = str(s.get("session_type") or s.get("session_name") or "").upper()
                if st == "RACE":
                    race_session = s
                    break
            if race_session is None:
                dated = [(s, s.get("date_start")) for s in all_sessions if s.get("date_start")]
                if dated:
                    dated.sort(key=lambda x: x[1])
                    race_session = dated[-1][0]

            date_display = ""
            time_display = ""
            if race_session and race_session.get("date_start"):
                try:
                    dt_utc = datetime.fromisoformat(
                        str(race_session["date_start"]).replace("Z", "+00:00")
                    ).astimezone(timezone.utc)
                    dt_est = dt_utc.astimezone(EASTERN)
                    date_display = dt_est.strftime("%A, %B %-d")
                    time_display = dt_est.strftime("%-I:%M %p %Z")
                except Exception:
                    pass

            return {
                "race_name": meeting_name,
                "country": lat.get("country_name") or "",
                "circuit": lat.get("circuit_short_name") or "",
                "date_display": date_display,
                "time_display": time_display,
            }
    except Exception:
        pass
    return None


def get_next_session() -> dict | None:
    """Return the next upcoming F1 session (any type) from OpenF1."""
    try:
        now = datetime.now(timezone.utc)
        for year in [now.year, now.year + 1]:
            sessions = _openf1_get("sessions", {"year": year})
            upcoming = []
            for s in sessions:
                ds = s.get("date_start")
                if not ds:
                    continue
                try:
                    dt = datetime.fromisoformat(str(ds).replace("Z", "+00:00")).astimezone(timezone.utc)
                except Exception:
                    continue
                if dt > now:
                    upcoming.append((dt, s))
            if upcoming:
                upcoming.sort(key=lambda x: x[0])
                dt, s = upcoming[0]
                return {
                    "session_name": s.get("session_name") or "Next Session",
                    "meeting_name": s.get("meeting_name") or "",
                    "date_iso": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
    except Exception:
        pass
    return None


def get_next_race() -> dict | None:
    """Return the next upcoming F1 race from OpenF1."""
    try:
        now = datetime.now(timezone.utc)
        for year in [now.year, now.year + 1]:
            sessions = _openf1_get("sessions", {"session_type": "Race", "year": year})
            upcoming = []
            for s in sessions:
                dt_str = s.get("date_start")
                if not dt_str:
                    continue
                try:
                    dt = datetime.fromisoformat(
                        str(dt_str).replace("Z", "+00:00")
                    ).astimezone(timezone.utc)
                except Exception:
                    continue
                if dt > now:
                    upcoming.append((dt, s))
            if upcoming:
                upcoming.sort(key=lambda x: x[0])
                dt, s = upcoming[0]
                return {
                    "name": s.get("meeting_name") or s.get("session_name") or "Next Race",
                    "circuit": s.get("circuit_short_name") or "",
                    "country": s.get("country_name") or "",
                    "flag": s.get("country_code") or "",
                    "date_iso": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "date_display": dt.strftime("%B %d, %Y"),
                }
    except Exception:
        pass
    return None


# ----------------------------
# Routes
# ----------------------------
@app.route("/")
def index():
    current_race = get_current_race_weekend()
    watch_party = load_watch_party()
    if current_race:
        watch_party["active"] = True
        watch_party["title"] = current_race["race_name"]
        if current_race.get("date_display"):
            watch_party["date"] = current_race["date_display"]
        if current_race.get("time_display"):
            watch_party["time"] = current_race["time_display"]
        if not watch_party.get("location") and current_race.get("circuit"):
            watch_party.setdefault("subtitle", current_race["circuit"])
    return render_template(
        "index.html",
        watch_party=watch_party,
        next_race=get_next_race(),
        next_session=get_next_session(),
        discord_invite=DISCORD_INVITE,
        instagram_url=INSTAGRAM_URL,
        twitter_url=TWITTER_URL,
        tiktok_url=TIKTOK_URL,
        now=datetime.now(timezone.utc),
    )


@app.route("/api/next-race")
def api_next_race():
    return jsonify(get_next_race() or {})


@app.route("/api/debug")
def api_debug():
    static_token = os.getenv("OPENF1_BEARER_TOKEN", "")
    bearer = _get_bearer_token()
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    raw = requests.get(f"{OPENF1_BASE}/sessions", params={"session_key": "latest"},
                       headers=_openf1_headers(), timeout=10).json()
    return jsonify({
        "static_token_set": bool(static_token),
        "oauth_auth_url_set": bool(os.getenv("OPENF1_AUTH_URL")),
        "oauth_credentials_set": bool(os.getenv("OPENF1_AUTH_USERNAME") and os.getenv("OPENF1_AUTH_PASSWORD")),
        "bearer_token_active": bool(bearer),
        "bearer_prefix": bearer[:8] + "..." if bearer else None,
        "env_path": env_path,
        "env_file_exists": os.path.exists(env_path),
        "raw_latest_response": raw,
        "next_session": get_next_session(),
        "next_race": get_next_race(),
        "current_race_weekend": get_current_race_weekend(),
        "cache_keys": list(_cache.keys()),
    })


if __name__ == "__main__":
    port = int(os.getenv("WEBSITE_PORT", "5001"))
    debug = os.getenv("WEBSITE_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)

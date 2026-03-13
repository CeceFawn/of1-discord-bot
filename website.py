from __future__ import annotations

import json
import os
import requests
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, jsonify

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
CACHE_TTL  = 3600  # seconds — reuse data for 1 hour, including during live-session lockouts

_cache: dict = {}  # key -> (fetched_at, data)

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
    token = os.getenv("OPENF1_BEARER_TOKEN", "").strip()
    headers = {"User-Agent": "OF1-Website"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _openf1_get(endpoint: str, params: dict) -> list:
    import time
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
        meeting_name = latest[0].get("meeting_name") or "Race Weekend"
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
            return {"race_name": meeting_name}
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
            resp = requests.get(
                "https://api.openf1.org/v1/sessions",
                params={"session_type": "Race", "year": year},
                timeout=10,
            )
            resp.raise_for_status()
            sessions = resp.json()
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


if __name__ == "__main__":
    port = int(os.getenv("WEBSITE_PORT", "5001"))
    debug = os.getenv("WEBSITE_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)

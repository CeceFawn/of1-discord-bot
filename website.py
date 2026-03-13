from __future__ import annotations

import json
import os
import requests
from datetime import datetime, timezone
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
STATE_PATH = os.path.join(os.path.dirname(__file__), "state.json")

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


def get_active_race_thread() -> dict | None:
    """Return race info if the bot has an active race thread open, else None."""
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            state = json.load(f)
        threads = state.get("_buckets", {}).get("race_threads", {})
        for round_key, round_obj in threads.items():
            if not isinstance(round_obj, dict):
                continue
            race_name = round_obj.get("race_name") or ""
            guilds = round_obj.get("guilds", {})
            for guild_rec in guilds.values():
                if isinstance(guild_rec, dict) and guild_rec.get("weekend_state") == "active":
                    return {"race_name": race_name or "Race Weekend", "round_key": round_key}
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
    active_thread = get_active_race_thread()
    watch_party = load_watch_party()
    # If the bot has an active race thread, use it as the watch party title and mark active.
    # Manual watch_party.json fields (location, time, details) are still shown if present.
    if active_thread:
        watch_party["active"] = True
        watch_party["title"] = active_thread["race_name"]
    return render_template(
        "index.html",
        watch_party=watch_party,
        next_race=get_next_race(),
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

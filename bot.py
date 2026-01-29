import discord
from discord.ext import commands
import os
import logging
from dotenv import load_dotenv
import asyncio
from datetime import datetime, timezone
from dashboard import start_dashboard_thread, set_bot_reference
import requests
from bs4 import BeautifulSoup
import re
import json

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    filename="bot.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------------
# Helpers: .env write/update
# ----------------------------
def set_env_value(key: str, value: str, env_path: str = ".env") -> None:
    """Upsert KEY=VALUE into .env while preserving other lines."""
    lines = []
    found = False

    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip().startswith(f"{key}="):
                    lines.append(f"{key}={value}\n")
                    found = True
                else:
                    lines.append(line)

    if not found:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] = lines[-1] + "\n"
        lines.append(f"{key}={value}\n")

    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(lines)

    os.environ[key] = str(value)

# ----------------------------
# Race thread + event simulator (TEST)
# ----------------------------
RACE_EVENTS = {
    "green": ("🟢 GREEN FLAG", "Session running normally."),
    "vsc":   ("🟣 VIRTUAL SAFETY CAR", "Slow zone / VSC in effect."),
    "sc":    ("🟡 SAFETY CAR", "Safety Car deployed."),
    "red":   ("🔴 RED FLAG", "Session stopped (red flag)."),
    "yellow":("🟠 YELLOW FLAG", "Local yellow / incident in a sector."),
    "end":   ("🏁 SESSION ENDED", "Session concluded."),
}

# In-memory: active simulated threads (so you can send events to “the last one”)
ACTIVE_RACE_TESTS = {}  # guild_id -> thread_id


async def _get_forum_channel(ctx):
    forum_id = os.getenv("RACE_FORUM_CHANNEL_ID")
    if not forum_id:
        await ctx.send("❌ RACE_FORUM_CHANNEL_ID is not set in .env")
        return None

    try:
        ch = bot.get_channel(int(forum_id)) or await bot.fetch_channel(int(forum_id))
    except Exception as e:
        await ctx.send(f"❌ Could not fetch forum channel {forum_id}: {e}")
        return None

    # This is the key: must be a ForumChannel
    if not isinstance(ch, discord.ForumChannel):
        await ctx.send(f"❌ Channel {forum_id} is not a Forum channel. (It is: {type(ch).__name__})")
        return None

    return ch


def _maybe_ping_text():
    rid = os.getenv("RACE_ALERT_ROLE_ID", "0").strip()
    if rid and rid != "0":
        return f"<@&{rid}> "
    return ""


async def _post_race_event(thread: discord.Thread, event_key: str, extra: str = ""):
    event_key = event_key.lower().strip()
    if event_key not in RACE_EVENTS:
        valid = ", ".join(RACE_EVENTS.keys())
        await thread.send(f"❌ Unknown event `{event_key}`. Valid: {valid}")
        return

    title, desc = RACE_EVENTS[event_key]
    ping = _maybe_ping_text()

    msg = f"{ping}**{title}** — {desc}"
    if extra:
        msg += f"\n> {extra}"

    await thread.send(msg)


@bot.command()
@commands.has_permissions(administrator=True)
async def race_test_create(ctx, *, name: str):
    """
    Creates a TEST forum post thread in the Race Threads forum.
    Usage: !race_test_create Bahrain GP - Quali
    """
    forum = await _get_forum_channel(ctx)
    if forum is None:
        return

    # Create the forum "post" (thread)
    # discord.py v2: ForumChannel.create_thread(name=..., content=...)
    try:
        thread = await forum.create_thread(
            name=f"TEST — {name}",
            content=f"🧪 Test thread created by {ctx.author.mention} for: **{name}**"
        )
    except Exception as e:
        await ctx.send(f"❌ Failed to create forum post: {e}")
        return

    # thread is a ThreadWithMessage in some versions; normalize:
    created_thread = getattr(thread, "thread", thread)

    ACTIVE_RACE_TESTS[ctx.guild.id] = created_thread.id
    await ctx.send(f"✅ Created test race thread: {created_thread.mention}")

    # Immediately post a green flag message so you can see it works
    await _post_race_event(created_thread, "green", "Auto test: initial green flag message.")

    # Optional auto-delete timer
    try:
        mins = int(os.getenv("RACE_TEST_AUTODELETE_MINUTES", "0") or "0")
    except ValueError:
        mins = 0

    if mins > 0:
        async def _autodelete():
            await asyncio.sleep(mins * 60)
            try:
                await created_thread.delete()
            except:
                pass
        bot.loop.create_task(_autodelete())


@bot.command()
@commands.has_permissions(administrator=True)
async def race_test_event(ctx, event: str, *, note: str = ""):
    """
    Sends a test event into the latest created test thread.
    Usage: !race_test_event red Big crash at T1
           !race_test_event sc Debris on track
    """
    thread_id = ACTIVE_RACE_TESTS.get(ctx.guild.id)
    if not thread_id:
        await ctx.send("❌ No active test thread. Run `!race_test_create <name>` first.")
        return

    try:
        thread = bot.get_channel(thread_id) or await bot.fetch_channel(thread_id)
    except Exception as e:
        await ctx.send(f"❌ Could not fetch thread {thread_id}: {e}")
        return

    await _post_race_event(thread, event, note)
    await ctx.send(f"✅ Sent `{event}` event to {thread.mention}")


@bot.command()
@commands.has_permissions(administrator=True)
async def race_test_demo(ctx, *, name: str):
    """
    Creates a test thread then runs a full demo sequence automatically.
    Usage: !race_test_demo Bahrain GP - Race
    """
    forum = await _get_forum_channel(ctx)
    if forum is None:
        return

    try:
        thread = await forum.create_thread(
            name=f"TEST DEMO — {name}",
            content=f"🧪 Demo thread created by {ctx.author.mention} for: **{name}**"
        )
    except Exception as e:
        await ctx.send(f"❌ Failed to create forum post: {e}")
        return

    t = getattr(thread, "thread", thread)
    ACTIVE_RACE_TESTS[ctx.guild.id] = t.id
    await ctx.send(f"✅ Created demo thread: {t.mention}")

    # Demo sequence (you can tweak timing)
    sequence = [
        ("green", "Session start"),
        ("yellow", "Local yellow sector 2"),
        ("vsc", "VSC deployed"),
        ("green", "VSC ending, back to green"),
        ("sc", "Safety Car deployed"),
        ("red", "Barrier repair needed"),
        ("green", "Restart underway"),
        ("end", "Session over"),
    ]

    for key, note in sequence:
        await _post_race_event(t, key, note)
        await asyncio.sleep(3)  # spacing so you can watch it

    await ctx.send("✅ Demo sequence completed.")


# ----------------------------
# Instagram scrape
# ----------------------------
def fetch_latest_instagram_post(username):
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
# Standings (Ergast API)
# ----------------------------
ERGAST_DRIVER_URL = "https://ergast.com/api/f1/current/driverStandings.json"
ERGAST_CONSTRUCTOR_URL = "https://ergast.com/api/f1/current/constructorStandings.json"

def _get_json(url: str):
    r = requests.get(url, timeout=20, headers={"User-Agent": "OF1-Discord-Bot"})
    r.raise_for_status()
    return r.json()

async def fetch_driver_standings_text(limit: int = 20) -> str:
    data = await asyncio.to_thread(_get_json, ERGAST_DRIVER_URL)
    lists = data["MRData"]["StandingsTable"]["StandingsLists"]
    if not lists:
        return "No standings available."
    standings = lists[0]["DriverStandings"][:limit]

    lines = []
    for s in standings:
        pos = s.get("position", "?")
        pts = s.get("points", "0")
        drv = s["Driver"]
        given = drv.get("givenName", "")
        family = drv.get("familyName", "")
        constructor = (s.get("Constructors") or [{}])[0].get("name", "")
        lines.append(f"{pos:>2}. {given} {family} — {pts} pts ({constructor})")

    updated = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return "🏁 **F1 Driver Standings (Current Season)**\n" + "\n".join(lines) + f"\n\n_Last updated: {updated}_"

async def fetch_constructor_standings_text(limit: int = 10) -> str:
    data = await asyncio.to_thread(_get_json, ERGAST_CONSTRUCTOR_URL)
    lists = data["MRData"]["StandingsTable"]["StandingsLists"]
    if not lists:
        return "No standings available."
    standings = lists[0]["ConstructorStandings"][:limit]

    lines = []
    for s in standings:
        pos = s.get("position", "?")
        pts = s.get("points", "0")
        name = s["Constructor"].get("name", "")
        lines.append(f"{pos:>2}. {name} — {pts} pts")

    updated = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return "🏁 **F1 Constructor Standings (Current Season)**\n" + "\n".join(lines) + f"\n\n_Last updated: {updated}_"

# ----------------------------
# Load env + Discord setup
# ----------------------------
load_dotenv()

intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.guilds = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)
set_bot_reference(bot)

# ----------------------------
# Reaction role maps
# ----------------------------
reaction_roles = {
    "🏎": "F1Breaking",
    "🎥": "Watch Party",
    "📸": "OF1 Instagram"
}

color_reactions = {
    "🔴": "Ferrari Red",
    "🔵": "Alpine Blue",
    "🟢": "Kick/Sauber Green",
    "🟣": "Purple",
    "🟠": "Papaya Orange",
    "🟡": "Renault Yellow",
    "⚪": "White",
    "🩶": "Mercedes Silver",
    "💗": "Pink",
    "🩵": "Cyan",
    "🟤": "Brown"
}

driver_reactions = {}
COLOR_ROLE_NAMES = list(color_reactions.values())

# ----------------------------
# Commands: Reaction role setup
# ----------------------------
@bot.command()
@commands.has_permissions(administrator=True)
async def setup_colors(ctx):
    description = "🎨 **Choose your name color!**\nReact with an emoji to get a matching role. Only one color can be active at a time."
    for emoji, role in color_reactions.items():
        description += f"\n{emoji} → `{role}`"
    message = await ctx.send(description)
    for emoji in color_reactions.keys():
        await message.add_reaction(emoji)
    logging.info(f"[Color Roles] Setup complete (Message ID: {message.id})")

@bot.command()
@commands.has_permissions(administrator=True)
async def setup_drivers(ctx):
    description = "🏎 **Choose your favorite F1 driver!**\nReact to get a fan role:"
    guild = ctx.guild
    emoji_to_role = {}

    emoji_name_map = {
        "Piastri": "Piastri",
        "Norris": "Norris",
        "Russell": "Russell",
        "Antonelli": "Antonelli",
        "Verstappen": "Verstappen",
        "Tsunoda": "Tsunoda",
        "Leclerc": "Leclerc",
        "Hamilton": "Hamilton",
        "Albon": "Albon",
        "Sainz": "Sainz",
        "Ocon": "Ocon",
        "Bearman": "Bearman",
        "Stroll": "Stroll",
        "Alonso": "Alonso",
        "Hadjar": "Hadjar",
        "Lawson": "Lawson",
        "Gasly": "Gasly",
        "Colapinto": "Colapinto",
        "Hulkenberg": "Hulkenberg",
        "Bortoleto": "Bortoleto"
    }

    missing = []
    for emoji_name, role_name in emoji_name_map.items():
        emoji = discord.utils.get(guild.emojis, name=emoji_name)
        if emoji:
            emoji_to_role[str(emoji)] = role_name
            description += f"\n{emoji} → `{role_name}`"
        else:
            missing.append(emoji_name)

    if missing:
        await ctx.send("⚠️ Missing custom emojis: " + ", ".join(missing))

    message = await ctx.send(description)
    for emoji_str in emoji_to_role.keys():
        await message.add_reaction(emoji_str)

    global driver_reactions
    driver_reactions = emoji_to_role
    logging.info(f"[Driver Roles] Setup complete (Message ID: {message.id})")

@bot.command()
@commands.has_permissions(administrator=True)
async def setup_notifications(ctx):
    description = "📰 **Get notified!**\nReact to opt in to pingable news roles."
    for emoji, role in reaction_roles.items():
        description += f"\n{emoji} → `{role}`"
    message = await ctx.send(description)
    for emoji in reaction_roles.keys():
        await message.add_reaction(emoji)
    logging.info(f"[Notification Roles] Setup complete (Message ID: {message.id})")

# ----------------------------
# Commands: Instagram quick check
# ----------------------------
@bot.command()
@commands.has_permissions(administrator=True)
async def insta_check(ctx, username: str = "of1.official"):
    post_url = fetch_latest_instagram_post(username)
    if post_url:
        await ctx.send(f"📸 Latest Instagram post from `{username}`:\n{post_url}")
    else:
        await ctx.send("❌ Could not retrieve the latest Instagram post.")

# ----------------------------
# Commands: message edit utilities etc.
# ----------------------------
@bot.command()
@commands.has_permissions(administrator=True)
async def editmsg(ctx, channel_id: int, message_id: int, *, new_text: str):
    channel = bot.get_channel(channel_id)
    if not channel:
        await ctx.send("❌ Could not find that channel.")
        return
    try:
        message = await channel.fetch_message(message_id)
        if message.author != bot.user:
            await ctx.send("⚠️ I can only edit my own messages.")
            return
        await message.edit(content=new_text)
        await ctx.send("✅ Message updated.")
    except Exception as e:
        await ctx.send(f"❌ Failed to edit message: {e}")

@bot.command()
@commands.has_permissions(administrator=True)
async def botinfo(ctx):
    uptime = datetime.now() - bot.launch_time
    await ctx.send(f"🛠 **Bot Uptime:** {uptime}")

@bot.command()
@commands.has_permissions(administrator=True)
async def serverlist(ctx):
    guild_names = ", ".join(g.name for g in bot.guilds)
    await ctx.send(f"🤖 Connected to: {guild_names}")

@bot.command()
@commands.has_permissions(administrator=True)
async def logrecent(ctx, lines: int = 10):
    try:
        with open("bot.log", "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        filtered = [line for line in all_lines if not any(x in line for x in ["/logs", "favicon.ico"])]
        await ctx.send(f"```\n{''.join(filtered[-lines:])}```")
    except Exception as e:
        await ctx.send(f"❌ Could not read log: {e}")

@bot.command()
async def remindme(ctx, time: str, *, reminder: str):
    try:
        run_time = datetime.fromisoformat(time)
        delay = (run_time - datetime.now()).total_seconds()
        if delay <= 0:
            await ctx.send("❌ Time must be in the future.")
            return

        async def remind():
            await asyncio.sleep(delay)
            await ctx.send(f"⏰ Reminder: {reminder}")

        bot.loop.create_task(remind())
        await ctx.send(f"✅ Reminder set for {run_time}.")
    except Exception:
        await ctx.send("❌ Invalid time format. Use YYYY-MM-DDTHH:MM. Example: 2025-05-05T14:00")

@bot.command()
async def ping(ctx):
    await ctx.send("Pong!")

@bot.command()
async def help(ctx):
    visible = []
    for cmd in bot.commands:
        try:
            if await cmd.can_run(ctx):
                visible.append(f"!{cmd.name} - {cmd.help or 'No description'}")
        except:
            continue
    if visible:
        await ctx.send("**Available Commands:**\n" + "\n".join(visible))
    else:
        await ctx.send("❌ You don't have access to any commands.")

# ----------------------------
# Standings: updater loop
# ----------------------------
STANDINGS_TASK = None

def _get_refresh_seconds() -> int:
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
    if driver_msg_id:
        try:
            msg = await channel.fetch_message(int(driver_msg_id))
            await msg.edit(content=await fetch_driver_standings_text())
        except Exception as e:
            logging.error(f"[Standings] Driver update failed: {e}")

    constructor_msg_id = os.getenv("CONSTRUCTOR_STANDINGS_MESSAGE_ID")
    if constructor_msg_id:
        try:
            msg = await channel.fetch_message(int(constructor_msg_id))
            await msg.edit(content=await fetch_constructor_standings_text())
        except Exception as e:
            logging.error(f"[Standings] Constructor update failed: {e}")

async def standings_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        await update_standings_once()
        await asyncio.sleep(_get_refresh_seconds())

def ensure_standings_task_running():
    global STANDINGS_TASK
    if STANDINGS_TASK is None or STANDINGS_TASK.done():
        STANDINGS_TASK = bot.loop.create_task(standings_loop())
        logging.info("[Standings] Loop started.")

# ----------------------------
# Race feature: OpenF1 Race Control -> Discord Forum Posts
# ----------------------------
OPENF1_BASE = "https://api.openf1.org/v1"
RACE_STATE_PATH = "race_state.json"
RACE_TASK = None

def load_race_state() -> dict:
    if not os.path.exists(RACE_STATE_PATH):
        return {}
    try:
        with open(RACE_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_race_state(state: dict) -> None:
    try:
        with open(RACE_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass

def _openf1_get(endpoint: str, params: dict | None = None):
    url = f"{OPENF1_BASE}/{endpoint.lstrip('/')}"
    r = requests.get(url, params=params or {}, timeout=20, headers={"User-Agent": "OF1-Discord-Bot"})
    r.raise_for_status()
    return r.json()

async def get_current_session_context():
    """
    Finds the current/most recent session under meeting_key=latest.
    Returns dict: {meeting_key, meeting_name, session_key, session_name}
    """
    sessions = await asyncio.to_thread(_openf1_get, "sessions", {"meeting_key": "latest"})
    if not sessions:
        return None

    # Pick active session if possible; else most recent
    now = datetime.now(timezone.utc)

    def parse_dt(s: str):
        try:
            # OpenF1 uses ISO strings
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None

    active = None
    latest = None
    latest_start = None

    for s in sessions:
        ds = parse_dt(s.get("date_start", "") or "")
        de = parse_dt(s.get("date_end", "") or "")
        if ds and de and ds <= now <= de:
            active = s
            break
        if ds and (latest_start is None or ds > latest_start):
            latest_start = ds
            latest = s

    chosen = active or latest
    if not chosen:
        return None

    meeting_key = chosen.get("meeting_key")
    session_key = chosen.get("session_key")
    session_name = chosen.get("session_name") or "Session"

    meeting_name = "F1 Weekend"
    try:
        meetings = await asyncio.to_thread(_openf1_get, "meetings", {"meeting_key": meeting_key})
        if meetings and isinstance(meetings, list):
            meeting_name = meetings[0].get("meeting_name") or meeting_name
    except Exception:
        pass

    return {
        "meeting_key": meeting_key,
        "meeting_name": meeting_name,
        "session_key": session_key,
        "session_name": session_name,
    }

async def ensure_forum_thread(forum_channel: discord.ForumChannel, meeting_key: int, meeting_name: str) -> int:
    """
    Ensures a forum thread exists for the current meeting_key.
    Returns thread_id.
    """
    state = load_race_state()
    threads = state.get("threads", {})
    key = str(meeting_key)

    if key in threads:
        return int(threads[key])

    # Create a new post in the forum
    title = f"{meeting_name} — Race Weekend"
    content = f"🏁 **{meeting_name}**\nLive session alerts will be posted here."

    created_thread_id = None

    # discord.py versions differ; try common patterns
    try:
        created = await forum_channel.create_thread(name=title, content=content)
        # could be Thread or (Thread, Message)
        if isinstance(created, tuple):
            thread = created[0]
        else:
            thread = created
        created_thread_id = thread.id
    except Exception as e:
        logging.error(f"[Race] Failed to create forum thread: {e}")
        raise

    threads[key] = str(created_thread_id)
    state["threads"] = threads
    save_race_state(state)
    return created_thread_id

def classify_race_control_item(item: dict) -> tuple[bool, str]:
    """
    Decide if we should post this item.
    Returns (should_post, formatted_text)
    """
    category = (item.get("category") or "").upper()
    flag = (item.get("flag") or "").upper()
    message = (item.get("message") or "").strip()
    lap = item.get("lap_number")

    # Keywords / flags we care about
    important_flags = {"RED", "YELLOW", "GREEN", "SAFETY CAR", "VIRTUAL SAFETY CAR", "VSC", "SC", "DOUBLE YELLOW"}
    important_words = ("RED FLAG", "SAFETY CAR", "VSC", "GREEN FLAG", "SESSION START", "SESSION ENDED", "YELLOW FLAG")

    should_post = False
    if flag and any(k in flag for k in important_flags):
        should_post = True
    if any(w in message.upper() for w in important_words):
        should_post = True

    if not should_post:
        return (False, "")

    icon = "ℹ️"
    if "RED" in flag or "RED FLAG" in message.upper():
        icon = "🟥"
    elif "SAFETY CAR" in flag or "SAFETY CAR" in message.upper() or "SC" == flag:
        icon = "🚗"
    elif "VIRTUAL" in flag or "VSC" in flag or "VSC" in message.upper():
        icon = "🟨"
    elif "GREEN" in flag or "GREEN FLAG" in message.upper():
        icon = "🟢"
    elif "YELLOW" in flag or "YELLOW FLAG" in message.upper():
        icon = "🟡"

    lap_txt = f" (Lap {lap})" if lap is not None else ""
    text = f"{icon} **{flag or category or 'Update'}**{lap_txt}\n{message}"
    return (True, text)

async def race_alert_loop():
    await bot.wait_until_ready()

    forum_id = os.getenv("RACE_FORUM_CHANNEL_ID")
    if not forum_id:
        logging.info("[Race] RACE_FORUM_CHANNEL_ID missing; race alerts disabled.")
        return

    try:
        poll_seconds = int(os.getenv("RACE_POLL_SECONDS", "15"))
    except ValueError:
        poll_seconds = 15
    poll_seconds = max(5, min(60, poll_seconds))

    role_id = os.getenv("RACE_ALERT_ROLE_ID")  # optional

    state = load_race_state()
    last_seen_date = state.get("last_seen_date")  # OpenF1 race_control 'date' string

    while not bot.is_closed():
        try:
            forum_channel = await bot.fetch_channel(int(forum_id))
            if not isinstance(forum_channel, discord.ForumChannel):
                logging.error("[Race] RACE_FORUM_CHANNEL_ID is not a ForumChannel.")
                await asyncio.sleep(60)
                continue

            ctx = await get_current_session_context()
            if not ctx:
                await asyncio.sleep(poll_seconds)
                continue

            meeting_key = ctx["meeting_key"]
            meeting_name = ctx["meeting_name"]
            session_key = ctx["session_key"]
            session_name = ctx["session_name"]

            thread_id = await ensure_forum_thread(forum_channel, meeting_key, meeting_name)
            thread = await bot.fetch_channel(thread_id)

            params = {"session_key": session_key}
            if last_seen_date:
                # OpenF1 supports filters like date=>=
                params["date"] = f">={last_seen_date}"

            items = await asyncio.to_thread(_openf1_get, "race_control", params)

            # Sort by date so we post in order
            def _dt(item):
                d = item.get("date") or ""
                try:
                    return datetime.fromisoformat(d.replace("Z", "+00:00"))
                except Exception:
                    return datetime.min.replace(tzinfo=timezone.utc)

            items = sorted(items, key=_dt)

            for it in items:
                date_str = it.get("date")
                if not date_str:
                    continue

                # Dedup: if equal to last_seen_date, skip
                if last_seen_date and date_str <= last_seen_date:
                    continue

                ok, text = classify_race_control_item(it)
                if not ok:
                    last_seen_date = date_str
                    continue

                # Add session name prefix so the post makes sense
                content = f"🏁 **{meeting_name} — {session_name}**\n{text}"

                # Optional ping on big events
                if role_id:
                    upper = (it.get("flag") or "").upper() + " " + (it.get("message") or "").upper()
                    if ("RED" in upper) or ("SAFETY CAR" in upper) or ("VSC" in upper) or ("VIRTUAL" in upper):
                        content = f"<@&{role_id}>\n" + content

                await thread.send(content)
                last_seen_date = date_str

            # Persist last seen
            state = load_race_state()
            state["last_seen_date"] = last_seen_date
            save_race_state(state)

        except Exception as e:
            logging.error(f"[Race] Error in race loop: {e}")

        await asyncio.sleep(poll_seconds)

def ensure_race_task_running():
    global RACE_TASK
    if RACE_TASK is None or RACE_TASK.done():
        RACE_TASK = bot.loop.create_task(race_alert_loop())
        logging.info("[Race] Race alert loop started.")

# ----------------------------
# Reaction role handlers
# ----------------------------
@bot.event
async def on_ready():
    logging.info(f"Bot is online as {bot.user}")
    bot.launch_time = datetime.now()

    bot.loop.create_task(periodic_reaction_role_check())
    ensure_standings_task_running()
    ensure_race_task_running()

@bot.event
async def on_raw_reaction_add(payload):
    if payload.user_id == bot.user.id:
        return
    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return

    emoji_str = str(payload.emoji)
    role_name = (
        reaction_roles.get(emoji_str) or
        color_reactions.get(emoji_str) or
        driver_reactions.get(emoji_str)
    )
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

    if role_name in COLOR_ROLE_NAMES:
        roles_to_remove = [
            discord.utils.get(guild.roles, name=r)
            for r in COLOR_ROLE_NAMES if r != role_name
        ]
        await member.remove_roles(*[r for r in roles_to_remove if r and r in member.roles])

    await member.add_roles(role)
    logging.info(f"[Roles] Assigned '{role_name}' to {member.name}")

@bot.event
async def on_raw_reaction_remove(payload):
    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return

    emoji_str = str(payload.emoji)
    role_name = (
        reaction_roles.get(emoji_str) or
        color_reactions.get(emoji_str) or
        driver_reactions.get(emoji_str)
    )
    if not role_name:
        return

    role = discord.utils.get(guild.roles, name=role_name)
    if role is None:
        return

    try:
        member = await guild.fetch_member(payload.user_id)
    except Exception:
        return

    await member.remove_roles(role)
    logging.info(f"[Roles] Removed '{role_name}' from {member.name}")

async def periodic_reaction_role_check():
    await bot.wait_until_ready()
    while not bot.is_closed():
        for guild in bot.guilds:
            for channel in guild.text_channels:
                me = guild.me
                if me is None:
                    continue
                perms = channel.permissions_for(me)
                if not (perms.read_message_history and perms.view_channel):
                    continue

                try:
                    async for message in channel.history(limit=100):
                        if message.author != bot.user:
                            continue

                        for reaction in message.reactions:
                            emoji = str(reaction.emoji).strip()
                            role_name = (
                                reaction_roles.get(emoji) or
                                color_reactions.get(emoji) or
                                driver_reactions.get(emoji)
                            )
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
                                        if role_name in COLOR_ROLE_NAMES:
                                            roles_to_remove = [
                                                discord.utils.get(guild.roles, name=r)
                                                for r in COLOR_ROLE_NAMES if r != role_name
                                            ]
                                            await member.remove_roles(*[r for r in roles_to_remove if r and r in member.roles])
                                        await member.add_roles(role)
                                        logging.info(f"[Recovery] Reassigned '{role_name}' to {member.name}")
                                except discord.Forbidden:
                                    logging.warning(f"[Access Denied] Cannot fetch member {user.id} in guild '{guild.name}'")
                                except Exception as e:
                                    logging.warning(f"[Recovery] Error for user {user.id}: {e}")
                except Exception as e:
                    logging.error(f"[Error during periodic check] {e}")

        await asyncio.sleep(3600)

# ----------------------------
# Start dashboard + run bot
# ----------------------------
start_dashboard_thread()

bot_token = os.getenv("DISCORD_BOT_TOKEN")
if not bot_token:
    raise RuntimeError("DISCORD_BOT_TOKEN is missing. Put it in your .env file.")

bot.run(bot_token)

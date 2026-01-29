# bot.py
from __future__ import annotations

import os
import re
import logging
import asyncio
from datetime import datetime
from typing import Dict, Optional, Any

import discord
from discord.ext import commands
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

from dashboard import start_dashboard_thread, set_bot_reference
from storage import load_config, save_config, load_state, save_state, set_env_value
from settings import LOG_PATH

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ----------------------------
# Load env
# ----------------------------
load_dotenv()

# ----------------------------
# Config + State (global in-memory)
# ----------------------------
CFG: Dict[str, Any] = {}
STATE: Dict[str, Any] = {}

def reload_config_state() -> None:
    global CFG, STATE
    CFG = load_config() or {}
    STATE = load_state() or {}

# Load once at import time
reload_config_state()

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

def _get_json(url: str):
    r = requests.get(url, timeout=20, headers={"User-Agent": "OF1-Discord-Bot"})
    r.raise_for_status()
    return r.json()

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

def get_prefix() -> str:
    return (CFG.get("prefix") or "!").strip() or "!"

bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)
set_bot_reference(bot)

# ----------------------------
# Helpers: role mapping
# ----------------------------
def cfg_reaction_roles() -> Dict[str, str]:
    return dict(CFG.get("reaction_roles") or {})

def cfg_color_roles() -> Dict[str, str]:
    return dict(CFG.get("color_roles") or {})

def cfg_driver_emoji_names() -> Dict[str, str]:
    """
    Mapping of custom emoji NAME -> role name.
    Example: {"Piastri":"Piastri"}
    """
    return dict(CFG.get("driver_emoji_names") or {})

def color_role_names() -> set[str]:
    return set((CFG.get("color_roles") or {}).values())

def state_driver_map() -> Dict[str, str]:
    # emoji string (e.g. "<:Piastri:123>") -> role name
    return dict(((STATE.get("driver_roles") or {}).get("emoji_to_role")) or {})

def write_state_driver_map(channel_id: int, message_id: int, emoji_to_role: Dict[str, str]) -> None:
    global STATE
    if "driver_roles" not in STATE:
        STATE["driver_roles"] = {}
    STATE["driver_roles"]["channel_id"] = str(channel_id)
    STATE["driver_roles"]["message_id"] = str(message_id)
    STATE["driver_roles"]["emoji_to_role"] = dict(emoji_to_role)
    save_state(STATE)

def resolve_role_name_from_emoji(emoji_str: str) -> Optional[str]:
    # order matters: notifications + colors + drivers(state)
    return (
        cfg_reaction_roles().get(emoji_str)
        or cfg_color_roles().get(emoji_str)
        or state_driver_map().get(emoji_str)
    )

# ----------------------------
# Commands: config tools
# ----------------------------
@bot.command()
@commands.has_permissions(administrator=True)
async def config_reload(ctx):
    """Reload config.json + state.json without restarting the bot."""
    reload_config_state()
    await ctx.send("✅ Reloaded config.json and state.json.")

# ----------------------------
# Commands: reaction role setup
# ----------------------------
@bot.command()
@commands.has_permissions(administrator=True)
async def setup_notifications(ctx):
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

    logging.info(f"[Notification Roles] Setup complete (Message ID: {msg.id})")
    await ctx.send(f"✅ Notifications setup message created: `{msg.id}`")

@bot.command()
@commands.has_permissions(administrator=True)
async def setup_colors(ctx):
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

    logging.info(f"[Color Roles] Setup complete (Message ID: {msg.id})")
    await ctx.send(f"✅ Colors setup message created: `{msg.id}`")

@bot.command()
@commands.has_permissions(administrator=True)
async def setup_drivers(ctx):
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
@bot.command()
@commands.has_permissions(administrator=True)
async def insta_check(ctx, username: str = "of1.official"):
    post_url = fetch_latest_instagram_post(username)
    if post_url:
        await ctx.send(f"📸 Latest Instagram post from `{username}`:\n{post_url}")
    else:
        await ctx.send("❌ Could not retrieve the latest Instagram post.")

# ----------------------------
# Utility commands
# ----------------------------
@bot.command()
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
        with open(LOG_PATH, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        await ctx.send(f"```\n{''.join(all_lines[-lines:])}```")
    except Exception as e:
        await ctx.send(f"❌ Could not read log: {e}")

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
        except Exception:
            continue
    if visible:
        await ctx.send("**Available Commands:**\n" + "\n".join(visible))
    else:
        await ctx.send("❌ You don't have access to any commands.")

# ----------------------------
# Standings updater
# ----------------------------
STANDINGS_TASK: Optional[asyncio.Task] = None

def _refresh_seconds() -> int:
    # env is the source of truth for these (and can be set by command)
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
        await asyncio.sleep(_refresh_seconds())

def ensure_standings_task_running():
    global STANDINGS_TASK
    if STANDINGS_TASK is None or STANDINGS_TASK.done():
        STANDINGS_TASK = bot.loop.create_task(standings_loop())
        logging.info("[Standings] Loop started.")

@bot.command()
@commands.has_permissions(administrator=True)
async def standings_setup(ctx, which: str = "both", refresh_minutes: int = 5):
    """
    Create standings message(s), store IDs in .env (and mirror into state.json), then auto-update them.
    Usage:
      !standings_setup drivers 5
      !standings_setup constructors 10
      !standings_setup both 5
    """
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

    # Mirror into state.json (optional but handy)
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
# Reaction role handlers + periodic recovery
# ----------------------------
@bot.event
async def on_ready():
    logging.info(f"Bot is online as {bot.user}")
    bot.launch_time = datetime.now()

    # Reload state/config in case dashboard edited files while bot was down
    reload_config_state()

    ensure_standings_task_running()
    bot.loop.create_task(periodic_reaction_role_check())

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id:
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

    # Enforce single-color role
    if role_name in color_role_names():
        roles_to_remove = [
            discord.utils.get(guild.roles, name=rname)
            for rname in color_role_names()
            if rname != role_name
        ]
        await member.remove_roles(*[r for r in roles_to_remove if r and r in member.roles])

    await member.add_roles(role)
    logging.info(f"[Roles] Assigned '{role_name}' to {member.name}")

@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
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

    await member.remove_roles(role)
    logging.info(f"[Roles] Removed '{role_name}' from {member.name}")

async def periodic_reaction_role_check():
    """
    Re-applies roles based on reactions to the bot's messages.
    Uses config.json + state.json mappings.
    """
    await bot.wait_until_ready()

    interval_min = int(CFG.get("periodic_role_recovery_minutes", 60))
    interval_min = max(5, min(240, interval_min))
    scan_limit = int(CFG.get("periodic_history_scan_limit", 100))
    scan_limit = max(10, min(1000, scan_limit))

    while not bot.is_closed():
        try:
            reload_config_state()

            for guild in bot.guilds:
                me = guild.me
                if me is None:
                    continue

                for channel in guild.text_channels:
                    perms = channel.permissions_for(me)
                    if not (perms.view_channel and perms.read_message_history):
                        continue

                    async for message in channel.history(limit=scan_limit):
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
                                            await member.remove_roles(*[r for r in roles_to_remove if r and r in member.roles])
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
# Start dashboard + run bot
# ----------------------------
start_dashboard_thread()

bot_token = os.getenv("DISCORD_BOT_TOKEN")
if not bot_token:
    raise RuntimeError("DISCORD_BOT_TOKEN is missing. Put it in your .env file.")

bot.run(bot_token)

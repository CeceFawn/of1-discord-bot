from __future__ import annotations

import os
import re
import logging
import asyncio
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Any, List, Tuple
from collections import deque

import discord
from discord.ext import commands
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import aiohttp

from dashboard import start_dashboard_thread, set_bot_reference
from storage import load_config, save_config, load_state, save_state, set_env_value
from settings import LOG_PATH

import io
from PIL import Image, ImageDraw, ImageFont

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
    if template_path:
        base = Image.open(template_path).convert("RGBA").resize((W, H), Image.LANCZOS)
    else:
        base = Image.new("RGBA", (W, H), (24, 26, 32, 255))

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
    try:
        font_name = ImageFont.truetype("fonts/Inter-SemiBold.ttf", 36)
        font_small = ImageFont.truetype("fonts/Inter-Regular.ttf", 22)
        font_tiny = ImageFont.truetype("fonts/Inter-Regular.ttf", 18)
    except Exception:
        font_name = ImageFont.load_default()
        font_small = ImageFont.load_default()
        font_tiny = ImageFont.load_default()

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
# XP State (global in-memory)
# ----------------------------
XP_STATE: Dict[str, Any] = load_xp_state()
XP_DIRTY: bool = False
XP_SAVE_LOCK = asyncio.Lock()

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
                    save_xp_state(XP_STATE)
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

def get_prefix(bot, message) -> str:
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

    # replace these with your real XP/level values
    level = 7
    xp = 320
    xp_next = 500
    title = "Regular"

    png_bytes = await build_rank_card_png(
        member=member,
        level=level,
        xp=xp,
        xp_next=xp_next,
        title=title,
        template_path = "assets/rank_template.png"
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
    await ctx.send("✅ Reloaded config.json and state.json.")

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
    post_url = fetch_latest_instagram_post(username)
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
        with open(LOG_PATH, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        await ctx.send(f"```\n{''.join(all_lines[-lines:])}```")
    except Exception as e:
        await ctx.send(f"❌ Could not read log: {e}")

@bot.command(name="ping")
async def ping(ctx):
    await ctx.send("Pong!")

@bot.command(name="help")
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
# Reaction role handlers + periodic recovery
# ----------------------------
@bot.event
async def on_ready():
    logging.info(f"Bot is online as {bot.user}")
    bot.launch_time = datetime.now()

    reload_config_state()

    ensure_standings_task_running()
    bot.loop.create_task(periodic_reaction_role_check())

    # XP flushing loop
    bot.loop.create_task(xp_flush_loop())

    # Race supervisor loop (your existing module)
    bot.loop.create_task(race_supervisor_loop())

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

                sigs = RACE_LIVE_POSTED_SIGS[gid]
                for item in rc[-30:]:
                    msg = str(item.get("message") or "").strip()
                    if not msg:
                        continue
                    dt = str(item.get("date") or "")
                    sig = f"{dt}|{msg}"
                    if sig in sigs:
                        continue
                    sigs.add(sig)
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

                        RACE_LIVE_TASKS[gid] = bot.loop.create_task(runner())

                    if (not in_window) and running:
                        _racelog(gid, "Supervisor stopping live loop (out of window)")
                        RACE_LIVE_ENABLED[gid] = False
                        task.cancel()

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

    RACE_LIVE_TASKS[gid] = bot.loop.create_task(runner())
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

    await thread.send("✅ Scenario complete.")

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

    task = bot.loop.create_task(runner())
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

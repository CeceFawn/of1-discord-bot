from flask import Flask, render_template_string, request, redirect
import threading
import os
import json
import psutil
from datetime import datetime

app = Flask(__name__)

LOG_FILTERS = [
    "favicon.ico", "Bad request syntax", "Invalid HTTP version",
    "code 400", "code 404", "code 505", "successfully RESUMED",
    "GET /logs", "GET /status"
]

CONFIG_PATH = "bot_config.json"
REACTIONS_PATH = "reaction_roles.json"

def load_json(path, fallback):
    if not os.path.exists(path):
        return fallback
    with open(path, "r") as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

NAVBAR = """
<div style='margin-bottom: 1em; font-family: sans-serif;'>
  <a href="/logs">Logs</a> |
  <a href="/status_page">Status</a> |
  <a href="/settings">Settings</a> |
  <a href="/reaction_roles">Reaction Roles</a> |
  <a href="/roles">Role Assigner</a> |
  <a href="/users">User Viewer</a>
</div>
"""

# Optional bot reference for user/role tools
bot_reference = None
def set_bot_reference(bot):  # if needed for future user viewing
    global bot_reference
    bot_reference = bot

@app.route("/")
def index():
    return redirect("/logs")

@app.route("/logs")
def logs():
    try:
        with open("bot.log", "r") as f:
            lines = f.readlines()[-200:]
    except FileNotFoundError:
        lines = ["Log file not found."]
    filtered = [line for line in lines if not any(x in line for x in LOG_FILTERS)]
    safe = ''.join(filtered).replace("<", "&lt;").replace(">", "&gt;")
    return render_template_string("""
    <html><body style='background:#111;color:#0f0;font-family:monospace;padding:1em'>
        {{ navbar | safe }}
        <pre>{{ logs | safe }}</pre>
    </body></html>
    """, navbar=NAVBAR, logs=safe)

@app.route("/status_page")
def status_page():
    uptime = datetime.now() - datetime.fromtimestamp(psutil.boot_time())
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent

    return render_template_string("""
    <html><body style='background:#111;color:#eee;font-family:sans-serif;padding:1em'>
        {{ navbar | safe }}
        <h2>Bot Status</h2>
        <ul>
            <li><b>Uptime:</b> {{ uptime }}</li>
            <li><b>CPU Usage:</b> {{ cpu }}%</li>
            <li><b>RAM Usage:</b> {{ ram }}%</li>
        </ul>
    </body></html>
    """, navbar=NAVBAR, uptime=uptime, cpu=cpu, ram=ram)

@app.route("/restart", methods=["POST"])
def restart():
    os._exit(1)

@app.route("/settings", methods=["GET", "POST"])
def settings():
    config = load_json(CONFIG_PATH, {"prefix": "!", "embed_style": "default"})
    if request.method == "POST":
        config["prefix"] = request.form["prefix"]
        config["embed_style"] = request.form["embed_style"]
        save_json(CONFIG_PATH, config)
        return redirect("/settings")
    return render_template_string("""
    <html><body style='background:#111;color:#eee;font-family:sans-serif;padding:1em'>
        {{ navbar | safe }}
        <h2>Bot Settings</h2>
        <form method="post">
            Prefix: <input name="prefix" value="{{ cfg.prefix }}"><br>
            Embed Style: <input name="embed_style" value="{{ cfg.embed_style }}"><br>
            <button type="submit">Save</button>
        </form>
        <form action="/restart" method="post">
            <button style="margin-top:20px;color:red">Restart Bot</button>
        </form>
    </body></html>
    """, cfg=config, navbar=NAVBAR)

@app.route("/reaction_roles", methods=["GET", "POST"])
def reaction_roles():
    roles = load_json(REACTIONS_PATH, {})
    if request.method == "POST":
        new_roles = {}
        for k, v in request.form.items():
            if k.startswith("emoji_"):
                emoji = request.form[k]
                role = request.form.get(f"role_{k[6:]}")
                if emoji and role:
                    new_roles[emoji] = role
        save_json(REACTIONS_PATH, new_roles)
        return redirect("/reaction_roles")
    return render_template_string("""
    <html><body style='background:#111;color:#eee;font-family:sans-serif;padding:1em'>
        {{ navbar | safe }}
        <h2>Reaction Roles</h2>
        <form method="post">
            {% for emoji, role in roles.items() %}
                <input name="emoji_{{ loop.index }}" value="{{ emoji }}" size="5">
                → <input name="role_{{ loop.index }}" value="{{ role }}" size="15"><br>
            {% endfor %}
            <input name="emoji_new" placeholder="Emoji" size="5">
            → <input name="role_new" placeholder="Role" size="15"><br>
            <button type="submit">Save</button>
        </form>
    </body></html>
    """, roles=roles, navbar=NAVBAR)

@app.route("/roles")
def roles():
    return render_template_string("""
    <html><body style='background:#111;color:#eee;font-family:sans-serif;padding:1em'>
        {{ navbar | safe }}
        <h2>👤 Role Assigner UI (coming soon)</h2>
    </body></html>
    """, navbar=NAVBAR)

@app.route("/users")
def users():
    return render_template_string("""
    <html><body style='background:#111;color:#eee;font-family:sans-serif;padding:1em'>
        {{ navbar | safe }}
        <h2>📋 User Viewer (coming soon)</h2>
    </body></html>
    """, navbar=NAVBAR)

def run_dashboard():
    app.run(host="0.0.0.0", port=int(os.getenv("DASHBOARD_PORT", 5000)))

def start_dashboard_thread():
    thread = threading.Thread(target=run_dashboard)
    thread.daemon = True
    thread.start()

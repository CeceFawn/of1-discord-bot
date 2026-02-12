from __future__ import annotations
from flask import Flask, render_template_string, request, redirect
import threading
import os
import psutil
from datetime import datetime
from settings import LOG_PATH
from storage import load_config, save_config, load_state, save_state

app = Flask(__name__)

# Optional bot reference (future: user viewer / commands)
bot_reference = None

def set_bot_reference(bot):
    global bot_reference
    bot_reference = bot

NAVBAR = """
<div style="margin-bottom: 14px; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;">
  <a href="/logs">Logs</a> |
  <a href="/status">Status</a> |
  <a href="/config">Config</a> |
  <a href="/state">State</a>
  <form action="/restart" method="post" style="display:inline; margin-left:12px;">
    <button style="color:#b00;">Restart</button>
  </form>
</div>
"""

PAGE_WRAPPER = """
<html>
  <head>
    <meta charset="utf-8" />
    <title>OF1 Bot Dashboard</title>
  </head>
  <body style="background:#111;color:#eee;padding:16px;">
    {navbar}
    {body}
  </body>
</html>
"""

@app.route("/")
def index():
    return redirect("/logs")

@app.route("/logs")
def logs():
    cfg = load_config()
    filters = cfg.get("log_filters", [])
    try:
        with open(LOG_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()[-400:]
    except FileNotFoundError:
        lines = ["Log file not found.\n"]

    filtered = [line for line in lines if not any(x in line for x in filters)]
    safe = "".join(filtered).replace("<", "&lt;").replace(">", "&gt;")
    body = f"<pre style='white-space:pre-wrap;background:#000;padding:12px;border-radius:8px;'>{safe}</pre>"
    return PAGE_WRAPPER.format(navbar=NAVBAR, body=body)

@app.route("/status")
def status():
    uptime = datetime.now() - datetime.fromtimestamp(psutil.boot_time())
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent
    body = f"""
      <h2>Status</h2>
      <ul>
        <li><b>CPU:</b> {cpu}%</li>
        <li><b>RAM:</b> {ram}%</li>
        <li><b>System uptime:</b> {uptime}</li>
      </ul>
    """
    return PAGE_WRAPPER.format(navbar=NAVBAR, body=body)

@app.route("/config", methods=["GET", "POST"])
def config():
    cfg = load_config()
    if request.method == "POST":
        # Very simple editor: paste full JSON
        raw = request.form.get("json", "")
        try:
            import json
            parsed = json.loads(raw)
            save_config(parsed)
            return redirect("/config")
        except Exception as e:
            err = f"<div style='color:#f66;'>Invalid JSON: {e}</div>"
            body = err + _json_editor("config.json", cfg)
            return PAGE_WRAPPER.format(navbar=NAVBAR, body=body)

    body = _json_editor("config.json", cfg)
    return PAGE_WRAPPER.format(navbar=NAVBAR, body=body)

@app.route("/state", methods=["GET", "POST"])
def state():
    st = load_state()
    if request.method == "POST":
        raw = request.form.get("json", "")
        try:
            import json
            parsed = json.loads(raw)
            save_state(parsed)
            return redirect("/state")
        except Exception as e:
            err = f"<div style='color:#f66;'>Invalid JSON: {e}</div>"
            body = err + _json_editor("state.json", st)
            return PAGE_WRAPPER.format(navbar=NAVBAR, body=body)

    body = _json_editor("state.json", st)
    return PAGE_WRAPPER.format(navbar=NAVBAR, body=body)

def _json_editor(title: str, obj) -> str:
    import json
    pretty = json.dumps(obj, indent=2, ensure_ascii=False)
    return render_template_string("""
      <h2>{{ title }}</h2>
      <form method="post">
        <textarea name="json" rows="28" cols="110"
          style="width:100%;max-width:1100px;background:#000;color:#0f0;padding:12px;border-radius:8px;"
        >{{ pretty }}</textarea>
        <div style="margin-top:10px;">
          <button type="submit">Save</button>
        </div>
      </form>
    """, title=title, pretty=pretty)

@app.route("/restart", methods=["POST"])
def restart():
    os._exit(1)

def run_dashboard():
    app.run(host="0.0.0.0", port=int(os.getenv("DASHBOARD_PORT", 5000)))

def start_dashboard_thread():
    thread = threading.Thread(target=run_dashboard, daemon=True)
    thread.start()
#lol
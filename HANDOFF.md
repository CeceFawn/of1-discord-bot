# Handoff Notes
_Last updated: 2026-03-12_

## Where we left off
- Removed the "Deploy both (bot + dashboard)" button from the dashboard dropdown — it was redundant now that "Deploy all (bot + dashboard + website)" exists.
- The dropdown now has: Start bot, Stop bot, Deploy bot update, Deploy dashboard update, Deploy website update, Deploy all.

## What's working
- Discord bot running as `discordbot.service`
- Dashboard running as `of1-dashboard.service` on port 5000 → `dashboard.orlandof1.com`
- Public website running as `of1-website.service` on port 5001 → `orlandof1.com`
- nginx reverse proxy routing by subdomain, SSL via certbot
- Driver + constructor standings posting to Discord
- Deploy-from-dashboard via git pull + systemctl restart

## What's next (ideas / things mentioned)
- Nothing specific was queued — session ended after the dropdown cleanup.
- Possible areas to continue:
  - Improve the public website (orlandof1.com) — more content, schedule page, etc.
  - Add watch party management UI to the dashboard (edit watch_party.json from the UI instead of manually)
  - Investigate the duplicate 17th-place driver in standings (was left as a known issue, expected to self-resolve after next race)
  - Any other dashboard or bot features

## Key files
- `bot.py` — Discord bot, standings logic
- `dashboard.py` — Admin dashboard (Flask, port 5000)
- `website.py` — Public website (Flask, port 5001)
- `templates/index.html` — Public website HTML (Tailwind CSS)
- `watch_party.json` — Watch party info displayed on the website
- `.env` — All secrets and service names (not in git)

from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any, Dict, List

from settings import RUNTIME_DB_PATH, STATE_PATH

_DB_LOCK = threading.RLock()


def _connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(RUNTIME_DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(RUNTIME_DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_runtime_db() -> None:
    with _DB_LOCK:
        conn = _connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runtime_status (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    ts TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    message TEXT NOT NULL,
                    guild_id INTEGER NOT NULL DEFAULT 0,
                    user_id INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS migrations (
                    key TEXT PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
        finally:
            conn.close()


def upsert_runtime_status(payload: Dict[str, Any]) -> None:
    init_runtime_db()
    ts = str((payload or {}).get("ts") or "")
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    with _DB_LOCK:
        conn = _connect()
        try:
            conn.execute(
                """
                INSERT INTO runtime_status (id, ts, payload_json)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  ts=excluded.ts,
                  payload_json=excluded.payload_json
                """,
                (ts, payload_json),
            )
            conn.commit()
        finally:
            conn.close()


def get_runtime_status() -> Dict[str, Any]:
    init_runtime_db()
    with _DB_LOCK:
        conn = _connect()
        try:
            row = conn.execute("SELECT payload_json FROM runtime_status WHERE id = 1").fetchone()
            if not row or not row[0]:
                return {}
            try:
                data = json.loads(str(row[0]))
                return data if isinstance(data, dict) else {}
            except Exception:
                return {}
        finally:
            conn.close()


def insert_alert(ts: str, kind: str, message: str, guild_id: int = 0, user_id: int = 0) -> None:
    init_runtime_db()
    with _DB_LOCK:
        conn = _connect()
        try:
            conn.execute(
                """
                INSERT INTO alerts (ts, kind, message, guild_id, user_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (str(ts or ""), str(kind or "info"), str(message or "")[:500], int(guild_id or 0), int(user_id or 0)),
            )
            conn.execute(
                """
                DELETE FROM alerts
                WHERE id NOT IN (
                    SELECT id FROM alerts ORDER BY id DESC LIMIT 500
                )
                """
            )
            conn.commit()
        finally:
            conn.close()


def list_alerts(limit: int = 20) -> List[Dict[str, Any]]:
    init_runtime_db()
    lim = max(1, min(200, int(limit)))
    with _DB_LOCK:
        conn = _connect()
        try:
            rows = conn.execute(
                """
                SELECT ts, kind, message, guild_id, user_id
                FROM alerts
                ORDER BY id DESC
                LIMIT ?
                """,
                (lim,),
            ).fetchall()
            out: List[Dict[str, Any]] = []
            for ts, kind, message, guild_id, user_id in rows:
                out.append(
                    {
                        "ts": str(ts or ""),
                        "kind": str(kind or "info"),
                        "message": str(message or ""),
                        "guild_id": int(guild_id or 0),
                        "user_id": int(user_id or 0),
                    }
                )
            return out
        finally:
            conn.close()


def migrate_alerts_from_state_json(state_path: str = STATE_PATH) -> int:
    init_runtime_db()
    with _DB_LOCK:
        conn = _connect()
        try:
            done = conn.execute(
                "SELECT 1 FROM migrations WHERE key = 'alerts_from_state_v1'"
            ).fetchone()
            if done:
                return 0

            count = 0
            if os.path.exists(state_path):
                try:
                    with open(state_path, "r", encoding="utf-8") as f:
                        state = json.load(f)
                    alerts = (((state or {}).get("alerts") or {}).get("items") or [])
                    if isinstance(alerts, list):
                        for a in alerts:
                            if not isinstance(a, dict):
                                continue
                            conn.execute(
                                """
                                INSERT INTO alerts (ts, kind, message, guild_id, user_id)
                                VALUES (?, ?, ?, ?, ?)
                                """,
                                (
                                    str(a.get("ts") or ""),
                                    str(a.get("kind") or "info"),
                                    str(a.get("message") or "")[:500],
                                    int(a.get("guild_id") or 0),
                                    int(a.get("user_id") or 0),
                                ),
                            )
                            count += 1
                except Exception:
                    pass

            conn.execute(
                "INSERT OR REPLACE INTO migrations (key, applied_at) VALUES ('alerts_from_state_v1', datetime('now'))"
            )
            conn.execute(
                """
                DELETE FROM alerts
                WHERE id NOT IN (
                    SELECT id FROM alerts ORDER BY id DESC LIMIT 500
                )
                """
            )
            conn.commit()
            return count
        finally:
            conn.close()

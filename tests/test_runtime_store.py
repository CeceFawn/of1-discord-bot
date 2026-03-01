from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import runtime_store as rs


class RuntimeStoreTests(unittest.TestCase):
    def test_runtime_status_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "runtime.db")
            with mock.patch.object(rs, "RUNTIME_DB_PATH", db):
                rs.init_runtime_db()
                payload = {"ts": "2026-01-01T00:00:00+00:00", "runtime": {"guild_count": 1}}
                rs.upsert_runtime_status(payload)
                got = rs.get_runtime_status()
                self.assertEqual(got.get("runtime", {}).get("guild_count"), 1)

    def test_alert_insert_and_list(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "runtime.db")
            with mock.patch.object(rs, "RUNTIME_DB_PATH", db):
                rs.init_runtime_db()
                rs.insert_alert("2026-01-01T00:00:00+00:00", "command_error", "boom", 1, 2)
                rows = rs.list_alerts(limit=5)
                self.assertTrue(rows)
                self.assertEqual(rows[0]["kind"], "command_error")


if __name__ == "__main__":
    unittest.main()

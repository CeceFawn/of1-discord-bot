from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from storage import load_json, save_json_atomic


class StorageResilienceTests(unittest.TestCase):
    def test_load_json_falls_back_to_backup_on_corrupt_primary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "state.json"
            b = Path(f"{p}.bak")
            p.write_text("{broken", encoding="utf-8")
            b.write_text(json.dumps({"ok": True}), encoding="utf-8")
            data = load_json(str(p), fallback={})
            self.assertEqual(data.get("ok"), True)

    def test_save_json_atomic_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "config.json"
            payload = {"a": 1, "b": {"c": 2}}
            save_json_atomic(str(p), payload)
            data = load_json(str(p), fallback={})
            self.assertEqual(data, payload)


if __name__ == "__main__":
    unittest.main()

import unittest
import json
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from ai_discovery.admin_store import AdminState, AdminStore


class AdminStoreTests(unittest.TestCase):
    def test_save_env_map_updates_managed_keys(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = AdminStore(root)
            store.save_env_map(
                {
                    "AI_API_BASE_URL": "https://example.com/v1",
                    "AI_API_KEY": "key",
                    "AI_MODEL": "model",
                    "CONTENT_PREFERENCE_ZH": "偏向抓取开发者工具",
                }
            )
            content = (root / ".env").read_text(encoding="utf-8")
            self.assertIn("AI_API_BASE_URL=https://example.com/v1", content)
            self.assertIn("CONTENT_PREFERENCE_ZH=偏向抓取开发者工具", content)

    def test_sources_round_trip(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = AdminStore(root)
            store.save_sources_text("https://news.ycombinator.com/show\n\nhttps://solo.xin\n")
            self.assertEqual(store.load_sources_text(), "https://news.ycombinator.com/show\nhttps://solo.xin\n")

    def test_load_state_returns_default_for_empty_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = AdminStore(root)
            store.state_path.write_text("", encoding="utf-8")
            state = store.load_state()
            self.assertEqual(state, AdminState())

    def test_load_state_returns_default_for_invalid_json(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = AdminStore(root)
            store.state_path.write_text("{", encoding="utf-8")
            state = store.load_state()
            self.assertEqual(state, AdminState())

    def test_save_state_writes_valid_json_atomically(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = AdminStore(root)
            state = AdminState(service_enabled=True, last_message="ok")
            store.save_state(state)
            payload = json.loads(store.state_path.read_text(encoding="utf-8"))
            self.assertTrue(payload["service_enabled"])
            self.assertEqual(payload["last_message"], "ok")

    def test_next_run_calculation(self) -> None:
        state = AdminState(service_enabled=True, schedule_enabled=True, schedule_time="09:30")
        next_run = AdminStore.compute_next_run_at(state)
        self.assertTrue(next_run)

    def test_next_run_is_today_when_local_time_not_passed(self) -> None:
        state = AdminState(service_enabled=True, schedule_enabled=True, schedule_time="21:30")
        now = datetime(2026, 4, 24, 19, 17, 12)
        next_run = AdminStore.compute_next_run_at(state, now=now)
        self.assertEqual(next_run, "2026-04-24T21:30")

    def test_next_run_is_tomorrow_when_local_time_already_passed(self) -> None:
        state = AdminState(service_enabled=True, schedule_enabled=True, schedule_time="09:30")
        now = datetime(2026, 4, 24, 19, 17, 12)
        next_run = AdminStore.compute_next_run_at(state, now=now)
        self.assertEqual(next_run, "2026-04-25T09:30")

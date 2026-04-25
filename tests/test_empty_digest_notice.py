import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from ai_discovery.config import Settings
from ai_discovery.pipeline import DiscoveryPipeline


class EmptyDigestNoticeTests(unittest.TestCase):
    def test_empty_notice_sent_when_no_chunks_and_not_dry_run(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "sub_sites.md").write_text("https://news.ycombinator.com/show\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.db_path = root / "discovery.db"
            settings.source_file = root / "sub_sites.md"
            settings.ai_api_base_url = "https://example.com/v1"
            settings.ai_api_key = "key"
            settings.ai_model = "model"
            settings.telegram_bot_token = "token"
            settings.telegram_chat_id = "chat"
            pipeline = DiscoveryPipeline(settings)
            try:
                pipeline.init_db()
                with patch.object(pipeline, "_send_telegram", return_value="100") as sender:
                    outcome = pipeline._persist_digest("run1", "2026-04-24", [], False, [])
                    self.assertTrue(outcome["empty_notice_sent"])
                    sender.assert_called_once()
            finally:
                pipeline.close()

    def test_empty_notice_is_resent_on_same_day(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "sub_sites.md").write_text("https://news.ycombinator.com/show\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.db_path = root / "discovery.db"
            settings.source_file = root / "sub_sites.md"
            settings.ai_api_base_url = "https://example.com/v1"
            settings.ai_api_key = "key"
            settings.ai_model = "model"
            settings.telegram_bot_token = "token"
            settings.telegram_chat_id = "chat"
            pipeline = DiscoveryPipeline(settings)
            try:
                pipeline.init_db()
                with patch.object(pipeline, "_send_telegram", side_effect=["100", "101"]) as sender:
                    first = pipeline._persist_digest("run1", "2026-04-24", [], False, [])
                    second = pipeline._persist_digest("run2", "2026-04-24", [], False, [])
                    self.assertTrue(first["empty_notice_sent"])
                    self.assertTrue(second["empty_notice_sent"])
                    self.assertEqual(sender.call_count, 2)
                    row = pipeline.db.get_digest_chunk("2026-04-24", 0)
                    self.assertEqual(row["send_status"], "sent")
                    self.assertEqual(row["telegram_message_id"], "101")
                    self.assertEqual(pipeline.db.count_digest_chunks("2026-04-24", 0), 2)
            finally:
                pipeline.close()

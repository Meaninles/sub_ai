import unittest
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory

from ai_discovery.db import DiscoveryDB
from ai_discovery.models import ProjectRecord, VerificationState


class DatabaseTests(unittest.TestCase):
    def test_db_init_and_project_upsert(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db = DiscoveryDB(Path(temp_dir) / "test.db")
            db.init_db()
            project = ProjectRecord(
                project_id="p1",
                canonical_name="Test Project",
                display_name_zh="测试项目",
                primary_link="https://example.com/test",
                maturity="alpha",
                category="tool",
                summary_200="Summary",
                verification_state=VerificationState.VERIFIED_SINGLE_SOURCE,
                verification_class="verified_single_source",
                verification_reason="ok",
                first_seen_at="2026-04-24T00:00:00+00:00",
                last_seen_at="2026-04-24T00:00:00+00:00",
                evidence_flags={"launch_cue": True},
            )
            db.upsert_project(project)
            row = db.existing_project_by_link("https://example.com/test")
            self.assertIsNotNone(row)
            self.assertEqual(row["canonical_name"], "Test Project")
            self.assertEqual(row["display_name_zh"], "测试项目")
            db.close()

    def test_state_event_and_digest_status_helpers(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db = DiscoveryDB(Path(temp_dir) / "test.db")
            db.init_db()
            db.record_state_event("e1", "run1", "hn_show", "obs1", "p1", "observed", "stored", {"ok": True})
            db.insert_digest_chunk(
                "s1",
                "d1",
                "2026-04-24",
                type("Chunk", (), {"chunk_index": 1, "text": "hello", "item_count": 1})(),
                True,
                "dry_run",
                None,
            )
            row = db.get_digest_chunk("2026-04-24", 1)
            self.assertIsNotNone(row)
            self.assertEqual(row["send_id"], "s1")
            db.update_digest_chunk_status("s1", "sent", "42")
            row = db.get_digest_chunk("2026-04-24", 1)
            self.assertEqual(row["send_status"], "sent")
            self.assertEqual(db.count_digest_chunks("2026-04-24", 1), 1)
            db.insert_digest_chunk(
                "s2",
                "d1",
                "2026-04-24",
                type("Chunk", (), {"chunk_index": 1, "text": "hello again", "item_count": 1})(),
                False,
                "sending",
                None,
            )
            db.update_digest_chunk_status("s2", "sent", "43")
            row = db.get_digest_chunk("2026-04-24", 1)
            self.assertEqual(row["send_id"], "s2")
            self.assertEqual(row["telegram_message_id"], "43")
            self.assertEqual(db.count_digest_chunks("2026-04-24", 1), 2)
            db.close()

    def test_fail_running_runs_marks_all_in_progress_runs_failed(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db = DiscoveryDB(Path(temp_dir) / "test.db")
            db.init_db()
            db.start_run("run1", False)
            db.start_run("run2", True)
            db.finish_run("run2", "success", "done")
            changed = db.fail_running_runs("stale task")
            self.assertEqual(changed, 1)
            row = db.connection.execute("SELECT status, notes FROM runs WHERE run_id = ?", ("run1",)).fetchone()
            self.assertEqual(row["status"], "failed")
            self.assertEqual(row["notes"], "stale task")
            db.close()

    def test_init_db_migrates_legacy_digests_into_history_table(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE digests (
                    digest_id TEXT PRIMARY KEY,
                    digest_date TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    message_text TEXT NOT NULL,
                    item_count INTEGER NOT NULL,
                    dry_run INTEGER NOT NULL,
                    send_status TEXT NOT NULL,
                    telegram_message_id TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(digest_date, chunk_index)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO digests (
                    digest_id, digest_date, chunk_index, message_text, item_count,
                    dry_run, send_status, telegram_message_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("2026-04-24:1", "2026-04-24", 1, "legacy", 1, 0, "sent", "42", "2026-04-24 10:00:00"),
            )
            conn.commit()
            conn.close()

            db = DiscoveryDB(db_path)
            db.init_db()
            row = db.get_digest_chunk("2026-04-24", 1)
            self.assertIsNotNone(row)
            self.assertEqual(row["digest_id"], "2026-04-24:1")
            self.assertEqual(row["send_status"], "sent")
            self.assertEqual(row["telegram_message_id"], "42")
            self.assertEqual(db.count_digest_chunks("2026-04-24", 1), 1)
            self.assertIn("send_id", row.keys())
            db.close()

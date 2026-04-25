import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from ai_discovery.admin_service import AdminService
from ai_discovery.admin_store import AdminState
from ai_discovery.db import DiscoveryDB


class AdminServiceTests(unittest.TestCase):
    def test_health_report_returns_local_service_state(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = AdminService(Path(temp_dir))
            service.ensure_scheduler()
            service.set_service_enabled(True)
            report = service.health_report()
            self.assertIn(report["overall"], {"正常", "已停用", "异常"})
            self.assertEqual(report["service_status"], "运行中")

    def test_service_events_are_recorded(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = AdminService(Path(temp_dir))
            service.ensure_scheduler()
            service.set_service_enabled(True)
            state = service.load_state()
            self.assertTrue(state.recent_events)

    def test_scheduler_should_trigger_after_local_time_reached(self) -> None:
        state = AdminState(service_enabled=True, schedule_enabled=True, schedule_time="19:32", last_scheduled_date="")
        now = datetime(2026, 4, 24, 19, 32, 5)
        self.assertTrue(AdminService._should_trigger_schedule(state, now))

    def test_scheduler_should_not_trigger_before_local_time(self) -> None:
        state = AdminState(service_enabled=True, schedule_enabled=True, schedule_time="19:32", last_scheduled_date="")
        now = datetime(2026, 4, 24, 19, 31, 59)
        self.assertFalse(AdminService._should_trigger_schedule(state, now))

    def test_scheduler_should_not_trigger_twice_same_day(self) -> None:
        state = AdminState(
            service_enabled=True,
            schedule_enabled=True,
            schedule_time="19:32",
            last_scheduled_date="2026-04-24",
            last_scheduled_slot="2026-04-24|19:32",
        )
        now = datetime(2026, 4, 24, 20, 0, 0)
        self.assertFalse(AdminService._should_trigger_schedule(state, now))

    def test_run_now_starts_manual_send_action(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = AdminService(Path(temp_dir))
            with patch.object(service, "_start_background_action", return_value=True) as starter:
                started = service.run_now(dry_run=False)
                self.assertTrue(started)
                starter.assert_called_once()
                self.assertEqual(starter.call_args.kwargs["action"], "manual:send")

    def test_stop_active_action_marks_state_for_shutdown(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = AdminService(root)
            service.store.save_state(
                AdminState(
                    active_action="manual:send",
                    task_items=[
                        {"source_id": "a", "status": "running", "summary": "running"},
                        {"source_id": "b", "status": "waiting", "summary": "waiting"},
                    ],
                )
            )
            stopped = service.stop_active_action()
            self.assertTrue(stopped)
            state = service.load_state()
            self.assertEqual(state.last_message, "manual:send 正在停止")
            self.assertTrue(all(task["status"] == "failed" for task in state.task_items))

    def test_service_init_recovers_stale_active_action_and_running_runs(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = AdminService(root).store
            store.save_state(
                AdminState(
                    active_action="manual:send",
                    last_status="running",
                    task_items=[{"source_id": "a", "status": "running", "summary": "running"}],
                )
            )
            db = DiscoveryDB(root / ".omx" / "data" / "discovery.db")
            db.init_db()
            db.start_run("run1", False)
            db.close()

            recovered = AdminService(root)
            state = recovered.load_state()
            self.assertEqual(state.active_action, "")
            self.assertEqual(state.last_status, "failed")
            self.assertIn("服务重启", state.last_message)
            self.assertEqual(state.task_items[0]["status"], "failed")

            db = DiscoveryDB(root / ".omx" / "data" / "discovery.db")
            row = db.connection.execute("SELECT status, notes FROM runs WHERE run_id = ?", ("run1",)).fetchone()
            self.assertEqual(row["status"], "failed")
            self.assertIn("服务重启", row["notes"])
            db.close()

    def test_process_telegram_follow_update_creates_follow_for_latest_list(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text(
                "\n".join(
                    [
                        "AI_API_BASE_URL=https://example.com/v1",
                        "AI_API_KEY=key",
                        "AI_MODEL=model",
                        "TELEGRAM_BOT_TOKEN=bot-token",
                        "TELEGRAM_CHAT_ID=12345",
                    ]
                ),
                encoding="utf-8",
            )
            db = DiscoveryDB(root / ".omx" / "data" / "discovery.db")
            db.init_db()
            db.create_telegram_project_list(
                list_id="older-list",
                run_id="run-old",
                digest_date="2026-04-24",
                chat_id="12345",
                items=[
                    {"project_id": "p-old", "item_index": 1, "project_name": "旧项目"},
                ],
            )
            db.mark_telegram_project_list_sent("older-list")
            db.create_telegram_project_list(
                list_id="latest-list",
                run_id="run-new",
                digest_date="2026-04-25",
                chat_id="12345",
                items=[
                    {"project_id": "p1", "item_index": 1, "project_name": "AI助手"},
                    {"project_id": "p2", "item_index": 2, "project_name": "语音信箱"},
                ],
            )
            db.mark_telegram_project_list_sent("latest-list")
            db.close()

            service = AdminService(root)
            settings = service.store.load_settings()
            update = {
                "update_id": 1001,
                "message": {
                    "message_id": 88,
                    "text": "2，1，2",
                    "chat": {"id": 12345},
                    "from": {"id": 678},
                },
            }
            with patch("ai_discovery.admin_service.AIClient.parse_follow_selection", return_value={"is_numeric_selection": True, "selected_indexes": [2, 1], "rationale": "ok"}), patch.object(
                service,
                "_send_follow_confirmation",
            ) as confirm:
                service._process_telegram_follow_update(settings, update)
                confirm.assert_called_once()
            db = DiscoveryDB(root / ".omx" / "data" / "discovery.db")
            rows = list(db.connection.execute("SELECT project_id, list_id, item_index FROM project_follows ORDER BY item_index ASC"))
            self.assertEqual([row["project_id"] for row in rows], ["p1", "p2"])
            self.assertTrue(all(row["list_id"] == "latest-list" for row in rows))
            inbound = db.connection.execute("SELECT parse_status, follow_list_id FROM telegram_inbound_messages WHERE update_id = 1001").fetchone()
            self.assertEqual(inbound["parse_status"], "followed")
            self.assertEqual(inbound["follow_list_id"], "latest-list")
            db.close()

    def test_process_telegram_follow_update_ignores_non_sequence_text(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text(
                "\n".join(
                    [
                        "AI_API_BASE_URL=https://example.com/v1",
                        "AI_API_KEY=key",
                        "AI_MODEL=model",
                        "TELEGRAM_BOT_TOKEN=bot-token",
                        "TELEGRAM_CHAT_ID=12345",
                    ]
                ),
                encoding="utf-8",
            )
            db = DiscoveryDB(root / ".omx" / "data" / "discovery.db")
            db.init_db()
            db.create_telegram_project_list(
                list_id="latest-list",
                run_id="run-new",
                digest_date="2026-04-25",
                chat_id="12345",
                items=[{"project_id": "p1", "item_index": 1, "project_name": "AI助手"}],
            )
            db.mark_telegram_project_list_sent("latest-list")
            db.close()

            service = AdminService(root)
            settings = service.store.load_settings()
            update = {
                "update_id": 1002,
                "message": {
                    "message_id": 89,
                    "text": "你给我10个",
                    "chat": {"id": 12345},
                    "from": {"id": 678},
                },
            }
            with patch.object(service, "_send_follow_confirmation") as confirm:
                service._process_telegram_follow_update(settings, update)
                confirm.assert_not_called()
            db = DiscoveryDB(root / ".omx" / "data" / "discovery.db")
            inbound = db.connection.execute("SELECT parse_status, notes FROM telegram_inbound_messages WHERE update_id = 1002").fetchone()
            self.assertEqual(inbound["parse_status"], "ignored")
            self.assertIn("message_not_numeric_selection_candidate", inbound["notes"])
            follows = db.connection.execute("SELECT COUNT(*) AS count FROM project_follows").fetchone()
            self.assertEqual(follows["count"], 0)
            db.close()

    def test_process_telegram_follow_update_hard_parses_without_ai(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text(
                "\n".join(
                    [
                        "AI_API_BASE_URL=https://example.com/v1",
                        "AI_API_KEY=key",
                        "AI_MODEL=model",
                        "TELEGRAM_BOT_TOKEN=bot-token",
                        "TELEGRAM_CHAT_ID=12345",
                    ]
                ),
                encoding="utf-8",
            )
            db = DiscoveryDB(root / ".omx" / "data" / "discovery.db")
            db.init_db()
            db.create_telegram_project_list(
                list_id="latest-list",
                run_id="run-new",
                digest_date="2026-04-25",
                chat_id="12345",
                items=[
                    {"project_id": "p1", "item_index": 1, "project_name": "AI助手"},
                    {"project_id": "p2", "item_index": 2, "project_name": "占位项目2"},
                    {"project_id": "p3", "item_index": 3, "project_name": "语音信箱"},
                    {"project_id": "p4", "item_index": 4, "project_name": "占位项目4"},
                    {"project_id": "p5", "item_index": 5, "project_name": "搜索工具"},
                ],
            )
            db.mark_telegram_project_list_sent("latest-list")
            db.close()

            service = AdminService(root)
            settings = service.store.load_settings()
            update = {
                "update_id": 1003,
                "message": {
                    "message_id": 90,
                    "text": "1，3，5",
                    "chat": {"id": 12345},
                    "from": {"id": 678},
                },
            }
            with patch("ai_discovery.admin_service.AIClient.parse_follow_selection") as ai_parse, patch.object(
                service,
                "_send_follow_confirmation",
            ) as confirm:
                service._process_telegram_follow_update(settings, update)
                ai_parse.assert_not_called()
                confirm.assert_called_once()
            db = DiscoveryDB(root / ".omx" / "data" / "discovery.db")
            inbound = db.connection.execute("SELECT parse_status, parse_result_json FROM telegram_inbound_messages WHERE update_id = 1003").fetchone()
            self.assertEqual(inbound["parse_status"], "followed")
            self.assertIn("hard_parsed_numeric_sequence", inbound["parse_result_json"])
            follows = list(db.connection.execute("SELECT item_index FROM project_follows ORDER BY item_index ASC"))
            self.assertEqual([row["item_index"] for row in follows], [1, 3, 5])
            db.close()

    def test_process_telegram_follow_update_hard_parses_chinese_enumeration_separator(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text(
                "\n".join(
                    [
                        "AI_API_BASE_URL=https://example.com/v1",
                        "AI_API_KEY=key",
                        "AI_MODEL=model",
                        "TELEGRAM_BOT_TOKEN=bot-token",
                        "TELEGRAM_CHAT_ID=12345",
                    ]
                ),
                encoding="utf-8",
            )
            db = DiscoveryDB(root / ".omx" / "data" / "discovery.db")
            db.init_db()
            db.create_telegram_project_list(
                list_id="latest-list",
                run_id="run-new",
                digest_date="2026-04-25",
                chat_id="12345",
                items=[
                    {"project_id": "p1", "item_index": 1, "project_name": "AI助手"},
                    {"project_id": "p2", "item_index": 2, "project_name": "语音信箱"},
                    {"project_id": "p3", "item_index": 3, "project_name": "搜索工具"},
                ],
            )
            db.mark_telegram_project_list_sent("latest-list")
            db.close()

            service = AdminService(root)
            settings = service.store.load_settings()
            update = {
                "update_id": 1005,
                "message": {
                    "message_id": 92,
                    "text": "1、2、3",
                    "chat": {"id": 12345},
                    "from": {"id": 678},
                },
            }
            with patch("ai_discovery.admin_service.AIClient.parse_follow_selection") as ai_parse, patch.object(
                service,
                "_send_follow_confirmation",
            ) as confirm:
                service._process_telegram_follow_update(settings, update)
                ai_parse.assert_not_called()
                confirm.assert_called_once()
            db = DiscoveryDB(root / ".omx" / "data" / "discovery.db")
            inbound = db.connection.execute("SELECT parse_status, parse_result_json FROM telegram_inbound_messages WHERE update_id = 1005").fetchone()
            self.assertEqual(inbound["parse_status"], "followed")
            self.assertIn("hard_parsed_numeric_sequence", inbound["parse_result_json"])
            follows = list(db.connection.execute("SELECT item_index FROM project_follows ORDER BY item_index ASC"))
            self.assertEqual([row["item_index"] for row in follows], [1, 2, 3])
            db.close()

    def test_process_telegram_follow_update_ignores_phone_number_without_ai(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text(
                "\n".join(
                    [
                        "AI_API_BASE_URL=https://example.com/v1",
                        "AI_API_KEY=key",
                        "AI_MODEL=model",
                        "TELEGRAM_BOT_TOKEN=bot-token",
                        "TELEGRAM_CHAT_ID=12345",
                    ]
                ),
                encoding="utf-8",
            )
            db = DiscoveryDB(root / ".omx" / "data" / "discovery.db")
            db.init_db()
            db.create_telegram_project_list(
                list_id="latest-list",
                run_id="run-new",
                digest_date="2026-04-25",
                chat_id="12345",
                items=[{"project_id": "p1", "item_index": 1, "project_name": "AI助手"}],
            )
            db.mark_telegram_project_list_sent("latest-list")
            db.close()

            service = AdminService(root)
            settings = service.store.load_settings()
            update = {
                "update_id": 1004,
                "message": {
                    "message_id": 91,
                    "text": "13938329077",
                    "chat": {"id": 12345},
                    "from": {"id": 678},
                },
            }
            with patch("ai_discovery.admin_service.AIClient.parse_follow_selection") as ai_parse:
                service._process_telegram_follow_update(settings, update)
                ai_parse.assert_not_called()
            db = DiscoveryDB(root / ".omx" / "data" / "discovery.db")
            inbound = db.connection.execute("SELECT parse_status, notes FROM telegram_inbound_messages WHERE update_id = 1004").fetchone()
            self.assertEqual(inbound["parse_status"], "ignored")
            self.assertIn("message_not_numeric_selection_candidate", inbound["notes"])
            db.close()

import json
from io import BytesIO
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from ai_discovery.admin_service import AdminService
from ai_discovery.admin_web import make_handler


class AdminWebSmokeTests(unittest.TestCase):
    def test_status_endpoint_payload_builder_exists(self) -> None:
        with TemporaryDirectory() as temp_dir:
            handler_cls = make_handler(Path(temp_dir))
            self.assertTrue(hasattr(handler_cls, "_build_status_payload"))

    def test_index_contains_run_now_action(self) -> None:
        with TemporaryDirectory() as temp_dir:
            handler_cls = make_handler(Path(temp_dir))
            handler = handler_cls.__new__(handler_cls)
            html = handler_cls._render_index(handler)
            self.assertIn("/action/run-now", html)
            self.assertIn("/action/stop-task", html)
            self.assertIn("立即执行", html)
            self.assertIn("停止任务", html)
            self.assertIn("/site-sessions", html)
            self.assertIn("站点登录会话", html)
            self.assertIn("用户关注", html)
            self.assertIn("/follow/unfollow", html)
            self.assertIn("renderProjectCards", html)
            self.assertIn("row.primary_link ? `<a class=\"title-link\"", html)

    def test_run_now_post_triggers_manual_send(self) -> None:
        with TemporaryDirectory() as temp_dir:
            with patch.object(AdminService, "run_now", return_value=True) as run_now:
                handler_cls = make_handler(Path(temp_dir))
                handler = handler_cls.__new__(handler_cls)
                redirects: list[str] = []
                handler.path = "/action/run-now"
                handler._read_form = lambda: {}
                handler._redirect = redirects.append
                handler.send_error = lambda status: self.fail(f"unexpected error: {status}")
                handler.do_POST()
                run_now.assert_called_once_with(dry_run=False)
                self.assertEqual(redirects, ["/"])

    def test_site_sessions_post_saves_sessions(self) -> None:
        with TemporaryDirectory() as temp_dir:
            handler_cls = make_handler(Path(temp_dir))
            handler = handler_cls.__new__(handler_cls)
            redirects: list[str] = []
            handler.path = "/site-sessions"
            handler._read_form = lambda: {
                "reddit_cookie": [
                    json.dumps(
                        {
                            "url": "https://www.reddit.com/",
                            "cookies": [{"name": "reddit_session", "value": "1"}],
                        },
                        ensure_ascii=False,
                    )
                ],
                "reddit_user_agent": ["Mozilla/5.0 Reddit"],
                "github_cookie": ["logged_in=yes"],
                "github_user_agent": [""],
                "x_cookie": ["auth_token=abc"],
                "x_user_agent": ["Mozilla/5.0 X"],
            }
            handler._redirect = redirects.append
            handler.send_error = lambda status: self.fail(f"unexpected error: {status}")
            handler.do_POST()
            self.assertEqual(redirects, ["/"])
            sessions_path = Path(temp_dir) / ".omx" / "data" / "site_sessions.json"
            payload = json.loads(sessions_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["reddit"]["cookie"], "reddit_session=1")
            self.assertEqual(payload["x"]["user_agent"], "Mozilla/5.0 X")

    def test_stop_task_post_requests_active_task_stop(self) -> None:
        with TemporaryDirectory() as temp_dir:
            with patch.object(AdminService, "stop_active_action", return_value=True) as stop_task:
                handler_cls = make_handler(Path(temp_dir))
                handler = handler_cls.__new__(handler_cls)
                redirects: list[str] = []
                handler.path = "/action/stop-task"
                handler._read_form = lambda: {}
                handler._redirect = redirects.append
                handler.send_error = lambda status: self.fail(f"unexpected error: {status}")
                handler.do_POST()
                stop_task.assert_called_once_with()
                self.assertEqual(redirects, ["/"])

    def test_unfollow_post_calls_service(self) -> None:
        with TemporaryDirectory() as temp_dir:
            with patch.object(AdminService, "unfollow_project", return_value=True) as unfollow:
                handler_cls = make_handler(Path(temp_dir))
                handler = handler_cls.__new__(handler_cls)
                redirects: list[str] = []
                handler.path = "/follow/unfollow"
                handler._read_form = lambda: {"follow_id": ["follow-1"]}
                handler._redirect = redirects.append
                handler.send_error = lambda status: self.fail(f"unexpected error: {status}")
                handler.do_POST()
                unfollow.assert_called_once_with("follow-1")
                self.assertEqual(redirects, ["/"])

    def test_send_json_ignores_broken_pipe(self) -> None:
        with TemporaryDirectory() as temp_dir:
            handler_cls = make_handler(Path(temp_dir))
            handler = handler_cls.__new__(handler_cls)
            handler.send_response = lambda code: None
            handler.send_header = lambda key, value: None
            handler.end_headers = lambda: None

            class _BrokenPipeWriter(BytesIO):
                def write(self, data):  # type: ignore[override]
                    raise BrokenPipeError("client disconnected")

            handler.wfile = _BrokenPipeWriter()

            handler._send_json({"ok": True})

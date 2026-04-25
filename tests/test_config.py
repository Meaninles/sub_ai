import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from ai_discovery.config import Settings


class ConfigTests(unittest.TestCase):
    def test_content_preference_is_loaded_from_dotenv(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text(
                "CONTENT_PREFERENCE_ZH=我更偏向抓取适合个人开发者的 AI 自动化项目\n",
                encoding="utf-8",
            )
            settings = Settings.from_env(root)
            self.assertEqual(settings.content_preference_zh, "我更偏向抓取适合个人开发者的 AI 自动化项目")

    def test_ai_timeout_is_loaded_from_dotenv(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text("AI_TIMEOUT_SECONDS=135\n", encoding="utf-8")
            settings = Settings.from_env(root)
            self.assertEqual(settings.ai_timeout_seconds, 135)

    def test_site_sessions_are_loaded_from_project_data(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            sessions_path = root / ".omx" / "data" / "site_sessions.json"
            sessions_path.parent.mkdir(parents=True, exist_ok=True)
            sessions_path.write_text(
                json.dumps(
                    {
                        "reddit": {"cookie": "reddit_session=1", "user_agent": ""},
                        "github": {"cookie": "", "user_agent": "Mozilla/5.0 GitHub"},
                        "x": {"cookie": "", "user_agent": ""},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            settings = Settings.from_env(root)
            self.assertEqual(settings.site_sessions["reddit"]["cookie"], "reddit_session=1")
            self.assertEqual(settings.site_sessions["github"]["user_agent"], "Mozilla/5.0 GitHub")

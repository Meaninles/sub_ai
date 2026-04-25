import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from ai_discovery.ai_client import AIClient
from ai_discovery.config import Settings
from ai_discovery.http import HttpClient
from ai_discovery.models import Observation


class AIClientPayloadTests(unittest.TestCase):
    def test_extract_payload_includes_content_preference(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text(
                "\n".join(
                    [
                        "CONTENT_PREFERENCE_ZH=更偏向抓取能直接看明白产品价值的 AI 项目",
                        "AI_API_BASE_URL=https://example.com/v1",
                        "AI_API_KEY=key",
                        "AI_MODEL=model",
                    ]
                ),
                encoding="utf-8",
            )
            settings = Settings.from_env(root)
            client = AIClient(settings, HttpClient())
            payload = client._build_extract_payload(
                Observation(
                    source_id="hn_show",
                    external_id="1",
                    observed_at="2026-04-24T00:00:00+00:00",
                    title="title",
                    body_text="body",
                    source_url="https://example.com",
                    raw_payload={"id": 1},
                )
            )
            user_payload = json.loads(payload["messages"][1]["content"])
            self.assertEqual(user_payload["content_preference_zh"], "更偏向抓取能直接看明白产品价值的 AI 项目")

    def test_rewrite_payload_includes_content_preference(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text(
                "\n".join(
                    [
                        "CONTENT_PREFERENCE_ZH=更偏向抓取能直接落地使用的工具型项目",
                        "AI_API_BASE_URL=https://example.com/v1",
                        "AI_API_KEY=key",
                        "AI_MODEL=model",
                    ]
                ),
                encoding="utf-8",
            )
            settings = Settings.from_env(root)
            client = AIClient(settings, HttpClient())
            payload = client._build_rewrite_payload(
                canonical_name="Demo",
                current_maturity="早期",
                current_category="工具",
                current_summary="旧简介",
                primary_link="https://example.com/demo",
            )
            user_payload = json.loads(payload["messages"][1]["content"])
            self.assertEqual(user_payload["content_preference_zh"], "更偏向抓取能直接落地使用的工具型项目")

    def test_ai_client_uses_dedicated_ai_timeout(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text(
                "\n".join(
                    [
                        "AI_API_BASE_URL=https://example.com/v1",
                        "AI_API_KEY=key",
                        "AI_MODEL=model",
                        "AI_TIMEOUT_SECONDS=120",
                    ]
                ),
                encoding="utf-8",
            )
            settings = Settings.from_env(root)
            client = AIClient(settings, HttpClient(timeout_seconds=20))
            self.assertEqual(client.ai_http_client.timeout_seconds, 120)
            self.assertEqual(client.http_client.timeout_seconds, 20)

    def test_follow_selection_payload_includes_max_index_and_preference(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / ".env").write_text(
                "\n".join(
                    [
                        "CONTENT_PREFERENCE_ZH=偏好开发者工具和有新意的应用",
                        "AI_API_BASE_URL=https://example.com/v1",
                        "AI_API_KEY=key",
                        "AI_MODEL=model",
                    ]
                ),
                encoding="utf-8",
            )
            settings = Settings.from_env(root)
            client = AIClient(settings, HttpClient())
            payload = client._build_follow_selection_payload(text="1，3、5", max_index=10)
            user_payload = json.loads(payload["messages"][1]["content"])
            self.assertEqual(user_payload["text"], "1，3、5")
            self.assertEqual(user_payload["max_index"], 10)
            self.assertEqual(user_payload["content_preference_zh"], "偏好开发者工具和有新意的应用")

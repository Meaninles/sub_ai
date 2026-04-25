import unittest

from ai_discovery.digest import MESSAGE_HARD_CAP, build_digest_chunks
from ai_discovery.models import DigestCard


def _card(index: int, summary: str = "A short summary", name: str | None = None) -> DigestCard:
    return DigestCard(
        project_id=f"project-{index}",
        project_name=name or f"项目 {index}",
        maturity="测试版",
        category="工具",
        url=f"https://example.com/{index}",
        summary=summary,
        verification_class="verified_single_source",
        last_seen_at="2026-04-24T00:00:00+00:00",
    )


class DigestTests(unittest.TestCase):
    def test_digest_starts_with_bold_numbered_title(self) -> None:
        chunks, trimmed = build_digest_chunks([_card(1, "说明文字")])
        self.assertFalse(trimmed)
        self.assertIn("<b>1. 项目 1</b>", chunks[0].chunk.text)
        self.assertNotIn("每日项目发现", chunks[0].chunk.text)

    def test_digest_chunks_respect_message_budget(self) -> None:
        cards = [_card(index, "x" * 180) for index in range(1, 16)]
        chunks, trimmed = build_digest_chunks(cards)
        self.assertTrue(chunks)
        self.assertLessEqual(len(chunks), 5)
        self.assertFalse(trimmed)
        self.assertTrue(all(len(chunk.chunk.text) <= MESSAGE_HARD_CAP for chunk in chunks))

    def test_digest_trims_when_budget_is_exceeded(self) -> None:
        cards = [_card(index, "y" * 290, name="Project " + ("z" * 4000)) for index in range(1, 4)]
        chunks, trimmed = build_digest_chunks(cards)
        self.assertTrue(trimmed)

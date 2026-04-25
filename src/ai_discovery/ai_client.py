from __future__ import annotations

import json
import re
from dataclasses import asdict

from .config import Settings
from .http import HttpClient
from .models import ExtractionResult, Observation


SYSTEM_PROMPT = """You extract software project, product, tool, and idea discovery cards.
Return JSON only with keys:
is_project_candidate (bool),
candidate_kind (string),
project_name (string),
display_name_zh (string),
maturity (string, Simplified Chinese),
category (string, Simplified Chinese),
primary_link (string),
secondary_links (array of strings),
summary (string in Simplified Chinese, max 200 chars, optimized for helping a reader quickly understand what the project actually is and what it does),
explicit_launch_cue (bool),
rationale (string),
contradiction_notes (string),
user_relevance_score (integer 0-5),
user_relevance_rationale (string).

Accept concrete projects, products, tools, prototypes, and well-formed ideas even if they are early, pre-launch, or only being discussed.
Ideas and projects do not have an intrinsic priority difference. Judge them by clarity, specificity, and apparent potential.
Reject mainstream news, generic discussion, pure community meta posts, product updates with no clear project angle, and content that does not point to a concrete project/product/tool/idea.
Rules:
- Prefer developer tools, agent tooling, automation, repo-based projects, and novel applications or concrete ideas that provide product or implementation inspiration.
- Reject job seeking, hiring, interview-prep, recruiting, freelancer-matching, agency or consulting offers, and source-code or turnkey template sales.
- Reject generic office or document tools, generic enterprise workflow SaaS, and foreign-market business tools that offer little transfer value for a mainland China individual developer.
- Do not accept a mature mainstream product unless the item clearly surfaces a fresh technical, interaction, or product angle.
- Revenue, MRR, monetization, or funding are not automatic rejection signals. Especially for Indie Hackers style listings, judge the actual product quality, specificity, originality, implementation insight, and reference value beyond the revenue number.
- `user_relevance_score` must judge whether this item matches the operator's actual interests:
  - 5 = strongly relevant developer tool/repo/agent/automation item, or a very strong novel app/idea worth learning from
  - 4 = relevant and useful for product, implementation, or idea reference
  - 3 = somewhat relevant but weaker, more mainstream, or narrower
  - 2 = marginally relevant
  - 0-1 = likely not worth pushing for this operator
- `user_relevance_rationale` must explain briefly why the item does or does not match the operator's interests.
- If a user preference in Chinese is provided, treat it as a weak secondary hint for `user_relevance_score`. It may slightly raise or lower borderline items, but it must not override the main operator policy or the actual substance of the item.
- `project_name` must keep the canonical project/product name in its original language when that is the official name.
- `display_name_zh` must be Chinese and readable for users in Chinese. If the original product name should be preserved, use a Chinese readable form like `中文说明（OriginalName）` or `OriginalName（中文说明）`.
- `maturity`, `category`, and `summary` must be Simplified Chinese.
- Prefer the project's own site/repo/product page as `primary_link` when available. If only a discussion thread exists but the project identity is still concrete and useful, the discussion page can be used.
- `explicit_launch_cue` is a useful signal but not a hard requirement for acceptance. A concrete idea or early project can still be a valid candidate without a launch cue.
- Use `contradiction_notes` only for material conflicts or ambiguity that should lower confidence. Do not add contradiction notes just because the item is early-stage, an idea, or discussed in a community thread.
- A user preference may be provided in Chinese. Use it only as a weak tie-break or emphasis hint within already-eligible content, not to override the operator policy above.
"""

REWRITE_PROMPT = """You rewrite stored software project cards for Chinese readers.
Return JSON only with keys:
display_name_zh (string),
maturity (string, Simplified Chinese),
category (string, Simplified Chinese),
summary (string in Simplified Chinese, max 200 chars).

Requirements:
- The result must be fully understandable to a Chinese reader.
- The summary goal is not marketing language; it must explain clearly what the project is, what it does, and why someone should care.
- Do not be vague. Prefer concrete nouns, user group, and actual function.
- Keep the summary under 200 Chinese characters.
- A user preference may be provided in Chinese. Use it to bias wording and emphasis toward what the operator cares about, but keep the project itself concrete and understandable.
"""

FOLLOW_SELECTION_PROMPT = """You parse Telegram user replies for selecting project numbers from the most recent delivered project list.
Return JSON only with keys:
is_numeric_selection (bool),
selected_indexes (array of integers),
rationale (string).

Rules:
- The operator reply format is often messy, but valid inputs are essentially a sequence of project indexes such as `1、2、3`, `1，3、5`, or `1 7 8，10.9，2`.
- Preserve the user's selection order while removing duplicates.
- Reject obvious non-selection text such as phone numbers, counts in sentences, requests, or any message that is not mainly a list of project indexes.
- Ignore punctuation and mixed separators when parsing.
- Only keep positive integers.
- `selected_indexes` must be empty when `is_numeric_selection` is false.
"""


class AIClient:
    def __init__(self, settings: Settings, http_client: HttpClient) -> None:
        self.settings = settings
        self.http_client = http_client
        self.ai_http_client = HttpClient(timeout_seconds=settings.ai_timeout_seconds, user_agent=http_client.user_agent)

    def ensure_ready(self) -> None:
        if not self.settings.has_ai_config:
            raise RuntimeError("AI provider gate failed: AI_API_BASE_URL, AI_API_KEY, and AI_MODEL must be configured.")

    def extract(self, observation: Observation) -> ExtractionResult:
        self.ensure_ready()
        payload = self._build_extract_payload(observation)
        headers = {"Authorization": f"Bearer {self.settings.ai_api_key}"}
        response = self.ai_http_client.post_json(
            f"{self.settings.ai_api_base_url}/chat/completions",
            payload,
            headers=headers,
            retries=2,
        )
        content = response["choices"][0]["message"]["content"]
        extracted = _parse_json_object(content)
        result = ExtractionResult(
            is_project_candidate=bool(extracted.get("is_project_candidate", False)),
            candidate_kind=str(extracted.get("candidate_kind", "other") or "other"),
            project_name=str(extracted.get("project_name", "")).strip(),
            display_name_zh=str(extracted.get("display_name_zh", "")).strip(),
            maturity=str(extracted.get("maturity", "unknown") or "unknown").strip(),
            category=str(extracted.get("category", "unknown") or "unknown").strip(),
            primary_link=str(extracted.get("primary_link", "")).strip(),
            secondary_links=[str(item).strip() for item in extracted.get("secondary_links", []) if str(item).strip()],
            summary=str(extracted.get("summary", "")).strip()[:200],
            explicit_launch_cue=bool(extracted.get("explicit_launch_cue", False)),
            rationale=str(extracted.get("rationale", "")).strip(),
            contradiction_notes=str(extracted.get("contradiction_notes", "")).strip(),
            user_relevance_score=max(0, min(5, int(extracted.get("user_relevance_score", 0) or 0))),
            user_relevance_rationale=str(extracted.get("user_relevance_rationale", "")).strip(),
        )
        if not result.project_name and result.is_project_candidate:
            raise ValueError("AI extraction missing project_name for candidate output.")
        return result

    def _build_extract_payload(self, observation: Observation) -> dict:
        user_payload = {
            "source_id": observation.source_id,
            "title": observation.title,
            "body_text": observation.body_text,
            "source_url": observation.source_url,
            "raw_payload": observation.raw_payload,
        }
        if self.settings.content_preference_zh:
            user_payload["content_preference_zh"] = self.settings.content_preference_zh
        return {
            "model": self.settings.ai_model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": json.dumps(user_payload, ensure_ascii=False),
                },
            ],
        }

    def rewrite_project_copy(
        self,
        *,
        canonical_name: str,
        current_maturity: str,
        current_category: str,
        current_summary: str,
        primary_link: str,
    ) -> dict:
        self.ensure_ready()
        payload = self._build_rewrite_payload(
            canonical_name=canonical_name,
            current_maturity=current_maturity,
            current_category=current_category,
            current_summary=current_summary,
            primary_link=primary_link,
        )
        headers = {"Authorization": f"Bearer {self.settings.ai_api_key}"}
        response = self.ai_http_client.post_json(
            f"{self.settings.ai_api_base_url}/chat/completions",
            payload,
            headers=headers,
            retries=2,
        )
        content = response["choices"][0]["message"]["content"]
        rewritten = _parse_json_object(content)
        return {
            "display_name_zh": str(rewritten.get("display_name_zh", "")).strip(),
            "maturity": str(rewritten.get("maturity", "")).strip(),
            "category": str(rewritten.get("category", "")).strip(),
            "summary": str(rewritten.get("summary", "")).strip()[:200],
        }

    def parse_follow_selection(self, *, text: str, max_index: int) -> dict:
        self.ensure_ready()
        payload = self._build_follow_selection_payload(text=text, max_index=max_index)
        headers = {"Authorization": f"Bearer {self.settings.ai_api_key}"}
        response = self.ai_http_client.post_json(
            f"{self.settings.ai_api_base_url}/chat/completions",
            payload,
            headers=headers,
            retries=2,
        )
        content = response["choices"][0]["message"]["content"]
        parsed = _parse_json_object(content)
        raw_indexes = parsed.get("selected_indexes", [])
        normalized_indexes: list[int] = []
        seen: set[int] = set()
        for item in raw_indexes if isinstance(raw_indexes, list) else []:
            try:
                value = int(item)
            except (TypeError, ValueError):
                continue
            if value <= 0 or value > max_index or value in seen:
                continue
            seen.add(value)
            normalized_indexes.append(value)
        is_numeric_selection = bool(parsed.get("is_numeric_selection", False)) and bool(normalized_indexes)
        return {
            "is_numeric_selection": is_numeric_selection,
            "selected_indexes": normalized_indexes if is_numeric_selection else [],
            "rationale": str(parsed.get("rationale", "")).strip(),
        }

    def _build_rewrite_payload(
        self,
        *,
        canonical_name: str,
        current_maturity: str,
        current_category: str,
        current_summary: str,
        primary_link: str,
    ) -> dict:
        user_payload = {
            "canonical_name": canonical_name,
            "current_maturity": current_maturity,
            "current_category": current_category,
            "current_summary": current_summary,
            "primary_link": primary_link,
        }
        if self.settings.content_preference_zh:
            user_payload["content_preference_zh"] = self.settings.content_preference_zh
        return {
            "model": self.settings.ai_model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": REWRITE_PROMPT},
                {
                    "role": "user",
                    "content": json.dumps(user_payload, ensure_ascii=False),
                },
            ],
        }

    def _build_follow_selection_payload(self, *, text: str, max_index: int) -> dict:
        user_payload = {
            "text": text,
            "max_index": max_index,
        }
        if self.settings.content_preference_zh:
            user_payload["content_preference_zh"] = self.settings.content_preference_zh
        return {
            "model": self.settings.ai_model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": FOLLOW_SELECTION_PROMPT},
                {
                    "role": "user",
                    "content": json.dumps(user_payload, ensure_ascii=False),
                },
            ],
        }


def _parse_json_object(content: str) -> dict:
    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```[a-zA-Z0-9_+-]*\n?", "", content)
        content = re.sub(r"\n?```$", "", content)
    return json.loads(content)

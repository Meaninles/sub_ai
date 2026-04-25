from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class SourceTier(str, Enum):
    TIER1 = "tier1_direct"
    TIER2 = "tier2_supporting"
    TIER3 = "tier3_deferred"


class VerificationState(str, Enum):
    OBSERVED = "observed"
    EXTRACTED = "extracted"
    CANDIDATE = "candidate"
    VERIFIED_SINGLE_SOURCE = "verified_single_source"
    VERIFIED_MULTI_SOURCE = "verified_multi_source"
    DIGEST_ELIGIBLE = "digest_eligible"
    SENT = "sent"
    REJECTED = "rejected"


@dataclass(slots=True)
class SourceProfile:
    source_id: str
    input_url: str
    normalized_url: str
    tier: SourceTier
    active: bool
    can_originate_candidate: bool
    kind: str
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Observation:
    source_id: str
    external_id: str
    observed_at: str
    title: str
    body_text: str
    source_url: str
    raw_payload: dict[str, Any]

    @property
    def content_hash(self) -> str:
        payload = json.dumps(self.raw_payload, sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @property
    def observation_id(self) -> str:
        return f"{self.source_id}:{self.external_id}"


@dataclass(slots=True)
class ExtractionResult:
    is_project_candidate: bool
    candidate_kind: str
    project_name: str
    display_name_zh: str
    maturity: str
    category: str
    primary_link: str
    secondary_links: list[str]
    summary: str
    explicit_launch_cue: bool
    rationale: str
    contradiction_notes: str
    user_relevance_score: int = 0
    user_relevance_rationale: str = ""

    def normalized_summary(self) -> str:
        summary = " ".join(self.summary.split())
        return summary[:200]

    def as_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True)


@dataclass(slots=True)
class ProjectRecord:
    project_id: str
    canonical_name: str
    display_name_zh: str
    primary_link: str
    maturity: str
    category: str
    summary_200: str
    verification_state: VerificationState
    verification_class: str
    verification_reason: str
    first_seen_at: str
    last_seen_at: str
    evidence_flags: dict[str, Any]
    secondary_links: list[str] = field(default_factory=list)


@dataclass(slots=True)
class DigestCard:
    project_id: str
    project_name: str
    maturity: str
    category: str
    url: str
    summary: str
    verification_class: str
    last_seen_at: str


@dataclass(slots=True)
class DigestChunk:
    chunk_index: int
    text: str
    item_count: int


@dataclass(slots=True)
class PackedDigestChunk:
    chunk: DigestChunk
    cards: list[DigestCard]

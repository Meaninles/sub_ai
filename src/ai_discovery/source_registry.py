from __future__ import annotations

import hashlib
from urllib.parse import urlparse

from .config import Settings
from .models import SourceProfile, SourceTier
from .http import HttpClient


def _stable_id(prefix: str, value: str) -> str:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:10]
    return f"{prefix}_{digest}"


def _deferred_profile(input_url: str, normalized_url: str, *, source_id: str | None = None, reason: str) -> SourceProfile:
    return SourceProfile(
        source_id=source_id or _stable_id("deferred", normalized_url),
        input_url=input_url,
        normalized_url=normalized_url,
        tier=SourceTier.TIER3,
        active=False,
        can_originate_candidate=False,
        kind="deferred",
        reason=reason,
    )


def _generic_profile(
    input_url: str,
    normalized_url: str,
    *,
    source_id: str | None = None,
    reason: str,
    kind: str = "generic_page",
) -> SourceProfile:
    return SourceProfile(
        source_id=source_id or _stable_id("generic", normalized_url),
        input_url=input_url,
        normalized_url=normalized_url,
        tier=SourceTier.TIER1,
        active=True,
        can_originate_candidate=True,
        kind=kind,
        reason=reason,
    )


def load_source_profiles(settings: Settings) -> list[SourceProfile]:
    lines = [line.strip() for line in settings.source_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    profiles: list[SourceProfile] = []
    seen: set[str] = set()
    for line in lines:
        normalized = HttpClient.canonicalize_url(line)
        if normalized in seen:
            continue
        seen.add(normalized)
        profiles.append(classify_source(line, normalized, settings))
    profiles.extend(supporting_source_profiles())
    return profiles


def supporting_source_profiles() -> list[SourceProfile]:
    return [
        SourceProfile(
            source_id="github_repo_metadata",
            input_url="github://repo-metadata",
            normalized_url="github://repo-metadata",
            tier=SourceTier.TIER2,
            active=False,
            can_originate_candidate=False,
            kind="github_repo_metadata",
            reason="Tier 2 supporting source for corroboration only; never originates candidates.",
        )
    ]


def classify_source(input_url: str, normalized_url: str, settings: Settings) -> SourceProfile:
    parsed = urlparse(normalized_url)
    host = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    if host == "news.ycombinator.com" and path == "/show":
        return SourceProfile(
            source_id="hn_show",
            input_url=input_url,
            normalized_url=normalized_url,
            tier=SourceTier.TIER1,
            active=True,
            can_originate_candidate=True,
            kind="hn_show",
            reason="Tier 1 direct source via official HN API.",
        )
    if host == "github.com" and path in {"/trending", "/trending/developers"}:
        return _generic_profile(
            input_url,
            normalized_url,
            kind="github_trending",
            reason="Tier 1 direct source via GitHub Trending card extraction.",
        )
    if host.endswith("reddit.com") and path.startswith("/r/"):
        return _generic_profile(
            input_url,
            normalized_url,
            kind="reddit_listing",
            reason="Tier 1 direct source via Reddit post extraction.",
        )
    if host == "www.indiehackers.com" and path == "/ideas":
        return _generic_profile(
            input_url,
            normalized_url,
            kind="indiehackers_ideas",
            reason="Tier 1 direct source via Indie Hackers idea card extraction.",
        )
    if host == "www.indiehackers.com" and path == "/products":
        return _generic_profile(
            input_url,
            normalized_url,
            kind="indiehackers_products",
            reason="Tier 1 direct source via Indie Hackers product directory search results.",
        )
    if host == "solo.xin":
        return _generic_profile(
            input_url,
            normalized_url,
            kind="solo_topics",
            reason="Tier 1 direct source via Solo topic extraction.",
        )
    return _generic_profile(
        input_url,
        normalized_url,
        reason="Tier 1 direct source via generic page extraction.",
    )

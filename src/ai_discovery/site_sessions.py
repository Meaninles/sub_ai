from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse
from typing import Any


SITE_SESSION_CONFIG: dict[str, dict[str, object]] = {
    "reddit": {
        "label": "Reddit",
        "domains": ["reddit.com", "www.reddit.com", "old.reddit.com"],
        "description": "适用于 Reddit 页面抓取。支持直接粘贴 Cookie Header，或粘贴 Cookie-Editor 导出的完整 JSON。",
    },
    "github": {
        "label": "GitHub",
        "domains": ["github.com", "www.github.com", "gist.github.com", "api.github.com"],
        "description": "适用于 GitHub 页面或趋势页抓取。支持直接粘贴 Cookie Header，或粘贴 Cookie-Editor 导出的完整 JSON。",
    },
    "x": {
        "label": "X / Twitter",
        "domains": ["x.com", "www.x.com", "twitter.com", "www.twitter.com", "mobile.twitter.com"],
        "description": "适用于 X/Twitter 页面抓取。支持直接粘贴 Cookie Header 或 Cookie-Editor JSON，建议同时填写浏览器 User-Agent。",
    },
}


def site_sessions_path(project_root: Path) -> Path:
    return project_root / ".omx" / "data" / "site_sessions.json"


def load_site_sessions(project_root: Path) -> dict[str, dict[str, str]]:
    path = site_sessions_path(project_root)
    if not path.exists():
        return default_site_sessions()
    raw = json.loads(path.read_text(encoding="utf-8"))
    return normalize_site_sessions(raw)


def save_site_sessions(project_root: Path, sessions: dict[str, dict[str, str]]) -> None:
    path = site_sessions_path(project_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = normalize_site_sessions(sessions)
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def default_site_sessions() -> dict[str, dict[str, str]]:
    return {key: {"cookie": "", "user_agent": ""} for key in SITE_SESSION_CONFIG}


def normalize_site_sessions(raw: dict | None) -> dict[str, dict[str, str]]:
    normalized = default_site_sessions()
    if not isinstance(raw, dict):
        return normalized
    for key in normalized:
        value = raw.get(key, {})
        if not isinstance(value, dict):
            continue
        normalized[key] = {
            "cookie": normalize_cookie_header(str(value.get("cookie", "")), site_key=key),
            "user_agent": " ".join(str(value.get("user_agent", "")).split()),
        }
    return normalized


def normalize_cookie_header(raw: str, site_key: str | None = None) -> str:
    value = raw.strip()
    if not value:
        return ""
    parsed_json = _normalize_cookie_editor_json(value, site_key=site_key)
    if parsed_json is not None:
        return parsed_json
    return _normalize_plain_cookie_header(value)


def _normalize_cookie_editor_json(raw: str, site_key: str | None) -> str | None:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if isinstance(payload, str):
        return _normalize_plain_cookie_header(payload)
    cookie_pairs = _extract_cookie_pairs(payload, site_key=site_key)
    if cookie_pairs:
        return "; ".join(f"{name}={value}" for name, value in cookie_pairs)
    cookie_header = _extract_cookie_header_text(payload)
    if cookie_header:
        return _normalize_plain_cookie_header(cookie_header)
    return ""


def _normalize_plain_cookie_header(raw: str) -> str:
    value = raw.strip()
    if value.lower().startswith("cookie:"):
        value = value.split(":", 1)[1].strip()
    pieces: list[str] = []
    for line in value.splitlines():
        stripped = line.strip().strip(";")
        if not stripped:
            continue
        if ":" in stripped and "=" not in stripped:
            continue
        pieces.extend(part.strip() for part in stripped.split(";") if part.strip())
    return "; ".join(pieces)


def _extract_cookie_pairs(
    payload: Any,
    *,
    site_key: str | None,
    scope_host: str | None = None,
) -> list[tuple[str, str]]:
    if isinstance(payload, list):
        pairs: list[tuple[str, str]] = []
        for item in payload:
            pairs.extend(_extract_cookie_pairs(item, site_key=site_key, scope_host=scope_host))
        return pairs
    if not isinstance(payload, dict):
        return []
    next_scope_host = _extract_scope_host(payload) or scope_host
    if _looks_like_cookie_item(payload):
        if not _cookie_matches_site(next_scope_host, site_key):
            return []
        name = str(payload.get("name", "")).strip()
        if not name:
            return []
        return [(name, str(payload.get("value", "")))]
    pairs: list[tuple[str, str]] = []
    for value in payload.values():
        pairs.extend(_extract_cookie_pairs(value, site_key=site_key, scope_host=next_scope_host))
    return pairs


def _extract_scope_host(payload: dict[str, Any]) -> str:
    for key in ("domain", "host", "hostname"):
        value = str(payload.get(key, "")).strip().lower().lstrip(".")
        if value:
            return value
    url = str(payload.get("url", "")).strip()
    if not url:
        return ""
    parsed = urlparse(url)
    return parsed.netloc.lower().lstrip(".")


def _extract_cookie_header_text(payload: Any) -> str:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, list):
        for item in payload:
            header = _extract_cookie_header_text(item)
            if header:
                return header
        return ""
    if not isinstance(payload, dict):
        return ""
    for key in ("cookie", "cookie_header", "cookieHeader", "header"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    for value in payload.values():
        header = _extract_cookie_header_text(value)
        if header:
            return header
    return ""


def _looks_like_cookie_item(payload: dict[str, Any]) -> bool:
    return "name" in payload and "value" in payload


def _cookie_matches_site(scope_host: str | None, site_key: str | None) -> bool:
    if not site_key or not scope_host:
        return True
    config = SITE_SESSION_CONFIG.get(site_key, {})
    domains = config.get("domains", [])
    return any(_host_matches(scope_host, str(domain)) for domain in domains)


def session_headers_for_url(url: str, sessions: dict[str, dict[str, str]] | None) -> dict[str, str]:
    if not sessions:
        return {}
    host = urlparse(url).netloc.lower()
    if not host:
        return {}
    for key, config in SITE_SESSION_CONFIG.items():
        domains = config.get("domains", [])
        if not any(_host_matches(host, str(domain)) for domain in domains):
            continue
        session = sessions.get(key, {})
        headers: dict[str, str] = {}
        cookie = normalize_cookie_header(str(session.get("cookie", "")))
        user_agent = " ".join(str(session.get("user_agent", "")).split())
        if cookie:
            headers["Cookie"] = cookie
        if user_agent:
            headers["User-Agent"] = user_agent
        return headers
    return {}


def _host_matches(host: str, domain: str) -> bool:
    normalized = domain.lower().lstrip(".")
    return host == normalized or host.endswith(f".{normalized}")

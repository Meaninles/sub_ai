from __future__ import annotations

import hashlib
import html
from html.parser import HTMLParser
import re
from datetime import UTC, datetime
from urllib.parse import urljoin, urlparse

from .config import Settings
from .http import HttpClient
from .models import Observation, SourceProfile


_CONTENT_CONTAINER_TAGS = {"article", "li", "section", "div", "main", "shreddit-post", "faceplate-tracker"}
_GITHUB_RESERVED_SEGMENTS = {
    "",
    "about",
    "account",
    "blog",
    "collections",
    "contact",
    "customer-stories",
    "enterprise",
    "explore",
    "features",
    "gist",
    "issues",
    "login",
    "marketplace",
    "new",
    "notifications",
    "organizations",
    "orgs",
    "pricing",
    "pulls",
    "search",
    "security",
    "sessions",
    "settings",
    "signup",
    "site",
    "sponsors",
    "topics",
    "trending",
    "users",
}
_REDDIT_HOSTS = {"reddit.com", "www.reddit.com", "old.reddit.com"}
_REDDIT_EXTERNAL_EXCLUDED_HOSTS = _REDDIT_HOSTS | {"preview.redd.it", "i.redd.it", "v.redd.it"}
_REDDIT_NOISE_TEXT = {
    "comments",
    "comment",
    "share",
    "award",
    "reply",
    "save",
    "report",
    "join",
    "sort by",
}


class UnifiedSourceFetcher:
    def __init__(self, settings: Settings, http_client: HttpClient) -> None:
        self.settings = settings
        self.http_client = http_client

    def fetch(self, profile: SourceProfile) -> list[Observation]:
        if profile.kind == "hn_show":
            return self._fetch_hn_show(profile)
        if profile.kind == "github_trending":
            return self._fetch_github_trending(profile)
        if profile.kind == "reddit_listing":
            return self._fetch_structured_page(profile, _extract_reddit_post_candidates)
        if profile.kind == "indiehackers_ideas":
            return self._fetch_indiehackers_section(profile, section="ideas")
        if profile.kind == "indiehackers_products":
            return self._fetch_indiehackers_section(profile, section="products")
        if profile.kind == "solo_topics":
            return self._fetch_structured_page(profile, _extract_solo_topic_candidates)
        if profile.kind == "generic_page":
            return self._fetch_generic_page(profile)
        return []

    def fetch_supporting(self, profile: SourceProfile, *, primary_link: str) -> dict | None:
        if profile.kind == "github_repo_metadata":
            return self._fetch_github_repo_metadata(primary_link)
        return None

    def _fetch_hn_show(self, profile: SourceProfile) -> list[Observation]:
        ids = self.http_client.get_json("https://hacker-news.firebaseio.com/v0/showstories.json")
        if self.settings.fetch_limit_hn > 0:
            ids = ids[: self.settings.fetch_limit_hn]
        observations: list[Observation] = []
        for story_id in ids:
            item = self.http_client.get_json(f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json")
            observed_at = datetime.fromtimestamp(int(item.get("time", 0)), UTC).isoformat()
            body_parts = [item.get("title", ""), item.get("text", ""), item.get("url", "")]
            observations.append(
                Observation(
                    source_id=profile.source_id,
                    external_id=str(item["id"]),
                    observed_at=observed_at,
                    title=str(item.get("title", "")).strip(),
                    body_text=" ".join(part for part in body_parts if part).strip(),
                    source_url=f"https://news.ycombinator.com/item?id={item['id']}",
                    raw_payload=item,
                )
            )
        return observations

    def _fetch_github_repo_metadata(self, primary_link: str) -> dict | None:
        parsed = self.http_client.canonicalize_url(primary_link)
        url = urlparse(parsed)
        if url.netloc.lower() != "github.com":
            return None
        path = parsed.split("github.com", 1)[-1].strip("/")
        path_parts = [part for part in path.split("/") if part]
        if len(path_parts) < 2:
            return None
        owner, repo = path_parts[:2]
        return self.http_client.get_json(f"https://api.github.com/repos/{owner}/{repo}")

    def _fetch_github_trending(self, profile: SourceProfile) -> list[Observation]:
        return self._fetch_structured_page(
            profile,
            _extract_github_trending_candidates,
            enrich_candidate=self._enrich_github_trending_candidate,
        )

    def _fetch_indiehackers_section(self, profile: SourceProfile, *, section: str) -> list[Observation]:
        return self._fetch_structured_page(
            profile,
            lambda root, base_url: _extract_indiehackers_candidates(root, base_url, section=section),
            enrich_candidate=self._enrich_indiehackers_candidate,
        )

    def _fetch_structured_page(
        self,
        profile: SourceProfile,
        extractor: callable,
        *,
        enrich_candidate: callable | None = None,
    ) -> list[Observation]:
        response = self.http_client.request("GET", profile.normalized_url, retries=1)
        page_title = _extract_title(response.body)
        root = _parse_html(response.body)
        candidates = extractor(root, response.final_url)
        observations: list[Observation] = []
        seen: set[str] = set()
        limit = self.settings.fetch_limit_generic if self.settings.fetch_limit_generic > 0 else len(candidates)
        for candidate in candidates:
            source_url = candidate["url"]
            if source_url in seen:
                continue
            seen.add(source_url)
            title = candidate["title"].strip() or page_title or profile.input_url
            context = " ".join(str(candidate.get("context_text", "")).split())
            enrichment = enrich_candidate(candidate) if enrich_candidate else {}
            body_lines = [
                f"Page title: {page_title}",
                f"Item title: {title}",
                f"Item URL: {source_url}",
            ]
            if candidate.get("discussion_url"):
                body_lines.append(f"Discussion URL: {candidate['discussion_url']}")
            if candidate.get("external_url") and candidate["external_url"] != source_url:
                body_lines.append(f"External URL: {candidate['external_url']}")
            if context:
                body_lines.append(f"Item context: {context[:1200]}")
            body_lines.extend(
                line for line in enrichment.get("body_lines", []) if isinstance(line, str) and line.strip()
            )
            external_id = hashlib.sha1(f"{profile.source_id}|{source_url}|{title}".encode("utf-8")).hexdigest()[:16]
            observations.append(
                Observation(
                    source_id=profile.source_id,
                    external_id=external_id,
                    observed_at=datetime.now(UTC).isoformat(),
                    title=title,
                    body_text="\n".join(body_lines),
                    source_url=source_url,
                    raw_payload={
                        "page_url": response.final_url,
                        "page_title": page_title,
                        **candidate.get("raw_payload", {}),
                        **enrichment.get("raw_payload", {}),
                    },
                )
            )
            if len(observations) >= limit:
                break
        if observations:
            return observations
        return self._fetch_generic_page(profile)

    def _fetch_generic_page(self, profile: SourceProfile) -> list[Observation]:
        response = self.http_client.request("GET", profile.normalized_url, retries=1)
        page_title = _extract_title(response.body)
        page_text = _extract_text(response.body)
        anchors = _extract_anchors(response.body, response.final_url)
        observations: list[Observation] = []
        seen: set[str] = set()
        limit = self.settings.fetch_limit_generic if self.settings.fetch_limit_generic > 0 else len(anchors)

        for anchor in anchors:
            anchor_url = anchor["url"]
            anchor_text = anchor["text"]
            if anchor_url in seen:
                continue
            seen.add(anchor_url)
            external_id = hashlib.sha1(f"{profile.source_id}|{anchor_url}|{anchor_text}".encode("utf-8")).hexdigest()[:16]
            observations.append(
                Observation(
                    source_id=profile.source_id,
                    external_id=external_id,
                    observed_at=datetime.now(UTC).isoformat(),
                    title=anchor_text or page_title or profile.input_url,
                    body_text=f"Page title: {page_title}\nLink text: {anchor_text}\nLinked URL: {anchor_url}\nPage text excerpt: {page_text[:1200]}",
                    source_url=anchor_url,
                    raw_payload={
                        "page_url": response.final_url,
                        "page_title": page_title,
                        "link_text": anchor_text,
                        "link_url": anchor_url,
                    },
                )
            )
            if len(observations) >= limit:
                break

        if observations:
            return observations

        fallback_id = hashlib.sha1(f"{profile.source_id}|{response.final_url}".encode("utf-8")).hexdigest()[:16]
        return [
            Observation(
                source_id=profile.source_id,
                external_id=fallback_id,
                observed_at=datetime.now(UTC).isoformat(),
                title=page_title or profile.input_url,
                body_text=page_text[:1500],
                source_url=response.final_url,
                raw_payload={"page_url": response.final_url, "page_title": page_title},
            )
        ]

    def _enrich_indiehackers_candidate(self, candidate: dict[str, str | dict]) -> dict[str, list[str] | dict]:
        detail_url = str(candidate.get("url", "")).strip()
        if not detail_url:
            return {}
        try:
            response = self.http_client.request("GET", detail_url, retries=1)
        except Exception:
            return {}
        detail_title = _extract_title(response.body)
        detail_root = _parse_html(response.body)
        detail_text = " ".join(_node_text(_main_content(detail_root)).split())
        external_links = _external_links(
            detail_root,
            response.final_url,
            excluded_hosts={"indiehackers.com", "www.indiehackers.com"},
        )
        preferred_external = _preferred_external_link(external_links)
        body_lines: list[str] = []
        if detail_title:
            body_lines.append(f"Detail title: {detail_title}")
        if detail_text:
            body_lines.append(f"Detail context: {detail_text[:2000]}")
        if external_links:
            body_lines.append(f"Detail links: {' | '.join(link['url'] for link in external_links[:5])}")
        raw_payload: dict[str, str | list[str]] = {
            "detail_url": response.final_url,
            "detail_page_title": detail_title,
            "detail_text_excerpt": detail_text[:4000],
            "detail_external_links": [link["url"] for link in external_links[:8]],
        }
        if preferred_external:
            raw_payload["external_url"] = preferred_external
        return {"body_lines": body_lines, "raw_payload": raw_payload}

    def _enrich_github_trending_candidate(self, candidate: dict[str, str | dict]) -> dict[str, list[str] | dict]:
        raw_payload = candidate.get("raw_payload", {})
        if not isinstance(raw_payload, dict):
            return {}
        repo_url = _github_repo_root_url(str(raw_payload.get("repo_url", "")).strip() or str(candidate.get("url", "")).strip())
        if not repo_url:
            return {}
        try:
            metadata = self._fetch_github_repo_metadata(repo_url)
        except Exception:
            return {}
        if not isinstance(metadata, dict):
            return {}
        description = str(metadata.get("description", "") or "").strip()
        homepage = str(metadata.get("homepage", "") or "").strip()
        language = str(metadata.get("language", "") or "").strip()
        topics = [str(topic).strip() for topic in metadata.get("topics", []) if str(topic).strip()]
        body_lines: list[str] = []
        if description:
            body_lines.append(f"Repository description: {description}")
        if language:
            body_lines.append(f"Primary language: {language}")
        if topics:
            body_lines.append(f"Repository topics: {', '.join(topics[:10])}")
        if homepage:
            body_lines.append(f"Repository homepage: {homepage}")
        stars = metadata.get("stargazers_count")
        if isinstance(stars, int):
            body_lines.append(f"GitHub stars: {stars}")
        metadata_payload = {
            "full_name": str(metadata.get("full_name", "") or "").strip(),
            "description": description,
            "homepage": homepage,
            "language": language,
            "topics": topics[:10],
            "stargazers_count": stars if isinstance(stars, int) else 0,
            "forks_count": metadata.get("forks_count", 0) if isinstance(metadata.get("forks_count"), int) else 0,
        }
        return {"body_lines": body_lines, "raw_payload": {"repo_metadata": metadata_payload}}


def _extract_title(body: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", body, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return " ".join(html.unescape(_strip_tags(match.group(1))).split())


def _extract_text(body: str) -> str:
    body = re.sub(r"<script.*?>.*?</script>", " ", body, flags=re.IGNORECASE | re.DOTALL)
    body = re.sub(r"<style.*?>.*?</style>", " ", body, flags=re.IGNORECASE | re.DOTALL)
    return " ".join(html.unescape(_strip_tags(body)).split())


def _extract_anchors(body: str, base_url: str) -> list[dict[str, str]]:
    anchors: list[dict[str, str]] = []
    for match in re.finditer(r"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", body, flags=re.IGNORECASE | re.DOTALL):
        raw_href, raw_text = match.groups()
        text = " ".join(html.unescape(_strip_tags(raw_text)).split())
        if len(text) < 3:
            continue
        if raw_href.startswith(("#", "javascript:", "mailto:")):
            continue
        resolved = urljoin(base_url, raw_href)
        parsed = urlparse(resolved)
        if parsed.scheme not in {"http", "https"}:
            continue
        anchors.append({"url": resolved, "text": text})
    return anchors


def _extract_github_trending_candidates(root: "HtmlNode", base_url: str) -> list[dict[str, str | dict]]:
    main = _main_content(root)
    candidates: list[dict[str, str | dict]] = []
    seen: set[str] = set()
    for article in _iter_nodes(main, tag="article"):
        anchors = _anchor_records(article, base_url)
        repo_anchors = []
        for anchor in anchors:
            repo_root = _github_repo_root_url(str(anchor["url"]))
            if not repo_root:
                continue
            repo_anchors.append({**anchor, "url": repo_root})
        developer_anchors = [anchor for anchor in anchors if _is_github_developer_url(anchor["url"])]
        if not repo_anchors and not developer_anchors:
            continue
        primary_repo = _prefer_heading_anchor(repo_anchors)
        primary_developer = _prefer_heading_anchor(developer_anchors)
        primary = primary_repo or primary_developer
        if primary is None:
            continue
        source_url = primary["url"]
        if source_url in seen:
            continue
        seen.add(source_url)
        title = primary["text"] or _github_display_name(source_url)
        if primary_repo and primary_developer and primary_developer["text"]:
            context_title = f"{title} | Developer: {primary_developer['text']}"
        else:
            context_title = title
        candidates.append(
            {
                "title": context_title,
                "url": source_url,
                "context_text": _node_text(article),
                "raw_payload": {
                    "item_kind": "github_trending",
                    "repo_url": primary_repo["url"] if primary_repo else "",
                    "developer_url": primary_developer["url"] if primary_developer else "",
                },
            }
        )
    return candidates


def _extract_reddit_post_candidates(root: "HtmlNode", base_url: str) -> list[dict[str, str | dict]]:
    main = _main_content(root)
    candidates: list[dict[str, str | dict]] = []
    seen: set[str] = set()
    for anchor in _anchor_records(main, base_url):
        permalink = _normalize_url(anchor["url"])
        if not _is_reddit_comments_url(permalink):
            continue
        if permalink in seen:
            continue
        container = _nearest_content_container(anchor["node"], main)
        title = _select_reddit_title(container)
        if not title:
            continue
        external_url = _find_reddit_external_url(container, base_url)
        source_url = external_url or permalink
        seen.add(permalink)
        candidates.append(
            {
                "title": title,
                "url": source_url,
                "discussion_url": permalink,
                "external_url": external_url or "",
                "context_text": _node_text(container),
                "raw_payload": {
                    "item_kind": "reddit_post",
                    "discussion_url": permalink,
                    "external_url": external_url or "",
                },
            }
        )
    return candidates


def _extract_indiehackers_candidates(root: "HtmlNode", base_url: str, *, section: str) -> list[dict[str, str | dict]]:
    main = _main_content(root)
    candidates: list[dict[str, str | dict]] = []
    seen: set[str] = set()
    for anchor in _anchor_records(main, base_url):
        detail_url = _normalize_url(anchor["url"])
        if not _is_indiehackers_detail_url(detail_url, section):
            continue
        if detail_url in seen:
            continue
        container = _nearest_content_container(anchor["node"], main)
        title = anchor["text"] or _select_best_anchor_text(container)
        if not title:
            continue
        seen.add(detail_url)
        candidates.append(
            {
                "title": title,
                "url": detail_url,
                "context_text": _node_text(container),
                "raw_payload": {
                    "item_kind": f"indiehackers_{section}",
                    "detail_url": detail_url,
                },
            }
        )
    return candidates


def _extract_solo_topic_candidates(root: "HtmlNode", base_url: str) -> list[dict[str, str | dict]]:
    main = _main_content(root)
    candidates: list[dict[str, str | dict]] = []
    seen: set[str] = set()
    for anchor in _anchor_records(main, base_url):
        topic_url = _normalize_url(anchor["url"])
        if not _is_solo_topic_url(topic_url):
            continue
        if topic_url in seen:
            continue
        container = _nearest_content_container(anchor["node"], main)
        title = anchor["text"] or _select_best_anchor_text(container)
        if not title:
            continue
        seen.add(topic_url)
        candidates.append(
            {
                "title": title,
                "url": topic_url,
                "context_text": _node_text(container),
                "raw_payload": {
                    "item_kind": "solo_topic",
                    "topic_url": topic_url,
                },
            }
        )
    return candidates


class HtmlNode:
    def __init__(self, tag: str, attrs: dict[str, str], parent: "HtmlNode | None" = None) -> None:
        self.tag = tag
        self.attrs = attrs
        self.parent = parent
        self.children: list[HtmlNode] = []
        self.text_chunks: list[str] = []

    def append_child(self, child: "HtmlNode") -> None:
        self.children.append(child)


class _HtmlTreeBuilder(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = HtmlNode("document", {})
        self.stack = [self.root]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        node = HtmlNode(tag.lower(), {key: value or "" for key, value in attrs}, parent=self.stack[-1])
        self.stack[-1].append_child(node)
        self.stack.append(node)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        node = HtmlNode(tag.lower(), {key: value or "" for key, value in attrs}, parent=self.stack[-1])
        self.stack[-1].append_child(node)

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        for idx in range(len(self.stack) - 1, 0, -1):
            if self.stack[idx].tag == lowered:
                del self.stack[idx:]
                break

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.stack[-1].text_chunks.append(data)


def _parse_html(body: str) -> HtmlNode:
    parser = _HtmlTreeBuilder()
    parser.feed(body)
    parser.close()
    return parser.root


def _main_content(root: HtmlNode) -> HtmlNode:
    for node in _iter_nodes(root, tag="main"):
        return node
    return root


def _iter_nodes(node: HtmlNode, *, tag: str | None = None):
    for child in node.children:
        if tag is None or child.tag == tag:
            yield child
        yield from _iter_nodes(child, tag=tag)


def _node_text(node: HtmlNode) -> str:
    if node.tag in {"script", "style", "noscript"}:
        return ""
    chunks = [" ".join(piece.split()) for piece in node.text_chunks if piece.strip()]
    for child in node.children:
        child_text = _node_text(child)
        if child_text:
            chunks.append(child_text)
    return " ".join(chunk for chunk in chunks if chunk).strip()


def _anchor_records(node: HtmlNode, base_url: str) -> list[dict[str, str | HtmlNode]]:
    records: list[dict[str, str | HtmlNode]] = []
    for anchor in _iter_nodes(node, tag="a"):
        href = anchor.attrs.get("href", "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:")):
            continue
        resolved = _normalize_url(urljoin(base_url, href))
        parsed = urlparse(resolved)
        if parsed.scheme not in {"http", "https"}:
            continue
        records.append({"node": anchor, "url": resolved, "text": _node_text(anchor)})
    return records


def _external_links(node: HtmlNode, base_url: str, *, excluded_hosts: set[str]) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    seen: set[str] = set()
    for anchor in _anchor_records(node, base_url):
        url = str(anchor["url"])
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if host in excluded_hosts:
            continue
        if url in seen:
            continue
        seen.add(url)
        links.append({"url": url, "text": " ".join(str(anchor["text"]).split())})
    return links


def _nearest_content_container(node: HtmlNode, boundary: HtmlNode) -> HtmlNode:
    current = node.parent or boundary
    fallback = boundary
    while current and current is not boundary:
        if current.tag in _CONTENT_CONTAINER_TAGS:
            fallback = current
            if len(_node_text(current)) >= 40:
                return current
        current = current.parent
    return fallback


def _prefer_heading_anchor(anchors: list[dict[str, str | HtmlNode]]) -> dict[str, str | HtmlNode] | None:
    if not anchors:
        return None
    for anchor in anchors:
        node = anchor["node"]
        parent = node.parent if isinstance(node, HtmlNode) else None
        if parent and parent.tag in {"h1", "h2", "h3"}:
            return anchor
    return sorted(anchors, key=lambda item: len(str(item["text"])), reverse=True)[0]


def _github_display_name(url: str) -> str:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 2:
        return f"{parts[0]} / {parts[1]}"
    return parsed.path.strip("/") or url


def _github_repo_root_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc.lower() != "github.com":
        return ""
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        return ""
    if parts[0].lower() in _GITHUB_RESERVED_SEGMENTS or parts[1].lower() in {"followers", "following"}:
        return ""
    return parsed._replace(path=f"/{parts[0]}/{parts[1]}", query="", fragment="").geturl()


def _is_github_repo_url(url: str) -> bool:
    return bool(_github_repo_root_url(url))


def _is_github_developer_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc.lower() != "github.com":
        return False
    parts = [part for part in parsed.path.split("/") if part]
    return len(parts) == 1 and parts[0].lower() not in _GITHUB_RESERVED_SEGMENTS


def _is_reddit_comments_url(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    parts = [part for part in parsed.path.split("/") if part]
    return host in _REDDIT_HOSTS and "comments" in parts


def _select_reddit_title(container: HtmlNode) -> str:
    candidates: list[str] = []
    for anchor in _anchor_records(container, "https://www.reddit.com"):
        text = " ".join(str(anchor["text"]).split())
        if not text or len(text) < 8:
            continue
        lowered = text.lower()
        if lowered.startswith(("r/", "u/")):
            continue
        if any(token == lowered for token in _REDDIT_NOISE_TEXT):
            continue
        if lowered.endswith(" comments") or lowered.endswith(" comment"):
            continue
        candidates.append(text)
    if candidates:
        return sorted(candidates, key=len, reverse=True)[0]
    container_text = _node_text(container)
    if not container_text:
        return ""
    return container_text[:160].strip()


def _find_reddit_external_url(container: HtmlNode, base_url: str) -> str:
    for anchor in _anchor_records(container, base_url):
        parsed = urlparse(str(anchor["url"]))
        host = parsed.netloc.lower()
        if host in _REDDIT_EXTERNAL_EXCLUDED_HOSTS:
            continue
        text = " ".join(str(anchor["text"]).split())
        if len(text) < 4:
            continue
        return str(anchor["url"])
    return ""


def _is_indiehackers_detail_url(url: str, section: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc.lower() != "www.indiehackers.com":
        return False
    parts = [part for part in parsed.path.split("/") if part]
    return len(parts) >= 2 and parts[0] == section


def _is_solo_topic_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc.lower() != "solo.xin":
        return False
    parts = [part for part in parsed.path.split("/") if part]
    return len(parts) >= 2 and parts[0] == "topic"


def _select_best_anchor_text(container: HtmlNode) -> str:
    texts = [" ".join(str(anchor["text"]).split()) for anchor in _anchor_records(container, "https://example.com")]
    texts = [text for text in texts if len(text) >= 4]
    if texts:
        return sorted(texts, key=len, reverse=True)[0]
    return ""


def _preferred_external_link(links: list[dict[str, str]]) -> str:
    preferred_markers = ("visit", "website", "site", "homepage", "app", "demo", "github", "repo")
    for link in links:
        text = link["text"].lower()
        if any(marker in text for marker in preferred_markers):
            return link["url"]
    return links[0]["url"] if len(links) == 1 else ""


def _normalize_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path or "/"
    return parsed._replace(fragment="").geturl().replace("#", "")


def _strip_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", value)

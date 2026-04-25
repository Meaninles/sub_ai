from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

from .site_sessions import session_headers_for_url


@dataclass(slots=True)
class HttpResponse:
    status: int
    body: str
    headers: dict[str, str]
    final_url: str


class HttpClient:
    def __init__(
        self,
        timeout_seconds: int = 20,
        user_agent: str = "ai-project-discovery/0.1",
        site_sessions: dict[str, dict[str, str]] | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent
        self.site_sessions = site_sessions or {}

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
        retries: int = 1,
    ) -> HttpResponse:
        merged_headers = {"User-Agent": self.user_agent}
        merged_headers.update(session_headers_for_url(url, self.site_sessions))
        merged_headers.update(headers or {})
        req = urllib.request.Request(url, data=body, headers=merged_headers, method=method)
        attempt = 0
        while True:
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_seconds) as response:
                    return HttpResponse(
                        status=response.status,
                        body=response.read().decode("utf-8", errors="replace"),
                        headers=dict(response.headers.items()),
                        final_url=response.geturl(),
                    )
            except urllib.error.HTTPError as exc:
                response = exc.read().decode("utf-8", errors="replace")
                if attempt >= retries:
                    return HttpResponse(
                        status=exc.code,
                        body=response,
                        headers=dict(exc.headers.items()),
                        final_url=url,
                    )
            except urllib.error.URLError:
                if attempt >= retries:
                    raise
            attempt += 1
            time.sleep(0.5 * attempt)

    def get_json(self, url: str, *, headers: dict[str, str] | None = None, retries: int = 1) -> dict:
        response = self.request("GET", url, headers=headers, retries=retries)
        return json.loads(response.body)

    def post_json(
        self,
        url: str,
        payload: dict,
        *,
        headers: dict[str, str] | None = None,
        retries: int = 1,
    ) -> dict:
        merged_headers = {"Content-Type": "application/json", **(headers or {})}
        response = self.request(
            "POST",
            url,
            headers=merged_headers,
            body=json.dumps(payload).encode("utf-8"),
            retries=retries,
        )
        return json.loads(response.body)

    @staticmethod
    def canonicalize_url(url: str) -> str:
        parts = urllib.parse.urlsplit(url.strip())
        scheme = parts.scheme or "https"
        netloc = parts.netloc.lower()
        path = parts.path.rstrip("/") or "/"
        query = urllib.parse.parse_qsl(parts.query, keep_blank_values=False)
        query = [(k, v) for k, v in query if not k.startswith("utm_")]
        return urllib.parse.urlunsplit((scheme, netloc, path, urllib.parse.urlencode(query), ""))

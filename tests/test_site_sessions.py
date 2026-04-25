import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from ai_discovery.site_sessions import (
    load_site_sessions,
    normalize_cookie_header,
    save_site_sessions,
    session_headers_for_url,
)


class SiteSessionsTests(unittest.TestCase):
    def test_cookie_header_normalization_accepts_cookie_prefix_and_newlines(self) -> None:
        raw = "Cookie: foo=1; bar=2\nbaz=3"
        self.assertEqual(normalize_cookie_header(raw), "foo=1; bar=2; baz=3")

    def test_cookie_header_normalization_accepts_cookie_editor_array_export(self) -> None:
        raw = json.dumps(
            [
                {"domain": ".github.com", "name": "logged_in", "value": "yes"},
                {"domain": ".github.com", "name": "dotcom_user", "value": "alice"},
                {"domain": ".reddit.com", "name": "reddit_session", "value": "skip-me"},
            ],
            ensure_ascii=False,
        )
        self.assertEqual(
            normalize_cookie_header(raw, site_key="github"),
            "logged_in=yes; dotcom_user=alice",
        )

    def test_cookie_header_normalization_accepts_cookie_editor_object_export(self) -> None:
        raw = json.dumps(
            {
                "url": "https://x.com/home",
                "cookies": [
                    {"name": "auth_token", "value": "abc"},
                    {"name": "ct0", "value": "xyz"},
                ],
            },
            ensure_ascii=False,
        )
        self.assertEqual(normalize_cookie_header(raw, site_key="x"), "auth_token=abc; ct0=xyz")

    def test_save_and_load_site_sessions_round_trip(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            save_site_sessions(
                root,
                {
                    "reddit": {"cookie": "Cookie: reddit_session=1", "user_agent": "Mozilla/5.0 test"},
                    "github": {
                        "cookie": json.dumps(
                            [{"domain": ".github.com", "name": "logged_in", "value": "yes"}],
                            ensure_ascii=False,
                        ),
                        "user_agent": "",
                    },
                    "x": {"cookie": "", "user_agent": ""},
                },
            )
            loaded = load_site_sessions(root)
            self.assertEqual(loaded["reddit"]["cookie"], "reddit_session=1")
            self.assertEqual(loaded["reddit"]["user_agent"], "Mozilla/5.0 test")
            self.assertEqual(loaded["github"]["cookie"], "logged_in=yes")

    def test_session_headers_match_supported_domains(self) -> None:
        sessions = {
            "reddit": {"cookie": "reddit_session=1", "user_agent": ""},
            "github": {"cookie": "logged_in=yes", "user_agent": "Mozilla/5.0 GitHub"},
            "x": {"cookie": "auth_token=abc", "user_agent": "Mozilla/5.0 X"},
        }
        github_headers = session_headers_for_url("https://github.com/trending", sessions)
        self.assertEqual(github_headers["Cookie"], "logged_in=yes")
        self.assertEqual(github_headers["User-Agent"], "Mozilla/5.0 GitHub")
        x_headers = session_headers_for_url("https://x.com/some/path", sessions)
        self.assertEqual(x_headers["Cookie"], "auth_token=abc")

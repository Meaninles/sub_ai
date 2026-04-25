import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from ai_discovery.config import Settings
from ai_discovery.models import SourceProfile, SourceTier
from ai_discovery.sources import UnifiedSourceFetcher


class _FakeHttpClient:
    def __init__(
        self,
        html_by_url: dict[str, str],
        json_by_url: dict[str, dict] | None = None,
        post_json_by_url: dict[str, dict] | None = None,
    ) -> None:
        self.html_by_url = html_by_url
        self.json_by_url = json_by_url or {}
        self.post_json_by_url = post_json_by_url or {}
        self.posted_json: list[dict[str, object]] = []

    def request(self, method: str, url: str, retries: int = 1):  # noqa: ARG002
        return SimpleNamespace(status=200, body=self.html_by_url[url], headers={}, final_url=url)

    def get_json(self, url: str, headers: dict[str, str] | None = None, retries: int = 1):  # noqa: ARG002
        if url not in self.json_by_url:
            raise AssertionError(f"unexpected json request: {url}")
        return self.json_by_url[url]

    def post_json(
        self,
        url: str,
        payload: dict,
        headers: dict[str, str] | None = None,
        retries: int = 1,
    ):  # noqa: ARG002
        self.posted_json.append({"url": url, "payload": payload, "headers": headers or {}})
        if url not in self.post_json_by_url:
            raise AssertionError(f"unexpected post json request: {url}")
        return self.post_json_by_url[url]

    def canonicalize_url(self, url: str) -> str:
        return url


class SourceParsingTests(unittest.TestCase):
    def _settings(self) -> Settings:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        settings = Settings.from_env(root)
        settings.fetch_limit_generic = 20
        return settings

    def test_github_trending_extracts_repository_and_developer_rows(self) -> None:
        settings = self._settings()
        html = """
        <html><head><title>Trending</title></head><body>
          <nav><a href="/pricing">Pricing</a></nav>
          <main>
            <div>Repositories Developers</div>
            <article>
              <h2><a href="/acme/project-x">acme / project-x</a></h2>
              <p>LLM browser automation toolkit.</p>
            </article>
            <article>
              <h2><a href="/alice">Alice</a></h2>
              <p>Popular repo</p>
              <a href="/alice/super-agent">super-agent</a>
            </article>
          </main>
        </body></html>
        """
        fetcher = UnifiedSourceFetcher(settings, _FakeHttpClient({"https://github.com/trending": html}))
        profile = SourceProfile(
            source_id="github_trending",
            input_url="https://github.com/trending",
            normalized_url="https://github.com/trending",
            tier=SourceTier.TIER1,
            active=True,
            can_originate_candidate=True,
            kind="github_trending",
            reason="test",
        )

        observations = fetcher.fetch(profile)

        self.assertEqual(
            [item.source_url for item in observations],
            ["https://github.com/acme/project-x", "https://github.com/alice/super-agent"],
        )
        self.assertIn("Developer: Alice", observations[1].title)

    def test_github_trending_enriches_repo_metadata_and_normalizes_deep_repo_links(self) -> None:
        settings = self._settings()
        html = """
        <html><head><title>Trending</title></head><body>
          <main>
            <article>
              <h2><a href="/alice/super-agent/tree/main">alice / super-agent</a></h2>
              <p>Terminal automation toolkit.</p>
            </article>
          </main>
        </body></html>
        """
        fetcher = UnifiedSourceFetcher(
            settings,
            _FakeHttpClient(
                {"https://github.com/trending": html},
                {
                    "https://api.github.com/repos/alice/super-agent": {
                        "full_name": "alice/super-agent",
                        "description": "Automates terminal workflows with local agents.",
                        "homepage": "https://super-agent.dev",
                        "language": "TypeScript",
                        "topics": ["agent", "cli", "automation"],
                        "stargazers_count": 4200,
                        "forks_count": 180,
                    }
                },
            ),
        )
        profile = SourceProfile(
            source_id="github_trending",
            input_url="https://github.com/trending",
            normalized_url="https://github.com/trending",
            tier=SourceTier.TIER1,
            active=True,
            can_originate_candidate=True,
            kind="github_trending",
            reason="test",
        )

        observations = fetcher.fetch(profile)

        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0].source_url, "https://github.com/alice/super-agent")
        self.assertIn("Repository description: Automates terminal workflows with local agents.", observations[0].body_text)
        self.assertIn("Primary language: TypeScript", observations[0].body_text)
        self.assertEqual(observations[0].raw_payload["repo_metadata"]["homepage"], "https://super-agent.dev")

    def test_reddit_listing_extracts_posts_and_prefers_external_project_link(self) -> None:
        settings = self._settings()
        html = """
        <html><head><title>r/SideProject</title></head><body>
          <aside><a href="/r/SaaS">r/SaaS</a></aside>
          <main>
            <article>
              <a href="https://tool.example.com">Launch your AI copilot</a>
              <a href="/r/SideProject/comments/abc123/launch_your_ai_copilot/">42 comments</a>
              <a href="/user/founder">u/founder</a>
            </article>
            <article>
              <a href="/r/SideProject/comments/def456/invoice_tool/">I built an invoice tool for freelancers</a>
              <a href="/r/SideProject/comments/def456/invoice_tool/">13 comments</a>
            </article>
          </main>
        </body></html>
        """
        fetcher = UnifiedSourceFetcher(settings, _FakeHttpClient({"https://www.reddit.com/r/SideProject": html}))
        profile = SourceProfile(
            source_id="reddit_sideproject",
            input_url="https://www.reddit.com/r/SideProject",
            normalized_url="https://www.reddit.com/r/SideProject",
            tier=SourceTier.TIER1,
            active=True,
            can_originate_candidate=True,
            kind="reddit_listing",
            reason="test",
        )

        observations = fetcher.fetch(profile)

        self.assertEqual(len(observations), 2)
        self.assertEqual(observations[0].source_url, "https://tool.example.com")
        self.assertIn("Discussion URL: https://www.reddit.com/r/SideProject/comments/abc123/launch_your_ai_copilot/", observations[0].body_text)
        self.assertEqual(
            observations[1].source_url,
            "https://www.reddit.com/r/SideProject/comments/def456/invoice_tool/",
        )

    def test_indiehackers_products_extracts_recently_updated_algolia_hits_and_caps_at_30(self) -> None:
        settings = self._settings()
        settings.fetch_limit_generic = 40
        query_url = "https://N86T1R3OWZ-dsn.algolia.net/1/indexes/products/query"
        hits = [
            {
                "productId": f"product-{index}",
                "name": f"Product {index}",
                "tagline": f"Tagline {index}",
                "description": f"Description {index}",
                "websiteUrl": f"https://product-{index}.example.com",
            }
            for index in range(35)
        ]
        fetcher = UnifiedSourceFetcher(
            settings,
            _FakeHttpClient(
                {"https://www.indiehackers.com/products": "<html><title>Products</title></html>"},
                post_json_by_url={query_url: {"hits": hits}},
            ),
        )
        fetcher._enrich_indiehackers_candidate = lambda candidate: {}  # type: ignore[method-assign]
        profile = SourceProfile(
            source_id="ih_products",
            input_url="https://www.indiehackers.com/products",
            normalized_url="https://www.indiehackers.com/products",
            tier=SourceTier.TIER1,
            active=True,
            can_originate_candidate=True,
            kind="indiehackers_products",
            reason="test",
        )

        observations = fetcher.fetch(profile)

        self.assertEqual(len(observations), 30)
        self.assertEqual(observations[0].source_url, "https://www.indiehackers.com/product/product-0")
        self.assertEqual(observations[-1].source_url, "https://www.indiehackers.com/product/product-29")
        self.assertEqual(len(fetcher.http_client.posted_json), 1)
        self.assertEqual(fetcher.http_client.posted_json[0]["url"], query_url)
        self.assertEqual(fetcher.http_client.posted_json[0]["payload"], {"params": "hitsPerPage=30&page=0"})

    def test_indiehackers_products_enrich_with_product_payload_context(self) -> None:
        settings = self._settings()
        query_url = "https://N86T1R3OWZ-dsn.algolia.net/1/indexes/products/query"
        fetcher = UnifiedSourceFetcher(
            settings,
            _FakeHttpClient(
                {"https://www.indiehackers.com/products": "<html><title>Products</title></html>"},
                {
                    "https://indie-hackers.firebaseio.com/products/fitbuild.json": {
                        "name": "FitBuild",
                        "tagline": "AI workout planner",
                        "description": "Builds AI-generated training plans for busy indie hackers.",
                        "websiteUrl": "https://fitbuild.app",
                        "selfReportedMonthlyRevenue": 225,
                    },
                    "https://indie-hackers.firebaseio.com/indexes/productStats/fitbuild.json": {"numViews": 128},
                },
                post_json_by_url={
                    query_url: {
                        "hits": [
                            {
                                "productId": "fitbuild",
                                "name": "FitBuild",
                                "tagline": "AI workout planner",
                                "description": "$225+ MRR AI workout planner",
                                "websiteUrl": "https://fitbuild.app",
                            }
                        ]
                    }
                },
            ),
        )
        profile = SourceProfile(
            source_id="ih_products",
            input_url="https://www.indiehackers.com/products",
            normalized_url="https://www.indiehackers.com/products",
            tier=SourceTier.TIER1,
            active=True,
            can_originate_candidate=True,
            kind="indiehackers_products",
            reason="test",
        )

        observations = fetcher.fetch(profile)

        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0].source_url, "https://www.indiehackers.com/product/fitbuild")
        self.assertIn("Detail title: FitBuild", observations[0].body_text)
        self.assertIn("Builds AI-generated training plans for busy indie hackers.", observations[0].body_text)
        self.assertIn("Self-reported monthly revenue: 225", observations[0].body_text)
        self.assertIn("Detail links: https://fitbuild.app", observations[0].body_text)
        self.assertEqual(observations[0].raw_payload["external_url"], "https://fitbuild.app")
        self.assertEqual(observations[0].raw_payload["detail_num_views"], 128)

    def test_solo_extracts_only_topic_links(self) -> None:
        settings = self._settings()
        html = """
        <html><head><title>Solo</title></head><body>
          <main>
            <div><a href="/user/12">Alice</a></div>
            <article><a href="/topic/1001">How I launched my SaaS</a></article>
            <article><a href="/topic/1002">MVPFast 2.0 release notes</a></article>
          </main>
        </body></html>
        """
        fetcher = UnifiedSourceFetcher(settings, _FakeHttpClient({"https://solo.xin/": html}))
        profile = SourceProfile(
            source_id="solo",
            input_url="https://solo.xin",
            normalized_url="https://solo.xin/",
            tier=SourceTier.TIER1,
            active=True,
            can_originate_candidate=True,
            kind="solo_topics",
            reason="test",
        )

        observations = fetcher.fetch(profile)

        self.assertEqual(
            [item.source_url for item in observations],
            ["https://solo.xin/topic/1001", "https://solo.xin/topic/1002"],
        )

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from ai_discovery.config import Settings
from ai_discovery.models import SourceTier
from ai_discovery.source_registry import load_source_profiles


class SourceRegistryTests(unittest.TestCase):
    def test_source_profiles_use_shared_classification(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text(
                "\n".join(
                    [
                        "https://news.ycombinator.com/show",
                        "https://github.com/trending",
                        "https://solo.xin",
                    ]
                ),
                encoding="utf-8",
            )
            settings = Settings.from_env(root)
            settings.source_file = source_file
            profiles = {profile.normalized_url: profile for profile in load_source_profiles(settings)}

            self.assertEqual(profiles["https://news.ycombinator.com/show"].tier, SourceTier.TIER1)
            self.assertEqual(profiles["https://github.com/trending"].tier, SourceTier.TIER1)
            self.assertTrue(profiles["https://github.com/trending"].can_originate_candidate)
            self.assertEqual(profiles["https://github.com/trending"].kind, "github_trending")
            self.assertEqual(profiles["https://solo.xin/"].tier, SourceTier.TIER1)
            self.assertEqual(profiles["https://solo.xin/"].kind, "solo_topics")
            self.assertIn("github://repo-metadata", profiles)
            self.assertEqual(profiles["github://repo-metadata"].tier, SourceTier.TIER2)

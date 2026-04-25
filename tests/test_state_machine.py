import unittest

from ai_discovery.pipeline import _name_matches_destination


class StateMachineHelperTests(unittest.TestCase):
    def test_name_matches_destination_from_title(self) -> None:
        self.assertTrue(_name_matches_destination("Cursor Build", "Cursor Build - Home", "https://example.com"))

    def test_name_matches_destination_from_url_path(self) -> None:
        self.assertTrue(_name_matches_destination("Open Devin", "", "https://example.com/open-devin"))

    def test_name_match_fails_when_identity_conflicts(self) -> None:
        self.assertFalse(_name_matches_destination("Alpha Tool", "Different Product", "https://example.com/other"))

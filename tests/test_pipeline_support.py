import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import time
from unittest.mock import patch

from ai_discovery.digest import build_digest_chunks
from ai_discovery.config import Settings
from ai_discovery.models import DigestCard, ExtractionResult, Observation
from ai_discovery.pipeline import DiscoveryPipeline, ObservationAnalysis, SourceBatchResult


class PipelineSupportTests(unittest.TestCase):
    def test_list_sources_populates_db(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://news.ycombinator.com/show\nhttps://github.com/trending\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            pipeline = DiscoveryPipeline(settings)
            try:
                profiles = pipeline.list_sources()
                self.assertEqual(len(profiles), 3)
                self.assertTrue(any(profile.source_id == "hn_show" for profile in profiles))
                self.assertTrue(any(profile.source_id == "github_repo_metadata" for profile in profiles))
            finally:
                pipeline.close()

    def test_multi_source_corroboration_uses_support_profile(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://news.ycombinator.com/show\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            pipeline = DiscoveryPipeline(settings)
            try:
                pipeline.list_sources()
                with patch.object(
                    pipeline.fetcher,
                    "fetch_supporting",
                    return_value={"name": "demo", "full_name": "owner/demo"},
                ) as mocked_support:
                    flags, evidence = pipeline._maybe_promote_multi_source("https://github.com/owner/demo", "demo")
                    mocked_support.assert_called_once()
                    self.assertEqual(mocked_support.call_args.kwargs["primary_link"], "https://github.com/owner/demo")
                    self.assertTrue(flags["github_repo_confirmed"])
                    self.assertEqual(evidence["source_id"], "github_repo_metadata")
            finally:
                pipeline.close()

    def test_candidate_failure_persists_candidate_and_rejected_states(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://news.ycombinator.com/show\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            settings.ai_api_base_url = "https://example.com/v1"
            settings.ai_api_key = "key"
            settings.ai_model = "model"
            pipeline = DiscoveryPipeline(settings)
            observation = Observation(
                source_id="hn_show",
                external_id="1",
                observed_at="2026-04-24T00:00:00+00:00",
                title="Launch candidate",
                body_text="A new launch with a real URL",
                source_url="https://news.ycombinator.com/item?id=1",
                raw_payload={"id": 1},
            )
            extraction = ExtractionResult(
                is_project_candidate=True,
                candidate_kind="project",
                project_name="Launch Candidate",
                display_name_zh="启动候选项目",
                maturity="早期",
                category="工具",
                primary_link="https://example.com/launch-candidate",
                secondary_links=[],
                summary="这是一个用于测试状态流转的候选项目。",
                explicit_launch_cue=False,
                rationale="",
                contradiction_notes="",
            )
            try:
                pipeline.init_db()
                profile = next(profile for profile in pipeline.list_sources() if profile.active)
                from ai_discovery.models import ProjectRecord, VerificationState

                pipeline.db.upsert_project(
                    ProjectRecord(
                        project_id="existing",
                        canonical_name="Launch Candidate",
                        display_name_zh="启动候选项目",
                        primary_link="https://example.com/launch-candidate",
                        maturity="早期",
                        category="工具",
                        summary_200="existing",
                        verification_state=VerificationState.VERIFIED_SINGLE_SOURCE,
                        verification_class=VerificationState.VERIFIED_SINGLE_SOURCE.value,
                        verification_reason="existing",
                        first_seen_at="2026-04-24T00:00:00+00:00",
                        last_seen_at="2026-04-24T00:00:00+00:00",
                        evidence_flags={"evidence_score": 5},
                    )
                )
                with patch.object(
                    pipeline,
                    "_collect_source_batches",
                    return_value=[
                        SourceBatchResult(
                            profile=profile,
                            analyses=[
                                ObservationAnalysis(
                                    observation=observation,
                                    extraction=extraction,
                                    resolved_info={"resolved_url": "https://example.com/launch-candidate", "identity_title": "Launch Candidate"},
                                )
                            ],
                        )
                    ],
                ):
                    result = pipeline.run(dry_run=True, digest_date="2026-04-24")
                    self.assertEqual(result.projects_promoted, 0)
                    rows = list(
                        pipeline.db.connection.execute(
                            "SELECT state FROM state_events WHERE observation_id = ? ORDER BY created_at ASC",
                            (observation.observation_id,),
                        )
                    )
                    states = [row["state"] for row in rows]
                    self.assertIn("candidate", states)
                    self.assertIn("rejected", states)
            finally:
                pipeline.close()

    def test_recoverable_ai_timeout_rejects_item_without_failing_run(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://news.ycombinator.com/show\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            settings.ai_api_base_url = "https://example.com/v1"
            settings.ai_api_key = "key"
            settings.ai_model = "model"
            pipeline = DiscoveryPipeline(settings)
            observation = Observation(
                source_id="hn_show",
                external_id="1",
                observed_at="2026-04-24T00:00:00+00:00",
                title="Slow item",
                body_text="This item causes the model to time out.",
                source_url="https://news.ycombinator.com/item?id=1",
                raw_payload={"id": 1},
            )
            try:
                pipeline.init_db()
                profile = next(profile for profile in pipeline.list_sources() if profile.active)
                with patch.object(
                    pipeline,
                    "_collect_source_batches",
                    return_value=[
                        SourceBatchResult(
                            profile=profile,
                            analyses=[ObservationAnalysis(observation=observation, extraction=None, error="AI extraction failed: timed out")],
                        )
                    ],
                ):
                    result = pipeline.run(dry_run=True, digest_date="2026-04-24")
                    self.assertEqual(result.projects_promoted, 0)
                    run_row = pipeline.db.connection.execute(
                        "SELECT status, notes FROM runs WHERE run_id = ?",
                        (result.run_id,),
                    ).fetchone()
                    self.assertEqual(run_row["status"], "success")
                    obs_row = pipeline.db.connection.execute(
                        "SELECT state, reason FROM observations WHERE observation_id = ?",
                        (observation.observation_id,),
                    ).fetchone()
                    self.assertEqual(obs_row["state"], "rejected")
                    self.assertIn("AI extraction failed: timed out", obs_row["reason"])
            finally:
                pipeline.close()

    def test_run_succeeds_when_one_source_fails_but_another_produces_project(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://news.ycombinator.com/show\nhttps://example.com/feed\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            settings.ai_api_base_url = "https://example.com/v1"
            settings.ai_api_key = "key"
            settings.ai_model = "model"
            pipeline = DiscoveryPipeline(settings)
            try:
                pipeline.init_db()
                profiles = pipeline.list_sources()
                hn_profile = next(profile for profile in profiles if profile.source_id == "hn_show")
                generic_profile = next(profile for profile in profiles if profile.source_id.startswith("generic_"))
                observation = Observation(
                    source_id=generic_profile.source_id,
                    external_id="1",
                    observed_at="2026-04-24T00:00:00+00:00",
                    title="Useful launch",
                    body_text="A useful project",
                    source_url="https://example.com/item/1",
                    raw_payload={"id": 1},
                )
                extraction = ExtractionResult(
                    is_project_candidate=True,
                    candidate_kind="project",
                    project_name="Useful Launch",
                    display_name_zh="有用项目",
                    maturity="早期",
                    category="工具",
                    primary_link="https://example.com/useful-launch",
                    secondary_links=[],
                summary="这是一条用于测试并发聚合的项目。",
                explicit_launch_cue=True,
                rationale="",
                contradiction_notes="",
                user_relevance_score=4,
                user_relevance_rationale="开发者会直接参考的实用项目。",
            )
                with patch.object(
                    pipeline,
                    "_collect_source_batches",
                    return_value=[
                        SourceBatchResult(profile=hn_profile, analyses=[], fatal_error="timed out"),
                        SourceBatchResult(
                            profile=generic_profile,
                            analyses=[
                                ObservationAnalysis(
                                    observation=observation,
                                    extraction=extraction,
                                    resolved_info={"resolved_url": "https://example.com/useful-launch", "identity_title": "Useful Launch"},
                                )
                            ],
                        ),
                    ],
                ):
                    result = pipeline.run(dry_run=True, digest_date="2026-04-24")
                    self.assertEqual(result.projects_promoted, 1)
                    run_row = pipeline.db.connection.execute(
                        "SELECT status, notes FROM runs WHERE run_id = ?",
                        (result.run_id,),
                    ).fetchone()
                    self.assertEqual(run_row["status"], "success")
                    self.assertIn("source_failures=1", run_row["notes"])
            finally:
                pipeline.close()

    def test_collect_source_batches_processes_profiles_in_parallel(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://news.ycombinator.com/show\nhttps://example.com/a\nhttps://example.com/b\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            pipeline = DiscoveryPipeline(settings)
            active = 0
            max_active = 0
            lock = threading.Lock()

            def fake_process(profile):
                nonlocal active, max_active
                with lock:
                    active += 1
                    max_active = max(max_active, active)
                time.sleep(0.05)
                with lock:
                    active -= 1
                return SourceBatchResult(profile=profile, analyses=[])

            try:
                profiles = [profile for profile in pipeline.list_sources() if profile.active]
                with patch.object(pipeline, "_process_source_profile", side_effect=fake_process):
                    results = pipeline._collect_source_batches(profiles)
                    self.assertEqual(len(results), len(profiles))
                    self.assertGreater(max_active, 1)
            finally:
                pipeline.close()

    def test_sent_digest_chunk_is_resent_on_same_day(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://news.ycombinator.com/show\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            pipeline = DiscoveryPipeline(settings)
            try:
                pipeline.init_db()
                card = DigestCard(
                    project_id="p1",
                    project_name="Example",
                    maturity="beta",
                    category="tool",
                    url="https://example.com",
                    summary="summary",
                    verification_class="verified_single_source",
                    last_seen_at="2026-04-24T00:00:00+00:00",
                )
                chunks, trimmed = build_digest_chunks([card])
                self.assertFalse(trimmed)
                pipeline.db.insert_digest_chunk("legacy-send", "2026-04-24:1", "2026-04-24", chunks[0].chunk, False, "sent", "42")
                with patch.object(pipeline, "_send_telegram", return_value="99") as sender, patch.object(
                    pipeline, "_build_cards", return_value=[card]
                ):
                    pipeline._persist_digest("run1", "2026-04-24", chunks, False, [])
                    sender.assert_called_once_with(chunks[0].chunk.text)
                    row = pipeline.db.get_digest_chunk("2026-04-24", 1)
                    self.assertEqual(row["send_status"], "sent")
                    self.assertEqual(row["telegram_message_id"], "99")
                    self.assertEqual(pipeline.db.count_digest_chunks("2026-04-24", 1), 2)
            finally:
                pipeline.close()

    def test_live_digest_persists_latest_telegram_project_list(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://news.ycombinator.com/show\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            settings.telegram_bot_token = "bot-token"
            settings.telegram_chat_id = "12345"
            pipeline = DiscoveryPipeline(settings)
            try:
                pipeline.init_db()
                cards = [
                    DigestCard(
                        project_id="p1",
                        project_name="AI助手",
                        maturity="beta",
                        category="工具",
                        url="https://example.com/1",
                        summary="summary1",
                        verification_class="verified_single_source",
                        last_seen_at="2026-04-24T00:00:00+00:00",
                    ),
                    DigestCard(
                        project_id="p2",
                        project_name="语音信箱",
                        maturity="beta",
                        category="工具",
                        url="https://example.com/2",
                        summary="summary2",
                        verification_class="verified_single_source",
                        last_seen_at="2026-04-24T00:00:00+00:00",
                    ),
                ]
                chunks, trimmed = build_digest_chunks(cards)
                self.assertFalse(trimmed)
                with patch.object(pipeline, "_send_telegram", side_effect=["901", "902"][: len(chunks)]):
                    pipeline._persist_digest("run-live", "2026-04-24", chunks, False, [])
                project_list = pipeline.db.latest_sent_telegram_project_list("12345")
                self.assertIsNotNone(project_list)
                items = pipeline.db.list_telegram_project_list_items(project_list["list_id"])
                self.assertEqual([(row["item_index"], row["project_name"]) for row in items], [(1, "AI助手"), (2, "语音信箱")])
            finally:
                pipeline.close()

    def test_build_cards_sorted_when_pool_exceeds_twenty(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://news.ycombinator.com/show\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            pipeline = DiscoveryPipeline(settings)
            try:
                pipeline.init_db()
                base = "2026-04-24T00:00:{:02d}+00:00"
                from ai_discovery.models import ProjectRecord, VerificationState

                for idx in range(25):
                    pipeline.db.upsert_project(
                        ProjectRecord(
                            project_id=f"p{idx}",
                            canonical_name=f"Project {idx}",
                            display_name_zh=f"项目 {idx}",
                            primary_link=f"https://example.com/{idx}",
                            maturity="测试版",
                            category="工具",
                            summary_200=f"说明 {idx}",
                            verification_state=VerificationState.DIGEST_ELIGIBLE,
                            verification_class=VerificationState.VERIFIED_SINGLE_SOURCE.value if idx % 2 else VerificationState.VERIFIED_MULTI_SOURCE.value,
                            verification_reason="ok",
                            first_seen_at=base.format(idx % 60),
                            last_seen_at=base.format(idx % 60),
                            evidence_flags={"evidence_score": 10 if idx == 7 else idx % 3},
                        )
                    )
                cards = pipeline._build_cards()
                self.assertEqual(len(cards), 25)
                self.assertEqual(cards[0].project_id, "p7")
            finally:
                pipeline.close()

    def test_flexible_candidate_pool_accepts_clear_idea_without_launch_cue(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://www.indiehackers.com/ideas\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            pipeline = DiscoveryPipeline(settings)
            observation = Observation(
                source_id="generic_ideas",
                external_id="1",
                observed_at="2026-04-24T00:00:00+00:00",
                title="Concrete workflow idea",
                body_text="A concrete idea for a developer workflow tool.",
                source_url="https://www.indiehackers.com/ideas/concrete-workflow-idea",
                raw_payload={"detail_url": "https://www.indiehackers.com/ideas/concrete-workflow-idea"},
            )
            extraction = ExtractionResult(
                is_project_candidate=True,
                candidate_kind="idea",
                project_name="Workflow Forge",
                display_name_zh="Workflow Forge",
                maturity="想法",
                category="开发工具",
                primary_link="https://www.indiehackers.com/ideas/concrete-workflow-idea",
                secondary_links=[],
                summary="一个用于组织开发工作流的产品想法。",
                explicit_launch_cue=False,
                rationale="",
                contradiction_notes="",
            )
            try:
                profile = next(profile for profile in pipeline.list_sources() if profile.kind == "indiehackers_ideas")
                with patch.object(
                    pipeline,
                    "_resolve_identity",
                    return_value={"resolved_url": extraction.primary_link, "identity_title": "A concrete workflow idea - Indie Hackers"},
                ):
                    decision = pipeline._promote_observation(profile, observation, extraction)
                self.assertIsNotNone(decision.project)
                self.assertEqual(decision.project.verification_state, decision.verification_state)
                self.assertEqual(decision.project.verification_state.value, "verified_single_source")
                self.assertGreaterEqual(decision.project.evidence_flags["evidence_score"], 2)
            finally:
                pipeline.close()

    def test_discussion_candidates_prefer_external_project_link(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://www.reddit.com/r/SideProject\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            pipeline = DiscoveryPipeline(settings)
            observation = Observation(
                source_id="generic_reddit",
                external_id="1",
                observed_at="2026-04-24T00:00:00+00:00",
                title="Useful utility",
                body_text="A useful utility discussed on reddit.",
                source_url="https://product.example.com",
                raw_payload={
                    "discussion_url": "https://www.reddit.com/r/SideProject/comments/abc123/useful_utility/",
                    "external_url": "https://product.example.com",
                },
            )
            extraction = ExtractionResult(
                is_project_candidate=True,
                candidate_kind="tool",
                project_name="Useful Utility",
                display_name_zh="Useful Utility",
                maturity="早期",
                category="工具",
                primary_link="https://www.reddit.com/r/SideProject/comments/abc123/useful_utility/",
                secondary_links=[],
                summary="一个被讨论的实用工具。",
                explicit_launch_cue=True,
                rationale="",
                contradiction_notes="still early",
                user_relevance_score=4,
                user_relevance_rationale="和开发者工具偏好直接相关。",
            )
            try:
                profile = next(profile for profile in pipeline.list_sources() if profile.kind == "reddit_listing")
                with patch.object(
                    pipeline,
                    "_resolve_identity",
                    return_value={"resolved_url": "https://product.example.com", "identity_title": "Useful Utility"},
                ):
                    decision = pipeline._promote_observation(profile, observation, extraction)
                self.assertIsNotNone(decision.project)
                self.assertEqual(decision.project.primary_link, "https://product.example.com/")
                self.assertEqual(decision.project.evidence_flags["external_reference_present"], True)
            finally:
                pipeline.close()

    def test_build_cards_uses_preference_as_secondary_ranking_signal(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://news.ycombinator.com/show\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            settings.content_preference_zh = "AI 自动化 开发工具"
            pipeline = DiscoveryPipeline(settings)
            try:
                pipeline.init_db()
                from ai_discovery.models import ProjectRecord, VerificationState

                pipeline.db.upsert_project(
                    ProjectRecord(
                        project_id="pref-hit",
                        canonical_name="Agent Kit",
                        display_name_zh="Agent Kit",
                        primary_link="https://example.com/agent-kit",
                        maturity="早期",
                        category="AI 自动化",
                        summary_200="面向开发者的自动化工具",
                        verification_state=VerificationState.VERIFIED_SINGLE_SOURCE,
                        verification_class=VerificationState.VERIFIED_SINGLE_SOURCE.value,
                        verification_reason="ok",
                        first_seen_at="2026-04-24T00:00:00+00:00",
                        last_seen_at="2026-04-24T00:00:00+00:00",
                        evidence_flags={"evidence_score": 3},
                    )
                )
                pipeline.db.upsert_project(
                    ProjectRecord(
                        project_id="pref-miss",
                        canonical_name="Wedding Board",
                        display_name_zh="Wedding Board",
                        primary_link="https://example.com/wedding-board",
                        maturity="早期",
                        category="婚礼",
                        summary_200="婚礼筹备社区",
                        verification_state=VerificationState.VERIFIED_SINGLE_SOURCE,
                        verification_class=VerificationState.VERIFIED_SINGLE_SOURCE.value,
                        verification_reason="ok",
                        first_seen_at="2026-04-24T00:00:00+00:00",
                        last_seen_at="2026-04-24T00:00:00+00:00",
                        evidence_flags={"evidence_score": 3},
                    )
                )
                cards = pipeline._build_cards()
                self.assertEqual(cards[0].project_id, "pref-hit")
            finally:
                pipeline.close()

    def test_build_cards_treats_preference_as_major_ranking_signal(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://news.ycombinator.com/show\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            settings.content_preference_zh = "开发者工具 agent automation"
            pipeline = DiscoveryPipeline(settings)
            try:
                pipeline.init_db()
                from ai_discovery.models import ProjectRecord, VerificationState

                pipeline.db.upsert_project(
                    ProjectRecord(
                        project_id="pref-strong",
                        canonical_name="Terminal Agent Kit",
                        display_name_zh="Terminal Agent Kit",
                        primary_link="https://example.com/terminal-agent-kit",
                        maturity="早期",
                        category="开发者工具",
                        summary_200="面向 agent automation 的开发者工具",
                        verification_state=VerificationState.VERIFIED_SINGLE_SOURCE,
                        verification_class=VerificationState.VERIFIED_SINGLE_SOURCE.value,
                        verification_reason="ok",
                        first_seen_at="2026-04-24T00:00:00+00:00",
                        last_seen_at="2026-04-24T00:00:00+00:00",
                        evidence_flags={"evidence_score": 2, "ai_relevance_score": 3},
                    )
                )
                pipeline.db.upsert_project(
                    ProjectRecord(
                        project_id="pref-none",
                        canonical_name="Generic Ops Suite",
                        display_name_zh="Generic Ops Suite",
                        primary_link="https://example.com/generic-ops-suite",
                        maturity="早期",
                        category="企业工具",
                        summary_200="通用企业运营平台",
                        verification_state=VerificationState.VERIFIED_SINGLE_SOURCE,
                        verification_class=VerificationState.VERIFIED_SINGLE_SOURCE.value,
                        verification_reason="ok",
                        first_seen_at="2026-04-24T00:00:00+00:00",
                        last_seen_at="2026-04-24T00:00:00+00:00",
                        evidence_flags={"evidence_score": 4, "ai_relevance_score": 3},
                    )
                )
                cards = pipeline._build_cards()
                self.assertEqual(cards[0].project_id, "pref-strong")
            finally:
                pipeline.close()

    def test_build_cards_preference_is_not_decisive_against_much_stronger_signal(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://news.ycombinator.com/show\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            settings.content_preference_zh = "开发者工具 automation"
            pipeline = DiscoveryPipeline(settings)
            try:
                pipeline.init_db()
                from ai_discovery.models import ProjectRecord, VerificationState

                pipeline.db.upsert_project(
                    ProjectRecord(
                        project_id="pref-match-but-weaker",
                        canonical_name="Automation Kit",
                        display_name_zh="Automation Kit",
                        primary_link="https://example.com/automation-kit",
                        maturity="早期",
                        category="开发者工具",
                        summary_200="自动化开发工具",
                        verification_state=VerificationState.VERIFIED_SINGLE_SOURCE,
                        verification_class=VerificationState.VERIFIED_SINGLE_SOURCE.value,
                        verification_reason="ok",
                        first_seen_at="2026-04-24T00:00:00+00:00",
                        last_seen_at="2026-04-24T00:00:00+00:00",
                        evidence_flags={"evidence_score": 1, "ai_relevance_score": 2},
                    )
                )
                pipeline.db.upsert_project(
                    ProjectRecord(
                        project_id="stronger-overall",
                        canonical_name="Headless Terminal",
                        display_name_zh="Headless Terminal",
                        primary_link="https://example.com/headless-terminal",
                        maturity="早期",
                        category="开发者工具",
                        summary_200="面向开发者的 headless terminal 与 agent tooling",
                        verification_state=VerificationState.VERIFIED_SINGLE_SOURCE,
                        verification_class=VerificationState.VERIFIED_SINGLE_SOURCE.value,
                        verification_reason="ok",
                        first_seen_at="2026-04-24T00:00:00+00:00",
                        last_seen_at="2026-04-24T00:00:00+00:00",
                        evidence_flags={
                            "evidence_score": 5,
                            "ai_relevance_score": 5,
                            "developer_reference_score": 5,
                            "novelty_reference_score": 3,
                        },
                    )
                )
                cards = pipeline._build_cards()
                self.assertEqual(cards[0].project_id, "stronger-overall")
            finally:
                pipeline.close()

    def test_rejects_career_and_recruiting_tools(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://www.indiehackers.com/products\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            pipeline = DiscoveryPipeline(settings)
            observation = Observation(
                source_id="ih_products",
                external_id="1",
                observed_at="2026-04-24T00:00:00+00:00",
                title="LinkedIn content generator",
                body_text="An AI tool for LinkedIn posts and job seekers.",
                source_url="https://www.indiehackers.com/products/easygen",
                raw_payload={},
            )
            extraction = ExtractionResult(
                is_project_candidate=True,
                candidate_kind="tool",
                project_name="EasyGen",
                display_name_zh="EasyGen",
                maturity="已上线",
                category="AI 内容创作",
                primary_link="https://easygen.example.com",
                secondary_links=[],
                summary="面向职场人士生成 LinkedIn 帖子和求职内容的工具。",
                explicit_launch_cue=True,
                rationale="",
                contradiction_notes="",
            )
            try:
                profile = next(profile for profile in pipeline.list_sources() if profile.kind == "indiehackers_products")
                with patch.object(
                    pipeline,
                    "_resolve_identity",
                    return_value={"resolved_url": "https://easygen.example.com", "identity_title": "EasyGen"},
                ):
                    decision = pipeline._promote_observation(profile, observation, extraction)
                self.assertIsNone(decision.project)
                self.assertIn("career-tool", decision.rejection_reason)
            finally:
                pipeline.close()

    def test_rejects_turnkey_source_code_sales(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://solo.xin\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            pipeline = DiscoveryPipeline(settings)
            observation = Observation(
                source_id="solo",
                external_id="1",
                observed_at="2026-04-24T00:00:00+00:00",
                title="圈子论坛源码",
                body_text="无需再找人开发，直接上线的社区论坛建站源码。",
                source_url="https://solo.xin/topic/123",
                raw_payload={},
            )
            extraction = ExtractionResult(
                is_project_candidate=True,
                candidate_kind="project",
                project_name="Forum Kit",
                display_name_zh="Forum Kit",
                maturity="已发布",
                category="社区源码",
                primary_link="https://example.com/forum-kit",
                secondary_links=[],
                summary="一套圈子论坛小程序源码，主打无需找人开发即可直接上线。",
                explicit_launch_cue=True,
                rationale="",
                contradiction_notes="",
            )
            try:
                profile = next(profile for profile in pipeline.list_sources() if profile.kind == "solo_topics")
                with patch.object(
                    pipeline,
                    "_resolve_identity",
                    return_value={"resolved_url": "https://example.com/forum-kit", "identity_title": "Forum Kit"},
                ):
                    decision = pipeline._promote_observation(profile, observation, extraction)
                self.assertIsNone(decision.project)
                self.assertIn("source-code sale", decision.rejection_reason)
            finally:
                pipeline.close()

    def test_accepts_novel_app_for_idea_reference(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://www.reddit.com/r/SideProject\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            pipeline = DiscoveryPipeline(settings)
            observation = Observation(
                source_id="reddit_sideproject",
                external_id="1",
                observed_at="2026-04-24T00:00:00+00:00",
                title="A playful macOS slap app",
                body_text="A playful interactive app with sound effects and screen effects.",
                source_url="https://example.com/slapmac",
                raw_payload={},
            )
            extraction = ExtractionResult(
                is_project_candidate=True,
                candidate_kind="project",
                project_name="SlapMac",
                display_name_zh="SlapMac",
                maturity="已发布",
                category="macOS 应用",
                primary_link="https://example.com/slapmac",
                secondary_links=[],
                summary="通过敲击笔记本触发音效与屏幕特效的有趣交互应用。",
                explicit_launch_cue=True,
                rationale="",
                contradiction_notes="",
                user_relevance_score=4,
                user_relevance_rationale="虽然不是日常刚需，但交互形式有明显 idea 参考价值。",
            )
            try:
                profile = next(profile for profile in pipeline.list_sources() if profile.kind == "reddit_listing")
                with patch.object(
                    pipeline,
                    "_resolve_identity",
                    return_value={"resolved_url": "https://example.com/slapmac", "identity_title": "SlapMac"},
                ):
                    decision = pipeline._promote_observation(profile, observation, extraction)
                self.assertIsNotNone(decision.project)
                self.assertGreaterEqual(decision.project.evidence_flags["novelty_reference_score"], 2)
                self.assertGreater(decision.project.evidence_flags["reference_value_score"], 0)
            finally:
                pipeline.close()

    def test_commercialized_indiehackers_project_is_not_rejected_for_mrr_alone(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://www.indiehackers.com/products\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            pipeline = DiscoveryPipeline(settings)
            observation = Observation(
                source_id="ih_products",
                external_id="1",
                observed_at="2026-04-24T00:00:00+00:00",
                title="PDF study pack CLI",
                body_text="Open-source CLI for turning PDFs into structured audio study packs. $225+ MRR and growing.",
                source_url="https://www.indiehackers.com/products/pdf-pack-cli",
                raw_payload={"detail_url": "https://www.indiehackers.com/products/pdf-pack-cli"},
            )
            extraction = ExtractionResult(
                is_project_candidate=True,
                candidate_kind="tool",
                project_name="PDF Pack CLI",
                display_name_zh="PDF Pack CLI",
                maturity="已商业化",
                category="开发者工具",
                primary_link="https://github.com/acme/pdf-pack-cli",
                secondary_links=[],
                summary="开源 CLI，把 PDF 转成结构化音频学习包，支持本地模型与自动化工作流。",
                explicit_launch_cue=True,
                rationale="",
                contradiction_notes="",
                user_relevance_score=3,
                user_relevance_rationale="虽然有营收，但仍然是具备实现参考价值的开发者工具。",
            )
            try:
                profile = next(profile for profile in pipeline.list_sources() if profile.kind == "indiehackers_products")
                with patch.object(
                    pipeline,
                    "_resolve_identity",
                    return_value={"resolved_url": "https://github.com/acme/pdf-pack-cli", "identity_title": "PDF Pack CLI"},
                ):
                    decision = pipeline._promote_observation(profile, observation, extraction)
                self.assertIsNotNone(decision.project)
                self.assertLess(decision.project.evidence_flags["generic_penalty"], 3)
            finally:
                pipeline.close()

    def test_rejects_foreign_market_specific_business_tools_without_transfer_value(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_file = root / "sub_sites.md"
            source_file.write_text("https://www.indiehackers.com/products\n", encoding="utf-8")
            settings = Settings.from_env(root)
            settings.source_file = source_file
            settings.db_path = root / "discovery.db"
            pipeline = DiscoveryPipeline(settings)
            observation = Observation(
                source_id="ih_products",
                external_id="1",
                observed_at="2026-04-24T00:00:00+00:00",
                title="Stripe fee pass-through",
                body_text="Software for US service businesses using Stripe invoices and payment links.",
                source_url="https://example.com/stripe-fee-tool",
                raw_payload={},
            )
            extraction = ExtractionResult(
                is_project_candidate=True,
                candidate_kind="project",
                project_name="Fee Relay",
                display_name_zh="Fee Relay",
                maturity="已上线",
                category="金融科技",
                primary_link="https://example.com/stripe-fee-tool",
                secondary_links=[],
                summary="面向美国服务型企业的 Stripe 手续费转嫁工具，用于支付链接和发票场景。",
                explicit_launch_cue=True,
                rationale="",
                contradiction_notes="",
                user_relevance_score=1,
                user_relevance_rationale="高度依赖海外商业环境，与当前兴趣相关性弱。",
            )
            try:
                profile = next(profile for profile in pipeline.list_sources() if profile.kind == "indiehackers_products")
                with patch.object(
                    pipeline,
                    "_resolve_identity",
                    return_value={"resolved_url": "https://example.com/stripe-fee-tool", "identity_title": "Fee Relay"},
                ):
                    decision = pipeline._promote_observation(profile, observation, extraction)
                self.assertIsNone(decision.project)
                self.assertTrue(
                    "weakly related" in decision.rejection_reason
                    or "foreign-market-specific" in decision.rejection_reason
                )
            finally:
                pipeline.close()

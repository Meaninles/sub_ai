from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import hashlib
import json
import socket
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.parse import urlparse
from urllib.error import URLError

from .ai_client import AIClient
from .config import Settings
from .db import DiscoveryDB
from .digest import build_digest_chunks
from .http import HttpClient
from .models import DigestCard, ExtractionResult, Observation, ProjectRecord, SourceProfile, VerificationState, utc_now
from .source_registry import load_source_profiles
from .sources import UnifiedSourceFetcher


@dataclass(slots=True)
class RunResult:
    run_id: str
    digest_date: str
    source_profiles: list[SourceProfile]
    observations_seen: int
    projects_promoted: int
    chunks_created: int
    trimmed_items: int
    dry_run: bool
    empty_notice_sent: bool


@dataclass(slots=True)
class PromotionDecision:
    project: ProjectRecord | None
    was_candidate: bool
    rejection_reason: str
    evidence_flags: dict[str, Any]
    canonical_link: str
    verification_state: VerificationState
    verification_reason: str


@dataclass(slots=True)
class ObservationAnalysis:
    observation: Observation
    extraction: ExtractionResult | None
    error: str = ""
    resolved_info: dict[str, str] | None = None
    corroboration_flags: dict[str, Any] | None = None
    supporting_evidence: dict[str, Any] | None = None


@dataclass(slots=True)
class CandidateFitAssessment:
    accepted: bool
    rejection_reason: str
    score_flags: dict[str, Any]


@dataclass(slots=True)
class SourceBatchResult:
    profile: SourceProfile
    analyses: list[ObservationAnalysis]
    fatal_error: str = ""


class TaskCancelledError(RuntimeError):
    pass


class DiscoveryPipeline:
    def __init__(
        self,
        settings: Settings,
        progress_hook: callable | None = None,
        cancel_check: callable | None = None,
    ) -> None:
        self.settings = settings
        self.http_client = HttpClient(timeout_seconds=settings.http_timeout_seconds, site_sessions=settings.site_sessions)
        self.db = DiscoveryDB(settings.db_path)
        self.ai_client = AIClient(settings, self.http_client)
        self.fetcher = UnifiedSourceFetcher(settings, self.http_client)
        self.profile_index: dict[str, SourceProfile] = {}
        self.progress_hook = progress_hook
        self.cancel_check = cancel_check

    def close(self) -> None:
        self.db.close()

    def init_db(self) -> None:
        self.db.init_db()

    def list_sources(self) -> list[SourceProfile]:
        profiles = load_source_profiles(self.settings)
        self.profile_index = {profile.source_id: profile for profile in profiles}
        self.db.init_db()
        for profile in profiles:
            self.db.upsert_source(profile)
        return profiles

    def run(self, *, dry_run: bool, digest_date: str | None = None) -> RunResult:
        self.db.init_db()
        self._ensure_not_cancelled()
        profiles = self.list_sources()
        self._ensure_not_cancelled()
        self.ai_client.ensure_ready()
        run_id = hashlib.sha256(f"{utc_now()}:{dry_run}".encode("utf-8")).hexdigest()[:16]
        resolved_digest_date = digest_date or date.today().isoformat()
        self.db.start_run(run_id, dry_run)
        self._progress(f"开始运行：run_id={run_id} dry_run={dry_run}")
        observations_seen = 0
        projects_promoted = 0
        try:
            active_profiles = [profile for profile in profiles if profile.active]
            source_results = self._collect_source_batches(active_profiles)
            completed_sources = 0
            failed_sources = 0
            for source_result in source_results:
                self._ensure_not_cancelled()
                profile = source_result.profile
                source_passed = 0
                source_rejected = 0
                if source_result.fatal_error:
                    failed_sources += 1
                    continue
                completed_sources += 1
                for analysis in source_result.analyses:
                    self._ensure_not_cancelled()
                    observation = analysis.observation
                    observations_seen += 1
                    self.db.insert_observation(run_id, observation, VerificationState.OBSERVED.value)
                    self._record_state(run_id, profile.source_id, observation.observation_id, "", VerificationState.OBSERVED, "Observation stored.", {"source_url": observation.source_url})
                    if analysis.error:
                        source_rejected += 1
                        reason = analysis.error
                        self._progress(f"TASK|{profile.source_id}|item_rejected|{observation.title} | {reason}")
                        self._progress(f"内容未通过：{observation.title} | 原因：{reason}")
                        self.db.insert_observation(run_id, observation, VerificationState.REJECTED.value, reason)
                        self._record_state(
                            run_id,
                            profile.source_id,
                            observation.observation_id,
                            "",
                            VerificationState.REJECTED,
                            reason,
                            {"stage": "ai_extract", "source_url": observation.source_url},
                        )
                        continue
                    extraction = analysis.extraction
                    if extraction is None:
                        source_rejected += 1
                        reason = "AI extraction returned no result."
                        self._progress(f"TASK|{profile.source_id}|item_rejected|{observation.title} | {reason}")
                        self._progress(f"内容未通过：{observation.title} | 原因：{reason}")
                        self.db.insert_observation(run_id, observation, VerificationState.REJECTED.value, reason)
                        self._record_state(
                            run_id,
                            profile.source_id,
                            observation.observation_id,
                            "",
                            VerificationState.REJECTED,
                            reason,
                            {"stage": "ai_extract", "source_url": observation.source_url},
                        )
                        continue
                    self.db.update_observation_state(observation.observation_id, VerificationState.EXTRACTED.value)
                    self._record_state(
                        run_id,
                        profile.source_id,
                        observation.observation_id,
                        "",
                        VerificationState.EXTRACTED,
                        "AI extraction produced a schema-valid payload.",
                        {"candidate_kind": extraction.candidate_kind, "primary_link": extraction.primary_link},
                    )
                    existing = False
                    if extraction.primary_link:
                        canonical_link = self.http_client.canonicalize_url(extraction.primary_link)
                        existing = self.db.existing_project_by_link(canonical_link) is not None
                    decision = self._promote_observation(
                        profile,
                        observation,
                        extraction,
                        existing_duplicate=existing,
                        resolved_info=analysis.resolved_info,
                        multi_source_flags=analysis.corroboration_flags,
                    )
                    if decision.was_candidate:
                        self._record_state(
                            run_id,
                            profile.source_id,
                            observation.observation_id,
                            "",
                            VerificationState.CANDIDATE,
                            "Candidate passed initial project gating.",
                            {"primary_link": decision.canonical_link, **decision.evidence_flags},
                        )
                    if not decision.project:
                        source_rejected += 1
                        self._progress(f"TASK|{profile.source_id}|item_rejected|{observation.title} | {decision.rejection_reason or '未通过规则'}")
                        self._progress(f"内容未通过：{observation.title} | 原因：{decision.rejection_reason or '未通过规则'}")
                        self.db.insert_observation(run_id, observation, VerificationState.REJECTED.value, "Failed candidate/verification gates.")
                        self._record_state(
                            run_id,
                            profile.source_id,
                            observation.observation_id,
                            "",
                            VerificationState.REJECTED,
                            decision.rejection_reason or "Failed candidate or verification gates.",
                            {
                                "candidate_kind": extraction.candidate_kind,
                                "primary_link": decision.canonical_link or extraction.primary_link,
                                **decision.evidence_flags,
                            },
                        )
                        continue
                    project = decision.project
                    projects_promoted += 1
                    source_passed += 1
                    self._progress(f"TASK|{profile.source_id}|item_passed|{project.display_name_zh or project.canonical_name} | {project.verification_state.value}")
                    self._progress(f"内容通过：{project.display_name_zh or project.canonical_name} | 状态：{project.verification_state.value}")
                    self.db.upsert_project(project)
                    self._record_state(
                        run_id,
                        profile.source_id,
                        observation.observation_id,
                        project.project_id,
                        project.verification_state,
                        project.verification_reason,
                        project.evidence_flags,
                    )
                    self.db.insert_evidence(
                        evidence_id=f"{project.project_id}:{observation.observation_id}",
                        project_id=project.project_id,
                        run_id=run_id,
                        source_id=profile.source_id,
                        observation_id=observation.observation_id,
                        evidence_kind=project.verification_class,
                        url=project.primary_link,
                        details=project.evidence_flags,
                    )
                    if analysis.supporting_evidence:
                        self.db.insert_evidence(
                            evidence_id=f"{project.project_id}:{analysis.supporting_evidence['source_id']}",
                            project_id=project.project_id,
                            run_id=run_id,
                            source_id=analysis.supporting_evidence["source_id"],
                            observation_id="",
                            evidence_kind=analysis.supporting_evidence["evidence_kind"],
                            url=analysis.supporting_evidence["url"],
                            details=analysis.supporting_evidence["details"],
                        )
                self._progress(f"TASK|{profile.source_id}|completed|抓取完成，通过 {source_passed} 条，淘汰 {source_rejected} 条")
            if active_profiles and completed_sources == 0:
                raise RuntimeError("All active sources failed before candidate analysis completed.")

            cards = self._build_cards()
            self._ensure_not_cancelled()
            chunks, trimmed = build_digest_chunks(cards)
            self._progress(f"准备摘要：cards={len(cards)} chunks={len(chunks)} trimmed={len(trimmed)}")
            digest_outcome = self._persist_digest(run_id, resolved_digest_date, chunks, dry_run, trimmed)
            notes = f"chunks={digest_outcome['chunks_created']} trimmed={len(trimmed)}"
            if digest_outcome["empty_notice_sent"]:
                notes += " empty_notice=sent"
            if failed_sources:
                notes += f" source_failures={failed_sources}"
            self.db.finish_run(run_id, "success", notes=notes)
            self._progress("运行完成")
            return RunResult(
                run_id=run_id,
                digest_date=resolved_digest_date,
                source_profiles=profiles,
                observations_seen=observations_seen,
                projects_promoted=projects_promoted,
                chunks_created=digest_outcome["chunks_created"],
                trimmed_items=len(trimmed),
                dry_run=dry_run,
                empty_notice_sent=digest_outcome["empty_notice_sent"],
            )
        except Exception as exc:
            self.db.finish_run(run_id, "failed", notes=str(exc))
            self._progress(f"运行失败：{exc}")
            raise

    def refresh_project_copy(self, *, limit: int = 15) -> int:
        self.db.init_db()
        self.ai_client.ensure_ready()
        rows = self.db.list_rewrite_candidates(limit)
        updated = 0
        for row in rows:
            rewritten = self.ai_client.rewrite_project_copy(
                canonical_name=row["canonical_name"],
                current_maturity=row["maturity"],
                current_category=row["category"],
                current_summary=row["summary_200"],
                primary_link=row["primary_link"],
            )
            display_name_zh = rewritten["display_name_zh"] or row["canonical_name"]
            maturity = rewritten["maturity"] or row["maturity"]
            category = rewritten["category"] or row["category"]
            summary = rewritten["summary"] or row["summary_200"]
            self.db.update_project_copy(
                row["project_id"],
                display_name_zh=display_name_zh,
                maturity=maturity,
                category=category,
                summary_200=summary,
            )
            updated += 1
        return updated

    def _collect_source_batches(self, profiles: list[SourceProfile]) -> list[SourceBatchResult]:
        if not profiles:
            return []
        self._ensure_not_cancelled()
        results: dict[str, SourceBatchResult] = {}
        max_workers = min(len(profiles), 8)
        executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="source-worker")
        try:
            future_map = {executor.submit(self._process_source_profile, profile): profile for profile in profiles}
            pending = set(future_map)
            while pending:
                self._ensure_not_cancelled()
                done, pending = wait(pending, timeout=0.2, return_when=FIRST_COMPLETED)
                for future in done:
                    profile = future_map[future]
                    try:
                        results[profile.source_id] = future.result()
                    except Exception as exc:
                        message = f"来源处理失败：{exc}"
                        self._progress(f"TASK|{profile.source_id}|failed|{message}")
                        self._progress(f"抓取来源失败：{profile.source_id} | 原因：{exc}")
                        results[profile.source_id] = SourceBatchResult(profile=profile, analyses=[], fatal_error=str(exc))
            return [results[profile.source_id] for profile in profiles]
        except TaskCancelledError:
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def _process_source_profile(self, profile: SourceProfile) -> SourceBatchResult:
        self._ensure_not_cancelled()
        http_client = HttpClient(timeout_seconds=self.settings.http_timeout_seconds, site_sessions=self.settings.site_sessions)
        fetcher = UnifiedSourceFetcher(self.settings, http_client)
        ai_client = AIClient(self.settings, http_client)
        analyses: list[ObservationAnalysis] = []
        self._progress(f"TASK|{profile.source_id}|start|开始抓取 {profile.input_url}")
        self._progress(f"抓取来源：{profile.source_id}")
        try:
            observations = fetcher.fetch(profile)
        except Exception as exc:
            self._progress(f"TASK|{profile.source_id}|failed|抓取失败：{exc}")
            self._progress(f"抓取来源失败：{profile.source_id} | 原因：{exc}")
            return SourceBatchResult(profile=profile, analyses=[], fatal_error=str(exc))
        self._progress(f"TASK|{profile.source_id}|fetched|获取到 {len(observations)} 条候选内容")
        self._progress(f"来源 {profile.source_id} 获取到 {len(observations)} 条候选内容")
        for observation in observations:
            self._ensure_not_cancelled()
            self._progress(f"TASK|{profile.source_id}|item_processing|{observation.title}")
            try:
                extraction = ai_client.extract(observation)
            except Exception as exc:
                if not _is_recoverable_observation_error(exc):
                    raise
                analyses.append(ObservationAnalysis(observation=observation, extraction=None, error=f"AI extraction failed: {exc}"))
                continue
            resolved_info: dict[str, str] | None = None
            corroboration_flags: dict[str, Any] | None = None
            supporting_evidence: dict[str, Any] | None = None
            if extraction.is_project_candidate and extraction.primary_link:
                canonical_link = http_client.canonicalize_url(extraction.primary_link)
                resolved_info = self._resolve_identity(canonical_link, http_client)
                corroboration_flags, supporting_evidence = self._maybe_promote_multi_source(
                    canonical_link,
                    extraction.project_name,
                    fetcher=fetcher,
                )
            analyses.append(
                ObservationAnalysis(
                    observation=observation,
                    extraction=extraction,
                    resolved_info=resolved_info,
                    corroboration_flags=corroboration_flags,
                    supporting_evidence=supporting_evidence,
                )
            )
        return SourceBatchResult(profile=profile, analyses=analyses)

    def _ensure_not_cancelled(self) -> None:
        if self.cancel_check and self.cancel_check():
            raise TaskCancelledError("任务已停止")

    def _promote_observation(
        self,
        profile: SourceProfile,
        observation,
        extraction,
        *,
        existing_duplicate: bool | None = None,
        resolved_info: dict[str, str] | None = None,
        multi_source_flags: dict[str, Any] | None = None,
    ) -> PromotionDecision:
        if not extraction.is_project_candidate:
            return PromotionDecision(None, False, "AI marked this observation as non-project.", {}, "", VerificationState.REJECTED, "AI marked this observation as non-project.")
        if extraction.candidate_kind in {"news", "update", "announcement", "mainstream_news"}:
            return PromotionDecision(None, False, "Candidate kind is filtered as news/update content.", {"candidate_kind": extraction.candidate_kind}, "", VerificationState.REJECTED, "Candidate kind is filtered as news/update content.")
        preferred_link = _preferred_primary_link(observation, extraction)
        if not preferred_link:
            return PromotionDecision(None, False, "Candidate is missing a canonical project link.", {}, "", VerificationState.REJECTED, "Candidate is missing a canonical project link.")

        canonical_link = self.http_client.canonicalize_url(preferred_link)
        duplicate_found = self.db.existing_project_by_link(canonical_link) is not None if existing_duplicate is None else existing_duplicate

        resolved_info = resolved_info or self._resolve_identity(canonical_link)
        project_specific_link = _link_looks_project_specific(canonical_link)
        external_reference = str(observation.raw_payload.get("external_url", "")).strip()
        discussion_url = str(observation.raw_payload.get("discussion_url", "")).strip()
        contradiction_present = bool(extraction.contradiction_notes.strip())
        evidence_flags = {
            "launch_cue": extraction.explicit_launch_cue,
            "resolved_url": resolved_info["resolved_url"],
            "identity_title": resolved_info["identity_title"],
            "identity_match": _name_matches_destination(extraction.project_name, resolved_info["identity_title"], resolved_info["resolved_url"]),
            "duplicate_pass": not duplicate_found,
            "contradiction_pass": not contradiction_present,
            "contradiction_present": contradiction_present,
            "source_tier": profile.tier.value,
            "project_specific_link": project_specific_link,
            "external_reference_present": bool(external_reference),
            "discussion_url_present": bool(discussion_url),
            "candidate_kind": extraction.candidate_kind,
        }
        if duplicate_found:
            return PromotionDecision(None, True, "Duplicate project link already exists in the database.", evidence_flags, canonical_link, VerificationState.REJECTED, "Duplicate project link already exists in the database.")

        fit_assessment = _assess_candidate_fit(profile, observation, extraction, canonical_link, resolved_info)
        evidence_flags.update(fit_assessment.score_flags)
        if not fit_assessment.accepted:
            return PromotionDecision(
                None,
                True,
                fit_assessment.rejection_reason,
                evidence_flags,
                canonical_link,
                VerificationState.REJECTED,
                fit_assessment.rejection_reason,
            )

        multi_source_flags = multi_source_flags or self._maybe_promote_multi_source(canonical_link, extraction.project_name)[0]
        if multi_source_flags:
            evidence_flags.update(multi_source_flags)
        evidence_flags["evidence_score"] = _evidence_score(evidence_flags)
        evidence_flags["project_self_confidence"] = _project_self_confidence_label(evidence_flags["evidence_score"])

        if not profile.can_originate_candidate:
            return PromotionDecision(None, True, "Source cannot originate project candidates.", evidence_flags, canonical_link, VerificationState.REJECTED, "Source cannot originate project candidates.")

        if evidence_flags["evidence_score"] <= 0:
            return PromotionDecision(None, True, "Candidate lacks enough concrete project evidence.", evidence_flags, canonical_link, VerificationState.REJECTED, "Candidate lacks enough concrete project evidence.")

        verification_state = VerificationState.VERIFIED_SINGLE_SOURCE
        verification_class = VerificationState.VERIFIED_SINGLE_SOURCE.value
        verification_reason = "Accepted into candidate pool via flexible single-source rules."

        if multi_source_flags and (evidence_flags["identity_match"] or evidence_flags["project_specific_link"]):
            verification_state = VerificationState.VERIFIED_MULTI_SOURCE
            verification_class = VerificationState.VERIFIED_MULTI_SOURCE.value
            verification_reason = "Accepted into candidate pool with supporting corroboration."

        project_id = hashlib.sha256(canonical_link.encode("utf-8")).hexdigest()[:20]
        now = observation.observed_at or utc_now()
        return PromotionDecision(
            project=ProjectRecord(
                project_id=project_id,
                canonical_name=extraction.project_name.strip(),
                display_name_zh=(extraction.display_name_zh.strip() or extraction.project_name.strip()),
                primary_link=canonical_link,
                maturity=extraction.maturity.strip() or "unknown",
                category=extraction.category.strip() or "unknown",
                summary_200=extraction.normalized_summary(),
                verification_state=verification_state,
                verification_class=verification_class,
                verification_reason=verification_reason,
                first_seen_at=now,
                last_seen_at=now,
                evidence_flags=evidence_flags,
                secondary_links=extraction.secondary_links,
            ),
            was_candidate=True,
            rejection_reason="",
            evidence_flags=evidence_flags,
            canonical_link=canonical_link,
            verification_state=verification_state,
            verification_reason=verification_reason,
        )

    def _resolve_identity(self, primary_link: str, http_client: HttpClient | None = None) -> dict[str, str]:
        client = http_client or self.http_client
        try:
            response = client.request("GET", primary_link, retries=1)
            title = _extract_title(response.body)
            return {"resolved_url": response.final_url, "identity_title": title}
        except Exception:
            return {"resolved_url": primary_link, "identity_title": ""}

    def _maybe_promote_multi_source(
        self,
        primary_link: str,
        project_name: str,
        *,
        fetcher: UnifiedSourceFetcher | None = None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        profile = self.profile_index.get("github_repo_metadata")
        if not profile:
            return None, None
        parsed = urlparse(primary_link)
        path_parts = [part for part in parsed.path.split("/") if part]
        owner = path_parts[0] if len(path_parts) > 0 else ""
        repo = path_parts[1] if len(path_parts) > 1 else ""
        try:
            payload = (fetcher or self.fetcher).fetch_supporting(profile, primary_link=primary_link)
        except Exception:
            return None, None
        if not payload:
            return None, None
        repo_name = str(payload.get("name", "")).strip()
        if not _name_matches_destination(project_name, repo_name, primary_link):
            return None, None
        flags = {
            "github_repo_confirmed": True,
            "github_repo_full_name": payload.get("full_name", ""),
        }
        evidence = {
            "source_id": "github_repo_metadata",
            "evidence_kind": "tier2_corroboration",
            "url": primary_link,
            "details": {"repo_name": repo_name, "full_name": payload.get("full_name", ""), "owner": owner, "repo": repo},
        }
        return flags, evidence

    def _build_cards(self) -> list[DigestCard]:
        rows = self.db.list_digest_candidates()
        scored_rows = sorted(
            rows,
            key=lambda row: (
                -_digest_rank_score(
                    _parse_evidence_flags(row["evidence_flags_json"]),
                    self.settings.content_preference_zh,
                    " ".join(
                        [
                            str(row["canonical_name"] or ""),
                            str(row["display_name_zh"] or ""),
                            str(row["category"] or ""),
                            str(row["summary_200"] or ""),
                        ]
                    ),
                ),
                (row["display_name_zh"] or row["canonical_name"]).lower(),
                -self._timestamp_value(row["last_seen_at"]),
            ),
        )
        return [
            DigestCard(
                project_id=row["project_id"],
                project_name=row["display_name_zh"] or row["canonical_name"],
                maturity=row["maturity"],
                category=row["category"],
                url=row["primary_link"],
                summary=row["summary_200"],
                verification_class=row["verification_class"],
                last_seen_at=row["last_seen_at"],
            )
            for row in scored_rows
        ]

    def _persist_digest(self, run_id: str, digest_date: str, chunks, dry_run: bool, trimmed_cards: list[DigestCard]) -> dict[str, int | bool]:
        rank_lookup = self._build_rank_lookup()
        empty_notice_sent = False
        if not chunks:
            if not dry_run:
                empty_notice_sent = self._send_empty_digest_notice(digest_date)
            return {"chunks_created": 0, "empty_notice_sent": empty_notice_sent}
        project_list_id = ""
        if not dry_run:
            sent_cards = [card for packed in chunks for card in packed.cards]
            project_list_id = self._build_project_list_id(run_id, digest_date, self.settings.telegram_chat_id)
            self.db.create_telegram_project_list(
                list_id=project_list_id,
                run_id=run_id,
                digest_date=digest_date,
                chat_id=self.settings.telegram_chat_id,
                items=[
                    {
                        "project_id": card.project_id,
                        "item_index": index,
                        "project_name": card.project_name,
                    }
                    for index, card in enumerate(sent_cards, start=1)
                ],
            )
        for packed in chunks:
            chunk = packed.chunk
            digest_id = f"{digest_date}:{chunk.chunk_index}"
            send_id = self._build_digest_send_id(run_id, digest_id, dry_run)
            if dry_run:
                send_status = "dry_run"
                telegram_message_id = None
                self.db.insert_digest_chunk(send_id, digest_id, digest_date, chunk, True, send_status, telegram_message_id)
            else:
                send_status = "sending"
                telegram_message_id = None
                self.db.insert_digest_chunk(send_id, digest_id, digest_date, chunk, False, send_status, telegram_message_id)
                telegram_message_id = self._send_telegram(chunk.text)
                send_status = "sent"
                self.db.update_digest_chunk_status(send_id, send_status, telegram_message_id)
                if project_list_id:
                    self.db.add_telegram_project_list_message(
                        list_id=project_list_id,
                        send_id=send_id,
                        telegram_message_id=telegram_message_id or "",
                        chunk_index=chunk.chunk_index,
                    )
            for card in packed.cards:
                self.db.update_project_state(card.project_id, VerificationState.DIGEST_ELIGIBLE, "Selected for digest delivery.")
                self._record_state(
                    run_id,
                    "digest_builder",
                    "",
                    card.project_id,
                    VerificationState.DIGEST_ELIGIBLE,
                    "Selected for digest chunk.",
                    {"digest_date": digest_date, "chunk_index": chunk.chunk_index},
                )
                self.db.insert_digest_item(f"{digest_id}:{card.project_id}", digest_id, card.project_id, rank_lookup.get(card.project_id, 999))
                if send_status == "sent":
                    self.db.update_project_state(card.project_id, VerificationState.SENT, "Telegram send recorded.")
                    self._record_state(
                        run_id,
                        "telegram_send",
                        "",
                        card.project_id,
                        VerificationState.SENT,
                        "Telegram send recorded.",
                        {"digest_date": digest_date, "chunk_index": chunk.chunk_index, "telegram_message_id": telegram_message_id or ""},
                    )
        for trimmed in trimmed_cards:
            digest_id = f"{digest_date}:trimmed"
            self.db.insert_digest_item(
                f"{digest_id}:{trimmed.project_id}",
                digest_id,
                trimmed.project_id,
                rank_lookup.get(trimmed.project_id, 999),
                "telegram_budget_trimmed",
            )
        if project_list_id:
            self.db.mark_telegram_project_list_sent(project_list_id)
        return {"chunks_created": len(chunks), "empty_notice_sent": empty_notice_sent}

    def _build_rank_lookup(self) -> dict[str, int]:
        return {card.project_id: idx for idx, card in enumerate(self._build_cards(), start=1)}

    def _record_state(
        self,
        run_id: str,
        source_id: str,
        observation_id: str,
        project_id: str,
        state: VerificationState,
        reason: str,
        details: dict[str, Any],
    ) -> None:
        event_id = hashlib.sha256(f"{run_id}:{source_id}:{observation_id}:{project_id}:{state.value}:{reason}".encode("utf-8")).hexdigest()[:20]
        self.db.record_state_event(event_id, run_id, source_id, observation_id, project_id, state.value, reason, details)

    def _send_telegram(self, text: str) -> str:
        if not self.settings.has_telegram_config:
            raise RuntimeError("Telegram delivery requested but TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are missing.")
        payload = {
            "chat_id": self.settings.telegram_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": self.settings.telegram_disable_preview,
        }
        response = self.http_client.post_json(
            f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/sendMessage",
            payload,
        )
        return str(response.get("result", {}).get("message_id", ""))

    def _send_empty_digest_notice(self, digest_date: str) -> bool:
        digest_id = f"{digest_date}:empty"
        text = "<b>今日项目推送</b>\n\n今天没有符合当前筛选规则的项目。"
        from .models import DigestChunk

        send_id = self._build_digest_send_id("empty-notice", digest_id, False)
        self.db.insert_digest_chunk(
            send_id,
            digest_id,
            digest_date,
            DigestChunk(chunk_index=0, text=text, item_count=0),
            False,
            "sending",
            None,
        )
        message_id = self._send_telegram(text)
        self.db.update_digest_chunk_status(send_id, "sent", message_id)
        return True

    def _progress(self, message: str) -> None:
        if self.progress_hook:
            self.progress_hook(message)

    @staticmethod
    def _build_digest_send_id(run_id: str, digest_id: str, dry_run: bool) -> str:
        marker = "dry" if dry_run else "live"
        return hashlib.sha256(f"{run_id}:{digest_id}:{marker}:{utc_now()}".encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def _build_project_list_id(run_id: str, digest_date: str, chat_id: str) -> str:
        return hashlib.sha256(f"{run_id}:{digest_date}:{chat_id}:project-list".encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def _timestamp_value(value: str) -> float:
        try:
            from datetime import datetime

            return datetime.fromisoformat(value).timestamp()
        except Exception:
            return 0.0


def _extract_title(body: str) -> str:
    lower = body.lower()
    start = lower.find("<title>")
    end = lower.find("</title>")
    if start == -1 or end == -1 or end <= start:
        return ""
    return " ".join(body[start + 7 : end].split())


def _name_matches_destination(project_name: str, identity_title: str, resolved_url: str) -> bool:
    project_tokens = {token for token in _tokenize(project_name) if len(token) > 2}
    title_tokens = {token for token in _tokenize(identity_title) if len(token) > 2}
    url_tokens = {token for token in _tokenize(urlparse(resolved_url).path) if len(token) > 2}
    if not project_tokens:
        return False
    if project_tokens & title_tokens or project_tokens & url_tokens:
        return True
    project_joined = "".join(sorted(project_tokens))
    title_joined = "".join(sorted(title_tokens))
    url_joined = "".join(sorted(url_tokens))
    if not project_joined:
        return False
    return project_joined in title_joined or project_joined in url_joined or title_joined in project_joined or url_joined in project_joined


def _tokenize(value: str) -> list[str]:
    cleaned = []
    buffer = []
    previous_lower = False
    for ch in value:
        if ch.isupper() and buffer and previous_lower:
            cleaned.append("".join(buffer).lower())
            buffer = [ch.lower()]
            previous_lower = False
            continue
        if ch.isalnum():
            buffer.append(ch.lower())
            previous_lower = ch.islower()
        else:
            if buffer:
                cleaned.append("".join(buffer))
                buffer = []
            previous_lower = False
    if buffer:
        cleaned.append("".join(buffer))
    return cleaned


def _preferred_primary_link(observation: Observation, extraction: ExtractionResult) -> str:
    discussion_url = str(observation.raw_payload.get("discussion_url", "")).strip()
    external_url = str(observation.raw_payload.get("external_url", "")).strip()
    primary_link = extraction.primary_link.strip()
    if external_url and (not primary_link or _normalize_url(primary_link) == _normalize_url(discussion_url)):
        return external_url
    if primary_link:
        return primary_link
    if external_url:
        return external_url
    return observation.source_url.strip()


def _link_looks_project_specific(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path_parts = [part for part in parsed.path.split("/") if part]
    if host in {"www.reddit.com", "reddit.com", "old.reddit.com", "news.ycombinator.com"}:
        return len(path_parts) >= 2
    if host == "solo.xin":
        return len(path_parts) >= 2
    if host == "www.indiehackers.com":
        return len(path_parts) >= 2
    if host == "github.com":
        return len(path_parts) >= 2
    if host == "gitlab.com":
        return len(path_parts) >= 2
    return bool(path_parts)


def _evidence_score(evidence_flags: dict[str, Any]) -> int:
    score = 1
    if evidence_flags.get("project_specific_link"):
        score += 1
    if evidence_flags.get("launch_cue"):
        score += 1
    if evidence_flags.get("external_reference_present"):
        score += 1
    if evidence_flags.get("github_repo_confirmed"):
        score += 1
    return score


def _project_self_confidence_label(score: int) -> str:
    if score >= 5:
        return "high"
    if score >= 3:
        return "medium"
    return "low"


def _parse_evidence_flags(raw: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _digest_rank_score(evidence_flags: dict[str, Any], preference: str, project_text: str) -> int:
    score = int(evidence_flags.get("evidence_score", 0) or 0)
    score *= 10
    score += int(evidence_flags.get("ai_relevance_score", 0) or 0) * 16
    score += int(evidence_flags.get("developer_reference_score", 0) or 0) * 14
    score += int(evidence_flags.get("novelty_reference_score", 0) or 0) * 12
    score += int(evidence_flags.get("idea_reference_score", 0) or 0) * 8
    score += int(evidence_flags.get("mainland_fit_score", 0) or 0) * 6
    score += int(evidence_flags.get("source_preference_score", 0) or 0) * 5
    score -= int(evidence_flags.get("generic_penalty", 0) or 0) * 10
    score -= int(evidence_flags.get("foreign_market_penalty", 0) or 0) * 10
    score -= int(evidence_flags.get("thin_wrapper_penalty", 0) or 0) * 12
    score -= int(evidence_flags.get("repo_collection_penalty", 0) or 0) * 6
    score += _preference_rank_score(preference, project_text) * 12
    return score


def _preference_rank_score(preference: str, project_text: str) -> int:
    preference_tokens = {token for token in _tokenize(preference) if len(token) > 1}
    project_tokens = {token for token in _tokenize(project_text) if len(token) > 1}
    if not preference_tokens or not project_tokens:
        return 0
    return len(preference_tokens & project_tokens)


def _normalize_url(value: str) -> str:
    parsed = urlparse(value or "")
    return parsed._replace(fragment="").geturl()


def _assess_candidate_fit(
    profile: SourceProfile,
    observation: Observation,
    extraction: ExtractionResult,
    canonical_link: str,
    resolved_info: dict[str, str],
) -> CandidateFitAssessment:
    text = _candidate_reference_text(observation, extraction, canonical_link, resolved_info)
    developer_score = _developer_reference_score(profile, canonical_link, text)
    novelty_score = _novelty_reference_score(profile, extraction, text)
    idea_score = _idea_reference_score(extraction)
    mainland_fit_score = _mainland_fit_score(text)
    source_preference_score = _source_preference_score(profile, canonical_link)
    repo_collection_penalty = _repo_collection_penalty(text, canonical_link)
    thin_wrapper_penalty = _thin_wrapper_penalty(text)
    generic_penalty = _generic_penalty(extraction, text)
    foreign_market_penalty = _foreign_market_penalty(text)
    ai_relevance_score = max(0, min(5, extraction.user_relevance_score))
    if ai_relevance_score <= 0:
        ai_relevance_score = _fallback_ai_relevance_score(developer_score, novelty_score, idea_score, mainland_fit_score)
    reference_value_score = (
        ai_relevance_score * 4
        + developer_score * 4
        + novelty_score * 3
        + idea_score * 2
        + mainland_fit_score * 2
        + source_preference_score * 2
        - repo_collection_penalty * 2
        - thin_wrapper_penalty * 4
        - generic_penalty * 3
        - foreign_market_penalty * 3
    )
    score_flags = {
        "ai_relevance_score": ai_relevance_score,
        "ai_relevance_rationale": extraction.user_relevance_rationale,
        "developer_reference_score": developer_score,
        "novelty_reference_score": novelty_score,
        "idea_reference_score": idea_score,
        "mainland_fit_score": mainland_fit_score,
        "source_preference_score": source_preference_score,
        "repo_collection_penalty": repo_collection_penalty,
        "thin_wrapper_penalty": thin_wrapper_penalty,
        "generic_penalty": generic_penalty,
        "foreign_market_penalty": foreign_market_penalty,
        "reference_value_score": reference_value_score,
    }

    hard_reject_reason = _hard_rejection_reason(text)
    if hard_reject_reason:
        return CandidateFitAssessment(False, hard_reject_reason, score_flags)
    if ai_relevance_score <= 1:
        return CandidateFitAssessment(False, "AI judged the candidate as too weakly related to the operator's interests.", score_flags)
    if ai_relevance_score == 2 and (developer_score + novelty_score + idea_score) < 4:
        return CandidateFitAssessment(False, "AI judged the candidate as only marginally related to the operator's interests.", score_flags)
    if thin_wrapper_penalty >= 2 and ai_relevance_score < 4:
        return CandidateFitAssessment(False, "Candidate is mostly a resale/proxy wrapper with limited reference value.", score_flags)
    if foreign_market_penalty >= 2 and mainland_fit_score <= -2 and ai_relevance_score < 4:
        return CandidateFitAssessment(False, "Candidate is too foreign-market-specific for the current preference profile.", score_flags)
    if generic_penalty >= 3 and ai_relevance_score < 4:
        return CandidateFitAssessment(False, "Candidate is a generic mainstream/business tool without enough fresh angle.", score_flags)
    if reference_value_score < 2:
        return CandidateFitAssessment(False, "Candidate reference value is too weak after preference scoring.", score_flags)
    return CandidateFitAssessment(True, "", score_flags)


def _candidate_reference_text(
    observation: Observation,
    extraction: ExtractionResult,
    canonical_link: str,
    resolved_info: dict[str, str],
) -> str:
    parts = [
        observation.title,
        observation.body_text,
        extraction.project_name,
        extraction.display_name_zh,
        extraction.maturity,
        extraction.category,
        extraction.summary,
        extraction.rationale,
        extraction.contradiction_notes,
        canonical_link,
        resolved_info.get("resolved_url", ""),
        resolved_info.get("identity_title", ""),
    ]
    return " \n".join(part for part in parts if part).lower()


def _contains_any(text: str, fragments: tuple[str, ...]) -> bool:
    return any(fragment in text for fragment in fragments)


def _developer_reference_score(profile: SourceProfile, canonical_link: str, text: str) -> int:
    score = 0
    if profile.kind == "github_trending":
        score += 3
    if canonical_link.startswith("https://github.com/") or canonical_link.startswith("https://gitlab.com/"):
        score += 2
    if _contains_any(
        text,
        (
            "developer tool",
            "developer",
            "coding",
            "code generation",
            "coding assistant",
            "programming",
            "repo",
            "repository",
            "github",
            "gitlab",
            "terminal",
            "cli",
            "tui",
            "sdk",
            "api",
            "mcp",
            "agent",
            "automation",
            "workflow",
            "infra",
            "tooling",
            "debug",
            "deploy",
            "self-host",
            "self hosted",
            "open source",
            "开发者",
            "开发工具",
            "开发效率",
            "编程",
            "代码",
            "终端",
            "命令行",
            "工作流",
            "自动化",
            "脚手架",
            "调试",
            "部署",
            "自托管",
            "开源",
            "仓库",
        ),
    ):
        score += 2
    if _contains_any(text, ("agent", "mcp", "terminal", "cli", "tui", "automation", "workflow", "终端", "自动化", "工作流")):
        score += 1
    return min(score, 5)


def _novelty_reference_score(profile: SourceProfile, extraction: ExtractionResult, text: str) -> int:
    score = 0
    if extraction.candidate_kind == "idea":
        score += 1
    if _contains_any(text, ("prototype", "experimental", "experiment", "原型", "实验", "展示中")):
        score += 1
    if _contains_any(
        text,
        (
            "novel",
            "new interaction",
            "interaction",
            "interactive",
            "creative",
            "playful",
            "3d",
            "ar",
            "gesture",
            "sound effect",
            "screen effect",
            "headless",
            "新意",
            "新颖",
            "交互",
            "有趣",
            "玩法",
            "特效",
            "创意",
            "漂亮且强交互",
        ),
    ):
        score += 2
    if profile.kind == "indiehackers_ideas":
        score += 1
    return min(score, 5)


def _idea_reference_score(extraction: ExtractionResult) -> int:
    score = 0
    if extraction.candidate_kind == "idea":
        score += 2
    if len(extraction.summary.strip()) >= 24:
        score += 1
    if _contains_any(extraction.summary.lower(), ("idea", "concept", "想法", "创意", "原型")):
        score += 1
    return min(score, 4)


def _mainland_fit_score(text: str) -> int:
    score = 0
    if _contains_any(text, ("self-host", "self hosted", "open source", "local", "private", "自托管", "开源", "本地", "私有")):
        score += 1
    if _contains_any(text, ("no third-party api key", "无需第三方搜索 api key", "无需第三方 api key")):
        score += 1
    if _contains_any(
        text,
        (
            "linkedin",
            "stripe",
            "us service businesses",
            "domestic freelancers",
            "海外客户",
            "海外号码",
            "美国服务型企业",
            "美国本地号码",
            "德国等本地号码",
            "出海企业",
        ),
    ):
        score -= 2
    return max(-3, min(score, 2))


def _source_preference_score(profile: SourceProfile, canonical_link: str) -> int:
    if profile.kind == "github_trending":
        return 3
    if profile.kind == "hn_show":
        return 2
    if profile.kind == "indiehackers_ideas":
        return 2
    if canonical_link.startswith("https://github.com/") or canonical_link.startswith("https://gitlab.com/"):
        return 2
    if profile.kind in {"indiehackers_products", "solo_topics", "reddit_listing"}:
        return 1
    return 0


def _repo_collection_penalty(text: str, canonical_link: str) -> int:
    if not (canonical_link.startswith("https://github.com/") or canonical_link.startswith("https://gitlab.com/")):
        return 0
    if _contains_any(
        text,
        ("awesome-", "awesome ", "resources", "curated list", "tutorial", "tutorials", "学习集合", "资料汇总", "roadmap", "leetcode", "100-days"),
    ):
        return 2
    return 0


def _thin_wrapper_penalty(text: str) -> int:
    if _contains_any(
        text,
        (
            "api access",
            "api gateway",
            "pay per call",
            "按次计费",
            "调用入口",
            "wrapper service",
            "proxy api",
        ),
    ) and not _contains_any(text, ("self-host", "self hosted", "open source", "自托管", "开源")):
        return 2
    return 0


def _generic_penalty(extraction: ExtractionResult, text: str) -> int:
    score = 0
    if _contains_any(
        text,
        (
            "pdf",
            "document tool",
            "文档处理",
            "office tool",
            "customer support",
            "客服",
            "contract workflow",
            "合同工作流",
            "fitness",
            "健身",
            "payment fee",
            "支付费用",
        ),
    ):
        score += 2
    if extraction.candidate_kind not in {"idea", "tool", "project"}:
        score += 1
    return min(score, 4)


def _foreign_market_penalty(text: str) -> int:
    if _contains_any(
        text,
        (
            "linkedin",
            "stripe",
            "us service businesses",
            "美国服务型企业",
            "海外客户",
            "海外号码",
            "local numbers",
            "payment links",
            "domestic freelancers",
        ),
    ):
        return 2
    if _contains_any(text, ("enterprise", "sales", "call center", "contract", "legal tech", "法律科技")):
        return 1
    return 0


def _hard_rejection_reason(text: str) -> str:
    if _contains_any(
        text,
        (
            "linkedin",
            "job",
            "jobs",
            "hiring",
            "recruit",
            "recruitment",
            "interview prep",
            "interview coaching",
            "career",
            "resume",
            "cofounder",
            "招聘",
            "求职",
            "面试",
            "简历",
            "招人",
            "猎头",
            "合伙人招募",
        ),
    ):
        return "Candidate is job, recruiting, or career-tool content."
    if _contains_any(
        text,
        (
            "agency",
            "consulting",
            "consultancy",
            "design-as-a-subscription",
            "design subscription",
            "subscription design",
            "done-for-you",
            "outsourcing",
            "design service",
            "咨询服务",
            "设计服务",
            "代运营",
            "代开发",
            "外包",
            "接单",
        ),
    ):
        return "Candidate is service, agency, or consulting content."
    if _contains_any(
        text,
        (
            "源码",
            "成品源码",
            "小程序源码",
            "论坛源码",
            "建站源码",
            "无需找人开发",
            "无需再找人开发",
            "buy source code",
            "white-label source code",
        ),
    ):
        return "Candidate is source-code sale or turnkey template content."
    return ""


def _fallback_ai_relevance_score(
    developer_score: int,
    novelty_score: int,
    idea_score: int,
    mainland_fit_score: int,
) -> int:
    score = 1
    if developer_score >= 4:
        score += 2
    elif developer_score >= 2:
        score += 1
    if novelty_score >= 3:
        score += 2
    elif novelty_score >= 1:
        score += 1
    if idea_score >= 2:
        score += 1
    if mainland_fit_score > 0:
        score += 1
    if mainland_fit_score < 0:
        score -= 1
    return max(0, min(score, 5))


def _is_recoverable_observation_error(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, socket.timeout, URLError, json.JSONDecodeError, KeyError, ValueError)):
        return True
    return "timed out" in str(exc).lower()

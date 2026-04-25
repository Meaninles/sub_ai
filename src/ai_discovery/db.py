from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .models import DigestChunk, Observation, ProjectRecord, SourceProfile, VerificationState, utc_now


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    input_url TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    tier TEXT NOT NULL,
    active INTEGER NOT NULL,
    can_originate_candidate INTEGER NOT NULL,
    kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    dry_run INTEGER NOT NULL,
    status TEXT NOT NULL,
    notes TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS observations (
    observation_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    external_id TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    title TEXT NOT NULL,
    body_text TEXT NOT NULL,
    source_url TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    raw_payload_json TEXT NOT NULL,
    state TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS state_events (
    event_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    observation_id TEXT NOT NULL DEFAULT '',
    project_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    details_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS projects (
    project_id TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    display_name_zh TEXT NOT NULL DEFAULT '',
    primary_link TEXT NOT NULL,
    secondary_links_json TEXT NOT NULL DEFAULT '[]',
    maturity TEXT NOT NULL,
    category TEXT NOT NULL,
    summary_200 TEXT NOT NULL,
    verification_state TEXT NOT NULL,
    verification_class TEXT NOT NULL,
    verification_reason TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    evidence_flags_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS evidence (
    evidence_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    evidence_kind TEXT NOT NULL,
    url TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS digests (
    send_id TEXT PRIMARY KEY,
    digest_id TEXT NOT NULL,
    digest_date TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    message_text TEXT NOT NULL,
    item_count INTEGER NOT NULL,
    dry_run INTEGER NOT NULL,
    send_status TEXT NOT NULL,
    telegram_message_id TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS digest_items (
    digest_item_id TEXT PRIMARY KEY,
    digest_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    rank_index INTEGER NOT NULL,
    trim_reason TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS telegram_project_lists (
    list_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    digest_date TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    list_status TEXT NOT NULL DEFAULT 'sending',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS telegram_project_list_items (
    list_item_id TEXT PRIMARY KEY,
    list_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    item_index INTEGER NOT NULL,
    project_name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS telegram_project_list_messages (
    list_message_id TEXT PRIMARY KEY,
    list_id TEXT NOT NULL,
    send_id TEXT NOT NULL,
    telegram_message_id TEXT NOT NULL DEFAULT '',
    chunk_index INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS telegram_inbound_messages (
    update_id INTEGER PRIMARY KEY,
    chat_id TEXT NOT NULL,
    telegram_message_id TEXT NOT NULL DEFAULT '',
    text TEXT NOT NULL DEFAULT '',
    parse_status TEXT NOT NULL DEFAULT '',
    parse_result_json TEXT NOT NULL DEFAULT '{}',
    follow_list_id TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS project_follows (
    follow_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    list_id TEXT NOT NULL,
    item_index INTEGER NOT NULL,
    project_name TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    telegram_user_id TEXT NOT NULL DEFAULT '',
    source_update_id INTEGER NOT NULL,
    source_message_id TEXT NOT NULL DEFAULT '',
    followed_at TEXT NOT NULL,
    unfollowed_at TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class DiscoveryDB:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row

    def close(self) -> None:
        self.connection.close()

    def init_db(self) -> None:
        self.connection.executescript(SCHEMA)
        columns = {row["name"] for row in self.connection.execute("PRAGMA table_info(projects)")}
        if "display_name_zh" not in columns:
            self.connection.execute("ALTER TABLE projects ADD COLUMN display_name_zh TEXT NOT NULL DEFAULT ''")
        digest_columns = {row["name"] for row in self.connection.execute("PRAGMA table_info(digests)")}
        if digest_columns and "send_id" not in digest_columns:
            self.connection.execute("ALTER TABLE digests RENAME TO digests_legacy")
            self.connection.executescript(
                """
                CREATE TABLE digests (
                    send_id TEXT PRIMARY KEY,
                    digest_id TEXT NOT NULL,
                    digest_date TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    message_text TEXT NOT NULL,
                    item_count INTEGER NOT NULL,
                    dry_run INTEGER NOT NULL,
                    send_status TEXT NOT NULL,
                    telegram_message_id TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                INSERT INTO digests (
                    send_id, digest_id, digest_date, chunk_index, message_text, item_count,
                    dry_run, send_status, telegram_message_id, created_at, updated_at
                )
                SELECT
                    digest_id || ':legacy',
                    digest_id,
                    digest_date,
                    chunk_index,
                    message_text,
                    item_count,
                    dry_run,
                    send_status,
                    telegram_message_id,
                    created_at,
                    created_at
                FROM digests_legacy;
                DROP TABLE digests_legacy;
                """
            )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_digests_lookup ON digests(digest_date, chunk_index)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_telegram_project_lists_lookup ON telegram_project_lists(chat_id, created_at)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_telegram_project_list_items_lookup ON telegram_project_list_items(list_id, item_index)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_project_follows_lookup ON project_follows(chat_id, active, followed_at)"
        )
        self.connection.commit()

    def upsert_source(self, profile: SourceProfile) -> None:
        self.connection.execute(
            """
            INSERT INTO sources (
                source_id, input_url, normalized_url, tier, active,
                can_originate_candidate, kind, reason, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                input_url=excluded.input_url,
                normalized_url=excluded.normalized_url,
                tier=excluded.tier,
                active=excluded.active,
                can_originate_candidate=excluded.can_originate_candidate,
                kind=excluded.kind,
                reason=excluded.reason,
                metadata_json=excluded.metadata_json
            """,
            (
                profile.source_id,
                profile.input_url,
                profile.normalized_url,
                profile.tier.value,
                int(profile.active),
                int(profile.can_originate_candidate),
                profile.kind,
                profile.reason,
                json.dumps(profile.metadata, ensure_ascii=False, sort_keys=True),
            ),
        )
        self.connection.commit()

    def start_run(self, run_id: str, dry_run: bool) -> None:
        self.connection.execute(
            "INSERT INTO runs (run_id, started_at, dry_run, status) VALUES (?, ?, ?, ?)",
            (run_id, utc_now(), int(dry_run), "running"),
        )
        self.connection.commit()

    def finish_run(self, run_id: str, status: str, notes: str = "") -> None:
        self.connection.execute(
            "UPDATE runs SET finished_at = ?, status = ?, notes = ? WHERE run_id = ?",
            (utc_now(), status, notes, run_id),
        )
        self.connection.commit()

    def fail_running_runs(self, notes: str) -> int:
        cursor = self.connection.execute(
            "UPDATE runs SET finished_at = ?, status = ?, notes = ? WHERE status = ?",
            (utc_now(), "failed", notes, "running"),
        )
        self.connection.commit()
        return cursor.rowcount

    def insert_observation(self, run_id: str, observation: Observation, state: str, reason: str = "") -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO observations (
                observation_id, run_id, source_id, external_id, observed_at, title,
                body_text, source_url, content_hash, raw_payload_json, state, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observation.observation_id,
                run_id,
                observation.source_id,
                observation.external_id,
                observation.observed_at,
                observation.title,
                observation.body_text,
                observation.source_url,
                observation.content_hash,
                json.dumps(observation.raw_payload, ensure_ascii=False, sort_keys=True),
                state,
                reason,
            ),
        )
        self.connection.commit()

    def update_observation_state(self, observation_id: str, state: str, reason: str = "") -> None:
        self.connection.execute(
            "UPDATE observations SET state = ?, reason = ? WHERE observation_id = ?",
            (state, reason, observation_id),
        )
        self.connection.commit()

    def record_state_event(
        self,
        event_id: str,
        run_id: str,
        source_id: str,
        observation_id: str,
        project_id: str,
        state: str,
        reason: str,
        details: dict[str, Any],
    ) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO state_events (
                event_id, run_id, source_id, observation_id, project_id, state, reason, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                run_id,
                source_id,
                observation_id,
                project_id,
                state,
                reason,
                json.dumps(details, ensure_ascii=False, sort_keys=True),
            ),
        )
        self.connection.commit()

    def existing_project_by_link(self, primary_link: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM projects WHERE primary_link = ?",
            (primary_link,),
        ).fetchone()

    def upsert_project(self, project: ProjectRecord) -> None:
        self.connection.execute(
            """
            INSERT INTO projects (
                project_id, canonical_name, display_name_zh, primary_link, secondary_links_json, maturity,
                category, summary_200, verification_state, verification_class, verification_reason,
                first_seen_at, last_seen_at, evidence_flags_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id) DO UPDATE SET
                canonical_name=excluded.canonical_name,
                display_name_zh=excluded.display_name_zh,
                primary_link=excluded.primary_link,
                secondary_links_json=excluded.secondary_links_json,
                maturity=excluded.maturity,
                category=excluded.category,
                summary_200=excluded.summary_200,
                verification_state=excluded.verification_state,
                verification_class=excluded.verification_class,
                verification_reason=excluded.verification_reason,
                last_seen_at=excluded.last_seen_at,
                evidence_flags_json=excluded.evidence_flags_json,
                updated_at=excluded.updated_at
            """,
            (
                project.project_id,
                project.canonical_name,
                project.display_name_zh,
                project.primary_link,
                json.dumps(project.secondary_links, ensure_ascii=False, sort_keys=True),
                project.maturity,
                project.category,
                project.summary_200,
                project.verification_state.value,
                project.verification_class,
                project.verification_reason,
                project.first_seen_at,
                project.last_seen_at,
                json.dumps(project.evidence_flags, ensure_ascii=False, sort_keys=True),
                utc_now(),
            ),
        )
        self.connection.commit()

    def update_project_copy(self, project_id: str, *, display_name_zh: str, maturity: str, category: str, summary_200: str) -> None:
        self.connection.execute(
            """
            UPDATE projects
            SET display_name_zh = ?, maturity = ?, category = ?, summary_200 = ?, updated_at = ?
            WHERE project_id = ?
            """,
            (display_name_zh, maturity, category, summary_200, utc_now(), project_id),
        )
        self.connection.commit()

    def update_project_state(self, project_id: str, state: VerificationState, reason: str) -> None:
        self.connection.execute(
            """
            UPDATE projects
            SET verification_state = ?, verification_reason = ?, updated_at = ?
            WHERE project_id = ?
            """,
            (state.value, reason, utc_now(), project_id),
        )
        self.connection.commit()

    def insert_evidence(
        self,
        evidence_id: str,
        project_id: str,
        run_id: str,
        source_id: str,
        observation_id: str,
        evidence_kind: str,
        url: str,
        details: dict[str, Any],
    ) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO evidence (
                evidence_id, project_id, run_id, source_id, observation_id, evidence_kind, url, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                evidence_id,
                project_id,
                run_id,
                source_id,
                observation_id,
                evidence_kind,
                url,
                json.dumps(details, ensure_ascii=False, sort_keys=True),
            ),
        )
        self.connection.commit()

    def list_digest_candidates(self) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT * FROM projects
                WHERE verification_state IN (?, ?, ?)
                ORDER BY
                    CASE verification_class
                        WHEN 'verified_multi_source' THEN 0
                        WHEN 'verified_single_source' THEN 1
                        ELSE 2
                    END,
                    last_seen_at DESC
                """,
                (
                    VerificationState.VERIFIED_MULTI_SOURCE.value,
                    VerificationState.VERIFIED_SINGLE_SOURCE.value,
                    VerificationState.DIGEST_ELIGIBLE.value,
                ),
            )
        )

    def list_rewrite_candidates(self, limit: int) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT * FROM projects
                WHERE verification_state IN (?, ?, ?, ?)
                ORDER BY last_seen_at DESC
                LIMIT ?
                """,
                (
                    VerificationState.DIGEST_ELIGIBLE.value,
                    VerificationState.VERIFIED_SINGLE_SOURCE.value,
                    VerificationState.VERIFIED_MULTI_SOURCE.value,
                    VerificationState.SENT.value,
                    limit,
                ),
            )
        )

    def get_digest_chunk(self, digest_date: str, chunk_index: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT * FROM digests
            WHERE digest_date = ? AND chunk_index = ?
            ORDER BY rowid DESC
            LIMIT 1
            """,
            (digest_date, chunk_index),
        ).fetchone()

    def insert_digest_chunk(
        self,
        send_id: str,
        digest_id: str,
        digest_date: str,
        chunk: DigestChunk,
        dry_run: bool,
        send_status: str,
        telegram_message_id: str | None = None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO digests (
                send_id, digest_id, digest_date, chunk_index, message_text, item_count,
                dry_run, send_status, telegram_message_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                send_id,
                digest_id,
                digest_date,
                chunk.chunk_index,
                chunk.text,
                chunk.item_count,
                int(dry_run),
                send_status,
                telegram_message_id,
                utc_now(),
            ),
        )
        self.connection.commit()

    def update_digest_chunk_status(self, send_id: str, send_status: str, telegram_message_id: str | None) -> None:
        self.connection.execute(
            """
            UPDATE digests
            SET send_status = ?, telegram_message_id = ?, updated_at = ?
            WHERE send_id = ?
            """,
            (send_status, telegram_message_id, utc_now(), send_id),
        )
        self.connection.commit()

    def count_digest_chunks(self, digest_date: str, chunk_index: int) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) AS count FROM digests WHERE digest_date = ? AND chunk_index = ?",
            (digest_date, chunk_index),
        ).fetchone()
        return int(row["count"]) if row else 0

    def insert_digest_item(self, digest_item_id: str, digest_id: str, project_id: str, rank_index: int, trim_reason: str = "") -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO digest_items (
                digest_item_id, digest_id, project_id, rank_index, trim_reason
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (digest_item_id, digest_id, project_id, rank_index, trim_reason),
        )
        self.connection.commit()

    def create_telegram_project_list(
        self,
        *,
        list_id: str,
        run_id: str,
        digest_date: str,
        chat_id: str,
        items: list[dict[str, Any]],
    ) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO telegram_project_lists (
                list_id, run_id, digest_date, chat_id, list_status, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (list_id, run_id, digest_date, chat_id, "sending", utc_now()),
        )
        self.connection.execute(
            "DELETE FROM telegram_project_list_items WHERE list_id = ?",
            (list_id,),
        )
        for item in items:
            self.connection.execute(
                """
                INSERT INTO telegram_project_list_items (
                    list_item_id, list_id, project_id, item_index, project_name
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    f"{list_id}:{item['item_index']}",
                    list_id,
                    item["project_id"],
                    item["item_index"],
                    item["project_name"],
                ),
            )
        self.connection.commit()

    def add_telegram_project_list_message(
        self,
        *,
        list_id: str,
        send_id: str,
        telegram_message_id: str,
        chunk_index: int,
    ) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO telegram_project_list_messages (
                list_message_id, list_id, send_id, telegram_message_id, chunk_index
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (f"{list_id}:{chunk_index}", list_id, send_id, telegram_message_id, chunk_index),
        )
        self.connection.commit()

    def mark_telegram_project_list_sent(self, list_id: str) -> None:
        self.connection.execute(
            """
            UPDATE telegram_project_lists
            SET list_status = ?, updated_at = ?
            WHERE list_id = ?
            """,
            ("sent", utc_now(), list_id),
        )
        self.connection.commit()

    def latest_sent_telegram_project_list(self, chat_id: str) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT *
            FROM telegram_project_lists
            WHERE chat_id = ? AND list_status = ?
            ORDER BY created_at DESC, rowid DESC
            LIMIT 1
            """,
            (chat_id, "sent"),
        ).fetchone()

    def list_telegram_project_list_items(self, list_id: str) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT *
                FROM telegram_project_list_items
                WHERE list_id = ?
                ORDER BY item_index ASC
                """,
                (list_id,),
            )
        )

    def get_telegram_project_list_item(self, list_id: str, item_index: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT *
            FROM telegram_project_list_items
            WHERE list_id = ? AND item_index = ?
            LIMIT 1
            """,
            (list_id, item_index),
        ).fetchone()

    def has_telegram_inbound_update(self, update_id: int) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM telegram_inbound_messages WHERE update_id = ?",
            (update_id,),
        ).fetchone()
        return row is not None

    def insert_telegram_inbound_message(
        self,
        *,
        update_id: int,
        chat_id: str,
        telegram_message_id: str,
        text: str,
        parse_status: str,
        parse_result: dict[str, Any],
        follow_list_id: str,
        notes: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO telegram_inbound_messages (
                update_id, chat_id, telegram_message_id, text, parse_status,
                parse_result_json, follow_list_id, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                update_id,
                chat_id,
                telegram_message_id,
                text,
                parse_status,
                json.dumps(parse_result, ensure_ascii=False, sort_keys=True),
                follow_list_id,
                notes,
            ),
        )
        self.connection.commit()

    def has_active_project_follow(self, chat_id: str, project_id: str) -> bool:
        row = self.connection.execute(
            """
            SELECT 1
            FROM project_follows
            WHERE chat_id = ? AND project_id = ? AND active = 1
            LIMIT 1
            """,
            (chat_id, project_id),
        ).fetchone()
        return row is not None

    def insert_project_follow(
        self,
        *,
        follow_id: str,
        project_id: str,
        list_id: str,
        item_index: int,
        project_name: str,
        chat_id: str,
        telegram_user_id: str,
        source_update_id: int,
        source_message_id: str,
        followed_at: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO project_follows (
                follow_id, project_id, list_id, item_index, project_name, chat_id,
                telegram_user_id, source_update_id, source_message_id, followed_at, active, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                follow_id,
                project_id,
                list_id,
                item_index,
                project_name,
                chat_id,
                telegram_user_id,
                source_update_id,
                source_message_id,
                followed_at,
                1,
                utc_now(),
            ),
        )
        self.connection.commit()

    def list_active_project_follows(self, limit: int = 500) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT
                    f.follow_id,
                    f.project_id,
                    f.project_name,
                    f.item_index,
                    f.chat_id,
                    f.followed_at,
                    substr(f.followed_at, 1, 10) AS followed_date,
                    l.digest_date,
                    COALESCE(p.display_name_zh, p.canonical_name, f.project_name) AS display_name_zh,
                    COALESCE(p.category, '') AS category,
                    COALESCE(p.summary_200, '') AS summary_200,
                    COALESCE(p.primary_link, '') AS primary_link
                FROM project_follows f
                LEFT JOIN telegram_project_lists l ON l.list_id = f.list_id
                LEFT JOIN projects p ON p.project_id = f.project_id
                WHERE f.active = 1
                ORDER BY f.followed_at DESC, f.rowid DESC
                LIMIT ?
                """,
                (limit,),
            )
        )

    def unfollow_project(self, follow_id: str) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE project_follows
            SET active = 0, unfollowed_at = ?, updated_at = ?
            WHERE follow_id = ? AND active = 1
            """,
            (utc_now(), utc_now(), follow_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

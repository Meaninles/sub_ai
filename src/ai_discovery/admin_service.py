from __future__ import annotations

import html
import hashlib
import json
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

from .ai_client import AIClient
from .admin_store import AdminState, AdminStore
from .config import Settings
from .db import DiscoveryDB
from .http import HttpClient
from .pipeline import DiscoveryPipeline, TaskCancelledError
from .source_registry import load_source_profiles


class AdminService:
    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.store = AdminStore(project_root)
        self._state_lock = threading.Lock()
        self._worker_lock = threading.Lock()
        self._cancel_event = threading.Event()
        self._scheduler_started = False
        self._scheduler_thread: threading.Thread | None = None
        self._telegram_follow_started = False
        self._telegram_follow_thread: threading.Thread | None = None
        self._recover_incomplete_action()

    def ensure_scheduler(self) -> None:
        if not self._scheduler_started:
            thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            thread.start()
            self._scheduler_thread = thread
            self._scheduler_started = True
        if not self._telegram_follow_started:
            thread = threading.Thread(target=self._telegram_follow_loop, daemon=True)
            thread.start()
            self._telegram_follow_thread = thread
            self._telegram_follow_started = True

    def load_state(self) -> AdminState:
        return self.store.load_state()

    def save_settings(self, updates: dict[str, str]) -> None:
        self.store.save_env_map(updates)

    def save_sources(self, raw_text: str) -> None:
        self.store.save_sources_text(raw_text)

    def update_schedule(self, *, enabled: bool, schedule_time: str) -> None:
        with self._state_lock:
            state = self.store.load_state()
            previous_time = state.schedule_time
            state.schedule_enabled = enabled
            state.schedule_time = schedule_time
            if previous_time != schedule_time:
                state.last_scheduled_slot = ""
            self.store.save_state(state)

    def set_service_enabled(self, enabled: bool) -> None:
        with self._state_lock:
            state = self.store.load_state()
            state.service_enabled = enabled
            state.last_message = "服务已启动" if enabled else "服务已停止"
            self.store.save_state(state)
        self._append_event("info", state.last_message)

    def run_now(self, *, dry_run: bool) -> bool:
        return self._start_background_action(
            action=f"manual:{'dry-run' if dry_run else 'send'}",
            runner=lambda: self._run_pipeline(dry_run=dry_run),
        )

    def refresh_project_copy(self, *, limit: int = 15) -> bool:
        return self._start_background_action(
            action="refresh-copy",
            runner=lambda: self._refresh_copy(limit=limit),
        )

    def test_telegram(self) -> bool:
        return self._start_background_action(
            action="test-telegram",
            runner=self._send_test_telegram,
        )

    def stop_active_action(self) -> bool:
        with self._worker_lock:
            state = self.store.load_state()
            if not state.active_action:
                return False
            self._cancel_event.set()
            state.last_message = f"{state.active_action} 正在停止"
            state.task_items = self._mark_pending_tasks_cancelled(state.task_items)
            self.store.save_state(state)
        self._append_event("warn", "已请求停止当前任务")
        return True

    def recent_runs(self, limit: int = 10) -> list[dict]:
        db = DiscoveryPipeline(self.store.load_settings()).db
        try:
            db.init_db()
            return [dict(row) for row in db.connection.execute("SELECT run_id, started_at, finished_at, dry_run, status, notes FROM runs ORDER BY started_at DESC LIMIT ?", (limit,))]
        finally:
            db.close()

    def recent_projects(self, limit: int = 200) -> list[dict]:
        db = DiscoveryPipeline(self.store.load_settings()).db
        try:
            db.init_db()
            return [
                dict(row)
                for row in db.connection.execute(
                    """
                    SELECT canonical_name, display_name_zh, maturity, category, summary_200,
                           verification_state, verification_reason, primary_link, last_seen_at
                    FROM projects
                    ORDER BY last_seen_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
            ]
        finally:
            db.close()

    def project_view(self, limit: int = 300) -> list[dict]:
        db = DiscoveryPipeline(self.store.load_settings()).db
        try:
            db.init_db()
            rows = db.connection.execute(
                """
                WITH primary_evidence AS (
                    SELECT e.project_id, e.run_id, e.source_id, MIN(e.created_at) AS created_at
                    FROM evidence e
                    WHERE e.source_id != 'github_repo_metadata'
                    GROUP BY e.project_id
                )
                SELECT
                    p.project_id,
                    p.canonical_name,
                    p.display_name_zh,
                    p.maturity,
                    p.category,
                    p.summary_200,
                    p.verification_state,
                    p.verification_reason,
                    p.primary_link,
                    p.last_seen_at,
                    pe.run_id AS task_run_id,
                    pe.source_id AS task_source_id,
                    substr(COALESCE(pe.created_at, p.last_seen_at), 1, 10) AS task_date
                FROM projects p
                LEFT JOIN primary_evidence pe ON pe.project_id = p.project_id
                ORDER BY task_date DESC, task_run_id DESC, p.last_seen_at DESC
                LIMIT ?
                """,
                (limit,),
            )
            return [dict(row) for row in rows]
        finally:
            db.close()

    def followed_projects(self, limit: int = 500) -> list[dict]:
        db = DiscoveryDB(self.store.load_settings().db_path)
        try:
            db.init_db()
            return [dict(row) for row in db.list_active_project_follows(limit=limit)]
        finally:
            db.close()

    def unfollow_project(self, follow_id: str) -> bool:
        db = DiscoveryDB(self.store.load_settings().db_path)
        try:
            db.init_db()
            unfollowed = db.unfollow_project(follow_id)
        finally:
            db.close()
        if unfollowed:
            self._append_event("info", f"已取消关注：{follow_id}")
        return unfollowed

    def health_report(self) -> dict[str, str]:
        state = self.store.load_state()
        scheduler_alive = bool(self._scheduler_thread and self._scheduler_thread.is_alive())
        scheduler_status = "正常" if scheduler_alive else "异常"
        follow_alive = bool(self._telegram_follow_thread and self._telegram_follow_thread.is_alive())
        follow_status = "正常" if follow_alive else "异常"
        service_status = "运行中" if state.service_enabled else "已停用"
        db_status = "正常"
        try:
            db = DiscoveryPipeline(self.store.load_settings()).db
            db.init_db()
            db.close()
        except Exception:
            db_status = "异常"
        ai_status = "已配置" if self.store.load_settings().has_ai_config else "未配置"
        telegram_status = "已配置" if self.store.load_settings().has_telegram_config else "未配置"
        if state.service_enabled and scheduler_alive and db_status == "正常":
            overall = "正常"
        elif not state.service_enabled:
            overall = "已停用"
        else:
            overall = "异常"
        return {
            "overall": overall,
            "service_status": service_status,
            "scheduler_status": scheduler_status,
            "telegram_follow_status": follow_status,
            "db_status": db_status,
            "ai_status": ai_status,
            "telegram_status": telegram_status,
            "heartbeat_at": state.scheduler_heartbeat_at,
            "telegram_follow_heartbeat_at": state.telegram_follow_heartbeat_at,
            "active_action": state.active_action,
            "last_status": state.last_status,
            "last_message": state.last_message,
            "telegram_follow_last_status": state.telegram_follow_last_status,
        }

    def status_snapshot(self) -> dict:
        state = self.store.load_state()
        return {
            "state": state.__dict__ if hasattr(state, "__dict__") else {
                "service_enabled": state.service_enabled,
                "schedule_enabled": state.schedule_enabled,
                "schedule_time": state.schedule_time,
                "last_scheduled_date": state.last_scheduled_date,
                "last_scheduled_slot": state.last_scheduled_slot,
                "active_run_id": state.active_run_id,
                "active_action": state.active_action,
                "last_started_at": state.last_started_at,
                "last_finished_at": state.last_finished_at,
                "last_status": state.last_status,
                "last_message": state.last_message,
                "scheduler_heartbeat_at": state.scheduler_heartbeat_at,
                "telegram_follow_heartbeat_at": state.telegram_follow_heartbeat_at,
                "telegram_follow_last_update_id": state.telegram_follow_last_update_id,
                "telegram_follow_last_status": state.telegram_follow_last_status,
                "recent_events": state.recent_events,
                "task_items": state.task_items,
            },
            "health": self.health_report(),
            "next_run_at": self.store.compute_next_run_at(state),
            "recent_runs": self.recent_runs(),
            "recent_projects": self.recent_projects(),
            "project_view": self.project_view(),
            "followed_projects": self.followed_projects(),
        }

    def _scheduler_loop(self) -> None:
        while True:
            time.sleep(5)
            try:
                with self._state_lock:
                    state = self.store.load_state()
                    state.scheduler_heartbeat_at = datetime.now().isoformat(timespec="seconds")
                    self.store.save_state(state)
            except FileNotFoundError:
                return
            if not (state.service_enabled and state.schedule_enabled):
                continue
            if state.active_action:
                continue
            now = datetime.now()
            today = now.date().isoformat()
            if self._should_trigger_schedule(state, now):
                started = self._start_background_action(action="scheduled-send", runner=lambda: self._run_pipeline(dry_run=False, scheduled_date=today))
                if started:
                    with self._state_lock:
                        state = self.store.load_state()
                        state.last_scheduled_date = today
                        state.last_scheduled_slot = f"{today}|{state.schedule_time}"
                    self.store.save_state(state)
                    self._append_event("info", f"定时任务触发：{today} {state.schedule_time}")

    def _telegram_follow_loop(self) -> None:
        while True:
            settings = self.store.load_settings()
            try:
                with self._state_lock:
                    state = self.store.load_state()
                    state.telegram_follow_heartbeat_at = datetime.now().isoformat(timespec="seconds")
                    self.store.save_state(state)
            except FileNotFoundError:
                return
            if not state.service_enabled:
                time.sleep(2)
                continue
            if not settings.has_telegram_config:
                self._set_telegram_follow_status("Telegram 未配置，未启动关注轮询")
                time.sleep(5)
                continue
            try:
                updates = self._poll_telegram_updates(settings)
                if updates:
                    self._set_telegram_follow_status(f"收到 {len(updates)} 条 Telegram 更新")
                else:
                    self._set_telegram_follow_status("Telegram 关注轮询正常")
            except Exception as exc:
                self._set_telegram_follow_status(f"Telegram 关注轮询失败：{exc}")
                self._append_event("warn", f"Telegram 关注轮询失败：{exc}")
                time.sleep(3)
                continue
            for update in updates:
                try:
                    self._process_telegram_follow_update(settings, update)
                except Exception as exc:
                    self._append_event("warn", f"处理 Telegram 关注消息失败：{exc}")

    def _poll_telegram_updates(self, settings: Settings) -> list[dict]:
        with self._state_lock:
            state = self.store.load_state()
            offset = int(state.telegram_follow_last_update_id or 0) + 1
        query = {
            "timeout": "20",
            "offset": str(offset),
            "allowed_updates": json.dumps(["message"], ensure_ascii=False),
        }
        from urllib.parse import urlencode

        url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/getUpdates?{urlencode(query)}"
        client = HttpClient(timeout_seconds=35, site_sessions=settings.site_sessions)
        payload = client.get_json(url, retries=1)
        results = payload.get("result", [])
        return results if isinstance(results, list) else []

    def _process_telegram_follow_update(self, settings: Settings, update: dict) -> None:
        try:
            update_id = int(update.get("update_id", 0))
        except (TypeError, ValueError):
            return
        self._append_event("info", f"Telegram 关注处理：收到 update_id={update_id}")
        db = DiscoveryDB(settings.db_path)
        try:
            db.init_db()
            if db.has_telegram_inbound_update(update_id):
                self._append_event("info", f"Telegram 关注处理：update_id={update_id} 已处理过，跳过")
                self._advance_telegram_follow_offset(update_id)
                return
            message = update.get("message") or {}
            text = str(message.get("text", "") or "").strip()
            chat_id = str((message.get("chat") or {}).get("id", "") or "").strip()
            telegram_message_id = str(message.get("message_id", "") or "")
            telegram_user_id = str((message.get("from") or {}).get("id", "") or "")
            if not text:
                self._append_event("info", f"Telegram 关注处理：update_id={update_id} 非文本或空消息，忽略")
                db.insert_telegram_inbound_message(
                    update_id=update_id,
                    chat_id=chat_id,
                    telegram_message_id=telegram_message_id,
                    text="",
                    parse_status="ignored",
                    parse_result={},
                    follow_list_id="",
                    notes="non_text_or_empty_message",
                )
                self._advance_telegram_follow_offset(update_id)
                return
            if chat_id != str(settings.telegram_chat_id).strip():
                self._append_event("info", f"Telegram 关注处理：update_id={update_id} 来自其他 chat_id={chat_id}，忽略")
                db.insert_telegram_inbound_message(
                    update_id=update_id,
                    chat_id=chat_id,
                    telegram_message_id=telegram_message_id,
                    text=text,
                    parse_status="ignored",
                    parse_result={},
                    follow_list_id="",
                    notes="chat_not_configured_delivery_chat",
                )
                self._advance_telegram_follow_offset(update_id)
                return
            if not _looks_like_follow_selection_candidate(text):
                self._append_event("info", f"Telegram 关注处理：update_id={update_id} 判定为明显非数字序列，忽略")
                db.insert_telegram_inbound_message(
                    update_id=update_id,
                    chat_id=chat_id,
                    telegram_message_id=telegram_message_id,
                    text=text,
                    parse_status="ignored",
                    parse_result={},
                    follow_list_id="",
                    notes="message_not_numeric_selection_candidate",
                )
                self._advance_telegram_follow_offset(update_id)
                return
            latest_list = db.latest_sent_telegram_project_list(chat_id)
            if not latest_list:
                self._append_event("info", f"Telegram 关注处理：update_id={update_id} 没有可用的上一份项目清单，忽略")
                db.insert_telegram_inbound_message(
                    update_id=update_id,
                    chat_id=chat_id,
                    telegram_message_id=telegram_message_id,
                    text=text,
                    parse_status="ignored",
                    parse_result={},
                    follow_list_id="",
                    notes="no_sent_project_list_available",
                )
                self._advance_telegram_follow_offset(update_id)
                return
            list_items = db.list_telegram_project_list_items(latest_list["list_id"])
            self._append_event(
                "info",
                f"Telegram 关注处理：update_id={update_id} 绑定最新项目清单 list_id={latest_list['list_id']} items={len(list_items)}",
            )
            parsed = _parse_follow_selection_hard(text=text, max_index=len(list_items))
            if parsed is None:
                if _looks_like_obvious_non_selection_text(text):
                    self._append_event("info", f"Telegram 关注处理：update_id={update_id} 判定为自然语言/手机号类消息，忽略")
                    db.insert_telegram_inbound_message(
                        update_id=update_id,
                        chat_id=chat_id,
                        telegram_message_id=telegram_message_id,
                        text=text,
                        parse_status="ignored",
                        parse_result={},
                        follow_list_id=latest_list["list_id"],
                        notes="message_not_numeric_selection_candidate",
                    )
                    self._advance_telegram_follow_offset(update_id)
                    return
                self._append_event("info", f"Telegram 关注处理：update_id={update_id} 未命中硬解析，转 AI 解析")
                ai_client = AIClient(settings, HttpClient(timeout_seconds=settings.http_timeout_seconds, site_sessions=settings.site_sessions))
                parsed = ai_client.parse_follow_selection(text=text, max_index=len(list_items))
            else:
                self._append_event(
                    "info",
                    f"Telegram 关注处理：update_id={update_id} 命中硬解析 selected={parsed['selected_indexes']}",
                )
            if not parsed["is_numeric_selection"]:
                self._append_event("info", f"Telegram 关注处理：update_id={update_id} AI/解析结果非有效数字序列，忽略")
                db.insert_telegram_inbound_message(
                    update_id=update_id,
                    chat_id=chat_id,
                    telegram_message_id=telegram_message_id,
                    text=text,
                    parse_status="ignored",
                    parse_result=parsed,
                    follow_list_id=latest_list["list_id"],
                    notes="ai_rejected_non_numeric_selection",
                )
                self._advance_telegram_follow_offset(update_id)
                return
            followed_rows: list[dict[str, str]] = []
            for item_index in parsed["selected_indexes"]:
                item = db.get_telegram_project_list_item(latest_list["list_id"], item_index)
                if not item:
                    self._append_event("info", f"Telegram 关注处理：update_id={update_id} 序号 {item_index} 不在当前清单中，跳过")
                    continue
                if db.has_active_project_follow(chat_id, item["project_id"]):
                    self._append_event("info", f"Telegram 关注处理：update_id={update_id} 序号 {item_index} 已关注，跳过")
                    continue
                follow_id = hashlib.sha256(
                    f"{chat_id}:{item['project_id']}:{latest_list['list_id']}:{update_id}".encode("utf-8")
                ).hexdigest()[:24]
                db.insert_project_follow(
                    follow_id=follow_id,
                    project_id=item["project_id"],
                    list_id=latest_list["list_id"],
                    item_index=item["item_index"],
                    project_name=item["project_name"],
                    chat_id=chat_id,
                    telegram_user_id=telegram_user_id,
                    source_update_id=update_id,
                    source_message_id=telegram_message_id,
                    followed_at=datetime.now().isoformat(timespec="seconds"),
                )
                followed_rows.append(
                    {
                        "project_name": str(item["project_name"]),
                        "item_index": str(item["item_index"]),
                    }
                )
                self._append_event(
                    "info",
                    f"Telegram 关注处理：update_id={update_id} 已关注序号 {item['item_index']} -> {item['project_name']}",
                )
            parse_status = "followed" if followed_rows else "parsed_noop"
            notes = "follow_success" if followed_rows else "all_selected_projects_already_followed_or_missing"
            db.insert_telegram_inbound_message(
                update_id=update_id,
                chat_id=chat_id,
                telegram_message_id=telegram_message_id,
                text=text,
                parse_status=parse_status,
                parse_result=parsed,
                follow_list_id=latest_list["list_id"],
                notes=notes,
            )
        finally:
            db.close()
        if followed_rows:
            self._append_event("info", f"Telegram 关注处理：update_id={update_id} 发送关注确认，共 {len(followed_rows)} 个项目")
            self._send_follow_confirmation(settings, followed_rows)
            self._append_event("success", f"已根据 Telegram 回复关注 {len(followed_rows)} 个项目")
        else:
            self._append_event("info", f"Telegram 关注处理：update_id={update_id} 未产生新的关注项目")
        self._advance_telegram_follow_offset(update_id)

    def _send_follow_confirmation(self, settings: Settings, followed_rows: list[dict[str, str]]) -> None:
        lines = ["已关注：", ""]
        for index, row in enumerate(followed_rows, start=1):
            lines.append(f"{index}. {html.escape(row['project_name'])}")
        pipeline = DiscoveryPipeline(settings)
        try:
            pipeline._send_telegram("\n".join(lines))
        finally:
            pipeline.close()

    def _advance_telegram_follow_offset(self, update_id: int) -> None:
        try:
            with self._state_lock:
                state = self.store.load_state()
                if update_id > int(state.telegram_follow_last_update_id or 0):
                    state.telegram_follow_last_update_id = update_id
                    self.store.save_state(state)
        except FileNotFoundError:
            return

    def _set_telegram_follow_status(self, message: str) -> None:
        try:
            with self._state_lock:
                state = self.store.load_state()
                state.telegram_follow_last_status = message
                self.store.save_state(state)
        except FileNotFoundError:
            return

    def _start_background_action(self, *, action: str, runner: Callable[[], str]) -> bool:
        with self._worker_lock:
            state = self.store.load_state()
            if state.active_action:
                return False
            self._cancel_event.clear()
            state.active_action = action
            state.active_run_id = ""
            state.last_started_at = datetime.now().isoformat(timespec="seconds")
            state.last_finished_at = ""
            state.last_status = "running"
            state.last_message = f"{action} 正在执行"
            if action in {"manual:send", "scheduled-send"}:
                state.task_items = self._build_task_items()
            self.store.save_state(state)
            self._append_event("info", f"{action} 已开始")
            thread = threading.Thread(target=self._run_action_thread, args=(action, runner), daemon=True)
            thread.start()
            return True

    def _run_action_thread(self, action: str, runner: Callable[[], str]) -> None:
        try:
            message = runner()
            status = "success"
        except TaskCancelledError as exc:
            message = str(exc)
            status = "failed"
        except Exception as exc:
            message = str(exc)
            status = "failed"
        with self._state_lock:
            state = self.store.load_state()
            state.active_action = ""
            state.active_run_id = ""
            state.last_finished_at = datetime.now().isoformat(timespec="seconds")
            state.last_status = status
            state.last_message = message
            if status == "failed" and message == "任务已停止":
                state.task_items = self._mark_pending_tasks_cancelled(state.task_items, stopped=True)
            self.store.save_state(state)
        self._append_event("success" if status == "success" else "error", f"{action} {status}: {message}")

    def _run_pipeline(self, *, dry_run: bool, scheduled_date: str | None = None) -> str:
        settings = self.store.load_settings()
        pipeline = DiscoveryPipeline(settings, progress_hook=self._progress, cancel_check=self._cancel_event.is_set)
        try:
            result = pipeline.run(dry_run=dry_run, digest_date=scheduled_date)
            suffix = "，已发送空结果通知" if result.empty_notice_sent else ""
            return f"运行完成：observations={result.observations_seen}, promoted={result.projects_promoted}, chunks={result.chunks_created}, dry_run={result.dry_run}{suffix}"
        finally:
            pipeline.close()

    def _refresh_copy(self, *, limit: int) -> str:
        settings = self.store.load_settings()
        pipeline = DiscoveryPipeline(settings)
        try:
            updated = pipeline.refresh_project_copy(limit=limit)
            return f"已刷新 {updated} 条项目文案"
        finally:
            pipeline.close()

    def _send_test_telegram(self) -> str:
        settings = self.store.load_settings()
        pipeline = DiscoveryPipeline(settings)
        try:
            message_id = pipeline._send_telegram("<b>管理页测试消息</b>\n\n本地管理服务可正常发送 Telegram 消息。")
            return f"测试消息发送成功，message_id={message_id}"
        finally:
            pipeline.close()

    def _append_event(self, level: str, message: str) -> None:
        try:
            with self._state_lock:
                state = self.store.load_state()
                events = list(state.recent_events)
                events.append(
                    {
                        "time": datetime.now().isoformat(timespec="seconds"),
                        "level": level,
                        "message": message,
                    }
                )
                state.recent_events = events[-30:]
                self.store.save_state(state)
        except FileNotFoundError:
            return

    def _progress(self, message: str) -> None:
        if message.startswith("TASK|"):
            _, source_id, stage, payload = message.split("|", 3)
            self._update_task(source_id, stage, payload)
        self._append_event("info", message)

    @staticmethod
    def _should_trigger_schedule(state: AdminState, now: datetime) -> bool:
        if not (state.service_enabled and state.schedule_enabled and state.schedule_time):
            return False
        schedule_hour, schedule_minute = [int(part) for part in state.schedule_time.split(":", 1)]
        scheduled_at = now.replace(hour=schedule_hour, minute=schedule_minute, second=0, microsecond=0)
        today = now.date().isoformat()
        slot = f"{today}|{state.schedule_time}"
        if state.last_scheduled_slot == slot:
            return False
        return now >= scheduled_at

    def _build_task_items(self) -> list[dict]:
        profiles = [profile for profile in load_source_profiles(self.store.load_settings()) if profile.active]
        tasks = []
        for index, profile in enumerate(profiles, start=1):
            tasks.append(
                {
                    "task_id": f"task-{index}",
                    "source_id": profile.source_id,
                    "title": profile.input_url,
                    "status": "waiting",
                    "summary": "等待开始",
                    "events": [],
                    "order": index,
                }
            )
        return tasks

    def _update_task(self, source_id: str, stage: str, payload: str) -> None:
        with self._state_lock:
            state = self.store.load_state()
            tasks = list(state.task_items)
            for task in tasks:
                if task["source_id"] != source_id:
                    continue
                event = {
                    "time": datetime.now().isoformat(timespec="seconds"),
                    "stage": stage,
                    "message": payload,
                }
                task_events = list(task.get("events", []))
                task_events.append(event)
                task["events"] = task_events[-20:]
                if stage == "start":
                    task["status"] = "running"
                    task["summary"] = payload
                elif stage == "fetched":
                    task["summary"] = payload
                elif stage == "completed":
                    task["status"] = "success"
                    task["summary"] = payload
                elif stage == "failed":
                    task["status"] = "failed"
                    task["summary"] = payload
                elif stage in {"item_passed", "item_rejected"}:
                    task["summary"] = payload
                break
            state.task_items = tasks
            self.store.save_state(state)

    def _recover_incomplete_action(self) -> None:
        with self._state_lock:
            state = self.store.load_state()
            had_active_action = bool(state.active_action)
            if had_active_action:
                stale_action = state.active_action
                state.active_action = ""
                state.active_run_id = ""
                state.last_finished_at = datetime.now().isoformat(timespec="seconds")
                state.last_status = "failed"
                state.last_message = f"{stale_action} 因服务重启已自动标记为失败"
                state.task_items = self._mark_pending_tasks_cancelled(state.task_items, stopped=True)
                self.store.save_state(state)
        db = DiscoveryPipeline(self.store.load_settings()).db
        try:
            db.init_db()
            failed_runs = db.fail_running_runs("任务因服务重启中断并自动标记为失败。")
        finally:
            db.close()
        if had_active_action or failed_runs:
            self._append_event("warn", "检测到上次运行中的任务已在本次启动时自动失败。")

    @staticmethod
    def _mark_pending_tasks_cancelled(tasks: list[dict], *, stopped: bool = False) -> list[dict]:
        updated: list[dict] = []
        summary = "任务已停止" if stopped else "停止请求已发送"
        for task in tasks or []:
            cloned = dict(task)
            if cloned.get("status") in {"waiting", "running"}:
                cloned["status"] = "failed"
                cloned["summary"] = summary
            updated.append(cloned)
        return updated


def _looks_like_follow_selection_candidate(text: str) -> bool:
    stripped = (text or "").strip()
    if not stripped:
        return False
    if _parse_follow_selection_hard(stripped, max_index=9999):
        return True
    return not _looks_like_obvious_non_selection_text(stripped)


def _parse_follow_selection_hard(text: str, max_index: int) -> dict[str, object] | None:
    stripped = (text or "").strip()
    if not stripped:
        return None
    patterns = (
        r"\d+",
        r"\d+(?:，\d+)+",
        r"\d+(?:,\d+)+",
        r"\d+(?:、\d+)+",
        r"\d+(?:\s+\d+)+",
        r"\d+(?:\.\d+)+",
        r"\d+(?:。\d+)+",
    )
    if not any(re.fullmatch(pattern, stripped) for pattern in patterns):
        return None
    raw_indexes = [int(token) for token in re.findall(r"\d+", stripped)]
    raw_tokens = re.findall(r"\d+", stripped)
    if any(len(token) >= 5 for token in raw_tokens):
        return None
    normalized_indexes: list[int] = []
    seen: set[int] = set()
    for value in raw_indexes:
        if value <= 0 or value > max_index or value in seen:
            continue
        seen.add(value)
        normalized_indexes.append(value)
    if not normalized_indexes:
        return {"is_numeric_selection": False, "selected_indexes": [], "rationale": "hard_parse_found_no_valid_indexes"}
    return {
        "is_numeric_selection": True,
        "selected_indexes": normalized_indexes,
        "rationale": "hard_parsed_numeric_sequence",
    }


def _looks_like_obvious_non_selection_text(text: str) -> bool:
    stripped = (text or "").strip()
    if not stripped:
        return True
    if re.search(r"[A-Za-z\u4e00-\u9fff]", stripped):
        return True
    numeric_tokens = re.findall(r"\d+", stripped)
    if not numeric_tokens:
        return True
    if len(numeric_tokens) == 1 and len(numeric_tokens[0]) >= 5:
        return True
    if re.search(r"[^\d\s,，、.。]", stripped):
        return True
    return False

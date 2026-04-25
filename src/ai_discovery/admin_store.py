from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile

from .config import Settings
from .site_sessions import load_site_sessions, save_site_sessions


ENV_KEYS = [
    "ADMIN_PORT",
    "CONTENT_PREFERENCE_ZH",
    "AI_API_BASE_URL",
    "AI_API_KEY",
    "AI_MODEL",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "DISCOVERY_DB_PATH",
    "FETCH_LIMIT_HN",
    "HTTP_TIMEOUT_SECONDS",
    "AI_TIMEOUT_SECONDS",
    "TELEGRAM_DISABLE_PREVIEW",
]


@dataclass(slots=True)
class AdminState:
    service_enabled: bool = False
    schedule_enabled: bool = False
    schedule_time: str = "09:00"
    last_scheduled_date: str = ""
    last_scheduled_slot: str = ""
    active_run_id: str = ""
    active_action: str = ""
    last_started_at: str = ""
    last_finished_at: str = ""
    last_status: str = ""
    last_message: str = ""
    scheduler_heartbeat_at: str = ""
    telegram_follow_heartbeat_at: str = ""
    telegram_follow_last_update_id: int = 0
    telegram_follow_last_status: str = ""
    recent_events: list[dict[str, str]] = field(default_factory=list)
    task_items: list[dict] = field(default_factory=list)


class AdminStore:
    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.env_path = project_root / ".env"
        self.sources_path = project_root / "sub_sites.md"
        self.state_path = project_root / ".omx" / "data" / "admin_state.json"
        self.state_path.parent.mkdir(parents=True, exist_ok=True)

    def load_settings(self) -> Settings:
        return Settings.from_env(self.project_root)

    def load_env_map(self) -> dict[str, str]:
        raw: dict[str, str] = {}
        if not self.env_path.exists():
            return raw
        for line in self.env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            raw[key.strip()] = value.strip()
        return raw

    def save_env_map(self, updates: dict[str, str]) -> None:
        current = self.load_env_map()
        current.update(updates)
        ordered = []
        for key in ENV_KEYS:
            if key in current:
                ordered.append(f"{key}={current[key]}")
        for key in sorted(current):
            if key not in ENV_KEYS:
                ordered.append(f"{key}={current[key]}")
        self.env_path.write_text("\n".join(ordered) + "\n", encoding="utf-8")

    def load_site_sessions(self) -> dict[str, dict[str, str]]:
        return load_site_sessions(self.project_root)

    def save_site_sessions(self, sessions: dict[str, dict[str, str]]) -> None:
        save_site_sessions(self.project_root, sessions)

    def load_sources_text(self) -> str:
        if not self.sources_path.exists():
            return ""
        return self.sources_path.read_text(encoding="utf-8")

    def save_sources_text(self, raw_text: str) -> None:
        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
        self.sources_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

    def load_state(self) -> AdminState:
        if not self.state_path.exists():
            return AdminState()
        raw = self.state_path.read_text(encoding="utf-8").strip()
        if not raw:
            return AdminState()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return AdminState()
        if not isinstance(data, dict):
            return AdminState()
        return AdminState(**data)

    def save_state(self, state: AdminState) -> None:
        payload = json.dumps(asdict(state), ensure_ascii=False, indent=2) + "\n"
        self._write_text_atomically(self.state_path, payload)

    @staticmethod
    def _write_text_atomically(path: Path, payload: str) -> None:
        with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
            tmp.write(payload)
            tmp.flush()
            os.fsync(tmp.fileno())
            temp_path = Path(tmp.name)
        temp_path.replace(path)

    @staticmethod
    def compute_next_run_at(state: AdminState, now: datetime | None = None) -> str:
        if not (state.service_enabled and state.schedule_enabled and state.schedule_time):
            return ""
        now = now or datetime.now()
        hour, minute = [int(part) for part in state.schedule_time.split(":", 1)]
        candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        current_hm = (now.hour, now.minute)
        target_hm = (hour, minute)
        if target_hm < current_hm:
            candidate = candidate + timedelta(days=1)
        return candidate.isoformat(timespec="minutes")

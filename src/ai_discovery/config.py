from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .site_sessions import load_site_sessions


def _load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class Settings:
    project_root: Path
    source_file: Path
    db_path: Path
    admin_port: int
    content_preference_zh: str
    ai_api_base_url: str
    ai_api_key: str
    ai_model: str
    telegram_bot_token: str
    telegram_chat_id: str
    fetch_limit_hn: int
    fetch_limit_generic: int
    http_timeout_seconds: int
    ai_timeout_seconds: int
    telegram_disable_preview: bool
    site_sessions: dict[str, dict[str, str]]

    @property
    def has_ai_config(self) -> bool:
        return bool(self.ai_api_base_url and self.ai_api_key and self.ai_model)

    @property
    def has_telegram_config(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)

    @classmethod
    def from_env(cls, project_root: Path | None = None) -> "Settings":
        root = (project_root or Path.cwd()).resolve()
        dotenv = _load_dotenv(root / ".env")

        def read(name: str, default: str = "") -> str:
            return os.getenv(name, dotenv.get(name, default))

        db_path = Path(read("DISCOVERY_DB_PATH", str(root / ".omx" / "data" / "discovery.db")))
        return cls(
            project_root=root,
            source_file=root / "sub_sites.md",
            db_path=db_path,
            admin_port=int(read("ADMIN_PORT", "8765")),
            content_preference_zh=read("CONTENT_PREFERENCE_ZH", ""),
            ai_api_base_url=read("AI_API_BASE_URL", "").rstrip("/"),
            ai_api_key=read("AI_API_KEY", ""),
            ai_model=read("AI_MODEL", ""),
            telegram_bot_token=read("TELEGRAM_BOT_TOKEN", ""),
            telegram_chat_id=read("TELEGRAM_CHAT_ID", ""),
            fetch_limit_hn=int(read("FETCH_LIMIT_HN", "25")),
            fetch_limit_generic=int(read("FETCH_LIMIT_GENERIC", "40")),
            http_timeout_seconds=int(read("HTTP_TIMEOUT_SECONDS", "60")),
            ai_timeout_seconds=int(read("AI_TIMEOUT_SECONDS", "300")),
            telegram_disable_preview=(read("TELEGRAM_DISABLE_PREVIEW", "1").strip().lower() in {"1", "true", "yes", "on"}),
            site_sessions=load_site_sessions(root),
        )

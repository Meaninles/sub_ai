from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from .admin_web import run_admin_server
from .config import Settings
from .pipeline import DiscoveryPipeline


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ai_discovery")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db")
    subparsers.add_parser("list-sources")
    subparsers.add_parser("check-config")
    admin_parser = subparsers.add_parser("serve-admin")
    admin_parser.add_argument("--host", default="127.0.0.1")
    admin_parser.add_argument("--port", type=int)
    refresh_parser = subparsers.add_parser("refresh-project-copy")
    refresh_parser.add_argument("--limit", type=int, default=15)

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--dry-run", action="store_true")
    run_parser.add_argument("--date", dest="digest_date")

    args = parser.parse_args(argv)
    settings = Settings.from_env()
    pipeline = DiscoveryPipeline(settings)
    try:
        if args.command == "init-db":
            pipeline.init_db()
            print("initialized")
            return 0
        if args.command == "list-sources":
            profiles = pipeline.list_sources()
            print(json.dumps([asdict(profile) for profile in profiles], ensure_ascii=False, indent=2))
            return 0
        if args.command == "serve-admin":
            run_admin_server(settings.project_root, args.host, args.port or settings.admin_port)
            return 0
        if args.command == "check-config":
            payload = {
                "ai_ready": settings.has_ai_config,
                "telegram_ready": settings.has_telegram_config,
                "content_preference_zh": settings.content_preference_zh,
                "missing": {
                    "ai": [
                        name
                        for name, value in (
                            ("AI_API_BASE_URL", settings.ai_api_base_url),
                            ("AI_API_KEY", settings.ai_api_key),
                            ("AI_MODEL", settings.ai_model),
                        )
                        if not value
                    ],
                    "telegram": [
                        name
                        for name, value in (
                            ("TELEGRAM_BOT_TOKEN", settings.telegram_bot_token),
                            ("TELEGRAM_CHAT_ID", settings.telegram_chat_id),
                        )
                        if not value
                    ],
                },
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0
        if args.command == "refresh-project-copy":
            updated = pipeline.refresh_project_copy(limit=args.limit)
            print(json.dumps({"updated": updated}, ensure_ascii=False, indent=2))
            return 0
        if args.command == "run":
            result = pipeline.run(dry_run=args.dry_run, digest_date=args.digest_date)
            print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
            return 0
        return 1
    except RuntimeError as exc:
        print(str(exc))
        return 2
    finally:
        pipeline.close()

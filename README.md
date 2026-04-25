# AI Project Discovery MVP

Personal-use MVP for discovering interesting AI projects from a small allowlist of sources, extracting structured project cards with a cloud AI API, storing evidence in SQLite, and sending a daily Telegram digest.

## Current MVP shape

- Python 3.12+
- SQLite persistence
- One unified acquisition pipeline for all supported sources
- Tiered source support:
  - Tier 1 direct: Hacker News Show HN
  - Tier 2 corroboration: GitHub repository metadata
  - Tier 3 deferred: GitHub Trending, RocketDevs, Reddit, Indie Hackers, solo.xin
- Telegram digest with deterministic chunking and idempotent send tracking

## Environment

Set these variables before running a real pipeline:

- `CONTENT_PREFERENCE_ZH`: optional natural-language preference in Chinese, for example `我更偏向抓取面向个人开发者、AI 自动化、能直接落地赚钱的项目`
- `AI_API_BASE_URL`: OpenAI-compatible chat completions endpoint base, for example `https://api.openai.com/v1`
- `AI_API_KEY`: cloud AI API key
- `AI_MODEL`: model name to use for extraction
- `TELEGRAM_BOT_TOKEN`: Telegram bot token
- `TELEGRAM_CHAT_ID`: Telegram chat id

Optional:

- `DISCOVERY_DB_PATH`: defaults to `.omx/data/discovery.db`
- `FETCH_LIMIT_HN`: defaults to `25`, meaning the current HN source fetch window before candidate pooling and ranking
- `HTTP_TIMEOUT_SECONDS`: defaults to `60`
- `AI_TIMEOUT_SECONDS`: defaults to `300`
- `TELEGRAM_DISABLE_PREVIEW`: defaults to `1`

## Getting credentials

### Telegram

`TELEGRAM_BOT_TOKEN`

1. Open `@BotFather` in Telegram.
2. Send `/newbot`.
3. Follow the prompts to create the bot.
4. Copy the token returned by BotFather.

`TELEGRAM_CHAT_ID`

1. Open your bot chat and press `Start`.
2. Send any message to the bot.
3. Open:

```text
https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates
```

4. Read `result[0].message.chat.id` from the JSON response.

## Commands

Initialize the database:

```bash
python3 -m ai_discovery init-db
```

Start the local admin UI:

```bash
PYTHONPATH=src python3 -m ai_discovery serve-admin
```

Run a dry-run:

```bash
PYTHONPATH=src python3 -m ai_discovery run --dry-run
```

Run with real Telegram delivery:

```bash
PYTHONPATH=src python3 -m ai_discovery run
```

Show active source classification:

```bash
PYTHONPATH=src python3 -m ai_discovery list-sources
```

Check local configuration readiness:

```bash
PYTHONPATH=src python3 -m ai_discovery check-config
```

Refresh existing project copy using your current Chinese preference:

```bash
PYTHONPATH=src python3 -m ai_discovery refresh-project-copy --limit 15
```

## Notes

- GitHub Trending is explicitly deferred for MVP.
- The AI provider is assumed to be OpenAI-compatible. The implementation validates extracted JSON against a deterministic local schema before promotion.
- `CONTENT_PREFERENCE_ZH` is a soft preference signal that biases extraction and Chinese rewrite output toward the kinds of projects you care about most.
- The final digest is selected from the whole candidate pool across supported sources and capped at 20 items maximum; it is not limited to the first few fetched candidates.
- For sites that gate content behind login, use the admin page's `站点登录会话` section to paste Cookie headers for `Reddit`, `GitHub`, or `X / Twitter`. These cookies are stored in `.omx/data/site_sessions.json` and are automatically attached to matching requests.

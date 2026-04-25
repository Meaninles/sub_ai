from __future__ import annotations

from dataclasses import asdict
import html
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .admin_service import AdminService
from .admin_store import AdminStore
from .site_sessions import SITE_SESSION_CONFIG


def make_handler(
    project_root: Path,
    *,
    service: AdminService | None = None,
    store: AdminStore | None = None,
) -> type[BaseHTTPRequestHandler]:
    service = service or AdminService(project_root)
    store = store or AdminStore(project_root)

    class AdminHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/api/status":
                self._send_json(self._build_status_payload())
                return
            if parsed.path != "/":
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            self._send_html(self._render_index())

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            form = self._read_form()
            if parsed.path == "/settings":
                updates = {
                    "ADMIN_PORT": form.get("admin_port", ["8765"])[0],
                    "CONTENT_PREFERENCE_ZH": form.get("content_preference_zh", [""])[0].strip(),
                    "AI_API_BASE_URL": form.get("ai_api_base_url", [""])[0].strip(),
                    "AI_API_KEY": form.get("ai_api_key", [""])[0].strip(),
                    "AI_MODEL": form.get("ai_model", [""])[0].strip(),
                    "TELEGRAM_BOT_TOKEN": form.get("telegram_bot_token", [""])[0].strip(),
                    "TELEGRAM_CHAT_ID": form.get("telegram_chat_id", [""])[0].strip(),
                    "FETCH_LIMIT_HN": form.get("fetch_limit_hn", ["25"])[0].strip(),
                    "HTTP_TIMEOUT_SECONDS": form.get("http_timeout_seconds", ["60"])[0].strip(),
                    "AI_TIMEOUT_SECONDS": form.get("ai_timeout_seconds", ["300"])[0].strip(),
                    "TELEGRAM_DISABLE_PREVIEW": "1" if "telegram_disable_preview" in form else "0",
                }
                service.save_settings(updates)
                self._redirect("/")
                return
            if parsed.path == "/sources":
                service.save_sources(form.get("sources_text", [""])[0])
                self._redirect("/")
                return
            if parsed.path == "/site-sessions":
                site_sessions = {}
                for key in SITE_SESSION_CONFIG:
                    site_sessions[key] = {
                        "cookie": form.get(f"{key}_cookie", [""])[0],
                        "user_agent": form.get(f"{key}_user_agent", [""])[0],
                    }
                store.save_site_sessions(site_sessions)
                self._redirect("/")
                return
            if parsed.path == "/schedule":
                service.update_schedule(
                    enabled="schedule_enabled" in form,
                    schedule_time=form.get("schedule_time", ["09:00"])[0],
                )
                self._redirect("/")
                return
            if parsed.path == "/service/start":
                service.set_service_enabled(True)
                self._redirect("/")
                return
            if parsed.path == "/service/stop":
                service.set_service_enabled(False)
                self._redirect("/")
                return
            if parsed.path == "/action/test-telegram":
                service.test_telegram()
                self._redirect("/")
                return
            if parsed.path == "/action/run-now":
                service.run_now(dry_run=False)
                self._redirect("/")
                return
            if parsed.path == "/action/stop-task":
                service.stop_active_action()
                self._redirect("/")
                return
            if parsed.path == "/follow/unfollow":
                service.unfollow_project(form.get("follow_id", [""])[0].strip())
                self._redirect("/")
                return
            self.send_error(HTTPStatus.NOT_FOUND)

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

        def _read_form(self) -> dict[str, list[str]]:
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length).decode("utf-8")
            return parse_qs(body, keep_blank_values=True)

        def _send_html(self, body: str) -> None:
            payload = body.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self._finish_response(payload)

        def _send_json(self, payload_obj: dict) -> None:
            payload = json.dumps(payload_obj, ensure_ascii=False).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self._finish_response(payload)

        def _redirect(self, location: str) -> None:
            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", location)
            self._finish_response()

        def _finish_response(self, payload: bytes | None = None) -> None:
            try:
                self.end_headers()
                if payload:
                    self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                return

        def _render_index(self) -> str:
            settings = store.load_settings()
            state = service.load_state()
            sources_text = store.load_sources_text()
            site_sessions = store.load_site_sessions()
            recent_runs = service.recent_runs()
            recent_projects = service.recent_projects()
            followed_projects = service.followed_projects()
            health = service.health_report()
            next_run_at = store.compute_next_run_at(state)
            def esc(value: str) -> str:
                return html.escape(value or "")

            def fmt_local(value: str) -> str:
                if not value:
                    return ""
                try:
                    from datetime import datetime

                    dt = datetime.fromisoformat(value)
                    if dt.tzinfo is not None:
                        dt = dt.astimezone()
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    return value

            runs_rows = "\n".join(
                f"<tr><td>{esc(fmt_local(row['started_at'] or ''))}</td><td>{esc(row['status'] or '')}</td><td>{esc(str(row['dry_run']))}</td><td>{esc(row['notes'] or '')}</td></tr>"
                for row in recent_runs
            ) or "<tr><td colspan='4'>暂无运行记录</td></tr>"
            event_rows = "\n".join(
                f"<tr><td>{esc(fmt_local(item.get('time', '')))}</td><td>{esc(item.get('level', ''))}</td><td>{esc(item.get('message', ''))}</td></tr>"
                for item in reversed(state.recent_events)
            ) or "<tr><td colspan='3'>暂无后台关键信息</td></tr>"
            task_list = state.task_items or []
            task_items_html = "\n".join(
                f"<button class='task-item' data-task-id='{esc(task['task_id'])}'><strong>{esc(str(task['order']))}. {esc(task['title'])}</strong><span>{esc(task['status'])}</span><small>{esc(task['summary'])}</small></button>"
                for task in task_list
            ) or "<div>暂无抓取任务</div>"
            session_blocks = "\n".join(
                f"""
                <div class="session-card">
                  <h3>{esc(str(config['label']))}</h3>
                  <p>{esc(str(config['description']))}</p>
                  <label>Cookie Header
                    <textarea name="{key}_cookie" placeholder="可直接粘贴 Cookie Header，或粘贴 Cookie-Editor 导出的完整 JSON">{esc(site_sessions.get(key, {}).get('cookie', ''))}</textarea>
                  </label>
                  <label>浏览器 User-Agent（可选）
                    <textarea name="{key}_user_agent" placeholder="建议对 X/Twitter 一并填写浏览器 UA">{esc(site_sessions.get(key, {}).get('user_agent', ''))}</textarea>
                  </label>
                </div>
                """
                for key, config in SITE_SESSION_CONFIG.items()
            )

            return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>AI 推送服务管理台</title>
  <style>
    :root {{
      --bg: #f4f1ea;
      --panel: #fffdf8;
      --ink: #1f262d;
      --muted: #5d6773;
      --line: #d9d1c4;
      --accent: #21558a;
      --accent-soft: #dce8f5;
      --ok: #2f6f3e;
      --warn: #985c12;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Noto Sans SC", "PingFang SC", "Microsoft YaHei", sans-serif;
      background: linear-gradient(180deg, #ece5d8 0%, var(--bg) 100%);
      color: var(--ink);
    }}
    .wrap {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 24px;
    }}
    h1, h2 {{ margin: 0 0 12px; }}
    p {{ color: var(--muted); }}
    .grid {{
      display: grid;
      gap: 18px;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 18px;
      box-shadow: 0 8px 24px rgba(31, 38, 45, 0.08);
    }}
    label {{
      display: block;
      font-size: 14px;
      margin-bottom: 10px;
    }}
    input, textarea, button {{
      width: 100%;
      border-radius: 10px;
      border: 1px solid var(--line);
      padding: 10px 12px;
      font: inherit;
    }}
    textarea {{ min-height: 140px; resize: vertical; }}
    input[type="checkbox"] {{ width: auto; }}
    .row {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }}
    .row > * {{ flex: 1; }}
    .button {{
      background: var(--accent);
      color: white;
      border: none;
      cursor: pointer;
      min-width: 120px;
    }}
    .button.alt {{
      background: var(--accent-soft);
      color: var(--accent);
    }}
    .button.warn {{
      background: #6b2e2e;
      color: white;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 8px 6px;
      text-align: left;
      vertical-align: top;
    }}
    code, pre {{
      background: #f0ece4;
      border-radius: 10px;
      padding: 10px;
      display: block;
      overflow: auto;
      white-space: pre-wrap;
    }}
    .status-ok {{ color: var(--ok); font-weight: 700; }}
    .status-warn {{ color: var(--warn); font-weight: 700; }}
    .actions {{
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    }}
    .scroll-box {{
      max-height: 520px;
      overflow-y: auto;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fff;
    }}
    .task-layout {{
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 16px;
    }}
    .task-list {{
      display: flex;
      flex-direction: column;
      gap: 8px;
    }}
    .task-item {{
      text-align: left;
      background: #fff;
      color: var(--ink);
      border: 1px solid var(--line);
      padding: 10px;
    }}
    .task-item.active {{
      border-color: var(--accent);
      box-shadow: inset 0 0 0 1px var(--accent);
    }}
    .task-item span, .task-item small {{
      display: block;
      color: var(--muted);
      margin-top: 4px;
    }}
    .project-filters {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-bottom: 12px;
    }}
    .project-filters select {{
      width: auto;
      min-width: 180px;
    }}
    .project-filters input[type="date"] {{
      width: auto;
      min-width: 160px;
    }}
    .project-list {{
      display: flex;
      flex-direction: column;
      gap: 12px;
    }}
    .project-card {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      background: #fff;
    }}
    .session-card {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      background: #fff;
      margin-bottom: 12px;
    }}
    .session-card h3 {{
      margin: 0 0 8px;
    }}
    .project-meta {{
      color: var(--muted);
      font-size: 13px;
      margin-top: 4px;
      margin-bottom: 8px;
    }}
    .title-link {{
      margin-left: 10px;
      font-size: 13px;
      color: var(--accent);
      text-decoration: none;
      word-break: break-all;
    }}
    .title-link:hover {{
      text-decoration: underline;
    }}
    .inline-form {{
      margin-top: 10px;
    }}
    .inline-form button {{
      width: auto;
      min-width: 120px;
    }}
    @media (max-width: 900px) {{
      .task-layout {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>AI 推送服务管理台</h1>
    <p>本机使用的后台控制页。你可以在这里配置模型、Telegram、来源、偏好、定时和运行方式。</p>

    <div class="grid">
      <section class="panel">
        <h2>服务状态</h2>
        <p>后台健康：<span id="health-overall" class="{'status-ok' if health['overall'] == '正常' else 'status-warn'}">{esc(health['overall'])}</span></p>
        <p>当前服务：<span id="service-status" class="{'status-ok' if state.service_enabled else 'status-warn'}">{'运行中' if state.service_enabled else '已停止'}</span></p>
        <p>调度线程：<span id="scheduler-status" class="{'status-ok' if health['scheduler_status'] == '正常' else 'status-warn'}">{esc(health['scheduler_status'])}</span></p>
        <p>数据库：<span id="db-status" class="{'status-ok' if health['db_status'] == '正常' else 'status-warn'}">{esc(health['db_status'])}</span></p>
        <p>AI 配置：<span id="ai-status" class="{'status-ok' if health['ai_status'] == '已配置' else 'status-warn'}">{esc(health['ai_status'])}</span></p>
        <p>Telegram 配置：<span id="telegram-status" class="{'status-ok' if health['telegram_status'] == '已配置' else 'status-warn'}">{esc(health['telegram_status'])}</span></p>
        <p>定时任务：<span id="schedule-status" class="{'status-ok' if state.schedule_enabled else 'status-warn'}">{'已启用（按本地时间）' if state.schedule_enabled else '未启用'}</span></p>
        <p>下一次计划运行（本地时间）：<span id="next-run">{esc(fmt_local(next_run_at)) or '未计划'}</span></p>
        <p>调度心跳（本地时间）：<span id="heartbeat-at">{esc(fmt_local(health['heartbeat_at'])) or '暂无'}</span></p>
        <p>当前动作：<span id="active-action">{esc(state.active_action) or '无'}</span></p>
        <p>最近状态：<span id="last-status">{esc(state.last_status) or '无'}</span> / <span id="last-message">{esc(state.last_message) or '无'}</span></p>
        <div class="actions">
          <form method="post" action="/service/start"><button class="button" type="submit">启动服务</button></form>
          <form method="post" action="/service/stop"><button class="button warn" type="submit">停止服务</button></form>
          <form method="post" action="/action/run-now"><button class="button" type="submit">立即执行</button></form>
          <form method="post" action="/action/stop-task"><button class="button warn" type="submit">停止任务</button></form>
          <form method="post" action="/action/test-telegram"><button class="button alt" type="submit">测试 Telegram</button></form>
        </div>
      </section>

      <section class="panel">
        <h2>定时配置</h2>
        <form method="post" action="/schedule">
          <label><input type="checkbox" name="schedule_enabled" {'checked' if state.schedule_enabled else ''}> 启用每日自动推送（本地时间）</label>
          <label>执行时间（本地时间）
            <input type="time" name="schedule_time" value="{esc(state.schedule_time)}">
          </label>
          <button class="button" type="submit">保存定时配置</button>
        </form>
      </section>
    </div>

    <div class="grid" style="margin-top:18px;">
      <section class="panel">
        <h2>模型与常量</h2>
        <form method="post" action="/settings">
          <label>管理页端口
            <input name="admin_port" value="{settings.admin_port}">
          </label>
          <label>内容偏好（自然语言）
            <textarea name="content_preference_zh">{esc(settings.content_preference_zh)}</textarea>
          </label>
          <label>AI API Base URL
            <input name="ai_api_base_url" value="{esc(settings.ai_api_base_url)}">
          </label>
          <label>AI API Key
            <input type="password" name="ai_api_key" value="{esc(settings.ai_api_key)}">
          </label>
          <label>AI 模型名
            <input name="ai_model" value="{esc(settings.ai_model)}">
          </label>
          <label>Telegram Bot Token
            <input type="password" name="telegram_bot_token" value="{esc(settings.telegram_bot_token)}">
          </label>
          <label>Telegram Chat ID
            <input name="telegram_chat_id" value="{esc(settings.telegram_chat_id)}">
          </label>
          <div class="row">
            <label>HN 抓取窗口
              <input name="fetch_limit_hn" value="{settings.fetch_limit_hn}">
            </label>
            <label>HTTP 超时（秒）
              <input name="http_timeout_seconds" value="{settings.http_timeout_seconds}">
            </label>
            <label>AI 超时（秒）
              <input name="ai_timeout_seconds" value="{settings.ai_timeout_seconds}">
            </label>
          </div>
          <label><input type="checkbox" name="telegram_disable_preview" {'checked' if settings.telegram_disable_preview else ''}> 关闭 Telegram 链接预览</label>
          <button class="button" type="submit">保存设置</button>
        </form>
      </section>

      <section class="panel">
        <h2>推送源</h2>
        <form method="post" action="/sources">
          <label>来源列表（每行一个 URL）
            <textarea name="sources_text">{esc(sources_text)}</textarea>
          </label>
          <button class="button" type="submit">保存来源</button>
        </form>
      </section>
    </div>

    <div class="grid" style="margin-top:18px;">
      <section class="panel">
        <h2>最近运行</h2>
        <div class="scroll-box">
        <table>
          <thead><tr><th>开始时间</th><th>状态</th><th>Dry Run</th><th>备注</th></tr></thead>
          <tbody id="runs-body">{runs_rows}</tbody>
        </table>
        </div>
      </section>
      <section class="panel">
        <h2>项目列表</h2>
        <div class="project-filters">
          <select id="project-date-filter">
            <option value="">全部抓取日期</option>
          </select>
          <select id="project-task-filter">
            <option value="">全部任务</option>
          </select>
        </div>
        <div class="scroll-box">
          <div id="project-list" class="project-list"></div>
        </div>
      </section>
    </div>

    <section class="panel" style="margin-top:18px;">
      <h2>用户关注</h2>
      <p>这里只展示用户通过 Telegram bot 回复数字序号后成功关注的项目。你可以手动取消关注，但不能在这里直接关注。</p>
      <div class="project-filters">
        <select id="follow-year-filter">
          <option value="">全部年份</option>
        </select>
        <select id="follow-month-filter">
          <option value="">全部月份</option>
        </select>
        <select id="follow-day-filter">
          <option value="">全部日期</option>
        </select>
        <input type="date" id="follow-date-from">
        <input type="date" id="follow-date-to">
      </div>
      <div class="scroll-box">
        <div id="follow-list" class="project-list">
          {"".join(
              f'''
              <article class="project-card">
                <div>
                  <strong>{esc(row.get("display_name_zh") or row.get("project_name") or "")}</strong>
                  {f'<a class="title-link" href="{esc(row.get("primary_link") or "",)}" target="_blank" rel="noreferrer">打开链接</a>' if row.get("primary_link") else ""}
                </div>
                <div class="project-meta">
                  关注日期：{esc(row.get("followed_date") or "")} ｜ 原序号：{esc(str(row.get("item_index") or ""))} ｜ 分类：{esc(row.get("category") or "")}
                </div>
                <div>{esc(row.get("summary_200") or "")}</div>
              </article>
              '''
              for row in followed_projects
          ) or "<div class='project-card'>暂无关注项目</div>"}
        </div>
      </div>
    </section>

    <section class="panel" style="margin-top:18px;">
      <h2>抓取任务</h2>
      <div class="task-layout">
        <div class="scroll-box">
          <div id="task-list" class="task-list">{task_items_html}</div>
        </div>
        <div class="scroll-box">
          <table>
            <thead><tr><th>时间（本地）</th><th>阶段</th><th>信息</th></tr></thead>
            <tbody id="task-events-body"><tr><td colspan="3">请选择左侧抓取任务</td></tr></tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="panel" style="margin-top:18px;">
      <h2>后台关键信息</h2>
      <div class="scroll-box">
        <table>
          <thead><tr><th>时间（本地）</th><th>级别</th><th>信息</th></tr></thead>
          <tbody id="events-body">{event_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel" style="margin-top:18px;">
      <h2>站点登录会话</h2>
      <p>先在浏览器登录 Reddit、GitHub 或 X，然后把对应请求里的 Cookie Header 粘贴到这里，或者直接粘贴 Cookie-Editor 导出的完整 JSON。保存时会自动解析并整理成真正的 Cookie Header，后续抓取命中这些域名时会自动带上 Cookie，减少被登录页拦住的情况。</p>
      <form method="post" action="/site-sessions">
        {session_blocks}
        <button class="button" type="submit">保存站点会话</button>
      </form>
    </section>
  </div>
  <script>
    let lastStatusPayload = null;
    let selectedTaskId = '';

    function escapeHtml(value) {{
      return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('\"', '&quot;')
        .replaceAll(\"'\", '&#39;');
    }}

    function fmtLocal(value) {{
      if (!value) return '';
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      const pad = (n) => String(n).padStart(2, '0');
      return `${{date.getFullYear()}}-${{pad(date.getMonth()+1)}}-${{pad(date.getDate())}} ${{pad(date.getHours())}}:${{pad(date.getMinutes())}}:${{pad(date.getSeconds())}}`;
    }}

    function renderRuns(rows) {{
      if (!rows.length) return '<tr><td colspan=\"4\">暂无运行记录</td></tr>';
      return rows.map(row => `<tr><td>${{escapeHtml(fmtLocal(row.started_at || ''))}}</td><td>${{escapeHtml(row.status || '')}}</td><td>${{escapeHtml(String(row.dry_run))}}</td><td>${{escapeHtml(row.notes || '')}}</td></tr>`).join('');
    }}

    function taskTitle(task) {{
      return `${{task.order}}. ${{task.title}}`;
    }}

    function renderTaskList(tasks) {{
      if (!tasks.length) return '<div>暂无抓取任务</div>';
      return tasks.map(task => {{
        const activeClass = task.task_id === selectedTaskId ? 'active' : '';
        return `<button class="task-item ${{activeClass}}" data-task-id="${{escapeHtml(task.task_id)}}"><strong>${{escapeHtml(taskTitle(task))}}</strong><span>${{escapeHtml(task.status || '')}}</span><small>${{escapeHtml(task.summary || '')}}</small></button>`;
      }}).join('');
    }}

    function renderTaskEvents(task) {{
      if (!task) return '<tr><td colspan="3">请选择左侧抓取任务</td></tr>';
      if (!task.events || !task.events.length) return '<tr><td colspan="3">该任务暂无关键过程信息</td></tr>';
      return task.events.map(item => `<tr><td>${{escapeHtml(fmtLocal(item.time || ''))}}</td><td>${{escapeHtml(item.stage || '')}}</td><td>${{escapeHtml(item.message || '')}}</td></tr>`).join('');
    }}

    function renderProjectCards(rows) {{
      if (!rows.length) return '<div class="project-card">暂无项目</div>';
      return rows.map(row => `
        <article class="project-card">
          <div>
            <strong>${{escapeHtml(row.display_name_zh || row.canonical_name || '')}}</strong>
            ${{row.primary_link ? `<a class="title-link" href="${{escapeHtml(row.primary_link)}}" target="_blank" rel="noreferrer">打开链接</a>` : ''}}
          </div>
          <div class="project-meta">
            抓取日期：${{escapeHtml(row.task_date || '未知')}} ｜ 任务：${{escapeHtml(row.task_source_id || '未知')}} ｜ 成熟度：${{escapeHtml(row.maturity || '')}} ｜ 分类：${{escapeHtml(row.category || '')}} ｜ 状态：${{escapeHtml(row.verification_state || '')}}
          </div>
          <div>${{escapeHtml(row.summary_200 || '')}}</div>
        </article>
      `).join('');
    }}

    function renderFollowCards(rows) {{
      if (!rows.length) return '<div class="project-card">暂无关注项目</div>';
      return rows.map(row => `
        <article class="project-card">
          <div>
            <strong>${{escapeHtml(row.display_name_zh || row.project_name || '')}}</strong>
            ${{row.primary_link ? `<a class="title-link" href="${{escapeHtml(row.primary_link)}}" target="_blank" rel="noreferrer">打开链接</a>` : ''}}
          </div>
          <div class="project-meta">
            关注日期：${{escapeHtml(row.followed_date || '')}} ｜ 原序号：${{escapeHtml(String(row.item_index || ''))}} ｜ 分类：${{escapeHtml(row.category || '')}}
          </div>
          <div>${{escapeHtml(row.summary_200 || '')}}</div>
          <form class="inline-form" method="post" action="/follow/unfollow">
            <input type="hidden" name="follow_id" value="${{escapeHtml(row.follow_id || '')}}">
            <button class="button warn" type="submit">取消关注</button>
          </form>
        </article>
      `).join('');
    }}

    function filterProjects(rows) {{
      const dateFilter = document.getElementById('project-date-filter').value;
      const taskFilter = document.getElementById('project-task-filter').value;
      return rows.filter(row => {{
        if (dateFilter && row.task_date !== dateFilter) return false;
        if (taskFilter && row.task_source_id !== taskFilter) return false;
        return true;
      }});
    }}

    function filterFollowedProjects(rows) {{
      const yearFilter = document.getElementById('follow-year-filter').value;
      const monthFilter = document.getElementById('follow-month-filter').value;
      const dayFilter = document.getElementById('follow-day-filter').value;
      const dateFrom = document.getElementById('follow-date-from').value;
      const dateTo = document.getElementById('follow-date-to').value;
      return rows.filter(row => {{
        const followedDate = String(row.followed_date || '');
        if (!followedDate) return false;
        const [year, month, day] = followedDate.split('-');
        if (yearFilter && year !== yearFilter) return false;
        if (monthFilter && month !== monthFilter) return false;
        if (dayFilter && day !== dayFilter) return false;
        if (dateFrom && followedDate < dateFrom) return false;
        if (dateTo && followedDate > dateTo) return false;
        return true;
      }});
    }}

    function syncProjectFilters(rows) {{
      const dateSelect = document.getElementById('project-date-filter');
      const taskSelect = document.getElementById('project-task-filter');
      const currentDate = dateSelect.value;
      const currentTask = taskSelect.value;
      const dates = [...new Set(rows.map(row => row.task_date).filter(Boolean))];
      const tasks = [...new Set(rows.map(row => row.task_source_id).filter(Boolean))];
      dateSelect.innerHTML = '<option value=\"\">全部抓取日期</option>' + dates.map(value => `<option value="${{escapeHtml(value)}}">${{escapeHtml(value)}}</option>`).join('');
      taskSelect.innerHTML = '<option value=\"\">全部任务</option>' + tasks.map(value => `<option value="${{escapeHtml(value)}}">${{escapeHtml(value)}}</option>`).join('');
      dateSelect.value = dates.includes(currentDate) ? currentDate : '';
      taskSelect.value = tasks.includes(currentTask) ? currentTask : '';
    }}

    function syncFollowFilters(rows) {{
      const yearSelect = document.getElementById('follow-year-filter');
      const monthSelect = document.getElementById('follow-month-filter');
      const daySelect = document.getElementById('follow-day-filter');
      const currentYear = yearSelect.value;
      const currentMonth = monthSelect.value;
      const currentDay = daySelect.value;
      const years = [...new Set(rows.map(row => String(row.followed_date || '').slice(0, 4)).filter(Boolean))];
      const months = [...new Set(rows.map(row => String(row.followed_date || '').slice(5, 7)).filter(Boolean))];
      const days = [...new Set(rows.map(row => String(row.followed_date || '').slice(8, 10)).filter(Boolean))];
      yearSelect.innerHTML = '<option value=\"\">全部年份</option>' + years.map(value => `<option value="${{escapeHtml(value)}}">${{escapeHtml(value)}}</option>`).join('');
      monthSelect.innerHTML = '<option value=\"\">全部月份</option>' + months.map(value => `<option value="${{escapeHtml(value)}}">${{escapeHtml(value)}}</option>`).join('');
      daySelect.innerHTML = '<option value=\"\">全部日期</option>' + days.map(value => `<option value="${{escapeHtml(value)}}">${{escapeHtml(value)}}</option>`).join('');
      yearSelect.value = years.includes(currentYear) ? currentYear : '';
      monthSelect.value = months.includes(currentMonth) ? currentMonth : '';
      daySelect.value = days.includes(currentDay) ? currentDay : '';
    }}

    function renderEvents(rows) {{
      if (!rows.length) return '<tr><td colspan=\"3\">暂无后台关键信息</td></tr>';
      return rows.slice().reverse().map(item => `<tr><td>${{escapeHtml(fmtLocal(item.time || ''))}}</td><td>${{escapeHtml(item.level || '')}}</td><td>${{escapeHtml(item.message || '')}}</td></tr>`).join('');
    }}

    async function refreshStatus() {{
      try {{
        const res = await fetch('/api/status', {{ cache: 'no-store' }});
        if (!res.ok) return;
        const data = await res.json();
        document.getElementById('health-overall').textContent = data.health.overall || '未知';
        document.getElementById('service-status').textContent = data.state.service_enabled ? '运行中' : '已停止';
        document.getElementById('scheduler-status').textContent = data.health.scheduler_status || '未知';
        document.getElementById('db-status').textContent = data.health.db_status || '未知';
        document.getElementById('ai-status').textContent = data.health.ai_status || '未知';
        document.getElementById('telegram-status').textContent = data.health.telegram_status || '未知';
        document.getElementById('schedule-status').textContent = data.state.schedule_enabled ? '已启用（按本地时间）' : '未启用';
        document.getElementById('next-run').textContent = fmtLocal(data.next_run_at || '') || '未计划';
        document.getElementById('heartbeat-at').textContent = fmtLocal(data.health.heartbeat_at || '') || '暂无';
        document.getElementById('active-action').textContent = data.state.active_action || '无';
        document.getElementById('last-status').textContent = data.state.last_status || '无';
        document.getElementById('last-message').textContent = data.state.last_message || '无';
        document.getElementById('runs-body').innerHTML = renderRuns(data.recent_runs || []);
        syncProjectFilters(data.project_view || []);
        document.getElementById('project-list').innerHTML = renderProjectCards(filterProjects(data.project_view || []));
        syncFollowFilters(data.followed_projects || []);
        document.getElementById('follow-list').innerHTML = renderFollowCards(filterFollowedProjects(data.followed_projects || []));
        const tasks = data.state.task_items || [];
        if (!selectedTaskId && tasks.length) {{
          selectedTaskId = tasks[0].task_id;
        }}
        if (selectedTaskId && !tasks.find(task => task.task_id === selectedTaskId)) {{
          selectedTaskId = tasks.length ? tasks[0].task_id : '';
        }}
        document.getElementById('task-list').innerHTML = renderTaskList(tasks);
        const selectedTask = tasks.find(task => task.task_id === selectedTaskId);
        document.getElementById('task-events-body').innerHTML = renderTaskEvents(selectedTask);
        document.getElementById('events-body').innerHTML = renderEvents(data.state.recent_events || []);
        lastStatusPayload = data;
      }} catch (_) {{
        // keep last successful UI state
      }}
    }}

    document.addEventListener('click', (event) => {{
      const button = event.target.closest('.task-item');
      if (!button) return;
      selectedTaskId = button.dataset.taskId || '';
      if (!lastStatusPayload) return;
      const tasks = lastStatusPayload.state.task_items || [];
      document.getElementById('task-list').innerHTML = renderTaskList(tasks);
      const selectedTask = tasks.find(task => task.task_id === selectedTaskId);
      document.getElementById('task-events-body').innerHTML = renderTaskEvents(selectedTask);
    }});

    document.getElementById('project-date-filter').addEventListener('change', () => {{
      if (!lastStatusPayload) return;
      document.getElementById('project-list').innerHTML = renderProjectCards(filterProjects(lastStatusPayload.project_view || []));
    }});
    document.getElementById('project-task-filter').addEventListener('change', () => {{
      if (!lastStatusPayload) return;
      document.getElementById('project-list').innerHTML = renderProjectCards(filterProjects(lastStatusPayload.project_view || []));
    }});
    ['follow-year-filter', 'follow-month-filter', 'follow-day-filter', 'follow-date-from', 'follow-date-to'].forEach(id => {{
      document.getElementById(id).addEventListener('change', () => {{
        if (!lastStatusPayload) return;
        document.getElementById('follow-list').innerHTML = renderFollowCards(filterFollowedProjects(lastStatusPayload.followed_projects || []));
      }});
    }});

    refreshStatus();
    setInterval(refreshStatus, 4000);
  </script>
</body>
</html>"""

        def _build_status_payload(self) -> dict:
            return service.status_snapshot()

    return AdminHandler


def run_admin_server(project_root: Path, host: str, port: int) -> None:
    service = AdminService(project_root)
    service.ensure_scheduler()
    store = AdminStore(project_root)
    server = ThreadingHTTPServer((host, port), make_handler(project_root, service=service, store=store))
    try:
        server.serve_forever()
    finally:
        server.server_close()

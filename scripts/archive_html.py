#!/usr/bin/env python3
"""Render private daily chat views from kirocc JSONL archives."""

import argparse
from datetime import datetime, timedelta, timezone
import html
import json
import os
from pathlib import Path
import re
import tempfile


INTERNAL_MARKERS = (
    "<transcript>", "<system-reminder>", "<local-command", "<command-name>",
    "pre-gathered recon", "claude.md configuration", "stage 1 does not",
    "respond with <severity>", "the user stepped away",
    "codebase and user instructions", "auto-mode config",
    "describe your most recent action in 3-5 words",
)
IMAGE_MARKER = re.compile(r"\[Image(?::[^\]]*| #\d+)\]")
SESSION = re.compile(r"<session>\s*(.*?)\s*</session>", re.DOTALL)
PROGRESS = re.compile(r"(?:Reading|Reviewing|Checking|Searching|Grepping) [^\n]{0,120}\Z")
CSS = """body{margin:0;background:#f4f7fb;color:#243044;font:15px/1.65 system-ui,'Microsoft YaHei',sans-serif}.wrap{max-width:1100px;margin:auto;padding:28px 18px 60px}header{background:#2563a8;color:white;border-radius:16px;padding:24px}.session{border-top:2px solid #cbd5e1;margin-top:30px}.turn{display:flex;flex-direction:column;margin:20px 0}.bubble{max-width:72%;padding:14px 18px;border-radius:14px;margin:6px 0;overflow-wrap:anywhere}.user{background:#e8f1ff;border-left:5px solid #5b8def;align-self:flex-start}.ai{background:#ecfdf3;border-left:5px solid #35a36a;align-self:flex-end}.label{font-weight:700}.stamp{font-size:12px;color:#64748b}pre{background:#0f172a;color:#dbeafe;padding:12px;overflow:auto}@media(max-width:700px){.bubble{max-width:90%}}"""


def user_text(message):
    if not isinstance(message, dict) or message.get("role") != "user":
        return ""
    content = message.get("content")
    if isinstance(content, str):
        text = content.strip()
    elif isinstance(content, list):
        text = "\n".join(block.get("text", "") for block in content
                         if isinstance(block, dict) and block.get("type") == "text").strip()
    else:
        return ""
    session = SESSION.search(text)
    if session:
        text = session.group(1).strip()
    elif (not text or len(text) > 2000 or
          any(marker in text.lower() for marker in INTERNAL_MARKERS) or
          (text.startswith(("{", "[", "<")) and not text.startswith("[Image"))):
        return ""
    return IMAGE_MARKER.sub("", text).strip()


def assistant_text(row):
    response = row.get("assistant_response")
    parts = []
    has_tool = False
    if isinstance(response, dict):
        for block in response.get("content", []):
            if not isinstance(block, dict):
                continue
            has_tool |= block.get("type") in ("tool_use", "server_tool_use")
            if block.get("type") == "text":
                parts.append(block.get("text", ""))
    elif isinstance(response, list):
        for event in response:
            data = event.get("data") if isinstance(event, dict) else None
            if not isinstance(data, dict):
                continue
            block = data.get("content_block") or {}
            if isinstance(block, dict):
                has_tool |= block.get("type") in ("tool_use", "server_tool_use")
            delta = data.get("delta") or {}
            if isinstance(delta, dict) and delta.get("type") == "text_delta":
                parts.append(delta.get("text", ""))
    text = "".join(parts).strip()
    if (has_tool or not text or text == "<block>no" or text.startswith(("{", "[")) or
        PROGRESS.fullmatch(text) or
        any(marker in text for marker in ("permissions.allow", "Slots left at shipped default",
                                         "<severity>", "autoMode settings", "claude auto-mode config"))):
        return ""
    return text


def turns(path):
    seen = set()
    current = None
    with path.open(encoding="utf-8") as source:
        for line in source:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue  # A concurrent append can leave one incomplete final line.
            messages = row.get("user_messages") or ([row["user_message"]] if row.get("user_message") else [])
            fresh = []
            for message in messages:
                text = user_text(message)
                if text and text not in seen:
                    seen.add(text)
                    fresh.append(text)
            if fresh:
                if current:
                    yield current
                current = {"users": fresh, "ai": "", "time": row.get("timestamp", "")}
            answer = assistant_text(row)
            if current and answer:
                current["ai"] = answer
    if current:
        yield current


def formatted(text):
    parts = []
    code = False
    for line in text.splitlines():
        if line.startswith("```"):
            parts.append("</code></pre>" if code else "<pre><code>")
            code = not code
        elif code:
            parts.append(html.escape(line) + "\n")
        elif line.strip():
            parts.append("<p>" + html.escape(line) + "</p>")
    if code:
        parts.append("</code></pre>")
    return "".join(parts)


def render_day(device_dir, day):
    sources = sorted(device_dir.glob("*.jsonl"))
    if not sources:
        return None
    output = device_dir / (day + "-readable.html")
    fd, temporary = tempfile.mkstemp(prefix=".archive-html-", suffix=".tmp", dir=device_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as out:
            out.write("<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'>")
            out.write("<meta name='viewport' content='width=device-width,initial-scale=1'>")
            out.write("<title>对话记录</title><style>" + CSS + "</style></head><body><main class='wrap'>")
            out.write("<header><h1>" + html.escape(device_dir.name) + " 对话记录</h1><p>")
            out.write(day + " · 用户自然语言与 AI 最终文本</p></header>\n")
            for source in sources:
                out.write('<section class="session"><h2>会话 ' + html.escape(source.stem) + "</h2>\n")
                for turn in turns(source):
                    out.write('<div class="turn"><div class="stamp">' + html.escape(turn["time"]) + "</div>")
                    for user in turn["users"]:
                        out.write('<article class="bubble user"><div class="label">用户</div>' + formatted(user) + "</article>")
                    if turn["ai"]:
                        out.write('<article class="bubble ai"><div class="label">AI</div>' + formatted(turn["ai"]) + "</article>")
                    out.write("</div>\n")
                out.write("</section>\n")
            out.write("</main></body></html>\n")
        os.chmod(temporary, 0o600)
        os.replace(temporary, output)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive_root", type=Path)
    parser.add_argument("--date", help="UTC date in YYYY-MM-DD; defaults to today and yesterday")
    parser.add_argument("--all", action="store_true", help="render every archived date")
    args = parser.parse_args()
    if args.all:
        days = sorted(path.name for path in args.archive_root.iterdir() if path.is_dir() and
                      re.fullmatch(r"\d{4}-\d{2}-\d{2}", path.name))
    elif args.date:
        days = [datetime.strptime(args.date, "%Y-%m-%d").date().isoformat()]
    else:
        today = datetime.now(timezone.utc).date()
        days = [(today - timedelta(days=1)).isoformat(), today.isoformat()]
    for day in days:
        date_dir = args.archive_root / day
        if date_dir.is_dir():
            for device_dir in sorted(date_dir.iterdir()):
                if device_dir.is_dir():
                    result = render_day(device_dir, day)
                    if result:
                        print(result)


if __name__ == "__main__":
    main()

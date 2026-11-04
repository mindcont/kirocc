import json
import os
from pathlib import Path
import tempfile
import unittest

import archive_html


class ArchiveHTMLTests(unittest.TestCase):
    def test_filters_split_internal_prompt_and_keeps_multiturn_dialogue(self):
        with tempfile.TemporaryDirectory() as temporary:
            device = Path(temporary) / "2026-11-04" / "ZZX"
            device.mkdir(parents=True)
            rows = [
                {"timestamp": "2026-11-04T01:00:00Z", "user_messages": [
                    {"role": "user", "content": [
                        {"type": "text", "text": "<session>第一轮 <b>提问</b></session>"},
                        {"type": "text", "text": "<transcript>internal</transcript>"}]},
                    {"role": "user", "content": "补充条件"}],
                 "assistant_response": [{"data": {"delta": {"type": "text_delta", "text": "第一轮回答"}}}]},
                {"timestamp": "2026-11-04T01:01:00Z", "user_messages": [
                    {"role": "user", "content": [{"type": "text", "text": "Describe your most recent action in 3-5 words"}]},
                    {"role": "user", "content": "<transcript>internal</transcript>"}],
                 "assistant_response": [{"data": {"delta": {"type": "text_delta", "text": "Reading internal.py"}}}]},
                {"timestamp": "2026-11-04T01:02:00Z", "user_messages": [
                    {"role": "user", "content": "第二轮问题"}],
                 "assistant_response": {"content": [
                     {"type": "text", "text": "tool interim"}, {"type": "tool_use", "name": "x"}]}},
                {"timestamp": "2026-11-04T01:03:00Z", "user_messages": [],
                 "assistant_response": {"content": [{"type": "text", "text": "最终回答"}]}},
            ]
            source = device / "session.jsonl"
            source.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")
            result = archive_html.render_day(device, "2026-11-04")
            page = result.read_text(encoding="utf-8")
            self.assertEqual(page.count('class="bubble user"'), 3)
            self.assertEqual(page.count('class="bubble ai"'), 2)
            for phrase in ("第一轮", "&lt;b&gt;提问&lt;/b&gt;", "补充条件", "第二轮问题", "第一轮回答", "最终回答"):
                self.assertIn(phrase, page)
            for phrase in ("internal", "Describe your most recent action", "tool interim", "Reading internal.py"):
                self.assertNotIn(phrase, page)
            if os.name != "nt":
                self.assertEqual(result.stat().st_mode & 0o777, 0o600)


if __name__ == "__main__":
    unittest.main()

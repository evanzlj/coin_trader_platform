"""
prompt_generator grade-aware schema tests.

A+ 保留 a_plus_impulse_assessment 首字段；A-only 派生版去掉它、首字段改 watch_summary，
消除 system(watch_summary 第一) 与 schema(a_plus 第一) 的矛盾。
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from prompt_generator.templates import get_user_chart_section, get_system


class TestGradeAwareSchema(unittest.TestCase):
    def test_aplus_keeps_impulse_block(self):
        ap = get_user_chart_section("BTC/USDT", "A+")
        self.assertIn("a_plus_impulse_assessment", ap)
        self.assertIn("MUST be a_plus_impulse_assessment", ap)

    def test_aonly_drops_impulse_block(self):
        ao = get_user_chart_section("BTC/USDT", "A-only")
        self.assertNotIn("a_plus_impulse_assessment", ao)
        self.assertIn("MUST be watch_summary", ao)
        # 仍保留正文
        self.assertIn("watch_summary", ao)
        self.assertIn("playbooks", ao)

    def test_aonly_first_field_is_watch_summary(self):
        ao = get_user_chart_section("BTC/USDT", "A-only")
        import re
        m = re.search(r'MUST be watch_summary:\s*\{\s*"(\w+)"', ao)
        self.assertIsNotNone(m, "schema 应紧接 { 后是首字段")
        self.assertEqual(m.group(1), "watch_summary")

    def test_aonly_system_and_schema_agree(self):
        # system 段(A-only)要求 watch_summary 第一；schema 也应一致
        sys_a = get_system("A")
        self.assertIn("first top-level field in your JSON response must be watch_summary", sys_a)
        ao = get_user_chart_section("BTC/USDT", "A-only")
        self.assertNotIn("MUST be a_plus_impulse_assessment", ao)

    def test_derivation_actually_removed_something(self):
        ap = get_user_chart_section("BTC/USDT", "A+")
        ao = get_user_chart_section("BTC/USDT", "A-only")
        self.assertGreater(len(ap), len(ao), "A-only 应比 A+ 短(去掉了 a_plus 块)")


if __name__ == "__main__":
    unittest.main()

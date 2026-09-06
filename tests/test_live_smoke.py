#!/usr/bin/env python3
"""Bounded indexed live smoke. Skipped unless MCP_LIVE_SMOKE=1. No global aggregates."""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@unittest.skipUnless(os.environ.get("MCP_LIVE_SMOKE") == "1", "set MCP_LIVE_SMOKE=1")
class LiveSmokeTests(unittest.TestCase):
    def test_income_tax_article_20_guarded_delegation(self):
        import server

        text = server.t_get_article({
            "law_name": "소득세법", "article_number": "20",
        })
        self.assertIn("소득세법 시행령 제38조", text)
        self.assertNotIn("일용근로자의 범위 및 주택임대소득", text)
        self.assertIn("원천 스냅샷:", text)
        self.assertIn("인용은 적용 확정이 아니다", text)

    def test_form_28_is_visible(self):
        import server

        text = server.t_get_annex({
            "law_name": "소득세법 시행규칙", "annex_number": "서식 28",
        })
        self.assertIn("서식 28", text)
        self.assertIn("계산서", text)

    def test_search_annexes_form_28_stays_in_current_scope(self):
        import server

        text = server.t_search_annexes({
            "query": "계산서", "law_name": "소득세법 시행규칙", "limit": 5,
        })
        self.assertIn("서식 28", text)
        self.assertNotIn("자산재평가법시행령", text)

    def test_list_laws_excludes_historical_names(self):
        import server

        text = server.t_list_laws({})
        self.assertIn("소득세법", text)
        self.assertNotIn("- 자산재평가법시행령", text)
        self.assertNotIn("- 자산재평가법시행규칙", text)
        self.assertIn("현행 그래프에서 제외", text)

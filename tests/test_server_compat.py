#!/usr/bin/env python3
"""MCP tool schema compatibility and guarded response formatting (no live DB)."""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import server  # noqa: E402


STABLE_TOOLS = {
    "list_laws": [],
    "search_articles": ["query"],
    "get_article": ["law_name", "article_number"],
    "get_article_history": ["law_name", "article_number"],
    "search_cases": ["query"],
    "search_interpretations": ["query"],
    "verify_citations": ["text"],
    "search_annexes": ["query"],
    "get_annex": ["law_name", "annex_number"],
    "list_upcoming": [],
    "list_treaties": [],
    "search_treaties": ["query"],
    "get_treaty_article": ["country", "article_number"],
    "search_tax": ["search_intents"],
    "get_evidence": ["evidence_type", "evidence_id"],
}


class SchemaCompatibilityTests(unittest.TestCase):
    def test_every_tool_is_read_only(self):
        for tool in server.TOOLS:
            self.assertTrue(tool["annotations"]["readOnlyHint"], tool["name"])
            self.assertFalse(tool["annotations"]["destructiveHint"], tool["name"])

    def test_tool_names_and_required_args_unchanged(self):
        names = [t["name"] for t in server.TOOLS]
        self.assertEqual(names, list(STABLE_TOOLS))
        for tool in server.TOOLS:
            required = tool["inputSchema"].get("required", [])
            self.assertEqual(required, STABLE_TOOLS[tool["name"]], tool["name"])
            self.assertFalse(tool["inputSchema"].get("additionalProperties", True))

    def test_version_and_initialize_payload(self):
        self.assertEqual(server.SERVER_VERSION, "0.12.0")
        resp = server.handle_message(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
            "127.0.0.1",
        )
        self.assertEqual(resp["result"]["serverInfo"]["version"], "0.12.0")
        self.assertIn("검증", resp["result"]["instructions"])

    def test_tools_list_keeps_input_schema_keys(self):
        resp = server.handle_message(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            "127.0.0.1",
        )
        listed = {t["name"]: t for t in resp["result"]["tools"]}
        self.assertEqual(set(listed), set(STABLE_TOOLS))
        art = listed["get_article"]["inputSchema"]["properties"]
        self.assertEqual(set(art), {"law_name", "article_number"})


LAW_NAMES = ["소득세법", "소득세법 시행령", "소득세법 시행규칙"]


class GetArticleFormatTests(unittest.TestCase):
    def setUp(self):
        # 법령명 해석은 현행 법령 목록을 한 번 읽는다 — 가짜 DB 쿼리 순서와 섞이지 않게 목록을 고정한다
        patcher = mock.patch.object(server, "_law_index", return_value=(
            LAW_NAMES, {server.law_key(n): n for n in LAW_NAMES}))
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_get_article_uses_guarded_queries_and_uncertainty_footer(self):
        calls = []

        def fake_cypher(stmt, parameters=None):
            calls.append(stmt)
            if "a.article_number = $no" in stmt and "article_snap" in stmt:
                return [{
                    "law": "소득세법", "enf": "20260701", "aid": "소득세법_020",
                    "no": "제20조", "title": "근로소득", "content": "근로소득은 …",
                    "law_snap": "moleg-eflaw:law", "article_snap": "moleg-eflaw:art20",
                }]
            if "DELEGATES_TO" in stmt:
                self.assertIn("rel.active = true", stmt)
                self.assertIn("upper_source_snapshot", stmt)
                return [{
                    "law": "소득세법 시행령", "no": "제38조", "title": "근로소득의 범위",
                }]
            if "DELEGATED_FROM" in stmt:
                return []
            if "HAS_ANNEX" in stmt:
                self.assertIn("evidence_source_label", stmt)
                return []
            if "HAS_UPCOMING" in stmt:
                return []
            if "HAS_CASE" in stmt:
                self.assertIn(server.DOCUMENT_PROJECTION_SCHEMA, stmt)
                return [{
                    "kind": "case", "org": "조세심판원", "no": "조심 2026인1522",
                    "d": "20260301", "tr": "unresolved", "vid": None,
                }]
            self.fail(f"unexpected cypher: {stmt[:120]}")

        with mock.patch.object(server, "cypher", fake_cypher):
            text = server.t_get_article({
                "law_name": "소득세법", "article_number": "20",
            })
        self.assertIn("소득세법 시행령 제38조", text)
        self.assertNotIn("일용근로자의 범위", text)
        self.assertIn("적용버전 미해소", text)
        self.assertIn("원천 스냅샷: moleg-eflaw:art20", text)
        self.assertIn("인용은 적용 확정이 아니다", text)
        self.assertTrue(any("DELEGATES_TO" in s for s in calls))

    def test_history_marks_unverified_versions(self):
        def fake_cypher(stmt, parameters=None):
            if "HAS_VERSION" in stmt:
                return [
                    {
                        "enf": "20260701", "vfrom": "20260701", "vto": None,
                        "vid": "v-cur", "verified": True, "snap": "snap-cur",
                    },
                    {
                        "enf": "20170101", "vfrom": "20170101", "vto": "20241231",
                        "vid": "v-old", "verified": False, "snap": "",
                    },
                ]
            return [{"content": "<개정 2026.7.1.>"}]

        with mock.patch.object(server, "cypher", fake_cypher):
            text = server.t_get_article_history({
                "law_name": "소득세법", "article_number": "제20조",
            })
        self.assertIn("검증·원천있음", text)
        self.assertIn("미검증 — 적용 단정 금지", text)
        self.assertIn("resolved_version_id 없는 인용", text)

    def test_list_laws_query_excludes_historical(self):
        captured = {}

        def fake_cypher(stmt, parameters=None):
            captured["stmt"] = stmt
            return [{
                "name": "소득세법", "type": "법률", "enf": "20260701",
                "ministry": "재정경제부", "inst": "law",
            }]

        with mock.patch.object(server, "cypher", fake_cypher):
            text = server.t_list_laws({})
        self.assertIn("historical", captured["stmt"])
        self.assertIn("자산재평가법시행령", text)
        self.assertIn("현행 그래프에서 제외", text)

    def test_search_annexes_uses_fail_closed_query(self):
        captured = {}

        def fake_cypher(stmt, parameters=None):
            captured["stmt"] = stmt
            return []

        with mock.patch.object(server, "cypher", fake_cypher):
            server.t_search_annexes({"query": "계산서", "law_name": "소득세법 시행규칙"})
        self.assertEqual(captured["stmt"], server.Q_SEARCH_ANNEXES)
        self.assertNotIn("coalesce(node.is_current, true)", captured["stmt"])
        self.assertIn("node.is_current = true", captured["stmt"])
        self.assertIn("Law {law_id: node.law_id}", captured["stmt"])

    def test_get_annex_fallback_keeps_fail_closed_guards(self):
        calls = []

        def fake_cypher(stmt, parameters=None):
            calls.append(stmt)
            if "replace(x.annex_number" in stmt:
                return []
            return [{"no": "서식 1", "t": "현행만"}]

        with mock.patch.object(server, "cypher", fake_cypher):
            text = server.t_get_annex({
                "law_name": "소득세법 시행규칙", "annex_number": "서식 999",
            })
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], server.Q_GET_ANNEX)
        self.assertEqual(calls[1], server.Q_GET_ANNEX_NEAR)
        self.assertIn("수록 별표·서식", text)
        self.assertIn("서식 1", text)
        for stmt in calls:
            self.assertNotIn("coalesce(x.is_current, true)", stmt)
            self.assertIn("x.is_current = true", stmt)
            self.assertIn("x.law_id", stmt)
            self.assertIn("owns.source_snapshot = x.source_snapshot", stmt)
            self.assertIn(".deleted, false) = false", stmt)

    def test_get_annex_hit_does_not_run_unguarded_fallback(self):
        calls = []

        def fake_cypher(stmt, parameters=None):
            calls.append(stmt)
            return [{
                "law": "소득세법 시행규칙", "no": "서식 28", "title": "계산서",
                "content": "계산서 본문", "hwp": None, "pdf": None, "arts": None,
            }]

        with mock.patch.object(server, "cypher", fake_cypher):
            text = server.t_get_annex({
                "law_name": "소득세법 시행규칙", "annex_number": "서식 28",
            })
        self.assertEqual(calls, [server.Q_GET_ANNEX])
        self.assertIn("서식 28", text)
        self.assertIn("계산서", text)


if __name__ == "__main__":
    unittest.main()


class InterpretationRankingTests(unittest.TestCase):
    def test_korean_stems_drop_particles_and_filler(self):
        self.assertEqual(server.ko_stems("출자공동사업자의 배당소득에 대한 세액감면 적용 여부"),
                         ["출자공동사업자", "배당소득", "세액감면"])

    def test_bigram_similarity_ignores_spacing(self):
        self.assertGreater(server.bigram_sim("적용 여부", "적용여부"), 0.99)

    def test_doc_key_ignores_format_and_leading_zeros(self):
        self.assertEqual(server.doc_key("서면-2023-법규소득-0950"), server.doc_key("서면2023법규소득950"))
        self.assertEqual(server.interp_number_keys("서면-2023-법규기본-2595[법규과-2973]"),
                         [server.doc_key("서면-2023-법규기본-2595"), server.doc_key("법규과-2973")])

    def test_number_patterns_skip_article_numbers(self):
        text = "법인세법 제52조와 서면-2023-법규기본-2595, 기획재정부 소득세제과-1059, 2015두41937, 조심2023서1234"
        self.assertEqual([m.group(0) for m in server.INTERP_NO_RE.finditer(text)],
                         ["서면-2023-법규기본-2595", "기획재정부 소득세제과-1059"])
        self.assertEqual([m.group(0) for m in server.CASE_NO_RE.finditer(text)], ["2015두41937", "조심2023서1234"])

    def test_lookup_requires_exact_normalized_number(self):
        rows = [{"id": "a", "no": "서면-2023-법규기본-2595[법규과-2973]"}, {"id": "b", "no": "서면-2023-법규기본-12595"},
                {"id": "c", "no": "사전-202-3법규부가0594"}]
        with mock.patch.object(server, "cypher", return_value=rows):
            self.assertEqual([r["id"] for r in server.lookup_interp_numbers("서면-2023-법규기본-2595")], ["a"])
            self.assertEqual([r["id"] for r in server.lookup_interp_numbers("법규과-2973")], ["a"])
            self.assertEqual([r["id"] for r in server.lookup_interp_numbers("사전-2023-법규부가-0594")], ["c"])
            self.assertEqual(server.lookup_interp_numbers("서면-2023-법규기본-9595"), [])

    def test_rerank_lifts_title_match_over_long_body_match(self):
        body = [{"id": "long", "title": "가산세 일반", "preview": "", "score": 10.0},
                {"id": "short", "title": "출자공동사업자 배당소득 창업중소기업 세액감면 적용여부", "preview": "", "score": 4.0}]
        title = [{"id": "short", "title": body[1]["title"], "preview": "", "score": 4.0}]
        with mock.patch.object(server, "_interp_rows", side_effect=[body, title]):
            ranked = server.rank_interpretations("출자공동사업자 배당소득의 창업중소기업 세액감면 적용 여부", 2)
        self.assertEqual([r["id"] for r in ranked], ["short", "long"])

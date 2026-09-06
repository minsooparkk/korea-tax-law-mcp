#!/usr/bin/env python3
"""Graph guard direction, version uncertainty, and current-law filters (no DB)."""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import server  # noqa: E402


class GuardDirectionTests(unittest.TestCase):
    def test_delegates_to_upper_is_start_lower_is_end(self):
        g = server.delegates_to_rel_guard(rel="rel")
        self.assertIn("rel.active = true AND rel.verified = true", g)
        self.assertIn("rel.upper_source_snapshot = startNode(rel).source_snapshot", g)
        self.assertIn("rel.lower_source_snapshot = endNode(rel).source_snapshot", g)
        self.assertNotIn("rel.upper_source_snapshot = endNode(rel).source_snapshot", g)

    def test_delegated_from_is_opposite_orientation(self):
        g = server.delegated_from_rel_guard(rel="rel")
        self.assertIn("rel.lower_source_snapshot = startNode(rel).source_snapshot", g)
        self.assertIn("rel.upper_source_snapshot = endNode(rel).source_snapshot", g)
        to_g = server.delegates_to_rel_guard(rel="rel")
        self.assertNotEqual(g, to_g)

    def test_document_citation_follows_document_not_article(self):
        g = server.document_citation_guard(document="c", article="a", edge="rel")
        self.assertIn("rel.source_snapshot = coalesce(nullif(c.source_snapshot, '')", g)
        self.assertIn("rel.target_source_snapshot = a.source_snapshot", g)
        self.assertIn("rel.source_projection_sha256 = c.source_projection_sha256", g)
        self.assertNotIn("rel.source_snapshot = a.source_snapshot", g)

    def test_annex_orientation_follows_evidence_source_label(self):
        g = server.article_annex_guard(article="a", annex="x", edge="rel")
        self.assertIn("rel.evidence_source_label = 'Article'", g)
        self.assertIn("rel.evidence_source_label = 'Annex'", g)
        self.assertIn("rel.source_snapshot = a.source_snapshot", g)
        self.assertIn("rel.source_snapshot = x.source_snapshot", g)

    def test_contains_requires_law_snapshot_not_inactive(self):
        g = server.current_contains_guard(edge="owns", law="l")
        self.assertIn("owns.active = true AND owns.verified = true", g)
        self.assertIn("owns.source_snapshot = l.source_snapshot", g)

    def test_current_article_rejects_either_deleted_flag(self):
        g = server.current_article_guard(node="a")
        self.assertIn("a.is_current = true", g)
        self.assertIn("coalesce(a.deleted, false) = false", g)
        self.assertIn("coalesce(a.is_deleted, false) = false", g)

    def test_current_law_excludes_historical_and_scheduled(self):
        g = server.current_law_guard(law="l")
        self.assertIn("l.is_current = true", g)
        self.assertIn("<> 'historical'", g)
        self.assertIn("<> 'scheduled'", g)


class VersionUncertaintyTests(unittest.TestCase):
    def test_version_citation_requires_resolved_id_and_verified_snapshot(self):
        g = server.article_version_citation_guard(
            document="cas", version="v", edge="rel"
        )
        self.assertIn("rel.resolved_version_id = v.version_id", g)
        self.assertIn("v.verified = true", g)
        self.assertIn("v.source_snapshot IS NOT NULL AND v.source_snapshot <> ''", g)

    def test_missing_resolved_version_is_not_treated_as_current_application(self):
        text = server.fmt_uncertainty("unresolved", None)
        self.assertIn("시점 unresolved", text)
        self.assertIn("적용버전 미해소", text)
        self.assertNotIn("현행", text)

    def test_resolved_version_id_is_shown_when_present(self):
        text = server.fmt_uncertainty("source_dated", "소득세법_020@19910101")
        self.assertIn("시점 source_dated", text)
        self.assertIn("적용버전 소득세법_020@19910101", text)


class QueryTemplateGuardTests(unittest.TestCase):
    def test_delegate_queries_keep_direction_guards(self):
        self.assertIn("DELEGATES_TO", server.Q_DELEGATES_TO)
        self.assertIn("upper_source_snapshot = startNode(rel).source_snapshot", server.Q_DELEGATES_TO)
        self.assertIn("DELEGATED_FROM", server.Q_DELEGATED_FROM)
        self.assertIn("lower_source_snapshot = startNode(rel).source_snapshot", server.Q_DELEGATED_FROM)

    def test_citation_query_uses_document_projection_and_limit(self):
        self.assertIn("HAS_CASE", server.Q_VERIFIED_CITATIONS)
        self.assertIn("HAS_INTERPRETATION", server.Q_VERIFIED_CITATIONS)
        self.assertIn("HAS_RULING", server.Q_VERIFIED_CITATIONS)
        self.assertIn(server.DOCUMENT_PROJECTION_SCHEMA, server.Q_VERIFIED_CITATIONS)
        self.assertIn("LIMIT $limit", server.Q_VERIFIED_CITATIONS)
        self.assertIn("UNION ALL", server.Q_VERIFIED_CITATIONS)

    def test_history_query_exposes_verified_and_snapshot(self):
        self.assertIn("coalesce(v.verified, false) AS verified", server.Q_ARTICLE_HISTORY)
        self.assertIn("source_snapshot", server.Q_ARTICLE_HISTORY)
        self.assertIn("owns.active = true", server.Q_ARTICLE_HISTORY)


class AnnexFailClosedTests(unittest.TestCase):
    QUERIES = (
        server.Q_SEARCH_ANNEXES,
        server.Q_GET_ANNEX,
        server.Q_GET_ANNEX_NEAR,
    )

    def test_missing_is_current_is_not_treated_as_current(self):
        g = server.current_annex_guard(node="x")
        self.assertIn("x.is_current = true", g)
        self.assertNotIn("coalesce(x.is_current", g)
        for q in self.QUERIES:
            self.assertNotIn("coalesce(node.is_current, true)", q)
            self.assertNotIn("coalesce(x.is_current, true)", q)
            self.assertRegex(q, r"(node|x)\.is_current = true")

    def test_deleted_hidden_on_search_get_and_fallback(self):
        for q in self.QUERIES:
            self.assertIn("coalesce(", q)
            self.assertIn(".deleted, false) = false", q)
            self.assertIn(".is_deleted, false) = false", q)

    def test_owner_edge_rejects_old_snapshot(self):
        g = server.law_annex_owner_guard(law="l", annex="x", edge="owns")
        self.assertIn("owns.active = true AND owns.verified = true", g)
        self.assertIn("owns.source_snapshot = x.source_snapshot", g)
        self.assertIn("owns.source_snapshot = l.source_snapshot", g)
        for q in self.QUERIES:
            self.assertIn("law_id", q)
            self.assertIn("HAS_ANNEX", q)
            self.assertIn("source_snapshot", q)
            self.assertIn("EXISTS", q)

    def test_fallback_uses_same_scope_and_owner_contract(self):
        near = server.Q_GET_ANNEX_NEAR
        self.assertIn("Law {law_id: x.law_id}", near)
        self.assertIn("x.source_snapshot IS NOT NULL AND x.source_snapshot <> ''", near)
        self.assertIn("<> 'historical'", near)
        self.assertIn("LIMIT 40", near)
        self.assertNotIn("coalesce(x.is_current, true)", near)


if __name__ == "__main__":
    unittest.main()

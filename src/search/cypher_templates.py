"""Edge 유형별 Cypher 쿼리 템플릿 — source-search.md 탐색 전략 구현."""

from src.search.edge_provenance import (
    article_annex_guard,
    article_references_guard,
    basic_rule_guard,
    current_article_guard,
    current_contains_guard,
    current_path_nodes_guard,
    delegated_from_rel_guard,
    delegates_to_rel_guard,
    document_citation_guard,
)
from src.search.evidence_identity import verified_version_guard

# Aliases match the per-template node/rel names so guards interpolate as-is.
_DELEGATES_TO_RELS = delegates_to_rel_guard(rel="rel")
_DELEGATED_FROM_RELS = delegated_from_rel_guard(rel="rel")
_PATH_NODES = current_path_nodes_guard(nodes="nodes(path)")
_CURRENT_A = current_article_guard(node="a")
_CURRENT_REF = current_article_guard(node="ref")
_CURRENT_SOURCE = current_article_guard(node="source")
_DOC_CASE = document_citation_guard(document="c", article="a", edge="rel")
_DOC_RULING = document_citation_guard(document="r", article="a", edge="rel")
_DOC_INTERP = document_citation_guard(document="i", article="a", edge="rel")
_DOC_CASE_SOURCE = document_citation_guard(document="cas", article="source", edge="rel")
_DOC_RULING_SOURCE = document_citation_guard(
    document="ruling", article="source", edge="rel"
)
_DOC_INTERP_SOURCE = document_citation_guard(
    document="interp", article="source", edge="rel"
)
_DOC_CASE_VERSION = document_citation_guard(document="cas", article="v", edge="rel")
_ARTICLE_REFS = article_references_guard(edge="r")
_ARTICLE_REFS_REL = article_references_guard(edge="rel")
_ARTICLE_ANNEX = article_annex_guard(article="a", annex="x", edge="rel")
_BASIC_RULE = basic_rule_guard(article="a", rule="b", edge="r", law="l")
_BASIC_RULE_SOURCE = basic_rule_guard(
    article="source", rule="b", edge="rel", law="l"
)


def _document_rel_map(rel: str, graph_path: str) -> str:
    """Relationship provenance copied onto a document map projection."""
    return (
        f"temporal_resolution: {rel}.temporal_resolution, "
        f"reference_semantics: {rel}.reference_semantics, "
        f"evidence: {rel}.evidence, "
        f"source_snapshot: {rel}.source_snapshot, "
        f"target_source_snapshot: {rel}.target_source_snapshot, "
        f"source_projection_schema: {rel}.source_projection_schema, "
        f"source_projection_kind: {rel}.source_projection_kind, "
        f"source_projection_sha256: {rel}.source_projection_sha256, "
        f"source_projection_snapshot: {rel}.source_projection_snapshot, "
        f"resolved_version_id: {rel}.resolved_version_id, "
        f"graph_path: '{graph_path}'"
    )


_CASE_REL_MAP = _document_rel_map("rel", "HAS_CASE edge")
_RULING_REL_MAP = _document_rel_map("rel", "HAS_RULING edge")
_INTERP_REL_MAP = _document_rel_map("rel", "HAS_INTERPRETATION edge")


def _node_id(node: str) -> str:
    return (
        f"coalesce({node}.version_id, {node}.article_id, {node}.case_id, "
        f"{node}.ruling_id, {node}.interp_id, {node}.rule_id, "
        f"{node}.amendment_id, {node}.annex_id, {node}.treaty_article_id)"
    )


def _path_node(node: str) -> str:
    return (
        f"{node} {{id: {_node_id(node)}, "
        f"label: head([label IN labels({node}) WHERE label IN "
        "['ArticleVersion', 'Article', 'Case', 'Ruling', 'Interpretation', "
        "'BasicRule', 'Amendment', 'Annex', 'TreatyArticle']]), "
        ".article_id, .version_id, .law_id, .law_name, .article_number, .source_snapshot}"
    )


def _path_relationship(rel: str, source: str, target: str) -> str:
    """Expose stored direction separately from the direction traversed by the query."""
    return (
        f"{rel} {{type: type({rel}), from_id: {_node_id(f'startNode({rel})')}, "
        f"to_id: {_node_id(f'endNode({rel})')}, "
        f"traversal_from_id: {_node_id(source)}, traversal_to_id: {_node_id(target)}, "
        ".active, .verified, .evidence, .source_snapshot, .target_source_snapshot, "
        ".upper_source_snapshot, .lower_source_snapshot, .parent_source_snapshot, "
        ".source_projection_schema, .source_projection_kind, .source_projection_sha256, "
        ".source_projection_snapshot, .temporal_resolution, .reference_semantics, "
        ".applicable_date, .resolved_version_id, .citation_role, .applicability_status}"
    )


def _root_path(seed: str = "seed") -> str:
    return (
        f"{{seed_article_id: {seed}.article_id, nodes: [{_path_node(seed)}], "
        "relationships: [], hops: 0}"
    )


def _extend_paths(*steps: tuple[str, str, str]) -> str:
    nodes = ", ".join(_path_node(target) for _, _, target in steps)
    rels = ", ".join(_path_relationship(*step) for step in steps)
    return (
        "[route IN source_paths | {seed_article_id: route.seed_article_id, "
        f"nodes: route.nodes + [{nodes}], relationships: route.relationships + [{rels}], "
        f"hops: route.hops + {len(steps)}}}]"
    )


_ARTICLE_FIELDS = (
    ".article_id, .article_number, .article_title, .article_content, .law_id, .law_name, "
    ".article_type, .source_snapshot, .enforcement_date, .promulgation_date, "
    ".is_current, .valid_from, .valid_to, .version_id, .applies_note, .applies_source"
)
_BODY_FIELDS = ".content, .full_content, .full_text, .body"
_DOCUMENT_FIELDS = (
    ".source_snapshot, .source_projection_schema, .source_projection_kind, "
    ".source_projection_sha256, .source_projection_snapshot, .citation_temporal_evidence"
)


def current_owned_article_guard(node: str) -> str:
    """Same fail-closed seed boundary for local and public integrated search."""
    return (
        f"{current_article_guard(node=node)} AND EXISTS {{ "
        f"MATCH (owner:Law)-[ownership:CONTAINS]->({node}) "
        "WHERE owner.is_current = true "
        "AND coalesce(owner.publication_status, '') <> 'historical' "
        "AND coalesce(owner.publication_status, '') <> 'scheduled' "
        f"AND {current_contains_guard(edge='ownership', law='owner')} }}"
    )


ARTICLE_OWNER_METADATA = f"""
MATCH (l:Law)-[owns:CONTAINS]->(a:Article)
WHERE a.article_id IN $article_ids AND {current_owned_article_guard('a')}
  AND l.is_current = true AND {current_contains_guard(edge='owns', law='l')}
RETURN a.article_id AS article_id, l.law_name AS law_name,
       l.enforcement_date AS enforcement_date, l.promulgation_date AS promulgation_date
"""

# ============================================================
# 1. 위임규정 탐색 (법 → 시행령 → 시행규칙, 최대 2 hop)
# ============================================================
TRAVERSE_DELEGATES_TO = f"""
MATCH (start:Article {{article_id: $article_id}})
MATCH path = (start)-[:DELEGATES_TO*1..2]->(delegated:Article)
WHERE all(rel IN relationships(path) WHERE {_DELEGATES_TO_RELS})
  AND {_PATH_NODES}
RETURN delegated {{
    _content_snapshot: delegated.source_snapshot,
    .article_id, .article_number, .article_title, .article_content,
    .law_id, .article_type, .source_snapshot
}} AS article,
length(path) AS hops,
'위임규정' AS edge_type
"""

# ============================================================
# 2. 상위법령 탐색 (시행규칙 → 시행령 → 법, 최대 2 hop)
# ============================================================
TRAVERSE_DELEGATED_FROM = f"""
MATCH (start:Article {{article_id: $article_id}})
MATCH path = (start)-[:DELEGATED_FROM*1..2]->(parent:Article)
WHERE all(rel IN relationships(path) WHERE {_DELEGATED_FROM_RELS})
  AND {_PATH_NODES}
RETURN parent {{
    _content_snapshot: parent.source_snapshot,
    .article_id, .article_number, .article_title, .article_content,
    .law_id, .article_type, .source_snapshot
}} AS article,
length(path) AS hops,
'상위법령' AS edge_type
"""

# ============================================================
# 3. 관련예규 탐색 (조문 → 예규, 1 hop)
# ============================================================
TRAVERSE_HAS_RULING = f"""
MATCH (a:Article {{article_id: $article_id}})-[rel:HAS_RULING]->(r:Ruling)
WHERE {_DOC_RULING}
  AND {_CURRENT_A}
  AND (NOT r:ReferenceBook OR r.active = true)
  AND (NOT r:AdminRule OR r.is_current = true)
RETURN r {{
    _content_snapshot: r.source_snapshot,
    .ruling_id, .ruling_number, .ruling_title, .ruling_org,
    .ruling_date, .query_summary, .answer_summary, .ruling_url,
    .source_doc_number, .department_doc_number,
    {_RULING_REL_MAP}
}} AS ruling,
rel.relevance_score AS relevance_score,
'관련예규' AS edge_type,
rel.temporal_resolution AS temporal_resolution,
rel.reference_semantics AS reference_semantics,
rel.evidence AS evidence,
rel.source_snapshot AS source_snapshot,
rel.target_source_snapshot AS target_source_snapshot,
rel.source_projection_schema AS source_projection_schema,
rel.source_projection_kind AS source_projection_kind,
rel.source_projection_sha256 AS source_projection_sha256,
rel.source_projection_snapshot AS source_projection_snapshot
ORDER BY r.ruling_date DESC
"""

# ============================================================
# 4. 인용판례 탐색 (조문 → 판례, 1 hop) — 인용 식별이지 적용 확정이 아님
# ============================================================
TRAVERSE_HAS_CASE = f"""
MATCH (a:Article {{article_id: $article_id}})-[rel:HAS_CASE]->(c:Case)
WHERE {_DOC_CASE}
  AND {_CURRENT_A}
RETURN c {{
    _content_snapshot: c.source_snapshot,
    .case_id, .case_number, .case_name, .court_name, .court_type,
    .ruling_date, .ruling_type, .case_holding, .ruling_summary, .case_url
}} AS case_data,
rel.relevance_score AS relevance_score,
'인용판례' AS edge_type,
rel.temporal_resolution AS temporal_resolution,
rel.reference_semantics AS reference_semantics,
rel.evidence AS evidence,
rel.source_snapshot AS source_snapshot,
rel.target_source_snapshot AS target_source_snapshot,
rel.source_projection_schema AS source_projection_schema,
rel.source_projection_kind AS source_projection_kind,
rel.source_projection_sha256 AS source_projection_sha256,
rel.source_projection_snapshot AS source_projection_snapshot,
rel.resolved_version_id AS resolved_version_id
ORDER BY c.ruling_date DESC
"""

# ============================================================
# 5. 참조조문 탐색 (양방향, 1 hop)
# ============================================================
TRAVERSE_REFERENCES = f"""
MATCH (a:Article {{article_id: $article_id}})-[r:REFERENCES]-(ref:Article)
WHERE {_ARTICLE_REFS}
  AND {_CURRENT_A}
  AND {_CURRENT_REF}
RETURN ref {{
    _content_snapshot: ref.source_snapshot,
    .article_id, .article_number, .article_title, .article_content,
    .law_id, .article_type, .source_snapshot
}} AS article,
r.reference_type AS reference_type,
'참조조문' AS edge_type,
r.temporal_resolution AS temporal_resolution,
r.reference_semantics AS reference_semantics,
r.evidence AS evidence,
r.source_snapshot AS source_snapshot,
r.target_source_snapshot AS target_source_snapshot
"""

# ============================================================
# 6. 기본통칙 탐색 (1 hop) — 공식 스냅샷·최신 발간본, 적용시점은 미확정
# ============================================================
TRAVERSE_HAS_BASIC_RULE = f"""
MATCH (a:Article {{article_id: $article_id}})-[r:HAS_BASIC_RULE]->(b:BasicRule)
MATCH (l:Law)
WHERE {_BASIC_RULE}
  AND {_CURRENT_A}
RETURN b {{
    .rule_id, .rule_number, .rule_title, .rule_content, .source_url,
    temporal_resolution: coalesce(r.temporal_resolution, b.temporal_resolution),
    edition_year: b.edition_year,
    reference_semantics: r.reference_semantics,
    evidence: coalesce(r.evidence, r.parser_rule),
    source_snapshot: r.source_snapshot,
    target_source_snapshot: r.target_source_snapshot,
    parent_source_snapshot: r.parent_source_snapshot,
    graph_path: 'HAS_BASIC_RULE edge'
}} AS basic_rule,
'기본통칙' AS edge_type,
coalesce(r.temporal_resolution, b.temporal_resolution) AS temporal_resolution,
b.edition_year AS edition_year,
r.source_snapshot AS source_snapshot,
r.target_source_snapshot AS target_source_snapshot,
r.parent_source_snapshot AS parent_source_snapshot
"""

# ============================================================
# 7. 유사쟁점 탐색 (예규/판례 → 유사 예규/판례, 양방향 1 hop)
# ============================================================
TRAVERSE_SIMILAR_RULING = """
MATCH (r:Ruling {ruling_id: $ruling_id})-[rel:SIMILAR_ISSUE]-(similar:Ruling)
RETURN similar {
    _content_snapshot: similar.source_snapshot,
    .ruling_id, .ruling_number, .ruling_title, .ruling_org,
    .ruling_date, .query_summary, .answer_summary
} AS ruling,
rel.similarity_score AS similarity_score,
'유사쟁점' AS edge_type
"""

TRAVERSE_SIMILAR_CASE = """
MATCH (c:Case {case_id: $case_id})-[rel:SIMILAR_ISSUE]-(similar:Case)
RETURN similar {
    _content_snapshot: similar.source_snapshot,
    .case_id, .case_number, .case_name, .court_name,
    .ruling_date, .case_holding, .ruling_summary
} AS case_data,
rel.similarity_score AS similarity_score,
'유사쟁점' AS edge_type
"""

# ============================================================
# 8. 조세심판원 결정례 전용 탐색
# ============================================================

# 조문 → 심판례 (court_type = '조세심판원'인 Case만 필터)
TRAVERSE_HAS_TRIBUNAL = f"""
MATCH (a:Article {{article_id: $article_id}})-[rel:HAS_CASE]->(c:Case)
WHERE {_DOC_CASE}
  AND {_CURRENT_A}
  AND c.court_type = '조세심판원'
RETURN c {{
    _content_snapshot: c.source_snapshot,
    .case_id, .case_number, .case_name, .court_name, .court_type,
    .ruling_date, .ruling_type, .case_holding, .ruling_summary,
    .full_content, .case_url
}} AS tribunal,
rel.relevance_score AS relevance_score,
'조세심판례' AS edge_type,
rel.temporal_resolution AS temporal_resolution,
rel.reference_semantics AS reference_semantics,
rel.evidence AS evidence,
rel.source_snapshot AS source_snapshot,
rel.target_source_snapshot AS target_source_snapshot,
rel.source_projection_schema AS source_projection_schema,
rel.source_projection_kind AS source_projection_kind,
rel.source_projection_sha256 AS source_projection_sha256,
rel.source_projection_snapshot AS source_projection_snapshot,
rel.resolved_version_id AS resolved_version_id
ORDER BY c.ruling_date DESC
"""

# 조세심판례 전문검색 (case_content_ft 인덱스 활용, court_type 필터)
FULLTEXT_SEARCH_TRIBUNALS = """
CALL db.index.fulltext.queryNodes('case_content_ft', $query)
YIELD node, score
WHERE score > $min_score AND node.court_type = '조세심판원'
RETURN node {
    _content_snapshot: node.source_snapshot,
    .case_id, .case_number, .case_name, .court_name, .court_type,
    .ruling_date, .ruling_type, .case_holding, .ruling_summary, .case_url
} AS tribunal, score
ORDER BY score DESC
LIMIT $limit
"""

# ============================================================
# 8b. 법령해석례 · 판례 인용 조문 · 적용시점 증거 (선택 조회)
# ============================================================
TRAVERSE_HAS_INTERPRETATION = f"""
MATCH (a:Article {{article_id: $article_id}})-[rel:HAS_INTERPRETATION]->(i:Interpretation)
WHERE {_DOC_INTERP}
  AND {_CURRENT_A}
RETURN i {{
    _content_snapshot: i.source_snapshot,
    .interp_id, .interp_number, .interp_title, .reply_date, .content,
    .full_text, .source_doc_number, .department_doc_number, .interp_url,
    {_INTERP_REL_MAP}
}} AS interpretation,
'법령해석례' AS edge_type,
rel.temporal_resolution AS temporal_resolution,
rel.reference_semantics AS reference_semantics,
rel.evidence AS evidence,
rel.source_snapshot AS source_snapshot,
rel.target_source_snapshot AS target_source_snapshot,
rel.source_projection_schema AS source_projection_schema,
rel.source_projection_kind AS source_projection_kind,
rel.source_projection_sha256 AS source_projection_sha256,
rel.source_projection_snapshot AS source_projection_snapshot
ORDER BY i.reply_date DESC
"""

TRAVERSE_CITES_ARTICLE = f"""
MATCH (c:Case {{case_id: $case_id}})-[rel:CITES_ARTICLE]->(a:Article)
WHERE {_DOC_CASE}
RETURN a {{
    _content_snapshot: a.source_snapshot,
    .article_id, .article_number, .article_title, .article_content,
    .law_id, .article_type, .source_snapshot
}} AS article,
'인용조문' AS edge_type,
rel.temporal_resolution AS temporal_resolution,
rel.reference_semantics AS reference_semantics,
rel.evidence AS evidence,
rel.source_snapshot AS source_snapshot,
rel.target_source_snapshot AS target_source_snapshot,
rel.source_projection_sha256 AS source_projection_sha256,
rel.resolved_version_id AS resolved_version_id
"""

# Article → Amendment. 적용례·경과조치 증거일 뿐, 적용일을 만들어 내지 않는다.
TRAVERSE_HAS_APPLICABILITY_EVIDENCE = f"""
MATCH (a:Article {{article_id: $article_id}})-[rel:HAS_APPLICABILITY_EVIDENCE]->(m:Amendment)
WHERE {_ARTICLE_REFS_REL}
  AND {_CURRENT_A}
RETURN m {{
    .amendment_id, .law_name, .promulgation_number, .promulgation_date,
    .enforcement_text, .application_text, .transitional_text,
    .clause_titles, .referenced_articles,
    temporal_resolution: coalesce(rel.temporal_resolution, 'conditions_unresolved'),
    reference_semantics: coalesce(rel.reference_semantics, 'applicability_evidence_only'),
    evidence: rel.evidence,
    source_snapshot: rel.source_snapshot,
    target_source_snapshot: rel.target_source_snapshot,
    graph_path: 'HAS_APPLICABILITY_EVIDENCE edge'
}} AS amendment,
'적용시점증거' AS edge_type,
coalesce(rel.temporal_resolution, 'conditions_unresolved') AS temporal_resolution,
coalesce(rel.reference_semantics, 'applicability_evidence_only') AS reference_semantics,
rel.evidence AS evidence,
rel.source_snapshot AS source_snapshot,
rel.target_source_snapshot AS target_source_snapshot
"""

# Amendment → Article. 부칙이 조문을 언급한 인용이지 현행 적용 확정이 아님.
TRAVERSE_AMENDMENT_REFERENCES = f"""
MATCH (m:Amendment {{amendment_id: $amendment_id}})-[rel:REFERENCES]->(a:Article)
WHERE {_ARTICLE_REFS_REL}
  AND {_CURRENT_A}
RETURN a {{
    _content_snapshot: a.source_snapshot,
    .article_id, .article_number, .article_title, .article_content,
    .law_id, .article_type, .source_snapshot
}} AS article,
m {{
    .amendment_id, .law_name, .promulgation_number, .promulgation_date
}} AS amendment,
'부칙참조조문' AS edge_type,
coalesce(rel.temporal_resolution, 'conditions_unresolved') AS temporal_resolution,
coalesce(rel.reference_semantics, 'applicability_evidence_only') AS reference_semantics,
rel.evidence AS evidence,
rel.source_snapshot AS source_snapshot,
rel.target_source_snapshot AS target_source_snapshot
"""

# 검증된 ArticleVersion 인용만. 현행 Article 본문으로 대체하지 않는다.
TRAVERSE_CITES_ARTICLE_VERSION = f"""
MATCH (cas:Case)-[rel:CITES_ARTICLE_VERSION]->(v:ArticleVersion)
WHERE cas.case_id = $case_id
  AND {_DOC_CASE_VERSION}
  AND rel.resolved_version_id = v.version_id
  AND coalesce(v.verified, false) = true
RETURN v {{
    .version_id, .article_id, .law_name, .article_title, .article_content,
    .valid_from, .valid_to, .enforcement_date, .applies_note, .applies_source
}} AS version,
cas.case_id AS case_id,
cas.case_number AS case_number,
'시점버전인용' AS edge_type,
rel.resolved_version_id AS resolved_version_id,
rel.temporal_resolution AS temporal_resolution,
rel.reference_semantics AS reference_semantics,
rel.evidence AS evidence,
rel.source_snapshot AS source_snapshot,
rel.target_source_snapshot AS target_source_snapshot,
rel.source_projection_sha256 AS source_projection_sha256
"""

# ============================================================
# 9. 통합 탐색 (source-search.md Step 1~5 한 번에 실행)
# ============================================================
INTEGRATED_SEARCH = f"""
// Step 1: Seed 조문 매칭
MATCH (seed:Article)
WHERE seed.article_id IN $seed_article_ids
  AND {current_owned_article_guard('seed')}

// Step 2: 위임규정 탐색 — 경로의 모든 엣지·노드가 가드를 통과해야 한다
OPTIONAL MATCH path = (seed)-[:DELEGATES_TO*1..2]->(delegated:Article)
WHERE all(rel IN relationships(path) WHERE {_DELEGATES_TO_RELS})
  AND {_PATH_NODES}

WITH seed, [d IN collect(DISTINCT delegated) WHERE d IS NOT NULL][..20] AS delegated_list,
     collect(DISTINCT CASE WHEN path IS NULL THEN null ELSE {{
         seed_article_id: seed.article_id,
         nodes: [n IN nodes(path) | {_path_node('n')}],
         relationships: [rel IN relationships(path) |
             {_path_relationship('rel', 'startNode(rel)', 'endNode(rel)')}],
         hops: length(path)
     }} END) AS delegation_paths
WITH seed, delegated_list,
     [d IN delegated_list | {{node: d, paths: [route IN delegation_paths
         WHERE last(route.nodes).id = d.article_id]}}] AS delegated_sources
WITH seed, delegated_list, delegated_sources,
     delegated_sources + [{{node: seed, paths: [{_root_path()}]}}] AS all_sources

// Step 3: 종류별로 독립 수집해 OPTIONAL MATCH 간 카테시안 곱을 방지한다.
UNWIND all_sources AS source_entry
WITH seed, delegated_list, delegated_sources, source_entry.node AS source, source_entry.paths AS source_paths
WITH seed, delegated_list, delegated_sources,
     collect([(source)-[rel:HAS_RULING]->(ruling:Ruling)
       WHERE {_DOC_RULING_SOURCE}
         AND {_CURRENT_SOURCE}
         AND (NOT ruling:ReferenceBook OR ruling.active = true)
         AND (NOT ruling:AdminRule OR ruling.is_current = true) |
       ruling {{_content_snapshot: ruling.source_snapshot, {_BODY_FIELDS},
               .ruling_id, .ruling_number, .ruling_title, .ruling_org,
               .ruling_date, .query_summary, .answer_summary, .ruling_url,
               .source_doc_number, .department_doc_number,
               {_RULING_REL_MAP}, graph_paths: {_extend_paths(('rel', 'source', 'ruling'))}}}][..25]) AS ruling_lists,
     collect([(source)-[rel:HAS_CASE]->(cas:Case)
       WHERE {_DOC_CASE_SOURCE}
         AND {_CURRENT_SOURCE} |
       cas {{_content_snapshot: cas.source_snapshot, {_BODY_FIELDS},
            .case_id, .case_number, .case_name, .court_name, .court_type,
            .ruling_date, .ruling_type, .case_holding, .ruling_summary, .case_url,
            {_CASE_REL_MAP}, graph_paths: {_extend_paths(('rel', 'source', 'cas'))}}}][..25]) AS case_lists,
     collect([(source)-[r:REFERENCES]-(ref:Article)
       WHERE {_ARTICLE_REFS}
         AND {_CURRENT_SOURCE}
         AND {_CURRENT_REF} |
       ref {{_content_snapshot: ref.source_snapshot, {_ARTICLE_FIELDS},
            temporal_resolution: r.temporal_resolution,
            reference_semantics: r.reference_semantics,
            evidence: r.evidence,
            source_snapshot: r.source_snapshot,
            target_source_snapshot: r.target_source_snapshot,
            graph_path: 'REFERENCES edge', graph_paths: {_extend_paths(('r', 'source', 'ref'))}}}][..20]) AS reference_lists,
     collect([(source)-[rel:HAS_INTERPRETATION]->(interp:Interpretation)
       WHERE {_DOC_INTERP_SOURCE}
         AND {_CURRENT_SOURCE} |
       interp {{_content_snapshot: interp.source_snapshot, .full_content, .body,
               .interp_id, .interp_number, .interp_title, .reply_date, .content,
               .full_text,
               .source_doc_number, .department_doc_number, .interp_url,
               {_INTERP_REL_MAP}, graph_paths: {_extend_paths(('rel', 'source', 'interp'))}}}][..25]) AS interpretation_lists,
     collect([(source)-[rel:HAS_BASIC_RULE]->(b:BasicRule)
       WHERE EXISTS {{
             MATCH (l:Law)
             WHERE {_BASIC_RULE_SOURCE}
           }}
         AND {_CURRENT_SOURCE} |
       b {{_content_snapshot: b.source_snapshot, .enforcement_date, .valid_from, .valid_to,
          .rule_id, .rule_number, .rule_title, .rule_content, .source_url,
          temporal_resolution: coalesce(rel.temporal_resolution, b.temporal_resolution),
          edition_year: b.edition_year,
          reference_semantics: rel.reference_semantics,
          evidence: coalesce(rel.evidence, rel.parser_rule),
          source_snapshot: rel.source_snapshot,
          target_source_snapshot: rel.target_source_snapshot,
          parent_source_snapshot: rel.parent_source_snapshot,
          graph_path: 'HAS_BASIC_RULE edge', graph_paths: {_extend_paths(('rel', 'source', 'b'))}}}][..25]) AS basic_rule_lists,
     collect([(source)-[rel:HAS_APPLICABILITY_EVIDENCE]->(m:Amendment)
       WHERE {_ARTICLE_REFS_REL}
         AND {_CURRENT_SOURCE} |
       m {{_content_snapshot: m.source_snapshot, {_BODY_FIELDS}, .enforcement_date,
          .amendment_id, .law_name, .promulgation_number, .promulgation_date,
          .enforcement_text, .application_text, .transitional_text,
          .clause_titles, .referenced_articles,
          temporal_resolution: coalesce(rel.temporal_resolution, 'conditions_unresolved'),
          reference_semantics: coalesce(rel.reference_semantics, 'applicability_evidence_only'),
          evidence: rel.evidence,
          source_snapshot: rel.source_snapshot,
          target_source_snapshot: rel.target_source_snapshot,
          graph_path: 'HAS_APPLICABILITY_EVIDENCE edge',
          graph_paths: {_extend_paths(('rel', 'source', 'm'))}}}][..10]) AS applicability_lists,
     collect([(source)-[version_rel:HAS_VERSION]->(v:ArticleVersion)<-[rel:CITES_ARTICLE_VERSION]-(cas:Case)
       WHERE {_DOC_CASE_VERSION}
         AND rel.resolved_version_id = v.version_id
         AND coalesce(v.verified, false) = true |
       {{case_id: cas.case_id, case_number: cas.case_number,
         resolved_version_id: rel.resolved_version_id,
         version_id: v.version_id,
         article_id: v.article_id, article_number: v.article_number,
         _content_snapshot: v.source_snapshot, verified: v.verified, source_verified: v.source_verified,
         applicable_date: rel.applicable_date,
         law_name: v.law_name,
         article_title: v.article_title,
         article_content: v.article_content,
         valid_from: v.valid_from,
         valid_to: v.valid_to,
         enforcement_date: v.enforcement_date,
         applies_note: v.applies_note,
         applies_source: v.applies_source,
         temporal_resolution: rel.temporal_resolution,
         reference_semantics: rel.reference_semantics,
         evidence: rel.evidence,
         source_snapshot: rel.source_snapshot,
         target_source_snapshot: rel.target_source_snapshot,
         source_projection_sha256: rel.source_projection_sha256,
         graph_path: 'CITES_ARTICLE_VERSION edge',
         graph_paths: {_extend_paths(('version_rel', 'source', 'v'), ('rel', 'v', 'cas'))}}}][..10]) AS cited_version_lists

RETURN seed {{_content_snapshot: seed.source_snapshot, {_ARTICLE_FIELDS},
              graph_path: 'seed article lookup', graph_paths: [{_root_path()}]}} AS seed_article,
       [d IN delegated_list | d {{_content_snapshot: d.source_snapshot,
          {_ARTICLE_FIELDS}, graph_path: 'DELEGATES_TO path',
          graph_paths: reduce(routes = [], entry IN delegated_sources |
              routes + CASE WHEN entry.node = d THEN entry.paths ELSE [] END)}}] AS delegated_articles,
       reduce(items = [], item_list IN ruling_lists | items + item_list)[..25] AS rulings,
       reduce(items = [], item_list IN case_lists | items + item_list)[..25] AS cases,
       reduce(items = [], item_list IN basic_rule_lists | items + item_list)[..25] AS basic_rules,
       reduce(items = [], item_list IN reference_lists | items + item_list)[..20] AS referenced_articles,
       reduce(items = [], item_list IN interpretation_lists | items + item_list)[..25] AS interpretations,
       reduce(items = [], item_list IN applicability_lists | items + item_list)[..10] AS applicability_evidence,
       reduce(items = [], item_list IN cited_version_lists | items + item_list)[..10] AS cited_versions
"""

# ============================================================
# 10. 전문검색 (Full-text index 활용)
# ============================================================
FULLTEXT_SEARCH_ARTICLES = f"""
CALL db.index.fulltext.queryNodes('article_content_ft', $query)
YIELD node, score
WHERE score > $min_score
  AND {current_owned_article_guard('node')}
RETURN node {{
    _content_snapshot: node.source_snapshot,
    .article_id, .article_number, .article_title, .article_content,
    .law_id, .article_type, .source_snapshot
}} AS article, score
ORDER BY score DESC, node.article_id
LIMIT $limit
"""
# ↑ is_current 방어선. find_seed_articles 에는 원래 있었는데 여기에만 빠져 있었다.
# 지금은 구버전 Law 에 Article 이 안 달려 있어 무해하지만, 과거 조문을 넣는 순간
# 2023년 조문과 2026년 조문이 같은 결과에 섞여 나오면서 **아무 경고도 안 난다.**
# 시점 조회는 아래 AS_OF 계열로만 한다.

# ============================================================
# 11. 시간 축 — as-of 조문 조회 · 개정 사건(부칙)
# ============================================================

# 조문의 특정 시점 본문. 키워드가 아니라 **날짜로 결정론적으로** 찍는다.
#
# 반환 형태를 세 갈래로 나눈 것이 요점이다.
#   found       — 그 시점 버전을 실제로 찾았다
#   before_data — 우리 시간축이 시작되기 전이다. **모른다.** 현행으로 대신 답하면 안 된다
#   no_version  — 그 조문에 버전 자체가 없다
# "없다"와 "확인 못 했다"를 코드 레벨에서 갈라 두지 않으면 모델이 현행 조문으로 메운다.
GET_ARTICLE_AS_OF = f"""
MATCH (a:Article {{article_id: $article_id}})
OPTIONAL MATCH (a)-[:HAS_VERSION]->(v:ArticleVersion)
WHERE v.article_id = a.article_id
WITH a, collect(DISTINCT v) AS versions
WITH a, versions,
     [x IN versions WHERE x.valid_from <= $as_of AND $as_of <= x.valid_to] AS hit,
     reduce(m = null, x IN versions |
        CASE WHEN m IS NULL OR x.valid_from < m THEN x.valid_from ELSE m END) AS earliest
WITH a, versions, earliest, hit,
     CASE WHEN size(hit) = 1 THEN hit[0] ELSE null END AS candidate
WITH a, versions, earliest, hit, candidate,
     CASE WHEN {verified_version_guard('candidate')} THEN candidate ELSE null END AS chosen
RETURN a.article_id AS article_id, a.article_number AS article_number,
  CASE WHEN size(hit) > 1 THEN 'ambiguous'
       WHEN chosen IS NOT NULL THEN 'found'
       WHEN candidate IS NOT NULL THEN 'unverified'
       WHEN size(versions) = 0 THEN 'no_version'
       WHEN $as_of < earliest THEN 'before_data'
       ELSE 'no_version' END AS status,
  earliest AS coverage_from,
  chosen.version_id AS version_id, chosen.article_title AS article_title,
  chosen.article_content AS article_content, chosen.valid_from AS valid_from,
  chosen.valid_to AS valid_to, chosen.enforcement_date AS enforcement_date,
  chosen.law_name AS law_name, chosen.is_prospective AS is_prospective,
  chosen.applies_note AS applies_note, chosen.applies_source AS applies_source,
  chosen.source_snapshot AS source_snapshot, chosen.verified AS verified,
  chosen.source_verified AS source_verified
"""

# 검색된 조문들의 버전 시간창 — 근거에 "이 본문이 어느 구간 것인지" 도장을 찍는 재료.
# 조문당 경계 목록만 가져온다 (본문 없음 — 가볍다).
# 키를 공백→하이픈으로 정규화하는 이유: 근거 id(law-소득세법-시행령_156_3)가
# 그 형태라서다. Article 은 수천 건이라 replace 스캔이어도 밀리초다.
GET_VERSION_WINDOWS = f"""
MATCH (a:Article)-[:HAS_VERSION]->(v:ArticleVersion)
WHERE replace(a.article_id, ' ', '-') IN $article_keys
  AND v.article_id = a.article_id AND {verified_version_guard()}
RETURN replace(a.article_id, ' ', '-') AS article_key,
       collect(v {{ .valid_from, .valid_to, .is_current }}) AS windows
"""

# 조문의 버전 이력 전체 — 본문까지 함께 가져온다.
#
# GET_ARTICLE_AS_OF 와 갈라지는 지점: 저쪽은 "한 시점에 무엇이 시행 중이었나"를
# 묻고 버전 하나를 고른다. 이쪽은 "구간마다 무엇이었나"를 묻고 전부 돌려준다.
# 연도별 이율·공제한도처럼 **현행 본문에는 없고 종전 본문에만 있는 수치**는
# 저쪽 경로로는 영원히 닿지 않는다(2026-08-31 세션 192 실측: 납부지연가산세
# 연도별 이자율 질문이 기준일 미특정이라 as-of 분기를 못 타고 유보로 끝났다).
GET_ARTICLE_VERSION_HISTORY = f"""
MATCH (a:Article {{article_id: $article_id}})-[:HAS_VERSION]->(v:ArticleVersion)
WHERE v.article_id = a.article_id AND {verified_version_guard()}
RETURN DISTINCT v.version_id        AS version_id,
       v.law_name          AS law_name,
       v.article_title     AS article_title,
       v.article_content   AS article_content,
       v.valid_from        AS valid_from,
       v.valid_to          AS valid_to,
       v.enforcement_date  AS enforcement_date,
       v.is_current        AS is_current,
       v.applies_note      AS applies_note,
       v.applies_source    AS applies_source,
       v.source_snapshot AS source_snapshot,
       v.verified AS verified, v.source_verified AS source_verified
ORDER BY v.valid_from DESC
LIMIT $limit
"""

# 경계 관련성 부칙 선정 — "이 법령의 최신 부칙"이 아니라 **검색된 조문을 언급하는**
# 부칙만 가져온다. 최신순 선정은 육아휴직수당 부칙이 분양권 검토에 실리는 결과를
# 낳았다(2026-08-27 실측). referenced_articles 는 개정 당시 번호라 힌트로만 쓰는
# 속성이지만, 부칙→조문 방향의 후보 축소에는 충분하다.
GET_AMENDMENTS_FOR_ARTICLES = """
MATCH (l:Law {law_name: $law_name})-[:HAS_AMENDMENT]->(m:Amendment)
WHERE m.promulgation_date >= $since
  AND (m.has_application_rule OR m.has_transitional)
  AND any(ref IN m.referenced_articles WHERE ref IN $refs)
WITH DISTINCT m
RETURN m {
    .amendment_id, .law_name, .promulgation_number, .promulgation_date,
    .enforcement_text, .application_text, .transitional_text,
    .clause_titles, .referenced_articles
} AS amendment
ORDER BY m.promulgation_date DESC
LIMIT $limit
"""

# 개정 사건(부칙) 검색 — **일반 검색과 완전히 분리된 통로**다.
#
# 왜 분리하는가: 소득세법 부칙에만 '적용례' 433회, '부터 적용한다' 522회가 나온다.
# 조문과 같은 인덱스에 넣으면 근거 예산(법령 12칸)을 부칙 조각이 잠식해
# 정작 봐야 할 조문이 밀려난다.
#
# since 기본값을 두는 이유: 소득세법 부칙 114개 중 대부분이 1990~2000년대 것이다.
# 시기를 안 자르면 30년 전 적용례가 현행 근거로 인용된다 — 노이즈가 아니라 오답이다.
SEARCH_AMENDMENTS = """
CALL db.index.fulltext.queryNodes('amendment_timing_ft', $query)
YIELD node, score
WHERE score > $min_score
  AND ($law_names IS NULL OR node.law_name IN $law_names)
  AND node.promulgation_date >= $since
RETURN node {
    .amendment_id, .law_name, .promulgation_number, .promulgation_date,
    .enforcement_text, .application_text, .transitional_text,
    .clause_titles, .referenced_articles,
    .has_application_rule, .has_transitional
} AS amendment, score
ORDER BY score DESC
LIMIT $limit
"""

# 특정 법령의 적용례·경과조치를 시기순으로. 5축(적용시점) 확인의 기본 통로.
GET_LAW_AMENDMENTS = """
MATCH (l:Law {law_name: $law_name})-[:HAS_AMENDMENT]->(m:Amendment)
WHERE m.promulgation_date >= $since
  AND ($only_timing = false OR m.has_application_rule OR m.has_transitional)
// 같은 법령명의 Law 노드가 버전별로 여러 개 남아 있어(cleanup_law_versions 는
// CONTAINS 만 정리한다) 같은 부칙이 여러 번 걸린다. DISTINCT 로 접는다.
WITH DISTINCT m
RETURN m {
    .amendment_id, .law_name, .promulgation_number, .promulgation_date,
    .enforcement_text, .application_text, .transitional_text,
    .clause_titles, .referenced_articles
} AS amendment
ORDER BY m.promulgation_date DESC
LIMIT $limit
"""

FULLTEXT_SEARCH_CASES = """
CALL db.index.fulltext.queryNodes('case_content_ft', $query)
YIELD node, score
WHERE score > $min_score
RETURN node {
    _content_snapshot: node.source_snapshot,
    .case_id, .case_number, .case_name, .court_name, .court_type,
    .ruling_date, .case_holding, .ruling_summary
} AS case_data, score
ORDER BY score DESC
LIMIT $limit
"""

FULLTEXT_SEARCH_RULINGS = """
CALL db.index.fulltext.queryNodes('ruling_content_ft', $query)
YIELD node, score
WHERE score > $min_score AND node.ruling_org IN $tax_orgs
  AND (NOT node:ReferenceBook OR node.active = true)
  AND (NOT node:AdminRule OR node.is_current = true)
RETURN node {
    _content_snapshot: node.source_snapshot,
    .ruling_id, .ruling_number, .ruling_title, .ruling_org,
    .ruling_date, .query_summary, .answer_summary,
    .source_doc_number, .department_doc_number
} AS ruling, score
ORDER BY score DESC
LIMIT $limit
"""

FULLTEXT_SEARCH_INTERPRETATIONS = """
CALL db.index.fulltext.queryNodes('interp_content_ft', $query)
YIELD node, score
WHERE score > $min_score
RETURN node {
    _content_snapshot: node.source_snapshot,
    .interp_id, .interp_number, .interp_title, .content, .full_text, .reply_date,
    .inquiry_org, .reply_org, .interp_url,
    .source_doc_number, .department_doc_number
} AS interpretation, score
ORDER BY score DESC
LIMIT $limit
"""

# 조세조약 조문 전문검색 — 국가명이 특정되면 country로 좁힌다.
# 한 나라에 원조약·개정의정서가 여럿이라(일본 5건) 키워드만으로는 다 걸린다.
# is_annex 제외: 부속 노드에는 폐기된 구협약 원문("최초협정(1970.10.29.)" 등)이
# 통째로 들어 있어, 걸리면 폐기된 세율이 현행 근거로 인용된다 (일본에서 실측).
# 발효일(effective_date)을 함께 반환해 근거에 시점 표기를 남긴다.
FULLTEXT_SEARCH_TREATY_ARTICLES = """
CALL db.index.fulltext.queryNodes('treaty_article_ft', $query)
YIELD node, score
WHERE score > $min_score
  AND ($country IS NULL OR node.country CONTAINS $country)
  AND NOT coalesce(node.is_annex, false)
MATCH (t:Treaty)-[:CONTAINS]->(node)
RETURN node {
    .treaty_article_id, .article_number, .article_title, .content, .country
} AS treaty_article,
       t.treaty_name AS treaty_name, t.treaty_type AS treaty_type,
       t.effective_date AS effective_date, score
ORDER BY score DESC
LIMIT $limit
"""

# 조약 체결국 목록 — search_intents의 treaty 키워드에서 국가명을 떼어낼 때 사용
LIST_TREATY_COUNTRIES = """
MATCH (t:Treaty)-[:CONTAINS]->(:TreatyArticle)
RETURN DISTINCT t.country AS country
"""

# ============================================================
# 11. 조문 직접 조회 (법령명 + 조번호)
# ============================================================
FIND_ARTICLE_BY_NAME = f"""
MATCH (l:Law)-[:CONTAINS]->(a:Article)
WHERE (l.law_name = $law_name OR l.law_name CONTAINS $law_name)
  AND l.is_current = true
  AND {current_owned_article_guard('a')}
  AND ($article_number = '' OR a.article_number = $article_number)
WITH a, l,
     CASE WHEN l.law_name = $law_name THEN 0 ELSE 1 END AS law_rank,
     CASE WHEN $article_number = '' OR a.article_number = $article_number THEN 0 ELSE 1 END AS article_rank
RETURN a {{
    _content_snapshot: a.source_snapshot,
    .article_id, .article_number, .article_title, .article_content,
    .law_id, .article_type, .source_snapshot,
    law_name: l.law_name, enforcement_date: l.enforcement_date
}} AS article,
l.law_name AS law_name
ORDER BY law_rank ASC, article_rank ASC, a.article_id ASC
"""


# ============================================================
# 12. 별표·서식 (Annex) — 조문이 위임한 세율표·계산서식·작성방법
# ============================================================
# 조문 인덱스(article_content_ft)와 분리된 전용 인덱스(annex_content_ft)를 쓴다.
# 서식 본문은 표 괘선이 대부분이라 조문 인덱스에 섞이면 점수만 흐린다.

# 법령명 + 번호("별표 4", "서식 20")로 바로 찍는다. 번호는 공백 차이를 흡수한다.
FIND_ANNEX_BY_NUMBER = """
MATCH (x:Annex)
WHERE x.law_name CONTAINS $law_name
  AND replace(x.annex_number, ' ', '') = replace($annex_number, ' ', '')
RETURN x {
    .annex_id, .law_name, .annex_type, .annex_number, .annex_branch,
    .annex_title, .content, .related_articles
} AS annex,
CASE WHEN x.law_name = $law_name THEN 0 ELSE 1 END AS law_rank
ORDER BY law_rank ASC, x.annex_branch ASC
LIMIT $limit
"""

# 번호를 모를 때 — 법령명이 잡히면 그 법령의 별표·서식으로 좁힌다.
# 같은 서식이 조특법 시행규칙에도 있어(감가상각비조정명세서 9의4) 법령 필터가
# 없으면 엉뚱한 법령의 서식이 점수 상위로 온다 (실측 2026-09-02).
FULLTEXT_SEARCH_ANNEXES = """
CALL db.index.fulltext.queryNodes('annex_content_ft', $query)
YIELD node, score
WHERE score > $min_score
  AND ($law_name IS NULL OR node.law_name CONTAINS $law_name)
RETURN node {
    .annex_id, .law_name, .annex_type, .annex_number, .annex_branch,
    .annex_title, .content, .related_articles
} AS annex, score
ORDER BY score DESC
LIMIT $limit
"""

# seed 조문이 직접 가리키는 별표·서식 (Article-[:HAS_ANNEX]->Annex).
# 세율·기준금액이 별표에 위임된 조문은 본문만으로는 숫자를 못 준다.
TRAVERSE_HAS_ANNEX = f"""
MATCH (a:Article)-[rel:HAS_ANNEX]->(x:Annex)
WHERE a.article_id IN $article_ids
  AND {_ARTICLE_ANNEX}
WITH x, min(a.article_id) AS from_article,
     collect(DISTINCT {{seed_article_id: a.article_id,
       nodes: [{_path_node('a')}, {_path_node('x')}],
       relationships: [{_path_relationship('rel', 'a', 'x')}], hops: 1}}) AS routes
RETURN x {{
    .annex_id, .law_name, .annex_type, .annex_number, .annex_branch,
    .annex_title, .content, .related_articles, .source_snapshot,
    _content_snapshot: x.source_snapshot, graph_paths: routes
}} AS annex, from_article
ORDER BY x.annex_number
LIMIT $limit
"""

# 제목에 검색어가 들어 있는 별표·서식 — 한국어 복합명사는 전문검색 토큰이
# 통째로 붙어("감가상각비조정명세서합계표") 부분 일치가 안 되므로, 제목 CONTAINS로
# 먼저 찾고 전문검색은 그 다음이다.
FIND_ANNEX_BY_TITLE_TERMS = """
MATCH (x:Annex)
WHERE ($law_name IS NULL OR x.law_name CONTAINS $law_name)
WITH x, size([t IN $terms WHERE x.annex_title CONTAINS t]) AS hits
WHERE hits > 0
RETURN x {
    .annex_id, .law_name, .annex_type, .annex_number, .annex_branch,
    .annex_title, .content, .related_articles
} AS annex, hits
ORDER BY hits DESC, size(x.annex_title) ASC, x.annex_number ASC
LIMIT $limit
"""

"""Graph DB 탐색기 — source-search.md 탐색 전략 구현."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from src.search import cypher_templates as cq

if TYPE_CHECKING:
    from src.db.neo4j_client import Neo4jClient


class _LoggingConsole:
    def print(self, message: str) -> None:
        logging.getLogger(__name__).info(message)


def _default_console():
    try:
        from rich.console import Console
    except ImportError:
        return _LoggingConsole()
    return Console()

# 조세 소관 부처 — 세무 판단의 근거가 될 수 있는 행정규칙의 발령기관.
# 행정안전부는 지방세 소관, 재정경제부는 기획재정부의 옛 명칭(과거 발령분).
TAX_RULING_ORGS = ["국세청", "기획재정부", "재정경제부", "관세청", "행정안전부"]

# 부칙 검색의 기본 하한. 소득세법 부칙 114개 중 대부분이 1990~2000년대 것이라,
# 시기를 안 자르면 30년 전 적용례가 현행 근거로 딸려온다.
# 과거 과세기간을 검토할 때는 호출부에서 그 시점에 맞춰 낮춰 부른다.
DEFAULT_AMENDMENT_SINCE = "20200101"


# Relationship provenance copied from Cypher rows onto node dictionaries.
# A cited law is not the version that applied to a case unless resolved_version_id
# is present from a verified CITES_ARTICLE_VERSION relation.
_PROVENANCE_FIELDS = (
    "temporal_resolution",
    "reference_semantics",
    "evidence",
    "source_snapshot",
    "target_source_snapshot",
    "source_projection_schema",
    "source_projection_kind",
    "source_projection_sha256",
    "source_projection_snapshot",
    "resolved_version_id",
    "edition_year",
    "parent_source_snapshot",
    "citation_role",
    "applicability_status",
    "lineage_kind",
    "does_not_imply_continued_applicability",
    "relevance_score",
    "edge_type",
    "graph_path",
    "graph_paths",
    "applicable_date",
    "hops",
    "reference_type",
)

_FULLTEXT_GRAPH_PATH = "fulltext search"


def _literal_fulltext_query(query: str) -> str:
    """Treat punctuation in generated search intents as text, not Lucene syntax."""
    return re.sub(r'([+\-!(){}\[\]^"~*?:\\/|&])', r'\\\1', query)


def _copy_provenance(payload: dict, row: dict, graph_path: str | None = None) -> dict:
    """Overlay relationship provenance onto a node map without dropping body fields."""
    merged = dict(payload)
    for key in _PROVENANCE_FIELDS:
        if key in row and row[key] is not None:
            merged[key] = row[key]
    if graph_path:
        merged.setdefault("graph_path", graph_path)
    return merged


def _append_unique_by(bucket: list[dict], item: dict | None, key: str) -> None:
    """Keep the first graph-derived row; later fulltext hits with the same id are skipped."""
    if not item:
        return
    ident = item.get(key)
    if ident:
        for existing in bucket:
            if existing.get(key) != ident:
                continue
            for path in item.get("graph_paths") or []:
                paths = existing.setdefault("graph_paths", [])
                if path not in paths:
                    paths.append(path)
            return
    elif item in bucket:
        return
    bucket.append(item)


def _resolved_version_payload(row: dict) -> dict:
    """Historical ArticleVersion body only — never the current Article node."""
    return {
        "version_id": row.get("version_id") or row.get("resolved_version_id"),
        "resolved_version_id": row.get("resolved_version_id") or row.get("version_id"),
        "law_name": row.get("law_name", ""),
        "article_title": row.get("article_title", ""),
        "article_content": row.get("article_content", ""),
        "valid_from": row.get("valid_from"),
        "valid_to": row.get("valid_to"),
        "enforcement_date": row.get("enforcement_date"),
        "applies_note": row.get("applies_note") or "",
        "applies_source": row.get("applies_source") or "",
        "temporal_resolution": row.get("temporal_resolution"),
        "reference_semantics": row.get("reference_semantics"),
        "evidence": row.get("evidence"),
        "source_snapshot": row.get("source_snapshot"),
        "target_source_snapshot": row.get("target_source_snapshot"),
        "source_projection_sha256": row.get("source_projection_sha256"),
        "graph_path": row.get("graph_path", "CITES_ARTICLE_VERSION edge"),
        "graph_paths": row.get("graph_paths") or [],
        "article_id": row.get("article_id"),
        "article_number": row.get("article_number"),
        "verified": row.get("verified"),
        "source_verified": row.get("source_verified"),
        "applicable_date": row.get("applicable_date"),
        "_content_snapshot": row.get("_content_snapshot"),
    }


@dataclass
class SearchResult:
    """통합 검색 결과."""
    seed_articles: list[dict] = field(default_factory=list)
    delegated_articles: list[dict] = field(default_factory=list)
    rulings: list[dict] = field(default_factory=list)
    cases: list[dict] = field(default_factory=list)
    tribunals: list[dict] = field(default_factory=list)
    basic_rules: list[dict] = field(default_factory=list)
    referenced_articles: list[dict] = field(default_factory=list)
    interpretations: list[dict] = field(default_factory=list)
    amendments: list[dict] = field(default_factory=list)
    treaty_articles: list[dict] = field(default_factory=list)
    # 별표·서식 — 조문이 위임한 세율표·계산서식과 그 작성방법
    annexes: list[dict] = field(default_factory=list)
    # 부칙 적용례 증거(HAS_APPLICABILITY_EVIDENCE). 적용일을 만들지 않는다.
    applicability_evidence: list[dict] = field(default_factory=list)
    # 검증된 ArticleVersion 인용. 현행 조문 본문이 아니다.
    cited_versions: list[dict] = field(default_factory=list)

    @property
    def total_count(self) -> int:
        return (
            len(self.seed_articles)
            + len(self.delegated_articles)
            + len(self.rulings)
            + len(self.cases)
            + len(self.tribunals)
            + len(self.basic_rules)
            + len(self.referenced_articles)
            + len(self.interpretations)
            + len(self.amendments)
            + len(self.treaty_articles)
            + len(self.annexes)
            + len(self.applicability_evidence)
            + len(self.cited_versions)
        )

    def to_summary(self) -> dict:
        """source-search.md의 search_summary 형식으로 변환."""
        return {
            "total_found": self.total_count,
            "by_type": {
                "law": len(self.seed_articles) + len(self.delegated_articles) + len(self.referenced_articles),
                "ruling": len(self.rulings),
                "case": len(self.cases),
                "tribunal": len(self.tribunals),
                "basic_rule": len(self.basic_rules),
                "interpretation": len(self.interpretations),
                "amendment": len(self.amendments),
                "treaty": len(self.treaty_articles),
                "annex": len(self.annexes),
                "applicability_evidence": len(self.applicability_evidence),
                "cited_version": len(self.cited_versions),
            },
        }


class GraphSearcher:
    """source-search.md의 탐색 전략을 구현하는 검색기."""

    def __init__(self, client: Neo4jClient, *, console=None):
        self.client = client
        self.console = console if console is not None else _default_console()
        self._has_basic_rule_support = self._detect_basic_rule_support()

    # ========================================================
    # Step 1: Seed 조문 검색
    # ========================================================

    def find_seed_articles(self, search_intents: dict) -> list[dict]:
        """search_intents.law에서 시작 조문(seed node) 탐색.

        Args:
            search_intents: {"law": ["소득세법 제20조", ...], "ruling": [...], "case": [...]}

        Returns:
            매칭된 Article 노드 리스트
        """
        seeds = []
        seen = set()

        for intent in search_intents.get("law", []):
            # "소득세법 제20조" → law_name="소득세법", article_number="제20조"
            law_name, article_number = self._parse_law_intent(intent)
            if not law_name:
                continue

            if article_number:
                results = self.client.execute_query(
                    f"""
                    MATCH (l:Law)-[:CONTAINS]->(a:Article)
                    WHERE l.law_name = $law_name
                      AND l.is_current = true
                      AND {cq.current_owned_article_guard('a')}
                      AND a.article_number = $article_number
                    RETURN a {{
                        .article_id, .article_number, .article_title, .article_content,
                        .law_id, .article_type, _content_snapshot: a.source_snapshot,
                        law_name: l.law_name, enforcement_date: l.enforcement_date
                    }} AS article,
                    l.law_name AS law_name
                    ORDER BY a.article_id
                    """,
                    {"law_name": law_name, "article_number": article_number},
                )
                if not results:
                    results = self.client.execute_query(
                        cq.FIND_ARTICLE_BY_NAME,
                        {"law_name": law_name, "article_number": article_number},
                    )
            else:
                results = self.client.execute_query(
                    cq.FIND_ARTICLE_BY_NAME,
                    {"law_name": law_name, "article_number": article_number},
                )

            if not results:
                # 조문번호 없는 키워드형 intent(예: "국제조세조정에 관한 법률
                # 글로벌최저한세 적용대상")는 법령명 정확 일치에 실패한다.
                # 전문검색으로 관련 조문을 seed로 확보하는 안전망.
                results = self.fulltext_search_articles(intent, limit=5)

            for r in results:
                article = r.get("article")
                if article and article["article_id"] not in seen:
                    seen.add(article["article_id"])
                    seeds.append(article)

        self.console.print(f"[bold]Seed 조문:[/] {len(seeds)}개 매칭")
        return seeds

    # ========================================================
    # Step 2~5: 통합 탐색
    # ========================================================

    def integrated_search(self, seed_article_ids: list[str]) -> SearchResult:
        """seed 조문에서 모든 Edge를 따라 통합 탐색.

        source-search.md의 Step 2~5를 한 번의 Cypher 쿼리로 실행.
        """
        if not seed_article_ids:
            return SearchResult()

        results = self.client.execute_query(
            cq.INTEGRATED_SEARCH,
            {"seed_article_ids": seed_article_ids},
        )

        search_result = SearchResult()

        for row in results:
            seed = row.get("seed_article")
            if seed:
                _append_unique_by(search_result.seed_articles, seed, "article_id")

            for a in row.get("delegated_articles", []):
                _append_unique_by(search_result.delegated_articles, a, "article_id")

            for r in row.get("rulings", []):
                _append_unique_by(search_result.rulings, r, "ruling_id")

            for c in row.get("cases", []):
                if not c:
                    continue
                # 불복 결정례(조세심판원·국세청)와 법원 판례 분리
                if c.get("court_type") in cq.DECISION_COURT_TYPES:
                    _append_unique_by(search_result.tribunals, c, "case_id")
                else:
                    _append_unique_by(search_result.cases, c, "case_id")

            for b in row.get("basic_rules", []):
                _append_unique_by(search_result.basic_rules, b, "rule_id")

            for ref in row.get("referenced_articles", []):
                _append_unique_by(search_result.referenced_articles, ref, "article_id")

            for interp in row.get("interpretations", []):
                _append_unique_by(search_result.interpretations, interp, "interp_id")

            for amendment in row.get("applicability_evidence", []):
                _append_unique_by(
                    search_result.applicability_evidence, amendment, "amendment_id"
                )

            for cited in row.get("cited_versions", []):
                if not cited:
                    continue
                payload = _resolved_version_payload(cited)
                payload["case_id"] = cited.get("case_id")
                payload["case_number"] = cited.get("case_number")
                _append_unique_by(
                    search_result.cited_versions, payload, "resolved_version_id"
                )
                case_id = cited.get("case_id")
                for bucket in (search_result.cases, search_result.tribunals):
                    for case in bucket:
                        if case.get("case_id") != case_id:
                            continue
                        versions = case.setdefault("resolved_versions", [])
                        _append_unique_by(versions, payload, "resolved_version_id")
                        case["resolved_version_id"] = versions[0]["resolved_version_id"] if len(versions) == 1 else None
                        case["resolved_version"] = versions[0] if len(versions) == 1 else None

        articles = (search_result.seed_articles + search_result.delegated_articles
                    + search_result.referenced_articles)
        ids = list(dict.fromkeys(a["article_id"] for a in articles))
        if ids:
            owners = {r["article_id"]: r for r in self.client.execute_query(
                cq.ARTICLE_OWNER_METADATA, {"article_ids": ids}) if r.get("article_id")}
            for article in articles:
                owner = owners.get(article["article_id"], {})
                for key in ("law_name", "enforcement_date", "promulgation_date"):
                    if owner.get(key) and not article.get(key):
                        article[key] = owner[key]

        self.console.print(f"[bold]통합 탐색 결과:[/] {search_result.to_summary()}")
        return search_result

    # ========================================================
    # 개별 Edge 탐색 (선택적 상세 탐색용)
    # ========================================================

    def traverse_delegates_to(self, article_id: str) -> list[dict]:
        """위임규정 탐색."""
        return self.client.execute_query(
            cq.TRAVERSE_DELEGATES_TO, {"article_id": article_id}
        )

    def traverse_delegated_from(self, article_id: str) -> list[dict]:
        """상위법령 탐색."""
        return self.client.execute_query(
            cq.TRAVERSE_DELEGATED_FROM, {"article_id": article_id}
        )

    def traverse_has_ruling(self, article_id: str) -> list[dict]:
        """관련예규 탐색."""
        rows = self.client.execute_query(
            cq.TRAVERSE_HAS_RULING, {"article_id": article_id}
        )
        return [
            _copy_provenance(row.get("ruling") or {}, row, "HAS_RULING edge")
            for row in rows
        ]

    def traverse_has_case(self, article_id: str) -> list[dict]:
        """인용판례 탐색 — 인용 식별이지 적용 법령 버전 확정이 아니다."""
        rows = self.client.execute_query(
            cq.TRAVERSE_HAS_CASE, {"article_id": article_id}
        )
        return [
            _copy_provenance(row.get("case_data") or {}, row, "HAS_CASE edge")
            for row in rows
        ]

    def traverse_references(self, article_id: str) -> list[dict]:
        """참조조문 탐색."""
        rows = self.client.execute_query(
            cq.TRAVERSE_REFERENCES, {"article_id": article_id}
        )
        return [
            _copy_provenance(row.get("article") or {}, row, "REFERENCES edge")
            for row in rows
        ]

    def traverse_basic_rules(self, article_id: str) -> list[dict]:
        """기본통칙 탐색."""
        if not self._has_basic_rule_support:
            return []
        rows = self.client.execute_query(
            cq.TRAVERSE_HAS_BASIC_RULE, {"article_id": article_id}
        )
        return [
            _copy_provenance(row.get("basic_rule") or {}, row, "HAS_BASIC_RULE edge")
            for row in rows
        ]

    def traverse_has_tribunal(self, article_id: str) -> list[dict]:
        """조세심판원 결정례 탐색."""
        rows = self.client.execute_query(
            cq.TRAVERSE_HAS_TRIBUNAL, {"article_id": article_id}
        )
        return [
            _copy_provenance(
                row.get("tribunal") or {}, row, "HAS_CASE edge (court_type='조세심판원')"
            )
            for row in rows
        ]

    def traverse_has_interpretation(self, article_id: str) -> list[dict]:
        """법령해석례 탐색."""
        rows = self.client.execute_query(
            cq.TRAVERSE_HAS_INTERPRETATION, {"article_id": article_id}
        )
        return [
            _copy_provenance(row.get("interpretation") or {}, row, "HAS_INTERPRETATION edge")
            for row in rows
        ]

    def traverse_cites_article(self, case_id: str) -> list[dict]:
        """판례가 인용한 조문 — 인용 식별, 적용 버전 아님."""
        rows = self.client.execute_query(
            cq.TRAVERSE_CITES_ARTICLE, {"case_id": case_id}
        )
        return [
            _copy_provenance(row.get("article") or {}, row, "CITES_ARTICLE edge")
            for row in rows
        ]

    def traverse_applicability_evidence(self, article_id: str) -> list[dict]:
        """부칙 적용례·경과조치 증거. 적용일을 만들지 않고 conditions_unresolved를 보존한다."""
        rows = self.client.execute_query(
            cq.TRAVERSE_HAS_APPLICABILITY_EVIDENCE, {"article_id": article_id}
        )
        return [
            _copy_provenance(
                row.get("amendment") or {}, row, "HAS_APPLICABILITY_EVIDENCE edge"
            )
            for row in rows
        ]

    def traverse_amendment_references(self, amendment_id: str) -> list[dict]:
        """부칙이 언급한 조문. 현행 적용 확정이 아니다."""
        rows = self.client.execute_query(
            cq.TRAVERSE_AMENDMENT_REFERENCES, {"amendment_id": amendment_id}
        )
        return [
            _copy_provenance(row.get("article") or {}, row, "REFERENCES edge")
            for row in rows
        ]

    def traverse_cites_article_version(self, case_id: str) -> list[dict]:
        """검증된 ArticleVersion 인용만. 현행 조문 본문으로 대체하지 않는다."""
        rows = self.client.execute_query(
            cq.TRAVERSE_CITES_ARTICLE_VERSION, {"case_id": case_id}
        )
        out = []
        for row in rows:
            nested = row.get("version") or {}
            payload = _resolved_version_payload({**nested, **row})
            payload["case_id"] = row.get("case_id")
            payload["case_number"] = row.get("case_number")
            if nested.get("article_content"):
                payload["article_content"] = nested["article_content"]
            out.append(
                _copy_provenance(payload, row, "CITES_ARTICLE_VERSION edge")
            )
        return out

    def traverse_similar_rulings(self, ruling_id: str) -> list[dict]:
        """유사쟁점 예규 탐색."""
        return self.client.execute_query(
            cq.TRAVERSE_SIMILAR_RULING, {"ruling_id": ruling_id}
        )

    def traverse_similar_cases(self, case_id: str) -> list[dict]:
        """유사쟁점 판례 탐색."""
        return self.client.execute_query(
            cq.TRAVERSE_SIMILAR_CASE, {"case_id": case_id}
        )

    # ========================================================
    # 전문검색 (Full-text)
    # ========================================================

    def fulltext_search_articles(
        self, query: str, min_score: float = 0.5, limit: int = 20
    ) -> list[dict]:
        """조문 전문검색."""
        return self.client.execute_query(
            cq.FULLTEXT_SEARCH_ARTICLES,
            {"query": _literal_fulltext_query(query), "min_score": min_score, "limit": limit},
        )

    # ========================================================
    # 시간 축 — as-of 조문 · 개정 사건(부칙)
    # ========================================================

    def get_article_as_of(self, article_id: str, as_of: str) -> dict:
        """그 시점에 시행 중이던 조문 본문을 돌려준다.

        Args:
            article_id: 조문 식별자 (예: "소득세법_020")
            as_of: 기준일 YYYYMMDD 또는 YYYY-MM-DD

        Returns:
            status 가 셋 중 하나다.
              found       — 그 시점 버전을 찾음. version 에 본문이 있다
              before_data — 시간축 시작 이전. **확인 불가**로 답해야 한다
              no_version  — 그 조문의 버전이 없다

        모델이 판단하지 않는다. 날짜를 주면 DB 가 결정론적으로 찍는다.
        "그 시점에 어땠을지"를 추론하게 두면 현행 조문으로 메운다.
        """
        normalized = str(as_of).replace("-", "").strip()
        rows = self.client.execute_query(
            cq.GET_ARTICLE_AS_OF, {"article_id": article_id, "as_of": normalized}
        )
        if not rows:
            return {
                "article_id": article_id,
                "as_of": normalized,
                "status": "not_found",
                "coverage_from": None,
                "version": None,
            }

        row = rows[0]
        version = None
        if row["status"] == "found":
            version = {
                "version_id": row["version_id"],
                "source_snapshot": row.get("source_snapshot"),
                "verified": row.get("verified"),
                "source_verified": row.get("source_verified"),
                "law_name": row["law_name"],
                "article_number": row["article_number"],
                "article_title": row["article_title"],
                "article_content": row["article_content"],
                "valid_from": row["valid_from"],
                "valid_to": row["valid_to"],
                "enforcement_date": row["enforcement_date"],
                "is_prospective": row["is_prospective"],
                # 적재 시 부칙에서 압축해 둔 "이 버전은 어떤 분부터 적용되는가"
                # (scripts/annotate_version_rules.py). 없으면 빈 문자열.
                "applies_note": row.get("applies_note") or "",
                "applies_source": row.get("applies_source") or "",
            }
        return {
            "article_id": row["article_id"],
            "article_number": row["article_number"],
            "as_of": normalized,
            "status": row["status"],
            "coverage_from": row["coverage_from"],
            "version": version,
        }

    def timeline_coverage(self) -> str | None:
        """조문 시점 버전이 커버하는 가장 이른 날짜 (YYYYMMDD).

        이 날짜 이전 과세기간은 **답할 수 없다.** 현행 조문으로 대신 답하면
        소급 적용한 셈이 되므로, 파이프라인이 이 값을 보고 명시적으로 물러선다.
        """
        rows = self.client.execute_query(
            f"MATCH (v:ArticleVersion) WHERE {cq.verified_version_guard()} RETURN min(v.valid_from) AS earliest"
        )
        return rows[0]["earliest"] if rows else None

    def _resolve_article_ids(self, law_intents: list[str]) -> list[tuple[str, str, str]]:
        """검색 의도(법령명 + 조문번호) → (article_id, law_name, intent) 목록.

        조문번호가 없는 의도(키워드형)는 건너뛴다 — 어느 조문인지 특정되지 않으면
        시점·연혁 조회의 대상이 아니다. 같은 조문이 여러 의도에 걸리면 한 번만 돌려준다.
        """
        seen: set[str] = set()
        out: list[tuple[str, str, str]] = []

        for intent in law_intents or []:
            law_name, article_number = self._parse_law_intent(intent)
            if not law_name or not article_number:
                continue

            rows = self.client.execute_query(
                """
                MATCH (l:Law)-[:CONTAINS]->(a:Article)
                WHERE l.law_name = $law_name
                  AND coalesce(l.is_current, true) = true
                  AND a.article_number = $article_number
                RETURN a.article_id AS article_id
                """,
                {"law_name": law_name, "article_number": article_number},
            )
            for row in rows:
                article_id = row["article_id"]
                if article_id in seen:
                    continue
                seen.add(article_id)
                out.append((article_id, law_name, intent))

        return out

    def get_articles_as_of(self, law_intents: list[str], as_of: str) -> list[dict]:
        """검색 의도(법령명 + 조문번호)를 그 시점 조문 버전으로 해석한다."""
        normalized = str(as_of).replace("-", "").strip()
        out: list[dict] = []

        for article_id, law_name, intent in self._resolve_article_ids(law_intents):
            result = self.get_article_as_of(article_id, normalized)
            result["law_name"] = law_name
            result["intent"] = intent
            out.append(result)

        return out

    def get_article_history(self, article_id: str, limit: int = 12) -> list[dict]:
        """그 조문이 거쳐온 버전 전부를 본문과 함께 최신순으로 돌려준다.

        as-of 조회와 달리 기준일이 필요 없다. "연도별로 얼마였나"는 시점이 하나가
        아니라 구간의 나열이라, 기준일 하나를 요구하는 경로로는 표현되지 않는다.
        """
        return self.client.execute_query(
            cq.GET_ARTICLE_VERSION_HISTORY,
            {"article_id": article_id, "limit": limit},
        )

    def get_article_histories(
        self,
        law_intents: list[str],
        *,
        max_articles: int = 2,
        max_versions: int = 12,
    ) -> list[dict]:
        """검색 의도로 특정된 조문들의 버전 이력.

        조문 수를 좁게 막는다(기본 2건). 연혁은 조문 하나가 버전 7~10개를 끌고
        오므로, 조문을 넉넉히 잡으면 근거 예산이 연혁으로만 찬다. 열거형 질문은
        보통 조문 한둘을 지목하므로 이 상한으로 충분하다.
        """
        out: list[dict] = []

        for article_id, law_name, intent in self._resolve_article_ids(law_intents):
            if len(out) >= max_articles:
                break
            versions = self.get_article_history(article_id, limit=max_versions)
            if not versions:
                continue
            out.append(
                {
                    "article_id": article_id,
                    "law_name": law_name,
                    "intent": intent,
                    "versions": versions,
                }
            )

        return out

    def search_amendments(
        self,
        query: str,
        law_names: list[str] | None = None,
        since: str = DEFAULT_AMENDMENT_SINCE,
        min_score: float = 0.5,
        limit: int = 5,
    ) -> list[dict]:
        """부칙(적용례·경과조치) 전용 검색 — 일반 조문 검색과 섞지 않는다.

        since 기본값이 있는 이유는 오래된 부칙을 자동으로 배제하기 위해서다.
        과거 과세기간을 검토할 때는 그 시점에 맞춰 since 를 낮춰 부른다.
        """
        return self.client.execute_query(
            cq.SEARCH_AMENDMENTS,
            {
                "query": _literal_fulltext_query(query),
                "law_names": law_names,
                "since": str(since).replace("-", ""),
                "min_score": min_score,
                "limit": limit,
            },
        )

    def get_amendments_for_articles(
        self,
        law_name: str,
        article_numbers: list[str],
        since: str = DEFAULT_AMENDMENT_SINCE,
        limit: int = 3,
    ) -> list[dict]:
        """검색된 조문을 실제로 언급하는 부칙만 — 경계 관련성 선정.

        법령명+최신순 선정(get_law_amendments)은 검토와 무관한 부칙을 실었다
        (실측: 분양권 검토에 육아휴직수당 부칙). 조문번호("제156조의3" 또는
        "156의3" 형식 모두 수용)로 좁혀서, 그 조문의 적용례·경과조치를 가진
        부칙만 돌려준다. 결과가 0건이면 0건이 정직한 답이다 — 최신 부칙으로
        메우지 않는다.
        """
        import re

        refs = []
        for number in article_numbers or []:
            m = re.match(r"(?:제\s*)?(\d+)\s*조?(?:\s*의\s*(\d+))?", str(number).strip())
            if m:
                ref = m.group(1) + (f"의{m.group(2)}" if m.group(2) else "")
                if ref not in refs:
                    refs.append(ref)
        if not refs:
            return []
        return self.client.execute_query(
            cq.GET_AMENDMENTS_FOR_ARTICLES,
            {
                "law_name": law_name,
                "refs": refs,
                "since": str(since).replace("-", ""),
                "limit": limit,
            },
        )

    def get_current_articles(
        self, law_name: str, article_numbers: list[str]
    ) -> list[dict]:
        """같은 법령의 현행 조문을 조문번호 목록으로 한 번에 가져온다.

        일몰 경과 조문의 후속 조문 후보(제29조의7 → 제29조의8 …)를 찾는 데 쓴다.
        """
        if not law_name or not article_numbers:
            return []
        rows = self.client.execute_query(
            """
            MATCH (l:Law)-[:CONTAINS]->(a:Article)
            WHERE l.law_name = $law_name
              AND coalesce(l.is_current, true) = true
              AND a.article_number IN $numbers
            RETURN a {
                .article_id, .article_number, .article_title, .article_content,
                .law_id, .article_type
            } AS article
            ORDER BY a.article_id
            """,
            {"law_name": law_name, "numbers": list(article_numbers)},
        )
        return [r["article"] for r in rows if r.get("article")]

    def get_version_windows(self, article_keys: list[str]) -> dict[str, list[dict]]:
        """조문들의 버전 시간창 목록 — 근거 도장용. 한 번의 배치 조회.

        키는 근거 id 형식(공백→하이픈, 예: "소득세법-시행령_156_3").
        {key: [{valid_from, valid_to, is_current}, ...]} (valid_from 오름차순)
        """
        if not article_keys:
            return {}
        rows = self.client.execute_query(
            cq.GET_VERSION_WINDOWS, {"article_keys": list(article_keys)}
        )
        return {
            row["article_key"]: sorted(row["windows"], key=lambda w: w["valid_from"])
            for row in rows
        }

    def get_law_amendments(
        self,
        law_name: str,
        since: str = DEFAULT_AMENDMENT_SINCE,
        only_timing: bool = True,
        limit: int = 5,
    ) -> list[dict]:
        """한 법령의 적용례·경과조치를 최근 것부터. 5축 확인의 기본 통로."""
        return self.client.execute_query(
            cq.GET_LAW_AMENDMENTS,
            {
                "law_name": law_name,
                "since": str(since).replace("-", ""),
                "only_timing": only_timing,
                "limit": limit,
            },
        )

    def fulltext_search_cases(
        self, query: str, min_score: float = 0.5, limit: int = 20
    ) -> list[dict]:
        """판례 전문검색."""
        return self.client.execute_query(
            cq.FULLTEXT_SEARCH_CASES,
            {"query": _literal_fulltext_query(query), "min_score": min_score, "limit": limit},
        )

    def fulltext_search_rulings(
        self, query: str, min_score: float = 0.5, limit: int = 20
    ) -> list[dict]:
        """예규 전문검색 — 조세 소관 부처 발령분으로 한정.

        Ruling에는 전 부처 행정규칙이 적재되어 있어(98%가 비조세 부처)
        제한 없이 검색하면 「국가유산청 정책연구 관리규정」 같은 문서가
        세법 근거 자리를 차지한다. 세무 판단에 인용할 수 있는 것은
        조세 소관 부처가 발령한 것뿐이다.
        """
        return self.client.execute_query(
            cq.FULLTEXT_SEARCH_RULINGS,
            {
                "query": _literal_fulltext_query(query),
                "min_score": min_score,
                "limit": limit,
                "tax_orgs": TAX_RULING_ORGS,
            },
        )

    def fulltext_search_interpretations(
        self, query: str, min_score: float = 0.5, limit: int = 20
    ) -> list[dict]:
        """국세청 질의회신(유권해석) 전문검색."""
        return self.client.execute_query(
            cq.FULLTEXT_SEARCH_INTERPRETATIONS,
            {"query": _literal_fulltext_query(query), "min_score": min_score, "limit": limit},
        )

    def fulltext_search_tribunals(
        self, query: str, min_score: float = 0.5, limit: int = 20
    ) -> list[dict]:
        """조세심판원 결정례 전문검색."""
        return self.client.execute_query(
            cq.FULLTEXT_SEARCH_TRIBUNALS,
            {"query": _literal_fulltext_query(query), "min_score": min_score, "limit": limit},
        )

    def fulltext_search_treaty_articles(
        self,
        query: str,
        country: str | None = None,
        min_score: float = 0.5,
        limit: int = 8,
    ) -> list[dict]:
        """조세조약 조문 전문검색 — 국가명이 있으면 그 나라 조약으로 좁힌다."""
        return self.client.execute_query(
            cq.FULLTEXT_SEARCH_TREATY_ARTICLES,
            {"query": _literal_fulltext_query(query), "country": country, "min_score": min_score, "limit": limit},
        )

    def _treaty_countries(self) -> list[str]:
        """본문이 적재된 조약 체결국 목록 (캐시)."""
        if not hasattr(self, "_treaty_country_cache"):
            rows = self.client.execute_query(cq.LIST_TREATY_COUNTRIES, {})
            self._treaty_country_cache = [
                r["country"] for r in rows if r.get("country")
            ]
        return self._treaty_country_cache

    def _split_treaty_intent(self, intent: str) -> tuple[str | None, str]:
        """treaty 검색 의도에서 국가명을 떼어낸다.

        "일본 사용료 제한세율" → ("일본", "사용료 제한세율")
        국가명 표기가 DB와 다르면(미체결 포함) (None, 원문) — 전문검색만 돈다.
        '키르기스스탄(키르기즈)'처럼 병기된 국가는 괄호 안팎 각각을 후보로 본다.
        """
        for stored in self._treaty_countries():
            candidates = [stored]
            if "(" in stored:
                head, _, tail = stored.partition("(")
                candidates = [head.strip(), tail.rstrip(")").strip()]
            for cand in candidates:
                if cand and cand in intent:
                    remainder = intent.replace(cand, " ").strip()
                    return stored, remainder or intent
        return None, intent

    # ========================================================
    # 별표·서식 (Annex)
    # ========================================================

    # "별표 4", "별표 2의2" / "별지 제20호서식", "별지 제20호(4)서식", "서식 20"
    _ANNEX_TABLE_RE = re.compile(r"별표\s*(\d+(?:의\d+)?)")
    _ANNEX_FORM_RE = re.compile(
        r"(?:별지\s*제?\s*(\d+(?:의\d+)?)\s*호(?:\s*\([^)]*\))?\s*(?:서식)?"
        r"|서식\s*(?:제)?\s*(\d+(?:의\d+)?)\s*호?)"
    )
    # 번호 없이 서식·별표를 가리키는 말 — law intent가 이 말을 품고 있으면 서식 검색으로도 돈다
    _ANNEX_HINT_RE = re.compile(r"별표|별지|서식|작성방법|작성요령|명세서|신고서|계산서")
    # 법령명 = "…법/…법률" + 선택적 " 시행령/시행규칙". 비탐욕으로 첫 법률명에서 끊되
    # "작성방법"처럼 '법'으로 끝나는 일반 명사는 법령명으로 보지 않는다.
    _LAW_NAME_RE = re.compile(r"^(.+?(?:법|법률))(\s*(?:시행령|시행규칙))?(?=\s|$)")
    _NOT_LAW_SUFFIX = ("방법", "방식", "기법", "요령")

    @classmethod
    def _parse_annex_intent(cls, intent: str) -> tuple[str, str, str]:
        """서식 검색 의도 → (법령명, 번호, 나머지 키워드).

        "법인세법 시행규칙 별지 제20호서식 감가상각비조정명세서"
            → ("법인세법 시행규칙", "서식 20", "감가상각비조정명세서")
        "소득세법 시행령 별표 1" → ("소득세법 시행령", "별표 1", "")
        "감가상각비조정명세서 작성방법" → ("", "", "감가상각비조정명세서 작성방법")
        """
        text = intent.strip()
        number = ""
        head = text
        tail = ""

        m = cls._ANNEX_TABLE_RE.search(text)
        if m:
            number = f"별표 {m.group(1)}"
        else:
            m = cls._ANNEX_FORM_RE.search(text)
            if m:
                number = f"서식 {m.group(1) or m.group(2)}"
        if m:
            head = text[: m.start()].strip()
            tail = text[m.end():].strip()

        law_name = ""
        lm = cls._LAW_NAME_RE.match(head)
        if lm and not lm.group(1).strip().endswith(cls._NOT_LAW_SUFFIX):
            law_name = (lm.group(1) + (lm.group(2) or "")).strip()
            law_name = re.sub(r"\s+", " ", law_name)
            head = head[lm.end():].strip()

        keywords = " ".join(part for part in (head, tail) if part).strip()
        return law_name, number, keywords

    @classmethod
    def _annex_intents(cls, search_intents: dict) -> list[str]:
        """명시된 annex 의도에, 서식·별표를 가리키는 law 의도를 합친다.

        Query Analysis가 "법인세법 시행규칙 별지 감가상각비조정명세서"를 law에
        넣어도(2026-09-02 실측) 조문 검색으로는 0건이다. 그 의도가 서식 검색으로도
        돌게 해서, 프롬프트가 annex 칸을 비워도 서식이 빠지지 않게 한다.
        """
        out: list[str] = []
        for intent in search_intents.get("annex", []) or []:
            if intent and intent not in out:
                out.append(intent)
        for intent in search_intents.get("law", []) or []:
            if not intent:
                continue
            _, article_number = cls._parse_law_intent(intent)
            if article_number:
                continue  # 조문번호가 있으면 조문 의도다
            if cls._ANNEX_HINT_RE.search(intent) and intent not in out:
                out.append(intent)
        return out

    # 모든 서식에 나오는 말 — 검색어로 쓰면 아무 서식이나 걸린다
    _ANNEX_GENERIC_TERMS = frozenset({
        "별지", "별표", "서식", "작성방법", "작성요령", "관련", "명세", "양식", "부표",
    })
    _LUCENE_SPECIAL = re.compile(r'[+\-!(){}\[\]^"~*?:\\/]')

    @classmethod
    def _annex_terms(cls, keywords: str) -> list[str]:
        out = []
        for tok in re.split(r"[\s,·/()]+", keywords or ""):
            tok = tok.strip()
            if len(tok) < 2 or tok in cls._ANNEX_GENERIC_TERMS:
                continue
            if tok not in out:
                out.append(tok)
        return out

    @classmethod
    def _annex_lucene_query(cls, terms: list[str]) -> str:
        """제목 일치를 높이 치고, 복합명사 뒤에 다른 말이 붙은 토큰도 잡는 접두 검색."""
        parts = []
        for t in terms:
            clean = cls._LUCENE_SPECIAL.sub(" ", t).strip()
            if not clean:
                continue
            parts.append(f"annex_title:{clean}*^4 OR {clean}*")
        return " OR ".join(parts)

    def find_annexes_by_title_terms(
        self, terms: list[str], law_name: str | None = None, limit: int = 3
    ) -> list[dict]:
        if not terms:
            return []
        return self.client.execute_query(
            cq.FIND_ANNEX_BY_TITLE_TERMS,
            {"terms": terms, "law_name": law_name or None, "limit": limit},
        )

    def find_annex_by_number(self, law_name: str, annex_number: str, limit: int = 2) -> list[dict]:
        return self.client.execute_query(
            cq.FIND_ANNEX_BY_NUMBER,
            {"law_name": law_name, "annex_number": annex_number, "limit": limit},
        )

    def fulltext_search_annexes(
        self,
        query: str,
        law_name: str | None = None,
        min_score: float = 0.5,
        limit: int = 3,
    ) -> list[dict]:
        return self.client.execute_query(
            cq.FULLTEXT_SEARCH_ANNEXES,
            {"query": _literal_fulltext_query(query), "law_name": law_name or None, "min_score": min_score, "limit": limit},
        )

    def traverse_has_annex(self, article_ids: list[str], limit: int = 4) -> list[dict]:
        """seed 조문이 HAS_ANNEX로 가리키는 별표·서식."""
        if not article_ids:
            return []
        return self.client.execute_query(
            cq.TRAVERSE_HAS_ANNEX, {"article_ids": article_ids, "limit": limit}
        )

    def search_annexes(self, intent: str, limit: int = 3) -> list[dict]:
        """서식 검색 의도 하나를 별표·서식 노드로 푼다.

        번호가 있으면 법령명+번호로 바로 찍고, 없으면 전문검색을 법령명으로
        좁혀 돈다. 법령명도 없으면 전체 전문검색 — 이때는 상위 몇 건만.
        """
        law_name, number, keywords = self._parse_annex_intent(intent)
        terms = self._annex_terms(keywords)
        rows: list[dict] = []
        # 1) 법령명 + 번호
        if law_name and number:
            rows = self.find_annex_by_number(law_name, number, limit=limit)
        # 2) 제목에 검색어가 든 서식 (법령명으로 좁혀서, 안 되면 전체)
        if not rows and terms:
            rows = self.find_annexes_by_title_terms(terms, law_name=law_name or None, limit=limit)
            if not rows and law_name:
                rows = self.find_annexes_by_title_terms(terms, law_name=None, limit=limit)
        # 3) 전문검색 (접두 일치 + 제목 가중)
        if not rows:
            query = self._annex_lucene_query(terms) if terms else self._LUCENE_SPECIAL.sub(" ", intent)
            if query.strip():
                rows = self.fulltext_search_annexes(query, law_name=law_name or None, limit=limit)
                if not rows and law_name:
                    rows = self.fulltext_search_annexes(query, law_name=None, limit=limit)
        out = []
        for r in rows:
            annex = r.get("annex")
            if annex:
                out.append({**annex, "graph_path": "annex lookup (intent)"})
        return out

    # ========================================================
    # source-search.md 전체 탐색 흐름
    # ========================================================

    def search_for_query(self, search_intents: dict) -> SearchResult:
        """Query Analysis Agent의 search_intents를 받아 전체 탐색 실행.

        source-search.md의 탐색 절차:
        Step 1: seed node 검색 (법령 조문 직접 매칭)
        Step 2: 위임규정 edge → 시행령/시행규칙
        Step 3: 관련예규, 인용판례, 기본통칙
        Step 4: 유사쟁점 edge (선택적)
        Step 5: issue_id 매핑
        """
        # Step 1: Seed 조문 매칭
        seeds = self.find_seed_articles(search_intents)
        seed_ids = [s["article_id"] for s in seeds]

        # Step 2~5: 통합 탐색
        result = self.integrated_search(seed_ids)

        # 전문검색으로 추가 보충 (ruling, case 키워드).
        # 독립 전문검색 결과는 검증된 그래프 관계가 아니다.
        for kw in search_intents.get("ruling", []):
            ft_results = self.fulltext_search_rulings(kw, limit=5)
            for r in ft_results:
                ruling = r.get("ruling")
                if ruling:
                    ruling.setdefault("graph_path", _FULLTEXT_GRAPH_PATH)
                    _append_unique_by(result.rulings, ruling, "ruling_id")

            # 국세청 질의회신(Interpretation)도 같은 키워드로 검색
            interp_results = self.fulltext_search_interpretations(kw, limit=5)
            for r in interp_results:
                interp = r.get("interpretation")
                if interp:
                    interp.setdefault("graph_path", _FULLTEXT_GRAPH_PATH)
                    _append_unique_by(result.interpretations, interp, "interp_id")

        for kw in search_intents.get("case", []):
            ft_results = self.fulltext_search_cases(kw, limit=5)
            for r in ft_results:
                case_data = r.get("case_data")
                if not case_data:
                    continue
                case_data.setdefault("graph_path", _FULLTEXT_GRAPH_PATH)
                if case_data.get("court_type") in cq.DECISION_COURT_TYPES:
                    _append_unique_by(result.tribunals, case_data, "case_id")
                else:
                    _append_unique_by(result.cases, case_data, "case_id")

        # 심판례 전용 키워드 검색
        for kw in search_intents.get("tribunal", []):
            ft_results = self.fulltext_search_tribunals(kw, limit=5)
            for r in ft_results:
                tribunal_data = r.get("tribunal")
                if tribunal_data:
                    tribunal_data.setdefault("graph_path", _FULLTEXT_GRAPH_PATH)
                    _append_unique_by(result.tribunals, tribunal_data, "case_id")

        # 조세조약 조문 — 국제조세 쟁점에서만 온다 (source-search.md §8 게이트)
        for kw in search_intents.get("treaty", []):
            country, remainder = self._split_treaty_intent(kw)
            if country is None:
                # 국가 미매칭이면 전체 조약으로 폴백하지 않는다 — 엉뚱한 나라
                # 조문이 근거로 딸려가는 것이 0건보다 나쁘다. 0건은 §8-2의
                # "적재본에서 확인되지 않음" 경로로 정직하게 처리된다.
                self.console.print(f"[yellow]조약 검색 스킵 (체결국 미매칭):[/] {kw}")
                continue
            ft_results = self.fulltext_search_treaty_articles(remainder, country=country)
            for r in ft_results:
                ta = r.get("treaty_article")
                if not ta:
                    continue
                # 조약 메타(조약명·발효일)를 조문에 합쳐 근거 조립이 한 번에 쓰게 한다
                ta = {
                    **ta,
                    "treaty_name": r.get("treaty_name", ""),
                    "treaty_type": r.get("treaty_type", ""),
                    "effective_date": r.get("effective_date", ""),
                }
                if all(
                    ta.get("treaty_article_id") != x.get("treaty_article_id")
                    for x in result.treaty_articles
                ):
                    result.treaty_articles.append(ta)

        # 별표·서식 — 명시된 annex 의도 + 서식을 가리키는 law 의도 + seed 조문의 HAS_ANNEX
        def _add_annex(annex: dict) -> None:
            _append_unique_by(result.annexes, annex, "annex_id")

        for intent in self._annex_intents(search_intents):
            try:
                for annex in self.search_annexes(intent):
                    _add_annex(annex)
            except Exception as error:  # noqa: BLE001 - 서식 검색 실패가 조문 검색을 막지 않는다
                self.console.print(f"[yellow]서식 검색 실패 (생략):[/] {intent} — {error}")
        try:
            for r in self.traverse_has_annex(seed_ids):
                annex = r.get("annex")
                if annex:
                    _add_annex({**annex, "graph_path": f"HAS_ANNEX edge ({r.get('from_article', '')})"})
        except Exception as error:  # noqa: BLE001
            self.console.print(f"[yellow]HAS_ANNEX 탐색 실패 (생략):[/] {error}")

        self.console.print(f"[bold green]최종 결과:[/] {result.to_summary()}")
        return result

    # ========================================================
    # 쟁점별 검색 (병렬 실행용)
    # ========================================================

    def search_for_issue(self, issue_id: str, search_intents: dict) -> tuple[str, SearchResult]:
        """단일 쟁점에 대한 검색을 수행하고 (issue_id, SearchResult)를 반환.

        pipeline.py에서 ThreadPoolExecutor로 쟁점별 병렬 실행할 때 사용.
        """
        self.console.print(f"[bold cyan]쟁점 [{issue_id}][/] 검색 시작...")
        result = self.search_for_query(search_intents)
        self.console.print(f"[bold cyan]쟁점 [{issue_id}][/] 완료 — {result.total_count}건")
        return (issue_id, result)

    # ========================================================
    # 헬퍼
    # ========================================================

    @staticmethod
    def _parse_law_intent(intent: str) -> tuple[str, str]:
        """search_intents 항목 파싱.

        "소득세법 제20조" → ("소득세법", "제20조")
        "국세기본법 제14조 실질과세" → ("국세기본법", "제14조")
        """
        import re

        match = re.search(r"(.+?)\s*(제\d+조(?:의\d+)?)", intent)
        if match:
            return match.group(1).strip(), match.group(2)

        # 조문번호 없는 경우 (키워드 검색용)
        return intent.strip(), ""

    def _detect_basic_rule_support(self) -> bool:
        """현재 DB에 BasicRule 노드/관계가 실제로 존재하는지 확인."""
        rows = self.client.execute_query(
            """
            MATCH ()-[r]->()
            WHERE type(r) = 'HAS_BASIC_RULE'
            RETURN count(r) AS cnt
            """
        )
        return bool(rows and rows[0].get("cnt", 0) > 0)

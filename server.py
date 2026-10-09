#!/usr/bin/env python3
"""세법 그래프 MCP 서버 — 공개용 read-only Streamable HTTP (stateless).

한국 세법 법령·판례·심판례·해석례·조세조약 그래프 DB(Neo4j)를 MCP 도구로 노출한다.
- 수록 범위: 국세 + 지방세 현행 법령(법·령·칙), 판례·조세심판원 결정례·국세청 심사·이의·적부 결정례,
  국세청·법제처·행정안전부 해석례, 조세조약(체결국별 협약·의정서 조문)
- 의존성 없음: 공유 검색 모듈 + 파이썬 표준 라이브러리만 사용 (3.11+)
- Neo4j 접근: HTTP Query API v2 (읽기 전용 파라미터 쿼리만, raw cypher 노출 없음)
- 방어: IP당 분당 호출 제한, 전역 동시 쿼리 제한, 쿼리 타임아웃
- 로그: logs/mcp/usage-YYYYMMDD.jsonl

실행:  python3 server.py            (기본 127.0.0.1:8788)
       PORT=9000 python3 server.py
엔드포인트: POST /mcp  (Cloudflare Tunnel 뒤에서 mcp.taxdoctorai.com/mcp 로 공개)
이 서버는 공개용으로 read-only 파라미터 쿼리 + rate limit + 사용량 로그를 갖춤.
공유 로컬 Neo4j의 현행 검증 그래프만 읽는다. 수집·적재·DB 쓰기는 이 리포의 범위가 아니다.
"""
from __future__ import annotations

import base64
import ipaddress
import json
import os
import queue
import re
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections import defaultdict, deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from socketserver import TCPServer
from urllib.parse import parse_qs, urlparse

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(SCRIPT_DIR)
if os.path.isdir(os.path.join(REPO, "src")):
    sys.path.insert(0, REPO)

from src.search.graph_searcher import (
    annex_body,
    original_attachment_url,
    source_date_notice,
    source_observation_notice,
)
from src.search.public_search import BODY_FIELDS, INTENT_TYPES, NODE_TYPES, PublicGraphSearch
from src.search.source_quality import interpretation_body_guard

LOG_DIR = os.environ.get("MCP_LOG_DIR") or os.path.join(REPO, "logs", "mcp")
BIND = os.environ.get("BIND", "127.0.0.1")
PORT = int(os.environ.get("PORT", "8788"))  # 8787은 사설망용 tax_db_mcp 계열이 사용 중

SERVER_NAME = "korea-tax-law"
SERVER_VERSION = "0.9.1"
SUPPORTED_PROTOCOLS = {"2024-11-05", "2025-03-26", "2025-06-18"}
DEFAULT_PROTOCOL = "2025-06-18"

RATE_PER_MIN = 30          # IP당 분당 tools/call 한도
HANDSHAKE_PER_MIN = 20     # IP당 분당 initialize+tools/list 한도 (정상 클라이언트는 연결당 2회)

# 하드 차단할 IP/대역. 운영자가 자기 로그를 보고 채운다 — 기본값은 비어 있다.
# IPv4/IPv6 프리픽스 표기를 모두 받는다. 예: ["203.0.113.4/32", "2001:db8::/64"]
#
# 채울 대상은 도구 호출 0건에 initialize/tools/list만 무한 반복하는 MCP 디렉터리
# 스캐너다. usage 로그를 IP×메서드로 갈라 tools/call이 0인 IP를 찾으면 된다.
# 주의: 160.79.106.0/24는 Anthropic 이그레스(= claude.ai 실사용자)이므로 넣지 말 것.
BLOCKED_NETS: list[str] = []
GLOBAL_CONCURRENCY = 4     # 동시 Neo4j 쿼리 한도
NEO4J_TIMEOUT = 8          # 쿼리 타임아웃(초)
MAX_BODY = 64 * 1024       # 요청 본문 한도

INSTRUCTIONS = (
    "쟁점별 자료 수집은 search_tax를 우선 사용하세요. 로컬 tax-ai-agent와 같은 통합 검색기로 "
    "법률→시행령→시행규칙과 인용 문서·참조 조문·통칙·별표를 탐색하고, 결과별 실제 경로·원문·시점을 반환합니다. "
    "search_intents에 law/case/ruling/tribunal/treaty/annex 배열을 지정하세요(tribunal은 조세심판원·국세청 불복 결정례). "
    "next_page가 있으면 그 인자로 다음 결과를 조회하고, original_text.continuation이 있으면 get_evidence로 원문을 이어 읽으세요. "
    "as_of는 별도 조문 버전 조회이며 현행 그래프의 인용을 과거 적용으로 확정하지 않습니다. "
    "한국 세법 법령 그래프 DB입니다. 국세청 조세법령 목록의 현행 법·령·칙, 판례, "
    "조세심판원 결정례, 국세청 심사·이의·적부 결정례, 국세청·법제처·행정안전부 해석례, 조세조약을 검색·조회할 수 있습니다. "
    "조문은 '현행 시행 버전' 기준이며 각 결과에 시행일이 표기됩니다. "
    "위임·별표·문서 인용은 활성·검증되고 원천 스냅샷이 맞는 관계만 따릅니다. "
    "키워드 검색 히트는 검증된 인용이 아니고, 인용은 적용 확정이 아닙니다. "
    "resolved_version_id가 없거나 시점이 unresolved이면 현행 조문 적용을 단정하지 마세요. "
    "세율·기준금액이 별표에 위임된 경우가 많으니 get_article이 별표를 가리키면 get_annex로 확인하고, "
    "미래 과세기간이 걸린 질문은 list_upcoming으로 시행예정 개정을 확인하세요. "
    "초안의 해석례·판례 문서번호와 조문 인용이 실제로 있는지는 verify_citations로 확인하세요. "
    "수록 법령 목록은 list_laws로, 조세조약 체결국은 list_treaties로 확인하세요"
    "(미수록 법령은 국가법령정보센터 law.go.kr 참조). "
    "목록의 역사 법령은 현행 그래프에서 제외됩니다. "
    "국내 조문과 조약이 충돌하면 조약이 우선하므로, 비거주자·외국법인 쟁점은 "
    "국내법 조문과 해당국 조약 조문을 함께 확인하세요. "
    "제공 정보는 실무 참고용이며 공식 유권해석이 아닙니다."
)

# ---------------------------------------------------------------- Neo4j 접속

def _load_env() -> dict:
    """환경변수 우선, 없으면 .env 파일(스크립트 옆 또는 상위 디렉토리) 순으로 읽는다."""
    env = {}
    for path in (os.path.join(REPO, ".env"), os.path.join(SCRIPT_DIR, ".env")):
        try:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        env.setdefault(k.strip(), v.strip().strip('"').strip("'"))
        except FileNotFoundError:
            continue
    for k in ("NEO4J_HTTP", "NEO4J_USER", "NEO4J_PASSWORD"):
        if os.environ.get(k):
            env[k] = os.environ[k]
    return env

_ENV = _load_env()
NEO4J_HTTP = _ENV.get("NEO4J_HTTP", "http://127.0.0.1:7474")
_AUTH = base64.b64encode(
    f"{_ENV.get('NEO4J_USER', 'neo4j')}:{_ENV.get('NEO4J_PASSWORD', '')}".encode()
).decode()

_query_slots = threading.BoundedSemaphore(GLOBAL_CONCURRENCY)


class BusyError(Exception):
    pass


class DatabaseQueryError(Exception):
    pass


def cypher(statement: str, parameters: dict | None = None) -> list[dict]:
    """읽기 전용 파라미터 쿼리 실행. 결과를 dict 행 목록으로 반환."""
    if not _query_slots.acquire(timeout=2):
        raise BusyError("동시 요청이 많습니다. 잠시 후 다시 시도해 주세요.")
    try:
        body = json.dumps({"statement": statement, "parameters": parameters or {}}).encode()
        req = urllib.request.Request(
            f"{NEO4J_HTTP}/db/neo4j/query/v2",
            data=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Basic {_AUTH}",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=NEO4J_TIMEOUT) as r:
            data = json.loads(r.read().decode())
        if data.get("errors"):
            raise DatabaseQueryError("Neo4j query failed")
        d = data.get("data", {})
        fields, values = d.get("fields", []), d.get("values", [])
        return [dict(zip(fields, row)) for row in values]
    finally:
        _query_slots.release()


# ---------------------------------------------------------------- 유틸

_LUCENE_SPECIALS = re.compile(r'([+\-!(){}\[\]^~*?:\\/]|&&|\|\|)')


def lucene_escape(q: str) -> str:
    # 따옴표(구문검색)와 AND/OR은 허용, 나머지 특수문자는 이스케이프
    return _LUCENE_SPECIALS.sub(r"\\\1", q)


def norm_article_no(no: str) -> str:
    """'97조의2', '제97조의2', '97', '97-2' → '제97조의2' 형태로 정규화."""
    s = str(no).strip().replace(" ", "")
    m = re.match(r"^제?(\d+)(?:조)?(?:의(\d+)|-(\d+))?$", s)
    if m:
        base = f"제{m.group(1)}조"
        sub = m.group(2) or m.group(3)
        return f"{base}의{sub}" if sub else base
    return s if s.startswith("제") else f"제{s}"


def fmt_date(d) -> str:
    s = str(d or "")
    return f"{s[:4]}.{s[4:6]}.{s[6:8]}" if len(s) == 8 and s.isdigit() else s


def clip(s, n=300) -> str:
    s = (s or "").replace("\n", " ").strip()
    return s[:n] + ("…" if len(s) > n else "")


# ---------------------------------------------------------------- 검증 그래프 가드
# tax-ai-agent ad94f69 `src/search/edge_provenance.py` 의 MCP 소비용 부분집합.
# 비활성·미검증·스냅샷 불일치·삭제 끝점은 인용으로 쓰지 않는다.
# 물리 관계 건수는 의미 확정이 아니다.

DOCUMENT_PROJECTION_SCHEMA = "tax-document-projection-v1"
DOCUMENT_PROJECTION_KIND = "stored_document_projection"
DOCUMENT_PROJECTION_SNAPSHOT_PREFIX = "stored-document:v1:sha256:"
CITATION_LIMIT = 5


def _and(*parts: str) -> str:
    return " AND ".join(part for part in parts if part)


def _nonempty(expr: str) -> str:
    return f"({expr} IS NOT NULL AND {expr} <> '')"


def _same_nonempty(left: str, right: str) -> str:
    return f"{left} = {right} AND {left} <> ''"


def current_law_guard(*, law: str = "l") -> str:
    """현행 목록만. 역사·예정 법령은 현행 그래프 소비자 경로에서 제외."""
    return _and(
        f"{law}.is_current = true",
        f"coalesce({law}.publication_status, '') <> 'historical'",
        f"coalesce({law}.publication_status, '') <> 'scheduled'",
    )


def active_verified_guard(*, edge: str) -> str:
    return f"{edge}.active = true AND {edge}.verified = true"


def current_article_guard(*, node: str = "a") -> str:
    """삭제 표시가 있는 현행 끝점은 쓰지 않는다."""
    return (
        f"{node}.is_current = true AND "
        f"coalesce({node}.deleted, false) = false AND "
        f"coalesce({node}.is_deleted, false) = false"
    )


def current_annex_guard(*, node: str = "x") -> str:
    """현행 별표만. is_current 누락은 현행이 아니다."""
    return (
        f"{node}.is_current = true AND "
        f"coalesce({node}.deleted, false) = false AND "
        f"coalesce({node}.is_deleted, false) = false"
    )


def current_contains_guard(*, edge: str = "owns", law: str = "l") -> str:
    """검증된 현행 Law 소유. 비활성 CONTAINS는 증거가 아니다."""
    return _and(
        active_verified_guard(edge=edge),
        _same_nonempty(f"{edge}.source_snapshot", f"{law}.source_snapshot"),
    )


def official_document_snapshot(*, document: str) -> str:
    """공식 스냅샷. 빈 문자열은 없음으로 보고, 엣지 해시로 메우지 않는다."""
    return (
        f"coalesce(nullif({document}.source_snapshot, ''), "
        f"{document}.source_projection_snapshot)"
    )


def document_projection_guard(*, document: str, edge: str) -> str:
    schema = DOCUMENT_PROJECTION_SCHEMA
    kind = DOCUMENT_PROJECTION_KIND
    prefix = DOCUMENT_PROJECTION_SNAPSHOT_PREFIX
    return _and(
        f"{document}.source_projection_schema = '{schema}'",
        f"{document}.source_projection_kind = '{kind}'",
        (
            f"{document}.source_projection_snapshot = '{prefix}' + "
            f"{document}.source_projection_sha256"
        ),
        f"{edge}.source_projection_schema = {document}.source_projection_schema",
        f"{edge}.source_projection_kind = {document}.source_projection_kind",
        f"{edge}.source_projection_sha256 = {document}.source_projection_sha256",
        f"{edge}.source_projection_snapshot = {document}.source_projection_snapshot",
        f"{edge}.source_projection_snapshot = '{prefix}' + {edge}.source_projection_sha256",
    )


def document_citation_guard(*, document: str, article: str, edge: str = "rel") -> str:
    """CITES_ARTICLE 및 역방향 HAS_CASE / HAS_RULING / HAS_INTERPRETATION.

    문서가 증거 원천이다. edge.source_snapshot 은 문서를 따르고,
    edge.target_source_snapshot 은 조문(또는 버전 노드)을 따른다.
    """
    return _and(
        active_verified_guard(edge=edge),
        document_projection_guard(document=document, edge=edge),
        f"{edge}.source_snapshot = {official_document_snapshot(document=document)}",
        _nonempty(f"{edge}.source_snapshot"),
        _same_nonempty(f"{edge}.target_source_snapshot", f"{article}.source_snapshot"),
    )


def delegates_to_rel_guard(*, rel: str = "rel") -> str:
    """DELEGATES_TO: upper=start, lower=end. 방향을 뒤집으면 안 된다."""
    return _and(
        active_verified_guard(edge=rel),
        _same_nonempty(f"{rel}.upper_source_snapshot", f"startNode({rel}).source_snapshot"),
        _same_nonempty(f"{rel}.lower_source_snapshot", f"endNode({rel}).source_snapshot"),
    )


def delegated_from_rel_guard(*, rel: str = "rel") -> str:
    """DELEGATED_FROM: lower=start, upper=end. DELEGATES_TO 와 스냅샷 방향이 반대다."""
    return _and(
        active_verified_guard(edge=rel),
        _same_nonempty(f"{rel}.lower_source_snapshot", f"startNode({rel}).source_snapshot"),
        _same_nonempty(f"{rel}.upper_source_snapshot", f"endNode({rel}).source_snapshot"),
    )


def current_scoped_annex_guard(*, law: str = "l", annex: str = "x") -> str:
    """현행 목록 Law에 law_id로 묶인 별표. 이름만으로 소유를 증명하지 않는다."""
    return _and(
        current_law_guard(law=law),
        current_annex_guard(node=annex),
        _same_nonempty(f"{annex}.law_id", f"{law}.law_id"),
        _nonempty(f"{annex}.source_snapshot"),
    )


def law_annex_owner_guard(*, law: str = "l", annex: str = "x", edge: str = "owns") -> str:
    """Law-HAS_ANNEX 공식 첨부. 스냅샷은 evidence_source_label을 따른다."""
    return _and(
        active_verified_guard(edge=edge),
        (
            f"(({edge}.evidence_source_label = 'Annex' AND "
            f"{_same_nonempty(f'{edge}.source_snapshot', f'{annex}.source_snapshot')}) OR "
            f"({edge}.evidence_source_label = 'Law' AND "
            f"{_same_nonempty(f'{edge}.source_snapshot', f'{law}.source_snapshot')} AND "
            f"{_same_nonempty(f'{edge}.target_source_snapshot', f'{annex}.source_snapshot')}))"
        ),
    )


def scoped_annex_owner_exists(*, law: str = "l", annex: str = "x") -> str:
    """활성·검증·원천이 맞는 소유 엣지. Law 또는 같은 법령의 Article."""
    law_edge = law_annex_owner_guard(law=law, annex=annex, edge="owns")
    art_edge = article_annex_guard(article="a", annex=annex, edge="rel")
    return (
        f"(EXISTS {{ MATCH ({law})-[owns:HAS_ANNEX]->({annex}) WHERE {law_edge} }} "
        f"OR EXISTS {{ MATCH (a:Article)-[rel:HAS_ANNEX]->({annex}) "
        f"WHERE a.law_id = {law}.law_id AND {art_edge} }})"
    )


def article_annex_guard(*, article: str = "a", annex: str = "x", edge: str = "rel") -> str:
    """HAS_ANNEX 스냅샷 방향은 evidence_source_label 을 따른다."""
    return _and(
        active_verified_guard(edge=edge),
        current_article_guard(node=article),
        current_annex_guard(node=annex),
        (
            f"(({edge}.evidence_source_label = 'Article' AND "
            f"{_same_nonempty(f'{edge}.source_snapshot', f'{article}.source_snapshot')} AND "
            f"{_same_nonempty(f'{edge}.target_source_snapshot', f'{annex}.source_snapshot')}) OR "
            f"({edge}.evidence_source_label = 'Annex' AND "
            f"{_same_nonempty(f'{edge}.source_snapshot', f'{annex}.source_snapshot')} AND "
            f"{_same_nonempty(f'{edge}.target_source_snapshot', f'{article}.source_snapshot')}))"
        ),
    )


def article_version_citation_guard(
    *, document: str, version: str = "v", edge: str = "rel"
) -> str:
    """CITES_ARTICLE_VERSION — 검증되고 원천이 있는 ArticleVersion 만."""
    return _and(
        document_citation_guard(document=document, article=version, edge=edge),
        f"{edge}.resolved_version_id = {version}.version_id",
        f"{version}.verified = true",
        _nonempty(f"{version}.source_snapshot"),
        _nonempty(f"{version}.version_id"),
    )


def fmt_uncertainty(temporal_resolution, resolved_version_id) -> str:
    """인용 시점·적용 버전. 없으면 현행 적용으로 메우지 않는다."""
    bits = []
    tr = (temporal_resolution or "").strip()
    if tr:
        bits.append(f"시점 {tr}")
    else:
        bits.append("시점 미기재")
    vid = (str(resolved_version_id).strip() if resolved_version_id else "")
    bits.append(f"적용버전 {vid}" if vid else "적용버전 미해소")
    return " · ".join(bits)


def q_current_article(cond: str) -> str:
    return (
        "MATCH (l:Law)-[owns:CONTAINS]->(a:Article) "
        f"WHERE {cond} AND {current_law_guard(law='l')} AND a.article_number = $no "
        f"AND {current_article_guard(node='a')} "
        f"AND {current_contains_guard(edge='owns', law='l')} "
        "RETURN l.law_name AS law, l.enforcement_date AS enf, a.article_id AS aid, "
        "a.article_number AS no, a.article_title AS title, a.article_content AS content, "
        "l.source_snapshot AS law_snap, a.source_snapshot AS article_snap "
        "LIMIT 1"
    )


Q_DELEGATES_TO = (
    "MATCH (a:Article {article_id: $aid})-[rel:DELEGATES_TO]->(d:Article)"
    "<-[owns:CONTAINS]-(dl:Law) "
    f"WHERE {current_law_guard(law='dl')} "
    f"AND {delegates_to_rel_guard(rel='rel')} "
    f"AND {current_article_guard(node='a')} "
    f"AND {current_article_guard(node='d')} "
    f"AND {current_contains_guard(edge='owns', law='dl')} "
    "RETURN DISTINCT dl.law_name AS law, d.article_number AS no, d.article_title AS title "
    "LIMIT 10"
)

Q_DELEGATED_FROM = (
    "MATCH (a:Article {article_id: $aid})-[rel:DELEGATED_FROM]->(p:Article)"
    "<-[owns:CONTAINS]-(pl:Law) "
    f"WHERE {current_law_guard(law='pl')} "
    f"AND {delegated_from_rel_guard(rel='rel')} "
    f"AND {current_article_guard(node='a')} "
    f"AND {current_article_guard(node='p')} "
    f"AND {current_contains_guard(edge='owns', law='pl')} "
    "RETURN DISTINCT pl.law_name AS law, p.article_number AS no, p.article_title AS title "
    "LIMIT 10"
)

Q_ARTICLE_ANNEX = (
    "MATCH (a:Article {article_id: $aid})-[rel:HAS_ANNEX]->(x:Annex) "
    f"WHERE {article_annex_guard(article='a', annex='x', edge='rel')} "
    "RETURN x.annex_number AS no, x.annex_title AS title, "
    "size(coalesce(x.content, '')) AS len ORDER BY x.annex_number"
)

Q_SEARCH_ANNEXES = (
    "CALL db.index.fulltext.queryNodes('annex_original_content_ft_v2', $q) YIELD node, score "
    "MATCH (l:Law {law_id: node.law_id}) "
    f"WHERE {current_scoped_annex_guard(law='l', annex='node')} "
    "AND ($law IS NULL OR l.law_name CONTAINS $law) "
    f"AND {scoped_annex_owner_exists(law='l', annex='node')} "
    "RETURN l.law_name AS law, node.annex_number AS no, node.annex_title AS title, "
    "node.annex_type AS kind, size(coalesce(node.content, '')) AS len, "
    "substring(coalesce(node.content, ''), 0, 240) AS preview, "
    "node {.content, .source_snapshot, .extracted_content, .extracted_content_sha256, .original_binary_sha256, .extraction_source_snapshot, .extraction_provenance} AS source_body, score "
    "ORDER BY score DESC LIMIT $limit"
)

Q_GET_ANNEX = (
    "MATCH (x:Annex) "
    "WHERE replace(x.annex_number, ' ', '') = replace($no, ' ', '') "
    f"AND {current_annex_guard(node='x')} "
    f"AND {_nonempty('x.source_snapshot')} "
    "MATCH (l:Law {law_id: x.law_id}) "
    f"WHERE {current_law_guard(law='l')} "
    "AND (l.law_name = $law OR l.law_name CONTAINS $law) "
    f"AND {scoped_annex_owner_exists(law='l', annex='x')} "
    "RETURN l.law_name AS law, x.annex_number AS no, x.annex_title AS title, "
    "x.content AS content, x.hwp_url AS hwp, x.pdf_url AS pdf, "
    "x.related_articles AS arts, "
    "x {.content, .source_snapshot, .extracted_content, .extracted_content_sha256, .original_binary_sha256, .extraction_source_snapshot, .extraction_provenance} AS source_body "
    "ORDER BY CASE WHEN l.law_name = $law THEN 0 ELSE 1 END "
    "LIMIT 1"
)

Q_GET_ANNEX_NEAR = (
    "MATCH (x:Annex) "
    f"WHERE {current_annex_guard(node='x')} "
    f"AND {_nonempty('x.source_snapshot')} "
    "MATCH (l:Law {law_id: x.law_id}) "
    f"WHERE {current_law_guard(law='l')} "
    "AND (l.law_name = $law OR l.law_name CONTAINS $law) "
    f"AND {scoped_annex_owner_exists(law='l', annex='x')} "
    "RETURN x.annex_number AS no, x.annex_title AS t "
    "ORDER BY x.annex_number LIMIT 40"
)

Q_VERIFIED_CITATIONS = (
    "MATCH (a:Article {article_id: $aid}) "
    f"WHERE {current_article_guard(node='a')} "
    "CALL (a) { "
    "MATCH (a)-[rel:HAS_CASE]->(c:Case) "
    f"WHERE {document_citation_guard(document='c', article='a', edge='rel')} "
    "AND coalesce(c.body_identity_status, '') <> 'unresolved' "
    "RETURN 'case' AS kind, coalesce(c.court_type, '판례') AS org, "
    "c.case_number AS no, c.ruling_date AS d, "
    "rel.temporal_resolution AS tr, rel.resolved_version_id AS vid "
    "ORDER BY c.ruling_date DESC LIMIT $limit "
    "UNION ALL "
    "MATCH (a)-[rel:HAS_INTERPRETATION]->(i:Interpretation) "
    f"WHERE {document_citation_guard(document='i', article='a', edge='rel')} "
    f"AND {interpretation_body_guard('i')} "
    "RETURN 'interpretation' AS kind, coalesce(i.reply_org, '해석례') AS org, "
    "i.interp_number AS no, i.reply_date AS d, "
    "rel.temporal_resolution AS tr, rel.resolved_version_id AS vid "
    "ORDER BY i.reply_date DESC LIMIT $limit "
    "UNION ALL "
    "MATCH (a)-[rel:HAS_RULING]->(r:Ruling) "
    f"WHERE {document_citation_guard(document='r', article='a', edge='rel')} "
    "AND (NOT r:ReferenceBook OR r.active = true) "
    "AND (NOT r:AdminRule OR r.is_current = true) "
    "RETURN 'ruling' AS kind, coalesce(r.ruling_org, '예규') AS org, "
    "r.ruling_number AS no, r.ruling_date AS d, "
    "rel.temporal_resolution AS tr, rel.resolved_version_id AS vid "
    "ORDER BY r.ruling_date DESC LIMIT $limit "
    "} "
    "RETURN kind, org, no, d, tr, vid"
)

Q_ARTICLE_HISTORY = (
    "MATCH (l:Law)-[owns:CONTAINS]->(a:Article) "
    f"WHERE l.law_name CONTAINS $law AND a.article_number = $no "
    f"AND {current_law_guard(law='l')} "
    f"AND {current_article_guard(node='a')} "
    f"AND {current_contains_guard(edge='owns', law='l')} "
    "WITH a LIMIT 1 "
    "MATCH (a)-[:HAS_VERSION]->(v:ArticleVersion) "
    "RETURN v.enforcement_date AS enf, v.valid_from AS vfrom, v.valid_to AS vto, "
    "v.version_id AS vid, coalesce(v.verified, false) AS verified, "
    "coalesce(v.source_snapshot, '') AS snap "
    "ORDER BY v.valid_from DESC LIMIT 20"
)

Q_CURRENT_ARTICLE_MARKS = (
    "MATCH (l:Law)-[owns:CONTAINS]->(a:Article) "
    f"WHERE l.law_name CONTAINS $law AND a.article_number = $no "
    f"AND {current_law_guard(law='l')} "
    f"AND {current_article_guard(node='a')} "
    f"AND {current_contains_guard(edge='owns', law='l')} "
    "RETURN a.article_content AS content LIMIT 1"
)

Q_SEARCH_ARTICLES_FT = (
    "CALL db.index.fulltext.queryNodes('article_content_ft', $q) YIELD node, score "
    "MATCH (l:Law)-[owns:CONTAINS]->(node) "
    f"WHERE {current_law_guard(law='l')} AND ($law IS NULL OR l.law_name CONTAINS $law) "
    f"AND {current_contains_guard(edge='owns', law='l')} "
    f"AND {current_article_guard(node='node')} "
    "RETURN l.law_name AS law, l.enforcement_date AS enf, node.article_number AS no, "
    "node.article_title AS title, substring(node.article_content, 0, 260) AS preview, score "
    "ORDER BY score DESC LIMIT $limit"
)

Q_SEARCH_ARTICLES_FALLBACK = (
    "MATCH (l:Law)-[owns:CONTAINS]->(a:Article) "
    f"WHERE {current_law_guard(law='l')} AND ($law IS NULL OR l.law_name CONTAINS $law) "
    f"AND {current_contains_guard(edge='owns', law='l')} "
    f"AND {current_article_guard(node='a')} "
    "AND (a.article_title CONTAINS $raw OR a.article_content CONTAINS $raw) "
    "RETURN l.law_name AS law, l.enforcement_date AS enf, a.article_number AS no, "
    "a.article_title AS title, substring(a.article_content, 0, 260) AS preview, 0 AS score "
    "ORDER BY CASE WHEN a.article_title CONTAINS $raw THEN 0 ELSE 1 END "
    "LIMIT $limit"
)

PROVENANCE_FOOTER = (
    "※ 위임·별표·인용은 활성·검증·원천 스냅샷이 일치하는 관계만 표시한다. "
    "인용은 적용 확정이 아니다. 시점 unresolved 이거나 적용버전이 없으면 "
    "현행 조문으로 메우지 말 것."
)


# ---------------------------------------------------------------- 도구 구현

def t_list_laws(args: dict) -> str:
    rows = cypher(
        "MATCH (l:Law) WHERE " + current_law_guard(law="l") + " "
        "RETURN l.law_name AS name, l.law_type AS type, l.enforcement_date AS enf, "
        "coalesce(l.ministry, '') AS ministry, coalesce(l.instrument_type, '') AS inst "
        "ORDER BY l.law_name"
    )
    # 조세특례제한법은 소관이 '재정경제부,행정안전부' 공동이라 지방세로 세면 안 된다.
    def kind(name, ministry):
        if str(name).startswith("지방세"):
            return "지방세"
        if "," in ministry:
            return "공통"
        return "지방세" if "행정안전부" in ministry else "국세"

    counts = {"국세": 0, "지방세": 0, "공통": 0}
    for r in rows:
        counts[kind(r["name"], r["ministry"])] += 1
    lines = [
        (f"수록 현행 법령 {len(rows)}건 "
         f"(국세 {counts['국세']} · 지방세 {counts['지방세']} · 국세/지방세 공통 {counts['공통']}):")
    ]
    for r in rows:
        k = kind(r["name"], r["ministry"])
        tag = "" if k == "국세" else f" [{k}]"
        lines.append(f"- {r['name']} ({r['type']}, 시행 {fmt_date(r['enf'])}){tag}")
    lines.append(
        "\n※ 국세청 조세법령 목록의 현행 법·령·칙. "
        "목록 역사 법령(자산재평가법시행령·자산재평가법시행규칙)은 현행 그래프에서 제외."
    )
    lines.append("※ 목록에 없는 법령은 이 DB에 미수록. 국가법령정보센터(law.go.kr) 확인 요망.")
    lines.append("※ 조세조약은 별도 수록 — list_treaties로 확인.")
    return "\n".join(lines)


def t_search_articles(args: dict) -> str:
    q = lucene_escape(str(args["query"]))
    law = args.get("law_name")
    limit = min(int(args.get("limit", 8)), 20)
    rows = cypher(Q_SEARCH_ARTICLES_FT, {"q": q, "law": law, "limit": limit})
    fallback_term = None
    if not rows:
        # 복합어(예: '대손세액공제')는 Lucene 토큰과 어긋나 빈 결과가 나올 수 있어
        # 원문 부분일치 폴백. 그것도 비면 접두어를 단계적으로 줄여 재시도
        # (예: 대손세액공제 → 대손세액 — 원문은 '대손세액 공제'로 띄어 쓰김)
        raw = str(args["query"]).strip().replace(" ", "")
        candidates = [raw]
        if len(raw) > 6:
            candidates.append(raw[:6])
        if len(raw) > 4:
            candidates.append(raw[:4])
        for term in candidates:
            rows = cypher(
                Q_SEARCH_ARTICLES_FALLBACK,
                {"raw": term, "law": law, "limit": limit},
            )
            if rows:
                fallback_term = term
                break
    if not rows:
        return "검색 결과 없음. 키워드를 바꾸거나(조사 없는 명사 위주), list_laws로 수록 여부를 확인하세요."
    out = []
    if fallback_term and fallback_term != str(args["query"]).strip().replace(" ", ""):
        out.append(f"※ '{fallback_term}' 부분일치 결과임 (원본 검색어 정확일치 없음):")
    for r in rows:
        out.append(
            f"[{r['law']} {r['no']}] {r['title'] or ''} (시행 {fmt_date(r['enf'])})\n  {clip(r['preview'], 260)}"
        )
    out.append("\n※ 원문 전체는 get_article로 조회. 삭제·비현행·비검증 CONTAINS는 제외.")
    return "\n\n".join(out)


def t_get_article(args: dict) -> str:
    law = str(args["law_name"]).strip()
    no = norm_article_no(args["article_number"])
    rows = cypher(q_current_article("l.law_name = $law"), {"law": law, "no": no})
    if not rows:  # 부분 법령명 허용 (예: '상속세' → '상속세 및 증여세법')
        rows = cypher(q_current_article("l.law_name CONTAINS $law"), {"law": law, "no": no})
    if not rows:
        return f"'{law} {no}' 조문을 찾지 못함. list_laws로 정확한 법령명을 확인하세요."
    r = rows[0]
    deleg = cypher(Q_DELEGATES_TO, {"aid": r["aid"]})
    parent = cypher(Q_DELEGATED_FROM, {"aid": r["aid"]})
    out = [
        f"# {r['law']} {r['no']} {r['title'] or ''}",
        f"(현행, 시행 {fmt_date(r['enf'])})",
        "",
        r["content"] or "",
    ]
    if parent:
        out.append("\n## 위임 상위법령 조문")
        for d in parent:
            out.append(f"- {d['law']} {d['no']} {d['title'] or ''}")
    if deleg:
        out.append("\n## 위임 하위법령 조문")
        for d in deleg:
            out.append(f"- {d['law']} {d['no']} {d['title'] or ''}")

    annex = cypher(Q_ARTICLE_ANNEX, {"aid": r["aid"]})
    if annex:
        # 세율표·기준금액이 별표에 있으면 조문만 읽어서는 숫자가 안 나온다
        out.append("\n## 이 조문이 위임한 별표")
        for x in annex:
            out.append(f"- [{x['no']}] {x['title']} ({x['len']:,}자)")
        out.append(f'  → 본문은 get_annex(law_name="{r["law"]}", annex_number="<별표 N>")')

    upcoming = cypher(
        "MATCH (a:Article {article_id: $aid})-[:HAS_UPCOMING]->(u:UpcomingArticle) "
        "RETURN u.enforcement_date AS d, u.change_type AS kind, u.content AS content "
        "ORDER BY d LIMIT 20",
        {"aid": r["aid"]},
    )
    if upcoming:
        out.append(f"\n## ⚠ 시행예정 개정 {len(upcoming)}건")
        out.append("위 본문은 **현행**이다. 아래 시행일 이후 사안이면 개정문으로 판단할 것.")
        for u in upcoming:
            out.append(f"\n### {fmt_date(u['d'])} 시행 ({u['kind']})")
            out.append(clip(u["content"], 1200))
        out.append("\n※ 시행예정 조문은 공포됐으나 아직 시행 전이다. 과세기간·거래일이 "
                   "시행일 전이면 위 현행 조문이 적용된다.")

    cites = cypher(Q_VERIFIED_CITATIONS, {"aid": r["aid"], "limit": CITATION_LIMIT})
    if cites:
        out.append("\n## 검증된 인용 (각 최대 5건 · 인용≠적용)")
        labels = {"case": "판례", "interpretation": "해석례", "ruling": "예규"}
        for c in cites:
            kind = labels.get(c.get("kind"), c.get("kind") or "문서")
            out.append(
                f"- [{kind}] {c.get('org') or ''} {c.get('no') or ''} "
                f"({fmt_date(c.get('d'))}) · {fmt_uncertainty(c.get('tr'), c.get('vid'))}"
            )

    snap = r.get("article_snap") or r.get("law_snap") or ""
    out.append(f"\n출처: {r['law']} {r['no']} (시행 {fmt_date(r['enf'])} 기준)")
    if snap:
        out.append(f"원천 스냅샷: {snap}")
    out.append(PROVENANCE_FOOTER)
    return "\n".join(out)


def t_get_article_history(args: dict) -> str:
    law = str(args["law_name"]).strip()
    no = norm_article_no(args["article_number"])
    rows = cypher(Q_ARTICLE_HISTORY, {"law": law, "no": no})
    cur = cypher(Q_CURRENT_ARTICLE_MARKS, {"law": law, "no": no})
    out = [f"# {law} {no} 개정 연혁"]
    if cur:
        marks = re.findall(r"<개정[^>]*>|<신설[^>]*>|\[전문개정[^\]]*\]", cur[0].get("content") or "")
        if marks:
            out.append("현행 조문 내 개정 표기: " + ", ".join(dict.fromkeys(marks)))
    if rows:
        out.append("\n조문 버전 이력 (최근순):")
        for r in rows:
            verified = bool(r.get("verified"))
            snap = (r.get("snap") or "").strip()
            flag = "검증·원천있음" if verified and snap else (
                "검증됨·원천없음" if verified else "미검증 — 적용 단정 금지"
            )
            span = fmt_date(r["vfrom"])
            if r.get("vto"):
                span += f"~{fmt_date(r['vto'])}"
            out.append(
                f"- 시행 {fmt_date(r['enf'])} (적용 구간 {span}) · {flag}"
            )
    if len(out) == 1:
        return f"'{law} {no}'의 연혁 정보를 찾지 못함."
    out.append(
        "\n※ 미검증이거나 원천 스냅샷이 없는 버전은 출처 확인된 연혁이 아니다. "
        "resolved_version_id 없는 인용을 이 이력의 현행 조문에 적용했다고 보지 말 것. "
        "확정 판단은 law.go.kr 연혁 대조 요망."
    )
    return "\n".join(out)


def t_search_cases(args: dict) -> str:
    q = lucene_escape(str(args["query"]))
    limit = min(int(args.get("limit", 5)), 15)
    rows = cypher(
        "CALL db.index.fulltext.queryNodes('case_original_content_ft_v2', $q) YIELD node, score "
        "WHERE coalesce(node.body_identity_status, '') <> 'unresolved' "
        "RETURN node.case_number AS no, node.case_name AS name, node.court_type AS court, "
        "node.ruling_date AS d, node.case_id AS case_id, node.source_kind AS source_kind, "
        "node.body_status AS body_status, node.identity_status AS identity_status, "
        "node.original_attachment_available AS original_attachment_available, "
        "node.date_status AS date_status, node.listing_date AS listing_date, "
        "node.listed_decision_date AS listed_decision_date, node.original_document_date AS original_document_date, "
        "node.document_decision_date AS document_decision_date, "
        
        "substring(coalesce(node.case_holding, node.ruling_summary, node.full_content, ''), 0, 320) AS preview, "
        "score ORDER BY score DESC LIMIT $limit",
        {"q": q, "limit": limit},
    )
    if not rows:
        return "검색 결과 없음. 단일 핵심 키워드로 다시 시도해 보세요."
    out = []
    for r in rows:
        out.append(
            f"[{r['court'] or '판례'}] {r['no']} {r['name'] or ''} ({fmt_date(r['d'])})\n  {clip(r['preview'], 320)}"
        )
        if notice := source_date_notice(r):
            out.append("[원천 날짜 정보 · 원문 아님] " + notice)
        if r.get("body_status") == "attachment_only":
            out.append("[자료 상태] 공식 첨부 원본을 확보했으며 전문 추출은 완료되지 않았습니다.")
        elif r.get("body_status") == "summary_only":
            out.append("[자료 상태] 공식 요지만 확보된 자료입니다.")
        if attachment := original_attachment_url(r):
            out.append("공식 원문 첨부: " + attachment)
    out.append(
        "\n※ 요지 발췌임. 키워드 검색은 본문 일치이며 현행 조문에 대한 검증된 인용이 아니다. "
        "조문 연결은 get_article의 검증된 인용만 쓴다. 인용 시 사건번호로 원문 확인 요망."
    )
    return "\n\n".join(out)


# ---------------------------------------------------------------- 해석례 순위·문서번호 조회
# 전문검색 인덱스는 standard 분석기라 '출자공동사업자의'와 '출자공동사업자', '적용여부'와 '적용 여부'를
# 다른 낱말로 본다. 해석례는 제목이 곧 쟁점이라, 본문 점수만으로는 짧은 회신(기재부 등)이 긴 본문에 밀린다.
# 그래서 (1) 본문 점수, (2) 조사를 뗀 핵심어가 제목에 몇 개 있는지, (3) 질문과 제목·요지의 글자 2-gram 겹침을
# 같은 비중으로 더해 후보를 다시 줄 세운다. 후보는 본문 검색과 제목 핵심어 검색에서 모은다.

_KO_TAIL = re.compile(
    r"(으로서|으로써|에게서|으로|에서|에게|로서|로써|까지|부터|이나|이며|이고|하는|되는|하여|해야|"
    r"의|가|이|은|는|을|를|에|와|과|로|도|만)$"
)
_STEM_STOP = {"여부", "경우", "해당", "관련", "대한", "있는지", "되는지", "하는지", "따른", "위한", "적용",
              "등", "및", "그", "수", "것", "때", "어떻게", "있나요", "되나요", "하나요"}
INTERP_POOL = 40


def ko_stems(q: str, k: int = 8) -> list[str]:
    out = []
    for w in re.findall(r"[가-힣A-Za-z0-9]+", q or ""):
        if len(w) > 2:
            w = _KO_TAIL.sub("", w)
        w = w.lower()
        if len(w) >= 2 and w not in _STEM_STOP and w not in out:
            out.append(w)
    return out[:k]


def _bigrams(t: str) -> set:
    t = re.sub(r"[^0-9A-Za-z가-힣]", "", t or "")
    return {t[i:i + 2] for i in range(len(t) - 1)}


def bigram_sim(q: str, t: str) -> float:
    qb, tb = _bigrams(q), _bigrams(t)
    return len(qb & tb) / ((len(qb) * len(tb)) ** 0.5) if qb and tb else 0.0


def doc_key(s: str) -> str:
    """문서번호 비교용 — 날짜 괄호·공백·기호를 떼고 숫자 앞 0을 없앤다('서면-2023-법규기본-0950' = '서면2023법규기본950')."""
    s = re.sub(r"\(\d{4}\.[^)]*\)", "", s or "")
    return re.sub(r"(?<!\d)0+(?=\d)", "", re.sub(r"[^0-9A-Za-z가-힣]", "", s))


def interp_number_keys(no: str) -> list[str]:
    """DB 문서번호 '서면-2023-법규기본-2595[법규과-2973]'은 본번호·괄호 안 부번호 어느 쪽으로도 찾는다."""
    main, _, rest = (no or "").partition("[")
    keys = [doc_key(main)]
    if rest:
        keys.append(doc_key(rest.rstrip("]")))
    return [k for k in keys if k]


# 해석례·결정례 문서번호 표기. 조문 번호(제52조)·금액과 섞이지 않게 기관·형식 표지가 있는 것만.
INTERP_NO_RE = re.compile(
    r"(?:서면|사전|기준|질의)\s*-?\s*\d{4}\s*-\s*[가-힣]{1,12}\s*-\s*\d{1,5}(?:\s*\[[^\]]{1,40}\])?"
    r"|(?:기획재정부|재정경제부|재경부|기재부)?\s?[가-힣]{2,14}(?:과|팀|국|관|실)\s?-\s?\d{1,6}"
    r"|[가-힣]{1,6}\d{5}-\d{1,6}"
)
CASE_NO_RE = re.compile(
    r"조심\s*-?\s*\d{4}\s*-?\s*[가-힣]{1,2}\s*-?\s*\d{1,6}"
    r"|국심\s*-?\s*\d{4}\s*-?\s*[가-힣]{1,2}\s*-?\s*\d{1,6}"
    r"|(?:심사|이의|적부)-[가-힣]{1,4}-\d{4}-\d{1,5}"
    r"|감사원-\d{4}-감심-\d{1,5}"
    r"|(?<![\d-])\d{4}\s?(?:헌[가-힣]{1,2}|구합|구단|두|누|다|나|도|노|구|마|카합)\s?\d{1,6}"
)


def _serial(no: str) -> str:
    m = re.search(r"(\d+)\D*$", no or "")
    return str(int(m.group(1))) if m else ""


def _number_tokens(no: str) -> list[str]:
    """후보를 좁힐 부분 문자열 — 한글 덩어리와 끝 일련번호(앞 0 제거). 일련번호만으로는 '-8'·'-0020'처럼 짧아 후보가 넘친다.
    연도는 넣지 않는다 — 원천 표기가 '사전-202-3법규부가0594'처럼 깨진 번호가 있어, 최종 일치는 doc_key로 가린다."""
    s = re.sub(r"^(?:기재부|재경부)", "", no.strip())
    toks = re.findall(r"[가-힣]+", s) + ([_serial(s)] if _serial(s) else [])
    return [t for t in dict.fromkeys(toks) if t] or [no]


def lookup_interp_numbers(no: str) -> list[dict]:
    """문서번호 정확 조회. 사용자 표기의 정규형이 DB 본번호·부번호와 같거나, 기관명을 앞에 붙였을 때 같으면 일치."""
    want = doc_key(re.sub(r"^(?:기재부|재경부)", "", no.strip()))
    serial = _serial(no)
    if len(want) < 5 or not serial:
        return []
    # 화면 번호(interp_number)가 다른 출처 번호일 때가 있어 국세청 공식 번호·부서 문서번호로도 찾는다
    # (예: interp_number 서면-2023-법규기본-2595 ↔ 국세청 서면-2024-법규기본-3219, 같은 원문)
    rows = cypher(
        "MATCH (i:Interpretation) "
        "WITH i, coalesce(i.interp_number, '') + ' ' + coalesce(i.source_doc_number, '') + ' ' + coalesce(i.department_doc_number, '') AS s "
        "WHERE all(t IN $toks WHERE s CONTAINS t) "
        "RETURN i.interp_id AS id, i.interp_number AS no, i.interp_title AS title, i.reply_date AS d, "
        "i.reply_org AS org, i.source_doc_number AS src, i.department_doc_number AS dep LIMIT 3000",
        {"toks": _number_tokens(no)},
    )
    hits = []
    for r in rows:
        keys = interp_number_keys(r.get("no") or "") + [doc_key(re.sub(r"\[.*$", "", x)) for x in (r.get("src"), r.get("dep")) if x]
        if any(k == want or (k.endswith(want) and re.match(r"[가-힣]", want)
                             and k[: len(k) - len(want)] in ("기획재정부", "재정경제부"))
               for k in keys):
            hits.append(r)
    return hits


def lookup_case_numbers(no: str) -> list[dict]:
    want = doc_key(no)
    serial = _serial(no)
    if len(want) < 5 or not serial:
        return []
    rows = cypher(
        "MATCH (c:Case) WHERE all(t IN $toks WHERE c.case_number CONTAINS t) "
        "RETURN c.case_id AS id, c.case_number AS no, c.case_name AS title, c.ruling_date AS d, "
        "coalesce(c.court_name, c.court_type) AS org LIMIT 3000",
        {"toks": _number_tokens(no)},
    )
    seen, hits = set(), []
    for r in rows:
        if doc_key(r.get("no") or "") == want and (r["no"], r.get("d")) not in seen:
            seen.add((r["no"], r.get("d")))
            hits.append(r)
    return hits


def _interp_rows(q: str, limit: int) -> list[dict]:
    return cypher(
        "CALL db.index.fulltext.queryNodes('interp_original_content_ft_v3', $q) YIELD node, score "
        f"WHERE {interpretation_body_guard('node')} "
        "RETURN node.interp_id AS id, node.interp_title AS title, node.interp_number AS no, node.reply_date AS d, "
        "node.reply_org AS org, node.source_doc_number AS src, substring(coalesce(node.content, ''), 0, 320) AS preview, "
        "node.maintenance_notice AS maintenance_notice, node.body_status AS body_status, "
        "node {.source_observed_at, .source_kind, .source_id, .latest_source_observation_status, .latest_source_observation_kind, "
        ".latest_source_observation_at, .latest_source_observation_raw_sha256, .latest_source_observation_url} AS source_observation, score "
        "ORDER BY score DESC LIMIT $limit",
        {"q": q, "limit": limit},
    )


def rank_interpretations(query: str, limit: int) -> list[dict]:
    """본문 점수·제목 핵심어·2-gram 겹침을 1/3씩. 각 항목은 0~1로 맞춘다."""
    body = _interp_rows(lucene_escape(query), INTERP_POOL)
    stems = ko_stems(query)
    title = _interp_rows("interp_title:(" + " ".join(s + "*" for s in stems) + ")", INTERP_POOL) if stems else []
    pool: dict = {}
    top = max((r["score"] for r in body), default=0) or 1
    for r in body:
        pool[r["id"]] = {**r, "_body": r["score"] / top, "_title": 0.0}
    for r in title:
        pool.setdefault(r["id"], {**r, "_body": 0.0})["_title"] = min(1.0, r["score"] / len(stems))
    for r in pool.values():
        if "_title" not in r:
            r["_title"] = 0.0
        r["_sim"] = bigram_sim(query, (r.get("title") or "") + " " + (r.get("preview") or "")[:200])
        r["_rank"] = (r["_body"] + r["_title"] + r["_sim"]) / 3
    return sorted(pool.values(), key=lambda r: r["_rank"], reverse=True)[:limit]


def t_search_interpretations(args: dict) -> str:
    query = str(args["query"])
    limit = min(int(args.get("limit", 5)), 15)
    found = {no: lookup_interp_numbers(no) for no in dict.fromkeys(x.group(0) for x in INTERP_NO_RE.finditer(query))}
    exact = [m for hits in found.values() for m in hits]
    missing = [no for no, hits in found.items() if not hits]
    rows = rank_interpretations(query, limit)
    if exact:
        ids = {e["id"] for e in exact}
        rows = [r for r in rows if r["id"] not in ids]
        head = cypher(
            f"MATCH (node:Interpretation) WHERE node.interp_id IN $ids AND {interpretation_body_guard('node')} "
            "RETURN true AS exact, node.interp_id AS id, node.interp_title AS title, node.interp_number AS no, node.reply_date AS d, "
            "node.reply_org AS org, node.source_doc_number AS src, substring(coalesce(node.content, ''), 0, 320) AS preview, "
            "node.maintenance_notice AS maintenance_notice, node.body_status AS body_status, "
            "node {.source_observed_at, .source_kind, .source_id, .latest_source_observation_status, .latest_source_observation_kind, "
            ".latest_source_observation_at, .latest_source_observation_raw_sha256, .latest_source_observation_url} AS source_observation",
            {"ids": list(ids)},
        )
        rows = (head + rows)[:limit]
    if not rows:
        return "검색 결과 없음. 단일 핵심 키워드로 다시 시도해 보세요."
    out = []
    if missing:
        out.append("[문서번호 미확인] " + ", ".join(missing) + ": 이 DB에서 같은 번호를 찾지 못했습니다. "
                   "번호 오기이거나 미수록 문서일 수 있어, 아래 결과는 그 문서가 아닙니다.")
    for r in rows:
        head = " ".join(x for x in [r.get("org"), r.get("no"), f"({fmt_date(r.get('d'))})" if r.get("d") else ""] if x)
        mark = "[문서번호 일치] " if r.get("exact") else ""
        out.append(f"{mark}[{head}] {r['title'] or ''}\n  {clip(r['preview'], 320)}")
        if r.get("src") and doc_key(re.sub(r"\[.*$", "", r["src"])) not in interp_number_keys(r.get("no") or ""):
            out.append(f"[국세청 공식 번호] {r['src']}. 같은 문서가 출처에 따라 다른 번호로 실려 있습니다.")
        if r.get("maintenance_notice"):
            out.append(r["maintenance_notice"])
        if notice := source_observation_notice(r.get("source_observation") or r):
            out.append("[최근 원천 확인 상태] " + notice)
        if r.get("body_status") == "partial_extraction":
            out.append("[자료 상태] 원문 그림·수식·표의 구조 완전성은 확인되지 않았으며 원본 확인이 필요합니다.")
        if r.get("body_status") == "reply_complete":
            out.append("[자료 범위] 공식 회신 원문을 확보한 자료입니다.")
        if r.get("body_status") in {"empty", "summary_only", "attachment_only", "source_empty", "unknown"}:
            out.append("[자료 상태] 원문 전문 확보가 완료되지 않은 자료입니다. 제공된 요지·회신의 범위를 확인하세요.")
    out.append(
        "\n※ 해석례는 개별 사실관계 전제임. 키워드 검색은 본문 일치이며 "
        "현행 조문에 대한 검증된 인용이 아니다. 문서번호로 원문 확인 요망."
    )
    return "\n\n".join(out)



LAWREF_RE = re.compile(
    r"((?:[가-힣]+에\s?관한\s?법률|(?:[가-힣]+\s및\s)?[가-힣]*?[가-힣]법)(?:\s?시행령|\s?시행규칙)?)\s*(제\s?\d+\s?조(?:\s?의\s?\d+)?)"
)


def t_verify_citations(args: dict) -> dict:
    """초안에 적힌 해석례·판례·결정례 문서번호와 '법령명 제N조'가 이 DB에 실제로 있는지 확인한다."""
    text = str(args.get("text") or "")[:20000]
    docs = []
    for m in dict.fromkeys(x.group(0).strip() for x in INTERP_NO_RE.finditer(text)):
        hits = lookup_interp_numbers(m)
        docs.append({"citation": m, "kind": "interpretation", "status": "found" if hits else "not_found",
                     "matches": [{"number": h["no"], **({"official_number": h["src"]} if h.get("src") and doc_key(h["src"]) not in interp_number_keys(h["no"]) else {}),
                                  "title": h.get("title"), "date": fmt_date(h.get("d")), "org": h.get("org")}
                                 for h in hits[:3]]})
    for m in dict.fromkeys(x.group(0).strip() for x in CASE_NO_RE.finditer(text)):
        hits = lookup_case_numbers(m)
        docs.append({"citation": m, "kind": "case", "status": "found" if hits else "not_found",
                     "matches": [{"number": h["no"], "title": h.get("title"), "date": fmt_date(h.get("d")), "court": h.get("org")}
                                 for h in hits[:3]]})
    arts = []
    for law, no in dict.fromkeys((a.strip(), re.sub(r"\s", "", b)) for a, b in LAWREF_RE.findall(text)):
        rows = cypher(q_current_article("replace(l.law_name, ' ', '') = $law"), {"law": re.sub(r"\s", "", law), "no": norm_article_no(no)})
        arts.append({"citation": f"{law} {no}", "status": "found" if rows else "not_found",
                     **({"law": rows[0]["law"], "title": rows[0]["title"], "enforcement_date": fmt_date(rows[0]["enf"])} if rows else {})})
    docs, arts = docs[:40], arts[:40]
    bad = [d["citation"] for d in docs + arts if d["status"] != "found"]
    return {
        "documents": docs,
        "articles": arts,
        "summary": f"문서번호 {len(docs)}건 · 조문 {len(arts)}건 중 이 DB에서 확인 안 된 것 {len(bad)}건",
        "not_found": bad,
        "notice": ("found는 같은 번호의 문서·현행 조문이 있다는 뜻일 뿐, 인용한 내용이 그 문서·조문과 맞는지는 "
                   "get_evidence·get_article로 본문을 확인해야 한다. not_found는 지어낸 번호일 수도, 이 DB 미수록일 수도 있다"
                   "(형사·민사 판결, 최근 공개분, 폐지·이동 조문). 조문은 현행 버전 기준이다."),
    }


# ---------------------------------------------------------------- 별표·시행예정

def t_search_annexes(args: dict) -> str:
    q = lucene_escape(str(args["query"]))
    law = args.get("law_name")
    limit = min(int(args.get("limit", 5)), 15)
    rows = cypher(Q_SEARCH_ANNEXES, {"q": q, "law": law, "limit": limit})
    if not rows:
        return (
            "검색 결과 없음. 별표는 세율표·기준금액·분류표가 많다 — "
            "'간이세액표', '미가공식료품', '과태료' 같은 표 제목 낱말로 시도해 보세요."
        )
    out = []
    for r in rows:
        body, representation = annex_body(r.get("source_body") or {"content": r.get("preview")})
        length = len(body) if representation == "extracted_attachment" else r["len"]
        out.append(f"[{r['law']} {r['no']}] {r['title']} ({length:,}자)\n  {clip(body, 240)}")
        if representation == "extracted_attachment":
            out.append("[첨부 추출정보 · 원문 아님] 공식 첨부 추출문이며 표의 행·열 구조는 미검증입니다.")
    out.append('\n※ 본문 전체는 get_annex(law_name, annex_number="별표 N")로 조회.')
    return "\n\n".join(out)


def t_get_annex(args: dict) -> str:
    law = str(args["law_name"]).strip()
    no = str(args["annex_number"]).strip()
    if not re.match(r"^(별표|서식)", no):
        no = f"별표 {no.lstrip('제').rstrip('호')}".strip()
    rows = cypher(Q_GET_ANNEX, {"law": law, "no": no})
    if not rows:
        near = cypher(Q_GET_ANNEX_NEAR, {"law": law})
        if near:
            listing = "\n".join(f"- [{r['no']}] {r['t']}" for r in near)
            return f"'{law} {no}'를 찾지 못함. 수록 별표·서식:\n{listing}"
        return f"'{law}'의 별표가 DB에 없음. list_laws로 법령명을 확인하세요."
    r = rows[0]
    body, representation = annex_body(r.get("source_body") or {"content": r.get("content")})
    out = [f"# {r['law']} [{r['no']}] {r['title']}"]
    if representation == "extracted_attachment":
        out.append("[첨부 추출정보 · 원문 아님] 공식 첨부 추출문이며 표의 행·열 구조는 미검증입니다.")
    if r["arts"]:
        out.append(f"근거 조문: {', '.join(r['arts'])}")
    out.append("")
    LIMIT = 6000
    out.append(body[:LIMIT])
    if len(body) > LIMIT:
        out.append(
            f"\n… (전체 {len(body):,}자 중 {LIMIT:,}자만 표시. 표가 길면 "
            "필요한 구간을 search_annexes로 좁혀 확인하거나 아래 원문 파일을 볼 것)"
        )
    if r["hwp"] or r["pdf"]:
        base = "https://www.law.go.kr"
        links = [f"{base}{u}" for u in (r["pdf"], r["hwp"]) if u]
        out.append("\n원문 파일: " + " · ".join(links))
    return "\n".join(out)


def t_list_upcoming(args: dict) -> str:
    law = args.get("law_name")
    rows = cypher(
        "MATCH (u:UpcomingVersion) WHERE ($law IS NULL OR u.law_name CONTAINS $law) "
        "RETURN u.law_name AS law, u.enforcement_date AS d, u.revision_type AS kind, "
        "u.changed_count AS n ORDER BY d, law",
        {"law": law},
    )
    if not rows:
        return (
            "시행예정 개정 없음."
            if law
            else "시행예정으로 수록된 개정이 없습니다."
        )
    out = [f"시행예정 개정 {len(rows)}건 (공포됐으나 아직 시행 전):"]
    for r in rows:
        out.append(
            f"- {fmt_date(r['d'])} 시행 · {r['law']} ({r['kind']}) — 달라지는 조문 {r['n']}개"
        )
    detail = cypher(
        "MATCH (u:UpcomingVersion)-[:CONTAINS]->(ua:UpcomingArticle) "
        "WHERE ($law IS NULL OR u.law_name CONTAINS $law) "
        "RETURN ua.law_name AS law, ua.enforcement_date AS d, "
        "ua.article_number AS no, ua.article_title AS title, ua.change_type AS kind "
        "ORDER BY d, law, no LIMIT 60",
        {"law": law},
    )
    if detail:
        out.append("\n달라지는 조문:")
        for x in detail:
            out.append(
                f"- {fmt_date(x['d'])} · {x['law']} {x['no']} {x['title']} [{x['kind']}]"
            )
        out.append("\n※ 개정문 본문은 get_article로 그 조문을 조회하면 함께 나온다.")
    return "\n".join(out)


# ---------------------------------------------------------------- 조세조약
#
# 조문 본문은 국세법령정보시스템이 국가당 조약 1건 단위로 준다. 두 가지를 조심해야 한다.
#  1) 개정의정서가 반영된 통합본이 아니다 — 스위스 제10조는 1980년 협약 원문(배당
#     10/15%)이고 이후 의정서가 반영돼 있지 않다.
#  2) 협약 전체를 대체한 신협정이 있으면 본문은 신협정인데 목록 발효일자는 옛 협약
#     것이 온다 — 싱가포르는 본문이 2019년 협정, 발효일자는 1981년이다.
# 그래서 조문에 발효일을 찍지 않는다. 대신 수집 파이프라인이 붙여 둔
# later_record_count / country_latest_effective_date 로 "뒤에 개정이 더 있다"를 알린다.


def _treaty_records(country: str) -> list:
    return cypher(
        "MATCH (t:Treaty) WHERE t.country CONTAINS $c "
        "RETURN t.country AS c, t.treaty_name AS name, t.signed_date AS sd, "
        "t.effective_date AS ed, t.treaty_url AS url, "
        "coalesce(t.body_article_count, 0) AS arts, "
        "coalesce(t.later_record_count, 0) AS later, "
        "coalesce(t.country_latest_effective_date, '') AS latest "
        "ORDER BY ed DESC",
        {"c": country},
    )


def _amendment_warning(recs: list) -> str:
    """본문을 든 레코드보다 늦게 발효된 조약이 있으면 경고 문구를 만든다."""
    holder = next((r for r in recs if r["arts"]), None)
    if not holder or not holder["later"]:
        return ""
    return (
        f"⚠ 이 국가에는 수록 본문보다 늦게 발효된 협약·의정서가 {holder['later']}건 있다"
        f"(최신 발효 {fmt_date(holder['latest'])}). 국세법령정보시스템 본문은 개정의정서를"
        " 반영하지 않은 경우가 있으니, 제한세율 등 수치는 협약·의정서 이력과"
        " law.go.kr 원문으로 반드시 확인할 것."
    )


def t_list_treaties(args: dict) -> str:
    country = str(args.get("country") or "").strip()
    if not country:
        rows = cypher(
            "MATCH (t:Treaty) WHERE coalesce(t.country, '') <> '' "
            "RETURN t.country AS c, count(t) AS n ORDER BY c"
        )
        total = sum(r["n"] for r in rows)
        return (
            f"조세조약 체결국 {len(rows)}개국 · 총 {total}건 (협약·개정의정서·교환각서 포함)\n\n"
            + ", ".join(f"{r['c']}({r['n']})" for r in rows)
            + "\n\n※ country를 지정하면 그 나라의 협약·의정서 이력을, "
            "search_treaties는 조문 본문을 검색한다."
        )
    recs = _treaty_records(country)
    if not recs:
        return (
            f"'{country}'와 체결된 조세조약이 DB에 없음. "
            "country 없이 호출해 체결국 목록을 먼저 확인하세요."
        )
    out = [f"{country} 조세조약 {len(recs)}건 (발효일 최신순):"]
    for r in recs:
        tag = f" — 조문 {r['arts']}개 수록" if r["arts"] else " — 조문 미수록"
        out.append(
            f"- {r['name']}\n  서명 {fmt_date(r['sd']) or '-'} · "
            f"발효 {fmt_date(r['ed']) or '-'}{tag}"
        )
    annex = _annexes(country)
    if annex:
        out.append(f"\n부속문서 {len(annex)}건 (개정의정서·교환각서·전문 등):")
        for a in annex:
            date = f" · {fmt_date(a['d'])}" if a["d"] else ""
            tag = " [개정]" if a["kind"] == "개정" else ""
            out.append(f"- {a['title']}{date}{tag}")
        out.append(
            '  → 전문은 get_treaty_article(country, article_number="<부속문서 제목>")'
        )
    warn = _amendment_warning(recs)
    if warn:
        out.append("\n" + warn)
    out.append("\n※ 조문 원문은 get_treaty_article, 키워드 검색은 search_treaties.")
    return "\n".join(out)


def t_search_treaties(args: dict) -> str:
    q = lucene_escape(str(args["query"]))
    country = str(args.get("country") or "").strip() or None
    limit = min(int(args.get("limit", 5)), 15)
    rows = cypher(
        "CALL db.index.fulltext.queryNodes('treaty_article_ft', $q) YIELD node, score "
        "WHERE ($c IS NULL OR node.country CONTAINS $c) "
        "RETURN node.is_whole_document AS is_whole_document, node.country AS c, node.article_number AS no, node.article_title AS title, "
        "substring(coalesce(node.content, node.content_en, ''), 0, 320) AS preview, score "
        "ORDER BY score DESC LIMIT $limit",
        {"q": q, "c": country, "limit": limit},
    )
    if not rows:
        return (
            "검색 결과 없음. 조약 용어(예: '사용료', '고정사업장', '배당', '이자')로 "
            "다시 시도하거나 list_treaties로 체결 여부를 확인하세요."
        )
    out = []
    for r in rows:
        out.append(
            f"[{r['c']} 조세조약 {r['no']}] {r['title'] or ''}\n  {clip(r['preview'], 320)}"
        )
        if r.get("is_whole_document"):
            out.append("[문서 범위 · 원문 아님] 조약 원문 문서 전체이며 개별 조항 또는 개정 반영 통합본이 아닙니다.")
    out.append(
        "\n※ 조문 전문은 get_treaty_article(country, article_number)로 조회. "
        "본문에 개정의정서가 반영되지 않았을 수 있으니 "
        "list_treaties(country)로 협약·의정서 이력을 함께 확인할 것."
    )
    return "\n\n".join(out)


def _annexes(country: str) -> list:
    return cypher(
        "MATCH (a:TreatyArticle) WHERE a.country CONTAINS $c AND a.is_annex = true "
        "RETURN a.article_number AS title, coalesce(a.annex_date, '') AS d, "
        "coalesce(a.annex_kind, '') AS kind ORDER BY d DESC, title",
        {"c": country},
    )


def _amendments_of(article_id: str) -> list:
    """이 조문을 고치는 부속문서. 해당 조문을 언급한 대목까지 함께 가져온다."""
    return cypher(
        "MATCH (x:TreatyArticle)-[:AMENDS]->(y:TreatyArticle {treaty_article_id: $id}) "
        "RETURN x.article_number AS title, coalesce(x.annex_date, '') AS d, "
        "coalesce(x.content, '') AS body ORDER BY d DESC",
        {"id": article_id},
    )


_AMEND_ANCHOR = "(?:협약|협정|조세조약|조세협약|이중과세방지협정|이중과세방지협약)"


def amend_excerpt(body: str, article_no: str) -> str:
    """개정 문서에서 그 조문을 고친다고 말한 대목을 잘라 낸다."""
    num = re.sub(r"[^0-9]", "", article_no)
    if not num:
        return ""
    m = re.search(_AMEND_ANCHOR + r"[^\n]{0,40}?제\s*" + num + r"\s*조", body or "")
    if not m:
        return ""
    chunk = (body or "")[m.start():m.start() + 420]
    return clip(chunk, 420)


def t_get_treaty_article(args: dict) -> str:
    country = str(args["country"]).strip()
    raw = str(args["article_number"]).strip()
    no = norm_article_no(raw)
    rows = cypher(
        "MATCH (t:Treaty)-[:CONTAINS]->(a:TreatyArticle) "
        "WHERE a.country CONTAINS $c AND a.article_number = $no "
        "AND coalesce(a.is_annex, false) = false "
        "RETURN a.treaty_article_id AS aid, a.country AS c, a.article_title AS title, "
        "a.content AS kr, a.content_en AS en, coalesce(a.source_url, t.treaty_url) AS url, t.treaty_name AS tname, "
        "a.is_whole_document AS is_whole_document "
        "LIMIT 1",
        {"c": country, "no": no},
    )
    if not rows:
        # 조문 번호가 아니면 부속문서 제목으로 본다 ('개정 의정서', '교환각서' …)
        rows = cypher(
            "MATCH (t:Treaty)-[:CONTAINS]->(a:TreatyArticle) "
            "WHERE a.country CONTAINS $c AND a.is_annex = true "
            "AND a.article_number CONTAINS $n "
            "RETURN a.treaty_article_id AS aid, a.country AS c, a.article_number AS title, "
            "a.content AS kr, a.content_en AS en, t.treaty_url AS url, t.treaty_name AS tname "
            "ORDER BY coalesce(a.annex_date, '') DESC LIMIT 1",
            {"c": country, "n": raw},
        )
        if rows:
            r = rows[0]
            out = [f"# {r['c']} 조세조약 부속문서 — {r['title']}", "",
                   (r["kr"] or "").strip() or "(국문본 없음)"]
            en = (r["en"] or "").strip()
            if en:
                out += ["", "## 영문본", en[:4000] + ("…" if len(en) > 4000 else "")]
            if r["url"]:
                out.append(f"\n출처: {r['url']}")
            return "\n".join(out)
    if not rows:
        near = cypher(
            "MATCH (a:TreatyArticle) WHERE a.country CONTAINS $c "
            "AND coalesce(a.is_annex, false) = false "
            "RETURN a.article_number AS n, a.article_title AS t ORDER BY a.seq LIMIT 30",
            {"c": country},
        )
        if near:
            listing = ", ".join(f"{r['n']} {r['t'] or ''}".strip() for r in near)
            annex = _annexes(country)
            tail = ("\n부속문서: " + ", ".join(a["title"] for a in annex)) if annex else ""
            return f"'{country} {no}' 조문 없음. 수록 조문: {listing} …{tail}"
        return (
            f"'{country} {no}' 조약 조문을 찾지 못함. "
            "list_treaties로 체결·수록 여부를 먼저 확인하세요."
        )
    r = rows[0]
    out = [
        f"# {r['c']} 조세조약 {no} {r['title'] or ''}",
        "",
        (r["kr"] or "").strip() or "(국문본 없음)",
    ]
    if r.get("is_whole_document"):
        out.insert(1, "[문서 범위 · 원문 아님] 조약 원문 문서 전체이며 개별 조항 또는 개정 반영 통합본이 아닙니다.")
    en = (r["en"] or "").strip()
    if en:
        out += ["", "## 영문본", en[:4000] + ("…" if len(en) > 4000 else "")]

    amends = _amendments_of(r["aid"])
    if amends:
        out.append(f"\n## ⚠ 이 조문을 개정한 부속문서 {len(amends)}건")
        out.append(
            "위 본문은 국세법령정보시스템이 주는 협약 원문이라 아래 개정이 반영돼 있지 "
            "않다. 제한세율 등 수치는 개정문을 먼저 확인할 것."
        )
        for a in amends:
            head = f"- {a['title']}" + (f" (발효 {fmt_date(a['d'])})" if a["d"] else "")
            out.append(head)
            ex = amend_excerpt(a["body"], no)
            if ex:
                out.append(f"  발췌: {ex}")
            out.append(
                f'  전문: get_treaty_article(country="{r["c"]}", '
                f'article_number="{a["title"]}")'
            )

    recs = _treaty_records(r["c"])
    origin = "국가법령정보센터 공식 조약 원문 문서" if r.get("is_whole_document") else "국세법령정보시스템 조세조약 본문"
    out.append(f"\n## 본문 출처\n{origin} — 수록 조약: {r['tname']}")
    if len(recs) > 1:
        out.append("\n## 해당국 협약·의정서 이력")
        for x in recs:
            mark = " (본문 수록)" if x["arts"] else ""
            out.append(f"- 발효 {fmt_date(x['ed']) or '-'} · {x['name']}{mark}")
    if not amends:
        warn = _amendment_warning(recs)
        if warn:
            out.append("\n" + warn)
    if r["url"]:
        out.append(f"\n출처: {r['url']}")
    return "\n".join(out)


def t_search_tax(args: dict) -> dict:
    return PublicGraphSearch(cypher).search(args)


def t_get_evidence(args: dict) -> dict:
    return PublicGraphSearch(cypher).get_evidence(args)


TOOLS = [
    {
        "name": "list_laws",
        "description": "이 DB에 수록된 현행 세법 법령 목록(법률·시행령·시행규칙)과 시행일을 반환한다. 국세청 조세법령 목록의 현행만 포함하며 목록 역사 법령은 제외. 다른 도구를 쓰기 전 수록 범위 확인용.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "fn": t_list_laws,
    },
    {
        "name": "search_articles",
        "description": "현행 세법 조문을 전문검색한다. 키워드는 조사 없는 명사 위주가 정확함 (예: '이월과세 배우자', '대손세액공제'). law_name으로 특정 법령 한정 가능.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "검색어 (명사 위주, 구문검색은 따옴표)"},
                "law_name": {"type": "string", "description": "법령명 필터 (부분 일치, 예: '소득세법')"},
                "limit": {"type": "integer", "description": "최대 결과 수 (기본 8, 최대 20)"},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        "fn": t_search_articles,
    },
    {
        "name": "get_article",
        "description": "특정 조문의 현행 원문 전체를 반환한다 (시행일·검증된 위임 상·하위법령·별표·검증된 인용 포함). 세율·한도·요건 등 정확한 수치는 반드시 이 도구로 원문을 확인할 것. 인용은 적용 확정이 아니다.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "law_name": {"type": "string", "description": "법령명 (예: '소득세법', '상속세 및 증여세법')"},
                "article_number": {"type": "string", "description": "조번호 (예: '제97조의2', '97조의2', '55')"},
            },
            "required": ["law_name", "article_number"],
            "additionalProperties": False,
        },
        "fn": t_get_article,
    },
    {
        "name": "get_article_history",
        "description": "특정 조문의 개정 연혁(버전 이력, 개정 표기, 검증·원천 여부)을 반환한다. '언제 바뀌었나' 질문에 사용. 미검증 버전은 적용 단정에 쓰지 말 것.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "law_name": {"type": "string"},
                "article_number": {"type": "string"},
            },
            "required": ["law_name", "article_number"],
            "additionalProperties": False,
        },
        "fn": t_get_article_history,
    },
    {
        "name": "search_cases",
        "description": "판례·불복 결정례를 전문검색한다 (대법원·고등법원 등 법원 판례, 조세심판원 심판례, 국세청 심사청구·이의신청·과세전적부심사 결정례). 단일 핵심 키워드가 정확함. 키워드 히트는 현행 조문에 대한 검증된 인용이 아니다.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "description": "기본 5, 최대 15"},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        "fn": t_search_cases,
    },
    {
        "name": "search_interpretations",
        "description": "국세청 질의회신·법제처·행정안전부 해석례를 전문검색한다. 실무 쟁점의 과세관청 입장 확인에 사용. 쟁점 문장을 그대로 넣어도 되고, 문서번호를 넣으면 그 문서를 맨 앞에 준다. 키워드 히트는 현행 조문에 대한 검증된 인용이 아니다.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "description": "기본 5, 최대 15"},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        "fn": t_search_interpretations,
    },
    {
        "name": "verify_citations",
        "description": "답변·의견서 초안에 적힌 해석례·판례·결정례 문서번호(예: 서면-2023-법규기본-2595, 기획재정부 소득세제과-1059, 2015두41937, 조심2023서1234)와 '법령명 제N조' 인용이 이 DB에 실제로 있는지 확인한다. 지어낸 번호·없는 조문을 거르는 용도이며, 존재 확인일 뿐 내용 일치 확인이 아니다.",
        "inputSchema": {
            "type": "object",
            "properties": {"text": {"type": "string", "description": "검증할 초안 본문 또는 문서번호 목록"}},
            "required": ["text"],
            "additionalProperties": False,
        },
        "fn": t_verify_citations,
    },
    {
        "name": "search_annexes",
        "description": "법령 별표·서식을 전문검색한다. 세율표·기준금액·한도·분류표는 조문이 아니라 별표에 있는 경우가 많다 (예: 근로소득 간이세액표, 면세 미가공식료품 분류표).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "검색어 (표 제목·항목 낱말)"},
                "law_name": {"type": "string", "description": "법령명 필터 (부분 일치)"},
                "limit": {"type": "integer", "description": "기본 5, 최대 15"},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        "fn": t_search_annexes,
    },
    {
        "name": "get_annex",
        "description": "특정 별표·서식의 본문을 반환한다. 조문이 '별표 N에 따른다'로 위임한 세율·기준금액을 확인할 때 사용.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "law_name": {"type": "string", "description": "법령명 (예: '소득세법 시행령')"},
                "annex_number": {"type": "string", "description": "'별표 2', '별표 1의3', '서식 1' 또는 숫자만"},
            },
            "required": ["law_name", "annex_number"],
            "additionalProperties": False,
        },
        "fn": t_get_annex,
    },
    {
        "name": "list_upcoming",
        "description": "공포됐으나 아직 시행 전인 개정(시행예정)을 반환한다. 시행일·법령·달라지는 조문 목록. 미래 과세기간이 걸린 질문에서 현행 조문만 보고 답하지 않도록 확인용.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "law_name": {"type": "string", "description": "법령명 필터 (생략 시 전체)"},
            },
            "additionalProperties": False,
        },
        "fn": t_list_upcoming,
    },
    {
        "name": "list_treaties",
        "description": "한국이 체결한 조세조약(이중과세방지협약) 수록 현황을 반환한다. country 없이 호출하면 체결국 목록, country를 주면 그 나라 조약(협약·개정의정서·교환각서) 목록과 발효일.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "체결국 한글명 (예: '미국', '일본'). 생략하면 전체 체결국 목록"},
            },
            "additionalProperties": False,
        },
        "fn": t_list_treaties,
    },
    {
        "name": "search_treaties",
        "description": "조세조약(이중과세방지협약) 조문을 전문검색한다. 비거주자·외국법인의 원천징수 제한세율, 고정사업장, 사용료·배당·이자 과세권 확인에 사용. country로 특정국 한정 가능. 본문에 개정의정서가 반영되지 않았을 수 있어 list_treaties로 이력 확인 필요.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "검색어 (예: '사용료', '고정사업장', '배당 제한세율')"},
                "country": {"type": "string", "description": "체결국 한글명 필터 (부분 일치)"},
                "limit": {"type": "integer", "description": "기본 5, 최대 15"},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        "fn": t_search_treaties,
    },
    {
        "name": "get_treaty_article",
        "description": "특정국 조세조약의 조문 원문 전체를 국문·영문으로 반환한다. 제한세율 등 정확한 수치는 반드시 이 도구로 원문을 확인할 것.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "체결국 한글명 (예: '미국')"},
                "article_number": {"type": "string", "description": "조번호 (예: '제12조', '12')"},
            },
            "required": ["country", "article_number"],
            "additionalProperties": False,
        },
        "fn": t_get_treaty_article,
    },
]
TOOLS.extend([
    {
        "name": "search_tax",
        "description": "로컬 tax-ai-agent와 같은 통합 그래프 탐색. law 시작 조문에서 위임을 최대 2단계 따라 판례·예규·해석례·참조조문·통칙·부칙 적용근거·별표를 모으고 전문검색으로 보충한다. 결과마다 실제 graph_paths, DB 원문, 시행일·적용버전·미확정 상태를 반환한다. 쟁점 자료 수집의 우선 도구. next_offset으로 결과를, get_evidence로 긴 원문을 이어 읽을 것.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "search_intents": {
                    "type": "object", "minProperties": 1, "additionalProperties": False,
                    "description": "유형별 검색 의도. 전체 1~8개. 예: {law:[소득세법 제97조의2],case:[이월과세],ruling:[배우자 증여]}",
                    "properties": {name: {"type": "array", "maxItems": 3,
                                          "items": {"type": "string", "minLength": 1, "maxLength": 300}}
                                   for name in INTENT_TYPES},
                },
                "as_of": {"type": "string", "description": "선택: YYYY-MM-DD 또는 YYYYMMDD. 조문별 당시 버전을 별도 반환하며 불명 시 현행으로 대체하지 않음."},
                "offset": {"type": "integer", "minimum": 0, "maximum": 100000, "default": 0},
                "limit": {"type": "integer", "minimum": 1, "maximum": 25, "default": 10},
                "result_set_id": {"type": "string", "pattern": "^[a-f0-9]{64}$", "description": "다음 페이지는 이전 응답의 식별자를 전달하여 검색 집합 변경을 감지"},
            },
            "required": ["search_intents"], "additionalProperties": False,
        },
        "outputSchema": {"type": "object", "required": ["schema_version", "results", "total_results", "notice"]},
        "annotations": {"readOnlyHint": True, "destructiveHint": False},
        "fn": t_search_tax,
    },
    {
        "name": "get_evidence",
        "description": "search_tax가 찾은 근거의 DB 원문을 읽는다. 판례·해석례·예규·조문·통칙·별표·부칙·조약·역사버전을 지원. 긴 본문은 next_offset까지 반복하면 누락 없이 읽을 수 있다. summary_only는 원문 전문이 없는 요지임. 적용 시점 미해소를 현행으로 보충하지 말 것.",
        "inputSchema": {
            "type": "object", "properties": {
                "evidence_type": {"type": "string", "enum": list(NODE_TYPES)},
                "evidence_id": {"type": "string", "minLength": 1, "maxLength": 500},
                "offset": {"type": "integer", "minimum": 0, "maximum": 10000000, "default": 0},
                "limit": {"type": "integer", "minimum": 1, "maximum": 24000, "default": 12000},
                "source_field": {"type": "string", "enum": sorted({f for fields in BODY_FIELDS.values() for f in fields}),
                                 "description": "선택: original_text.available_fields의 다른 원문 필드 조회"},
            }, "required": ["evidence_type", "evidence_id"], "additionalProperties": False,
        },
        "outputSchema": {"type": "object", "required": ["found", "evidence_type", "evidence_id"]},
        "annotations": {"readOnlyHint": True, "destructiveHint": False},
        "fn": t_get_evidence,
    },
])
TOOL_MAP = {t["name"]: t for t in TOOLS}

# ---------------------------------------------------------------- rate limit / 로그

_BLOCKED = [ipaddress.ip_network(n) for n in BLOCKED_NETS]


def ip_blocked(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return any(addr in net for net in _BLOCKED)


_rate_lock = threading.Lock()
_rate: dict[str, deque] = defaultdict(deque)


def rate_ok(ip: str, bucket: str = "call", limit: int = RATE_PER_MIN) -> bool:
    now = time.time()
    key = f"{bucket}:{ip}"
    with _rate_lock:
        q = _rate[key]
        while q and now - q[0] > 60:
            q.popleft()
        if len(q) >= limit:
            return False
        q.append(now)
        if len(_rate) > 10000:  # 메모리 보호
            _rate.clear()
    return True


_log_lock = threading.Lock()


def log_usage(ip: str, method: str, tool: str, ms: int, status: str, ua: str = "", client: str = ""):
    """ua는 요청 헤더, client는 initialize의 clientInfo(이름/버전) — 어떤 프로그램으로 붙는지 보려고."""
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        rec = {
            "ts": datetime.now().isoformat(timespec="seconds"),  # noqa: DTZ005 - Preserve local log timestamps.
            "ip": ip, "method": method, "tool": tool, "ms": ms, "status": status,
        }
        if ua:
            rec["ua"] = ua[:120]
        if client:
            rec["client"] = client[:80]
        path = os.path.join(LOG_DIR, f"usage-{datetime.now():%Y%m%d}.jsonl")  # noqa: DTZ005 - Local daily buckets.
        with _log_lock, open(path, "a") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError:
        pass


# 질의 기록 — 무엇을 찾는지 분류하려고 도구 인자 중 검색어·법령·조문만 남긴다.
# 이메일·전화·주민번호·긴 숫자는 쓰기 전에 가리고, 파일은 QUERY_RETENTION_DAYS 뒤 지운다.
QUERY_RETENTION_DAYS = 30
QUERY_ARG_KEYS = ("query", "law_name", "article_number", "annex_number", "country", "as_of", "evidence_type", "search_intents")
_MASKS = (
    (re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"), "[이메일]"),
    (re.compile(r"\d{6}\s*-\s*[1-8]\d{6}"), "[주민번호]"),
    (re.compile(r"01[016789][\s-]?\d{3,4}[\s-]?\d{4}"), "[전화]"),
    (re.compile(r"\d[\d,.-]{6,}\d"), "[숫자]"),
)
_query_purged = {"day": ""}


def mask_text(value) -> str:
    text = str(value)[:300]
    for pattern, repl in _MASKS:
        text = pattern.sub(repl, text)
    return text


def query_args(args: dict) -> dict:
    out = {}
    for key in QUERY_ARG_KEYS:
        value = args.get(key)
        if value in (None, "", {}, []):
            continue
        if key == "search_intents" and isinstance(value, dict):
            out[key] = {str(k)[:20]: [mask_text(v) for v in (vs if isinstance(vs, list) else [vs])[:3]] for k, vs in list(value.items())[:8]}
        else:
            out[key] = mask_text(value)
    return out


def log_query(ip: str, tool: str, args: dict, status: str, ua: str = ""):
    try:
        rec_args = query_args(args if isinstance(args, dict) else {})
        if not rec_args:
            return
        os.makedirs(LOG_DIR, exist_ok=True)
        now = datetime.now()  # noqa: DTZ005 - Local daily buckets like usage logs.
        day = f"{now:%Y%m%d}"
        rec = {"ts": now.isoformat(timespec="seconds"), "ip": ip, "tool": tool, "status": status, "args": rec_args}
        if ua:
            rec["ua"] = ua[:120]
        with _log_lock:
            if _query_purged["day"] != day:
                _query_purged["day"] = day
                cutoff = f"{datetime.fromtimestamp(now.timestamp() - QUERY_RETENTION_DAYS * 86400):%Y%m%d}"  # noqa: DTZ006
                for name in os.listdir(LOG_DIR):
                    if name.startswith("queries-") and name.endswith(".jsonl") and name[8:16] < cutoff:
                        os.remove(os.path.join(LOG_DIR, name))
            with open(os.path.join(LOG_DIR, f"queries-{day}.jsonl"), "a") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError:
        pass


# ---------------------------------------------------------------- JSON-RPC 디스패치

def rpc_error(id_, code, message):
    return {"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": message}}


def handle_message(msg: dict, ip: str, ua: str = ""):
    """JSON-RPC 메시지 처리. 응답 dict 반환, 알림(id 없음)이면 None."""
    method = msg.get("method", "")
    id_ = msg.get("id")
    params = msg.get("params") or {}
    if id_ is None:
        return None

    if method in ("initialize", "tools/list") and not rate_ok(
        ip, "handshake", HANDSHAKE_PER_MIN
    ):
        log_usage(ip, method, "", 0, "rate_limited", ua)
        return rpc_error(id_, -32000, "요청이 너무 잦습니다. 잠시 후 다시 시도해 주세요.")

    if method == "initialize":
        client_proto = str(params.get("protocolVersion", DEFAULT_PROTOCOL))
        proto = client_proto if client_proto in SUPPORTED_PROTOCOLS else DEFAULT_PROTOCOL
        info = params.get("clientInfo") if isinstance(params.get("clientInfo"), dict) else {}
        client = "/".join(str(info.get(k) or "").strip()[:40] for k in ("name", "version")).strip("/")
        log_usage(ip, "initialize", "", 0, "ok", ua, client)
        return {
            "jsonrpc": "2.0", "id": id_,
            "result": {
                "protocolVersion": proto,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION,
                               "title": "한국 세법 그래프 DB"},
                "instructions": INSTRUCTIONS,
            },
        }

    if method == "ping":
        return {"jsonrpc": "2.0", "id": id_, "result": {}}

    if method == "tools/list":
        tools = [{k: t[k] for k in ("name", "description", "inputSchema", "outputSchema", "annotations") if k in t} for t in TOOLS]
        log_usage(ip, "tools/list", "", 0, "ok", ua)
        return {"jsonrpc": "2.0", "id": id_, "result": {"tools": tools}}

    if method == "tools/call":
        t0 = time.time()
        name = params.get("name", "")
        tool = TOOL_MAP.get(name)
        if not tool:
            return rpc_error(id_, -32602, f"알 수 없는 도구: {name}")
        if not rate_ok(ip):
            log_usage(ip, "tools/call", name, 0, "rate_limited", ua)
            return {
                "jsonrpc": "2.0", "id": id_,
                "result": {"content": [{"type": "text",
                           "text": "호출 한도 초과(분당 30회). 잠시 후 다시 시도해 주세요."}],
                           "isError": True},
            }
        output = None
        try:
            output = tool["fn"](params.get("arguments") or {})
            text = json.dumps(output, ensure_ascii=False) if isinstance(output, dict) else output
            status, is_err = "ok", False
        except BusyError as e:
            text, status, is_err = str(e), "busy", True
        except KeyError as e:
            text, status, is_err = f"필수 인자 누락: {e}", "bad_args", True
        except ValueError as e:
            text, status, is_err = str(e), "bad_args", True
        except TimeoutError:
            text, status, is_err = "통합 검색 시간·조회 한도 초과. 검색 의도를 좁혀 다시 호출하세요.", "timeout", True
        except urllib.error.URLError:
            text, status, is_err = "DB 연결 실패. 잠시 후 다시 시도해 주세요.", "db_down", True
        except Exception as e:  # noqa: BLE001 - Serialize unexpected tool errors at the MCP boundary.
            text, status, is_err = f"조회 실패: {type(e).__name__}", "error", True
        log_usage(ip, "tools/call", name, int((time.time() - t0) * 1000), status, ua)
        log_query(ip, name, params.get("arguments") or {}, status, ua)
        result = {"content": [{"type": "text", "text": text}], "isError": is_err}
        if isinstance(output, dict) and not is_err:
            result["structuredContent"] = output
        return {
            "jsonrpc": "2.0", "id": id_,
            "result": result,
        }

    return rpc_error(id_, -32601, f"지원하지 않는 메서드: {method}")


# ---------------------------------------------------------------- 레거시 HTTP+SSE 세션

_sessions: dict = {}
_sessions_lock = threading.Lock()
MAX_SESSIONS = 50


# ---------------------------------------------------------------- MCP HTTP 핸들러


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = f"{SERVER_NAME}/{SERVER_VERSION}"

    def log_message(self, *a):  # 기본 stderr 로그 억제
        pass

    def client_ip(self) -> str:
        return (
            self.headers.get("CF-Connecting-IP")
            or self.headers.get("X-Forwarded-For", "").split(",")[0].strip()
            or self.client_address[0]
        )

    def reject_blocked(self, ip: str) -> bool:
        """차단 IP면 403으로 끊고 True. 로그는 IP당 분당 1줄로 눌러 통계 오염을 막는다."""
        if not ip_blocked(ip):
            return False
        if rate_ok(ip, "blocklog", 1):
            log_usage(ip, "", "", 0, "blocked")
        self.send_json({"error": "Forbidden"}, 403)
        return True

    def send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.cors()
        self.end_headers()
        self.wfile.write(body)

    def send_empty(self, status=202):
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.cors()
        self.end_headers()

    def cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header(
            "Access-Control-Allow-Headers",
            "Content-Type, Authorization, Mcp-Session-Id, MCP-Protocol-Version, Last-Event-ID",
        )

    def do_OPTIONS(self):
        self.send_empty(204)

    # ---- 응답 모드: 클라이언트 Accept에 따라 JSON 또는 SSE(단일 이벤트 후 종료)

    def _wants_sse(self) -> bool:
        acc = self.headers.get("Accept", "")
        return "text/event-stream" in acc and "application/json" not in acc

    def send_sse_single(self, obj):
        body = ("event: message\ndata: " + json.dumps(obj, ensure_ascii=False) + "\n\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(body)))
        self.cors()
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path.rstrip("/")
        if path in ("", "/healthz"):
            try:
                n = cypher(
                    "MATCH (l:Law) WHERE " + current_law_guard(law="l") + " RETURN count(l) AS n"
                )[0]["n"]
                self.send_json({"status": "ok", "server": SERVER_NAME, "current_laws": n})
            except Exception:  # noqa: BLE001 - Health must report degradation for all DB failures.
                self.send_json({"status": "degraded", "server": SERVER_NAME}, 503)
            return
        if path == "/sse":
            if self.reject_blocked(self.client_ip()):
                return
            self.legacy_sse()
            return
        # /mcp GET: 서버 주도 스트림 미지원 (stateless) — 스펙상 405 허용
        if path == "/mcp":
            self.send_json({"error": "Method Not Allowed"}, 405)
            return
        # 그 밖의 경로(/.well-known/oauth-* 포함)는 404 — 인증 없는 서버임을 클라이언트가 알게
        self.send_json({"error": "Not Found"}, 404)

    def do_POST(self):
        ip = self.client_ip()
        if self.reject_blocked(ip):
            return
        path = urlparse(self.path).path.rstrip("/")
        if path == "/messages":
            self.legacy_messages(ip)
            return
        if path not in ("", "/mcp"):
            self.send_json({"error": "Not Found"}, 404)
            return
        msg = self._read_rpc()
        if msg is None:
            return
        resp = handle_message(msg, ip, self.headers.get("User-Agent", ""))
        if resp is None:  # 알림
            self.send_empty(202)
        elif self._wants_sse():
            self.send_sse_single(resp)
        else:
            self.send_json(resp)

    def _read_rpc(self):
        """요청 본문을 JSON-RPC 메시지로 파싱. 실패 시 응답까지 보내고 None."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length > MAX_BODY:
                self.send_json(rpc_error(None, -32600, "요청이 너무 큽니다"), 400)
                return None
            msg = json.loads(self.rfile.read(length).decode())
        except (ValueError, UnicodeError, OSError):
            self.send_json(rpc_error(None, -32700, "JSON 파싱 실패"), 400)
            return None
        if isinstance(msg, list):
            self.send_json(rpc_error(None, -32600, "batch 미지원"), 400)
            return None
        return msg

    # ---- 레거시 HTTP+SSE 트랜스포트 (2024-11-05): GET /sse + POST /messages

    def legacy_sse(self):
        with _sessions_lock:
            if len(_sessions) >= MAX_SESSIONS:
                self.send_json({"error": "세션 한도 초과"}, 503)
                return
            sid = uuid.uuid4().hex
            q = queue.Queue()
            _sessions[sid] = q
        log_usage(self.client_ip(), "sse/open", "", 0, "ok")
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.cors()
            self.end_headers()
            self.wfile.write(f"event: endpoint\ndata: /messages?sessionId={sid}\n\n".encode())
            self.wfile.flush()
            while True:
                try:
                    item = q.get(timeout=15)
                except queue.Empty:
                    # Cloudflare 유휴 차단 방지 키프얼라이브
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    continue
                self.wfile.write(
                    ("event: message\ndata: " + json.dumps(item, ensure_ascii=False) + "\n\n").encode()
                )
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            with _sessions_lock:
                _sessions.pop(sid, None)

    def legacy_messages(self, ip):
        sid = (parse_qs(urlparse(self.path).query).get("sessionId") or [""])[0]
        with _sessions_lock:
            q = _sessions.get(sid)
        if q is None:
            self.send_json({"error": "알 수 없는 세션"}, 404)
            return
        msg = self._read_rpc()
        if msg is None:
            return
        resp = handle_message(msg, ip, self.headers.get("User-Agent", ""))
        if resp is not None:
            q.put(resp)
        self.send_empty(202)


class Server(ThreadingHTTPServer):
    """기동 시 역방향 DNS를 조회하지 않는 HTTP 서버.

    HTTPServer.server_bind() 는 socket.getfqdn(host) 로 서버 이름을 채운다. DNS가
    느리거나 막힌 환경에서는 그 한 줄에서 몇 분씩 멈춰 포트가 열리지 않는다
    (2026-09-01 launchd 아래에서 실제로 겪었다 — 셸에서는 멀쩡했다).
    server_name 은 응답에 쓰지 않으므로 조회 없이 채운다.
    """

    def server_bind(self):
        TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = host
        self.server_port = port


def main():
    httpd = Server((BIND, PORT), Handler)
    httpd.daemon_threads = True
    print(f"[{SERVER_NAME}] listening on {BIND}:{PORT} (neo4j: {NEO4J_HTTP})")
    httpd.serve_forever()


if __name__ == "__main__":
    main()

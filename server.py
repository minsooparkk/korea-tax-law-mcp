#!/usr/bin/env python3
"""세법 그래프 MCP 서버 — 공개용 read-only Streamable HTTP (stateless).

한국 세법 법령·판례·심판례·해석례·조세조약 그래프 DB(Neo4j)를 MCP 도구로 노출한다.
- 수록 범위: 국세 + 지방세 현행 법령(법·령·칙), 판례·조세심판원 결정례,
  국세청·법제처·행정안전부 해석례, 조세조약(체결국별 협약·의정서 조문)
- 의존성 없음: 파이썬 표준 라이브러리만 사용 (3.9+)
- Neo4j 접근: HTTP Query API v2 (읽기 전용 파라미터 쿼리만, raw cypher 노출 없음)
- 방어: IP당 분당 호출 제한, 전역 동시 쿼리 제한, 쿼리 타임아웃
- 로그: logs/mcp/usage-YYYYMMDD.jsonl

실행:  python3 mcp/server.py            (기본 127.0.0.1:8788)
       PORT=9000 python3 mcp/server.py
엔드포인트: POST /mcp  (Cloudflare Tunnel 뒤에서 mcp.taxjarvis.com/mcp 로 공개)
참고: scripts/tax_db_mcp.py(사설망 Tailscale 전용, 서브프로세스 방식)와는 별개 서비스다.
이 서버는 공개용으로 read-only 파라미터 쿼리 + rate limit + 사용량 로그를 갖춤.
"""
from __future__ import annotations

import base64
import json
import os
import queue
import re
import threading
import time
import urllib.error
import urllib.request
import uuid
from urllib.parse import parse_qs, urlparse
from collections import defaultdict, deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(SCRIPT_DIR)
LOG_DIR = os.environ.get("MCP_LOG_DIR") or os.path.join(REPO, "logs", "mcp")
BIND = os.environ.get("BIND", "127.0.0.1")
PORT = int(os.environ.get("PORT", "8788"))  # 8787은 사설망용 tax_db_mcp 계열이 사용 중

SERVER_NAME = "korea-tax-law"
SERVER_VERSION = "0.4.0"
SUPPORTED_PROTOCOLS = {"2024-11-05", "2025-03-26", "2025-06-18"}
DEFAULT_PROTOCOL = "2025-06-18"

RATE_PER_MIN = 30          # IP당 분당 호출 한도
GLOBAL_CONCURRENCY = 4     # 동시 Neo4j 쿼리 한도
NEO4J_TIMEOUT = 8          # 쿼리 타임아웃(초)
MAX_BODY = 64 * 1024       # 요청 본문 한도

INSTRUCTIONS = (
    "한국 세법 법령 그래프 DB입니다. 국세·지방세 현행 조문(법률·시행령·시행규칙), 판례, "
    "조세심판원 결정례, 국세청·법제처·행정안전부 해석례, 조세조약을 검색·조회할 수 있습니다. "
    "조문은 '현행 시행 버전' 기준이며 각 결과에 시행일이 표기됩니다. "
    "수록 법령 목록은 list_laws로, 조세조약 체결국은 list_treaties로 확인하세요"
    "(미수록 법령은 국가법령정보센터 law.go.kr 참조). "
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


# ---------------------------------------------------------------- 도구 구현

def t_list_laws(args: dict) -> str:
    rows = cypher(
        "MATCH (l:Law) WHERE l.is_current = true "
        "RETURN l.law_name AS name, l.law_type AS type, l.enforcement_date AS enf, "
        "coalesce(l.ministry, '') AS ministry "
        "ORDER BY l.law_name"
    )
    # 조세특례제한법은 소관이 '재정경제부,행정안전부' 공동이라 지방세로 세면 안 된다.
    def kind(m):
        if "," in m:
            return "공통"
        return "지방세" if "행정안전부" in m else "국세"

    counts = {"국세": 0, "지방세": 0, "공통": 0}
    for r in rows:
        counts[kind(r["ministry"])] += 1
    lines = [
        f"수록 현행 법령 {len(rows)}건 "
        f"(국세 {counts['국세']} · 지방세 {counts['지방세']} · 국세/지방세 공통 {counts['공통']}):"
    ]
    for r in rows:
        k = kind(r["ministry"])
        tag = "" if k == "국세" else f" [{k}]"
        lines.append(f"- {r['name']} ({r['type']}, 시행 {fmt_date(r['enf'])}){tag}")
    lines.append("\n※ 목록에 없는 법령은 이 DB에 미수록. 국가법령정보센터(law.go.kr) 확인 요망.")
    lines.append("※ 조세조약은 별도 수록 — list_treaties로 확인.")
    return "\n".join(lines)


def t_search_articles(args: dict) -> str:
    q = lucene_escape(str(args["query"]))
    law = args.get("law_name")
    limit = min(int(args.get("limit", 8)), 20)
    rows = cypher(
        "CALL db.index.fulltext.queryNodes('article_content_ft', $q) YIELD node, score "
        "MATCH (l:Law)-[:CONTAINS]->(node) "
        "WHERE l.is_current = true AND ($law IS NULL OR l.law_name CONTAINS $law) "
        "RETURN l.law_name AS law, l.enforcement_date AS enf, node.article_number AS no, "
        "node.article_title AS title, substring(node.article_content, 0, 260) AS preview, score "
        "ORDER BY score DESC LIMIT $limit",
        {"q": q, "law": law, "limit": limit},
    )
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
                "MATCH (l:Law)-[:CONTAINS]->(a:Article) "
                "WHERE l.is_current = true AND ($law IS NULL OR l.law_name CONTAINS $law) "
                "AND (a.article_title CONTAINS $raw OR a.article_content CONTAINS $raw) "
                "RETURN l.law_name AS law, l.enforcement_date AS enf, a.article_number AS no, "
                "a.article_title AS title, substring(a.article_content, 0, 260) AS preview, 0 AS score "
                "ORDER BY CASE WHEN a.article_title CONTAINS $raw THEN 0 ELSE 1 END "
                "LIMIT $limit",
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
    out.append("\n※ 원문 전체는 get_article로 조회.")
    return "\n\n".join(out)


def t_get_article(args: dict) -> str:
    law = str(args["law_name"]).strip()
    no = norm_article_no(args["article_number"])
    stmt = (
        "MATCH (l:Law)-[:CONTAINS]->(a:Article) "
        "WHERE {cond} AND l.is_current = true AND a.article_number = $no "
        "RETURN l.law_name AS law, l.enforcement_date AS enf, a.article_id AS aid, "
        "a.article_number AS no, a.article_title AS title, a.article_content AS content "
        "LIMIT 1"
    )
    rows = cypher(stmt.format(cond="l.law_name = $law"), {"law": law, "no": no})
    if not rows:  # 부분 법령명 허용 (예: '상속세' → '상속세 및 증여세법')
        rows = cypher(stmt.format(cond="l.law_name CONTAINS $law"), {"law": law, "no": no})
    if not rows:
        return f"'{law} {no}' 조문을 찾지 못함. list_laws로 정확한 법령명을 확인하세요."
    r = rows[0]
    deleg = cypher(
        "MATCH (a:Article {article_id: $aid})-[:DELEGATES_TO]->(d:Article)<-[:CONTAINS]-(dl:Law) "
        "WHERE dl.is_current = true "
        "RETURN DISTINCT dl.law_name AS law, d.article_number AS no, d.article_title AS title LIMIT 10",
        {"aid": r["aid"]},
    )
    out = [
        f"# {r['law']} {r['no']} {r['title'] or ''}",
        f"(현행, 시행 {fmt_date(r['enf'])})",
        "",
        r["content"] or "",
    ]
    if deleg:
        out.append("\n## 위임 하위법령 조문")
        for d in deleg:
            out.append(f"- {d['law']} {d['no']} {d['title'] or ''}")
    out.append(f"\n출처: {r['law']} {r['no']} (시행 {fmt_date(r['enf'])} 기준)")
    return "\n".join(out)


def t_get_article_history(args: dict) -> str:
    law = str(args["law_name"]).strip()
    no = norm_article_no(args["article_number"])
    rows = cypher(
        "MATCH (l:Law)-[:CONTAINS]->(a:Article) "
        "WHERE l.law_name CONTAINS $law AND a.article_number = $no "
        "WITH DISTINCT a LIMIT 1 "
        "MATCH (a)-[:HAS_VERSION]->(v:ArticleVersion) "
        "RETURN v.enforcement_date AS enf, v.valid_from AS vfrom "
        "ORDER BY v.valid_from DESC LIMIT 20",
        {"law": law, "no": no},
    )
    cur = cypher(
        "MATCH (l:Law)-[:CONTAINS]->(a:Article) "
        "WHERE l.law_name CONTAINS $law AND l.is_current = true AND a.article_number = $no "
        "RETURN a.article_content AS content LIMIT 1",
        {"law": law, "no": no},
    )
    out = [f"# {law} {no} 개정 연혁"]
    if cur:
        marks = re.findall(r"<개정[^>]*>|<신설[^>]*>|\[전문개정[^\]]*\]", cur[0].get("content") or "")
        if marks:
            out.append("현행 조문 내 개정 표기: " + ", ".join(dict.fromkeys(marks)))
    if rows:
        out.append("\n조문 버전 이력 (최근순):")
        for r in rows:
            out.append(f"- 시행 {fmt_date(r['enf'])} (적용 시점 {fmt_date(r['vfrom'])})")
    if len(out) == 1:
        return f"'{law} {no}'의 연혁 정보를 찾지 못함."
    out.append("\n※ 버전 이력은 수집 시점에 따라 일부 누락 가능. 확정 판단은 law.go.kr 연혁 대조 요망.")
    return "\n".join(out)


def t_search_cases(args: dict) -> str:
    q = lucene_escape(str(args["query"]))
    limit = min(int(args.get("limit", 5)), 15)
    rows = cypher(
        "CALL db.index.fulltext.queryNodes('case_content_ft', $q) YIELD node, score "
        "RETURN node.case_number AS no, node.case_name AS name, node.court_type AS court, "
        "node.ruling_date AS d, "
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
    out.append("\n※ 요지 발췌임. 인용 시 사건번호로 원문 확인 요망.")
    return "\n\n".join(out)


def t_search_interpretations(args: dict) -> str:
    q = lucene_escape(str(args["query"]))
    limit = min(int(args.get("limit", 5)), 15)
    rows = cypher(
        "CALL db.index.fulltext.queryNodes('interp_content_ft', $q) YIELD node, score "
        "RETURN node.interp_title AS title, node.interp_number AS no, node.reply_date AS d, "
        "node.reply_org AS org, substring(coalesce(node.content, ''), 0, 320) AS preview, score "
        "ORDER BY score DESC LIMIT $limit",
        {"q": q, "limit": limit},
    )
    if not rows:
        return "검색 결과 없음. 단일 핵심 키워드로 다시 시도해 보세요."
    out = []
    for r in rows:
        head = " ".join(x for x in [r.get("org"), r.get("no"), f"({fmt_date(r.get('d'))})" if r.get("d") else ""] if x)
        out.append(f"[{head}] {r['title'] or ''}\n  {clip(r['preview'], 320)}")
    out.append("\n※ 해석례는 개별 사실관계 전제임. 문서번호로 원문 확인 요망.")
    return "\n\n".join(out)


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
        "RETURN node.country AS c, node.article_number AS no, node.article_title AS title, "
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
        "a.content AS kr, a.content_en AS en, t.treaty_url AS url, t.treaty_name AS tname "
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
    out.append(f"\n## 본문 출처\n국세법령정보시스템 조세조약 본문 — 수록 조약: {r['tname']}")
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


TOOLS = [
    {
        "name": "list_laws",
        "description": "이 DB에 수록된 현행 세법 법령 목록(법률·시행령·시행규칙)과 시행일을 반환한다. 다른 도구를 쓰기 전 수록 범위 확인용.",
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
        "description": "특정 조문의 현행 원문 전체를 반환한다 (시행일·위임 하위법령 조문 포함). 세율·한도·요건 등 정확한 수치는 반드시 이 도구로 원문을 확인할 것.",
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
        "description": "특정 조문의 개정 연혁(버전 이력, 개정 표기)을 반환한다. '언제 바뀌었나' 질문에 사용.",
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
        "description": "판례·조세심판원 결정례를 전문검색한다 (대법원·고등법원 판례, 조세심판원 심판례 15만+건). 단일 핵심 키워드가 정확함.",
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
        "description": "국세청 질의회신·법제처 해석례를 전문검색한다 (14만+건). 실무 쟁점의 과세관청 입장 확인에 사용.",
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
TOOL_MAP = {t["name"]: t for t in TOOLS}

# ---------------------------------------------------------------- rate limit / 로그

_rate_lock = threading.Lock()
_rate: dict[str, deque] = defaultdict(deque)


def rate_ok(ip: str) -> bool:
    now = time.time()
    with _rate_lock:
        q = _rate[ip]
        while q and now - q[0] > 60:
            q.popleft()
        if len(q) >= RATE_PER_MIN:
            return False
        q.append(now)
        if len(_rate) > 10000:  # 메모리 보호
            _rate.clear()
    return True


_log_lock = threading.Lock()


def log_usage(ip: str, method: str, tool: str, ms: int, status: str):
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        rec = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "ip": ip, "method": method, "tool": tool, "ms": ms, "status": status,
        }
        path = os.path.join(LOG_DIR, f"usage-{datetime.now():%Y%m%d}.jsonl")
        with _log_lock, open(path, "a") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError:
        pass


# ---------------------------------------------------------------- JSON-RPC 디스패치

def rpc_error(id_, code, message):
    return {"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": message}}


def handle_message(msg: dict, ip: str):
    """JSON-RPC 메시지 처리. 응답 dict 반환, 알림(id 없음)이면 None."""
    method = msg.get("method", "")
    id_ = msg.get("id")
    params = msg.get("params") or {}
    if id_ is None:
        return None

    if method == "initialize":
        client_proto = str(params.get("protocolVersion", DEFAULT_PROTOCOL))
        proto = client_proto if client_proto in SUPPORTED_PROTOCOLS else DEFAULT_PROTOCOL
        log_usage(ip, "initialize", "", 0, "ok")
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
        tools = [{k: t[k] for k in ("name", "description", "inputSchema")} for t in TOOLS]
        log_usage(ip, "tools/list", "", 0, "ok")
        return {"jsonrpc": "2.0", "id": id_, "result": {"tools": tools}}

    if method == "tools/call":
        t0 = time.time()
        name = params.get("name", "")
        tool = TOOL_MAP.get(name)
        if not tool:
            return rpc_error(id_, -32602, f"알 수 없는 도구: {name}")
        if not rate_ok(ip):
            log_usage(ip, "tools/call", name, 0, "rate_limited")
            return {
                "jsonrpc": "2.0", "id": id_,
                "result": {"content": [{"type": "text",
                           "text": "호출 한도 초과(분당 30회). 잠시 후 다시 시도해 주세요."}],
                           "isError": True},
            }
        try:
            text = tool["fn"](params.get("arguments") or {})
            status, is_err = "ok", False
        except BusyError as e:
            text, status, is_err = str(e), "busy", True
        except KeyError as e:
            text, status, is_err = f"필수 인자 누락: {e}", "bad_args", True
        except urllib.error.URLError:
            text, status, is_err = "DB 연결 실패. 잠시 후 다시 시도해 주세요.", "db_down", True
        except Exception as e:
            text, status, is_err = f"조회 실패: {type(e).__name__}", "error", True
        log_usage(ip, "tools/call", name, int((time.time() - t0) * 1000), status)
        return {
            "jsonrpc": "2.0", "id": id_,
            "result": {"content": [{"type": "text", "text": text}], "isError": is_err},
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
                n = cypher("MATCH (l:Law) WHERE l.is_current RETURN count(l) AS n")[0]["n"]
                self.send_json({"status": "ok", "server": SERVER_NAME, "current_laws": n})
            except Exception:
                self.send_json({"status": "degraded", "server": SERVER_NAME}, 503)
            return
        if path == "/sse":
            self.legacy_sse()
            return
        # /mcp GET: 서버 주도 스트림 미지원 (stateless) — 스펙상 405 허용
        self.send_json({"error": "Method Not Allowed"}, 405)

    def do_POST(self):
        ip = self.client_ip()
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
        resp = handle_message(msg, ip)
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
        except Exception:
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
        resp = handle_message(msg, ip)
        if resp is not None:
            q.put(resp)
        self.send_empty(202)


def main():
    httpd = ThreadingHTTPServer((BIND, PORT), Handler)
    httpd.daemon_threads = True
    print(f"[{SERVER_NAME}] listening on {BIND}:{PORT} (neo4j: {NEO4J_HTTP})")
    httpd.serve_forever()


if __name__ == "__main__":
    main()

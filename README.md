# korea-tax-law MCP 서버

한국 세법 그래프 DB를 AI에 연결하는 공개 MCP(Model Context Protocol) 서버입니다.
단순 법 조문을 찾는 것을 넘어, 유권해석 본문, 예규판례 찾는 데 탁월한 DB입니다.

국세·지방세 현행 조문(법률·시행령·시행규칙 60개), 판례·조세심판원 결정례 15만+건, 국세청·법제처·행정안전부 해석례 14.8만+건, 107개국 조세조약 조문 3천+개가 그래프로 연결되어 있고, 조문마다 **시행일과 개정 연혁**이 붙어 있습니다. AI가 세법을 "기억"이 아니라 **현행 원문 조회**로 답하게 만듭니다.

국가법령정보센터에서 직접 API로 호출하는 것이 아니라, Graph DB로 세법 DB를 다시 설계하여 AI로 하여금 법률 추론의 효과성/효율성을 극대화할 수 있도록 한 DB입니다.
```
https://mcp.taxjarvis.com/mcp
```

## 왜 필요한가

프론티어 모델이 급진적으로 발달했음에도 불구하고 LLM은 세법 질문에 자주 유권해석, 판례 등을 지어내서 답합니다. 또한, 일반 Web 검색을 통한 AI 답변은 학습 시점 이후 개정을 모르고, 세율·한도 숫자를 섞고, 폐지된 조문을 인용합니다. 이 서버를 연결하면 AI가 답하기 전에 **현행 시행 버전 조문 원문**을 직접 확인합니다.

## 도구

현재 버전 **v0.3.0** — 조세조약 도구 3종 추가, 지방세 표기 ([CHANGELOG](CHANGELOG.md))

| 도구 | 용도 |
|---|---|
| `list_laws` | 수록 현행 법령 목록 + 시행일 (국세·지방세 구분) |
| `search_articles` | 조문 전문검색 (복합어 부분일치 폴백 포함) |
| `get_article` | 조문 현행 원문 전체 + 시행일 + 위임 하위법령 조문 |
| `get_article_history` | 조문 개정 연혁 ("언제 바뀌었나") |
| `search_cases` | 판례·조세심판원 결정례 검색 |
| `search_interpretations` | 국세청·법제처·행정안전부 해석례 검색 |
| `list_treaties` | 조세조약 체결국 목록 / 국가별 협약·의정서 이력 |
| `search_treaties` | 조세조약 조문 전문검색 (제한세율·고정사업장 등) |
| `get_treaty_article` | 조세조약 조문 원문 (국문 + 영문) |

## 연결 방법

**Claude** (웹/데스크톱): 설정 → Connectors → Add custom connector → `https://mcp.taxjarvis.com/mcp`

**Claude Code**:
```bash
claude mcp add --transport http korea-tax-law https://mcp.taxjarvis.com/mcp
```

**Codex CLI** (`~/.codex/config.toml`):
```toml
[mcp_servers.korea-tax-law]
url = "https://mcp.taxjarvis.com/mcp"
```

**Gemini CLI** (`~/.gemini/settings.json`):
```json
{"mcpServers": {"korea-tax-law": {"httpUrl": "https://mcp.taxjarvis.com/mcp"}}}
```

**구형 SSE 전용 클라이언트**: `https://mcp.taxjarvis.com/sse`
**stdio 전용 클라이언트**: `npx -y mcp-remote https://mcp.taxjarvis.com/mcp`

## 데이터

- 원천: 국가법령정보 OpenAPI 등 공공데이터. 법령·판례는 저작권법 제7조 비보호저작물
- 갱신: 매일 아침 증분 수집 (법령 개정·신규 판례·해석례)
- 국세(기재부 소관 45개) + 지방세(행안부 소관 15개) 현행 법령
- 조세조약: 107개국 148건(협약·개정의정서·교환각서), 조문 3,002개 국문·영문 병존
- 수록 범위는 `list_laws`·`list_treaties`로 확인. 미수록 법령은 [국가법령정보센터](https://law.go.kr) 참조

**조세조약 주의**: 조문 본문은 개정의정서가 반영된 통합본이지만, 원천 데이터 구조상
조문이 붙어 있는 조약 레코드의 조약명·발효일은 최초 협약 기준일 수 있습니다(예: 싱가포르 —
본문은 2019년 개정 협정, 레코드 발효일은 1981년). 그래서 조문 도구는 조문에 발효일을
붙이지 않고, 국가별 협약·의정서 이력을 `list_treaties`로 따로 제공합니다. 발효 시점이
쟁점이면 반드시 이력과 law.go.kr 원문을 함께 확인하세요.

## 직접 호스팅

`server.py` 하나가 전부입니다. 파이썬 3.9+ 표준 라이브러리만 사용 (의존성 0).

```bash
NEO4J_HTTP=http://127.0.0.1:7474 NEO4J_USER=neo4j NEO4J_PASSWORD=... PORT=8788 python3 server.py
```

단, 같은 스키마의 Neo4j 그래프 DB가 필요합니다 (Law/Article/Case/Interpretation/Treaty/TreatyArticle 노드 + 풀텍스트 인덱스). 데이터 수집 파이프라인은 이 리포에 포함되어 있지 않으므로, 일반 사용자는 호스팅 엔드포인트 사용을 권장합니다.

기본 방어 설정: IP당 분당 30회, 동시 쿼리 4개, 쿼리 타임아웃 8초, read-only.

## 주의

제공 정보는 실무 참고용이며 공식 유권해석이 아닙니다. 구체적 사안은 반드시 원문(국가법령정보센터·국세법령정보시스템)을 확인하고 전문가와 상의하세요.

## 만든 사람

박민수 — 한국공인회계사(KICPA), 안세회계법인
세무 이야기: [Threads @mangopeach.cpa](https://www.threads.com/@mangopeach.cpa)

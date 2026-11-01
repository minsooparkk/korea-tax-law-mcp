# TaxDoctor 한국 세법 DB (korea-tax-law MCP)

한국 세법 법령·시행령·시행규칙, 판례·조세심판원 결정례, 국세청·법제처·행정안전부 해석례, 조세조약을
원문과 시행일, TaxDoctor 3단보기 링크로 찾아 주는 **읽기 전용** 공개 MCP 서버입니다.
공인회계사가 운영하는 [TaxDoctor](https://taxdoctorai.com)의 세법 그래프 DB를 그대로 씁니다.

- 연결 주소: `https://mcp.taxdoctorai.com/mcp` (Streamable HTTP). 구형 SSE 클라이언트는 `https://mcp.taxdoctorai.com/sse`
- 현재 판: 0.25.4 (`curl -sI https://mcp.taxdoctorai.com/healthz` 의 `Server` 헤더로 확인)
- 로그인·API 키 없음, 무료. 데이터를 바꾸는 도구는 없습니다. 검색어와 대화 내용은 기록하지 않습니다.
- 도움말(연결 방법·도구·예시 질문): https://taxdoctorai.com/mcp-guide
- 개인정보처리방침: https://taxdoctorai.com/privacy
- 문의: mspark@kernelacademy.io

> 제공 정보는 실무 참고용이며 공식 유권해석이나 세무 자문이 아닙니다.
> 실제 신고·판단은 원문과 세무 전문가의 확인을 거치세요.

## 연결하기

**Claude (웹·데스크톱)**: [설정] → [커넥터] → [커스텀 커넥터 추가] → 이름 `TaxDoctor`, 주소 `https://mcp.taxdoctorai.com/mcp`.
인증 설정은 비워 둡니다. 주소는 홈페이지가 아니라 `/mcp` 로 끝나는 위 주소입니다.

**Claude Code**
```bash
claude mcp add --transport http taxdoctor https://mcp.taxdoctorai.com/mcp
```

**Codex CLI**
```bash
codex mcp add taxdoctor --url https://mcp.taxdoctorai.com/mcp
```

**Gemini CLI** (`~/.gemini/settings.json`)
```json
{"mcpServers": {"taxdoctor": {"httpUrl": "https://mcp.taxdoctorai.com/mcp"}}}
```

**그 밖의 MCP 클라이언트**: Streamable HTTP는 `/mcp`, SSE 전용 구형 클라이언트는 `/sse`.
stdio만 되는 클라이언트는 `npx -y mcp-remote https://mcp.taxdoctorai.com/mcp` 로 잇습니다.

## 들어 있는 자료

- 국세·지방세 현행 법률·시행령·시행규칙과 별표·서식, 시행 예정 개정
- 지난 사업연도·과세기간 판단을 위한 조문의 과거 판(기준일 조회)과 부칙의 적용례·경과조치
- 법원 판례, 조세심판원 결정례, 국세청 심사·이의·과세전적부심사 결정례
- 국세청·법제처·행정안전부 해석례(질의 요지와 회신)
- 한국이 맺은 조세조약 조문(국문·영문)과 개정 의정서
- 세법 아닌 참조 법령(민법·주택법 등, 응답마다 '세법 아님' 표시)과 조정대상지역 지정 이력

원천은 국가법령정보 OpenAPI 등 공공 데이터이며 매일 갱신합니다. 수록 범위는 `list_laws`·`list_treaties` 로 확인합니다.

## 도구 (20개, 모두 `readOnlyHint: true`)

| 도구 | 용도 |
|---|---|
| `search_tax` | 쟁점 하나로 조문·하위법령·판례·해석례·별표를 함께 모으고 결과마다 원문·시행일·적용 시점 미확정 여부를 붙임 |
| `get_evidence` | 찾은 자료의 긴 원문 이어 읽기 |
| `list_laws` | 수록 법령과 시행일 |
| `search_articles` | 조문 본문 검색 |
| `get_article` | 조문 원문. `as_of` 를 주면 그날 시행 중이던 판 |
| `get_article_tiers` | 법률 조문과 짝이 되는 시행령·시행규칙 3단 |
| `get_article_history` | 조문 개정 연혁 |
| `get_amendments` | 부칙의 적용례·경과조치 |
| `list_upcoming` | 시행 예정 개정 |
| `search_cases` | 판례·조세심판원 결정례 검색 |
| `search_interpretations` | 국세청·법제처·행정안전부 해석례 검색 |
| `verify_citations` | 초안에 적힌 조문·판례·해석례 번호가 실제로 있는지 확인 |
| `search_annexes` | 별표·서식 검색 |
| `get_annex` | 별표·서식 원문 |
| `list_treaties` | 조세조약 체결국과 협약·의정서 이력 |
| `search_treaties` | 조약 조문 검색 |
| `get_treaty_article` | 조약 조문 원문(국문·영문) |
| `search_reference_articles` | 세법 아닌 참조 법령 조문 검색 |
| `get_reference_article` | 참조 법령 조문 원문(기준일 판) |
| `get_regulated_area_history` | 조정대상지역·투기과열지구·투기지역 지정·해제 이력(관보 대조) |

## 이렇게 물어보세요

1. 배우자에게 증여받은 아파트를 8년 뒤 팔면 이월과세가 적용되나요? 소득세법 제97조의2 원문과 관련 해석례를 같이 보여 주세요.
2. 2024 사업연도 말 기준으로 법인세법 시행령 제19조 손금 범위 조문이 어떻게 돼 있었는지, 지금과 달라진 점을 알려 주세요.
3. 미국 법인에 소프트웨어 사용료를 줄 때 한미 조세조약상 원천징수 제한세율과 근거 조문은?
4. 이 의견서 초안에 인용한 판례·해석례 번호와 조문이 실제로 있는지 확인해 주세요. (초안 붙여넣기)
5. 혼인·출산 증여재산 공제(상속세 및 증여세법 제53조의2)는 언제부터 적용되고, 부칙의 적용례는 어떻게 되나요?

## 알아 둘 점

- 조문은 현행 시행 판이 기준입니다. 지난 연도는 기준일(예: 사업연도 말일)을 주면 그날 시행 중이던 판을 찾습니다.
  수록 전이거나 확인되지 않은 판은 현행으로 메우지 않고 확인 불가로 답합니다.
- 검색 결과의 인용 관계는 적용 확정이 아닙니다. 적용 시점이 확인되지 않은 결과는 따로 표시합니다.
- 호출 한도: 같은 접속 주소에서 1분에 일정량을 넘으면 몇 초 뒤 다시 부르라는 안내(`retry_after_seconds`)가 나옵니다.
  안내를 무시하고 10분 안에 30번 넘게 한도를 넘기면 그 주소의 도구 호출을 10분 동안 거절합니다.
- 응답 크기: `search_tax` 는 결과마다 원문 앞부분(한 번에 합쳐 약 3,000자)과 시행일·적용 상태·탐색 경로 한 줄을 싣습니다.
  나머지 원문은 응답의 `continuation` 그대로 `get_evidence` 로 이어 읽고, 여러 건을 훑을 때는 `detail: "list"` 로 목록부터 받습니다.
- 개인정보: 로그인이 없어 회원 정보를 받지 않고, 검색어·도구 입력·대화 내용은 기록하지 않습니다.
  과도한 호출 차단과 장애 대응을 위해 접속 IP·연결 프로그램 정보·호출한 도구 이름·처리 시간만 남기고 90일 뒤 자동 삭제합니다.
  삭제 전에 IP 없는 하루 집계(도구별 호출 수 등)로 바꿔 둡니다. 자세한 내용은 https://taxdoctorai.com/privacy

## 변경 기록

- 0.25.4 (2026-11): 검색어 기록 중단, 접속 기록 보관 90일, `search_tax` 기본 응답을 원문 발췌 중심으로 줄임(도구 이름·입력 형식은 그대로)
- 0.25.3 (2026-11): 모든 응답에 실무 참고용 고지, 오류 안내를 구체적으로, 서버 안내문 정리

## 문의

연결이 안 되거나 결과가 이상하면 mspark@kernelacademy.io 로 질문과 받은 답을 함께 보내 주세요.

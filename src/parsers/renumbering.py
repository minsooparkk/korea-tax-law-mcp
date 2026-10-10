"""조문 번호 이동 — 개정문 해석(대응표 만들기)과 옛 인용 번호를 현행 번호로 되짚기.

대응표(article_renumbering.json)는 scripts/collect_renumbering.py 가 법제처 개정문에서 만든다. 여기서는
1. 개정문 한 편에서 조 단위 이동·삭제·신설·전부개정을 읽고(parse_amendment),
2. 문서(판례·해석)가 인용한 "그때의 번호"를 지금 번호로 옮긴다(Renumbering.resolve).

문서 날짜는 법 적용 시점이 아니라 **상한**으로만 쓴다 — 2015년 문서가 2019년에 새로 짠 번호를 인용할 수는
없다. 그보다 뒤 문서는 "구 ○○법(2018. 12. 24. 법률 제16008호로 개정되기 전의 것)"처럼 스스로 밝힌 경우만
그 개정 전 번호로 본다. 옮길 수 없으면(전부개정·삭제·수록 전) 연결하지 않는다 — 틀린 조문에 붙이지 않는다.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

# 저장소에 든 대응표가 기준본, 일일 갱신이 새로 만든 대응표는 데이터 폴더에 둔다(있으면 그쪽이 우선)
TABLE_PATH = Path(__file__).with_name("article_renumbering.json")
DAILY_TABLE_PATH = Path(__file__).resolve().parents[2] / "data" / "source_cache" / "revisions" / "article_renumbering.json"

_ART = r"제\s*(\d+)\s*조(?:\s*의\s*(\d+))?"
# 범위: "제1조부터 제3조까지" · 옛 법령의 "제1조 내지 제3조"
_ITEM = rf"{_ART}(?:\s*(?:부터|내지)\s*{_ART}(?:\s*까지)?)?"
_LIST = rf"{_ITEM}(?:\s*(?:,|ㆍ|·|및)\s*{_ITEM})*"
# 조 단위만 — "제16조제2항을 제3항으로"(항 이동)·"제5조 중 …을"(문구 수정)은 목록 뒤가 조사가 아니라 걸리지 않는다
_HEAD = r"(?:(?<=^)|(?<=[\s,.]))"
_MOVE = re.compile(rf"{_HEAD}(?P<old>{_LIST})\s*(?:를|을)\s*(?:각각\s*)?(?P<new>{_LIST})\s*(?:로|으로)\s*(?:하고|하며|한다|하되)")
_DELETE = re.compile(rf"{_HEAD}({_LIST})\s*(?:를|을)\s*(?:각각\s*)?삭제")
_INSERT = re.compile(rf"{_HEAD}({_LIST})\s*(?:를|을)\s*(?:각각\s*)?다음과\s*같이\s*신설")
_FORMER = re.compile(rf"{_ART}\s*\(\s*종전의\s*{_ART}\s*\)")
_REWRITE_TYPES = ("전부개정", "제정", "폐지제정")
_QUALIFIER = re.compile(
    r"\([^()]*?(\d{4})\s*\.\s*(\d{1,2})\s*\.\s*(\d{1,2})\s*\.?[^()]*?(개정\s*되기\s*전의\s*것|개정\s*전|개정\s*된\s*것)"
)
# 판 번호만 적은 꼴 — "법인세법(1988. 12. 26. 법률 제4020호) 제1조" = 그 판(개정된 것과 같다)
_EDITION = re.compile(
    r"\(\s*(\d{4})\s*\.\s*(\d{1,2})\s*\.\s*(\d{1,2})\s*\.?\s*(?:법률|대통령령|총리령|[가-힣]*부령)\s*제\s*\d+\s*호\s*[,)]"
)


def _num(base: str, branch: str | None) -> str:
    return f"제{int(base)}조" + (f"의{int(branch)}" if branch and int(branch) else "")


def _expand(item: re.Match) -> list[str] | None:
    """"제1조부터 제3조까지" → [제1조, 제2조, 제3조]. 가지 번호끼리("제45조의3부터 제45조의6까지")도.
    섞인 범위(제1조부터 제3조의2까지)는 사이에 무엇이 있었는지 모르므로 None."""
    base1, branch1, base2, branch2 = item.group(1), item.group(2), item.group(3), item.group(4)
    if base2 is None:
        return [_num(base1, branch1)]
    if not branch1 and not branch2:
        return [_num(str(n), None) for n in range(int(base1), int(base2) + 1)]
    if branch1 and branch2 and base1 == base2:
        return [_num(base1, str(n)) for n in range(int(branch1), int(branch2) + 1)]
    return None


def _expand_list(text: str) -> list[str] | None:
    out: list[str] = []
    for item in re.finditer(_ITEM, text):
        numbers = _expand(item)
        if numbers is None:
            return None
        out.extend(numbers)
    return out


def parse_amendment(segment: str) -> dict:
    """개정문(이 법령 몫) → {"moves": {옛: 새}, "deleted": [...], "inserted": [...], "unparsed": [...]}."""
    moves: dict[str, str] = {}
    unparsed: list[str] = []
    for match in _MOVE.finditer(segment):
        old, new = _expand_list(match.group("old")), _expand_list(match.group("new"))
        if old is None or new is None or len(old) != len(new):
            unparsed.append(match.group(0)[:160])
            continue
        moves.update({a: b for a, b in zip(old, new) if a != b})
    for match in _FORMER.finditer(segment):
        new, old = _num(match.group(1), match.group(2)), _num(match.group(3), match.group(4))
        if old != new:
            if moves.get(old, new) != new:
                unparsed.append(f"종전 표시와 이동 문장이 다름: {old}→{moves[old]} / {new}")
                continue
            moves[old] = new
    # 삭제·신설은 섞인 범위("제81조의2 내지 제85조")도 범위 그대로 둔다 — 그 사이 어느 번호든 걸린다
    deleted, deleted_ranges = _collect(_DELETE, segment, skip=moves)
    inserted, inserted_ranges = _collect(_INSERT, segment)
    return {"moves": moves, "deleted": deleted, "inserted": inserted,
            "deleted_ranges": deleted_ranges, "inserted_ranges": inserted_ranges, "unparsed": unparsed}


def _collect(pattern: re.Pattern, segment: str, skip: dict | None = None) -> tuple[list[str], list[list[str]]]:
    numbers: set[str] = set()
    ranges: list[list[str]] = []
    for match in pattern.finditer(segment):
        for item in re.finditer(_ITEM, match.group(1)):
            expanded = _expand(item)
            if expanded is None:
                ranges.append([_num(item.group(1), item.group(2)), _num(item.group(3), item.group(4))])
            else:
                numbers.update(n for n in expanded if not skip or n not in skip)
    return sorted(numbers), ranges


def _key(number: str) -> tuple[int, int]:
    match = re.fullmatch(r"제(\d+)조(?:의(\d+))?", number)
    return (int(match.group(1)), int(match.group(2) or 0)) if match else (-1, -1)


def _in_ranges(number: str, ranges: list[list[str]]) -> bool:
    key = _key(number)
    return any(_key(low) <= key <= _key(high) for low, high in ranges)


def build_table(amend_dir: Path, family_laws: list[tuple[str, str]], extras: dict | None = None) -> dict:
    """받아 둔 개정문 → 법령별 번호 사건(이동·삭제·신설·전부개정) 목록. 번호가 안 바뀐 개정은 싣지 않는다.

    extras: {법령: {"epoch": 수록 시작일, "mappings": {전부개정 MST: {옛 번호: 새 번호}}}} — 없으면 family_laws 의 EPOCH.
    """
    extras = extras or {}
    by_law: dict[str, list[dict]] = {law: [] for law, _ in family_laws}
    for path in sorted(amend_dir.glob("*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        law = record.get("law_name")
        if law not in by_law:
            continue
        rewrite = any(t in (record.get("revision_type") or "") for t in _REWRITE_TYPES) or bool(
            re.search(r"전부를\s*다음과\s*같이\s*개정한다", record.get("segment") or "")
        )
        parsed = parse_amendment(record.get("segment") or "") if not rewrite else {
            "moves": {}, "deleted": [], "inserted": [], "deleted_ranges": [], "inserted_ranges": [], "unparsed": []}
        if not (rewrite or parsed["unparsed"] or any(parsed.get(k) for k in (
                "moves", "deleted", "inserted", "deleted_ranges", "inserted_ranges"))):
            continue
        dates = record.get("enforcement_dates") or [record.get("promulgation_date")]
        mapping = (extras.get(law, {}).get("mappings") or {}).get(record["mst"])
        if mapping:
            parsed = {**parsed, "mapping": mapping}
        by_law[law].append({
            "mst": record["mst"], "date": dates[0], "promulgated": record.get("promulgation_date") or "",
            "promulgation_number": record.get("promulgation_number") or "",
            "revision_type": record.get("revision_type") or "", "rewrite": rewrite,
            "segment_found": bool(record.get("segment")), **parsed,
        })
    epochs = {law: extras.get(law, {}).get("epoch") or epoch for law, epoch in family_laws}
    return {
        "source": "법제처 법령 본문 API <개정문> — scripts/collect_renumbering.py",
        "laws": {
            law: {"epoch": epochs[law], "events": sorted(events, key=lambda e: (e["date"], e["promulgated"], e["mst"]))}
            for law, events in by_law.items()
        },
    }


# "구 ○○법 제N조"처럼 판 표시 없이 옛 판을 가리키면, 문서가 다루는 거래 시점(부과제척기간 최장 15년) 안에
# 그 번호의 뜻이 바뀐 적이 있는지만 본다 — 바뀐 적이 있으면 어느 판인지 몰라 잇지 않는다
FORMER_WINDOW_YEARS = 15


def _changes_meaning(event: dict, number: str, vacated: set[str], vacated_ranges: list) -> bool:
    """이 개정 앞뒤로 같은 번호가 다른 조문을 가리키게 되었는가.

    옮겨 나감·옮겨 들어옴·비었던 자리에 다시 신설·전부개정에서 같은 번호로 짝지어지지 않음만 센다.
    처음 생긴 번호의 신설(그 전 판엔 그 번호가 없었다)과 삭제(그 전 뜻은 하나뿐)는 판이 헷갈리지 않는다.
    """
    if event["rewrite"]:
        return (event.get("mapping") or {}).get(number) != number
    if number in event["moves"] or number in event["moves"].values():
        return True
    reinserted = number in event["inserted"] or _in_ranges(number, event.get("inserted_ranges", []))
    return reinserted and (number in vacated or _in_ranges(number, vacated_ranges))


@dataclass(frozen=True)
class Resolution:
    number: str | None  # 옮긴 현행 번호, 연결하지 않으면 None
    reason: str  # "" 그대로 · "renumbered" 옮김 · "deleted_since" 지금은 삭제 자리 · 그 밖은 연결하지 않는 이유
    cutoff: str = ""
    path: tuple[str, ...] = ()


def qualifier(evidence: str) -> tuple[str, bool] | None:
    """"(2018. 12. 24. 법률 제16008호로 개정되기 전의 것)" → ("20181224", True: 그 개정 전)."""
    match = _QUALIFIER.search(evidence or "")
    edition = None if match else _EDITION.search(evidence or "")
    if not match and not edition:
        return None
    found = match or edition
    date = f"{int(found.group(1)):04d}{int(found.group(2)):02d}{int(found.group(3)):02d}"
    return date, bool(match) and not re.sub(r"\s+", "", match.group(4)).startswith("개정된")


class Renumbering:
    def __init__(self, table: dict):
        self.laws = table.get("laws", {})

    def covers(self, law_name: str) -> bool:
        return law_name in self.laws

    def resolve(self, law_name: str, number: str, document_date: str, evidence: str = "",
                former: bool = False) -> Resolution:
        """문서가 인용한 번호를 지금 번호로. document_date 는 YYYYMMDD(없으면 그대로 둔다).
        former: 법령 이름 앞에 "구"가 붙었다(판 표시가 없으면 어느 옛 판인지 모른다)."""
        entry = self.laws.get(law_name)
        date = re.sub(r"\D", "", document_date or "")[:8]
        if entry is None or len(date) != 8 or not number.startswith("제"):
            return Resolution(number, "")
        qual = qualifier(evidence)
        if former and not qual:
            since = f"{int(date[:4]) - FORMER_WINDOW_YEARS:04d}{date[4:]}"
            vacated: set[str] = set()
            vacated_ranges: list = []
            for event in entry["events"]:
                if event["date"] > date:
                    break
                if since < event["date"] and _changes_meaning(event, number, vacated, vacated_ranges):
                    return Resolution(None, "former_edition_unknown", date, (number,))
                vacated.update(event["deleted"])
                vacated.update(event["moves"])
                vacated_ranges.extend(event.get("deleted_ranges", []))

        def after_citation(event: dict) -> bool:
            if event["date"] > date:
                return True
            if qual:
                promulgated = event.get("promulgated") or event["date"]
                return promulgated >= qual[0] if qual[1] else promulgated > qual[0]
            return False

        cutoff = min(date, qual[0]) if qual else date
        # "개정된 것"·판 번호의 날짜는 공포일이다 — 전부개정 법 자체(1996.12.30 공포·1997.1.1 시행)를 가리킬 수 있으니
        # 기준일(시행일)과는 시행까지 1년 여유를 두고 비교한다. "개정되기 전의 것"은 그 날짜 그대로.
        epoch_check = cutoff
        if qual and not qual[1]:
            epoch_check = min(date, f"{int(qual[0][:4]) + 1:04d}{qual[0][4:]}")
        if epoch_check < entry["epoch"]:
            return Resolution(None, "predates_numbering_epoch", cutoff)
        current, path, deleted = number, [number], False
        for event in entry["events"]:
            if not after_citation(event):
                continue
            if event["rewrite"]:
                # 전부개정 — 직전 판과 전부개정 판을 견주어 확실한 짝만 있는 조문은 옮기고, 나머지는 잇지 않는다
                mapping = event.get("mapping") or {}
                if not deleted and current in mapping:
                    current = mapping[current]
                    path.append(f"{current}@{event['date']}(전부개정)")
                    continue
                return Resolution(None, "renumbered_by_full_revision", cutoff, tuple(path))
            if not deleted and current in event["moves"]:
                current = event["moves"][current]
                path.append(f"{current}@{event['date']}")
                continue
            occupied = (current in event["inserted"] or current in event["moves"].values()
                        or _in_ranges(current, event.get("inserted_ranges", [])))
            if occupied:
                # 비어 있던(지워졌거나 원래 없던) 번호에 다른 조문이 들어왔다 — 인용한 조문은 지금 그 번호에 없다.
                # 옮겨 온 조문이 있는데 원래 조문이 어디로 갔는지 개정문에서 못 읽은 경우도 여기서 끊는다.
                return Resolution(None, "renumbered_target_gone", cutoff, tuple(path))
            if current in event["deleted"] or _in_ranges(current, event.get("deleted_ranges", [])):
                deleted = True  # "제○조 삭제" 자리로 남는다 — 뒤에 그 번호를 다시 쓰지 않으면 그 자리에 그대로 잇는다
                path.append(f"삭제@{event['date']}")
        if deleted:
            return Resolution(current, "deleted_since", cutoff, tuple(path))
        return Resolution(current, "renumbered" if current != number else "", cutoff, tuple(path))


@lru_cache(maxsize=1)
def load_renumbering(path: str | None = None) -> Renumbering | None:
    candidates = [Path(path)] if path else [DAILY_TABLE_PATH, TABLE_PATH]
    for candidate in candidates:
        if candidate.exists():
            return Renumbering(json.loads(candidate.read_text(encoding="utf-8")))
    return None


# ── 전부개정 전후 조문 짝짓기 ────────────────────────────────────────────────────
# 전부개정은 법제처가 신구 대비를 주지 않는다(신구법존재여부 N). 전부개정 직전 판과 전부개정 판의 조문을
# 제목·본문으로 견주어 확실한 짝만 쓴다 — 1등이 충분히 닮았고 2등과 차이가 뚜렷할 때만. 나머지는 잇지 않는다.
MATCH_MIN = 0.55  # 제목이 다를 때 1등의 최소 점수
MATCH_RATIO = 0.6  # 2등이 1등의 이 비율을 넘으면 나뉜 조문으로 보고 잇지 않는다
TITLE_MIN = 0.4  # 제목이 같은 조문이 하나뿐일 때의 최소 점수
_NOISE = re.compile(r"제\s*\d+\s*(?:조|항|호|장|절|관|편)(?:\s*의\s*\d+)?|[0-9①-⑳]|<[^>]*>|[\s\W_]+")


def _grams(text: str, n: int = 3) -> set[str]:
    squashed = _NOISE.sub("", text or "")
    return {squashed[i:i + n] for i in range(max(0, len(squashed) - n + 1))}


def _title_key(title: str) -> str:
    """옛 판 제목은 "(과세표준의 계산방법)"·"…<개정 1998.12.31>"처럼 싸여 있다 — 떼고 견준다."""
    title = re.sub(r"<[^>]*>?.*$", "", title or "")
    return re.sub(r"[\s·ㆍ,()]", "", title)


def match_rewrite(old: list[dict], new: list[dict]) -> tuple[dict[str, str], list[dict]]:
    """전부개정 직전 판 조문 → 전부개정 판 조문. (짝, 판정 기록)"""
    new_grams = [(_grams(a["title"] + " " + a["text"]), a) for a in new if a["title"] != "삭제"]
    title_count: dict[str, int] = {}
    for _, other in new_grams:
        key = _title_key(other["title"])
        title_count[key] = title_count.get(key, 0) + 1
    mapping: dict[str, str] = {}
    records: list[dict] = []
    for article in old:
        if article["title"] == "삭제" or re.search(r"^\S*\s*삭제\s*(?:<|$)", article["text"][:30]):
            continue
        grams = _grams(article["title"] + " " + article["text"])
        if not grams:
            continue
        scored = []
        for other_grams, other in new_grams:
            if not other_grams:
                continue
            overlap = len(grams & other_grams)
            # 옛 조문이 새 조문 안에 얼마나 담겼는지(나뉘거나 합쳐진 조문도) + 전체 겹침
            score = 0.5 * overlap / len(grams) + 0.5 * overlap / len(grams | other_grams)
            same_title = bool(_title_key(article["title"])) and _title_key(article["title"]) == _title_key(other["title"])
            if same_title:
                score += 0.15
            scored.append((score, other["number"], other["title"], same_title))
        scored.sort(reverse=True)
        best = scored[0] if scored else (0.0, "", "", False)
        second = scored[1][0] if len(scored) > 1 else 0.0
        unique_title = best[3] and title_count.get(_title_key(best[2]), 0) == 1
        accepted = (unique_title and best[0] >= TITLE_MIN) or (
            best[0] >= MATCH_MIN and second <= best[0] * MATCH_RATIO)
        if accepted:
            mapping[article["number"]] = best[1]
        records.append({"old": article["number"], "old_title": article["title"], "new": best[1], "new_title": best[2],
                        "same_title": best[3],
                        "score": round(best[0], 3), "second": round(second, 3), "accepted": accepted})
    return mapping, records

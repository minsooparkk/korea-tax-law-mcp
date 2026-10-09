"""Official NTS interpretation-maintenance events, separate from original text.

The source's maintained/deleted lists describe a particular maintenance event.
They are not legal repeal dates or a universal applicability determination.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

SOURCE_URL = "https://taxlaw.nts.go.kr/qt/USEQTE001M.do"
SCHEMA = "nts-interpretation-maintenance-v1"
MAINTENANCE_FIELDS = (
    "maintenance_status", "maintenance_notice", "maintenance_semantic_revision",
    "maintenance_history_json", "maintenance_followups_json",
    "maintenance_previous_history_json",
)
_NOTICE_END = "정비 등록일은 법적 효력일이 아닙니다. 적용 여부는 해당 정비 사유와 후속 자료를 함께 확인하세요."


def _json(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def nts_document_id(url: str | None) -> str | None:
    """Only the official public detail URL establishes an NTS source identity."""
    parsed = urlparse(url or "")
    if parsed.hostname != "taxlaw.nts.go.kr":
        return None
    values = parse_qs(parsed.query).get("ntstDcmId", [])
    return values[0] if len(values) == 1 and re.fullmatch(r"\d+", values[0]) else None


def _date(value: str | None) -> str | None:
    value = (value or "").strip()
    if not value:
        return None
    if not re.fullmatch(r"\d{8}", value):
        raise ValueError("Maintenance registration date must be YYYYMMDD")
    date.fromisoformat(value)
    return value


def build_event(row: dict, receipt: dict) -> dict:
    """Validate one public source row; preserve blank-ID references by position."""
    ident = str(row.get("ntstItrpMntcId") or "").strip()
    if not re.fullmatch(r"\d+", ident) or row.get("stttInfpClCd") != "01":
        raise ValueError("A public maintenance source ID is required")
    flag = row.get("ntstItrpMntcNdYn")
    if flag not in {"Y", "N"}:
        raise ValueError("Unknown maintenance planned/completed status")
    if not re.fullmatch(r"[a-f0-9]{64}", receipt.get("sha256", "")):
        raise ValueError("Maintenance source-page receipt is required")
    members = []
    for prefix, role in (("mntn", "maintained"), ("dlt", "listed_deleted")):
        columns = [[part.strip() for part in (row.get(prefix + suffix) or "").split(",")]
                   for suffix in ("CaseDcmId", "Case", "CaseDcmDtm")]
        if len({len(column) for column in columns}) != 1:
            raise ValueError("Maintenance reference columns have different lengths")
        for index, (source_id, number, document_date) in enumerate(zip(*columns)):
            if not (source_id or number or document_date):
                continue
            members.append({"role": role, "position": index,
                            "nts_dcm_id": source_id if re.fullmatch(r"\d+", source_id) else None,
                            "source_id_raw": source_id, "document_number": number,
                            "document_date_raw": document_date})
    raw_json = _json(row)
    source_snapshot = f"{SCHEMA}:sha256:{hashlib.sha256(raw_json.encode()).hexdigest()}"
    return {
        "event_id": f"nts-maintenance:{ident}", "source_id": ident,
        "source_url": SOURCE_URL, "source_snapshot": source_snapshot,
        "parser_version": SCHEMA, "raw_row_json": raw_json,
        "registered_date": _date(row.get("frsRgtDtm")),
        "status": "planned" if flag == "Y" else "completed",
        "source_status_label": row.get("ntstItrpMntcSchuNm") or "",
        "title": row.get("ntstItrpMntcTtl") or "",
        "reason": row.get("ntstItrpMntcCntn") or "",
        "reason_code": row.get("ntstItrpMntcRsnClCd") or "",
        "effective_date": None, "effective_date_status": "not_provided",
        "memberships_json": _json(members),
        "maintained_source_ids": sorted({m["nts_dcm_id"] for m in members
                                          if m["role"] == "maintained" and m["nts_dcm_id"]}),
        "listed_deleted_source_ids": sorted({m["nts_dcm_id"] for m in members
                                              if m["role"] == "listed_deleted" and m["nts_dcm_id"]}),
        "source_page_sha256": receipt["sha256"], "source_page_path": receipt["path"],
        "observed_at": receipt.get("observed_at") or "",
    }


def build_stage(cache_dir: Path) -> dict:
    """Require complete, consecutive public pages and an identical aggregate."""
    files = sorted(Path(cache_dir).glob("revisions-page-*.json"))
    if not files:
        raise ValueError("No maintenance source pages")
    events, receipts, ids, rows = [], [], set(), []
    total = None
    for page_number, path in enumerate(files, 1):
        raw = path.read_bytes()
        page = json.loads(raw)
        if page.get("page") != page_number or (total is not None and page.get("total") != total):
            raise ValueError("Maintenance pages are not a stable consecutive catalogue")
        total = page.get("total")
        if not isinstance(total, int) or total < 1:
            raise ValueError("Missing maintenance population")
        batch = page.get("rows")
        if not isinstance(batch, list) or len(batch) != min(100, total - (page_number - 1) * 100):
            raise ValueError("Maintenance source page is incomplete")
        receipt = {"path": str(path.resolve()), "sha256": hashlib.sha256(raw).hexdigest(),
                   "observed_at": page.get("at") or "", "page": page_number}
        receipts.append(receipt)
        for row in batch:
            event = build_event(row, receipt)
            if event["event_id"] in ids:
                raise ValueError("Duplicate maintenance source ID")
            ids.add(event["event_id"])
            events.append(event)
            rows.append(row)
    aggregate = Path(cache_dir) / "revisions-all.json"
    if len(events) != total or json.loads(aggregate.read_text()) != rows:
        raise ValueError("Maintenance pages do not cover the aggregate population")
    receipts.append({"path": str(aggregate.resolve()),
                     "sha256": hashlib.sha256(aggregate.read_bytes()).hexdigest(), "kind": "aggregate"})
    return {"schema": SCHEMA, "source_url": SOURCE_URL, "expected_events": total,
            "receipts": receipts, "events": events, "events_sha256": _hash(events)}


def validate_stage(stage: dict) -> None:
    if (stage.get("schema") != SCHEMA or stage.get("expected_events") != len(stage.get("events", []))
            or stage.get("events_sha256") != _hash(stage.get("events", []))):
        raise ValueError("Maintenance stage checksum or population mismatch")
    for receipt in stage["receipts"]:
        if hashlib.sha256(Path(receipt["path"]).read_bytes()).hexdigest() != receipt["sha256"]:
            raise ValueError("Maintenance source receipt changed")
    if build_stage(Path(stage["receipts"][0]["path"]).parent)["events_sha256"] != stage["events_sha256"]:
        raise ValueError("Maintenance stage does not match the complete source pages")
    # Re-derive each event from its official row, not just a re-hashed edited projection.
    receipt_by_hash = {r["sha256"]: r for r in stage["receipts"]}
    for event in stage["events"]:
        receipt = receipt_by_hash.get(event["source_page_sha256"])
        if receipt is None or build_event(json.loads(event["raw_row_json"]), receipt) != event:
            raise ValueError("Maintenance event does not match its source projection")


def project_document(source_id: str, events: list[dict], documents: dict[str, list[dict]]) -> dict:
    """Create a semantic projection without touching the original document body."""
    history, followups = [], []
    for event in events:
        members = json.loads(event["memberships_json"])
        own = [m for m in members if m["nts_dcm_id"] == source_id]
        if not own:
            continue
        for member in own:
            history.append({key: event.get(key) for key in (
                "event_id", "source_id", "source_url", "source_snapshot", "registered_date",
                "status", "source_status_label", "title", "reason", "reason_code",
                "effective_date", "effective_date_status",
            )} | {"role": member["role"], "reference_position": member["position"]})
        if any(m["role"] == "listed_deleted" for m in own):
            for member in members:
                if member["role"] != "maintained":
                    continue
                targets = documents.get(member["nts_dcm_id"], [])
                followups.append({"event_id": event["event_id"], "event_status": event["status"],
                                  "registered_date": event.get("registered_date"),
                                  "nts_dcm_id": member["nts_dcm_id"],
                                  "document_number": member["document_number"],
                                  "document_date_raw": member["document_date_raw"],
                                  "source_ids": sorted(d["id"] for d in targets),
                                  "availability": "stored" if targets else "not_stored",
                                  "source_url": ("https://taxlaw.nts.go.kr/qt/USEQTA002P.do?ntstDcmId="
                                                 + member["nts_dcm_id"]) if member["nts_dcm_id"] else None})
    if not history:
        return {}
    history.sort(key=lambda r: (r["registered_date"] or "", r["event_id"], r["role"], r["reference_position"]))
    followups.sort(key=lambda r: (r["registered_date"] or "", r["event_id"], r["nts_dcm_id"] or "", r["document_number"]))
    completed = [r for r in history if r["status"] == "completed"]
    if completed:
        latest_date = max(r["registered_date"] or "" for r in completed)
        roles = {r["role"] for r in completed if (r["registered_date"] or "") == latest_date}
        if len(roles) > 1:
            status, label = "mixed", "같은 등록일의 공식 정비에 유지·삭제 양쪽으로 표시되어 있습니다."
        elif "listed_deleted" in roles:
            status, label = "listed_deleted", "공식 세법해석정비의 삭제사례로 표시되어 있습니다. 현재 적용 근거로 단독 사용하지 마세요."
        elif any(r["role"] == "listed_deleted" for r in completed):
            status, label = "maintained_after_deletion", "삭제사례 기록 이후의 공식 정비에 유지사례로 표시되어 있습니다."
        else:
            status, label = "maintained", "공식 세법해석정비에 유지사례로 표시되어 있습니다."
        date_label = latest_date or "등록일 미제공"
    else:
        status, label = "planned", "공식 세법해석정비의 예정 항목입니다. 완료된 정비로 해석하지 마세요."
        date_label = max(r["registered_date"] or "" for r in history) or "등록일 미제공"
    notice = f"[공식 정비정보 · 원문 아님] {date_label}: {label} {_NOTICE_END}"
    if completed and any(r["status"] == "planned" for r in history):
        notice += " 별도의 정비 예정 기록도 있습니다."
    if followups:
        references = list(dict.fromkeys(r["document_number"] or r["nts_dcm_id"] or "문서번호 미제공"
                                        for r in followups))
        notice += " 정비목록의 유지사례: " + ", ".join(references) + "."
        if any(r["availability"] != "stored" for r in followups):
            notice += " 일부 후속 자료는 DB에 없어 공식 원문 확인이 필요합니다."
    return {"maintenance_status": status, "maintenance_notice": notice,
            "maintenance_semantic_revision": f"{SCHEMA}:sha256:{_hash([history, followups])}",
            "maintenance_history_json": _json(history), "maintenance_followups_json": _json(followups)}


def maintenance_metadata(row: dict) -> dict | None:
    """Read the separate semantic projection; original content remains untouched."""
    if not row.get("maintenance_semantic_revision"):
        return None
    return {"status": row.get("maintenance_status"), "notice": row.get("maintenance_notice"),
            "semantic_revision": row["maintenance_semantic_revision"],
            "events": json.loads(row.get("maintenance_history_json") or "[]"),
            "historical_events": json.loads(row.get("maintenance_previous_history_json") or "[]"),
            "followups": json.loads(row.get("maintenance_followups_json") or "[]"),
            "legal_effective_date": None, "legal_effective_date_status": "not_determined"}


def decorate_interpretation(row: dict) -> dict:
    metadata = maintenance_metadata(row)
    return {**row, "maintenance": metadata} if metadata else row


def reconcile_projection(projection: dict, previous: dict) -> dict:
    """Retain superseded membership evidence without advertising it as current."""
    if not projection and not previous.get("maintenance_semantic_revision"):
        return {}
    current = json.loads(projection.get("maintenance_history_json") or "[]")
    current_keys = {_json(item) for item in current}
    archived = { _json(item): item for item in json.loads(previous.get("maintenance_previous_history_json") or "[]") }
    for item in json.loads(previous.get("maintenance_history_json") or "[]"):
        if _json(item) not in current_keys:
            archived[_json(item)] = item
    historical = [archived[key] for key in sorted(archived) if key not in current_keys]
    if not projection:
        projection = {
            "maintenance_status": "no_current_membership",
            "maintenance_notice": "[공식 정비정보 · 원문 아님] 이전 정비 참조가 현재 원천 기록에서 제외되었습니다. "
                                  "이전 이력은 보존하며, 현재 적용 상태는 별도 확인이 필요합니다. " + _NOTICE_END,
            "maintenance_history_json": "[]", "maintenance_followups_json": "[]",
        }
    projection = {**projection, "maintenance_previous_history_json": _json(historical)}
    semantic = {key: value for key, value in projection.items() if key != "maintenance_semantic_revision"}
    projection["maintenance_semantic_revision"] = f"{SCHEMA}:sha256:{_hash(semantic)}"
    return projection


_EVENT_QUERY = """
UNWIND $batch AS row
MERGE (e:InterpretationMaintenanceEvent {event_id: row.event_id})
SET e.source_id = row.source_id, e.current_snapshot = row.source_snapshot,
    e.source_url = row.source_url
MERGE (v:InterpretationMaintenanceRevision {source_snapshot: row.source_snapshot})
ON CREATE SET v += row
MERGE (e)-[:HAS_MAINTENANCE_REVISION]->(v)
"""
_CURRENT_EVENTS = """
MATCH (e:InterpretationMaintenanceEvent)-[:HAS_MAINTENANCE_REVISION]->(v:InterpretationMaintenanceRevision)
WHERE v.source_snapshot = e.current_snapshot
RETURN properties(v) AS event
"""
_DOCUMENT_FIELDS = ("i.interp_id AS id, i.interp_url AS url, i.interp_number AS number, "
                    + ", ".join(f"i.{key} AS {key}" for key in MAINTENANCE_FIELDS))
_DOCUMENTS = """
MATCH (i:Interpretation)
WHERE $ids IS NULL OR i.interp_id IN $ids
RETURN """ + _DOCUMENT_FIELDS
_UPDATE_DOCUMENT = """
UNWIND $batch AS row
MATCH (i:Interpretation {interp_id: row.id})
SET i += row.projection
WITH i, row
OPTIONAL MATCH (i)-[old:HAS_MAINTENANCE_EVENT]->(previous:InterpretationMaintenanceEvent)
WHERE NOT any(m IN row.memberships WHERE m.event_id = previous.event_id
              AND m.role = old.role AND m.reference_position = old.position)
DELETE old
WITH DISTINCT i, row
UNWIND row.memberships AS membership
MATCH (e:InterpretationMaintenanceEvent {event_id: membership.event_id})
MERGE (i)-[r:HAS_MAINTENANCE_EVENT {role: membership.role, position: membership.reference_position}]->(e)
SET r.source_snapshot = membership.source_snapshot,
    r.semantic_revision = row.projection.maintenance_semantic_revision
"""


def sync_interpretation_maintenance(client, interp_ids: list[str] | None = None) -> dict:
    """Loader hook: source-ID membership also applies to subsequently loaded docs.

    The caller controls publication authorization. This function acquires the
    existing writer gate; it never edits content, full_text or source projections.
    Re-project all linked documents when new follow-up records become available.
    """
    from src.db.write_lock import database_writer

    events = [r["event"] for r in client.execute_query(_CURRENT_EVENTS)]
    if not events:
        return {"events": 0, "updated": 0}
    if interp_ids is None:
        docs = client.execute_query(_DOCUMENTS, {"ids": None})
    else:
        docs = client.execute_query("""
            MATCH (i:Interpretation)-[:HAS_MAINTENANCE_EVENT]->(:InterpretationMaintenanceEvent)
            RETURN DISTINCT """ + _DOCUMENT_FIELDS)
        requested_docs = client.execute_query(_DOCUMENTS, {"ids": interp_ids})
        docs = list({d["id"]: d for d in [*docs, *requested_docs]}.values())
    by_source = defaultdict(list)
    for doc in docs:
        if source_id := nts_document_id(doc.get("url")):
            by_source[source_id].append(doc)
    affected_sources = None
    if interp_ids is not None:
        requested = set(interp_ids)
        requested_sources = {sid for sid, matches in by_source.items()
                             if any(d["id"] in requested for d in matches)}
        affected_events = [e for e in events if requested_sources.intersection(
            e["maintained_source_ids"] + e["listed_deleted_source_ids"])]
        affected_sources = {sid for e in affected_events
                            for sid in e["maintained_source_ids"] + e["listed_deleted_source_ids"]}
        affected_sources.update(requested_sources)
    events_by_source = defaultdict(list)
    for event in events:
        for source_id in set(event["maintained_source_ids"] + event["listed_deleted_source_ids"]):
            events_by_source[source_id].append(event)
    updates, unchanged, cleared = [], 0, 0
    for source_id, matches in by_source.items():
        if affected_sources is not None and source_id not in affected_sources:
            continue
        projection = project_document(source_id, events_by_source.get(source_id, []), by_source)
        for doc in matches:
            reconciled = reconcile_projection(projection, doc)
            if not reconciled:
                continue
            if reconciled["maintenance_semantic_revision"] == doc.get("maintenance_semantic_revision"):
                unchanged += 1
                continue
            cleared += reconciled["maintenance_status"] == "no_current_membership"
            updates.append({"id": doc["id"], "projection": reconciled,
                            "memberships": json.loads(reconciled["maintenance_history_json"])})
    with database_writer("interpretation maintenance semantic projection"):
        client.execute_batch(_UPDATE_DOCUMENT, updates, batch_size=100)
    return {"events": len(events), "updated": len(updates), "unchanged": unchanged, "cleared": cleared}


def publish_stage(client, stage: dict) -> dict:
    from src.db.write_lock import database_writer

    validate_stage(stage)
    with database_writer("official interpretation maintenance publication"):
        client.execute_write("CREATE CONSTRAINT interpretation_maintenance_event_id IF NOT EXISTS "
                             "FOR (e:InterpretationMaintenanceEvent) REQUIRE e.event_id IS UNIQUE")
        client.execute_write("CREATE CONSTRAINT interpretation_maintenance_revision_id IF NOT EXISTS "
                             "FOR (v:InterpretationMaintenanceRevision) REQUIRE v.source_snapshot IS UNIQUE")
        client.execute_batch(_EVENT_QUERY, stage["events"], batch_size=100)
        projection = sync_interpretation_maintenance(client)
    return {"published_events": len(stage["events"]), **projection}

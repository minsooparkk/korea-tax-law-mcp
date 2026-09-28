"""Canonical stored-document projection fingerprints for Graph document rows."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from typing import Any

DOCUMENT_PROJECTION_SCHEMA = "tax-document-projection-v1"

DOCUMENT_TEXT_FIELDS = (
    "referenced_statutes",
    "related_laws",
    "related_statutes",
    "case_name",
    "case_title",
    "ruling_title",
    "interp_title",
    "title",
    "admin_rule_title",
    "case_holding",
    "query_summary",
    "answer_summary",
    "ruling_summary",
    "summary",
    "content",
    "full_content",
    "full_text",
    "body",
)

_KIND = "stored_document_projection"
_SNAPSHOT_PREFIX = "stored-document:v1:sha256:"

_LABEL_ID_KEYS: dict[str, tuple[str, ...]] = {
    "Case": ("case_id",),
    "Interpretation": ("interp_id",),
    "Ruling": ("ruling_id",),
    "AdminRule": ("admin_rule_id", "ruling_id"),
    "Form": ("form_id",),
}


def document_projection(row: Mapping[str, Any], *, label: str | None = None) -> dict[str, str]:
    """Return derived projection fields for a stored document row.

    The input row is never mutated. Official ``source_snapshot`` is left
    untouched and is not part of the returned mapping.
    """
    if not isinstance(row, Mapping):
        raise TypeError(f"row must be a mapping, got {type(row).__name__}")

    resolved_label = _resolve_label(row, label)
    stable_id = _resolve_stable_id(row, resolved_label)

    payload: dict[str, Any] = {
        "schema": DOCUMENT_PROJECTION_SCHEMA,
        "label": resolved_label,
        "id": stable_id,
        "citation_temporal_evidence": normalize_temporal_evidence(
            row.get("citation_temporal_evidence")
        ),
    }
    for field in DOCUMENT_TEXT_FIELDS:
        payload[field] = _normalize_text(field, row.get(field))

    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return {
        "source_projection_schema": DOCUMENT_PROJECTION_SCHEMA,
        "source_projection_kind": _KIND,
        "source_projection_sha256": digest,
        "source_projection_snapshot": _SNAPSHOT_PREFIX + digest,
    }


def _resolve_label(row: Mapping[str, Any], label: str | None) -> str:
    if label is None:
        if "label" not in row:
            raise ValueError("missing document label")
        label = row["label"]
    if not isinstance(label, str):
        raise TypeError(f"label must be str, got {type(label).__name__}")
    if label not in _LABEL_ID_KEYS:
        raise ValueError(f"unsupported label: {label!r}")
    return label


def _resolve_stable_id(row: Mapping[str, Any], label: str) -> str:
    keys = _LABEL_ID_KEYS[label]
    for key in keys:
        if key not in row:
            continue
        value = row[key]
        if value is None:
            continue
        if not isinstance(value, str):
            raise TypeError(f"{key} must be str, got {type(value).__name__}")
        if value == "":
            continue
        return value
    raise ValueError(f"missing nonempty stable id for {label}")


def _normalize_text(field: str, value: Any) -> str | list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        for index, item in enumerate(value):
            if not isinstance(item, str):
                raise TypeError(
                    f"{field}[{index}] must be str, got {type(item).__name__}"
                )
        return value
    raise TypeError(
        f"{field} must be None, str, or list[str], got {type(value).__name__}"
    )


def normalize_temporal_evidence(
    value: Any,
) -> dict[str, Any] | list[dict[str, Any]] | None:
    """Normalize stored or in-memory ``citation_temporal_evidence``.

    Accepts ``None``, a dict, a list of dicts, or a JSON string of those
    forms (Neo4j stores nested maps as a canonical JSON string). JSON
    ``null`` is ``None``. Invalid JSON, JSON scalars other than null,
    nonfinite numbers, and non-JSON types are rejected.
    """
    if isinstance(value, str):
        value = _parse_temporal_json(value)
    return _validate_temporal_structure(value)


def _parse_temporal_json(raw: str) -> Any:
    try:
        return json.loads(raw, parse_constant=_reject_nonfinite_json_constant)
    except json.JSONDecodeError as exc:
        raise ValueError("citation_temporal_evidence is not valid JSON") from exc


def _reject_nonfinite_json_constant(name: str) -> Any:
    raise ValueError(
        f"citation_temporal_evidence contains nonfinite JSON number {name!r}"
    )


def _validate_temporal_structure(
    value: Any,
) -> dict[str, Any] | list[dict[str, Any]] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        _require_json_value(value, path="citation_temporal_evidence")
        return value
    if isinstance(value, list):
        for index, item in enumerate(value):
            if not isinstance(item, dict):
                raise TypeError(
                    f"citation_temporal_evidence[{index}] must be dict, "
                    f"got {type(item).__name__}"
                )
            _require_json_value(item, path=f"citation_temporal_evidence[{index}]")
        return value
    raise TypeError(
        "citation_temporal_evidence must be None, dict, list[dict], "
        f"or JSON of those forms, got {type(value).__name__}"
    )


def _require_json_value(value: Any, *, path: str) -> None:
    if value is None or isinstance(value, (str, bool)):
        return
    if isinstance(value, int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} is not a finite number")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(
                    f"{path} keys must be str, got {type(key).__name__}"
                )
            _require_json_value(item, path=f"{path}.{key}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _require_json_value(item, path=f"{path}[{index}]")
        return
    raise TypeError(f"{path} has unsupported type {type(value).__name__}")

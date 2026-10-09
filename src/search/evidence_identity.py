"""Shared evidence identity and verification rules for search and source views."""

import hashlib
import json
from collections.abc import Iterable, Mapping

_TEMPORAL_PROOF_FIELDS = ("valid_from", "valid_to", "content_sha256", "source_snapshot")


def temporal_proof_guard(node: str = "v") -> str:
    """A proved interval stops being eligible if its body or boundaries change."""
    matching = " AND ".join(
        f"trim(coalesce({node}.temporal_proof_{field}, '')) <> ''"
        f" AND {node}.temporal_proof_{field} = {node}.{field}"
        for field in _TEMPORAL_PROOF_FIELDS
    )
    return (
        f"coalesce({node}.temporal_gate_state, '') <> 'interval_pending'"
        f" AND (coalesce({node}.temporal_gate_state, '') <> 'interval_verified'"
        f" OR ({matching}))"
    )


def temporal_proof_matches(meta: Mapping) -> bool:
    state = meta.get("temporal_gate_state")
    if state == "interval_pending":
        return False
    return state != "interval_verified" or all(
        str(meta.get(f"temporal_proof_{field}") or "").strip()
        and meta.get(f"temporal_proof_{field}") == meta.get(field)
        for field in _TEMPORAL_PROOF_FIELDS
    )


def verified_version_guard(node: str = "v") -> str:
    """Cypher counterpart of verified_version; node names are code constants."""
    return (
        f"(coalesce({node}.verified, false) = true OR coalesce({node}.source_verified, false) = true)"
        f" AND {temporal_proof_guard(node)}"
        f" AND trim(coalesce({node}.source_snapshot, '')) <> ''"
        f" AND trim(coalesce({node}.article_content, '')) <> ''"
    )


def verified_version(meta: Mapping, content: str | None = None) -> bool:
    return bool(
        (meta.get("verified") is True or meta.get("source_verified") is True)
        and temporal_proof_matches(meta)
        and str(meta.get("source_snapshot") or "").strip()
        and str(content if content is not None else meta.get("article_content") or "").strip()
    )


def history_snapshot_id(versions: Iterable[Mapping]) -> str:
    """Pin a historical selection to its version identities and source receipts."""
    payload = sorted(
        (str(v.get("version_id") or ""), str(v.get("source_snapshot") or "")) for v in versions
    )
    return "history:" + hashlib.sha256(json.dumps(payload, ensure_ascii=False).encode()).hexdigest()

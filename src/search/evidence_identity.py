"""Shared evidence identity and verification rules for search and source views."""

import hashlib
import json
from collections.abc import Iterable, Mapping


def verified_version_guard(node: str = "v") -> str:
    """Cypher counterpart of verified_version; node names are code constants."""
    return (
        f"(coalesce({node}.verified, false) = true OR coalesce({node}.source_verified, false) = true)"
        f" AND trim(coalesce({node}.source_snapshot, '')) <> ''"
        f" AND trim(coalesce({node}.article_content, '')) <> ''"
    )


def verified_version(meta: Mapping, content: str | None = None) -> bool:
    return bool(
        (meta.get("verified") is True or meta.get("source_verified") is True)
        and str(meta.get("source_snapshot") or "").strip()
        and str(content if content is not None else meta.get("article_content") or "").strip()
    )


def history_snapshot_id(versions: Iterable[Mapping]) -> str:
    """Pin a historical selection to its version identities and source receipts."""
    payload = sorted(
        (str(v.get("version_id") or ""), str(v.get("source_snapshot") or "")) for v in versions
    )
    return "history:" + hashlib.sha256(json.dumps(payload, ensure_ascii=False).encode()).hexdigest()

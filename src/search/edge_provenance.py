"""Cypher predicates for verified edge provenance.

Search templates and web related-evidence queries interpolate these strings
into WHERE clauses. Aliases default to the search-template conventions
(rel / r / a / c / b / x) so a later template worker can reuse them as-is.

Existing function names and keyword signatures stay stable. New helpers are
additive.
"""

from __future__ import annotations

from src.pipeline.document_projection import DOCUMENT_PROJECTION_SCHEMA

DOCUMENT_PROJECTION_KIND = "stored_document_projection"
DOCUMENT_PROJECTION_SNAPSHOT_PREFIX = "stored-document:v1:sha256:"

_INSTRUMENT_TYPES = "['law', 'decree', 'rule']"


def _and(*parts: str) -> str:
    return " AND ".join(part for part in parts if part)


def _nonempty(expr: str) -> str:
    return f"({expr} IS NOT NULL AND {expr} <> '')"


def _same_nonempty(left: str, right: str) -> str:
    return f"{left} = {right} AND {left} <> ''"


def active_verified_guard(*, edge: str) -> str:
    return f"{edge}.active = true AND {edge}.verified = true"


def current_article_guard(*, node: str = "a") -> str:
    """Reject current endpoints if either deleted flag is true."""
    return (
        f"{node}.is_current = true AND "
        f"coalesce({node}.deleted, false) = false AND "
        f"coalesce({node}.is_deleted, false) = false"
    )


def current_annex_guard(*, node: str = "x") -> str:
    """Reject current annexes if either deleted flag is true."""
    return (
        f"{node}.is_current = true AND "
        f"coalesce({node}.deleted, false) = false AND "
        f"coalesce({node}.is_deleted, false) = false"
    )


def current_path_nodes_guard(*, nodes: str = "nodes(path)") -> str:
    return f"all(n IN {nodes} WHERE {current_article_guard(node='n')})"


def current_contains_guard(*, edge: str = "owns", law: str = "l") -> str:
    """Verified current Law ownership. Inactive CONTAINS is not proof."""
    return _and(
        active_verified_guard(edge=edge),
        _same_nonempty(f"{edge}.source_snapshot", f"{law}.source_snapshot"),
    )


def official_document_snapshot(*, document: str) -> str:
    """Official snapshot, treating '' as absent like verified_links._snapshot.

    Falls back to the document's projection snapshot, never to edge metadata.
    """
    return (
        f"coalesce(nullif({document}.source_snapshot, ''), "
        f"{document}.source_projection_snapshot)"
    )


def document_projection_guard(*, document: str, edge: str) -> str:
    """Edge and document projection identifiers must match. Official snapshots stay separate."""
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
    """CITES_ARTICLE and reverse HAS_CASE / HAS_RULING / HAS_INTERPRETATION.

    The document remains the evidence source even when the stored edge starts at
    the Article. ``edge.source_snapshot`` follows the document, never the edge
    hash. ``edge.target_source_snapshot`` follows the Article (or ArticleVersion
    when ``article`` is bound to a version node).
    """
    return _and(
        active_verified_guard(edge=edge),
        document_projection_guard(document=document, edge=edge),
        f"{edge}.source_snapshot = {official_document_snapshot(document=document)}",
        _nonempty(f"{edge}.source_snapshot"),
        _same_nonempty(f"{edge}.target_source_snapshot", f"{article}.source_snapshot"),
    )


def delegates_to_rel_guard(*, rel: str = "rel") -> str:
    return _and(
        active_verified_guard(edge=rel),
        _same_nonempty(f"{rel}.upper_source_snapshot", f"startNode({rel}).source_snapshot"),
        _same_nonempty(f"{rel}.lower_source_snapshot", f"endNode({rel}).source_snapshot"),
    )


def delegated_from_rel_guard(*, rel: str = "rel") -> str:
    return _and(
        active_verified_guard(edge=rel),
        _same_nonempty(f"{rel}.lower_source_snapshot", f"startNode({rel}).source_snapshot"),
        _same_nonempty(f"{rel}.upper_source_snapshot", f"endNode({rel}).source_snapshot"),
    )


def delegation_rel_guard(*, rel: str = "r") -> str:
    """Mixed-type path predicate for DELEGATES_TO and DELEGATED_FROM."""
    return _and(
        active_verified_guard(edge=rel),
        f"type({rel}) IN ['DELEGATES_TO', 'DELEGATED_FROM']",
        (
            f"{rel}.upper_source_snapshot = CASE type({rel}) "
            f"WHEN 'DELEGATES_TO' THEN startNode({rel}).source_snapshot "
            f"ELSE endNode({rel}).source_snapshot END"
        ),
        (
            f"{rel}.lower_source_snapshot = CASE type({rel}) "
            f"WHEN 'DELEGATES_TO' THEN endNode({rel}).source_snapshot "
            f"ELSE startNode({rel}).source_snapshot END"
        ),
        _nonempty(f"{rel}.upper_source_snapshot"),
        _nonempty(f"{rel}.lower_source_snapshot"),
    )


def article_references_guard(*, edge: str = "r") -> str:
    return _and(
        active_verified_guard(edge=edge),
        _same_nonempty(f"{edge}.source_snapshot", f"startNode({edge}).source_snapshot"),
        _same_nonempty(f"{edge}.target_source_snapshot", f"endNode({edge}).source_snapshot"),
    )


def article_annex_guard(*, article: str = "a", annex: str = "x", edge: str = "rel") -> str:
    """Article-HAS_ANNEX snapshot orientation follows evidence_source_label."""
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


def basic_rule_guard(
    *, article: str = "a", rule: str = "b", edge: str = "r", law: str = "l"
) -> str:
    """Official BasicRule snapshots only — no document projection.

    Current ownership is the Article's law_id and source_snapshot against the
    current Law. Callers must not treat an inactive CONTAINS edge as proof.
    """
    return _and(
        active_verified_guard(edge=edge),
        current_article_guard(node=article),
        f"{article}.law_id = {law}.law_id",
        _same_nonempty(f"{article}.source_snapshot", f"{law}.source_snapshot"),
        _same_nonempty(f"{edge}.source_snapshot", f"{rule}.source_snapshot"),
        _same_nonempty(f"{edge}.target_source_snapshot", f"{article}.source_snapshot"),
        f"{rule}.active = true",
        f"{rule}.is_latest_official_edition = true",
        f"{law}.is_current = true",
        f"{law}.law_id = {rule}.parent_law_id",
        _same_nonempty(f"{edge}.parent_source_snapshot", f"{law}.source_snapshot"),
    )


def law_instrument_family_guard(*, selected_law: str = "selected_law", law: str = "l") -> str:
    """Fail closed unless a shared family scalar/array or parent chain is proven."""
    selected = selected_law
    shared_family = (
        f"({_nonempty(f'{selected}.family_id')} AND "
        f"{selected}.family_id = {law}.family_id) OR "
        f"({_nonempty(f'{selected}.family_id')} AND "
        f"{selected}.family_id IN coalesce({law}.family_ids, [])) OR "
        f"({_nonempty(f'{law}.family_id')} AND "
        f"{law}.family_id IN coalesce({selected}.family_ids, [])) OR "
        f"any(fid IN coalesce({selected}.family_ids, []) "
        f"WHERE fid <> '' AND fid IN coalesce({law}.family_ids, []))"
    )
    parent_chain = (
        f"({_nonempty(f'{law}.parent_law_id')} AND {law}.parent_law_id = {selected}.law_id) OR "
        f"({_nonempty(f'{selected}.parent_law_id')} AND "
        f"{selected}.parent_law_id = {law}.law_id) OR "
        f"{selected}.law_id IN coalesce({law}.parent_law_ids, []) OR "
        f"{law}.law_id IN coalesce({selected}.parent_law_ids, []) OR "
        f"({_nonempty(f'{selected}.parent_law_id')} AND "
        f"{selected}.parent_law_id = {law}.parent_law_id)"
    )
    return _and(
        f"{selected}.instrument_type IN {_INSTRUMENT_TYPES}",
        f"{law}.instrument_type IN {_INSTRUMENT_TYPES}",
        f"(({shared_family}) OR ({parent_chain}))",
    )


def applicability_evidence_guard(
    *, article: str = "a", amendment: str = "m", edge: str = "rel"
) -> str:
    """HAS_APPLICABILITY_EVIDENCE / Amendment REFERENCES. Evidence is the amendment.

    Snapshot orientation does not follow edge direction. This is not automatic
    application of the cited article.
    """
    return _and(
        active_verified_guard(edge=edge),
        current_article_guard(node=article),
        _same_nonempty(f"{edge}.source_snapshot", f"{amendment}.source_snapshot"),
        _same_nonempty(f"{edge}.target_source_snapshot", f"{article}.source_snapshot"),
    )


def article_version_source_backed(*, version: str = "v") -> str:
    """Official snapshot body. Closed historical versions may be source_verified only."""
    return _and(
        (
            f"(coalesce({version}.verified, false) = true OR "
            f"coalesce({version}.source_verified, false) = true)"
        ),
        _nonempty(f"{version}.source_snapshot"),
        _nonempty(f"{version}.version_id"),
    )


def article_version_citation_guard(
    *, document: str, version: str = "v", edge: str = "rel"
) -> str:
    """CITES_ARTICLE_VERSION to a source-backed ArticleVersion.

    Resolved applicability still requires temporally verified versions.
    Cited historical text may use source_verified closed versions and must not
    claim temporal_resolution=resolved.
    """
    return _and(
        document_citation_guard(document=document, article=version, edge=edge),
        f"{edge}.resolved_version_id = {version}.version_id",
        article_version_source_backed(version=version),
        (
            f"(({edge}.temporal_resolution = 'resolved' AND "
            f"{version}.verified = true AND "
            f"{_nonempty(f'{edge}.applicable_date')}) OR "
            f"({edge}.reference_semantics = 'cited_historical_text' AND "
            f"{edge}.temporal_resolution <> 'resolved'))"
        ),
    )


def current_correspondence_guard(
    *, version: str = "v", article: str = "a", edge: str = "e"
) -> str:
    """ArticleVersion to current Article successor. Not continued applicability."""
    return _and(
        active_verified_guard(edge=edge),
        f"{edge}.reference_semantics = 'current_correspondence_only'",
        f"{edge}.does_not_imply_continued_applicability = true",
        f"{edge}.temporal_resolution = 'correspondence_not_applicability'",
        (
            f"{edge}.lineage_kind IN "
            f"['renumbering', 'amendment', 'split', 'merge']"
        ),
        (
            f"{edge}.evidence_kind IN "
            f"['official_amendment', 'official_comparison', 'official_supplement']"
        ),
        _nonempty(f"{edge}.evidence"),
        _nonempty(f"{version}.source_snapshot"),
        _same_nonempty(f"{edge}.source_snapshot", f"{version}.source_snapshot"),
        current_article_guard(node=article),
        _same_nonempty(f"{edge}.target_source_snapshot", f"{article}.source_snapshot"),
        (
            f"(coalesce({version}.verified, false) = true OR "
            f"coalesce({version}.source_verified, false) = true)"
        ),
    )

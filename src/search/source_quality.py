"""Source-scope checks shared by query guards and read-only evidence displays."""

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

CATALOGUE_PLACEHOLDER = "[상세본문 미제공] NTS 상세 API에서 본문 dcmDVO가 반환되지 않아 목록 메타데이터로 보강함."
CATALOGUE_METADATA_NOTICE = (
    "목록의 문서번호·제목 등 정보만 보유한 자료입니다. 실제 회신 본문이나 전문은 확보되지 않았습니다. "
    "보존된 안내문은 공식 원문이 아니라 목록 메타데이터로 만든 문서정보입니다."
)
CATALOGUE_MATCH_FIELDS = ("catalogue_match_target_id", "catalogue_match_target_url", "catalogue_match_relation",
                          "catalogue_match_notice", "catalogue_match_observed_at")
IMAGE_TRANSCRIPTION_FIELDS = (
    "extraction_source_raw_sha256", "extraction_source_projection_sha256", "extraction_source_id",
    "original_image_sha256", "extraction_review_sha256", "extraction_complete",
    "source_image_transcriptions_json",
)
IMAGE_TRANSCRIPTION_NOTICE = (
    "공식 원문에 포함된 이미지에서 판독·대조한 문자를 별도로 전사한 자료입니다. "
    "기존 원문 텍스트와 구분하며, 전체 문서의 완전성을 뜻하지 않습니다."
)
EDITOR_HWP_METHOD = "visual_editor_hwp_transcription_bundle_v1"
EDITOR_HWP_PROVENANCE = "verified_official_editor_hwp_image_transcription_bundle"


def validated_image_source_contexts(source: dict, bundle: dict) -> list[dict] | None:
    """Validate optional original-body quotations separately from image text.

    An absent context preserves older bundles. A declared but invalid context
    invalidates the derivative too: showing its table alone could lose a date
    or other qualification that the approved quotation was meant to preserve.
    """
    if "source_contexts" not in bundle:
        return []
    contexts = bundle["source_contexts"]
    if not isinstance(contexts, list) or len(contexts) > 4:
        return None
    if not contexts:
        return []
    if source.get("interp_id") and not source.get("case_id"):
        fields = {"full_text", "content"}
    elif source.get("case_id") and not source.get("interp_id"):
        fields = {"full_content"}
    else:
        return None
    expected = {"source_field", "source_field_sha256", "start", "end", "text", "text_sha256", "source_positions"}
    try:
        included = {image["source_position"] for image in bundle["images"]}
        total = 0
        for context in contexts:
            if not isinstance(context, dict) or set(context) != expected:
                return None
            field = context["source_field"]
            if not isinstance(field, str) or field not in fields:
                return None
            body = source.get(field)
            text, start, end = context["text"], context["start"], context["end"]
            if (not isinstance(body, str) or not isinstance(text, str) or not text.strip()
                    or type(start) is not int or type(end) is not int
                    or not 0 <= start < end <= len(body) or end - start > 2000
                    or body[start:end] != text):
                return None
            for value, key in ((body, "source_field_sha256"), (text, "text_sha256")):
                digest = context[key]
                if (not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
                        or hashlib.sha256(value.encode()).hexdigest() != digest):
                    return None
            positions = context["source_positions"]
            if (not isinstance(positions, list) or not positions
                    or not all(type(position) is int and position in included for position in positions)
                    or positions != sorted(set(positions))):
                return None
            total += len(text)
        return contexts if total <= 4000 else None
    except (TypeError, KeyError):
        return None


def _editor_hwp_origin(bundle: dict) -> dict | None:
    """Validate the publisher's byte-bound current-body reference inventory."""
    origin = bundle.get("origin")
    try:
        if (not isinstance(origin, dict) or origin.get("kind") != "official_editor_hwp"
                or origin.get("parser_version") != "bounded-current-hwp-image-reference-v1"):
            return None
        for key in ("binary_sha256", "receipt_sha256", "source_approval_sha256",
                    "active_reference_receipt_sha256", "active_reference_sha256"):
            if not isinstance(origin.get(key), str) or not re.fullmatch(r"[0-9a-f]{64}", origin[key]):
                return None
        for key in ("file_id", "file_serial"):
            if not isinstance(origin.get(key), str) or not re.fullmatch(r"[0-9]{1,40}", origin[key]) or int(origin[key]) <= 0:
                return None
        observed = datetime.fromisoformat(origin["observed_at"])
        if observed.tzinfo is None:
            return None
        url = urlparse(origin["source_url"])
        if (url.scheme != "https" or url.netloc != "taxlaw.nts.go.kr" or url.fragment
                or url.path != "/downloadFile.do"
                or parse_qs(url.query, keep_blank_values=True) != {
                    "fleId": [origin["file_id"]], "fleSn": [origin["file_serial"]]}):
            return None
        refs = origin["active_references"]
        if not isinstance(refs, list) or not refs:
            return None
        serialized = json.dumps(refs, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if hashlib.sha256(serialized.encode()).hexdigest() != origin["active_reference_sha256"]:
            return None
        hashes = []
        for position, ref in enumerate(refs, 1):
            if (type(ref.get("source_position")) is not int or ref["source_position"] != position
                    or not re.fullmatch(r"BodyText/Section[0-9]+", ref.get("section") or "")
                    or ref.get("kind") not in {"rectangle_image_fill", "picture"}
                    or type(ref.get("bin_item_id")) is not int or not 1 <= ref["bin_item_id"] <= 65535
                    or ref.get("treated_as_character") is not True or ref.get("angle_degrees") != 0
                    or ref.get("effect") != 0):
                return None
            for key in ("stream_sha256", "decoded_png_sha256", "control_payload_sha256", "shape_payload_sha256"):
                if not isinstance(ref.get(key), str) or not re.fullmatch(r"[0-9a-f]{64}", ref[key]):
                    return None
            if ref["decoded_png_sha256"] not in hashes:
                hashes.append(ref["decoded_png_sha256"])
        if hashes != bundle["source_image_sha256s"]:
            return None
        return origin
    except (TypeError, ValueError, KeyError, AttributeError):
        return None


def _image_bundle(source: dict) -> dict | None:
    """Validate the ordered, path-free manifest before exposing a multi-image derivative."""
    raw = source.get("source_image_transcriptions_json")
    if not isinstance(raw, str):
        return None
    try:
        bundle = json.loads(raw)
        if not isinstance(bundle, dict) or bundle.get("schema") != "nts-image-transcriptions-v1":
            return None
        hashes = bundle["source_image_sha256s"]
        images, omitted = bundle["images"], bundle["omitted"]
        if (not isinstance(hashes, list) or not hashes or len(set(hashes)) != len(hashes)
                or not all(isinstance(h, str) and re.fullmatch(r"[0-9a-f]{64}", h) for h in hashes)
                or not isinstance(images, list) or not images or not isinstance(omitted, list)):
            return None
        positions = []
        for item in [*images, *omitted]:
            position = item["source_position"]
            if (type(position) is not int or not 1 <= position <= len(hashes)
                    or item["image_sha256"] != hashes[position - 1]):
                return None
            positions.append(position)
        if sorted(positions) != list(range(1, len(hashes) + 1)):
            return None
        if [r["source_position"] for r in images] != sorted(r["source_position"] for r in images):
            return None
        for item in omitted:
            if not isinstance(item.get("reason"), str) or not item["reason"].strip():
                return None
        for item in images:
            text, parts = item["text"], item["parts"]
            if (not isinstance(text, str) or not text.strip() or not isinstance(parts, list) or not parts
                    or item.get("scope") != "reviewed_parts_only"
                    or hashlib.sha256(text.encode()).hexdigest() != item["text_sha256"]
                    or not all(re.fullmatch(r"[0-9a-f]{64}", item.get(key) or "")
                               for key in ("review_sha256", "confirmation_sha256"))):
                return None
            if len({part["id"] for part in parts}) != len(parts):
                return None
            for part in parts:
                bbox = part["bbox_pixels"]
                if (not isinstance(part.get("id"), str) or not part["id"]
                        or not isinstance(part.get("text"), str) or not part["text"].strip()
                        or not isinstance(part.get("scope"), str) or not part["scope"].strip()
                        or not isinstance(part.get("structure"), dict)
                        or not isinstance(bbox, list) or len(bbox) != 4
                        or not all(type(v) is int and v >= 0 for v in bbox)
                        or bbox[0] >= bbox[2] or bbox[1] >= bbox[3]):
                    return None
            if text != "\n\n".join(part["text"] for part in parts):
                return None
            regions = item.get("omitted_regions", [])
            if not isinstance(regions, list):
                return None
            for region in regions:
                box = region["bbox_pixels"]
                if (not isinstance(region.get("reason"), str) or not region["reason"].strip()
                        or not isinstance(box, list) or len(box) != 4
                        or not all(type(v) is int and v >= 0 for v in box)
                        or box[0] >= box[2] or box[1] >= box[3]):
                    return None
        rendered = "\n\n".join(f"[이미지 {item['source_position']} 부분 전사]\n{item['text']}" for item in images)
        if rendered != source.get("extracted_content") or source.get("original_image_sha256") is not None:
            return None
        if validated_image_source_contexts(source, bundle) is None:
            return None
        return bundle
    except (TypeError, ValueError, KeyError):
        return None


def image_transcription(source: dict) -> str:
    """Expose only the derivative still bound to the same original and v1 body."""
    methods = {"visual_transcription_v1": "verified_official_inline_image_transcription",
               "visual_transcription_bundle_v1": "verified_official_inline_image_transcription_bundle",
               EDITOR_HWP_METHOD: EDITOR_HWP_PROVENANCE}
    method = source.get("extraction_method")
    if (source.get("source_kind") != "nts_action_detail" or source.get("identity_status") != "confirmed"
            or not isinstance(method, str) or method not in methods or source.get("extraction_provenance") != methods[method]
            or source.get("extraction_complete") is not False):
        return ""
    source_id = source.get("source_id")
    if not isinstance(source_id, str) or not source_id or source.get("extraction_source_id") != source_id:
        return ""
    for current, binding in (("source_raw_sha256", "extraction_source_raw_sha256"),
                             ("source_projection_sha256", "extraction_source_projection_sha256")):
        digest = source.get(current)
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest) or source.get(binding) != digest:
            return ""
    bundled = method in {"visual_transcription_bundle_v1", EDITOR_HWP_METHOD}
    fields = ("extraction_review_sha256",) if bundled else (
        "original_image_sha256", "extraction_review_sha256")
    for field in fields:
        value = source.get(field)
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
            return ""
    if bundled:
        bundle = _image_bundle(source)
        if not bundle:
            return ""
        if method == EDITOR_HWP_METHOD:
            if not _editor_hwp_origin(bundle):
                return ""
        elif bundle.get("origin") is not None:
            return ""
    text = source.get("extracted_content")
    if (not isinstance(text, str) or not text.strip()
            or hashlib.sha256(text.encode()).hexdigest() != source.get("extracted_content_sha256")):
        return ""
    return text


def image_transcription_scope(source: dict) -> dict:
    """Public geometry/scope summary; never return internal paths or the raw JSON."""
    if not image_transcription(source) or not (bundle := _image_bundle(source)):
        return {}
    result = {"source_image_count": len(bundle["source_image_sha256s"]),
            "transcribed_image_count": len(bundle["images"]),
            "images": [{"source_position": item["source_position"], "image_sha256": item["image_sha256"],
                        "text_sha256": item["text_sha256"], "scope": item["scope"],
                        "parts": [{key: part[key] for key in ("id", "bbox_pixels", "scope")} for part in item["parts"]],
                        "omitted_regions": [{key: region[key] for key in ("bbox_pixels", "reason")}
                                            for region in item.get("omitted_regions", [])]}
                       for item in bundle["images"]],
            "omitted_images": [{key: item[key] for key in ("source_position", "image_sha256", "reason")}
                               for item in bundle["omitted"]]}
    if source.get("extraction_method") == EDITOR_HWP_METHOD:
        origin = _editor_hwp_origin(bundle)
        result["origin"] = {"kind": "official_editor_hwp", "label": "공식 편집용 HWP",
                            **{key: origin[key] for key in ("source_url", "observed_at", "binary_sha256")}}
    if contexts := validated_image_source_contexts(source, bundle):
        result["source_contexts"] = contexts
    return result


def image_transcription_notice(source: dict) -> str:
    scope = image_transcription_scope(source)
    if not scope:
        return IMAGE_TRANSCRIPTION_NOTICE
    omissions = [f"이미지 {item['source_position']}: {item['reason']}" for item in scope["omitted_images"]]
    omissions.extend(f"이미지 {item['source_position']}: {region['reason']}"
                     for item in scope["images"] for region in item["omitted_regions"])
    origin = scope.get("origin")
    lead = ("공식 편집용 HWP에 실제 연결된 이미지의 검토 부분을 별도로 전사한 자료입니다. "
            "기존 원문 텍스트와 구분하며 HWP 전체 문서의 완전한 추출을 뜻하지 않습니다. "
            f"HWP 원형 관측: {origin['observed_at']}." if origin else IMAGE_TRANSCRIPTION_NOTICE)
    notice = (lead + f" 실질 이미지 {scope['source_image_count']}개 중 "
              f"{scope['transcribed_image_count']}개의 검토된 부분만 전사했습니다. "
              "전사에 포함되지 않은 영역과 이미지의 내용은 원형을 확인하세요.")
    notice += " 생략 범위: " + "; ".join(omissions) if omissions else ""
    for context in scope.get("source_contexts", []):
        notice += "\n[원문 문맥 인용]\n" + context["text"]
    return notice


def image_transcription_matches(source: dict, literal: str) -> bool:
    """Only reviewed part text can bypass a sparse-field score threshold."""
    if not literal or not (text := image_transcription(source)):
        return False
    needle = literal.lower()
    if bundle := _image_bundle(source):
        return any(needle in part["text"].lower() for item in bundle["images"] for part in item["parts"])
    return needle in text.lower()


def catalogue_match_reference(source: dict) -> dict | None:
    if source.get("catalogue_match_relation") != "matching_catalogue_metadata":
        return None
    observed_at = source.get("catalogue_match_observed_at")
    try:
        observed = datetime.fromisoformat(observed_at)
        if observed.tzinfo is None:
            return None
    except (TypeError, ValueError):
        return None
    observed_day = observed.astimezone(timezone(timedelta(hours=9))).date().isoformat()
    ident = source.get("catalogue_match_target_id") or ""
    url = source.get("catalogue_match_target_url") or ""
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    if (not re.fullmatch(r"NTS-(?:SITE-)?\d+", ident) or parsed.scheme != "https"
            or parsed.hostname != "taxlaw.nts.go.kr" or parsed.username or parsed.password or parsed.fragment
            or parsed.path != "/qt/USEQTA002P.do" or set(query) != {"ntstDcmId"}
            or len(query["ntstDcmId"]) != 1 or not query["ntstDcmId"][0].isdigit()):
        return None
    return {"target_id": ident, "url": url, "relation": "matching_catalogue_metadata",
            "observed_at": observed_at,
            "notice": f"{observed_day} 확인 당시 문서번호·부서문서번호·회신일·제목이 일치했던 공개자료입니다. 이전 자료와 원문이 같은지는 확인되지 않았습니다."}


def catalogue_metadata_only(source: dict) -> bool:
    full = source.get("full_text")
    if isinstance(full, str) and full.strip():
        return False
    content = source.get("content")
    if isinstance(content, str):
        return content.lstrip().startswith(CATALOGUE_PLACEHOLDER)
    # Queries compute this boolean from the two actual fields, so lists need
    # not transfer every full document merely to perform the scope check.
    computed = source.get("catalogue_metadata_only")
    if isinstance(computed, bool):
        return computed
    return False


def catalogue_metadata_predicate(node: str) -> str:
    return (f"(trim(coalesce({node}.full_text, '')) = '' AND "
            f"trim(coalesce({node}.content, '')) STARTS WITH '{CATALOGUE_PLACEHOLDER}')")


def interpretation_body_guard(node: str) -> str:
    return "NOT " + catalogue_metadata_predicate(node)

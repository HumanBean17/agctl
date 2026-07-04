"""Pure path-template router for HTTP mock server."""

from __future__ import annotations

import re
from urllib.parse import urlsplit

_PARAM_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")

__all__ = ["split_segments", "is_param_segment", "param_name", "match_path"]


def split_segments(path: str) -> list[str]:
    """Split a path on `/`, preserving empty segments.

    Leading, trailing, and consecutive slashes produce empty segments,
    making trailing slash significant (`/orders` ≠ `/orders/`).

    Examples:
        `/orders` → `['', 'orders']`
        `/orders/` → `['', 'orders', '']`
        `/` → `['', '']`
        `orders` → `['orders']`
        `` → `[]`
    """
    if path == "":
        return []
    return path.split("/")


def is_param_segment(seg: str) -> bool:
    """Return True iff ``seg`` is exactly ``{name}`` for a valid placeholder name.

    Valid names match ``[A-Za-z_][A-Za-z0-9_]*`` (identical to
    ``resolution._PLACEHOLDER_RE``).

    Examples:
        `{order_id}` → True
        `{_private}` → True (underscore start)
        `{2id}` → False (digit start)
        `{user-id}` → False (hyphen)
        `orders` → False
    """
    return _PARAM_RE.fullmatch(seg) is not None


def param_name(seg: str) -> str:
    """Extract the ``name`` inside ``{name}``.

    Caller must ensure ``is_param_segment(seg)`` is True first.

    Examples:
        `{order_id}` → `order_id`
        `{id}` → `id`
    """
    match = _PARAM_RE.fullmatch(seg)
    if match is None:
        raise ValueError(f"Invalid parameter segment: {seg}")
    return match.group(1)


def match_path(template_path: str, request_path: str) -> dict[str, str] | None:
    """Match a template path against a request path, extracting parameters.

    Query string is stripped from ``request_path`` before matching.
    Returns the captures dict on full match, else ``None``.

    Examples:
        `match_path("/api/v1/orders/{order_id}", "/api/v1/orders/42")`
        → `{"order_id": "42"}`
        `match_path("/orders", "/orders/")` → None (trailing slash)
        `match_path("/{org}/{repo}", "/a/b")` → `{"org": "a", "repo": "b"}`
        `match_path("/", "/")` → `{}`
        `match_path("/", "/x")` → None
    """
    # Strip query string
    clean_path = urlsplit(request_path).path

    # Split both paths into segments
    template_segs = split_segments(template_path)
    request_segs = split_segments(clean_path)

    # Segment count must match
    if len(template_segs) != len(request_segs):
        return None

    captures: dict[str, str] = {}

    # Compare segment by segment
    for template_seg, request_seg in zip(template_segs, request_segs):
        if is_param_segment(template_seg):
            # Capture the request segment
            name = param_name(template_seg)
            captures[name] = request_seg
        elif template_seg != request_seg:
            # Literal mismatch
            return None

    return captures

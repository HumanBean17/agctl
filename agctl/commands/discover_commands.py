"""Top-level `discover` command (DESIGN §3.6, D9).

``agctl discover`` has four modes, selected by flags:

- **summary** (no flags) — counts across all categories.
- **category listing** (``--category <c>``) — name + description per item.
- **item detail** (``--category <c> --name <n>``) — full detail for one item,
  including the D9 verbatim ``sql`` for db-templates.
- **search** (``--search <term>``) — case-insensitive substring match across
  all categories by name and description.

The envelope ``command`` field varies by mode (``discover.summary`` /
``discover.category`` / ``discover.item`` / ``discover.search``), so each mode
has its own ``@envelope``-wrapped core and the Click command dispatches.
Argument-validation errors (``--name`` without ``--category``, or
``--category`` + ``--search`` together) are routed through whichever mode core
the invocation is closest to, so they still surface as ``ConfigError`` exit 2
with the appropriate command tag.
"""

from __future__ import annotations

import re
from typing import Any

import click

from ..command import envelope, load_config_or_raise
from ..errors import ConfigError, TemplateNotFound

__all__ = ["discover"]

# Reuse the resolution regexes so token extraction stays in lockstep with
# substitution (DESIGN D2): ``{name}`` for http/kafka, ``:name`` for db SQL
# (with ``::`` casts protected via the negative lookbehind).
_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
_SQL_PARAM_RE = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")

_VALID_CATEGORIES = ("services", "http-templates", "kafka-patterns", "db-templates")

_SUMMARY_HINT = (
    "Run 'agctl discover --category <name>' to list items. "
    "Categories: services, http-templates, kafka-patterns, db-templates"
)
_CATEGORY_HINT = "Run 'agctl discover --category <c> --name <name>' for full detail"
_SEARCH_HINT = (
    "Run 'agctl discover --category <c> --name <n>' for full detail on any match"
)


# --------------------------------------------------------------------------- #
# Param / example helpers
# --------------------------------------------------------------------------- #


def _collect_brace_tokens(value: Any) -> list[str]:
    """Collect ``{name}`` tokens from a string OR nested string values."""
    found: set[str] = set()

    def _scan(v: Any) -> None:
        if isinstance(v, str):
            for m in _PLACEHOLDER_RE.finditer(v):
                found.add(m.group(1))
        elif isinstance(v, dict):
            for item in v.values():
                _scan(item)
        elif isinstance(v, list):
            for item in v:
                _scan(item)

    _scan(value)
    return sorted(found)


def _http_params(template) -> list[str]:
    """Tokens from template.path AND all string values in template.body."""
    tokens: set[str] = set()
    tokens.update(_collect_brace_tokens(template.path))
    tokens.update(_collect_brace_tokens(template.body))
    return sorted(tokens)


def _kafka_params(pattern) -> list[str]:
    if pattern.match is None:
        return []
    return _collect_brace_tokens(pattern.match)


def _db_params(template) -> list[str]:
    return sorted({m.group(1) for m in _SQL_PARAM_RE.finditer(template.sql)})


def _http_example(name: str, params: list[str]) -> str:
    if not params:
        return f"agctl http call {name}"
    pieces = [f"agctl http call {name}"]
    for i, p in enumerate(params):
        pieces.append(f"--param {p}={'Y' if i else 'X'}")
    return " ".join(pieces)


def _kafka_example(name: str, params: list[str]) -> str:
    if not params:
        return f"agctl kafka assert --pattern {name} --timeout 10"
    pieces = [f"agctl kafka assert --pattern {name}"]
    for i, p in enumerate(params):
        pieces.append(f"--param {p}={'Y' if i else 'X'}")
    pieces.append("--timeout 10")
    return " ".join(pieces)


def _db_example(name: str, params: list[str]) -> str:
    if not params:
        return f"agctl db query --template {name}"
    pieces = [f"agctl db query --template {name}"]
    for i, p in enumerate(params):
        pieces.append(f"--param {p}={'Y' if i else 'X'}")
    return " ".join(pieces)


# --------------------------------------------------------------------------- #
# Mode cores (each wrapped in its own envelope)
# --------------------------------------------------------------------------- #


def _summary_core(config_path: str | None) -> dict:
    cfg = load_config_or_raise(config_path)
    return {
        "services": len(cfg.services),
        "http_templates": len(cfg.templates),
        "kafka_patterns": len(cfg.kafka.patterns),
        "db_templates": len(cfg.database.templates),
        "hint": _SUMMARY_HINT,
    }


_summary_envelope = envelope("discover.summary")(_summary_core)


def _category_core(config_path: str | None, category: str) -> dict:
    cfg = load_config_or_raise(config_path)
    if category not in _VALID_CATEGORIES:
        raise ConfigError(f"Unknown category: {category}", {"category": category})

    items: list[dict] = []
    if category == "services":
        for name in cfg.services:
            items.append({"name": name})  # services have no description
    elif category == "http-templates":
        for name, tpl in cfg.templates.items():
            items.append({"name": name, "description": tpl.description})
    elif category == "kafka-patterns":
        for name, pat in cfg.kafka.patterns.items():
            items.append({"name": name, "description": pat.description})
    elif category == "db-templates":
        for name, tpl in cfg.database.templates.items():
            items.append({"name": name, "description": tpl.description})

    return {
        "category": category,
        "count": len(items),
        "items": items,
        "hint": _CATEGORY_HINT,
    }


_category_envelope = envelope("discover.category")(_category_core)


def _item_core(config_path: str | None, category: str, name: str) -> dict:
    cfg = load_config_or_raise(config_path)
    if category not in _VALID_CATEGORIES:
        raise ConfigError(f"Unknown category: {category}", {"category": category})

    if category == "services":
        if name not in cfg.services:
            raise TemplateNotFound(
                f"Unknown service: {name}", {"path": f"services.{name}"}
            )
        svc = cfg.services[name]
        # DESIGN §4.2: every discover.item carries name/params/example. Services
        # take no params, but emit an empty list + a ready-to-use check command
        # so the shape is uniform across categories.
        item: dict = {
            "category": "services",
            "name": name,
            "base_url": svc.base_url,
            "params": [],
            "example": f"agctl check ready --service {name}",
        }
        if svc.health_path is not None:
            item["health_path"] = svc.health_path
        return item

    if category == "http-templates":
        if name not in cfg.templates:
            raise TemplateNotFound(
                f"Unknown HTTP template: {name}", {"path": f"templates.{name}"}
            )
        tpl = cfg.templates[name]
        params = _http_params(tpl)
        return {
            "category": "http-templates",
            "name": name,
            "description": tpl.description,
            "method": tpl.method,
            "service": tpl.service,
            "path": tpl.path,
            "params": params,
            "example": _http_example(name, params),
        }

    if category == "kafka-patterns":
        if name not in cfg.kafka.patterns:
            raise TemplateNotFound(
                f"Unknown kafka pattern: {name}", {"path": f"kafka.patterns.{name}"}
            )
        pat = cfg.kafka.patterns[name]
        params = _kafka_params(pat)
        item = {
            "category": "kafka-patterns",
            "name": name,
            "description": pat.description,
            "topic": pat.topic,
            "params": params,
            "example": _kafka_example(name, params),
        }
        if pat.match is not None:
            item["match"] = pat.match
        return item

    # category == "db-templates"
    if name not in cfg.database.templates:
        raise TemplateNotFound(
            f"Unknown database template: {name}",
            {"path": f"database.templates.{name}"},
        )
    tpl = cfg.database.templates[name]
    params = _db_params(tpl)
    item = {
        "category": "db-templates",
        "name": name,
        "description": tpl.description,
        "sql": tpl.sql,  # D9: include verbatim sql
        "params": params,
        "example": _db_example(name, params),
    }
    if tpl.connection is not None:
        item["connection"] = tpl.connection
    return item


_item_envelope = envelope("discover.item")(_item_core)


def _search_core(config_path: str | None, term: str) -> dict:
    cfg = load_config_or_raise(config_path)
    needle = term.lower()

    def _matches(haystack: str | None) -> bool:
        return haystack is not None and needle in haystack.lower()

    matches: list[dict] = []

    for name in cfg.services:
        # Services have no description — match on name only.
        if needle in name.lower():
            matches.append({"category": "services", "name": name})

    for name, tpl in cfg.templates.items():
        if needle in name.lower() or _matches(tpl.description):
            matches.append(
                {
                    "category": "http-templates",
                    "name": name,
                    "description": tpl.description,
                }
            )

    for name, pat in cfg.kafka.patterns.items():
        if needle in name.lower() or _matches(pat.description):
            matches.append(
                {
                    "category": "kafka-patterns",
                    "name": name,
                    "description": pat.description,
                }
            )

    for name, tpl in cfg.database.templates.items():
        if needle in name.lower() or _matches(tpl.description):
            matches.append(
                {
                    "category": "db-templates",
                    "name": name,
                    "description": tpl.description,
                }
            )

    return {
        "query": term,
        "matches": matches,
        "hint": _SEARCH_HINT,
    }


_search_envelope = envelope("discover.search")(_search_core)


# --------------------------------------------------------------------------- #
# Click command (registered directly on the root cli group)
#
# Validation errors (mutual exclusion of --category/--search; --name without
# --category) are surfaced as ConfigError exit 2. They are emitted under the
# ``discover.summary`` envelope tag, which is the neutral default mode.
# --------------------------------------------------------------------------- #


def _emit_argument_error(message: str) -> None:
    """Emit a ConfigError envelope (command ``discover.summary``) and exit 2."""
    from ..output import emit
    import time

    err = ConfigError(message, {})
    emit(
        ok=False,
        command="discover.summary",
        error=err.to_dict(),
        duration_ms=0,
    )
    raise SystemExit(2)


@click.command("discover")
@click.option("--category", "category", default=None, help="Category to list/inspect")
@click.option("--name", "name", default=None, help="Item name within a category")
@click.option("--search", "search", default=None, help="Substring to search for")
@click.option("--config", "config_path", default=None, help="Path to agctl.yaml")
@click.pass_context
def discover(
    ctx: click.Context,
    category: str | None,
    name: str | None,
    search: str | None,
    config_path: str | None,
) -> None:
    """Discover configured services, templates, patterns."""
    resolved_config = config_path or (ctx.obj.get("config_path") if ctx.obj else None)

    # Mutual exclusion: --category + --search together is an error.
    if category is not None and search is not None:
        _emit_argument_error("Use either --category or --search, not both")
        return
    # --name requires --category.
    if name is not None and category is None:
        _emit_argument_error("--name requires --category")
        return

    if search is not None:
        _search_envelope(resolved_config, search)
        return

    if category is not None and name is None:
        _category_envelope(resolved_config, category)
        return

    if category is not None and name is not None:
        _item_envelope(resolved_config, category, name)
        return

    # No flags — summary.
    _summary_envelope(resolved_config)

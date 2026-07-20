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
from ..config.models import parse_listen
from ..errors import ConfigError, TemplateNotFound
from .kafka_commands import resolve_cluster_name, resolve_topic_format

__all__ = ["discover"]

# Reuse the resolution regexes so token extraction stays in lockstep with
# substitution (DESIGN D2): ``{name}`` for http/kafka, ``:name`` for db SQL
# (with ``::`` casts protected via the negative lookbehind).
_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
_SQL_PARAM_RE = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")

_VALID_CATEGORIES = (
    "services",
    "http-templates",
    "kafka-patterns",
    "db-templates",
    "mock-http-stubs",
    "mock-kafka-reactors",
    "log-sources",
    "grpc-services",
    "grpc-methods",
    "mock-grpc-stubs",
)

_SUMMARY_HINT = (
    "Run 'agctl discover --category <name>' to list items. "
    "Categories: services, http-templates, kafka-patterns, db-templates, "
    "mock-http-stubs, mock-kafka-reactors, log-sources, grpc-services, "
    "grpc-methods, mock-grpc-stubs"
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


def _db_example(name: str, params: list[str], mode: str = "read") -> str:
    if mode == "write":
        if not params:
            return f"agctl db execute --template {name} --write"
        pieces = [f"agctl db execute --template {name} --write"]
        for i, p in enumerate(params):
            pieces.append(f"--param {p}={'Y' if i else 'X'}")
        return " ".join(pieces)
    else:
        if not params:
            return f"agctl db query --template {name}"
        pieces = [f"agctl db query --template {name}"]
        for i, p in enumerate(params):
            pieces.append(f"--param {p}={'Y' if i else 'X'}")
        return " ".join(pieces)


# --------------------------------------------------------------------------- #
# Mock helpers (mock-http-stubs / mock-kafka-reactors categories)
#
# Mocks are declared under the optional top-level ``mocks:`` config section and
# have no runtime registry, so these accessors treat absent sections as empty
# (graceful zero in summary/listing, never an error).
# --------------------------------------------------------------------------- #


def _mock_http_stubs(cfg) -> dict:
    """``cfg.mocks.http.stubs``, or ``{}`` when mocks/http are absent."""
    if cfg.mocks is None or cfg.mocks.http is None:
        return {}
    return cfg.mocks.http.stubs


def _mock_kafka_reactors(cfg) -> dict:
    """``cfg.mocks.kafka.reactors``, or ``{}`` when mocks/kafka are absent."""
    if cfg.mocks is None or cfg.mocks.kafka is None:
        return {}
    return cfg.mocks.kafka.reactors


def _mock_http_listen(cfg) -> str:
    """Configured HTTP mock listen address (default ``0.0.0.0:18080``)."""
    if cfg.mocks is None or cfg.mocks.http is None:
        return "0.0.0.0:18080"
    return cfg.mocks.http.listen


def _mock_http_example(stub, listen: str) -> str:
    # Parse with the canonical parser (handles ``[ipv6]:port``) and normalize a
    # wildcard bind — IPv4 ``0.0.0.0`` or IPv6 ``::`` — to localhost so the URL
    # is copy-pasteable. IPv6 hosts are re-bracketed for a valid URL.
    host, port = parse_listen(listen)
    if host in ("0.0.0.0", "::", ""):
        host = "localhost"
    bracketed = f"[{host}]" if ":" in host else host
    return f"curl -i -X {stub.method} http://{bracketed}:{port}{stub.path}"


def _mock_kafka_example(reactor) -> str:
    # Reactors consume from ``topic``; trigger by producing, then the reactor
    # emits to ``reaction.topic``. ``--message`` is required by kafka produce.
    return (
        f"agctl kafka produce --topic {reactor.topic} --message '<json>'"
        f"  # reactor emits to {reactor.reaction.topic}"
    )


def _mock_grpc_stubs(cfg) -> dict:
    """``cfg.mocks.grpc.stubs``, or ``{}`` when mocks/grpc are absent."""
    if cfg.mocks is None or cfg.mocks.grpc is None:
        return {}
    return cfg.mocks.grpc.stubs


def _mock_grpc_listen(cfg) -> str:
    """Configured gRPC mock listen address (default ``0.0.0.0:50051``)."""
    if cfg.mocks is None or cfg.mocks.grpc is None:
        return "0.0.0.0:50051"
    return cfg.mocks.grpc.listen


def _grpc_mock_params(stub) -> list[str]:
    """Capture keys + ``{placeholder}`` tokens scanned over the authored response.

    Mirrors the mock-http-stubs philosophy (surface what the operator can
    parameterize), but folds capture *names* into ``params`` rather than
    exposing the structured ``capture`` dict (per Task 11 brief). The
    ``{placeholder}`` scan covers both ``response.message`` (unary /
    client-stream / bidi single payload) and each entry of
    ``response.messages`` (server-stream sequence).
    """
    tokens: set[str] = set()
    if stub.capture:
        tokens.update(stub.capture.keys())
    if stub.response.message is not None:
        tokens.update(_collect_brace_tokens(stub.response.message))
    if stub.response.messages is not None:
        for entry in stub.response.messages:
            tokens.update(_collect_brace_tokens(entry.message))
    return sorted(tokens)


def _mock_grpc_example(stub, listen: str) -> str:
    # Mirror ``_mock_http_example``: a ready-to-use external command against the
    # mock listen address. ``grpcurl`` is the gRPC analogue of ``curl``; the
    # wildcard bind is normalized to localhost and IPv6 hosts are re-bracketed.
    # These are MOCK stubs (not call templates), so ``agctl grpc call`` is the
    # wrong hint — the operator exercises the mock directly via grpcurl.
    host, port = parse_listen(listen)
    if host in ("0.0.0.0", "::", ""):
        host = "localhost"
    bracketed = f"[{host}]" if ":" in host else host
    return f"grpcurl -plaintext {bracketed}:{port} {stub.service}/{stub.method}"


# --------------------------------------------------------------------------- #
# Mode cores (each wrapped in its own envelope)
# --------------------------------------------------------------------------- #


def _summary_core(config_path: str | None, overlay_paths: list[str] | None = None, env_file: str | None = None) -> dict:
    cfg = load_config_or_raise(config_path, overlay_paths, env_file=env_file)
    return {
        "services": len(cfg.services),
        "http_templates": len(cfg.templates),
        "kafka_patterns": len(cfg.kafka.patterns),
        "db_templates": len(cfg.database.templates),
        "mock_http_stubs": len(_mock_http_stubs(cfg)),
        "mock_kafka_reactors": len(_mock_kafka_reactors(cfg)),
        "log_sources": len(cfg.logs.sources),
        "grpc_targets": len(cfg.grpc.targets),
        "grpc_methods": len(cfg.grpc.templates),
        "grpc_mock_stubs": len(_mock_grpc_stubs(cfg)),
        "hint": _SUMMARY_HINT,
    }


_summary_envelope = envelope("discover.summary")(_summary_core)


def _category_core(config_path: str | None, category: str, overlay_paths: list[str] | None = None, env_file: str | None = None) -> dict:
    cfg = load_config_or_raise(config_path, overlay_paths, env_file=env_file)
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
            items.append({"name": name, "description": tpl.description, "mode": tpl.mode})
    elif category == "mock-http-stubs":
        for name, stub in _mock_http_stubs(cfg).items():
            items.append(
                {
                    "name": name,
                    "description": stub.description,
                    "method": stub.method,
                    "path": stub.path,
                }
            )
    elif category == "mock-kafka-reactors":
        for name, reactor in _mock_kafka_reactors(cfg).items():
            items.append(
                {
                    "name": name,
                    "description": reactor.description,
                    "topic": reactor.topic,
                    "consumer_group": reactor.consumer_group,
                }
            )
    elif category == "log-sources":
        for name, src in cfg.logs.sources.items():
            items.append(
                {
                    "name": name,
                    "description": f"{src.type} logs for {name} ({src.path or '?'})",
                }
            )
    elif category == "grpc-services":
        for name, tgt in cfg.grpc.targets.items():
            items.append(
                {
                    "name": name,
                    "description": f"gRPC target {name} at {tgt.address} (tls={tgt.use_tls})",
                }
            )
    elif category == "grpc-methods":
        for name, tpl in cfg.grpc.templates.items():
            items.append(
                {
                    "name": name,
                    "description": tpl.description or f"{tpl.service}/{tpl.method}",
                }
            )
    elif category == "mock-grpc-stubs":
        for name, stub in _mock_grpc_stubs(cfg).items():
            items.append(
                {
                    "name": name,
                    "description": stub.description,
                }
            )

    return {
        "category": category,
        "count": len(items),
        "items": items,
        "hint": _CATEGORY_HINT,
    }


_category_envelope = envelope("discover.category")(_category_core)


def _item_core(config_path: str | None, category: str, name: str, overlay_paths: list[str] | None = None, env_file: str | None = None) -> dict:
    cfg = load_config_or_raise(config_path, overlay_paths, env_file=env_file)
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
        # Resolved cluster name (DESIGN §6): pattern.cluster > default_cluster
        # > single-cluster auto-default. Surfaces where this pattern asserts.
        # A pattern is legitimately cluster-agnostic (disambiguated via
        # ``--cluster`` at ``kafka assert`` time), and a config with >1 cluster
        # and no ``default_cluster`` passes validation — so an inspection command
        # must NOT hard-fail on the unresolvable case. On ConfigError, set
        # ``cluster = None``: the item still renders its topic/match/params/
        # example, and ``cluster: null`` signals "no implicit cluster — pass
        # ``--cluster`` at assert time".
        try:
            cluster = resolve_cluster_name(
                cfg.kafka, binding_cluster=pat.cluster
            )
        except ConfigError:
            cluster = None

        # Resolve formats (Task 10): topic override > cluster default > defaults.
        # When cluster is None, use the ultimate defaults (json/string).
        if cluster is not None:
            value_format = resolve_topic_format(cfg, pat.topic, cluster, "value").value
            key_format = resolve_topic_format(cfg, pat.topic, cluster, "key").value
        else:
            value_format = "json"
            key_format = "string"

        item = {
            "category": "kafka-patterns",
            "name": name,
            "description": pat.description,
            "topic": pat.topic,
            "params": params,
            "example": _kafka_example(name, params),
            "cluster": cluster,
            "value_format": value_format,
            "key_format": key_format,
        }
        if pat.match is not None:
            item["match"] = pat.match
        return item

    if category == "mock-http-stubs":
        stubs = _mock_http_stubs(cfg)
        if name not in stubs:
            raise TemplateNotFound(
                f"Unknown mock HTTP stub: {name}",
                {"path": f"mocks.http.stubs.{name}"},
            )
        stub = stubs[name]
        item = {
            "category": "mock-http-stubs",
            "name": name,
            "description": stub.description,
            "method": stub.method,
            "path": stub.path,
            "response": stub.response.model_dump(by_alias=True, exclude_none=True),
            "delay_ms": stub.delay_ms,
            "example": _mock_http_example(stub, _mock_http_listen(cfg)),
            "note": "Served only while `agctl mock run` is running on this listen address.",
        }
        if stub.match is not None:
            item["match"] = stub.match.model_dump(by_alias=True, exclude_none=True)
        if stub.capture:
            item["capture"] = {
                k: v.model_dump(by_alias=True, exclude_none=True)
                for k, v in stub.capture.items()
            }
        return item

    if category == "mock-kafka-reactors":
        reactors = _mock_kafka_reactors(cfg)
        if name not in reactors:
            raise TemplateNotFound(
                f"Unknown mock Kafka reactor: {name}",
                {"path": f"mocks.kafka.reactors.{name}"},
            )
        reactor = reactors[name]
        item = {
            "category": "mock-kafka-reactors",
            "name": name,
            "description": reactor.description,
            "topic": reactor.topic,
            "consumer_group": reactor.consumer_group,
            "reaction": reactor.reaction.model_dump(by_alias=True, exclude_none=True),
            "example": _mock_kafka_example(reactor),
            "note": "Active only while `agctl mock run` (kafka engine) is running.",
        }
        if reactor.match is not None:
            item["match"] = reactor.match
        if reactor.capture:
            item["capture"] = {
                k: v.model_dump(by_alias=True, exclude_none=True)
                for k, v in reactor.capture.items()
            }
        return item

    if category == "log-sources":
        # Local import to avoid module-load cycle (logs imports this file)
        from ..clients.log_client import LogClient

        if name not in cfg.logs.sources:
            raise TemplateNotFound(
                f"Unknown logs source: {name}",
                {"path": f"logs.sources.{name}"},
            )
        src = cfg.logs.sources[name]
        schema = LogClient(src).sample_schema(sample_lines=100)
        return {
            "category": "log-sources",
            "name": name,
            "description": f"{src.type} logs for {name}",
            "path": src.path,
            "type": src.type,
            "format": src.format,
            "schema_fields": {
                "standard": schema.standard,
                "conditional": schema.conditional,
                "observed": schema.observed,
            },
            "example": f"agctl logs query --source {name} --level ERROR --since 5m",
        }

    if category == "grpc-services":
        if name not in cfg.grpc.targets:
            raise TemplateNotFound(
                f"Unknown gRPC target: {name}",
                {"path": f"grpc.targets.{name}"},
            )
        tgt = cfg.grpc.targets[name]
        return {
            "category": "grpc-services",
            "name": name,
            "description": f"gRPC target {name} at {tgt.address} (tls={tgt.use_tls})",
            "address": tgt.address,
            "use_tls": tgt.use_tls,
            "reflection": tgt.reflection,
            "example": f"agctl grpc call --target {name} --service <fq> --method <m>",
        }

    if category == "grpc-methods":
        if name not in cfg.grpc.templates:
            raise TemplateNotFound(
                f"Unknown gRPC method template: {name}",
                {"path": f"grpc.templates.{name}"},
            )
        tpl = cfg.grpc.templates[name]
        # Local import to avoid module-load cycle (grpc_commands imports this file)
        from ..commands.grpc_commands import new_grpc_client
        from google.protobuf.descriptor import FieldDescriptor

        try:
            client = new_grpc_client(cfg.grpc.targets[tpl.target], descriptors=cfg.grpc.descriptors)
            md = client.find_method(tpl.service, tpl.method)
            # Map field type int to type name
            type_map = {v: k for k, v in vars(FieldDescriptor).items() if k.startswith('TYPE_') and isinstance(v, int)}
            request_fields = [
                {"name": f.name, "type": type_map.get(f.type, f"UNKNOWN_{f.type}"), "repeated": f.is_repeated}
                for f in md.input_type.fields
            ]
            call_type = client.call_type_of(md)
            return {
                "category": "grpc-methods",
                "name": name,
                "description": tpl.description,
                "target": tpl.target,
                "service": tpl.service,
                "method": tpl.method,
                "call_type": call_type,
                "request_fields": request_fields,
                "example": f"agctl grpc call {name}",
            }
        except ConfigError as err:
            # Target unreachable or reflection unavailable - return unavailable marker
            # instead of failing the whole discover call
            return {
                "category": "grpc-methods",
                "name": name,
                "unavailable": True,
                "error": str(err),
            }

    if category == "mock-grpc-stubs":
        stubs = _mock_grpc_stubs(cfg)
        if name not in stubs:
            raise TemplateNotFound(
                f"Unknown mock gRPC stub: {name}",
                {"path": f"mocks.grpc.stubs.{name}"},
            )
        stub = stubs[name]
        item = {
            "category": "mock-grpc-stubs",
            "name": name,
            "description": stub.description,
            "service": stub.service,
            "method": stub.method,
            "params": _grpc_mock_params(stub),
            "example": _mock_grpc_example(stub, _mock_grpc_listen(cfg)),
            "note": "Active only while `agctl mock run` (grpc engine) is running.",
        }
        if stub.match is not None:
            item["match"] = stub.match.model_dump(by_alias=True, exclude_none=True)
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
        "mode": tpl.mode,
        "sql": tpl.sql,  # D9: include verbatim sql
        "params": params,
        "example": _db_example(name, params, tpl.mode),
    }
    if tpl.connection is not None:
        item["connection"] = tpl.connection
    return item


_item_envelope = envelope("discover.item")(_item_core)


def _search_core(config_path: str | None, term: str, overlay_paths: list[str] | None = None, env_file: str | None = None) -> dict:
    cfg = load_config_or_raise(config_path, overlay_paths, env_file=env_file)
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
                    "mode": tpl.mode,
                }
            )

    for name, stub in _mock_http_stubs(cfg).items():
        if (
            needle in name.lower()
            or _matches(stub.description)
            or needle in stub.path.lower()
        ):
            matches.append(
                {
                    "category": "mock-http-stubs",
                    "name": name,
                    "description": stub.description,
                }
            )

    for name, reactor in _mock_kafka_reactors(cfg).items():
        if (
            needle in name.lower()
            or _matches(reactor.description)
            or needle in reactor.topic.lower()
        ):
            matches.append(
                {
                    "category": "mock-kafka-reactors",
                    "name": name,
                    "description": reactor.description,
                }
            )

    for name, src in cfg.logs.sources.items():
        if needle in name.lower() or (src.path and needle in src.path.lower()):
            matches.append(
                {
                    "category": "log-sources",
                    "name": name,
                    "description": f"{src.type} logs for {name} ({src.path or '?'})",
                }
            )

    for name, tgt in cfg.grpc.targets.items():
        if needle in name.lower() or needle in tgt.address.lower():
            matches.append(
                {
                    "category": "grpc-services",
                    "name": name,
                    "description": f"gRPC target {name} at {tgt.address} (tls={tgt.use_tls})",
                }
            )

    for name, tpl in cfg.grpc.templates.items():
        if needle in name.lower() or _matches(tpl.description):
            matches.append(
                {
                    "category": "grpc-methods",
                    "name": name,
                    "description": tpl.description or f"{tpl.service}/{tpl.method}",
                }
            )

    for name, stub in _mock_grpc_stubs(cfg).items():
        if needle in name.lower() or _matches(stub.description):
            matches.append(
                {
                    "category": "mock-grpc-stubs",
                    "name": name,
                    "description": stub.description,
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
@click.option("--overlay", "overlay_paths", multiple=True, default=None, help="Overlay config paths")
@click.option("--env-file", "env_file", default=None, help="Path to .env file (default: .env next to agctl.yaml)")
@click.pass_context
def discover(
    ctx: click.Context,
    env_file: str | None,
    category: str | None,
    name: str | None,
    search: str | None,
    config_path: str | None,
    overlay_paths: tuple[str, ...] | None,
) -> None:
    """Discover configured services, templates, patterns."""
    resolved_config = config_path or (ctx.obj.get("config_path") if ctx.obj else None)
    ovs = tuple(overlay_paths) or (ctx.obj.get("overlay_paths") if ctx.obj else None)
    resolved_overlay = list(ovs) if ovs else None
    env_file = env_file or (ctx.obj.get("env_file") if ctx.obj else None)

    # Mutual exclusion: --category + --search together is an error.
    if category is not None and search is not None:
        _emit_argument_error("Use either --category or --search, not both")
        return
    # --name requires --category.
    if name is not None and category is None:
        _emit_argument_error("--name requires --category")
        return

    if search is not None:
        _search_envelope(resolved_config, search, resolved_overlay, env_file=env_file)
        return

    if category is not None and name is None:
        _category_envelope(resolved_config, category, resolved_overlay, env_file=env_file)
        return

    if category is not None and name is not None:
        _item_envelope(resolved_config, category, name, resolved_overlay, env_file=env_file)
        return

    # No flags — summary.
    _summary_envelope(resolved_config, resolved_overlay, env_file=env_file)

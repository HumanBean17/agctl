"""Structural config validation (DESIGN §3.5 dangling refs, §3.6 warnings).

Operates on a schema-validated Config instance and reports cross-reference
errors that pydantic cannot catch, plus missing-description warnings.
"""

from ..errors import ConfigError
from .models import Config


def validate_config(cfg: Config) -> tuple[list[dict], list[dict]]:
    """Return (errors, warnings).

    Each entry is ``{"path": str, "message": str}``. Errors represent dangling
    references that make the config unusable; warnings are advisory (e.g.
    missing descriptions that degrade discovery).
    """
    errors: list[dict] = []
    warnings: list[dict] = []

    services = set(cfg.services.keys())
    connections = set(cfg.database.connections.keys())

    # §3.5.1 — HTTP template -> service dangling refs.
    for name, tpl in cfg.templates.items():
        if tpl.service not in services:
            errors.append(
                {
                    "path": f"templates.{name}.service",
                    "message": f"Template references unknown service '{tpl.service}'",
                }
            )
        if _missing_description(tpl.description):
            warnings.append(
                {
                    "path": f"templates.{name}",
                    "message": "missing description (discovery degrades without it)",
                }
            )

    # §3.5.2 — DB template -> connection dangling refs.
    for name, tpl in cfg.database.templates.items():
        if tpl.connection is not None and tpl.connection not in connections:
            errors.append(
                {
                    "path": f"database.templates.{name}.connection",
                    "message": f"Template references unknown connection '{tpl.connection}'",
                }
            )
        # Write-mode templates must target a writable connection.
        if tpl.mode == "write":
            # Resolve connection name: template's connection or default.
            resolved_connection = tpl.connection or cfg.defaults.database_connection
            if (
                resolved_connection is None
                or resolved_connection not in cfg.database.connections
                or not cfg.database.connections[resolved_connection].writable
            ):
                errors.append(
                    {
                        "path": f"database.templates.{name}",
                        "message": f"Write template '{name}' must target a writable connection",
                    }
                )
        if _missing_description(tpl.description):
            warnings.append(
                {
                    "path": f"database.templates.{name}",
                    "message": "missing description (discovery degrades without it)",
                }
            )

    # §3.5.3 — default connection dangling ref.
    if (
        cfg.defaults.database_connection is not None
        and cfg.defaults.database_connection not in connections
    ):
        errors.append(
            {
                "path": "defaults.database_connection",
                "message": (
                    f"Default references unknown connection "
                    f"'{cfg.defaults.database_connection}'"
                ),
            }
        )

    # §3.6 — Kafka pattern missing-description warnings + cluster dangling refs.
    for name, pattern in cfg.kafka.patterns.items():
        if _missing_description(pattern.description):
            warnings.append(
                {
                    "path": f"kafka.patterns.{name}",
                    "message": "missing description (discovery degrades without it)",
                }
            )
        # KafkaPattern.cluster dangling ref (Task 2 consumes the field).
        if pattern.cluster is not None and pattern.cluster not in cfg.kafka.clusters:
            errors.append(
                {
                    "path": f"kafka.patterns.{name}.cluster",
                    "message": (
                        f"Pattern references unknown cluster '{pattern.cluster}'"
                    ),
                }
            )

    # kafka.default_cluster dangling ref (DESIGN §3.5 dangling refs, v3).
    if (
        cfg.kafka.default_cluster is not None
        and cfg.kafka.default_cluster not in cfg.kafka.clusters
    ):
        errors.append(
            {
                "path": "kafka.default_cluster",
                "message": (
                    f"Default references unknown cluster "
                    f"'{cfg.kafka.default_cluster}'"
                ),
            }
        )

    # --- mock server validation -----------------------------------------------

    # Check 1: mocks.kafka reactors require a resolvable default cluster with
    # non-empty brokers. Resolution mirrors resolve_cluster_name (default_cluster
    # -> single-cluster auto-default) but is inlined here so config/ stays free
    # of a commands/ import. (Reactor.cluster dangling-ref is deferred to Task 3
    # where it is consumed.)
    if (
        cfg.mocks is not None
        and cfg.mocks.kafka is not None
        and cfg.mocks.kafka.reactors
    ):
        reactor_cluster = cfg.kafka.default_cluster
        if reactor_cluster is None and len(cfg.kafka.clusters) == 1:
            reactor_cluster = next(iter(cfg.kafka.clusters))
        if reactor_cluster is None or reactor_cluster not in cfg.kafka.clusters:
            errors.append(
                {
                    "path": "mocks.kafka",
                    "message": "reactors require a resolvable default cluster",
                }
            )
        elif not cfg.kafka.clusters[reactor_cluster].brokers:
            errors.append(
                {
                    "path": "mocks.kafka",
                    "message": (
                        f"kafka mocks require kafka.clusters.{reactor_cluster}.brokers"
                    ),
                }
            )

    # Check 2: Missing description warnings for stubs and reactors
    if cfg.mocks is not None:
        if cfg.mocks.http is not None:
            for name, stub in cfg.mocks.http.stubs.items():
                if _missing_description(stub.description):
                    warnings.append(
                        {
                            "path": f"mocks.http.stubs.{name}",
                            "message": "missing description (discovery degrades without it)",
                        }
                    )

        if cfg.mocks.kafka is not None:
            for name, reactor in cfg.mocks.kafka.reactors.items():
                if _missing_description(reactor.description):
                    warnings.append(
                        {
                            "path": f"mocks.kafka.reactors.{name}",
                            "message": "missing description (discovery degrades without it)",
                        }
                    )

    # Check 3: Path-template shadowing warning for HTTP stubs
    if cfg.mocks is not None and cfg.mocks.http is not None:
        stubs = list(cfg.mocks.http.stubs.items())
        for i, (later_name, later_stub) in enumerate(stubs):
            later_segments = later_stub.path.strip("/").split("/")
            for earlier_name, earlier_stub in stubs[:i]:
                earlier_segments = earlier_stub.path.strip("/").split("/")
                # Check if the earlier stub has a parameter at a position where the later stub has a literal
                for pos, (earlier_seg, later_seg) in enumerate(zip(earlier_segments, later_segments)):
                    if earlier_seg.startswith("{") and earlier_seg.endswith("}") and not later_seg.startswith("{"):
                        warnings.append(
                            {
                                "path": f"mocks.http.stubs.{later_name}",
                                "message": f"Path template '{later_name}' is shadowed by '{earlier_name}' — literal segment '{later_seg}' at position {pos} would never match because '{earlier_name}' has parameter {{{earlier_seg}}} at that position (first match wins)",
                            }
                        )
                        break  # Only warn once per later stub

    # Check 4: jq-shadowing warning for HTTP stubs (method-gated, spec §10).
    # Two stubs sharing the same method (case-insensitive) AND the same path
    # template AND both carrying a non-None ``match.jq`` are "distinguished
    # only by jq" — a wrong predicate can silently fire the wrong branch
    # (first match wins). Unlike Check 3 this is method-gated so that
    # ``GET /api/{id}`` vs ``DELETE /api/users`` does not false-warn.
    if cfg.mocks is not None and cfg.mocks.http is not None:
        stubs = list(cfg.mocks.http.stubs.items())
        for i, (later_name, later_stub) in enumerate(stubs):
            for earlier_name, earlier_stub in stubs[:i]:
                if (
                    earlier_stub.method.upper() == later_stub.method.upper()
                    and earlier_stub.path == later_stub.path
                    and earlier_stub.match is not None
                    and earlier_stub.match.jq is not None
                    and later_stub.match is not None
                    and later_stub.match.jq is not None
                ):
                    warnings.append(
                        {
                            "path": f"mocks.http.stubs.{later_name}",
                            "message": f"Stub '{later_name}' is shadowed by '{earlier_name}' — same method+path and both use match.jq (first match wins; a wrong predicate can fire the wrong branch silently).",
                        }
                    )
                    break  # Only warn once per later stub

    # --- logs sources validation -----------------------------------------------

    # Validate each log source (backend discovery + config validation)
    for name, source in cfg.logs.sources.items():
        try:
            # Local import to avoid module-load cycle
            from ..clients.log_client import LogClient

            LogClient(source)  # Construction runs validate_config()
        except ConfigError as err:
            errors.append(
                {"path": f"logs.sources.{name}", "message": str(err)}
            )

    return errors, warnings


def _missing_description(value: str | None) -> bool:
    return value is None or not str(value).strip()

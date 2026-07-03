"""Structural config validation (DESIGN §3.5 dangling refs, §3.6 warnings).

Operates on a schema-validated Config instance and reports cross-reference
errors that pydantic cannot catch, plus missing-description warnings.
"""

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

    # §3.6 — Kafka pattern missing-description warnings.
    for name, pattern in cfg.kafka.patterns.items():
        if _missing_description(pattern.description):
            warnings.append(
                {
                    "path": f"kafka.patterns.{name}",
                    "message": "missing description (discovery degrades without it)",
                }
            )

    return errors, warnings


def _missing_description(value: str | None) -> bool:
    return value is None or not str(value).strip()

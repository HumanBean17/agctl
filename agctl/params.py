"""--param tuple parsing (DESIGN D2).

Turns the repeatable ``--param k=v`` CLI tuples into a ``dict[str, str]``.
"""

from __future__ import annotations

from .errors import ConfigError

__all__ = ["parse_params"]


def parse_params(values: tuple[str, ...]) -> dict[str, str]:
    """Turn repeatable ``--param k=v`` tuples into a dict.

    - Split on the FIRST ``=`` only (the value may itself contain ``=``).
    - A value with no ``=`` is invalid and raises :class:`ConfigError`.
    - An empty tuple yields an empty dict.
    """
    result: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise ConfigError(
                f"invalid --param value {item!r}: expected 'key=value' form",
                {"value": item},
            )
        key, _, value = item.partition("=")
        result[key] = value
    return result

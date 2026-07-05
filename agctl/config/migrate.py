"""Pure helper that rewrites a v1 config to dialect ``"2"`` (Task 6).

``agctl`` v2 made every ``match`` expression envelope-rooted (HTTP ``.body``,
Kafka ``.value``). A v1 config loaded by the v2 tool is rejected by the loader
with a pointer to ``agctl config migrate``; this module performs the actual
rewrite — :func:`migrate_match_exprs` is a pure dict→dict transform with no
file I/O, so the Click command in :mod:`agctl.commands.config_commands` can
drive it for both real runs and ``--dry-run`` previews.

Semantics (DESIGN §3.5; Task 6 brief):

- ``source_major = str(config.get("version", "")).split(".")[0]``.
- If ``source_major == "2"``: already-v2 — return ``already_v2=True``, deep
  copy unchanged, ``rewrites=[]``. A v2-native config may carry a v2-style
  expr like ``.body.amount`` (no ``.body | `` prefix); the helper must NOT
  prepend to it.
- Otherwise (``"1"``, ``""``, or any legacy value): deep-copy the config,
  set ``config["version"] = "2"``, walk the three match-site families and, for
  each expression that does NOT already ``startswith`` its transport prefix,
  prepend the prefix and record a rewrite:

  * ``mocks.http.stubs.<name>.match.jq`` → prefix ``.body | ``
    (only when both ``match`` and ``match.jq`` are present).
  * ``mocks.kafka.reactors.<name>.match`` → prefix ``.value | ``
    (only when ``match`` is present and a string).
  * ``kafka.patterns.<name>.match`` → prefix ``.value | ``
    (only when ``match`` is present and a string).

Traversal order for ``rewrites``: HTTP stubs (dict order), then Kafka reactors
(dict order), then ``kafka.patterns`` (dict order). ``capture.*.from`` and
``match.body`` are NOT visited (out of scope). Defensive ``.get()`` chains: a
missing section contributes no sites and never raises.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

#: Prefix prepended to HTTP ``match.jq`` expressions.
HTTP_PREFIX = ".body | "
#: Prefix prepended to Kafka ``match`` expressions.
KAFKA_PREFIX = ".value | "

#: Target dialect for any migration.
TO_VERSION = "2"


@dataclass
class MigrateResult:
    """Result of :func:`migrate_match_exprs`.

    Attributes:
        config: A deep-copied, transformed config (always a fresh dict — the
            input is never mutated). When ``already_v2`` is True, this is a
            deep copy of the input unchanged.
        from_version: The original raw ``version`` value (or ``""`` if absent).
        to_version: Always ``"2"``.
        rewrites: Per-site rewrite records, in traversal order, each
            ``{"path": str, "before": str, "after": str}``. Empty when
            ``already_v2`` is True.
        already_v2: True iff ``source_major == "2"`` (no migration performed).
    """

    config: dict[str, Any]
    from_version: str
    to_version: str = TO_VERSION
    rewrites: list[dict[str, str]] = field(default_factory=list)
    already_v2: bool = False


def _prepend(expr: str, prefix: str) -> str:
    """Idempotently prepend ``prefix`` to ``expr`` (no double-prefix)."""
    return expr if expr.startswith(prefix) else prefix + expr


def _migrate_http_stubs(config: dict[str, Any], rewrites: list[dict[str, str]]) -> None:
    """Walk ``mocks.http.stubs.<name>.match.jq`` and prepend the HTTP prefix."""
    stubs = (
        config.get("mocks", {}).get("http", {}).get("stubs", {})
    )
    for name, stub in stubs.items():
        if not isinstance(stub, dict):
            continue
        match = stub.get("match")
        if not isinstance(match, dict) or "jq" not in match:
            continue
        jq = match["jq"]
        if not isinstance(jq, str):
            continue
        path = f"mocks.http.stubs.{name}.match.jq"
        before = jq
        after = _prepend(jq, HTTP_PREFIX)
        match["jq"] = after
        if after != before:
            rewrites.append({"path": path, "before": before, "after": after})


def _migrate_kafka_reactors(
    config: dict[str, Any], rewrites: list[dict[str, str]]
) -> None:
    """Walk ``mocks.kafka.reactors.<name>.match`` and prepend the Kafka prefix."""
    reactors = (
        config.get("mocks", {}).get("kafka", {}).get("reactors", {})
    )
    for name, reactor in reactors.items():
        if not isinstance(reactor, dict):
            continue
        match = reactor.get("match")
        if not isinstance(match, str):
            continue
        path = f"mocks.kafka.reactors.{name}.match"
        before = match
        after = _prepend(match, KAFKA_PREFIX)
        reactor["match"] = after
        if after != before:
            rewrites.append({"path": path, "before": before, "after": after})


def _migrate_kafka_patterns(
    config: dict[str, Any], rewrites: list[dict[str, str]]
) -> None:
    """Walk ``kafka.patterns.<name>.match`` and prepend the Kafka prefix."""
    patterns = config.get("kafka", {}).get("patterns", {})
    for name, pattern in patterns.items():
        if not isinstance(pattern, dict):
            continue
        match = pattern.get("match")
        if not isinstance(match, str):
            continue
        path = f"kafka.patterns.{name}.match"
        before = match
        after = _prepend(match, KAFKA_PREFIX)
        pattern["match"] = after
        if after != before:
            rewrites.append({"path": path, "before": before, "after": after})


def migrate_match_exprs(config: dict[str, Any]) -> MigrateResult:
    """Rewrite a v1 config to dialect ``"2"`` (pure dict→dict transform).

    See the module docstring for the full contract. Returns a
    :class:`MigrateResult` whose ``config`` is always a fresh deep copy — the
    caller's input is never mutated.
    """
    from_version = str(config.get("version", ""))
    source_major = from_version.split(".")[0]
    if source_major == TO_VERSION:
        return MigrateResult(
            config=copy.deepcopy(config),
            from_version=from_version,
            to_version=TO_VERSION,
            rewrites=[],
            already_v2=True,
        )

    new_config = copy.deepcopy(config)
    new_config["version"] = TO_VERSION
    rewrites: list[dict[str, str]] = []
    # Traversal order per the brief: HTTP stubs → Kafka reactors → kafka.patterns.
    _migrate_http_stubs(new_config, rewrites)
    _migrate_kafka_reactors(new_config, rewrites)
    _migrate_kafka_patterns(new_config, rewrites)

    return MigrateResult(
        config=new_config,
        from_version=from_version,
        to_version=TO_VERSION,
        rewrites=rewrites,
        already_v2=False,
    )

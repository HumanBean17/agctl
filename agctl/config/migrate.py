"""Pure helper that rewrites a v1/v2 config to dialect ``"3"``.

``agctl`` v3 restructured ``Config.kafka`` from a single flat object into a
named map ``kafka.clusters.<name>`` (mirroring ``database.connections``), plus
the version bump ``2 -> 3``. v1 configs additionally need the v2 jq-dialect
rewrite (every ``match`` expression envelope-rooted: HTTP ``.body``, Kafka
``.value``); :func:`migrate_config` carries a v1 input through BOTH the jq
rewrite and the structural lift in one pass. A v2 input needs only the lift
(its match exprs are already envelope-rooted). A config already at v3 is a
clean no-op.

:func:`migrate_config` is a pure dict->dict transform with no file I/O, so the
Click command in :mod:`agctl.commands.config_commands` can drive it for both
real runs and ``--dry-run`` previews.

Semantics (DESIGN §8; Task 1 brief):

- ``source_major = str(config.get("version", "")).strip().split(".")[0]`` -- the
  ``.strip()`` mirrors the loader's ``_check_version`` so a loader-accepted
  ``version: " 3 "`` is seen as already-current here too (not force-lifted).
- If ``source_major == "3"``: already-current -- return ``already_current=True``,
  deep copy unchanged, ``rewrites=[]``.
- Otherwise (``"1"``, ``"2"``, ``""``, or any legacy value): deep-copy the
  config, set ``config["version"] = "3"``. Then, **only when source_major is
  ``"1"``**, walk the three match-site families and prefix each expression that
  does NOT already ``startswith`` its transport prefix:

  * ``mocks.http.stubs.<name>.match.jq`` -> prefix ``.body | ``
    (only when both ``match`` and ``match.jq`` are present).
  * ``mocks.kafka.reactors.<name>.match`` -> prefix ``.value | ``
    (only when ``match`` is present and a string).
  * ``kafka.patterns.<name>.match`` -> prefix ``.value | ``
    (only when ``match`` is present and a string).

  (v2 inputs skip the jq walkers entirely -- their exprs are already
  envelope-rooted, and force-prepending would double-prefix v2-native exprs
  like ``.body.amount`` into the broken ``.body | .body.amount``.)

- Then run the structural lift ``_lift_kafka_clusters`` unconditionally (both
  v1 and v2 sources may carry a flat ``kafka:`` block): when
  ``config["kafka"]`` is a dict containing at least one of the five flat keys
  (brokers/ssl/timeout_seconds/default_consumer_group/schema_registry_url) and
  NO ``clusters`` key, move those keys into
  ``config["kafka"]["clusters"]["default"]`` and set
  ``config["kafka"]["default_cluster"] = "default"``, recording one structural
  rewrite per lifted key (deterministic order: brokers, ssl, timeout_seconds,
  default_consumer_group, schema_registry_url) plus the ``default_cluster`` set.
  A missing or already-clustered ``kafka`` contributes no rewrites and never
  raises.

Traversal order for ``rewrites``: jq-prefix rewrites first (HTTP stubs (dict
order), then Kafka reactors (dict order), then ``kafka.patterns`` (dict
order)), then structural rewrites. ``capture.*.from`` and ``match.body`` are
NOT visited (out of scope). Defensive ``.get()`` chains throughout. The
function never mutates its input (deep-copy first).
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

#: Prefix prepended to HTTP ``match.jq`` expressions (v1 sources only).
HTTP_PREFIX = ".body | "
#: Prefix prepended to Kafka ``match`` expressions (v1 sources only).
KAFKA_PREFIX = ".value | "

#: Target dialect for any migration.
TO_VERSION = "3"

#: The five flat ``kafka:`` keys lifted into ``kafka.clusters.<name>``.
#: Order is the deterministic rewrite-emission order.
_FLAT_KAFKA_KEYS = (
    "brokers",
    "ssl",
    "timeout_seconds",
    "default_consumer_group",
    "schema_registry_url",
)

#: Default cluster name assigned to a lifted flat ``kafka:`` block.
_DEFAULT_CLUSTER_NAME = "default"


@dataclass
class MigrateResult:
    """Result of :func:`migrate_config`.

    Attributes:
        config: A deep-copied, transformed config (always a fresh dict -- the
            input is never mutated). When ``already_current`` is True, this is
            a deep copy of the input unchanged.
        from_version: The original raw ``version`` value (or ``""`` if absent).
        to_version: Always ``"3"``.
        rewrites: Per-site rewrite records, in traversal order. jq-prefix
            rewrites are ``{"path": str, "before": str, "after": str}``;
            structural rewrites are
            ``{"path": "kafka.clusters.<name>.<field>", "before": <value>,
            "after": <value>}`` (plus the ``default_cluster`` set record).
            Empty when ``already_current`` is True.
        already_current: True iff ``source_major == "3"`` (no migration
            performed).
    """

    config: dict[str, Any]
    from_version: str
    to_version: str = TO_VERSION
    rewrites: list[dict[str, Any]] = field(default_factory=list)
    already_current: bool = False


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


def _lift_kafka_clusters(
    config: dict[str, Any], rewrites: list[dict[str, Any]]
) -> None:
    """Lift a flat ``kafka:`` block into ``kafka.clusters.<default>``.

    Fires only when ``config["kafka"]`` is a dict holding at least one of the
    five flat keys and NO ``clusters`` key. Records one structural rewrite per
    lifted key (deterministic order) plus the ``default_cluster`` set. A
    missing or already-clustered ``kafka`` contributes nothing and never
    raises.
    """
    kafka = config.get("kafka")
    if not isinstance(kafka, dict):
        return
    if "clusters" in kafka:
        return  # already clustered -- never double-lift
    if not any(key in kafka for key in _FLAT_KAFKA_KEYS):
        return  # nothing flat to lift (e.g. only patterns)

    kafka.setdefault("clusters", {})
    cluster: dict[str, Any] = {}
    kafka["clusters"][_DEFAULT_CLUSTER_NAME] = cluster

    for key in _FLAT_KAFKA_KEYS:
        if key in kafka:
            before = kafka[key]
            cluster[key] = before
            del kafka[key]
            rewrites.append(
                {
                    "path": f"kafka.clusters.{_DEFAULT_CLUSTER_NAME}.{key}",
                    "before": before,
                    "after": before,
                }
            )

    before_default = kafka.get("default_cluster")
    kafka["default_cluster"] = _DEFAULT_CLUSTER_NAME
    rewrites.append(
        {
            "path": "kafka.default_cluster",
            "before": before_default,
            "after": _DEFAULT_CLUSTER_NAME,
        }
    )


def migrate_config(config: dict[str, Any]) -> MigrateResult:
    """Rewrite a v1/v2 config to dialect ``"3"`` (pure dict->dict transform).

    See the module docstring for the full contract. Returns a
    :class:`MigrateResult` whose ``config`` is always a fresh deep copy -- the
    caller's input is never mutated.
    """
    # Match the loader's _check_version exactly: str(...).strip(). Without the
    # strip, a loader-accepted `version: " 3 "` reads as source_major " 3 " and
    # would force-lift an already-v3 config.
    from_version = str(config.get("version", "")).strip()
    source_major = from_version.split(".")[0]
    if source_major == TO_VERSION:
        return MigrateResult(
            config=copy.deepcopy(config),
            from_version=from_version,
            to_version=TO_VERSION,
            rewrites=[],
            already_current=True,
        )

    new_config = copy.deepcopy(config)
    new_config["version"] = TO_VERSION
    rewrites: list[dict[str, Any]] = []

    # jq-prefix walkers run ONLY on v1 sources: v2+ exprs are already
    # envelope-rooted, so force-prepending would double-prefix them.
    if source_major == "1":
        # Traversal order per the brief: HTTP stubs -> Kafka reactors -> kafka.patterns.
        _migrate_http_stubs(new_config, rewrites)
        _migrate_kafka_reactors(new_config, rewrites)
        _migrate_kafka_patterns(new_config, rewrites)

    # Structural lift runs for any non-current source (v1 and v2 both may carry
    # a flat kafka: block). Defensive: a missing/already-clustered kafka is a
    # no-op.
    _lift_kafka_clusters(new_config, rewrites)

    return MigrateResult(
        config=new_config,
        from_version=from_version,
        to_version=TO_VERSION,
        rewrites=rewrites,
        already_current=False,
    )

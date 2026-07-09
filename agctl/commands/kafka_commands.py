"""`kafka produce/consume/assert` commands (DESIGN §3.2, D6, D10).

- ``kafka produce`` publishes one JSON-encoded message and returns the
  DESIGN §4.2 ``kafka.produce`` shape.
- ``kafka consume`` reads the lookback window (D6), optionally filters via a jq
  predicate (``--match``; ``--filter-key`` is a deprecated alias), and optionally
  asserts a minimum count (D10: a count miss is an AssertionError, exit 1).
- ``kafka assert`` polls the window incrementally and returns the FIRST message
  matching all supplied modes (``--contains`` / ``--match`` / ``--pattern``),
  using :meth:`KafkaClient.find_in_window` for early-stop. A timeout with no
  match is an AssertionError (D10).

Both ``consume`` and ``assert`` honor the D6 lookback window: partitions are
seeked to ``now - lookback_seconds`` via ``offsets_for_times`` (or to the
partition beginning via the logical ``OFFSET_BEGINNING`` with
``--from-beginning``).
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any, Callable

import click

from ..assertions import compile_jq, jq_bool, jq_value, json_subset
from ..command import envelope, load_config_or_raise

if TYPE_CHECKING:
    from ..config.models import KafkaCluster, KafkaConfig
from ..errors import AssertionFailure, ConfigError, TemplateNotFound
from ..params import parse_params
from ..resolution import fill_placeholders

__all__ = [
    "kafka_produce",
    "kafka_consume",
    "kafka_assert",
    "new_kafka_client",
    "resolve_cluster_name",
]


def resolve_cluster_name(
    cfg_kafka: "KafkaConfig",
    explicit: str | None,
    binding_cluster: str | None = None,
) -> str:
    """Resolve the Kafka cluster **name** to use (DESIGN §6, D2/D3).

    Precedence (mirrors :func:`resolve_connection_name` in ``db_commands``,
    plus single-cluster auto-default):

    * ``explicit`` (the ``--cluster`` flag — wired in Task 2)
    * ``binding_cluster`` (a pattern/reactor ``.cluster`` — Tasks 2-3)
    * ``cfg_kafka.default_cluster``
    * the single cluster when exactly one is defined

    Raises :class:`ConfigError` if none resolves (>1 cluster / no default), or
    if the resolved name is absent from ``cfg_kafka.clusters``. Returns the
    cluster NAME; callers index ``cfg_kafka.clusters[name]``.
    """
    name = explicit
    if name is None:
        name = binding_cluster
    if name is None:
        name = cfg_kafka.default_cluster
    if name is None:
        # Single-cluster auto-default (D3): the overwhelmingly common config.
        cluster_names = list(cfg_kafka.clusters.keys())
        if len(cluster_names) == 1:
            name = cluster_names[0]
    if name is None:
        raise ConfigError("No kafka cluster specified", {})
    if name not in cfg_kafka.clusters:
        raise ConfigError(f"Unknown kafka cluster: {name}", {"cluster": name})
    return name


def _kafka_ssl_conf(cluster: "KafkaCluster") -> dict[str, str]:
    """Translate a cluster's ``ssl`` block into librdkafka conf keys.

    Returns an empty dict when no TLS knobs are configured. When any knob is
    set, ``security.protocol`` defaults to ``"SSL"`` (mTLS) unless
    ``ssl.security_protocol`` overrides it — so users can enable TLS by filling
    in CA/cert/key without also remembering to flip the protocol. Hostname
    verification is left to librdkafka's secure default unless
    ``endpoint_identification_algorithm`` is set (e.g. ``"none"``).

    Empty strings count as unset: ``${VAR:-}`` interpolation resolves an
    absent env var to ``""``, and an empty ``ssl.endpoint.identification.algorithm``
    must NOT flip verification off the way librdkafka treats ``""`` == ``"none"``.
    """
    ssl = cluster.ssl
    if ssl is None:
        return {}
    conf: dict[str, str] = {}
    if ssl.ca_location:
        conf["ssl.ca.location"] = ssl.ca_location
    if ssl.certificate_location:
        conf["ssl.certificate.location"] = ssl.certificate_location
    if ssl.key_location:
        conf["ssl.key.location"] = ssl.key_location
    if ssl.key_password:
        conf["ssl.key.password"] = ssl.key_password
    if ssl.endpoint_identification_algorithm:
        conf["ssl.endpoint.identification.algorithm"] = ssl.endpoint_identification_algorithm
    if conf:
        conf["security.protocol"] = ssl.security_protocol or "SSL"
    return conf


def new_kafka_client(cluster, group_id=None):
    """Build a real :class:`KafkaClient` from a resolved :class:`KafkaCluster`.

    Test seam: tests monkeypatch this attribute
    (``monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)``) to
    return a KafkaClient built with FakeConsumer/FakeProducer, avoiding any real
    broker connection. The factory signature takes the resolved ``cluster``
    (not ``cfg.kafka``) so multi-cluster selection (Tasks 2-3) is decoupled
    from client construction.
    """
    from ..clients.kafka_client import KafkaClient

    return KafkaClient(
        cluster.brokers,
        group_id=group_id,
        extra_conf=_kafka_ssl_conf(cluster),
    )


def _resolve_timeout(cli_timeout, cluster_timeout, fallback=30):
    """First non-None of (cli, cluster.timeout_seconds, 30); coerced to float."""
    for candidate in (cli_timeout, cluster_timeout, fallback):
        if candidate is not None:
            return float(candidate)
    return float(fallback)


def _resolve_group(cli_group, cluster):
    """``--consumer-group`` > ``cluster.default_consumer_group`` > default."""
    if cli_group is not None:
        return cli_group
    if cluster.default_consumer_group is not None:
        return cluster.default_consumer_group
    return "agctl-consumer"


# --------------------------------------------------------------------------- #
# kafka produce
# --------------------------------------------------------------------------- #


def _kafka_produce_core(
    config_path: str | None,
    topic: str,
    message: str,
    key: str | None,
    header: tuple[str, ...],
    overlay_paths: list[str] | None = None,
) -> dict:
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths)
    value = json.loads(message)
    headers = parse_params(header) if header else None

    # Resolve the cluster (Task 1: no flag/binding yet -> default/single-cluster).
    name = resolve_cluster_name(cfg.kafka, None)
    cluster = cfg.kafka.clusters[name]
    client = new_kafka_client(cluster)
    return client.produce(topic, value, key=key, headers=headers or None)


@click.command("produce")
@click.option("--topic", "topic", required=True, help="Kafka topic to produce to")
@click.option("--message", "message", required=True, help="JSON message body")
@click.option("--key", "key", default=None, help="Message key")
@click.option("--header", "header", multiple=True, help="k=v message header")
@click.pass_context
def kafka_produce(
    ctx: click.Context,
    topic: str,
    message: str,
    key: str | None,
    header: tuple[str, ...],
) -> None:
    """Produce one message to a Kafka topic."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    _kafka_produce_envelope(config_path, topic, message, key, header, overlay_paths=list(ovs) if ovs else None)


_kafka_produce_envelope = envelope("kafka.produce")(_kafka_produce_core)


# --------------------------------------------------------------------------- #
# kafka consume
# --------------------------------------------------------------------------- #


def _kafka_consume_core(
    config_path: str | None,
    topic: str,
    timeout: float | None,
    lookback: float | None,
    match: str | None,
    filter_key: str | None,
    expect_count: int | None,
    from_beginning: bool,
    consumer_group: str | None,
    overlay_paths: list[str] | None = None,
) -> dict:
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths)

    # --filter-key is a deprecated alias of --match; both given -> error.
    if match is not None and filter_key is not None:
        raise ConfigError(
            "--match and --filter-key are mutually exclusive (--filter-key is a "
            "deprecated alias of --match)",
            {},
        )
    match_expr = match if match is not None else filter_key

    # Resolve the cluster (Task 1: no flag/binding yet -> default/single-cluster).
    name = resolve_cluster_name(cfg.kafka, None)
    cluster = cfg.kafka.clusters[name]

    resolved_timeout = _resolve_timeout(timeout, cluster.timeout_seconds)
    if resolved_timeout <= 0:
        # timeout <= 0 makes the poll deadline already-passed, so the loop never
        # runs and consume returns ok:0 — a silent no-op masquerading as success.
        raise ConfigError(
            "kafka consume --timeout must be > 0",
            {"timeout": resolved_timeout},
        )
    # D6: default lookback = resolved timeout.
    resolved_lookback = float(lookback) if lookback is not None else resolved_timeout
    group = _resolve_group(consumer_group, cluster)

    # Build the optional jq filter as an inline predicate so consume_window can
    # apply it incrementally AND short-circuit as soon as --expect-count matching
    # messages arrive (DESIGN §3.2 "whichever comes first").
    predicate = None
    if match_expr is not None:
        # Validate syntax ONCE up front: jq_bool swallows compile/runtime errors
        # per-message (returns False, DESIGN §3.2), so a typo'd --match would
        # otherwise silently match nothing and report ok:0. compile_jq surfaces a
        # malformed expression loudly as a ConfigError before any polling.
        compile_jq(match_expr, label="kafka consume --match")

        def predicate(msg, _expr=match_expr):
            return jq_bool(msg, _expr)

    client = new_kafka_client(cluster, group_id=group)
    matched = client.consume_window(
        topic,
        lookback_seconds=resolved_lookback,
        timeout_seconds=resolved_timeout,
        from_beginning=from_beginning,
        predicate=predicate,
        expect_count=expect_count,
    )

    # D10: consume-count-miss is an AssertionError (exit 1).
    if expect_count is not None and len(matched) < expect_count:
        raise AssertionFailure(
            f"Expected {expect_count} messages, got {len(matched)}",
            {
                "expected": expect_count,
                "actual": len(matched),
                "topic": topic,
            },
        )

    # timed_out: the window elapsed without satisfying --expect-count. When the
    # count was met, consume_window short-circuited (not timed out); without an
    # expect-count the window always runs to completion by design (not a failure).
    timed_out = bool(expect_count is not None and len(matched) < expect_count)

    return {
        "topic": topic,
        "messages": matched,
        "count": len(matched),
        "timed_out": timed_out,
    }


@click.command("consume")
@click.option("--topic", "topic", required=True, help="Kafka topic to consume")
@click.option("--timeout", "timeout", type=float, default=None, help="Poll timeout (s)")
@click.option("--lookback", "lookback", type=float, default=None, help="Lookback window (s)")
@click.option(
    "--match",
    "match",
    default=None,
    help="jq predicate against the message envelope {key, value, partition, offset, timestamp, headers}; reach value fields via .value.<field>",
)
@click.option(
    "--filter-key",
    "filter_key",
    default=None,
    help="DEPRECATED alias of --match",
)
@click.option("--expect-count", "expect_count", type=int, default=None, help="Min expected count")
@click.option("--from-beginning", "from_beginning", is_flag=True, default=False)
@click.option("--consumer-group", "consumer_group", default=None, help="Consumer group override")
@click.pass_context
def kafka_consume(
    ctx: click.Context,
    topic: str,
    timeout: float | None,
    lookback: float | None,
    match: str | None,
    filter_key: str | None,
    expect_count: int | None,
    from_beginning: bool,
    consumer_group: str | None,
) -> None:
    """Consume messages from a Kafka topic window."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    _kafka_consume_envelope(
        config_path,
        topic,
        timeout,
        lookback,
        match,
        filter_key,
        expect_count,
        from_beginning,
        consumer_group,
        overlay_paths=list(ovs) if ovs else None,
    )


_kafka_consume_envelope = envelope("kafka.consume")(_kafka_consume_core)


# --------------------------------------------------------------------------- #
# kafka assert
# --------------------------------------------------------------------------- #


def _build_assert_predicate(
    *,
    needle: dict | list | None,
    match: str | None,
    path: str | None,
    filled_pattern_match: str | None,
) -> Callable[[dict], bool]:
    """Build a single predicate combining all supplied assertion modes.

    ALL active modes must pass for a message to match. ``--contains``/``--path``
    reference ``msg["value"]`` and degrade gracefully when the value is not a dict
    (jq/subset return False); ``--match``/``--pattern`` reference the whole ``msg``
    (always a dict).

    Takes the already-parsed ``needle`` (``--contains``) and the already-filled
    ``filled_pattern_match`` (``--pattern``) so the no-match failure detail can
    echo the SAME values without re-parsing — see :func:`_kafka_assert_core`.
    """
    def predicate(msg: dict) -> bool:
        value = msg.get("value")
        # --contains mode
        if needle is not None:
            if path is not None:
                target = jq_value(value, path)
            else:
                target = value
            if not json_subset(needle, target):
                return False
        # --match mode
        if match is not None:
            if not jq_bool(msg, match):
                return False
        # --pattern mode
        if filled_pattern_match is not None:
            if not jq_bool(msg, filled_pattern_match):
                return False
        return True

    return predicate


def _run_kafka_custom_assertion(name, topic, messages, params):
    """DESIGN §9.3: dispatch ``--assertion <name>`` to a registered Assertion mode.

    The mode receives the consumed window's messages as ``context`` and returns
    ``{"passed": bool, ...}``; see :func:`agctl.assertion_registry.evaluate_custom`.
    """
    from ..assertion_registry import evaluate_custom

    context = {
        "topic": topic,
        "messages": messages,
        "count": len(messages),
        "params": params,
    }
    _, detail = evaluate_custom(name, context)
    return {
        "topic": topic,
        "assertion_type": name,
        "passed": True,
        "count": len(messages),
        **detail,
    }


def _kafka_assert_core(
    config_path: str | None,
    topic: str | None,
    contains: str | None,
    match: str | None,
    pattern: str | None,
    param: tuple[str, ...],
    path: str | None,
    lookback: float | None,
    timeout: float,
    from_beginning: bool,
    consumer_group: str | None,
    assertion: str | None,
    overlay_paths: list[str] | None = None,
) -> dict:
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths)
    params = parse_params(param)

    # DESIGN §9.3: a custom assertion mode is mutually exclusive with the
    # built-in match modes.
    if assertion is not None and (
        contains is not None or match is not None or pattern is not None
    ):
        raise ConfigError(
            "--assertion is mutually exclusive with --contains/--match/--pattern",
            {},
        )

    pattern_match: str | None = None
    inferred_topic: str | None = topic
    if pattern is not None:
        if pattern not in cfg.kafka.patterns:
            raise TemplateNotFound(
                f"Unknown kafka pattern: {pattern}",
                {"path": f"kafka.patterns.{pattern}"},
            )
        pat = cfg.kafka.patterns[pattern]
        pattern_match = pat.match
        # Topic inferred from the pattern when --topic is omitted.
        if inferred_topic is None:
            inferred_topic = pat.topic

    if inferred_topic is None:
        raise ConfigError(
            "kafka assert requires --topic (or a --pattern with a topic)", {}
        )

    resolved_timeout = float(timeout)
    if resolved_timeout <= 0:
        raise ConfigError(
            "kafka assert --timeout must be > 0",
            {"timeout": resolved_timeout},
        )
    resolved_lookback = float(lookback) if lookback is not None else resolved_timeout
    # Resolve the cluster (Task 1: no flag/binding yet -> default/single-cluster;
    # Task 2 passes the pattern's cluster as binding_cluster).
    name = resolve_cluster_name(cfg.kafka, None)
    cluster = cfg.kafka.clusters[name]
    group = _resolve_group(consumer_group, cluster)

    # DESIGN §9.3: a custom assertion mode evaluates the full consumed window.
    if assertion is not None:
        client = new_kafka_client(cluster, group_id=group)
        messages = client.consume_window(
            inferred_topic,
            lookback_seconds=resolved_lookback,
            timeout_seconds=resolved_timeout,
            from_beginning=from_beginning,
        )
        return _run_kafka_custom_assertion(assertion, inferred_topic, messages, params)

    # Parse --contains / fill --pattern ONCE here so the predicate and the
    # no-match failure detail share one source of truth (no double json.loads,
    # and the detail echoes the exact expr that was evaluated, params filled).
    needle = json.loads(contains) if contains is not None else None
    filled_pattern_match = (
        fill_placeholders(pattern_match, params) if pattern_match is not None else None
    )

    # Validate jq expressions ONCE up front: the predicate swallows per-message jq
    # errors (returns False, DESIGN §3.2), so a typo'd --match/--path/--pattern
    # would otherwise silently never match and report "No matching message".
    if match is not None:
        compile_jq(match, label="kafka assert --match")
    if path is not None:
        compile_jq(path, label="kafka assert --path")
    if filled_pattern_match is not None:
        compile_jq(
            filled_pattern_match,
            label=(
                f"kafka pattern {pattern!r}"
                if pattern is not None
                else "kafka assert --pattern"
            ),
        )

    predicate = _build_assert_predicate(
        needle=needle,
        match=match,
        path=path,
        filled_pattern_match=filled_pattern_match,
    )

    client = new_kafka_client(cluster, group_id=group)
    start = time.monotonic()
    matched, scanned = client.find_in_window(
        inferred_topic,
        predicate=predicate,
        lookback_seconds=resolved_lookback,
        timeout_seconds=resolved_timeout,
        from_beginning=from_beginning,
    )
    elapsed_ms = int((time.monotonic() - start) * 1000)

    if matched is None:
        # Echo the active modes with their jq roots so a "no match" is
        # self-debugging: the agent sees which modes were AND-ed and that
        # --match/--pattern root at the message envelope while --contains/--path
        # root at the message value. A per-message ``value`` snapshot is
        # deliberately OMITTED here — on a no-match there is no single
        # representative message to show; the agent runs ``kafka consume`` to
        # inspect the window. (Plan deviation from "value for kafka", confirmed
        # intentional: the modes list is the actual diagnostic.)
        modes = []
        if needle is not None:
            entry = {"mode": "contains", "root": "message value", "needle": needle}
            if path is not None:
                entry["path"] = path
            modes.append(entry)
        if match is not None:
            modes.append({"mode": "match", "root": "message envelope", "expr": match})
        if pattern is not None:
            # name for discovery + the filled expr that was actually evaluated
            modes.append(
                {
                    "mode": "pattern",
                    "root": "message envelope",
                    "pattern": pattern,
                    "expr": filled_pattern_match,
                }
            )
        raise AssertionFailure(
            f"No matching message within {resolved_timeout}s window",
            {
                "topic": inferred_topic,
                "timeout": resolved_timeout,
                "messages_scanned": scanned,
                "modes": modes,
            },
        )

    return {
        "topic": inferred_topic,
        "matched": True,
        "matching_message": matched,
        "messages_scanned": scanned,
        "elapsed_ms": elapsed_ms,
    }


@click.command("assert")
@click.option("--topic", "topic", default=None, help="Kafka topic")
@click.option("--contains", "contains", default=None, help="JSON subset to match")
@click.option(
    "--match",
    "match",
    default=None,
    help="jq predicate against the message envelope {key, value, partition, offset, timestamp, headers}; reach value fields via .value.<field>",
)
@click.option("--pattern", "pattern", default=None, help="Named kafka pattern")
@click.option("--param", "param", multiple=True, help="k=v pattern placeholder")
@click.option(
    "--path",
    "path",
    default=None,
    help="jq path into the MESSAGE VALUE that narrows --contains (e.g. .eventType)",
)
@click.option("--lookback", "lookback", type=float, default=None, help="Lookback window (s)")
@click.option("--timeout", "timeout", type=float, required=True, help="Poll timeout (s)")
@click.option("--from-beginning", "from_beginning", is_flag=True, default=False)
@click.option("--consumer-group", "consumer_group", default=None, help="Consumer group override")
@click.option(
    "--assertion",
    "assertion",
    default=None,
    help="Named custom assertion mode",
)
@click.pass_context
def kafka_assert(
    ctx: click.Context,
    topic: str | None,
    contains: str | None,
    match: str | None,
    pattern: str | None,
    param: tuple[str, ...],
    path: str | None,
    lookback: float | None,
    timeout: float,
    from_beginning: bool,
    consumer_group: str | None,
    assertion: str | None,
) -> None:
    """Assert a matching message exists in a Kafka window."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    _kafka_assert_envelope(
        config_path,
        topic,
        contains,
        match,
        pattern,
        param,
        path,
        lookback,
        timeout,
        from_beginning,
        consumer_group,
        assertion,
        overlay_paths=list(ovs) if ovs else None,
    )


_kafka_assert_envelope = envelope("kafka.assert")(_kafka_assert_core)

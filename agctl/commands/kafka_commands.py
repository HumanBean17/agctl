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
    from ..config.models import Config, KafkaCluster, KafkaConfig
from ..errors import AssertionFailure, ConfigError, ConnectionFailure, TemplateNotFound
from ..params import parse_params
from ..resolution import fill_placeholders

__all__ = [
    "kafka_produce",
    "kafka_consume",
    "kafka_assert",
    "new_kafka_client",
    "resolve_cluster_name",
    "resolve_topic_format",
    "resolve_subject_strategy",
    "resolve_schema_registry_client",
    "probe_schema_registry",
]


# Module-level cache for SchemaRegistryClient instances, keyed by cluster name.
# Memoized per-invocation: a single CLI call resolves the SR client at most
# once per cluster even when produce/consume/assert internals ask repeatedly.
# Tests clear this dict to isolate memoization assertions.
_sr_client_cache: dict[str, object] = {}


def resolve_cluster_name(
    cfg_kafka: "KafkaConfig",
    *,
    explicit: str | None = None,
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


# --------------------------------------------------------------------------- #
# Format / Schema-Registry resolvers (Task 9 — DESIGN §6.2/§6.3).
#
# Precedence for the value/key format:
#   (1) ``--value-format`` / ``--key-format`` CLI flag — applied by the Click
#       layer BEFORE calling :func:`resolve_topic_format` (override level 1);
#   (2) ``cfg.kafka.topics[topic].value_format`` / ``key_format``;
#   (3) ``cfg.kafka.clusters[cluster].value_format`` / ``key_format``;
#   (4) default — ``Format.JSON`` for values, ``Format.KEY_STRING`` for keys.
#
# Unknown topics and missing fields fall through to the next level — these
# resolvers NEVER raise for absence (a ConfigError for a misconfigured
# format-vs-SR combination is the validator's job, surfaced earlier).
# --------------------------------------------------------------------------- #


def resolve_topic_format(
    cfg: "Config", topic: str, cluster: str, which: str
):
    """Resolve the value or key :class:`Format` for ``topic`` on ``cluster``.

    ``which`` is ``"value"`` or ``"key"``. Precedence: topic-level format →
    cluster-level format → default (``Format.JSON`` for value,
    ``Format.KEY_STRING`` for key). An unknown topic or unset field falls
    through to the cluster default and ultimately to the side's default —
    this function does NOT raise for absence.
    """
    from ..serialization import Format

    if which == "value":
        default = Format.JSON
    elif which == "key":
        default = Format.KEY_STRING
    else:
        raise ConfigError(
            f"resolve_topic_format: 'which' must be 'value' or 'key', got {which!r}",
            {"which": which},
        )

    topic_cfg = cfg.kafka.topics.get(topic) if topic else None
    if topic_cfg is not None:
        topic_fmt = (
            topic_cfg.value_format if which == "value" else topic_cfg.key_format
        )
        if topic_fmt is not None:
            return Format(topic_fmt)

    cluster_cfg = cfg.kafka.clusters.get(cluster)
    if cluster_cfg is not None:
        cluster_fmt = (
            cluster_cfg.value_format if which == "value" else cluster_cfg.key_format
        )
        if cluster_fmt is not None:
            return Format(cluster_fmt)

    return default


def resolve_subject_strategy(cfg: "Config", topic: str, cluster: str) -> str:
    """Resolve the encode subject-name strategy for ``topic`` on ``cluster``.

    Returns the topic-level ``subject_strategy`` when set, otherwise the
    Confluent default ``"topic"``. (There is no cluster-level strategy field
    today — the cluster slot in the precedence chain is a reserved extension
    point; falling through to ``"topic"`` matches Confluent's default.)
    """
    topic_cfg = cfg.kafka.topics.get(topic) if topic else None
    if topic_cfg is not None and topic_cfg.subject_strategy is not None:
        return topic_cfg.subject_strategy
    return "topic"


def resolve_schema_registry_client(
    cfg: "Config", cluster: str
):
    """Build (and memoize) the cluster's :class:`SchemaRegistryClient`.

    Returns the cached instance on repeat calls for the same cluster name.
    Returns ``None`` when the cluster has no ``schema_registry_url`` (an
    empty/missing URL counts as absent — the ``${VAR:-}`` interpolation
    resolves an unset env var to ``""``). Construction delegates to
    :class:`SchemaRegistryClient`; a missing ``kafka`` extra surfaces as
    :class:`ConfigError` from there.
    """
    cached = _sr_client_cache.get(cluster)
    if cached is not None:
        return cached

    cluster_cfg = cfg.kafka.clusters.get(cluster)
    if cluster_cfg is None or not cluster_cfg.schema_registry_url:
        return None

    from ..serialization.registry import SchemaRegistryClient

    client = SchemaRegistryClient(
        cluster_cfg.schema_registry_url,
        cluster_cfg.schema_registry,
    )
    _sr_client_cache[cluster] = client
    return client


def probe_schema_registry(sr, cluster: str) -> None:
    """Pre-flight reachability probe for the cluster's Schema Registry.

    Calls :meth:`SchemaRegistryClient.check_reachable`; on failure re-raises
    as :class:`ConnectionFailure` naming BOTH the cluster (available only
    here — the SR client itself knows just the URL) and the URL so operators
    can locate the misconfigured cluster fast. Returns ``None`` on success.
    """
    try:
        sr.check_reachable()
    except ConnectionFailure as exc:
        url = getattr(sr, "_url", None) or "<unknown>"
        raise ConnectionFailure(
            message=(
                f"Schema Registry for cluster {cluster!r} unreachable at "
                f"{url}: {exc.message}"
            ),
            detail={"cluster": cluster},
        ) from exc
    return None


def _resolve_codec(
    cfg: "Config",
    topic: str,
    cluster_name: str,
    cli_value_fmt: str | None,
    cli_key_fmt: str | None,
):
    """Resolve value/key formats and build the ``KafkaClient`` codec dict.

    Returns ``(codec, value_fmt, key_fmt)``. ``codec`` is ``None`` for pure
    JSON topics so the legacy byte-identical decode path applies (avoids a
    ``Format.JSON``-codec divergence). When at least one side resolves to a
    non-default format, an SR client is required and the startup probe runs
    exactly once before the codec is handed back.

    Defense-in-depth: a non-JSON format with no SR URL raises
    :class:`ConfigError` here (the validator already flags this; the command
    layer catches it again so an in-code config can't bypass it).
    """
    from ..serialization import Format

    # Level-1 CLI override wins over topic/cluster resolution.
    value_fmt = (
        Format(cli_value_fmt) if cli_value_fmt is not None
        else resolve_topic_format(cfg, topic, cluster_name, "value")
    )
    key_fmt = (
        Format(cli_key_fmt) if cli_key_fmt is not None
        else resolve_topic_format(cfg, topic, cluster_name, "key")
    )

    # Pure-JSON/string path: pass codec=None so the legacy byte-identical
    # decode applies (T8 review: a Format.JSON codec would diverge from
    # today's json.loads-with-fallback behavior on non-JSON bytes).
    if value_fmt == Format.JSON and key_fmt == Format.KEY_STRING:
        return None, value_fmt, key_fmt

    sr = resolve_schema_registry_client(cfg, cluster_name)
    if sr is None:
        raise ConfigError(
            f"Cluster {cluster_name!r} requires a Schema Registry for "
            f"value={value_fmt.value} key={key_fmt.value} but has no "
            f"schema_registry_url",
            {"cluster": cluster_name},
        )
    # Probe ONCE up front: a misconfigured SR should surface before the
    # first message rather than mid-flow.
    probe_schema_registry(sr, cluster_name)

    subject_strategy = resolve_subject_strategy(cfg, topic, cluster_name)
    codec = {
        "value": {"fmt": value_fmt, "subject_strategy": subject_strategy},
        "key": {"fmt": key_fmt, "subject_strategy": subject_strategy},
        "sr": sr,
    }
    return codec, value_fmt, key_fmt


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


def new_kafka_client(cluster, group_id=None, *, codec=None):
    """Build a real :class:`KafkaClient` from a resolved :class:`KafkaCluster`.

    Test seam: tests monkeypatch this attribute
    (``monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)``) to
    return a KafkaClient built with FakeConsumer/FakeProducer, avoiding any real
    broker connection. The factory signature takes the resolved ``cluster``
    (not ``cfg.kafka``) so multi-cluster selection (Tasks 2-3) is decoupled
    from client construction.

    ``codec`` (Task 9) is forwarded to :class:`KafkaClient` unchanged; the
    shape is the T8 contract
    (``{"value": {"fmt": Format, ...}, "key": {...}, "sr": SchemaRegistryClient | None}``).
    ``None`` keeps the legacy byte-identical JSON/string decode path.
    """
    from ..clients.kafka_client import KafkaClient

    return KafkaClient(
        cluster.brokers,
        group_id=group_id,
        extra_conf=_kafka_ssl_conf(cluster),
        codec=codec,
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
    cluster: str | None = None,
    value_format: str | None = None,
    key_format: str | None = None,
    overlay_paths: list[str] | None = None,
    env_file: str | None = None,
) -> dict:
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths, env_file=env_file)
    value = json.loads(message)
    headers = parse_params(header) if header else None

    # Resolve the cluster: --cluster (explicit) > default > single-cluster.
    name = resolve_cluster_name(cfg.kafka, explicit=cluster)
    resolved = cfg.kafka.clusters[name]
    codec, _value_fmt, _key_fmt = _resolve_codec(
        cfg, topic, name, value_format, key_format
    )
    if codec is not None:
        client = new_kafka_client(resolved, codec=codec)
    else:
        # Pure-JSON path: codec=None keeps the legacy byte-identical encode and
        # the call signature backward-compatible with test fakes that don't
        # accept the codec kwarg (Option B — only thread codec when non-None).
        client = new_kafka_client(resolved)
    return client.produce(topic, value, key=key, headers=headers or None)


@click.command("produce")
@click.option("--topic", "topic", required=True, help="Kafka topic to produce to")
@click.option("--message", "message", required=True, help="JSON message body")
@click.option("--key", "key", default=None, help="Message key")
@click.option("--header", "header", multiple=True, help="k=v message header")
@click.option("--cluster", "cluster", default=None, help="Cluster name override")
@click.option(
    "--value-format",
    "value_format",
    type=click.Choice(["json", "avro", "protobuf"]),
    default=None,
    help="Override the value serialization format (level-1 precedence; "
         "otherwise resolved from topic/cluster config).",
)
@click.option(
    "--key-format",
    "key_format",
    type=click.Choice(["string", "avro", "protobuf"]),
    default=None,
    help="Override the key serialization format (level-1 precedence; "
         "otherwise resolved from topic/cluster config).",
)
@click.pass_context
def kafka_produce(
    ctx: click.Context,
    topic: str,
    message: str,
    key: str | None,
    header: tuple[str, ...],
    cluster: str | None,
    value_format: str | None,
    key_format: str | None,
) -> None:
    """Produce one message to a Kafka topic."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    env_file = ctx.obj.get("env_file") if ctx.obj else None
    _kafka_produce_envelope(
        config_path,
        topic,
        message,
        key,
        header,
        cluster,
        value_format,
        key_format,
        overlay_paths=list(ovs) if ovs else None,
        env_file=env_file,
    )


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
    cluster: str | None = None,
    value_format: str | None = None,
    key_format: str | None = None,
    overlay_paths: list[str] | None = None,
    env_file: str | None = None,
) -> dict:
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths, env_file=env_file)

    # --filter-key is a deprecated alias of --match; both given -> error.
    if match is not None and filter_key is not None:
        raise ConfigError(
            "--match and --filter-key are mutually exclusive (--filter-key is a "
            "deprecated alias of --match)",
            {},
        )
    match_expr = match if match is not None else filter_key

    # Resolve the cluster: --cluster (explicit) > default > single-cluster.
    # No pattern binding on consume (no --pattern option) -> binding_cluster None.
    name = resolve_cluster_name(cfg.kafka, explicit=cluster)
    resolved = cfg.kafka.clusters[name]

    resolved_timeout = _resolve_timeout(timeout, resolved.timeout_seconds)
    if resolved_timeout <= 0:
        # timeout <= 0 makes the poll deadline already-passed, so the loop never
        # runs and consume returns ok:0 — a silent no-op masquerading as success.
        raise ConfigError(
            "kafka consume --timeout must be > 0",
            {"timeout": resolved_timeout},
        )
    # D6: default lookback = resolved timeout.
    resolved_lookback = float(lookback) if lookback is not None else resolved_timeout
    group = _resolve_group(consumer_group, resolved)

    codec, _value_fmt, _key_fmt = _resolve_codec(
        cfg, topic, name, value_format, key_format
    )

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

    # Per-side decode failure counter (T8 on_decode_error callback). Stays at 0
    # for pure-JSON paths (codec=None -> callback never invoked). Non-fatal:
    # the failed side becomes None on the message envelope, the message is
    # still collected, and the count surfaces in the result envelope.
    decode_errors = 0

    def _on_decode_error(_msg):
        nonlocal decode_errors
        decode_errors += 1

    if codec is not None:
        client = new_kafka_client(resolved, group_id=group, codec=codec)
    else:
        client = new_kafka_client(resolved, group_id=group)
    matched = client.consume_window(
        topic,
        lookback_seconds=resolved_lookback,
        timeout_seconds=resolved_timeout,
        from_beginning=from_beginning,
        predicate=predicate,
        expect_count=expect_count,
        on_decode_error=_on_decode_error if codec is not None else None,
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
        "decode_errors": decode_errors,
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
@click.option("--cluster", "cluster", default=None, help="Cluster name override")
@click.option(
    "--value-format",
    "value_format",
    type=click.Choice(["json", "avro", "protobuf"]),
    default=None,
    help="Override the value serialization format (level-1 precedence; "
         "otherwise resolved from topic/cluster config).",
)
@click.option(
    "--key-format",
    "key_format",
    type=click.Choice(["string", "avro", "protobuf"]),
    default=None,
    help="Override the key serialization format (level-1 precedence; "
         "otherwise resolved from topic/cluster config).",
)
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
    cluster: str | None,
    value_format: str | None,
    key_format: str | None,
) -> None:
    """Consume messages from a Kafka topic window."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    env_file = ctx.obj.get("env_file") if ctx.obj else None
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
        cluster,
        value_format,
        key_format,
        overlay_paths=list(ovs) if ovs else None,
        env_file=env_file,
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
    cluster: str | None = None,
    value_format: str | None = None,
    key_format: str | None = None,
    overlay_paths: list[str] | None = None,
    env_file: str | None = None,
) -> dict:
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths, env_file=env_file)
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
    # The pattern's cluster is the binding value for cluster resolution
    # (explicit --cluster, if given, wins — see resolve_cluster_name).
    binding_cluster: str | None = None
    if pattern is not None:
        if pattern not in cfg.kafka.patterns:
            raise TemplateNotFound(
                f"Unknown kafka pattern: {pattern}",
                {"path": f"kafka.patterns.{pattern}"},
            )
        pat = cfg.kafka.patterns[pattern]
        pattern_match = pat.match
        binding_cluster = pat.cluster
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
    # Resolve the cluster: --cluster (explicit) > pattern.cluster (binding) >
    # default > single-cluster.
    name = resolve_cluster_name(
        cfg.kafka, explicit=cluster, binding_cluster=binding_cluster
    )
    resolved = cfg.kafka.clusters[name]
    group = _resolve_group(consumer_group, resolved)

    codec, _value_fmt, _key_fmt = _resolve_codec(
        cfg, inferred_topic, name, value_format, key_format
    )

    # DESIGN §9.3: a custom assertion mode evaluates the full consumed window.
    if assertion is not None:
        decode_errors = 0

        def _on_decode_error(_msg):
            nonlocal decode_errors
            decode_errors += 1

        if codec is not None:
            client = new_kafka_client(resolved, group_id=group, codec=codec)
        else:
            client = new_kafka_client(resolved, group_id=group)
        messages = client.consume_window(
            inferred_topic,
            lookback_seconds=resolved_lookback,
            timeout_seconds=resolved_timeout,
            from_beginning=from_beginning,
            on_decode_error=_on_decode_error if codec is not None else None,
        )
        result = _run_kafka_custom_assertion(
            assertion, inferred_topic, messages, params
        )
        result["decode_errors"] = decode_errors
        return result

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

    decode_errors = 0

    def _on_decode_error(_msg):
        nonlocal decode_errors
        decode_errors += 1

    if codec is not None:
        client = new_kafka_client(resolved, group_id=group, codec=codec)
    else:
        client = new_kafka_client(resolved, group_id=group)
    start = time.monotonic()
    matched, scanned = client.find_in_window(
        inferred_topic,
        predicate=predicate,
        lookback_seconds=resolved_lookback,
        timeout_seconds=resolved_timeout,
        from_beginning=from_beginning,
        on_decode_error=_on_decode_error if codec is not None else None,
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
        "decode_errors": decode_errors,
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
@click.option("--cluster", "cluster", default=None, help="Cluster name override")
@click.option(
    "--value-format",
    "value_format",
    type=click.Choice(["json", "avro", "protobuf"]),
    default=None,
    help="Override the value serialization format (level-1 precedence; "
         "otherwise resolved from topic/cluster config).",
)
@click.option(
    "--key-format",
    "key_format",
    type=click.Choice(["string", "avro", "protobuf"]),
    default=None,
    help="Override the key serialization format (level-1 precedence; "
         "otherwise resolved from topic/cluster config).",
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
    cluster: str | None,
    value_format: str | None,
    key_format: str | None,
) -> None:
    """Assert a matching message exists in a Kafka window."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    env_file = ctx.obj.get("env_file") if ctx.obj else None
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
        cluster,
        value_format,
        key_format,
        overlay_paths=list(ovs) if ovs else None,
        env_file=env_file,
    )


_kafka_assert_envelope = envelope("kafka.assert")(_kafka_assert_core)

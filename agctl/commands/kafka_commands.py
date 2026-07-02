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
seeked to ``now - lookback_seconds`` via ``offsets_for_times`` (or offset 0 with
``--from-beginning``).
"""

from __future__ import annotations

import json
import time
from typing import Any, Callable

import click

from ..assertions import jq_bool, jq_value, json_subset
from ..command import envelope, load_config_or_raise
from ..errors import AssertionFailure, ConfigError, TemplateMissing
from ..params import parse_params
from ..resolution import fill_placeholders

__all__ = [
    "kafka_produce",
    "kafka_consume",
    "kafka_assert",
    "new_kafka_client",
]


def new_kafka_client(cfg_kafka, group_id=None):
    """Build a real :class:`KafkaClient` from ``cfg.kafka``.

    Test seam: tests monkeypatch this attribute
    (``monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)``) to
    return a KafkaClient built with FakeConsumer/FakeProducer, avoiding any real
    broker connection.
    """
    from ..clients.kafka_client import KafkaClient

    return KafkaClient(cfg_kafka.brokers, group_id=group_id)


def _resolve_timeout(cli_timeout, cfg_kafka_timeout, fallback=30):
    """First non-None of (cli, cfg.kafka.timeout_seconds, 30); coerced to float."""
    for candidate in (cli_timeout, cfg_kafka_timeout, fallback):
        if candidate is not None:
            return float(candidate)
    return float(fallback)


def _resolve_group(cli_group, cfg_kafka):
    """``--consumer-group`` > ``cfg.kafka.default_consumer_group`` > default."""
    if cli_group is not None:
        return cli_group
    if cfg_kafka.default_consumer_group is not None:
        return cfg_kafka.default_consumer_group
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
) -> dict:
    cfg = load_config_or_raise(config_path)
    value = json.loads(message)
    headers = parse_params(header) if header else None

    client = new_kafka_client(cfg.kafka)
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
    """Produce one message to a Kafka topic (DESIGN §3.2)."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    _kafka_produce_envelope(config_path, topic, message, key, header)


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
) -> dict:
    cfg = load_config_or_raise(config_path)

    # --filter-key is a deprecated alias of --match; both given -> error.
    if match is not None and filter_key is not None:
        raise ConfigError(
            "--match and --filter-key are mutually exclusive (--filter-key is a "
            "deprecated alias of --match)",
            {},
        )
    match_expr = match if match is not None else filter_key

    resolved_timeout = _resolve_timeout(timeout, cfg.kafka.timeout_seconds)
    # D6: default lookback = resolved timeout.
    resolved_lookback = float(lookback) if lookback is not None else resolved_timeout
    group = _resolve_group(consumer_group, cfg.kafka)

    client = new_kafka_client(cfg.kafka, group_id=group)
    msgs = client.consume_window(
        topic,
        lookback_seconds=resolved_lookback,
        timeout_seconds=resolved_timeout,
        from_beginning=from_beginning,
    )

    if match_expr is not None:
        matched = [m for m in msgs if jq_bool(m["value"], match_expr)]
    else:
        matched = list(msgs)

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

    # timed_out: simplest documented heuristic — when expect-count is set and
    # unmet the window is treated as having elapsed; otherwise False (the window
    # ran to completion by construction).
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
@click.option("--match", "match", default=None, help="jq predicate filter")
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
    """Consume messages from a Kafka topic window (DESIGN §3.2, D6, D10)."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
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
    )


_kafka_consume_envelope = envelope("kafka.consume")(_kafka_consume_core)


# --------------------------------------------------------------------------- #
# kafka assert
# --------------------------------------------------------------------------- #


def _build_assert_predicate(
    *,
    contains: str | None,
    match: str | None,
    path: str | None,
    pattern_match: str | None,
    params: dict[str, str],
) -> Callable[[dict], bool]:
    """Build a single predicate combining all supplied assertion modes.

    ALL active modes must pass for a message to match. Modes referencing
    ``msg["value"]`` degrade gracefully when the value is not a dict (jq/subset
    return False).
    """
    needle = json.loads(contains) if contains is not None else None
    filled_pattern_match = None
    if pattern_match is not None:
        filled_pattern_match = fill_placeholders(pattern_match, params)

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
            if not jq_bool(value, match):
                return False
        # --pattern mode
        if filled_pattern_match is not None:
            if not jq_bool(value, filled_pattern_match):
                return False
        return True

    return predicate


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
) -> dict:
    cfg = load_config_or_raise(config_path)
    params = parse_params(param)

    pattern_match: str | None = None
    inferred_topic: str | None = topic
    if pattern is not None:
        if pattern not in cfg.kafka.patterns:
            raise TemplateMissing(
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
    resolved_lookback = float(lookback) if lookback is not None else resolved_timeout
    group = _resolve_group(consumer_group, cfg.kafka)

    predicate = _build_assert_predicate(
        contains=contains,
        match=match,
        path=path,
        pattern_match=pattern_match,
        params=params,
    )

    client = new_kafka_client(cfg.kafka, group_id=group)
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
        raise AssertionFailure(
            f"No matching message within {resolved_timeout}s window",
            {"topic": inferred_topic, "timeout": resolved_timeout},
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
@click.option("--match", "match", default=None, help="jq predicate")
@click.option("--pattern", "pattern", default=None, help="Named kafka pattern")
@click.option("--param", "param", multiple=True, help="k=v pattern placeholder")
@click.option("--path", "path", default=None, help="jq path to narrow --contains target")
@click.option("--lookback", "lookback", type=float, default=None, help="Lookback window (s)")
@click.option("--timeout", "timeout", type=float, required=True, help="Poll timeout (s)")
@click.option("--from-beginning", "from_beginning", is_flag=True, default=False)
@click.option("--consumer-group", "consumer_group", default=None, help="Consumer group override")
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
) -> None:
    """Assert a matching message exists in a Kafka window (DESIGN §3.2, D6, D10)."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
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
    )


_kafka_assert_envelope = envelope("kafka.assert")(_kafka_assert_core)

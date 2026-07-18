"""confluent_kafka-backed Kafka client for agctl commands (DESIGN §3.2, D6).

Implements the two raw mechanics the ``kafka`` command layer needs:

- :meth:`KafkaClient.produce` — publish one JSON-encoded message and return the
  DESIGN §4.2 ``kafka.produce`` shape.
- :meth:`KafkaClient.consume_window` — seek each partition to a timestamp
  lookback window (``now - lookback_seconds``) and read forward until a
  timeout, returning *all* messages in the window. Matching/filtering is done
  by the command layer; this module returns raw messages only.

``confluent_kafka`` is an optional extra and is lazy-imported inside both
methods so the module imports cleanly without it. Test seams
(``producer_factory`` / ``consumer_factory``) inject fakes that share the real
Producer/Consumer contract.
"""

import enum
import json
import time
from datetime import datetime, timezone

from ..errors import ConfigError, ConnectionFailure, SerializationError

_KAFKA_EXTRA_MSG = "Kafka support requires the 'kafka' extra: pip install 'agctl[kafka]'"

# Default subject-name strategy when a codec config omits ``subject_strategy``
# (the most common Confluent convention). ``resolve_subject`` requires a
# concrete strategy, so the codec path fills in ``"topic"`` rather than
# passing ``None`` through.
_DEFAULT_SUBJECT_STRATEGY = "topic"


class ReactionResult(enum.Enum):
    """Result of a message handler in consume_loop.

    COMMIT: message was processed successfully → commit offset and continue.
    RETRY: transient failure → seek back and retry (only when final=False).
    STOP: reactor is done → exit loop immediately.
    """
    COMMIT = "commit"
    RETRY = "retry"
    STOP = "stop"


def _import_kafka():
    """Lazy-import confluent_kafka primitives or raise ConfigError."""
    try:
        from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

        try:
            from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, TopicPartition
        except ImportError:  # pragma: no cover - always present in confluent_kafka
            OFFSET_BEGINNING = -2
            OFFSET_END = -1
            TopicPartition = None
    except ImportError as exc:
        raise ConfigError(_KAFKA_EXTRA_MSG) from exc
    return (
        Consumer,
        Producer,
        TopicPartition,
        KafkaError,
        KafkaException,
        OFFSET_END,
        OFFSET_BEGINNING,
    )


def _import_serialization():
    """Lazy-import the serialization surface (Task 7 API).

    Kept lazy so this module imports cleanly even when callers never use a
    codec (``codec=None``) — the serialization package pulls in the SR client
    and (transitively) the typed config models, which are wasted import cost
    for the legacy JSON path. Each codec-aware method imports once on first
    use; Python caches the module afterward.
    """
    from ..serialization import Format, decode_payload, encode_payload, resolve_subject

    return Format, decode_payload, encode_payload, resolve_subject


def _ms_to_iso8601z(ts_ms):
    """Convert a Kafka timestamp (ms since epoch) to an ISO8601Z string."""
    if ts_ms is None or ts_ms < 0:
        return None
    return (
        datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )


def _decode_bytes(raw):
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return raw


class KafkaClient:
    """Minimal Kafka client wrapping confluent_kafka Producer/Consumer.

    Parameters
    ----------
    brokers:
        ``list[str]`` or a single ``str`` of broker addresses.
    group_id:
        Optional consumer group id. Falls back to ``"agctl-consumer"`` when
        consuming if left ``None``.
    extra_conf:
        Optional mapping of extra confluent_kafka conf keys merged into both the
        producer and consumer confs (e.g. ``security.protocol`` / ``ssl.*`` for
        TLS). The client stays transport-agnostic; the command layer owns the
        typed→librdkafka translation.
    consumer_factory / producer_factory:
        Test-injection seams. Each is a callable ``conf -> (Consumer|Producer)``
        used in place of the real confluent_kafka classes.
    codec:
        Optional serialization codec (Task 8). Shape::

            {"value": {"fmt": Format, "subject_strategy": str | None} | None,
             "key":   {"fmt": Format, "subject_strategy": str | None} | None,
             "sr":    SchemaRegistryClient | None}

        When ``None`` (the default) all methods behave byte-for-byte as today
        (raw JSON values, string keys). When set, ``produce`` encodes value/key
        via :func:`encode_payload` (+ :func:`resolve_subject`) before publish
        and the consume methods decode value/key via :func:`decode_payload`
        per side. Single-side decode failures are non-fatal: reported via the
        consume method's ``on_decode_error`` callback and the failed side
        becomes ``None``. Tombstones (``value=None``) decode to ``None``.
    """

    def __init__(
        self,
        brokers,
        group_id=None,
        *,
        extra_conf=None,
        consumer_factory=None,
        producer_factory=None,
        codec=None,
    ):
        self._brokers = brokers if isinstance(brokers, list) else [brokers]
        self._group_id = group_id
        self._extra_conf = dict(extra_conf or {})
        self._consumer_factory = consumer_factory
        self._producer_factory = producer_factory
        self._codec = codec

    # ------------------------------------------------------------------
    # produce
    # ------------------------------------------------------------------

    def produce(self, topic, value, *, key=None, headers=None) -> dict:
        """Publish one message and return the DESIGN §4.2 ``kafka.produce`` shape.

        ``value`` is JSON-encoded; ``key`` and header values are encoded to
        bytes if they are strings. When a ``codec`` is configured with a
        non-JSON value format (or non-string key format), ``value``/``key``
        are encoded via :func:`encode_payload` against the codec's SR client
        and resolved subject BEFORE publish; encode failures surface as
        :class:`SerializationError`. The returned shape's ``key`` stays
        ``_decode_bytes(key_bytes)`` (today's behavior) regardless of codec.
        """
        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END, OFFSET_BEGINNING = _import_kafka()

        producer_conf = {"bootstrap.servers": ",".join(self._brokers)}
        producer_conf.update(self._extra_conf)
        if self._producer_factory is not None:
            producer = self._producer_factory(producer_conf)
        else:
            producer = Producer(producer_conf)

        value_bytes, key_bytes = self._encode_payload(topic, value, key)

        header_pairs = None
        if headers:
            header_pairs = []
            for k, v in headers.items():
                v_bytes = v.encode("utf-8") if isinstance(v, str) else v
                header_pairs.append((k, v_bytes))

        # One-cell holder for the delivery report: [err, msg].
        holder = [None, None]

        def _on_delivery(err, msg):
            holder[0] = err
            holder[1] = msg

        try:
            producer.produce(
                topic,
                value=value_bytes,
                key=key_bytes,
                headers=header_pairs,
                on_delivery=_on_delivery,
            )
            remaining = producer.flush(timeout=30)
        except KafkaException as exc:
            raise ConnectionFailure(message=str(exc)) from exc
        except Exception as exc:  # broker connection issues, etc.
            raise ConnectionFailure(message=str(exc)) from exc

        err, msg = holder[0], holder[1]
        if err is not None:
            raise ConnectionFailure(message=str(err))
        if remaining:
            # flush() returns the count of messages still queued (undelivered). A
            # non-zero count with no delivery-report error means the broker was
            # unreachable within the timeout — fail loudly instead of reporting
            # null partition/offset as a silent success.
            raise ConnectionFailure(
                message=f"{remaining} message(s) not delivered within flush timeout"
            )

        ts_type, ts_ms = (None, None)
        if msg is not None:
            ts_type, ts_ms = msg.timestamp()

        return {
            "topic": topic,
            "partition": msg.partition() if msg is not None else None,
            "offset": msg.offset() if msg is not None else None,
            "key": _decode_bytes(key_bytes),
            "timestamp": _ms_to_iso8601z(ts_ms),
        }

    def _encode_payload(self, topic, value, key):
        """Encode value+key bytes per the configured codec (or legacy JSON).

        Returns ``(value_bytes, key_bytes)`` ready for ``producer.produce``.
        When ``self._codec is None`` (or a side's config is absent / JSON /
        KEY_STRING) the legacy path applies byte-for-byte — json.dumps for
        the value, utf-8 for a string key. Only non-JSON value formats and
        non-string key formats route through ``encode_payload``.

        ``SerializationError`` from the codec propagates unchanged (produce
        is the write path — a schema-violating record is a fatal
        contract/config bug, not a per-message skip).
        """
        if self._codec is None:
            return json.dumps(value).encode("utf-8"), (
                key.encode("utf-8") if isinstance(key, str) else key
            )

        Format, _decode, encode_payload, resolve_subject = _import_serialization()
        sr = self._codec.get("sr")
        value_cfg = self._codec.get("value") or {}
        key_cfg = self._codec.get("key") or {}
        value_fmt = value_cfg.get("fmt", Format.JSON)
        key_fmt = key_cfg.get("fmt", Format.KEY_STRING)

        # Value: JSON / unset fmt -> legacy json.dumps (byte-identical). Any
        # other fmt routes through encode_payload against the resolved subject.
        if value_fmt == Format.JSON:
            value_bytes = json.dumps(value).encode("utf-8")
        else:
            subject = resolve_subject(
                topic,
                "value",
                value_cfg.get("subject_strategy") or _DEFAULT_SUBJECT_STRATEGY,
                value,
            )
            value_bytes = encode_payload(value, value_fmt, sr, subject=subject)

        # Key: None -> None (real Kafka null key). KEY_STRING / unset -> legacy
        # utf-8 (byte-identical). Other fmts -> encode_payload.
        if key is None:
            key_bytes = None
        elif key_fmt == Format.KEY_STRING:
            key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        else:
            subject = resolve_subject(
                topic,
                "key",
                key_cfg.get("subject_strategy") or _DEFAULT_SUBJECT_STRATEGY,
                key,
            )
            key_bytes = encode_payload(key, key_fmt, sr, subject=subject)

        return value_bytes, key_bytes

    # ------------------------------------------------------------------
    # consume_window
    # ------------------------------------------------------------------

    def consume_window(
        self,
        topic,
        *,
        lookback_seconds,
        timeout_seconds,
        from_beginning=False,
        predicate=None,
        expect_count=None,
        on_decode_error=None,
    ) -> list[dict]:
        """Seek each partition to the lookback window and read forward.

        With ``from_beginning=True`` partitions are seeked to offset 0.
        Otherwise each partition is seeked to the earliest offset at/after
        ``now - lookback_seconds`` (via ``offsets_for_times``). The poll loop
        runs until ``timeout_seconds`` of wall-clock time elapses, OR — per
        DESIGN §3.2 ("whichever comes first") — until ``expect_count`` matching
        messages have been collected.

        ``predicate`` (optional) filters messages: only messages for which it
        returns truthy are counted/collected; a predicate that raises is treated
        as a non-match (silently skipped, DESIGN §3.2). ``expect_count`` (optional)
        stops the loop as soon as that many (matched) messages are in hand.

        ``on_decode_error`` (optional) is invoked once per single-side decode
        failure when a codec is configured; the failed side becomes ``None`` in
        that message's envelope (non-fatal — the message is still collected).
        Ignored when ``codec is None``.

        Returns a list of normalized message dicts (DESIGN §4.2 message shape).
        """
        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END, OFFSET_BEGINNING = _import_kafka()

        consumer = self._build_consumer()

        messages: list[dict] = []
        poll_errors: list[str] = []
        try:
            self._setup_seek(consumer, topic, lookback_seconds, from_beginning)

            deadline = time.monotonic() + timeout_seconds
            while time.monotonic() < deadline:
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    # Transient per-message fetch errors (e.g. a brief leader
                    # handover) are skipped; they typically resolve within the
                    # window and a later poll succeeds. We record them so a window
                    # that yielded ZERO messages ENTIRELY due to errors (broker
                    # down / auth failure / topic deleted mid-consume) is surfaced
                    # below as a ConnectionFailure rather than a silent ok:0.
                    poll_errors.append(str(msg.error()))
                    continue
                normalized = self._normalize_message(msg, on_decode_error=on_decode_error)
                if predicate is not None:
                    try:
                        if not predicate(normalized):
                            continue
                    except Exception:
                        # Predicate error -> silently skip this message (DESIGN §3.2).
                        continue
                messages.append(normalized)
                # DESIGN §3.2: return as soon as --expect-count matching messages
                # are received — "whichever comes first" (count satisfied or the
                # timeout window elapses).
                if expect_count is not None and len(messages) >= expect_count:
                    break

            # No messages AND every poll errored: the window didn't fail to match
            # — it failed to READ. A genuinely empty topic yields None polls (no
            # errors), so this only fires when something is actually broken.
            if not messages and poll_errors:
                raise ConnectionFailure(
                    message=(
                        f"Consuming {topic!r} produced only fetch errors "
                        f"({len(poll_errors)} poll(s), last: {poll_errors[-1]})"
                    )
                )
        finally:
            try:
                consumer.close()
            except Exception:  # pragma: no cover - defensive
                pass

        return messages

    # ------------------------------------------------------------------
    # find_in_window  (D6 early-stop path for `kafka assert`)
    # ------------------------------------------------------------------

    def find_in_window(
        self,
        topic,
        *,
        predicate,
        lookback_seconds,
        timeout_seconds,
        from_beginning=False,
        on_decode_error=None,
    ):
        """Poll the window incrementally; return the first message dict for
        which ``predicate(msg)`` is True, or ``None`` if the window elapses with
        no match. Uses the SAME seek/lookback mechanics as :meth:`consume_window`
        (``offsets_for_times`` seek to ``now - lookback_seconds``, or
        ``from_beginning``). ``predicate`` is called on each normalized message
        dict. Terminates promptly: stops polling the moment a match is found.

        ``on_decode_error`` (optional) is invoked once per single-side decode
        failure when a codec is configured; the failed side becomes ``None`` in
        that message's envelope (non-fatal — the message is still scanned).

        Returns a ``(message, scanned_count)`` tuple so callers can report
        ``messages_scanned``; if no match, returns ``(None, scanned_count)``.
        """
        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END, OFFSET_BEGINNING = _import_kafka()

        consumer = self._build_consumer()

        scanned = 0
        poll_errors: list[str] = []
        try:
            self._setup_seek(consumer, topic, lookback_seconds, from_beginning)

            deadline = time.monotonic() + timeout_seconds
            while time.monotonic() < deadline:
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    # See consume_window: skip transient errors, but record them
                    # so an all-error window is surfaced rather than reported as
                    # a clean "no match".
                    poll_errors.append(str(msg.error()))
                    continue
                normalized = self._normalize_message(msg, on_decode_error=on_decode_error)
                scanned += 1
                try:
                    matched = predicate(normalized)
                except Exception:
                    matched = False
                if matched:
                    return normalized, scanned

            # No message scanned AND every poll errored: not "no match" but
            # "couldn't read". Surface it so `kafka assert` distinguishes a broken
            # broker (ConnectionError, exit 2) from a legitimate no-match (exit 1).
            if scanned == 0 and poll_errors:
                raise ConnectionFailure(
                    message=(
                        f"Consuming {topic!r} for assert produced only fetch "
                        f"errors ({len(poll_errors)} poll(s), last: {poll_errors[-1]})"
                    )
                )
        finally:
            try:
                consumer.close()
            except Exception:  # pragma: no cover - defensive
                pass

        return None, scanned

    # ------------------------------------------------------------------
    # consume_loop (committed consume loop for reactors)
    # ------------------------------------------------------------------

    def consume_loop(
        self,
        topic,
        *,
        group_id,
        stop_event,
        handle,
        poll_timeout=0.5,
        max_retries=3,
        on_assign=None,
        on_revoke=None,
        on_decode_error=None,
    ) -> None:
        """Run a committed consume loop with retry logic.

        The loop polls messages, calls ``handle`` with each normalized message
        (plus ``attempt``/``final`` flags), and commits on success. The client
        manages the retry budget; ``handle`` is called with ``attempt`` (1-indexed)
        and ``final`` (True when attempt >= max_retries) and returns a
        :class:`ReactionResult`:

        - ``COMMIT``: message processed → ``store_offsets(msg)`` + ``commit()``.
        - ``RETRY``: transient failure → re-handle the same in-memory message
          (only when ``final`` is False). ``RETRY`` on ``final`` is treated as
          ``COMMIT`` (defensive poison-message guard). No seek/re-poll: see the
          retry-loop rationale in the loop body.
        - ``STOP``: exit loop immediately (reactor is done/dying).

        The consumer is built with the given ``group_id`` (each reactor has its
        own consumer group), used only on this thread, and ``close()``d in
        ``finally`` (D13).

        ``on_decode_error`` (optional) is invoked once per single-side decode
        failure when a codec is configured; the failed side becomes ``None`` in
        that message's envelope (non-fatal — the message is still handed to
        ``handle``, which can COMMIT past it).

        Args:
            topic: Kafka topic to consume.
            group_id: Consumer group id for this reactor (unique per reactor).
            stop_event: ``threading.Event``; the loop exits when set.
            handle: Callable ``handle(msg, *, attempt, final) -> ReactionResult``.
            poll_timeout: Timeout for each ``poll()`` call (default 0.5s).
            max_retries: Maximum retry attempts per message (default 3).
            on_assign: Optional rebalance callback for partition assignment.
            on_revoke: Optional rebalance callback for partition revocation.
            on_decode_error: Optional callback for per-side decode failures.
        """
        if max_retries < 1:
            raise ValueError("max_retries must be >= 1")

        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END, OFFSET_BEGINNING = _import_kafka()

        consumer = self._build_consumer(group_id=group_id)

        try:
            # confluent_kafka rejects an explicit None for on_assign/on_revoke
            # ("on_assign expects a callable"); they must be callable OR omitted.
            # Forward each only when the caller actually provided one (#28).
            subscribe_kwargs = {}
            if on_assign is not None:
                subscribe_kwargs["on_assign"] = on_assign
            if on_revoke is not None:
                subscribe_kwargs["on_revoke"] = on_revoke
            consumer.subscribe([topic], **subscribe_kwargs)

            while not stop_event.is_set():
                msg = consumer.poll(poll_timeout)
                if msg is None:
                    continue
                if msg.error():
                    # Skip individual poll errors
                    continue

                # Retry loop: re-handle the SAME in-memory message until
                # COMMIT/STOP/max_retries. We deliberately do NOT seek+re-poll:
                # on a multi-partition topic a post-seek poll() can return a
                # message from a *different* partition (the seek invalidated this
                # partition's buffer, so a buffered message from another
                # partition is delivered first). That would run later attempts
                # against the wrong message and reset the attempt counter when
                # the original is re-delivered — silently spinning on a poison
                # message forever, defeating the final-attempt forced-COMMIT
                # guard and emitting no kafka.error (violating the fail-loudly
                # contract, DESIGN §11). The handler only needs the normalized
                # dict, so re-handling in-memory is correct and simpler; the
                # commit offset advances only on store_offsets+commit, so
                # crash-recovery positioning is unaffected.
                normalized = self._normalize_message(msg, on_decode_error=on_decode_error)
                for attempt in range(1, max_retries + 1):
                    if stop_event.is_set():
                        return

                    final = attempt >= max_retries
                    result = handle(normalized, attempt=attempt, final=final)

                    if result == ReactionResult.STOP:
                        # Exit loop immediately
                        return

                    if result == ReactionResult.COMMIT or (result == ReactionResult.RETRY and final):
                        # COMMIT (or forced COMMIT on RETRY-at-final)
                        try:
                            consumer.store_offsets(msg)
                            consumer.commit()
                        except KafkaException as exc:
                            raise ConnectionFailure(message=str(exc)) from exc
                        break  # Move to next message

                    # RETRY (not final): re-handle the same in-memory message
                    # (loop continues to the next attempt — no seek, no re-poll).
        finally:
            try:
                consumer.close()
            except Exception:  # pragma: no cover - defensive
                pass

    # ------------------------------------------------------------------
    # probe (one-shot broker connectivity check)
    # ------------------------------------------------------------------

    def probe(self, topic, *, group_id, timeout=5.0) -> None:
        """One-shot connectivity check: list_topics for the topic.

        Builds a consumer with the given ``group_id``, calls
        ``consumer.list_topics(topic, timeout=timeout)``, and closes the
        consumer. Raises ``ConnectionFailure`` on any Kafka/broker error
        (message includes broker list). Propagates ``ConfigError`` if the
        ``kafka`` extra is missing.

        This is the connectivity probe the engine calls before binding HTTP
        (spec §11 "broker unreachable at startup → exit 2").

        Args:
            topic: Kafka topic to check.
            group_id: Consumer group id (unique per reactor).
            timeout: Timeout for ``list_topics`` call (default 5.0s).

        Raises:
            ConfigError: If ``kafka`` extra is not installed.
            ConnectionFailure: If broker is unreachable.
        """
        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END, OFFSET_BEGINNING = _import_kafka()

        consumer = self._build_consumer(group_id=group_id)

        try:
            consumer.list_topics(topic, timeout=timeout)
        except KafkaException as exc:
            raise ConnectionFailure(
                message=f"Kafka broker(s) {','.join(self._brokers)} unreachable: {exc}"
            ) from exc
        except Exception as exc:
            # Broker connection issues, timeout, etc.
            raise ConnectionFailure(
                message=f"Kafka broker(s) {','.join(self._brokers)} unreachable: {exc}"
            ) from exc
        finally:
            try:
                consumer.close()
            except Exception:  # pragma: no cover - defensive
                pass

    # ------------------------------------------------------------------
    # shared helpers
    # ------------------------------------------------------------------

    def _build_consumer(self, group_id=None):
        """Build a consumer (real or via the test factory) with the standard conf.

        Args:
            group_id: Optional override for the consumer group id. If None, uses
                self._group_id (or "agctl-consumer" if that's also None).
        """
        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END, OFFSET_BEGINNING = _import_kafka()

        effective_group_id = group_id if group_id is not None else (self._group_id or "agctl-consumer")
        conf = {
            "bootstrap.servers": ",".join(self._brokers),
            "group.id": effective_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            # Manual offset storage so consume_loop can store_offsets(msg) only
            # AFTER a successful reaction (at-least-once). The librdkafka default
            # (True) stores on poll() — ahead of the reaction — and ALSO makes a
            # later store_offsets(msg) fail with _INVALID_ARG (#28 third half).
            "enable.auto.offset.store": False,
        }
        conf.update(self._extra_conf)

        if self._consumer_factory is not None:
            return self._consumer_factory(conf)
        return Consumer(conf)

    def _setup_seek(self, consumer, topic, lookback_seconds, from_beginning):
        """Subscribe, wait for assignment, then seek partitions to the lookback
        window (or to the logical ``OFFSET_BEGINNING`` when ``from_beginning``).
        Shared by :meth:`consume_window` and :meth:`find_in_window`.

        Raises :class:`ConnectionFailure` if no partitions are assigned after
        the grace window (non-existent topic / unreachable broker).
        """
        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END, OFFSET_BEGINNING = _import_kafka()

        try:
            consumer.subscribe([topic])
        except KafkaException as exc:
            raise ConnectionFailure(message=str(exc)) from exc

        # Assignment may be empty briefly after subscribe; poll a few
        # times to allow it to populate.
        assignment = consumer.assignment()
        attempts = 0
        while not assignment and attempts < 20:
            consumer.poll(0.2)
            assignment = consumer.assignment()
            attempts += 1

        # No partitions assigned after the grace window means the topic does
        # not exist (auto-create off / not yet complete) OR no broker is
        # reachable. Failing loudly here prevents two silent-success modes:
        #   - default mode would otherwise call offsets_for_times([]) and surface
        #     a cryptic "_INVALID_ARG / Failed to get offsets: Invalid argument";
        #   - --from-beginning would otherwise poll an empty assignment to the
        #     timeout and return ok:true count:0 (false success on a dead/empty
        #     topic), which for a testing tool is the worst outcome.
        if not assignment:
            raise ConnectionFailure(
                message=(
                    f"No partitions assigned for topic {topic!r} after "
                    f"{attempts * 0.2:.1f}s — topic does not exist or broker(s) "
                    f"{','.join(self._brokers)} unreachable"
                )
            )

        if from_beginning:
            # confluent_kafka's Consumer has NO seek_to_beginning (that is a
            # kafka-python API); seek to the logical OFFSET_BEGINNING so
            # librdkafka resolves each partition's ACTUAL log-start offset.
            # Seeking to an absolute offset (e.g. 0) is wrong once retention or
            # compaction has advanced the start offset past 0: the broker returns
            # "Offset out of range" and librdkafka logs
            # "fetch failed due to requested offset not available on the broker".
            for tp in assignment:
                try:
                    consumer.seek(
                        TopicPartition(tp.topic, tp.partition, OFFSET_BEGINNING)
                    )
                except KafkaException as exc:
                    raise ConnectionFailure(message=str(exc)) from exc
        else:
            target_ms = int((time.time() - lookback_seconds) * 1000)
            seek_tps = []
            for tp in assignment:
                seek_tps.append(TopicPartition(topic, tp.partition, target_ms))
            try:
                resolved = consumer.offsets_for_times(seek_tps)
            except KafkaException as exc:
                raise ConnectionFailure(message=str(exc)) from exc

            for rtp in resolved:
                if rtp.offset is not None and rtp.offset >= 0:
                    try:
                        consumer.seek(rtp)
                    except KafkaException as exc:
                        raise ConnectionFailure(message=str(exc)) from exc
                else:
                    # offsets_for_times returns -1 when the partition's newest
                    # message is older than the window. Such a partition must NOT
                    # be left at its default fetch position — with
                    # auto.offset.reset=earliest it would re-read every stale
                    # message, violating the lookback window. Seek it to the end
                    # so it contributes nothing old (new messages still arrive).
                    try:
                        consumer.seek(
                            TopicPartition(topic, rtp.partition, OFFSET_END)
                        )
                    except KafkaException as exc:
                        raise ConnectionFailure(message=str(exc)) from exc

    def _normalize_message(self, msg, *, on_decode_error=None) -> dict:
        """Build the DESIGN §4.2 message envelope from a confluent_kafka msg.

        When ``self._codec is None`` (or a side's codec config is absent / a
        legacy fmt) the decode is byte-for-byte the legacy path: json.loads
        for the value (utf-8 replace fallback), _decode_bytes for the key.
        When a codec is configured with a non-legacy fmt, the value and key
        are decoded per side via :func:`decode_payload`. Decode failures are
        NON-fatal: ``on_decode_error`` (if provided) is invoked once per
        failed side and that side becomes ``None`` (the other side still
        decodes — a key failure does not lose the value). Tombstones
        (``value=None``) always decode to ``value=None`` and are NOT counted
        as decode errors.
        """
        raw_value = msg.value()
        raw_key = msg.key()

        if self._codec is None:
            value = self._legacy_decode_value(raw_value)
            key = _decode_bytes(raw_key)
        else:
            value, key = self._codec_decode(raw_value, raw_key, on_decode_error)

        headers = {}
        raw_headers = msg.headers() if msg.headers() else None
        for hk, hv in (raw_headers or []):
            headers[hk] = _decode_bytes(hv)

        _, ts_ms = msg.timestamp()

        return {
            "key": key,
            "value": value,
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": _ms_to_iso8601z(ts_ms),
            "headers": headers,
        }

    # ------------------------------------------------------------------
    # codec helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _legacy_decode_value(raw_value):
        """Today's pre-codec value decode: json.loads with utf-8 replace fallback.

        Kept as a helper so the ``codec is None`` path is byte-for-byte
        identical to the pre-Task-8 implementation (the backward-compat
        guarantee). Non-JSON bytes fall back to a replace-decoded string.
        """
        if raw_value is None:
            return None
        try:
            return json.loads(raw_value.decode("utf-8"))
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
            return raw_value.decode("utf-8", errors="replace")

    def _codec_decode(self, raw_value, raw_key, on_decode_error):
        """Decode value+key per the configured codec (per side).

        Returns ``(value, key)``. Tombstones (``raw is None``) decode to
        ``None`` for that side — never raised, never counted as an error
        (a None value is a Kafka delete marker, not a corrupt payload).
        For each non-None side, ``decode_payload`` runs inside a try/except:
        on :class:`SerializationError` the callback (if any) is invoked once
        and that side becomes ``None`` (the OTHER side still decodes, so a
        bad key does not discard a healthy value).

        Default formats: when the codec omits ``value`` or ``key``, that
        side keeps today's behavior (JSON for value, KEY_STRING for key) —
        so a codec configured only for the value leaves the key on the
        legacy string path.
        """
        Format, decode_payload, _encode, _resolve = _import_serialization()
        sr = self._codec.get("sr")
        value_cfg = self._codec.get("value") or {}
        key_cfg = self._codec.get("key") or {}
        value_fmt = value_cfg.get("fmt", Format.JSON)
        key_fmt = key_cfg.get("fmt", Format.KEY_STRING)

        value = self._decode_one_side(
            raw_value, value_fmt, sr, on_decode_error, "value"
        )
        key = self._decode_one_side(
            raw_key, key_fmt, sr, on_decode_error, "key"
        )
        return value, key

    @staticmethod
    def _decode_one_side(raw, fmt, sr, on_decode_error, label):
        """Decode one side (value or key); tombstone- and error-aware.

        - ``raw is None`` → ``None`` (tombstone / absent), no error counted.
        - :class:`SerializationError` from ``decode_payload`` → callback
          invoked once with ``f"{label}: {exc}"``, returns ``None``.
        - Any other exception propagates (ConfigError for a misconfigured
          codec, etc.) — those are NOT per-message data failures.
        """
        if raw is None:
            return None
        # Lazy import: only paid when a codec is actually configured for decode.
        _Format, decode_payload, _encode, _resolve = _import_serialization()
        try:
            return decode_payload(raw, fmt, sr)
        except SerializationError as exc:
            if on_decode_error is not None:
                on_decode_error(f"{label}: {exc}")
            return None

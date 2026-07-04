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

from ..errors import ConfigError, ConnectionFailure

_KAFKA_EXTRA_MSG = "Kafka support requires the 'kafka' extra: pip install 'agctl[kafka]'"


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
            from confluent_kafka import OFFSET_END, TopicPartition
        except ImportError:  # pragma: no cover - always present in confluent_kafka
            OFFSET_END = -1
            TopicPartition = None
    except ImportError as exc:
        raise ConfigError(_KAFKA_EXTRA_MSG) from exc
    return Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END


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
    """

    def __init__(
        self,
        brokers,
        group_id=None,
        *,
        extra_conf=None,
        consumer_factory=None,
        producer_factory=None,
    ):
        self._brokers = brokers if isinstance(brokers, list) else [brokers]
        self._group_id = group_id
        self._extra_conf = dict(extra_conf or {})
        self._consumer_factory = consumer_factory
        self._producer_factory = producer_factory

    # ------------------------------------------------------------------
    # produce
    # ------------------------------------------------------------------

    def produce(self, topic, value, *, key=None, headers=None) -> dict:
        """Publish one message and return the DESIGN §4.2 ``kafka.produce`` shape.

        ``value`` is JSON-encoded; ``key`` and header values are encoded to
        bytes if they are strings.
        """
        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END = _import_kafka()

        producer_conf = {"bootstrap.servers": ",".join(self._brokers)}
        producer_conf.update(self._extra_conf)
        if self._producer_factory is not None:
            producer = self._producer_factory(producer_conf)
        else:
            producer = Producer(producer_conf)

        value_bytes = json.dumps(value).encode("utf-8")
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key

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

        Returns a list of normalized message dicts (DESIGN §4.2 message shape).
        """
        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END = _import_kafka()

        consumer = self._build_consumer()

        messages: list[dict] = []
        try:
            self._setup_seek(consumer, topic, lookback_seconds, from_beginning)

            deadline = time.monotonic() + timeout_seconds
            while time.monotonic() < deadline:
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    # Skip individual poll errors; a silent window yields [].
                    continue
                normalized = self._normalize_message(msg)
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
    ):
        """Poll the window incrementally; return the first message dict for
        which ``predicate(msg)`` is True, or ``None`` if the window elapses with
        no match. Uses the SAME seek/lookback mechanics as :meth:`consume_window`
        (``offsets_for_times`` seek to ``now - lookback_seconds``, or
        ``from_beginning``). ``predicate`` is called on each normalized message
        dict. Terminates promptly: stops polling the moment a match is found.

        Returns a ``(message, scanned_count)`` tuple so callers can report
        ``messages_scanned``; if no match, returns ``(None, scanned_count)``.
        """
        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END = _import_kafka()

        consumer = self._build_consumer()

        scanned = 0
        try:
            self._setup_seek(consumer, topic, lookback_seconds, from_beginning)

            deadline = time.monotonic() + timeout_seconds
            while time.monotonic() < deadline:
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    continue
                normalized = self._normalize_message(msg)
                scanned += 1
                try:
                    matched = predicate(normalized)
                except Exception:
                    matched = False
                if matched:
                    return normalized, scanned
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
    ) -> None:
        """Run a committed consume loop with retry logic.

        The loop polls messages, calls ``handle`` with each normalized message
        (plus ``attempt``/``final`` flags), and commits on success. The client
        manages the retry budget; ``handle`` is called with ``attempt`` (1-indexed)
        and ``final`` (True when attempt >= max_retries) and returns a
        :class:`ReactionResult`:

        - ``COMMIT``: message processed → ``store_offset(msg)`` + ``commit()``.
        - ``RETRY``: transient failure → re-handle the same in-memory message
          (only when ``final`` is False). ``RETRY`` on ``final`` is treated as
          ``COMMIT`` (defensive poison-message guard). No seek/re-poll: see the
          retry-loop rationale in the loop body.
        - ``STOP``: exit loop immediately (reactor is done/dying).

        The consumer is built with the given ``group_id`` (each reactor has its
        own consumer group), used only on this thread, and ``close()``d in
        ``finally`` (D13).

        Args:
            topic: Kafka topic to consume.
            group_id: Consumer group id for this reactor (unique per reactor).
            stop_event: ``threading.Event``; the loop exits when set.
            handle: Callable ``handle(msg, *, attempt, final) -> ReactionResult``.
            poll_timeout: Timeout for each ``poll()`` call (default 0.5s).
            max_retries: Maximum retry attempts per message (default 3).
            on_assign: Optional rebalance callback for partition assignment.
            on_revoke: Optional rebalance callback for partition revocation.
        """
        if max_retries < 1:
            raise ValueError("max_retries must be >= 1")

        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END = _import_kafka()

        consumer = self._build_consumer(group_id=group_id)

        try:
            consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)

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
                # commit offset advances only on store_offset+commit, so
                # crash-recovery positioning is unaffected.
                normalized = self._normalize_message(msg)
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
                            consumer.store_offset(msg)
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
        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END = _import_kafka()

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
        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END = _import_kafka()

        effective_group_id = group_id if group_id is not None else (self._group_id or "agctl-consumer")
        conf = {
            "bootstrap.servers": ",".join(self._brokers),
            "group.id": effective_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        conf.update(self._extra_conf)

        if self._consumer_factory is not None:
            return self._consumer_factory(conf)
        return Consumer(conf)

    def _setup_seek(self, consumer, topic, lookback_seconds, from_beginning):
        """Subscribe, wait for assignment, then seek partitions to the lookback
        window (or offset 0 when ``from_beginning``). Shared by
        :meth:`consume_window` and :meth:`find_in_window`.
        """
        Consumer, Producer, TopicPartition, KafkaError, KafkaException, OFFSET_END = _import_kafka()

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

        if from_beginning:
            for tp in assignment:
                if hasattr(consumer, "seek_to_beginning"):
                    try:
                        consumer.seek_to_beginning(tp)
                    except KafkaException as exc:
                        raise ConnectionFailure(message=str(exc)) from exc
                else:  # pragma: no cover - real consumer always has it
                    consumer.seek(TopicPartition(tp.topic, tp.partition, 0))
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

    @staticmethod
    def _normalize_message(msg) -> dict:
        raw_value = msg.value()
        value = None
        if raw_value is not None:
            try:
                value = json.loads(raw_value.decode("utf-8"))
            except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
                value = raw_value.decode("utf-8", errors="replace")

        key = _decode_bytes(msg.key())

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

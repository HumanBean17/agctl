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

import json
import time
from datetime import datetime, timezone

from ..errors import ConfigError, ConnectionFailure

_KAFKA_EXTRA_MSG = "Kafka support requires the 'kafka' extra: pip install 'agctl[kafka]'"


def _import_kafka():
    """Lazy-import confluent_kafka primitives or raise ConfigError."""
    try:
        from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

        try:
            from confluent_kafka import TopicPartition
        except ImportError:  # pragma: no cover - TopicPartition always present
            TopicPartition = None
    except ImportError as exc:
        raise ConfigError(_KAFKA_EXTRA_MSG) from exc
    return Consumer, Producer, TopicPartition, KafkaError, KafkaException


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
    consumer_factory / producer_factory:
        Test-injection seams. Each is a callable ``conf -> (Consumer|Producer)``
        used in place of the real confluent_kafka classes.
    """

    def __init__(
        self,
        brokers,
        group_id=None,
        *,
        consumer_factory=None,
        producer_factory=None,
    ):
        self._brokers = brokers if isinstance(brokers, list) else [brokers]
        self._group_id = group_id
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
        Consumer, Producer, TopicPartition, KafkaError, KafkaException = _import_kafka()

        if self._producer_factory is not None:
            producer = self._producer_factory({"bootstrap.servers": ",".join(self._brokers)})
        else:
            producer = Producer({"bootstrap.servers": ",".join(self._brokers)})

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
            producer.flush(timeout=30)
        except KafkaException as exc:
            raise ConnectionFailure(message=str(exc)) from exc
        except Exception as exc:  # broker connection issues, etc.
            raise ConnectionFailure(message=str(exc)) from exc

        err, msg = holder[0], holder[1]
        if err is not None:
            raise ConnectionFailure(message=str(err))

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
    ) -> list[dict]:
        """Seek each partition to the lookback window and read forward.

        With ``from_beginning=True`` partitions are seeked to offset 0.
        Otherwise each partition is seeked to the earliest offset at/after
        ``now - lookback_seconds`` (via ``offsets_for_times``). The poll loop
        runs until ``timeout_seconds`` of wall-clock time elapses.

        Returns a list of normalized message dicts (DESIGN §4.2 message shape).
        """
        Consumer, Producer, TopicPartition, KafkaError, KafkaException = _import_kafka()

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
                messages.append(self._normalize_message(msg))
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
        Consumer, Producer, TopicPartition, KafkaError, KafkaException = _import_kafka()

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
    # shared helpers
    # ------------------------------------------------------------------

    def _build_consumer(self):
        """Build a consumer (real or via the test factory) with the standard conf."""
        Consumer, Producer, TopicPartition, KafkaError, KafkaException = _import_kafka()

        group_id = self._group_id or "agctl-consumer"
        conf = {
            "bootstrap.servers": ",".join(self._brokers),
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }

        if self._consumer_factory is not None:
            return self._consumer_factory(conf)
        return Consumer(conf)

    def _setup_seek(self, consumer, topic, lookback_seconds, from_beginning):
        """Subscribe, wait for assignment, then seek partitions to the lookback
        window (or offset 0 when ``from_beginning``). Shared by
        :meth:`consume_window` and :meth:`find_in_window`.
        """
        Consumer, Producer, TopicPartition, KafkaError, KafkaException = _import_kafka()

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

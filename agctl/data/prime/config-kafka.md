# agctl — config-kafka: extract a Kafka pattern

Produce a `kafka.patterns.<name>` block from a producer / emitter / event
schema. A pattern answers "what does the event I care about look like?" —
narrow enough to not match stale events from prior runs on busy topics.

## Extraction

1. **topic** — the literal topic string (computed in code → ask for the
   concrete value).
2. **match** — a jq boolean predicate over the message **envelope**
   (`{key, value, partition, offset, timestamp, headers}`). Payload fields
   live under `.value.` (`.value.eventType`); reach the key/headers with
   `.key` / `.headers.<name>` (**case-sensitive** — producer's exact name).
   `{placeholder}` for the value that varies per assert.
3. **cluster** *(optional)* — key under `kafka.clusters`; omit →
   `kafka.default_cluster` → single-cluster auto-default. Set only for a
   non-default cluster (dangling name = validate error). CLI
   `kafka assert --cluster` overrides.
4. **description** — one line. **name** — kebab-case from the event
   (`ORDER_CREATED` → `order-created`).

## Writing the jq `match`

- Envelope-rooted, not value-rooted: `.value.eventType ==
  "ORDER_CREATED" and .value.payload.orderId == "{orderId}"`; nested
  `.value.payload.customer.id`.
- `{placeholder}` is the only substitution here — never `${}` or `:`.
- Prefer a sharp `match` predicate over `--contains` (subset) for
  large/variable payloads.

## Stack snippets

- **Spring**: `kafkaTemplate.send("orders.created", event)` /
  `@KafkaListener(topics = …)` → topic; event-class fields → `.value.` paths.
- **Python**: `confluent_kafka`/`aiokafka` `Producer.produce(topic, value=…)`,
  faust agents; Pydantic/dataclass event → `.value.` paths.
- **Node**: `kafkajs` `producer.send({topic, messages})` → topic + value.

## Value format (JSON vs Avro/Protobuf)

agctl defaults to raw-JSON decode. Avro/Protobuf decode only when the topic
opts in via `kafka.topics.<t>.value_format` (or cluster-level default) AND
the cluster has a `schema_registry_url` — a binary topic without that config
won't match a jq predicate. Assume JSON; if the producer serializes
Avro/Protobuf, say so and add the `kafka.topics.<t>` block (or cluster
default).

## Migrating v1/v2

A `version: "1"`/`"2"` config is rejected (exit 2) → `agctl config migrate`
lifts to v3 (named `kafka.clusters` for both; `.value | ` prefix on pattern
and reactor `match` for **v1 only**; `--dry-run`, backups, `already_current`
supported). CLI `--match` flags in scripts/prompts are NOT rewritten —
prefix by hand, v1 inputs only.

## Gotchas

- The pattern's `topic` is what `kafka assert --pattern` uses (omit
  `--topic` then).
- `kafka assert` reads a window (lookback default = `--timeout`) — narrow
  with `match`.
- Touching `kafka.clusters.<n>.ssl`: `security_protocol` ∈ {PLAINTEXT, SSL,
  SASL_SSL, SASL_PLAINTEXT}.

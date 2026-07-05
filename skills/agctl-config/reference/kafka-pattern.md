# `kafka` mode — extract a Kafka pattern

Point at a producer / emitter / event schema; produce a `kafka.patterns.<name>` block. The
non-negotiable contract (placeholder syntaxes, naming, verify-after) lives in `SKILL.md` —
this file is the extraction detail.

## Inputs

- The config path.
- The artifact: a `producer.send(topic, value)` call, an `@KafkaListener`, an event/DTO class,
  or a description of the event.

## Extraction

1. **topic** — the literal topic string. If the code computes it, ask for the concrete value.
2. **match** — a jq boolean predicate over the message **envelope** (`{key, value, partition,
   offset, timestamp, headers}`) that identifies *this* event. Prefix payload fields with
   `.value.` (e.g. `.value.eventType`, `.value.payload.orderId == "{orderId}"`). Reach the
   message key / a header with `.key` / `.headers.<name>` (header keys are **case-sensitive** —
   use the producer's exact name). Use `{placeholder}` for the value that varies per assert.
3. **description** — one line.
4. **name** — kebab-case from the event (`ORDER_CREATED` → `order-created`;
   `PAYMENT_FAILED` → `payment-failed`).

A pattern answers "what does the event I care about look like?" — narrow enough to not match
stale events from prior runs on busy topics.

## Stack snippets

### Spring (JVM)

- `kafkaTemplate.send("orders.created", event)` → topic + (read the event class for `match`).
- `@KafkaListener(topics = "orders.created")` → topic.
- Event class fields → jq paths under `.value.` (`.value.eventType`, `.value.payload.orderId`).

### Python

- `confluent_kafka` / `aiokafka` `Producer.produce(topic, value=…)`, `faust` agents → topic +
  value shape.
- A Pydantic / dataclass event → jq paths under `.value.`.

### Node

- `kafkajs` `producer.send({ topic, messages: [{ value }] })` → topic + value.

## Writing the jq `match`

- The predicate runs over the message **envelope**, not the bare value — so payload fields
  live under `.value.`: `.value.eventType == "ORDER_CREATED"`.
- Combine with `and`: `.value.eventType == "ORDER_CREATED" and .value.payload.orderId == "{orderId}"`.
- Drill nested fields: `.value.payload.customer.id`.
- Reach the message key (`.key`) or a header (`.headers.<name>` — exact producer casing, do
  **not** lowercase). These reach transport-level metadata the value alone can't see.
- The **only** substitution here is `{placeholder}` (call-time, via `--param`). Never `${}` or `:`.
- Prefer a sharp `--match` predicate over `--contains` (subset) for large/variable payloads —
  patterns live in config precisely so you can write one.

## Migrating from dialect `"1"`

If the repo's `agctl.yaml` is still at `version: "1"`, `agctl` rejects it (exit 2) with a
pointer to `agctl config migrate`. Run that to rewrite every `kafka.patterns.<name>.match`
(and reactor `match`) by prepending `.value | ` and bumping `version` to `"2"` — backups and
`--dry-run` are supported; CLI `--match` flags in scripts/prompts are **not** rewritten
(prefix those by hand).

## What to clarify

- The concrete topic string if the code computes it.
- Which field(s) identify the event for assertions (the `{placeholder}` carriers).
- Whether the value is always JSON — agctl treats values as raw JSON today (no Avro/Protobuf
  decode), so a binary/Avro topic won't match a jq predicate. Default: assume JSON and note the
  assumption for the user to confirm on the first `kafka assert`.

## Where it writes

Under `kafka.patterns:` (nested under `kafka:`).

## Gotchas

- `{placeholder}` only — not `${}` (env) or `:` (SQL).
- The pattern's `topic` is what `kafka assert --pattern` uses (then omit `--topic`).
- `kafka assert` reads a **window** (default lookback = `--timeout`); narrow with `match` so you
  don't match a stale event from a previous run.
- If you also touch `kafka.ssl`, `security_protocol` (if set) must be one of
  PLAINTEXT / SSL / SASL_SSL / SASL_PLAINTEXT.

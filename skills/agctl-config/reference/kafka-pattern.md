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
2. **match** — a jq boolean predicate over the message **value** that identifies *this* event.
   Use `{placeholder}` for the value that varies per assert
   (e.g. `.payload.orderId == "{orderId}"`).
3. **description** — one line.
4. **name** — kebab-case from the event (`ORDER_CREATED` → `order-created`;
   `PAYMENT_FAILED` → `payment-failed`).

A pattern answers "what does the event I care about look like?" — narrow enough to not match
stale events from prior runs on busy topics.

## Stack snippets

### Spring (JVM)

- `kafkaTemplate.send("orders.created", event)` → topic + (read the event class for `match`).
- `@KafkaListener(topics = "orders.created")` → topic.
- Event class fields → jq paths (`.eventType`, `.payload.orderId`).

### Python

- `confluent_kafka` / `aiokafka` `Producer.produce(topic, value=…)`, `faust` agents → topic +
  value shape.
- A Pydantic / dataclass event → jq paths.

### Node

- `kafkajs` `producer.send({ topic, messages: [{ value }] })` → topic + value.

## Writing the jq `match`

- The predicate runs over `.value` (agctl parses the message value as JSON):
  `.eventType == "ORDER_CREATED"`.
- Combine with `and`: `.eventType == "ORDER_CREATED" and .payload.orderId == "{orderId}"`.
- Drill nested fields: `.payload.customer.id`.
- The **only** substitution here is `{placeholder}` (call-time, via `--param`). Never `${}` or `:`.
- Prefer a sharp `--match` predicate over `--contains` (subset) for large/variable payloads —
  patterns live in config precisely so you can write one.

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

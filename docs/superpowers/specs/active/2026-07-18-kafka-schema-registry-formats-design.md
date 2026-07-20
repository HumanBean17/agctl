# Design: `agctl kafka` — Schema Registry / Avro / Protobuf serialization

**Status:** Approved (brainstormed 2026-07-18)
**Spec date:** 2026-07-18
**Branch:** `feat/kafka-schema-registry-formats`
**Precedent:**
- [`2026-07-09-agctl-kafka-multi-cluster-design.md`](./2026-07-09-agctl-kafka-multi-cluster-design.md) — the named-cluster model and `resolve_cluster_name` this builds on.
- [`2026-07-09-grpc-support-design.md`](./2026-07-09-grpc-support-design.md) — the shared `clients/grpc_descriptors.py` kernel (descriptor pool + `DynamicMessage`) reused for Protobuf decode/encode.

---

## 1. Background & Problem

Every Kafka surface in `agctl` (`kafka produce` / `consume` / `assert`, the
`kafka listen` capture daemon, and `mocks.kafka` reactors) treats message
**values as raw JSON**. ARCHITECTURE §15 records this verbatim:

> **No Schema Registry / Avro/Protobuf decoding** — Kafka values are raw JSON;
> `schema_registry_url` is parsed but unused.

The `KafkaCluster` model already carries a `schema_registry_url` field
(`config/models.py`), but nothing reads it. This produces two distinct
false-negative surfaces on any real Kafka stack that serializes with Avro or
Protobuf via Confluent Schema Registry (the dominant production pattern):

1. **Read path.** `kafka consume` / `kafka assert` / `kafka listen` receive
   binary Avro/Protobuf bytes, cannot decode them, and either fail to match jq
   predicates (`.value.eventType` evaluates against opaque bytes) or surface
   garbage. A runbook that verifies an Avro-backed event cannot work at all.
2. **Write path.** `kafka produce` JSON-encodes and publishes as-is, so it
   cannot publish a valid Avro/Protobuf message to a serialized topic. Mock
   reactions likewise emit raw JSON, which a real consumer of an Avro topic
   rejects. `mocks.kafka` reactors already emit `kafka.skipped` for
   "non-object message value" — i.e. any Avro/Protobuf trigger — making
   Avro/Protobuf topics effectively un-mockable.

These are exactly the silent false-green / false-negative failures `agctl`
exists to eliminate (DESIGN §1 "Fail loudly"). This spec closes the gap by
adding opt-in **decode and encode of Avro and Protobuf message keys and values
via Confluent Schema Registry**, wired into all four Kafka surfaces.

## 2. Goals

- **Decode** Avro/Protobuf message values (and keys) to JSON on the read path
  (`consume`, `assert`, `listen`), so existing jq predicates (`--match`),
  `--contains`, and `capture.from: .value.x` work **unchanged** against the
  logical message.
- **Encode** JSON payloads to Avro/Protobuf on the write path (`produce`, mock
  reactions), producing Confluent-wire-framed bytes the SUT accepts.
- **Both formats, both directions, both key and value**, across all four
  surfaces, in one coherent sub-project.
- **Schema Registry access in all three modes**: plaintext, HTTPS + basic auth
  (Confluent Cloud: API key/secret), and HTTPS + mTLS.
- **Strict opt-in, zero behavior change by default.** A topic/cluster with no
  format declared behaves byte-for-byte as today. No major config-version bump,
  no `config migrate`.
- **Fail loud.** An unreachable/auth-failing registry, a missing optional extra,
  a declared format with no registry URL, or an encode/decode schema mismatch is
  a typed error with a deterministic exit code — never silent.
- **Reuse, don't fork.** Protobuf decode/encode reuses the existing
  `clients/grpc_descriptors.py` descriptor kernel; config follows the named-map +
  default + precedence pattern already used for `database.connections` and
  `kafka.clusters`.

## 3. Scope & Design Constraints

- **Schema source = Confluent Schema Registry only.** The Confluent wire format
  is the contract: a 1-byte magic (`0x00`) + 4-byte big-endian schema id +
  serialized payload. Decode reads the embedded schema id and fetches the schema
  by id (subject-strategy-independent); encode registers/looks up the schema
  under a resolved subject and embeds the returned id.
- **Serialization is a topic-level contract.** A topic's values (and keys) are
  uniformly one format. Format metadata lives in a dedicated `kafka.topics` map,
  not on patterns/reactors (which `produce` and `listen` do not share).
- **Format detection is declared, not heuristic.** Because the format is
  configured, decoding only attempts Confluent-wire parse when the resolved
  format is `avro`/`protobuf`. Magic-byte false positives are a non-issue (JSON
  payloads never start with `0x00`).
- **All heavy imports stay lazy.** The new `agctl/serialization/` kernel is pure
  Python at module top (mirrors `clients/grpc_descriptors.py`); `fastavro` and
  `protobuf` import inside the functions that need them. A missing library
  surfaces as `ConfigError` pointing at the right extra.
- **Both key and value** can be encoded, with independent per-topic formats.
- **No daemon/IPC changes.** The `kafka listen` daemon and `mock run` daemon
  gain decode/encode in-process; their daemon model, signal handling, and log
  vocabularies are unchanged except for additive event/result fields.

## 4. Non-Goals

- **JSON-Schema SR format** (Confluent's third subject type) — Avro and Protobuf
  cover the dominant cases; JSON-Schema is a follow-on.
- **Non-Confluent wire formats** and **non-Confluent registries** (AWS Glue,
  Azure, Apicurio-proprietary APIs) — Confluent SR + wire format only in v1.
- **Record-level schema-evolution tooling** — no compatibility-checking UI, no
  version pinning beyond the wire-embedded id. Encode registers/looks up the
  latest schema under the resolved subject.
- **Message-ordering assertions** (`--ordered`) — orthogonal; tracked separately
  in DESIGN §10.
- **Compiled `*_pb2.py` codegen** — Protobuf uses dynamic messages from the
  descriptor pool (same as the gRPC client/server), no startup codegen.
- **Pluggable third-party codecs via a new entry point** — in-tree codecs only
  in v1 (same posture as in-tree gRPC). A future `agctl.serializers` entry point
  is not precluded.

## 5. Decisions (recorded with rationale)

1. **Opt-in, byte-for-byte default.** A topic/cluster with no `value_format` /
   `key_format` declared must behave exactly as today (raw JSON values, string
   keys). *Rationale:* `agctl`'s entire posture is "fail loudly, never quietly
   reinterpret." A silent format change on existing configs would violate that
   and break every current user on upgrade. *Rejected:* auto-detecting Avro from
   the magic byte for unconfigured topics — reintroduces heuristic behavior and
   makes the default nondeterministic across topics.

2. **`kafka.topics.<name>` map as the single source of truth for serialization.**
   Format metadata is a topic-level contract, so it lives in a dedicated
   `kafka.topics` map consulted by every surface (`produce`, `consume`/`assert`,
   `listen`, mock trigger + reaction), with cluster-level defaults. *Rejected:*
   (a) overloading `kafka.patterns` — `produce` and `listen` have no pattern, and
   a topic may have several patterns; (b) cluster-only — too coarse for
   mixed-format clusters (common during JSON→Avro migration or Avro/Protobuf
   coexistence); (c) per-call flags only — not declarative, not discoverable, not
   reusable across surfaces.

3. **New shared kernel `agctl/serialization/`, pure-Python at module top.**
   Decode/encode/wire-frame/registry live in one subpackage, heavy imports inside
   functions. *Rationale:* matches the `clients/grpc_descriptors.py` precedent
   (shared kernel, import-light, unit-testable without extras, AST-pinned "no
   heavy import at top"). *Rejected:* implementing codecs inline in
   `kafka_client.py` — couples transport to serialization, blocks reuse by mock
   reactors and listen, and bloats the already-large client module.

4. **Reuse the gRPC descriptor kernel for Protobuf.** Protobuf decode/encode
   compiles the SR-returned `.proto` schema via `grpc_descriptors.build_descriptor_pool`
   and (de)serializes via `DynamicMessage` + `json_format` — the same machinery the
   gRPC client and gRPC mock server use. *Rationale:* one descriptor path, already
   battle-tested, already handles order-tolerant multi-file loading. *Rejected:*
   requiring compiled `*_pb2.py` modules — needs codegen at config time, defeats
   the dynamic model, and is incompatible with schemas fetched from a registry.

5. **Decode drops decoded JSON into the existing message envelope.** The decoded
   JSON-native value replaces raw bytes at `.value` (and `.key` when the key
   format is set), so jq predicates, `--contains`, and `capture.from` work without
   any operator change. *Rationale:* zero migration of existing runbooks/patterns;
   the envelope contract (`{key, value, partition, offset, timestamp, headers}`)
   is preserved. *Rejected:* a separate `.decoded_value` field — fragments the
   operator surface and forces every predicate/capture to choose a root.

6. **Per-message decode failure is non-fatal and surfaced; registry-down is fatal.**
   A single corrupt/unknown-schema message is skipped with a reason (counted in
   `decode_errors` on `consume`/`assert`, a non-fatal `decode.error` event in
   `listen`, a `kafka.skipped` event for mock reactors). An unreachable or
   auth-failing registry at startup is `ConnectionFailure` (exit 2) via a startup
   probe. *Rationale:* distinguishes "one bad message" (recoverable, keep scanning)
   from "cannot read this topic at all" (fail loud) — the same split the broker
   probe already enforces. *Rejected:* fatal-on-first-bad-message — one corrupt
   byte would blank an entire high-volume scan, a new false-negative source.

7. **New `SerializationError` type (exit 2).** Encode-time schema-conformance
   failure (payload does not match the registered schema) and structural
   decode failure that is not a single-message skip raise `SerializationError`,
   carrying `subject` / `schema_id` / `topic` detail. *Rationale:* a distinct type
   lets an agent self-correct the payload in one shot instead of guessing at a
   generic `ConfigError`. *Rejected:* mapping everything onto `ConfigError` —
   erases the "your data is wrong" vs "your config is wrong" distinction.

8. **Startup Schema-Registry reachability probe.** Any surface whose resolved
   formats require SR runs one lightweight SR connectivity check at startup
   (alongside the existing broker probe). *Rationale:* satisfies the fail-loudly
   guarantee — a typo'd URL or expired Confluent Cloud key fails at start, not
   mid-scan. *Rejected:* lazy first-use failure — surfaces a config/env problem as
   a confusing mid-run error and can mask it behind a partial result.

9. **New `avro` and `protobuf` extras; standalone `protobuf` (no grpcio).**
   `avro` adds `fastavro`; `protobuf` adds `protobuf` only. A convenience
   `schema-registry` meta-extra bundles both. *Rationale:* matches the granular
   extra + lazy-import + `ConfigError`-pointing-at-the-extra pattern (`jq`,
   `http`, `grpc`). A Kafka-Protobuf user must not be forced into the gRPC extra.
   *Rejected:* bundling codecs into the `kafka` extra — taxes every Kafka user for
   a feature many do not need.

## 6. Configuration Schema

All additions are **additive** under the existing v3 `kafka:` block. No
`config migrate`; no major-version bump (the version guard is major-only, and
minor/patch are not tracked).

### 6.1 Cluster-level registry + format defaults

`KafkaCluster` (`config/models.py`) gains a `schema_registry` sub-block and
cluster-level format defaults. The existing bare `schema_registry_url` field is
retained (still parsed, now used) and works alone for plaintext.

```yaml
kafka:
  default_cluster: default
  clusters:
    default:
      brokers: ["${KAFKA_BROKER}:9092"]
      schema_registry_url: "${SR_URL}"          # EXISTING — now used when a format is declared
      # NEW — registry auth/TLS (all three modes supported):
      schema_registry:
        auth: basic                             # plaintext | basic | mtls (auto-inferred if omitted:
                                               #   basic_auth present → basic; ssl present → mtls; else plaintext)
        basic_auth:
          username: "${SR_USER}"                # Confluent Cloud: API key
          password: "${SR_PASS}"                # Confluent Cloud: API secret
        ssl:                                    # auth=mtls — mirrors the Kafka broker ssl: block (§2.1)
          ca_location: "${SR_CA:-}"
          certificate_location: "${SR_CERT:-}"
          key_location: "${SR_KEY:-}"
          key_password: "${SR_KEY_PASSWORD:-}"
          endpoint_identification_algorithm: none   # uncomment to disable hostname verification
      value_format: json                        # cluster default: json | avro | protobuf (default json = today)
      key_format: string                        # cluster default: string | avro | protobuf (default string = today)
```

The URL stays at the existing bare `schema_registry_url` field (the
`schema_registry:` block holds auth/TLS only — there is no nested `url`, so no
aliasing/conflict rule is needed).

### 6.2 Topic-level serialization contracts

New `kafka.topics` map — `TopicConfig` in `config/models.py`:

```yaml
  topics:
    orders.created:
      cluster: default                          # optional; resolves which cluster's SR to use
                                               #   (default: kafka.default_cluster / single-cluster auto-default)
      value_format: avro                        # overrides the cluster default
      key_format: string
    payments.commands:
      value_format: protobuf
      key_format: avro
      subject_strategy: record                  # topic (default) | record | topic_record — ENCODE subject resolution
```

`subject_strategy` governs only the **encode** subject (decode reads the embedded
schema id and is strategy-independent): `topic` → `<topic>-value` / `<topic>-key`
(Confluent `TopicNameStrategy`, the default); `record` → the schema's record name
(`RecordNameStrategy`); `topic_record` → `<topic>-<record-name>`
(`TopicRecordNameStrategy`).

### 6.3 Format resolution precedence

For a given `(topic, key|value)`, resolved by a single command-layer function
`resolve_topic_format(cfg, topic, cluster, which)` (mirrors `resolve_cluster_name`
/ `resolve_connection_name`):

1. CLI flag (`--value-format` / `--key-format`) — ad-hoc, highest precedence.
2. `kafka.topics.<topic>.value_format` / `.key_format`.
3. `kafka.clusters.<resolved cluster>.value_format` / `.key_format`.
4. `json` (value) / `string` (key) — today's behavior.

A companion `resolve_schema_registry_client(cfg, cluster)` builds (and memoizes
for the invocation) the `SchemaRegistryClient` from the resolved cluster's
`schema_registry` block. Both resolvers live in the command layer (e.g.
`commands/kafka_commands.py`), keeping `config/*` free of a `serialization`
import.

### 6.4 Validation additions (`config/validator.py`)

New cross-reference checks, surfaced by `agctl config validate`:

- `kafka.topics.<t>.cluster` → unknown cluster = **error**.
- A topic whose resolved `value_format`/`key_format` is `avro`/`protobuf` but
  whose resolved cluster has no `schema_registry_url` = **error** (fail-loud at
  validate time, not at first message).
- `subject_strategy` present on a topic whose value format is `json` = **warning**
  (no encode effect).
- `schema_registry.auth` value outside the allowed set, or `basic` without
  `basic_auth`, or `mtls` without `ssl` = **error**.

## 7. Command Surface & Per-Surface Wiring

All four surfaces share the §6.3 resolver. The `KafkaClient` (`clients/kafka_client.py`)
gains decode hooks on its consume methods (`consume_window`, `find_in_window`,
`consume_loop`) and an encode step in `produce`, receiving the resolved formats
and an `SchemaRegistryClient` via dependency-injection seams (mirrors the existing
`consumer_factory` / `producer_factory` test seams).

### 7.1 Read path — `kafka consume` / `kafka assert`

New optional flags: `--value-format <json|avro|protobuf>`, `--key-format
<string|avro|protobuf>` (override §6.3). On delivery, if the resolved value/key
format is `avro`/`protobuf`, the raw bytes pass through `decode_payload`; the
decoded JSON replaces bytes in the envelope. All operators (`--match`, `--contains`,
`--path`, `capture`) work unchanged. **Decode failures:** a single bad message is
non-matching and counted (see §8); SR-down at startup is `ConnectionFailure`
(§9). The `--lookback` / `--from-beginning` seek model is unchanged.

### 7.2 Capture path — `kafka listen`

`listen start` resolves each topic's format and constructs the SR client once
for the daemon. The `CaptureLoop` (`listen/capture.py`) decodes each message per
its topic format **before** appending to the per-topic `<topic>.ndjson` capture
file, so `kafka listen results` / `messages` assertions read decoded JSON with no
wall-clock deadline (unchanged). The seek-to-`OFFSET_END`-at-start and overflow
valve are unchanged. Decode failures emit a non-fatal `decode.error` event in
`events.log` and skip the message.

### 7.3 Write path — `kafka produce`

New optional flags: `--value-format`, `--key-format`. `--message` remains the
**logical JSON payload**. If the resolved value format is `avro`/`protobuf`,
`encode_payload` resolves the subject (per `subject_strategy`), registers/looks
up the schema under that subject, embeds the returned schema id in the Confluent
preamble, and publishes the framed bytes. The key follows the same path under
the `<topic>-key` subject. The `kafka.produce` result envelope is unchanged.
Encode-time schema-conformance failure → `SerializationError` (§9).

### 7.4 Mock reactors — `mocks.kafka`

`mock/kafka_reactor.py` decodes each trigger message per the **trigger topic's**
format (so `match` jq predicates and `capture.from` see decoded JSON) and encodes
each reaction's `value`/`key` per the **reaction topic's** format before produce.
Formats resolve independently per direction, so a reactor can decode a JSON
trigger and emit an Avro reaction (or any combination). A trigger that decodes
to a non-object still emits `kafka.skipped`; a trigger that **fails to decode**
emits `kafka.skipped` with `reason: "decode failed: …"` (non-fatal, consistent
with current skip semantics). The reactor client is constructed with the SR
client(s) for the cluster(s) it binds.

## 8. Output Shape Changes

Additive only; no existing field is removed or renamed.

- **`kafka.consume`** result gains `decode_errors: int` (count of messages that
  failed to decode and were excluded from `messages[]`), and optionally
  `last_decode_error: str | null`.
- **`kafka.assert`** — on failure, `error.detail` gains `decode_errors: int`
  (separate from `messages_scanned`); decoded-value snapshots in the self-debug
  payload reflect decoded JSON.
- **`kafka listen` `events.log`** — new event type `decode.error`
  (`{event, topic, error, fatal:false, timestamp}`); non-fatal. The `summary`
  event gains `decode_errors` per topic.
- **`mock run` NDJSON** — no new event type; decode failures reuse `kafka.skipped`
  with a `decode failed` reason; encode failures are fatal `kafka.error` events
  (a reaction that cannot be encoded is a reactor failure).
- **`discover --category kafka-patterns --name <x>`** item detail gains
  `value_format` and `key_format` (resolved for the pattern's topic) so an agent
  knows the value shape before writing a predicate. No new category.

## 9. Error & Exit-Code Model

| Situation | Type | Exit |
|---|---|---|
| SR unreachable / auth-fail / probe fail at startup | `ConnectionFailure` | 2 |
| Bad format string; topic → unknown cluster; declared format with no SR URL; missing `avro`/`protobuf` extra; bad `schema_registry.auth` | `ConfigError` | 2 |
| Encode payload does not conform to the registered schema; structural decode failure that is not a single-message skip | **`SerializationError`** (new; `errors.py`, `type_name "SerializationError"`) | 2 |
| Single corrupt/unknown-schema message mid-stream | *not an exception* — surfaced per §8 | n/a |

`SerializationError.to_dict()` carries `subject`, `schema_id`, `topic`, and the
offending field/message where available. The existing `@envelope` failure paths
cover `SerializationError` unchanged (it is an `AgctlError` subclass).

## 10. Packaging / Extras

`pyproject.toml` gains:

- **`avro`** extra → `fastavro`.
- **`protobuf`** extra → `protobuf` (standalone; **no** `grpcio`).
- **`schema-registry`** meta-extra → `agctl[avro,protobuf]` (one install, both).
- `integration` extra gains the Confluent Schema Registry testcontainer.
- **No new entry-point group.** Codecs are in-tree (same as gRPC).

The `serialization` kernel lazy-imports `fastavro` / `protobuf` inside the
functions that need them; a missing library for a declared format → `ConfigError`
(exit 2) pointing at `pip install 'agctl[avro]'` / `'agctl[protobuf]'` (identical
to the `jq` / `grpc` extra pattern). The Confluent `SchemaRegistryClient` ships
with the existing `kafka` extra, so the registry client adds no new dependency.

## 11. Testing

- **Unit (no extras required):** Confluent wire-frame parse/build (pure
  functions); `Format` resolution precedence (flag > topic > cluster > default);
  `SchemaRegistryConfig` / `TopicConfig` model validation; validator cross-refs
  (topic→cluster, SR-URL-required-for-format, auth shape); `SchemaRegistryClient`
  construction from typed config with an injected fake. An AST test pins
  "no `import fastavro` / `import protobuf` / `import google.protobuf` at module
  top" of the `serialization` kernel (mirrors the gRPC dispatch-core AST test).
- **Codec round-trips** (gated on extras present; `pytest.skip` if absent): avro
  and protobuf encode→decode equality, including key paths.
- **Per-surface wiring** via existing fakes: `KafkaClient` decode through
  `consumer_factory`; `produce` encode through `producer_factory`; mock reactor
  decode-trigger + encode-reaction through `FakeKafkaClient`; `listen` `CaptureLoop`
  decode; the SR probe via a fake registry client.
- **Integration (self-skipping):** new `require_schema_registry` fixture in
  `tests/integration/conftest.py`; under `AGCTL_TEST_LIVE=1` spin Confluent
  `cp-schema-registry` beside the existing Kafka+KRaft container. Avro + Protobuf
  round-trips across `produce` → `consume`/`assert` → `listen` → mock-reactor,
  plus the plaintext/basic-auth/mTLS auth matrix. Self-skips when SR/Kafka or the
  relevant extra is absent.

## 12. Backward Compatibility

- No format declared → **byte-for-byte today** (raw JSON values, string keys).
- Bare `schema_registry_url` still parsed (now used when a format is declared;
  works alone for plaintext).
- Existing jq / `--contains` / `--match` / `--path` / `capture.from` expressions
  **unchanged** (they operate on decoded JSON that lands in the same envelope
  slot).
- No major config-version bump; `kafka.topics` and `schema_registry` are
  additive.
- No `config migrate` required.

## 13. Known Limitations (documented honestly)

- **Protobuf is best-effort beyond single-file, self-contained schemas.** Avro is
  the primary, fully-supported path. Protobuf SR is materially harder (imported
  transitive `.proto` references, descriptor indexes, oneofs, maps, editions). v1
  fully supports single-file self-contained Protobuf schemas (the common
  topic-value case). Multi-file Protobuf with imports is best-effort via the
  kernel's order-tolerant descriptor loader; **if it cannot resolve, it fails
  loudly** (`SerializationError` / `ConfigError`), never silently. This is
  recorded in DESIGN §10 and ARCHITECTURE §15 (same honesty posture as the mock
  limitation tables).
- **Schema-cache lifetime = invocation or daemon lifetime.** `agctl` is stateless
  per invocation, so each one-shot command re-connects to SR (a few HTTP
  round-trips for schema get/register). Long-lived daemons (`listen`, `mock`)
  cache for the daemon's lifetime. Acceptable; no cross-invocation disk cache is
  planned.
- **No JSON-Schema SR format, no non-Confluent registries, no record-level
  schema-evolution tooling** (see §4 Non-Goals).

## 14. Implementation Phasing (informational; not binding)

Each phase is independently shippable and useful; phasing is an
implementation-plan concern, not part of this design contract:

1. Kernel + config + **Avro decode** on `consume` / `assert` (+ tests, no extras needed).
2. **Avro encode** on `produce`.
3. Avro in **`listen`** + **mock reactors** (both directions).
4. **Protobuf** across all surfaces (reusing the gRPC descriptor kernel).
5. SR **mTLS** polish + **key encoding** hardening + auth-matrix integration tests.

## 15. Open Questions / Follow-ons

- **JSON-Schema SR format** as a third codec (low demand expected).
- **Non-Confluent registries** (AWS Glue, Azure) behind the same `serialization`
  kernel interface.
- A future **`agctl.serializers` entry point** for out-of-tree codecs, once a
  stable codec interface emerges from the two in-tree formats.
- **Message-ordering assertions** (`--ordered`) — orthogonal, tracked in
  DESIGN §10.
- **SR over the broker's existing mTLS** — share credentials with the Kafka
  `ssl:` block instead of redeclaring under `schema_registry.ssl:` (convenience,
  not correctness).

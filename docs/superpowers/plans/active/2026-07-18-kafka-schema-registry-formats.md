# Kafka Schema Registry / Avro / Protobuf Serialization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in Avro/Protobuf decode + encode of Kafka message keys and values via Confluent Schema Registry across `kafka produce`/`consume`/`assert`, `kafka listen`, and `mocks.kafka` reactors, so jq/`--contains`/capture operators work on the logical message and produced/reaction bytes are wire-framed.

**Architecture:** A new pure-Python-at-top `agctl/serialization/` subpackage owns Confluct wire-framing, a lazy `SchemaRegistryClient` wrapper, and Avro (`fastavro`) + Protobuf (`DynamicMessage` via the existing `grpc_descriptors` kernel) codecs behind one `decode_payload`/`encode_payload` API. Serialization is a declared topic-level contract in a new additive `kafka.topics` map (with cluster defaults), resolved by one command-layer resolver. Decode drops JSON into the existing message envelope (zero operator change); encode frames Confluent bytes for `produce` and mock reactions. Strict opt-in: no format declared = today's raw-JSON behavior, byte-for-byte.

**Tech Stack:** Python ≥3.11, Pydantic v2, `confluent-kafka` (existing `kafka` extra, ships `schema_registry`), `fastavro` (new `avro` extra), `protobuf` (new standalone `protobuf` extra), `grpc_descriptors` kernel (existing), `jq` (existing). Lazy-import discipline preserved.

**Spec:** [`docs/superpowers/specs/active/2026-07-18-kafka-schema-registry-formats-design.md`](../specs/active/2026-07-18-kafka-schema-registry-formats-design.md)

## Global Constraints

(Copied verbatim from the spec's project-wide requirements; every task's requirements implicitly include these.)

- **Python ≥3.11.** New optional extras: `avro` (`fastavro`), `protobuf` (`protobuf` only, no `grpcio`), and a convenience meta-extra `schema-registry` = `agctl[avro,protobuf]`.
- **Lazy import:** the `agctl/serialization/` kernel MUST NOT `import fastavro`, `import protobuf`, or `import google.protobuf` at module top — those imports live inside the functions that need them. A missing library for a declared format surfaces as `ConfigError` (exit 2) pointing at `pip install 'agctl[avro]'` / `'agctl[protobuf]'`, never a bare `ModuleNotFoundError`. The `confluent_kafka.schema_registry` import is lazy too (inside `SchemaRegistryClient` construction).
- **Strict opt-in / zero behavior change:** a topic/cluster with no `value_format`/`key_format` declared behaves byte-for-byte as today. No major config-version bump (stays `"3"`). No `config migrate`.
- **Fail loud:** unreachable/auth-failing SR at startup → `ConnectionFailure` (exit 2) via a startup probe; bad format string, topic→unknown cluster, declared format with no SR URL, missing extra, bad SR auth shape → `ConfigError` (exit 2); encode schema-conformance mismatch / structural decode failure → new `SerializationError` (exit 2); a single corrupt message mid-stream is NON-fatal and surfaced (never an exception).
- **One-emit / streaming contract:** `kafka consume`/`assert`/`produce` stay `@envelope`-wrapped one-emit; `kafka listen run` / `mock run` stay NDJSON streaming exceptions; new fields/events are additive only.
- **Exit codes:** `0` clean, `1` assertion, `2` config/env — unchanged model.
- **Naming/copy:** dotted config paths in error messages; new listen event name `decode.error`; new result field `decode_errors`; new error type_name `SerializationError`. Follow existing conventions.
- **Cross-cutting lockstep:** (a) `KafkaClient` decode/encode seams are consumed by `kafka_commands` + `listen` + `mock` together — keep the seam signature stable in Task 8 so Tasks 9/11/12/14 don't churn it; (b) the `Format` enum + `resolve_topic_format` in Task 7/9 are the single source of truth consumed by every surface; (c) the `kafka.topics` config map (Task 1) is the single source of truth consulted by resolver (Task 9), validator (Task 2), and discover (Task 10).

---

## File Structure

```
agctl/
├── errors.py                          # EXTEND — SerializationError (Task 3)
├── serialization/                     # NEW subpackage
│   ├── __init__.py                    # public re-exports (Task 7)
│   ├── wire.py                        # NEW — Confluent wire-frame parse/build, pure (Task 4)
│   ├── registry.py                    # NEW — SchemaRegistryClient + build_schema_registry_conf (Task 5)
│   ├── avro_codec.py                  # NEW — fastavro decode/encode, lazy (Task 6)
│   ├── protobuf_codec.py              # NEW — DynamicMessage decode/encode, lazy (Task 13)
│   └── api.py                         # NEW — Format enum, decode_payload, encode_payload, subject helpers (Tasks 7, 14)
├── config/
│   ├── models.py                      # EXTEND — SchemaRegistryConfig, KafkaTopicConfig, KafkaCluster fields, KafkaConfig.topics (Task 1)
│   └── validator.py                   # EXTEND — topic/cluster/SR cross-refs (Task 2)
├── clients/
│   └── kafka_client.py                # EXTEND — decode in consume methods, encode in produce (Tasks 8, 14)
├── commands/
│   ├── kafka_commands.py              # EXTEND — resolve_topic_format, resolve_schema_registry_client, SR probe, CLI flags (Tasks 9, 14)
│   └── discover_commands.py           # EXTEND — kafka-pattern item value_format/key_format (Task 10)
├── listen/
│   ├── capture.py                     # EXTEND — CaptureLoop decode before write (Task 11)
│   └── engine.py                      # EXTEND — format resolution, SR client, decode.error event (Task 11)
├── mock/
│   ├── kafka_reactor.py               # EXTEND — decode trigger, encode reaction (Task 12)
│   └── engine.py                      # EXTEND — thread SR clients + formats to reactors (Task 12)
└── pyproject.toml                     # EXTEND — avro/protobuf/schema-registry extras, SR testcontainer (Task 15)
tests/
├── unit/
│   ├── test_serialization_wire.py        (Task 4)
│   ├── test_serialization_registry.py    (Task 5)
│   ├── test_serialization_avro.py        (Task 6)
│   ├── test_serialization_api.py         (Tasks 7, 14)
│   ├── test_serialization_protobuf.py    (Task 13)
│   ├── test_config_models.py (extend)    (Task 1)
│   ├── test_config_validator.py (extend) (Task 2)
│   ├── test_errors.py (extend)           (Task 3)
│   ├── test_kafka_client_codec.py        (Tasks 8, 14)
│   ├── test_kafka_commands_format.py     (Tasks 9, 14)
│   ├── test_discover_format.py           (Task 10)
│   ├── test_listen_codec.py              (Task 11)
│   └── test_mock_reactor_codec.py        (Task 12)
└── integration/
    └── test_kafka_schema_registry.py     (Task 16)
```

**Dependency order (respect in execution):** 1 → 2; 1 → 5; 3,4 → 6,7; 5,6,7 → 8; 8 → 9; 1 → 10; 9 → 11,12; 4 → 13; 13 → 14; (15 independent, any time after 1); all → 16.

---

## Task 1: Config models — `SchemaRegistryConfig`, `KafkaTopicConfig`, cluster format defaults, `KafkaConfig.topics`

**Files:**
- Modify: `agctl/config/models.py` (add models near `KafkaSSL` at line 85 and extend `KafkaCluster` at 119 / `KafkaConfig` at 135)
- Test: `tests/unit/test_config_models.py` (extend)

**Interfaces:**
- Consumes: existing `KafkaCluster`, `KafkaConfig`, `KafkaSSL` Pydantic v2 models; existing `${ENV}` interpolation (handled by loader, not here).
- Produces:
  - `SchemaRegistryConfig(BaseModel)` with fields: `auth: Literal["plaintext","basic","mtls"] | None = None`; `basic_auth: { username: str | None = None, password: str | None = None } | None = None` (define as a small nested `BasicAuth` model or inline); `ssl: KafkaSSL | None = None` (reuse the existing model). All optional; default `None`.
  - `KafkaTopicConfig(BaseModel)` with fields: `cluster: str | None = None`; `value_format: Literal["json","avro","protobuf"] | None = None`; `key_format: Literal["string","avro","protobuf"] | None = None`; `subject_strategy: Literal["topic","record","topic_record"] | None = None`. All optional.
  - `KafkaCluster` gains: `schema_registry: SchemaRegistryConfig | None = None`; `value_format: Literal["json","avro","protobuf"] = "json"`; `key_format: Literal["string","avro","protobuf"] = "string"`.
  - `KafkaConfig` gains: `topics: dict[str, KafkaTopicConfig] = Field(default_factory=dict)`.
  - Validation rules enforced by Pydantic: invalid literal values → `ValidationError` (which the loader maps to `ConfigError`). Field semantics (auth inference, format-requires-SR) are NOT enforced here — they are cross-refs in Task 2.

- [ ] **Step 1: Write the failing tests**

  Add tests to `test_config_models.py` verifying: (a) `KafkaConfig()` with a `topics: {"orders.created": {value_format: "avro"}}` parses and `cfg.kafka.topics["orders.created"].value_format == "avro"`; (b) `KafkaCluster()` defaults `value_format == "json"` and `key_format == "string"` and `schema_registry is None`; (c) `KafkaCluster(schema_registry={"auth":"basic","basic_auth":{"username":"u","password":"p"}})` parses to a `SchemaRegistryConfig` with `auth == "basic"`; (d) an invalid `value_format: "yaml"` raises Pydantic `ValidationError`; (e) a `KafkaTopicConfig(subject_strategy="record", value_format="json")` parses (strategy validity vs format is a Task-2 warning, not a model error).

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_config_models.py -v`
  Expected: FAIL — `SchemaRegistryConfig` / `KafkaTopicConfig` undefined; `topics` / `value_format` attributes absent.

- [ ] **Step 3: Write minimal implementation**

  Add the two new models and extend `KafkaCluster` / `KafkaConfig` with the fields listed in Produces. Use Pydantic v2 `Literal` types and `Field(default_factory=dict)` for `topics`. No cross-field validation logic here (that is Task 2). Keep `${ENV}` behavior unchanged (interpolation runs before model validation).

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_config_models.py -v`
  Expected: PASS. Also run `pytest tests/unit -k config -q` to confirm no existing config-model test regressed.

- [ ] **Step 5: Commit**

  Run: `git add agctl/config/models.py tests/unit/test_config_models.py`
  Run: `git commit -m "feat(config): add kafka.topics map, schema_registry block, cluster format defaults"`

---

## Task 2: Validator cross-references for topics + SR auth shape

**Files:**
- Modify: `agctl/config/validator.py` (`validate_config` at line 11; add a new block after the existing kafka-pattern checks near line 89)
- Test: `tests/unit/test_config_validator.py` (extend)

**Interfaces:**
- Consumes: Task 1's `KafkaTopicConfig`, `KafkaCluster.schema_registry`, `SchemaRegistryConfig`; existing `validate_config(cfg) -> tuple[list[dict], list[dict]]` returning `(errors, warnings)` where each entry is `{"path": str, "message": str}`. Existing cluster-resolution helper to reuse: `commands/kafka_commands.resolve_cluster_name` is NOT importable here (config must stay free of a commands import) — so resolve a topic's cluster inline: `topic.cluster` → else `cfg.kafka.default_cluster` → else the single cluster when `len(cfg.kafka.clusters) == 1`.
- Produces: `validate_config` now ALSO emits, for the serialization config:
  - **error** `kafka.topics.<t>.cluster` when it names a key absent from `cfg.kafka.clusters`.
  - **error** when a topic's *resolved* `value_format`/`key_format` (topic override else resolved-cluster default) is `avro`/`protobuf` AND the resolved cluster's `schema_registry_url` is `None`/empty — path `kafka.topics.<t>` (or `kafka.clusters.<c>` when the need arises only from a cluster default), message naming the missing SR URL.
  - **error** `kafka.clusters.<c>.schema_registry.auth` when `auth` is set to a value outside `{plaintext,basic,mtls,None}` (Pydantic already rejects this, but guard anyway), OR `auth == "basic"` without `basic_auth` populated, OR `auth == "mtls"` without `ssl` populated.
  - **warning** `kafka.topics.<t>.subject_strategy` when `subject_strategy` is set but the topic's resolved `value_format` is `json` (no encode effect).

- [ ] **Step 1: Write the failing tests**

  Tests in `test_config_validator.py` (build `Config` objects in-code or load minimal YAML fragments), each asserting the `(errors, warnings)` output: (a) a topic with `cluster: nope` → one error at `kafka.topics.<t>.cluster`; (b) a topic `value_format: avro` whose resolved cluster has no `schema_registry_url` → one error; (c) same topic but cluster HAS `schema_registry_url` → zero errors; (d) `schema_registry.auth: basic` with no `basic_auth` → one error at `...schema_registry.auth`; (e) `auth: mtls` with no `ssl` → one error; (f) a topic with `subject_strategy: record` and resolved `value_format: json` → one warning (zero errors); (g) a config with no `kafka.topics` and no SR → zero errors/warnings (unchanged baseline).

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_config_validator.py -v`
  Expected: FAIL — new error/warning cases not produced.

- [ ] **Step 3: Write minimal implementation**

  In `validate_config`, after the existing kafka block, iterate `cfg.kafka.topics.items()`: resolve the cluster inline (precedence `topic.cluster` → `cfg.kafka.default_cluster` → single-cluster auto-default), then emit the error/warning entries per the Produces rules. For the SR-auth-shape checks, iterate `cfg.kafka.clusters.items()` and inspect `cluster.schema_registry`. Path strings use the dotted conventions shown. Keep all messages concrete (name the topic/cluster/field).

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_config_validator.py -v && pytest tests/unit -k "config or validate" -q`
  Expected: PASS, no regressions.

- [ ] **Step 5: Commit**

  Run: `git add agctl/config/validator.py tests/unit/test_config_validator.py`
  Run: `git commit -m "feat(config): validate kafka topics/cluster refs and SR auth shape"`

---

## Task 3: `SerializationError` error type

**Files:**
- Modify: `agctl/errors.py` (append after `TemplateNotFound`)
- Test: `tests/unit/test_errors.py` (extend, or create if absent)

**Interfaces:**
- Consumes: `AgctlError` base (`__init__(self, message: str, detail: dict | None = None)`, class attrs `type_name`, `exit_code`, method `to_dict() -> {"type","message","detail"}`).
- Produces: `SerializationError(AgctlError)` with `type_name = "SerializationError"` and `exit_code = 2`. Constructed as `SerializationError(message, detail)` where `detail` may carry `subject`/`schema_id`/`topic`/`field` keys. The `@envelope` wrapper needs no change (it already handles any `AgctlError` subclass).

- [ ] **Step 1: Write the failing tests**

  Test: `SerializationError("payload does not conform", {"subject":"orders.created-value","topic":"orders.created"}).to_dict()` returns `{"type":"SerializationError","message":"payload does not conform","detail":{"subject":"orders.created-value","topic":"orders.created"}}`; and `SerializationError("x").exit_code == 2`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_errors.py -v`
  Expected: FAIL — `SerializationError` not defined.

- [ ] **Step 3: Write minimal implementation**

  Add the subclass exactly mirroring `ConfigError`/`ConnectionFailure` (two class-attribute lines). No new logic.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_errors.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/errors.py tests/unit/test_errors.py`
  Run: `git commit -m "feat(errors): add SerializationError (exit 2) for codec schema-conformance failures"`

---

## Task 4: Confluent wire-frame kernel (pure)

**Files:**
- Create: `agctl/serialization/__init__.py` (empty for now) and `agctl/serialization/wire.py`
- Test: `tests/unit/test_serialization_wire.py`

**Interfaces:**
- Consumes: nothing (pure Python, no heavy imports — module top imports only `struct` from stdlib).
- Produces (exact signatures in `agctl/serialization/wire.py`):
  - `MAGIC_BYTE: int = 0x00` (constant).
  - `is_confluent_frame(raw: bytes) -> bool` — `True` iff `len(raw) >= 5 and raw[0] == MAGIC_BYTE`.
  - `parse_wire(raw: bytes) -> tuple[int, bytes]` — returns `(schema_id, payload)` where `schema_id` is the 4-byte big-endian int at `raw[1:5]` and `payload` is `raw[5:]`. Raises `ValueError("not a Confluent frame")` when `not is_confluent_frame(raw)`.
  - `build_wire(schema_id: int, payload: bytes) -> bytes` — returns `bytes([MAGIC_BYTE]) + struct.pack(">I", schema_id) + payload`.
  - No `fastavro`/`protobuf`/`confluent_kafka` import anywhere in the module.

- [ ] **Step 1: Write the failing tests**

  Tests (exact expected bytes): `build_wire(42, b"\x18\x00")` == `b"\x00\x00\x00\x00\x2a\x18\x00"`; `build_wire(0, b"")` == `b"\x00\x00\x00\x00\x00"`; `parse_wire(b"\x00\x00\x00\x00\x2a\x18\x00")` == `(42, b"\x18\x00")`; `parse_wire(build_wire(305419896, b"payload")) == (305419896, b"payload")` (round-trip); `is_confluent_frame(b"\x00\x00\x00\x00\x00")` is `True`; `is_confluent_frame(b'{"a":1}')` is `False`; `is_confluent_frame(b"")` is `False`; `is_confluent_frame(b"\x00\x00\x00")` is `False` (<5 bytes); `parse_wire(b"not framed")` raises `ValueError`; `parse_wire(b"")` raises `ValueError`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_serialization_wire.py -v`
  Expected: FAIL — module/import not found.

- [ ] **Step 3: Write minimal implementation**

  Implement the four names above using only `struct`. No edge-case branching beyond the length/magic checks specified.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_serialization_wire.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/serialization/__init__.py agctl/serialization/wire.py tests/unit/test_serialization_wire.py`
  Run: `git commit -m "feat(serialization): pure Confluent wire-frame parse/build kernel"`

---

## Task 5: `SchemaRegistryClient` + `build_schema_registry_conf`

**Files:**
- Create: `agctl/serialization/registry.py`
- Test: `tests/unit/test_serialization_registry.py`

**Interfaces:**
- Consumes: Task 1 `SchemaRegistryConfig` (fields `auth`, `basic_auth`, `ssl`); Task 1 `KafkaCluster.schema_registry_url`. The real `confluent_kafka.schema_registry.SchemaRegistryClient` is lazy-imported inside `__init__` (a missing `kafka` extra surfaces as `ConfigError` pointing at `pip install 'agctl[kafka]'`).
- Produces (in `agctl/serialization/registry.py`):
  - `build_schema_registry_conf(url: str, sr: SchemaRegistryConfig | None) -> dict` — pure function (no imports) translating typed config to the confluent SR conf dict: always includes `"url": url`; if `sr` and `sr.basic_auth` populated, add `"basic.auth.user.info": f"{username}:{password}"`; if `sr` and `sr.ssl` populated, add the mirrored keys `"ssl.ca.location"`, `"ssl.certificate.location"`, `"ssl.key.location"`, `"ssl.key.password"` (only those that are non-empty on the `KafkaSSL`). The `auth` field itself is not a conf key (it is only a hint to the validator).
  - `class SchemaRegistryClient`:
    - `__init__(self, url: str, sr_config: SchemaRegistryConfig | None = None, *, client_factory=None)` — builds the conf via `build_schema_registry_conf`; if `client_factory` is given use it (test seam), else lazy-import and construct `SchemaRegistryClient` from `confluent_kafka.schema_registry` (raise `ConfigError(...)` on `ImportError` pointing at `agctl[kafka]`). Initialize two in-memory caches: `_by_id: dict[int, tuple[str,str]]` and `_by_subject: dict[tuple[str,str], int]`.
    - `get_schema(self, schema_id: int) -> tuple[str, str]` — cached: returns `(schema_type, schema_str)` where `schema_type` is `"AVRO"`/`"PROTOBUF"`/`"JSON"` and `schema_str` is the schema source. Cache miss → call the underlying `get_schema(schema_id)` (confluent returns an object with `.schema_type` and `.schema_str`). Network/HTTP errors propagate as `ConnectionFailure(message=...)`.
    - `register_schema(self, subject: str, schema_str: str, schema_type: str) -> int` — cached by `(subject, schema_str)`: returns the schema id. Calls `register_schema((subject, Schema(schema_str, type=schema_type)))` on the underlying client. HTTP errors → `ConnectionFailure`.
    - `check_reachable(self) -> None` — performs a lightweight call (e.g. `getSubjects()` or `get_config`) against the underlying client; raises `ConnectionFailure(message=...)` naming the cluster + URL on any HTTP/connectivity/auth error; returns `None` on success. Used by the startup probe (Task 9).

- [ ] **Step 1: Write the failing tests**

  Tests (mostly with `client_factory` injecting a fake SR client; one import-skip for the real path): (a) `build_schema_registry_conf("http://sr:8081", None)` == `{"url":"http://sr:8081"}`; (b) with `auth=basic, basic_auth={username:"u",password:"p"}` → conf has `"basic.auth.user.info": "u:p"`; (c) with `ssl` populated (`ca_location`, `certificate_location`, `key_location`) → conf has the three `ssl.*` keys; an unset `ssl` field is omitted; (d) `get_schema(7)` returns the fake's value AND a second `get_schema(7)` does not call the fake again (cache hit — assert fake call count); (e) `register_schema("t-value", "...", "AVRO")` returns the fake's id and is cached; (f) `check_reachable()` returns `None` when the fake succeeds and raises `ConnectionFailure` when the fake raises; (g) real-path test `pytest.importorskip("confluent_kafka")` constructs a client from conf without raising.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_serialization_registry.py -v`
  Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

  Implement `build_schema_registry_conf` (pure) and `SchemaRegistryClient` per Produces. Lazy-import `confluent_kafka.schema_registry` inside `__init__`. Map underlying `SchemaRegistryError`/HTTP errors to `ConnectionFailure` with a clear message. Keep caches per-instance.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_serialization_registry.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/serialization/registry.py tests/unit/test_serialization_registry.py`
  Run: `git commit -m "feat(serialization): lazy SchemaRegistryClient with by-id/by-subject caching + conf builder"`

---

## Task 6: Avro codec (`fastavro`, lazy)

**Files:**
- Create: `agctl/serialization/avro_codec.py`
- Test: `tests/unit/test_serialization_avro.py` (gate round-trip tests on `pytest.importorskip("fastavro")`)

**Interfaces:**
- Consumes: Task 4 `parse_wire`/`build_wire`; lazy `fastavro` (missing → `ConfigError` pointing at `pip install 'agctl[avro]'`). An Avro schema is a JSON string (the writer schema fetched from SR by id).
- Produces (in `agctl/serialization/avro_codec.py`):
  - `_require_fastavro()` — lazy import; on `ImportError` raise `ConfigError("Avro codec requires the 'avro' extra: pip install 'agctl[avro]'", {})`.
  - `decode_avro(raw: bytes, schema_str: str) -> dict | list | scalar` — strips no framing (caller passes `payload` after `parse_wire`); decodes the binary payload against the parsed Avro schema to a JSON-native Python object. Uses `fastavro.schemaless_reader`.
  - `encode_avro(value, schema_str: str) -> bytes` — encodes the value against the parsed schema via `fastavro.schemaless_writer` to bytes (no framing; caller wraps with `build_wire`).
  - `parse_schema(schema_str: str)` — cached parse to a parsed-schema object (avoids re-parsing per message).

- [ ] **Step 1: Write the failing tests**

  Tests (inside `importorskip("fastavro")`): pick a fixed Avro schema `{"type":"record","name":"E","fields":[{"name":"id","type":"string"}]}`. (a) `decode_avro(encode_avro({"id":"x"}, schema), schema) == {"id":"x"}` (round-trip); (b) `encode_avro({"id":"abc"}, schema)` is `bytes` and starts with the Confluent-irrelevant payload (NOT magic-byte — framing is caller's job); decode of those bytes returns the original; (c) a value with an extra field not in the schema raises on encode (fastavro error) — this is acceptable, the api layer (Task 7) wraps encode failures as `SerializationError`; (d) repeated `decode_avro` uses the cached parsed schema (assert parse count via monkeypatch, or simply that it doesn't raise and is correct).

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_serialization_avro.py -v`
  Expected: FAIL — module not found (or skipped if fastavro absent; install `pip install fastavro` locally to run, the test self-skips otherwise).

- [ ] **Step 3: Write minimal implementation**

  Implement the four names; lazy-import fastavro inside each function via `_require_fastavro()`. Keep a module-level `_parsed: dict[str, object]` cache keyed by schema string.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pip install fastavro && pytest tests/unit/test_serialization_avro.py -v` (skip if you cannot install — the importorskip keeps CI green without it).
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/serialization/avro_codec.py tests/unit/test_serialization_avro.py`
  Run: `git commit -m "feat(serialization): lazy fastavro Avro codec (decode/encode)"`

---

## Task 7: Serialization API — `Format`, `decode_payload`, `encode_payload`, subject resolution

**Files:**
- Create: `agctl/serialization/api.py` and populate `agctl/serialization/__init__.py` with public re-exports
- Test: `tests/unit/test_serialization_api.py`

**Interfaces:**
- Consumes: Task 3 `SerializationError`; Task 4 wire kernel; Task 5 `SchemaRegistryClient`; Task 6 `decode_avro`/`encode_avro`. (Protobuf codec from Task 13 is wired in Task 14; until then the api raises `ConfigError` for protobuf.)
- Produces (in `agctl/serialization/api.py`, re-exported from `__init__.py`):
  - `class Format(str, enum.Enum): JSON = "json"; AVRO = "avro"; PROTOBUF = "protobuf"` plus `KEY_STRING = "string"` as a sibling enum or constant used for key resolution (keys default to string, not json).
  - `def decode_payload(raw: bytes, fmt: Format, sr: SchemaRegistryClient | None) -> Any` — when `fmt is JSON`: return `json.loads(raw)` if it parses else the original decoded string (preserve today's `_decode_bytes` behavior for non-JSON bytes). When `fmt in (AVRO, PROTOBUF)`: `schema_id, payload = parse_wire(raw)` (non-frame → `SerializationError("not a Confluent frame", {fmt})`); `schema_type, schema_str = sr.get_schema(schema_id)`; dispatch to `avro_codec.decode_avro(payload, schema_str)` or (Task 14) `protobuf_codec.decode_protobuf(payload, schema_str)`. A codec raising fastavro/protobuf/`SerializationError` propagates as `SerializationError` (caller decides fatal-vs-skip). A missing SR client when `fmt != JSON` → `ConfigError`.
  - `def encode_payload(value, fmt: Format, sr: SchemaRegistryClient | None, *, subject: str) -> bytes` — `JSON`: `json.dumps(value).encode()`; `AVRO`/`PROTOBUF`: resolve schema via `sr.get_latest_schema(subject)` (or register — see note) → `schema_type, schema_str, schema_id`; encode via the codec; return `build_wire(schema_id, encoded)`. Encode-time codec failure → `SerializationError` with `{subject, schema_id}` detail.
  - `def resolve_subject(topic: str, which: str, strategy: str, value: dict | None) -> str` — returns the encode subject: `strategy == "topic"` → `f"{topic}-{which}"` (which is `"value"` or `"key"`); `"record"` → the record name read from the value's Avro schema (for v1, derive from the schema; if unavailable, fall back to `f"{topic}-{which}"` with a warning path); `"topic_record"` → `f"{topic}-{record_name}"`.
  - `def decode_message(value_raw, key_raw, *, value_fmt, key_fmt, sr) -> tuple[Any, Any]` — convenience: decode value and key per their formats, returning `(value, key)`; a `string` key is decoded as today (`_decode_bytes`).

  **Note on encode schema resolution:** SR's `get_latest_version(subject)` returns the latest schema + its id for a subject; prefer that (no registration) when the subject exists. If registration is required (subject absent), call `sr.register_schema(...)`. Keep this logic inside `encode_payload`/a helper, cached by subject on the SR client.

- [ ] **Step 1: Write the failing tests**

  With a fake SR client (in-memory `get_schema`/`register_schema`/`get_latest_schema`) and real avro codec (`importorskip("fastavro")`): (a) `decode_payload(json_bytes, Format.JSON, None)` returns the parsed dict; a non-JSON string returns the string; (b) `decode_payload(build_wire(sid, encode_avro({"id":"x"}, schema)), Format.AVRO, fake_sr)` returns `{"id":"x"}` where `fake_sr.get_schema(sid)` returns `("AVRO", schema)`; (c) the same call with a non-framed `raw` raises `SerializationError`; (d) `encode_payload({"id":"x"}, Format.AVRO, fake_sr, subject="t-value")` returns bytes whose `parse_wire` yields the registered id and whose `decode_payload(..., Format.AVRO, fake_sr)` round-trips to `{"id":"x"}`; (e) `encode_payload` with a value violating the schema raises `SerializationError` carrying `subject`; (f) `resolve_subject("orders.created","value","topic",None) == "orders.created-value"`; (g) `Format("avro") is Format.AVRO`. (Protobuf dispatch is tested in Task 14.)

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_serialization_api.py -v`
  Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

  Implement the api per Produces, dispatching AVRO to `avro_codec` and leaving a PROTOBUF branch that raises `ConfigError("Protobuf codec requires the 'protobuf' extra...")` until Task 14. Re-export public names from `__init__.py`.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_serialization_api.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/serialization/api.py agctl/serialization/__init__.py tests/unit/test_serialization_api.py`
  Run: `git commit -m "feat(serialization): decode_payload/encode_payload api + subject resolution (Avro)"`

---

## Task 8: `KafkaClient` decode/encode seams (read path in consume methods, write path in produce)

**Files:**
- Modify: `agctl/clients/kafka_client.py` (constructor at 102; `produce` at 121; `consume_window` at 195; `find_in_window` at 281; `consume_loop` at 351; `_normalize_message` at 625)
- Test: `tests/unit/test_kafka_client_codec.py`

**Interfaces:**
- Consumes: Task 7 `decode_message`, `encode_payload`, `Format`, `resolve_subject`; `SchemaRegistryClient`.
- Produces: `KafkaClient.__init__` gains two keyword-only DI seams (defaults `None` preserve today's behavior exactly):
  - `value_codec: dict | None = None` and `key_codec: dict | None = None` — OR a single consolidated seam `codec: "CodecHooks | None = None"`. **Use a single seam** `codec` with shape: `{"value": {"fmt": Format, "subject_strategy": str|None} | None, "key": {...} | None, "sr": SchemaRegistryClient | None}`. When `codec is None`, all methods behave byte-for-byte as today (raw JSON values, string keys).
  - `produce(self, topic, value, *, key=None, headers=None)` — when a codec is set with a non-JSON value format, encode `value` (and `key` if its format is non-string) via `encode_payload` + `resolve_subject` BEFORE publishing; the returned `kafka.produce` shape keeps `key` as the decoded key (today it returns `_decode_bytes(key_bytes)` — unchanged). Encode failure → `SerializationError` propagates.
  - The three consume methods return/normalize messages whose `value`/`key` are DECODED JSON when a codec is set: a decode failure on a single message is caught and reported via a counter rather than raised. Concretely, add an optional `on_decode_error: Callable[[str], None] | None = None` parameter (or a returned/accumulated counter) so callers (`consume`/`assert`) can surface `decode_errors`. `_normalize_message` calls `decode_message(value_raw, key_raw, value_fmt=codec["value"]["fmt"], key_fmt=codec["key"]["fmt"], sr=codec["sr"])` inside a try/except: on `SerializationError` it records the error and substitutes `value=None` (or keeps raw) for that message.
  - The seam is the ONLY coupling between the client and serialization — `consume_loop` (used by listen + mock) threads the same codec so reactor/listen decode is automatic.

- [ ] **Step 1: Write the failing tests**

  Using the existing `consumer_factory`/`producer_factory` fakes (see ARCHITECTURE §12) plus a fake SR client and real avro codec: (a) `produce` with `codec=None` publishes `json.dumps(value)` bytes (today's behavior — assert fake producer received JSON bytes); (b) `produce` with `codec={"value":{"fmt":Format.AVRO,"subject_strategy":"topic"},"key":{"fmt":"string"},"sr":fake}` publishes Confluent-framed bytes (assert fake received bytes starting with `\x00` + matching the registered schema id); (c) `consume_window` with a codec set returns messages whose `value` is the decoded dict (feed the fake consumer framed Avro bytes → assert decoded `{"id":"x"}`); (d) `consume_window` with `codec=None` decodes as JSON (unchanged); (e) a single corrupt framed message increments the decode-error callback/counter and is excluded (or null-valued) but does not raise; (f) `consume_loop` (fake) decodes each delivered message before invoking the handler (assert handler received decoded value).

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_kafka_client_codec.py -v`
  Expected: FAIL — `codec` seam absent.

- [ ] **Step 3: Write minimal implementation**

  Add the `codec=None` kwarg to `__init__` (store `self._codec`). In `produce`, branch on codec to encode before publish. In `_normalize_message`, branch on codec to decode value/key with try/except → record via an injected callback or a per-`consume_*` counter returned alongside results. Thread the codec through `consume_window`/`find_in_window`/`consume_loop`. Keep `codec=None` paths identical to current code (this is the backward-compat guarantee — verify with the existing kafka unit suite).

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_kafka_client_codec.py tests/unit -k kafka -q`
  Expected: PASS, no regressions in existing kafka unit tests.

- [ ] **Step 5: Commit**

  Run: `git add agctl/clients/kafka_client.py tests/unit/test_kafka_client_codec.py`
  Run: `git commit -m "feat(kafka): KafkaClient codec seam — decode on consume, encode on produce"`

---

## Task 9: Command-layer resolver + SR probe + CLI flags (consume/assert/produce)

**Files:**
- Modify: `agctl/commands/kafka_commands.py` (near `resolve_cluster_name` at 45, `new_kafka_client` at 115)
- Test: `tests/unit/test_kafka_commands_format.py`

**Interfaces:**
- Consumes: Task 1 config models; Task 5 `SchemaRegistryClient`; Task 7 `Format`; Task 8 `KafkaClient` codec seam.
- Produces (in `kafka_commands.py`, command layer — keeps `config/*` free of a serialization import):
  - `def resolve_topic_format(cfg, topic: str, cluster: str, which: str) -> Format` — precedence (spec §6.3): (1) none from CLI here (CLI handled in the Click layer — see below); (2) `cfg.kafka.topics[topic].value_format/key_format`; (3) `cfg.kafka.clusters[cluster].value_format/key_format`; (4) default `Format.JSON` (value) / key-string. `which` is `"value"` or `"key"`. Unknown topic → fall through to cluster default → JSON (never raises for absence).
  - `def resolve_subject_strategy(cfg, topic: str, cluster: str) -> str` — `cfg.kafka.topics[topic].subject_strategy` → `cfg.kafka.clusters[cluster]...` → default `"topic"`.
  - `def resolve_schema_registry_client(cfg, cluster: str) -> SchemaRegistryClient | None` — builds (and memoizes in a module-level cache keyed by cluster name for the invocation) a `SchemaRegistryClient` from the cluster's `schema_registry_url` + `schema_registry`; returns `None` if no URL. Raises `ConfigError` if a format needs SR but the URL is absent (defense-in-depth; validator already flags it).
  - `def probe_schema_registry(sr: SchemaRegistryClient, cluster: str) -> None` — calls `sr.check_reachable()`; on failure raise `ConnectionFailure(message=f"Schema Registry for cluster '{cluster}' unreachable: ...", {"cluster": cluster})`.
  - CLI flags added to the three commands: `--value-format <json|avro|protobuf>` and `--key-format <string|avro|protobuf>` on `kafka produce`, `kafka consume`, `kafka assert` (override precedence level 1). When supplied, they take precedence over the resolved topic/cluster format.
  - `consume`/`assert`/`produce` cores: after cluster resolution, call `resolve_topic_format` for value+key; if either is non-JSON and a SR client resolves, run `probe_schema_registry` once before the operation; build the `codec` dict and pass it to `new_kafka_client` (extend `new_kafka_client(cluster, group_id=None, *, codec=None)` to forward to `KafkaClient(..., codec=codec)`). `consume`/`assert` results gain `decode_errors: int` (from the client counter).
  - **Lockstep note:** this task establishes the resolver + probe + seam wiring consumed unchanged by Tasks 11 (listen) and 12 (mock). Do not change `KafkaClient`'s `codec` shape here.

- [ ] **Step 1: Write the failing tests**

  Build `Config` in-code. (a) `resolve_topic_format(cfg, "orders.created", "default", "value")` returns `Format.AVRO` when the topic map sets it, `Format.JSON` when absent and cluster default is JSON; (b) cluster default `value_format: avro` with no topic override → `Format.AVRO`; (c) topic override beats cluster default; (d) `resolve_schema_registry_client` returns a client when URL present, `None` when absent; (e) memoization: two calls for the same cluster return the same instance; (f) `probe_schema_registry` raises `ConnectionFailure` when `check_reachable` raises, returns None otherwise; (g) Click-level: `kafka produce --topic t --value-format avro --message '{...}'` resolves value format to AVRO from the flag (assert the codec passed to the client has `Format.AVRO`) — use `CliRunner` with `new_kafka_client` monkeypatched to a fake capturing the codec; (h) `consume` result envelope carries `decode_errors: 0` on a clean run (assert via fake).

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_kafka_commands_format.py -v`
  Expected: FAIL — resolver/probe/flags absent.

- [ ] **Step 3: Write minimal implementation**

  Add the resolver/probe functions; extend `new_kafka_client` with `codec=None`; add the two CLI flags to the three commands; wire codec construction + probe + `decode_errors` into the `_core`s. Run the probe only when a non-JSON format is in play AND an SR client exists.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_kafka_commands_format.py tests/unit -k "kafka" -q`
  Expected: PASS, no regressions.

- [ ] **Step 5: Commit**

  Run: `git add agctl/commands/kafka_commands.py tests/unit/test_kafka_commands_format.py`
  Run: `git commit -m "feat(kafka): topic-format resolver, SR startup probe, --value-format/--key-format flags"`

---

## Task 10: `discover` kafka-pattern item shows resolved value/key format

**Files:**
- Modify: `agctl/commands/discover_commands.py` (kafka-pattern item builder near lines 392–415)
- Test: `tests/unit/test_discover_format.py`

**Interfaces:**
- Consumes: Task 1 `KafkaConfig.topics`/`KafkaCluster.value_format`/`key_format`; the pattern's `topic` and resolved cluster.
- Produces: the `discover --category kafka-patterns --name <x>` item dict (currently has `category`, `name`, `description`, `topic`, `cluster`, `match`, `params`, `example`) gains `value_format: str` and `key_format: str` — the **resolved** format for the pattern's topic (topic override → cluster default → `"json"`/`"string"`). No new category; no count change.

- [ ] **Step 1: Write the failing tests**

  With a `Config` whose pattern `order-created` binds topic `orders.created` and that topic has `value_format: avro`: (a) `discover --category kafka-patterns --name order-created` result dict has `value_format == "avro"` and `key_format == "string"`; (b) a pattern whose topic is absent from `kafka.topics` and whose cluster default is JSON → `value_format == "json"`. (Use the existing discover unit-test harness / `CliRunner`.)

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_discover_format.py -v`
  Expected: FAIL — fields absent.

- [ ] **Step 3: Write minimal implementation**

  In the kafka-pattern item builder, resolve the format (reuse the same precedence — you may import `resolve_topic_format` from `kafka_commands`, or inline the simple resolution to avoid a commands→commands import; prefer a tiny shared helper). Add the two keys to the item dict.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_discover_format.py tests/unit -k discover -q`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/commands/discover_commands.py tests/unit/test_discover_format.py`
  Run: `git commit -m "feat(discover): expose resolved value_format/key_format on kafka-pattern items"`

---

## Task 11: Avro decode in `kafka listen` (capture daemon)

**Files:**
- Modify: `agctl/listen/engine.py` (`ListenEngine.__init__` at 66, `start` at 170) and `agctl/listen/capture.py` (`CaptureLoop.__init__` at 54, `_handle` at 169)
- Test: `tests/unit/test_listen_codec.py`

**Interfaces:**
- Consumes: Task 7 `Format`, `decode_message`; Task 8 `KafkaClient` codec seam; Task 9 `resolve_topic_format`/`resolve_schema_registry_client`/`probe_schema_registry`; the existing `kafka listen start`/`run` argument plumbing (`--topic`/`--pattern`/`--cluster`).
- Produces:
  - `ListenEngine.start` resolves each topic's value/key format + the SR client (once for the daemon) and runs `probe_schema_registry` when a non-JSON format is in play; it builds one `KafkaClient` per cluster with the `codec` seam set so the underlying `consume_loop` delivers decoded messages.
  - `CaptureLoop._handle` writes the **decoded** envelope to `<topic>.ndjson` (the client already decoded it). A decode failure (the client surfaces a per-message error rather than raising) emits a non-fatal `decode.error` event `{event:"decode.error", topic, error, fatal:false, timestamp}` via the engine's `emit_event` and skips writing that message.
  - The `summary` event gains `decode_errors` per topic (a counter the CaptureLoop accumulates).

- [ ] **Step 1: Write the failing tests**

  With a fake `KafkaClient` whose `consume_loop` yields one decoded Avro message and one decode-error: (a) the capture file contains exactly the decoded message (assert the `.ndjson` content); (b) `emit_event` received a `decode.error` event for the bad message with `fatal: false`; (c) the `summary` event's per-topic entry carries `decode_errors: 1`; (d) `ListenEngine.start` calls `probe_schema_registry` when a topic resolves to AVRO and an SR client exists (assert probe invoked, and that it raises `ConnectionFailure` → surfaces as a startup error envelope before any `started` line).

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_listen_codec.py -v`
  Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

  Resolve formats + SR in `start`; pass the codec into the per-cluster `KafkaClient`; in `CaptureLoop._handle` branch on a decode-error signal from the client to emit `decode.error` + skip, else write; accumulate `decode_errors`; include it in `_emit_summary`.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_listen_codec.py tests/unit -k listen -q`
  Expected: PASS, no regressions.

- [ ] **Step 5: Commit**

  Run: `git add agctl/listen/engine.py agctl/listen/capture.py tests/unit/test_listen_codec.py`
  Run: `git commit -m "feat(listen): decode Avro values on capture; decode.error event; decode_errors in summary"`

---

## Task 12: Avro decode-trigger + encode-reaction in `mocks.kafka` reactors

**Files:**
- Modify: `agctl/mock/kafka_reactor.py` (`KafkaReactor.__init__` at 39, `_handle` at 118) and `agctl/mock/engine.py` (`MockEngine.__init__` at 54, the reactor-construction block near 271–281)
- Test: `tests/unit/test_mock_reactor_codec.py`

**Interfaces:**
- Consumes: Task 7 `Format`, `decode_message`, `encode_payload`, `resolve_subject`; Task 8 `KafkaClient` codec seam; Task 9 resolver; existing `FakeKafkaClient` test seam.
- Produces:
  - `MockEngine` constructs each reactor's `KafkaClient` with the **trigger topic's** codec (so `consume_loop` delivers decoded trigger values) and gives the reactor a `reaction_codec` resolved from the **reaction topic's** format + the SR client(s) for the reaction cluster.
  - `KafkaReactor._handle`: the incoming `msg` is already decoded (trigger codec) → existing `match` jq / `capture.from` work unchanged. The reaction produce now encodes `value`/`key` per the reaction codec before publish. A trigger that the client reports as a decode failure → emit `kafka.skipped` with `reason: "decode failed: ..."` (non-fatal, COMMIT — consistent with today's non-object skip). A reaction encode failure → `kafka.error` event (fatal per existing semantics).

- [ ] **Step 1: Write the failing tests**

  Using `FakeKafkaClient`: (a) a reactor whose trigger topic is AVRO and whose reaction topic is JSON: feed a decoded Avro trigger → `match` predicate evaluates against the decoded dict → reaction published as JSON (today's path); (b) trigger JSON, reaction AVRO: the reaction is published as Confluent-framed bytes (assert the fake producer received `\x00`-prefixed bytes matching the registered schema id); (c) a trigger decode failure → one `kafka.skipped` event with a `decode failed` reason and no `kafka.reacted`; (d) a reaction encode failure (payload violates schema) → one `kafka.error` event with `fatal` set.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_reactor_codec.py -v`
  Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

  Thread trigger codec into the reactor's client (engine construction) and a reaction codec + SR client into `KafkaReactor`. In `_handle`, encode the reaction via the reaction codec before `client.produce`; surface client decode-failure signals as `kafka.skipped`. Keep the JSON-trigger/JSON-reaction path unchanged.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_mock_reactor_codec.py tests/unit -k mock -q`
  Expected: PASS, no regressions.

- [ ] **Step 5: Commit**

  Run: `git add agctl/mock/kafka_reactor.py agctl/mock/engine.py tests/unit/test_mock_reactor_codec.py`
  Run: `git commit -m "feat(mock): decode Avro triggers + encode Avro reactions in kafka reactors"`

---

## Task 13: Protobuf codec (`DynamicMessage`, lazy)

**Files:**
- Create: `agctl/serialization/protobuf_codec.py`
- Test: `tests/unit/test_serialization_protobuf.py` (gate on `pytest.importorskip("google.protobuf")`)

**Interfaces:**
- Consumes: Task 4 `parse_wire`/`build_wire`; the existing `agctl/clients/grpc_descriptors.py` kernel (`build_descriptor_pool`, `find_method`-style lookup adapted to a single message, `message_class`, `serialize`, `deserialize`, `add_file_protos_order_tolerant`); lazy `protobuf`/`grpcio-tools` (missing → `ConfigError` pointing at `pip install 'agctl[protobuf]'`). A Protobuf schema fetched from SR is a `.proto` source string.
- Produces (in `agctl/serialization/protobuf_codec.py`):
  - `_require_protobuf()` — lazy import; on `ImportError` raise `ConfigError("Protobuf codec requires the 'protobuf' extra: pip install 'agctl[protobuf]'", {})`.
  - `_message_descriptor(schema_str: str) -> MessageDescriptor` — build a `DescriptorPool` from the `.proto` string via `build_descriptor_pool` (cache by schema string), then resolve the single top-level message descriptor (the last message defined, or the schema's declared message — v1 assumes one message per schema; multi-message schemas pick the one matching the record name, else the sole message).
  - `decode_protobuf(raw: bytes, schema_str: str) -> dict` — build the message descriptor, `deserialize(message_desc)(raw)` (kernel helper → JSON-native dict).
  - `encode_protobuf(value: dict, schema_str: str) -> bytes` — `serialize(message_desc)(value)` → bytes.
  - **Limitation (documented, fail-loud):** multi-file Protobuf with imports is best-effort via `add_file_protos_order_tolerant`; if resolution fails, raise `SerializationError("cannot resolve protobuf schema (multi-file imports?)", {schema_snippet})`.

- [ ] **Step 1: Write the failing tests**

  `importorskip("google.protobuf")`. Use a fixed single-message `.proto` source string `syntax="proto3"; message E { string id = 1; }`. (a) `decode_protobuf(encode_protobuf({"id":"x"}, proto), proto) == {"id":"x"}` (round-trip); (b) `encode_protobuf({"id":"abc"}, proto)` is bytes; decode returns the original; (c) a deliberately malformed proto string raises `SerializationError` (fail-loud). (Note: the kernel's `build_descriptor_pool` may need the proto compiled via `grpc_tools.protoc`; that machinery already exists — do not reimplement.)

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pip install protobuf grpcio-tools && pytest tests/unit/test_serialization_protobuf.py -v` (self-skips without the extra).
  Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

  Implement the three functions, reusing the descriptor kernel. Cache the per-schema descriptor pool/message descriptor. Map kernel/protobuf errors to `SerializationError`.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_serialization_protobuf.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/serialization/protobuf_codec.py tests/unit/test_serialization_protobuf.py`
  Run: `git commit -m "feat(serialization): lazy Protobuf codec via grpc_descriptors kernel (single-file v1)"`

---

## Task 14: Wire Protobuf into the API and all surfaces

**Files:**
- Modify: `agctl/serialization/api.py` (PROTOBUF branch in `decode_payload`/`encode_payload`), `tests/unit/test_serialization_api.py` (extend)
- Verify (no code change expected, but test): the surfaces (Tasks 8/9/11/12) handle `Format.PROTOBUF` purely through the seam — add coverage tests
- Test: `tests/unit/test_kafka_client_codec.py`, `tests/unit/test_kafka_commands_format.py`, `tests/unit/test_listen_codec.py`, `tests/unit/test_mock_reactor_codec.py` (extend each with a Protobuf case)

**Interfaces:**
- Consumes: Task 13 `decode_protobuf`/`encode_protobuf`; the existing seam-based surfaces.
- Produces: `decode_payload`/`encode_payload` now dispatch `Format.PROTOBUF` to `protobuf_codec` (instead of the Task-7 placeholder `ConfigError`). No surface code changes are expected — the surfaces consume `Format` opaquely.

- [ ] **Step 1: Write the failing tests**

  `importorskip("google.protobuf")`. (a) `decode_payload(build_wire(sid, encode_protobuf({"id":"x"}, proto)), Format.PROTOBUF, fake_sr)` returns `{"id":"x"}` (fake `get_schema` returns `("PROTOBUF", proto)`); (b) `encode_payload({"id":"x"}, Format.PROTOBUF, fake_sr, subject="t-value")` round-trips through `decode_payload`; (c) one Protobuf case added to each of the four surface test files: produce emits framed Protobuf bytes; consume decodes; listen capture writes decoded; mock reactor decode-trigger + encode-reaction. These should pass without surface edits if the seam is format-agnostic — if a surface hardcoded Avro, this step exposes it.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_serialization_api.py tests/unit/test_kafka_client_codec.py tests/unit/test_kafka_commands_format.py tests/unit/test_listen_codec.py tests/unit/test_mock_reactor_codec.py -v`
  Expected: FAIL on the Protobuf-dispatch test (Task 7 placeholder raised `ConfigError`); surface tests may pass already (good — seam is clean) or fail (fix the surface to be format-agnostic).

- [ ] **Step 3: Write minimal implementation**

  Replace the Task-7 PROTOBUF placeholder with a dispatch to `protobuf_codec`. If any surface test failed, generalize that surface (it should branch on `Format`, never on a hardcoded Avro name). Keep the Avro paths untouched.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit -k "serialization or kafka or listen or mock" -q`
  Expected: PASS across both codecs.

- [ ] **Step 5: Commit**

  Run: `git add agctl/serialization/api.py tests/`
  Run: `git commit -m "feat(serialization): wire Protobuf into decode_payload/encode_payload + surface coverage"`

---

## Task 15: Packaging — `avro` / `protobuf` / `schema-registry` extras + SR integration container

**Files:**
- Modify: `pyproject.toml` (`[project.optional-dependencies]` and the `integration` extra)
- Test: none (packaging); verify via install + import probe

**Interfaces:**
- Consumes: nothing code-side.
- Produces:
  - `[project.optional-dependencies]` gains `avro = ["fastavro>=1.8"]`, `protobuf = ["protobuf>=4.25"]`, `schema-registry = ["agctl[avro,protobuf]"]`.
  - The `integration` extra gains the Confluent Schema Registry container dependency used by Task 16 (e.g. add `agctl[avro,protobuf]` to the integration extra so live tests can decode; the actual container is spun in conftest, Task 16).
  - No new `[project.entry-points]` group (in-tree codecs).

- [ ] **Step 1: Write the failing test**

  A small test `tests/unit/test_packaging_extras.py` that asserts the three extras are declared in `pyproject.toml` (parse via `tomllib`): `avro`, `protobuf`, `schema-registry` keys exist in `[project.optional-dependencies]`; `schema-registry` references both `agctl[avro]` and `agctl[protobuf]`.

- [ ] **Step 2: Run test to verify it fails**

  Run: `pytest tests/unit/test_packaging_extras.py -v`
  Expected: FAIL — extras absent.

- [ ] **Step 3: Write minimal implementation**

  Add the three extras and extend `integration` exactly as in Produces. Do not bump the package version in this task (version bump happens at release).

- [ ] **Step 4: Run test to verify it passes**

  Run: `pytest tests/unit/test_packaging_extras.py -v && pip install -e '.[avro,protobuf]' && python -c "import fastavro, google.protobuf; print('ok')"`
  Expected: PASS; the import probe prints `ok`.

- [ ] **Step 5: Commit**

  Run: `git add pyproject.toml tests/unit/test_packaging_extras.py`
  Run: `git commit -m "build: add avro/protobuf/schema-registry extras; integration extra gains codecs"`

---

## Task 16: Integration tests — Avro + Protobuf round-trips + SR auth matrix

**Files:**
- Modify: `tests/integration/conftest.py` (add `require_schema_registry` fixture + container wiring under `AGCTL_TEST_LIVE=1`)
- Create: `tests/integration/test_kafka_schema_registry.py`

**Interfaces:**
- Consumes: Tasks 1–15 (full stack); the existing `require_kafka` fixture + testcontainers Kafka+KRaft setup.
- Produces:
  - A `require_schema_registry` fixture that, under `AGCTL_TEST_LIVE=1`, starts Confluent `cp-schema-registry` paired with the existing Kafka container (SR needs a running broker), wires its URL into `AGCTL_TEST_*` env, and yields the SR URL; otherwise `pytest.skip()`.
  - `tests/integration/test_kafka_schema_registry.py` with self-skipping tests covering: (a) Avro round-trip `produce` → `consume` → `assert` (value + key); (b) Avro capture via `kafka listen start`/`assert`/`results`; (c) Avro mock reactor decode-trigger + encode-reaction; (d) Protobuf equivalents of (a); (e) the auth matrix: plaintext SR, HTTPS+basic-auth SR (Confluent-Cloud-style via a local basic-auth-configured SR), and (f) a `--value-format` CLI override case. Each test registers a schema (or lets `produce` register it) and asserts decoded JSON in results.

- [ ] **Step 1: Write the failing tests**

  Write the test module + fixture. Each test calls `pytest.skip()` when `require_schema_registry`/`require_kafka` yield nothing (the existing self-skip convention). Assert decoded JSON payloads appear in `consume`/`assert`/`listen results` and that produced bytes are Confluent-framed (where observable).

- [ ] **Step 2: Run tests to verify they fail (or skip)**

  Run: `pytest tests/integration/test_kafka_schema_registry.py -v` (no `AGCTL_TEST_LIVE`) → Expected: all SKIP (services absent). Run `AGCTL_TEST_LIVE=1 pytest tests/integration/test_kafka_schema_registry.py -v` in an environment with Docker → Expected: FAIL until the feature is wired end-to-end (this is the integration gate; by this task the unit-tested pieces should compose, so failures here flag integration gaps).

- [ ] **Step 3: Write minimal implementation**

  Implement the fixture (container start/stop + env wiring) and any glue the tests reveal is missing (typically none, if Tasks 1–14 are correct). Keep self-skip behavior exact: no Docker/`AGCTL_TEST_LIVE` → SKIP, never FAIL.

- [ ] **Step 4: Run tests to verify they pass (live) or skip (default)**

  Run: `AGCTL_TEST_LIVE=1 pytest tests/integration/test_kafka_schema_registry.py -v` → Expected: PASS (live). Run: `pytest tests/integration/test_kafka_schema_registry.py -q` → Expected: all SKIP.

- [ ] **Step 5: Commit**

  Run: `git add tests/integration/conftest.py tests/integration/test_kafka_schema_registry.py`
  Run: `git commit -m "test(integration): Avro+Protobuf SR round-trips across produce/consume/assert/listen/mock; auth matrix"`

---

## Self-Review (run after writing; fixes applied inline)

1. **Code scan:** No method bodies, algorithms, or test/impl code in the plan — each step states behavior + exact expected results + signatures only. (Held.)
2. **Self-containment:** Every task lists Consumes/Produces with exact signatures and data shapes; no task says "see the spec" or "see earlier." (Held — repeated contracts deliberately where tasks may be read out of order.)
3. **Spec coverage:** spec §2 goals → Tasks 1–16; §6 config → Tasks 1–2; §7 surfaces → Tasks 8–12; §8 output → Tasks 8–11; §9 errors → Tasks 3,5,7,9; §10 packaging → Task 15; §11 testing → each task + Task 16; §12 backward-compat → Task 8 (`codec=None`) + Global Constraints; §13 Protobuf limitation → Task 13 Produces + Task 14. No spec section uncovered.
4. **Placeholder scan:** No TBD/TODO/"handle edge cases"/"add validation." Each validation rule and error case is spelled out. (Held.)
5. **Type consistency:** `Format` (Task 7) used consistently in Tasks 8/9/11/12/14; `codec` seam shape (Task 8) referenced unchanged in Tasks 9/11/12; `SchemaRegistryClient.get_schema`/`register_schema`/`check_reachable` (Task 5) used in Tasks 7/9; `resolve_topic_format`/`resolve_schema_registry_client`/`probe_schema_registry` (Task 9) used in Tasks 10/11/12. Names stable across tasks. (Held.)

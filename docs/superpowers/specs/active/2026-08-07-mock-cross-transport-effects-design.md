# Design: `agctl mock` — Cross-Transport Effects (HTTP ↔ Kafka)

**Status:** in_progress
**Date:** 2026-08-07
**Branch:** `feat/mock-cross-transport-effects`
**Affects:** `agctl/config/models.py`; `agctl/mock/effects.py` (new); `agctl/mock/engine.py`; `agctl/mock/http_server.py`; `agctl/mock/kafka_reactor.py`; `agctl/mock/daemon.py`; `agctl/mock/capture_validate.py`; `agctl/commands/mock_commands.py`; `agctl/commands/config_commands.py`; `agctl/config/validator.py`; DESIGN.md §2.1 / §3.6 / §10; ARCHITECTURE.md §3 / §4 / §6 / §10
**Precedent:** [`2026-07-17-grpc-mock-server-design.md`](./2026-07-17-grpc-mock-server-design.md) (third mock engine; same capture/render/event seams), [`2026-07-04-agctl-mock-capture-design.md`](../archive/2026-07-04-agctl-mock-capture-design.md) (the shared capture pipeline this reuses)
**Relation to docs:** Fills the deferred "cross-transport reactions" item in DESIGN.md §10 (HTTP trigger → Kafka produce; Kafka trigger → HTTP callback). On implementation, DESIGN.md §2.1 (add `effects` to `mocks.http.stubs` / `mocks.kafka.reactors`), §3.6 (effect lifecycle, events, semantics), §10 (move cross-transport reactions from deferred → done; add new deferred items), and ARCHITECTURE.md §3 (module map: `mock/effects.py`), §4 (effect trace for both invocation points), §6 (new events), §10 are synced via `docs-watcher`.

---

## 1. Background & Problem

`agctl mock` runs three engines (HTTP stubs, Kafka reactors, gRPC stubs) that share one event stream, one capture/render pipeline, and one daemon layer. Each engine's *primary* result is same-protocol:

- An **HTTP stub** (`mocks.http.stubs.<name>`) matches an incoming request and returns an HTTP `response`. Its only non-response action is emitting an NDJSON event line. It cannot produce a Kafka message or call another endpoint.
- A **Kafka reactor** (`mocks.kafka.reactors.<name>`) consumes a topic, matches, and produces a Kafka `reaction`. This is the only "trigger → effect" pattern that exists today, and it is hardwired Kafka→Kafka.
- A **gRPC stub** returns a gRPC message.

Two cross-transport behaviors are **not** expressible today, and both are explicitly deferred in DESIGN.md §10: *"Mock: cross-transport reactions — HTTP trigger → Kafka produce; Kafka trigger → HTTP callback. The trigger→reaction model admits this later without a rewrite."*

Concretely, an agent cannot configure:

1. **HTTP → Kafka:** a test POSTs the mock and the mock *publishes a Kafka event* the SUT then consumes (webhook/event-injection simulation), and
2. **Kafka → HTTP:** the SUT publishes an event and the mock *makes an outbound HTTP call* in response (downstream/webhook/callback simulation).

This spec closes that gap by generalizing the reactor's single Kafka `reaction` into an ordered, polymorphic **`effects`** list that both `HttpStub` and `KafkaReactor` carry, reusing the transport-agnostic capture/render pipeline and the already-DI-injected `KafkaClient` / `HttpClient`.

## 2. Goals

- **HTTP trigger → Kafka produce:** an HTTP stub match fires one or more Kafka messages.
- **Kafka trigger → HTTP call:** a reactor match fires one or more outbound HTTP calls.
- **Compose, don't fork:** a trigger may fire an **ordered list** of effects (fan-out), effects may **chain** (an HTTP call's response feeds the next effect via capture), and a single kafka effect may emit **multiple messages**.
- **Reuse the seams:** `CaptureSpec` / `resolve_captures` / `render_typed`, the per-cluster `KafkaClient` (+ independent reaction-codec encode path), and `HttpClient` are reused unchanged. No new transport code.
- **Fail loud:** an effect that fails (Kafka undelivered, HTTP connection refused, unresolved service/cluster) is fatal to the run — surfaced as an event and the exit-1 escalation, never a silent false-green.
- **Zero cost when unused:** configs without `effects` behave byte-for-byte as today; the legacy `reaction` keeps its `kafka.reacted` event and semantics.

## 3. Scope & Design Constraints

- **Effects ride on existing triggers.** An `effects:` list is an optional sibling of `HttpStub.response` and `KafkaReactor.reaction`. There is no new trigger kind and no new top-level mock section.
- **Effects are synchronous and ordered.** For an HTTP trigger, effects run *before* the HTTP response is sent (a Kafka produce blocks until acked, as `KafkaClient.produce` + flush already does), so a returned 2xx guarantees the side-effect landed. For a Kafka trigger, effects run inside the reactor's per-message handler.
- **Effect failure is fatal.** An `effect.error` sets the runtime-error flag → `mock run` exits 1 at shutdown / `mock stop` raises `AssertionFailure`, consistent with `kafka.error` / `grpc.unmatched`. The trigger message is **COMMITted, not retried** (the trigger was valid; retrying would re-fire side effects and risk duplicates).
- **Cluster / codec resolution for kafka effects** reuses the `kafka produce` chain. For an HTTP trigger there is no reactor cluster to inherit, so a kafka effect resolves its own.
- **HTTP effects reuse `HttpClient`** (TLS / timeout / exception-mapping inherited from the named `service` or `url`).

## 4. Non-Goals

- **gRPC stub effects** — deferred. The `Effect` union and executor are shaped so `GrpcStub.effects` and a future `grpc` effect variant slot in without config redesign; covered by a later spec.
- **New CLI commands** — none. Effects are config-only and surface through the existing `mock run` / `start` / `stop` / `status` event stream.
- **DB / log / delay effects, and a pluggable effect registry** — not now. Two in-tree variants (kafka, http) do not justify the `agctl.mock_effects` entry-point machinery yet; the executor's dispatch can grow into that registry without a config change if a third variant lands.
- **Full assertion modes on http-effect responses** (`--status` / `--match`) — deferred. The effect `capture` block + the `http.called` event's `status_code` cover the common "did the callback succeed and what did it return" need.
- **Referencing a named HTTP `template` from an http effect** — deferred; http effects carry inline fields only.

---

## 5. Decisions (recorded with rationale)

1. **Generalized `effects` list over dedicated per-direction fields.** A single ordered, polymorphic list satisfies all four scope items (basics, fan-out, capture-response-chaining, multiple-messages); dedicated `produce_kafka:` / `call_http:` fields cannot fan out or chain and dead-end fast. *Rejected:* dedicated fields.
2. **`type:` discriminator (kafka | http).** Consistent with agctl's pervasive `type:` idiom (`database.connections.*.type`, `logs.sources.*.type`, codec `value_format`). *Rejected:* a `{kafka: …}` / `{http: …}` single-key object style — awkward to model in Pydantic and inconsistent with the codebase.
3. **`value` XOR `values` on the kafka effect.** `value` covers one message; `values: [ {value, key?, headers?}, … ]` covers the "multiple Kafka messages" item within one effect. Top-level `key` / `headers` are per-message defaults (and fallback for list items that omit their own). *Rejected:* forcing multiple messages to be multiple effects (loses rendering a `values` list from one captured array).
4. **HTTP effect dual target mode (service | url).** Mirrors `http request`: `service` + `path` (resolved against a configured `services.*`) or a literal `url`, mutually exclusive. Reuses `HttpClient` and the service's TLS/timeout config for free.
5. **Capture chaining: ordered accumulation, last-wins on collision.** The trigger's own `capture` runs first; each effect's `capture` adds to the same namespace; later effects may reference earlier names in their `value` / `body` / `path` / `key`. On a name collision the later value wins — predictable because the list is ordered. This is documented behavior, not a validate-time error (ordered overwrite is the intended chaining mechanism).
6. **`KafkaReactor.reaction` is back-compat sugar.** It stays valid and is exactly equivalent to `effects: [{type: kafka, topic, key, value, headers}]` bound to the reactor's existing trigger-cluster client + `reaction_codec`. Setting **both** `reaction` and `effects` on one reactor is a validate-time `ConfigError`. The legacy path keeps emitting `kafka.reacted` unchanged.
7. **Synchronous, produce/call-before-respond for HTTP triggers.** Effects run inside the HTTP handler's existing concurrency-semaphore section, before the response bytes are written, so the response is an honest acknowledgement. *Rejected:* fire-and-forget (respond immediately, produce in the background) — breaks test determinism and the fail-loud guarantee.
8. **Effect failure: fatal + COMMIT-not-retry.** Mirrors `kafka.error` / `grpc.unmatched`. The trigger is valid; the side-effect failed. Surfaced via `effect.error` + exit 1, never via re-delivery. `--fail-fast` stops on the first `effect.error`.
9. **HTTP response under `--fail-fast` (the one open runtime detail, closed).** Default mode: all effects run; on any failure the response is still sent and the run fails at shutdown. `--fail-fast`: on the first `effect.error` the process aborts before responding, so the SUT sees a connection-level failure (hard-fail opt-in). No retroactive response rewriting in either mode.
10. **Codec/client pre-resolution stays in the command layer.** `mock_run` resolves each kafka effect's cluster + codec (reusing `resolve_cluster_name` / `_resolve_codec`) and ensures a per-cluster `KafkaClient` exists, mirroring how `reaction_codecs` / `kafka_clients` are built and passed to the engine today. Keeps `mock/` and `config/` free of command-layer imports.

---

## 6. Config Schema — `effects`

New Pydantic models join `config/models.py`: `KafkaEffect`, `HttpEffect`, and a `type:`-discriminated `Effect` union. `HttpStub` and `KafkaReactor` each gain an optional `effects: list[Effect]`. They reuse `CaptureSpec` verbatim.

```yaml
mocks:
  http:
    stubs:
      fire-order:                       # HTTP → Kafka (multi-message fan-out)
        method: POST
        path: /orders
        capture: { orderId: { from: ".body.id" } }
        response: { status: 202, body: { accepted: true } }
        effects:
          - type: kafka
            topic: orders.events
            value: { id: "{orderId}", type: "CREATED" }
          - type: kafka
            topic: item.events
            values:
              - { value: { orderId: "{orderId}", sku: "x" } }
              - { value: { orderId: "{orderId}", sku: "y" } }

  kafka:
    reactors:
      on-order:                         # Kafka → HTTP → Kafka (chained capture)
        topic: orders.events
        match: '.value.type == "CREATED"'
        capture: { orderId: { from: ".value.id" } }
        effects:
          - type: http
            service: order-service
            method: POST
            path: /internal/notify
            body: { id: "{orderId}" }
            capture: { ackId: { from: ".body.ackId" } }
          - type: kafka
            topic: audit.events
            value: { ref: "{ackId}", orderId: "{orderId}" }
```

### 6.1 `type: kafka` — produce one or more messages

| Field | Required | Notes |
|---|---|---|
| `topic` | yes | produce target |
| `value` **xor** `values` | yes (exactly one) | `value` = one message; `values` = ordered list, each `{value, key?, headers?}`. Both rendered via `render_typed`. |
| `key`, `headers` | no | top-level per-message defaults (single-`value` case; fallback for `values` items that omit their own) |
| `cluster` | no | resolution reuses the documented topic chain: `cluster` → `kafka.topics.<topic>.cluster` → `kafka.default_cluster` → single-cluster auto-default. **An HTTP trigger has no reactor cluster to inherit**, so the effect must resolve its own. |
| `value_format`, `key_format` | no | same chain as `kafka produce`: `kafka.topics.<topic>` → cluster default → `json` / `string`. A non-default format whose resolved cluster has no `schema_registry_url` → `ConfigError` (defense-in-depth), as today. |

Encode reuses `KafkaClient.produce` (+ the independent reaction-codec `_raw` path the reactor already uses), so an HTTP trigger can emit an Avro message against a JSON-format cluster.

### 6.2 `type: http` — one outbound call

| Field | Required | Notes |
|---|---|---|
| `service` **xor** `url` | yes (exactly one; mutually exclusive) | service mode: `service` (a `services.*` key) + `path`; url mode: a full `url`. Mirrors `http request`. |
| `path` | service mode | supports `{placeholder}` |
| `method` | no (default `GET`) | |
| `headers`, `body` | no | rendered via `render_typed` |
| `timeout` | no | seconds; falls back to the service's `timeout_seconds` → `defaults.timeout_seconds` → `10` (hard default, mirroring `resolve_timeout` — a None chain never disables timeouts) |
| `capture` | no | a `CaptureSpec` map rooted in the **HTTP response envelope** `{status_code, response_time_ms, headers (lowercased), body, url, method}` — same root as `http call --match`, so `.body.ackId`, `.status_code`, `.headers.x`. Feeds the namespace for later effects. |

The call goes through the existing `HttpClient`, so `service.use_tls` / `service.tls` / `service.base_url` apply for free.

### 6.3 Field-contract notes

- `Effect` is a `type:`-discriminated union (`Annotated[Union[KafkaEffect, HttpEffect], Field(discriminator="type")]`).
- A kafka effect requires exactly one of `value` / `values` (structural; `ConfigError` otherwise).
- An http effect requires exactly one of `service` / `url` (structural; `ConfigError` otherwise).
- `effects` is optional on both `HttpStub` and `KafkaReactor`; absent ≡ no side-effects.
- **`KafkaReactor.reaction` becomes optional** (it is required today). At least one of `reaction` / `effects` must be present — a reactor with neither is a `ConfigError`. This is what makes a pure Kafka→HTTP tap (consume → call HTTP → produce nothing) expressible. `HttpStub.response` stays required (unchanged).

---

## 7. Behavior & Semantics

### 7.1 Capture chaining

Captures accumulate in execution order: the trigger's own `capture` runs first (rooted in the trigger envelope — HTTP request `{method, path, headers (lowercased), body}` / Kafka message `{key, value, partition, offset, timestamp, headers (case-sensitive)}`), then each effect's `capture` adds to the same namespace. Later effects render against the accumulated namespace. **On a name collision, the later value wins** (ordered, deterministic).

### 7.2 Ordering & execution

Effects run in list order. Each is fully resolved (render + dispatch + ack) before the next begins; the executor short-circuits on the **first** failing effect. The primary result is rendered from the trigger's own captures only (independent of effect captures):

- **HTTP trigger:** render `response` → run `effects` inside the concurrency-semaphore section → send the response. A returned 2xx means every effect acked.
- **Kafka trigger:** match → trigger capture → run `effects` → `COMMIT`. The legacy `reaction` is expanded at startup into one bound kafka effect (same trigger-cluster client + `reaction_codec`) and runs in its list position if `effects` is absent.

### 7.3 Multiple Kafka messages

A kafka effect with `values` produces one message per list item, in order, each rendered independently against the namespace. Each emit is a separate `kafka.produced` event. A delivery failure on any item short-circuits the effect (fatal).

### 7.4 Cluster / client / codec resolution (command layer)

`mock_run` walks every `effects` entry across HTTP stubs and Kafka reactors. For each kafka effect it resolves the cluster (`resolve_cluster_name`), resolves the codec (`_resolve_codec`), and ensures a `KafkaClient` exists for that cluster (extending the existing `clients_by_cluster` map so HTTP-triggered effects that target a cluster no reactor uses still get a client). For each http effect it validates the `service` resolves (or that `url` is well-formed). The resolved clients + an `effect_codecs` map are handed to the engine, which threads them into the executor — mirroring the existing `kafka_clients` / `reaction_codecs` plumbing.

---

## 8. Runtime — `EffectExecutor` (new `mock/effects.py`)

`EffectExecutor` owns side-effect execution. It is constructed by `MockEngine` (which owns client lifecycles and the single-writer `emit_event`) and shared by both invocation points. It holds:

- the per-cluster `KafkaClient` map + the `effect_codecs` map (kafka-effect → resolved codec), both built in the command layer;
- a lazily-created `HttpClient`, resolved per http effect from the named `service` (or `url`), cached by service for connection reuse and per-service TLS config.

Its contract is a single dispatch: given an ordered `effects` list, a capture namespace, and a trigger label, render each via `render_typed`, dispatch by `type`, emit the success/failure event, and stop at the first failure. Kafka effects reuse `KafkaClient.produce` (+ independent-codec `_raw` encode); http effects reuse `HttpClient` (TLS/timeout/exception-mapping inherited). No new transport code.

**Invocation points:**

- **`make_handler` (HTTP):** after match → `resolve_captures` → render `response`, the handler runs `stub.effects` through the executor **inside the existing concurrency-semaphore section, before the response is written**.
- **`KafkaReactor._handle`:** after match → capture, the reactor runs its `effects` list. The legacy `reaction` is expanded at startup into one bound kafka effect (preserving today's Kafka→Kafka path byte-for-byte). Explicit kafka effects resolve their **own** cluster/client/codec.

---

## 9. Output Contract & Events

Streaming exception (unchanged model): not wrapped by `@envelope`; one NDJSON object per event via the single-writer `emit_event` lock. New events on the shared stream:

```json
{"event":"kafka.produced","trigger":"fire-order","topic":"orders.events","key":"ord-1","duration_ms":2,"timestamp":"…"}
{"event":"http.called","trigger":"on-order","service":"order-service","method":"POST","path":"/internal/notify","status_code":200,"duration_ms":18,"timestamp":"…"}
{"event":"effect.error","trigger":"on-order","effect_type":"http","error":"Connection refused","url":"…","fatal":true,"timestamp":"…"}
```

| Event | When | Fatal |
|---|---|---|
| `kafka.produced` | each explicit kafka effect succeeds (one per `values` item) | no |
| `http.called` | each http effect succeeds | no |
| `effect.error` | any effect fails (`effect_type` = `kafka` \| `http`) | **yes** |

- The legacy `reaction` path keeps emitting **`kafka.reacted`** unchanged. Only *explicit* kafka effects emit `kafka.produced`. (Back-compat for existing fixtures/tests that grep `kafka.reacted`.)
- `effect.error` joins `FATAL_FAILURE_EVENTS`; `kafka.produced` / `http.called` / `effect.error` join `EVENT_TO_COUNTER` (all in `mock/daemon.py`).
- `summary` gains `kafka_produced`, `http_called`, `effect_errors`. The `started` event needs no new block (effects ride on existing stubs/reactors).

---

## 10. Error & Exit-Code Model

No new exception classes. Reused:

- **`ConfigError` (exit 2)** — validate-time: unknown http-effect `service`; unresolvable / empty-broker kafka-effect `cluster`; kafka-effect topic format needing SR without `schema_registry_url`; `reaction` + `effects` on one reactor; structural `value`/`values` or `service`/`url` mutual-exclusion violations; unknown `{{...}}` generator tokens in effect fields. Runtime: missing `kafka` / `http` extra surfaces as `ConfigError` pointing at the right `pip install` (lazy-import convention).
- **`effect.error` (fatal, exit 1 at shutdown / `AssertionFailure` at `mock stop`)** — Kafka produce undelivered, HTTP call connection/timeout failure, effect encode failure. The trigger message is COMMITted (not retried). `--fail-fast` stops on the first `effect.error`.

Every code path still emits exactly one structured object before exit (the streaming contract); startup errors emit one envelope before any event line.

---

## 11. Module Layout

```
agctl/
├── mock/
│   ├── effects.py             # NEW — EffectExecutor: render + dispatch + first-failure short-circuit
│   ├── engine.py              # EXTEND — construct executor; thread into http handler + reactors;
│   │                          #         kafka_produced/http_called/effect_errors counters; effect.error fatal flag
│   ├── http_server.py         # EXTEND — make_handler receives executor; runs effects in semaphore section before response
│   ├── kafka_reactor.py       # EXTEND — _handle runs effects; startup expands legacy reaction into one bound kafka effect
│   ├── daemon.py              # EXTEND — FATAL_FAILURE_EVENTS += effect.error; EVENT_TO_COUNTER += kafka.produced/http.called/effect.error
│   └── capture_validate.py    # EXTEND — collect_capture_placement_errors walks http-effect capture
├── config/
│   ├── models.py              # EXTEND — KafkaEffect, HttpEffect, Effect union; effects on HttpStub + KafkaReactor;
│   │                          #         KafkaReactor.reaction → optional (≥1 of reaction/effects required)
│   └── validator.py           # EXTEND — cross-refs: http-effect service; kafka-effect cluster/brokers + format-needs-SR; reaction+effects mutex
└── commands/
    ├── mock_commands.py       # EXTEND — mock_run resolves per-kafka-effect cluster+codec, ensures clients, validates http services, builds executor ingredients
    └── config_commands.py     # EXTEND — collect_unknown_template_errors walks effect fields
```

`mock/jq_precompile.py` is **unchanged** — effects introduce no new jq predicates (captures use runtime `jq_value`, not startup-compiled predicates).

---

## 12. Validation Rules

- **Offline (`config validate` + Step 0):**
  - Each effect's `capture.<name>.from` placement is valid (`collect_capture_placement_errors` extended to walk http-effect captures; kafka effects have no capture).
  - Unknown `{{...}}` generator tokens in effect fields (kafka `value`/`values`/`key`/`headers`/`topic`; http `body`/`path`/`headers`/`url`) are caught by `collect_unknown_template_errors`.
  - Cross-refs: http-effect `service` exists (error, like HTTP template → unknown service); kafka-effect `cluster` resolves with non-empty brokers (error); kafka-effect topic whose resolved format needs SR without `schema_registry_url` (error); `reaction` + `effects` on one reactor (error).
  - Structural: kafka effect has exactly one of `value`/`values`; http effect has exactly one of `service`/`url`; a reactor has at least one of `reaction`/`effects`.
- **Runtime (`mock_run` startup):**
  - Each kafka effect's cluster + codec resolves (reuses the SR startup probe, once per cluster); each http effect's service resolves. Unresolved → `ConfigError` before the engine binds.
- Each error carries a precise `mocks.{http.stubs,kafka.reactors}.<name>.effects[<i>].<field>` path.

---

## 13. Testing Strategy

- **Unit (extra-free dispatch core):** an `EffectExecutor` dispatch test without `confluent_kafka` / `httpx` (injected fakes sharing the real client contracts, as existing reactor tests do). Covers: `render_typed` over each variant; `values` multi-message ordering; capture-chaining accumulation + last-wins on collision; `reaction`→effects expansion equivalence (byte-for-byte with today's `kafka.reacted` path); first-failure short-circuit; codec/client resolution seams in the command layer.
- **Unit (config):** `KafkaEffect` / `HttpEffect` validation — `value`/`values` and `service`/`url` mutual exclusion; `reaction` + `effects` mutex; cross-ref errors (unknown service, dangling cluster, format-needs-SR); unknown-`{{...}}` surfacing.
- **Unit (daemon taxonomy):** `effect.error` is fatal; `kafka.produced` / `http.called` / `effect.error` counted; `mock stop` raises on `effect.error`.
- **Integration (`AGCTL_TEST_LIVE=1`, real plaintext broker):** HTTP→Kafka verified via `kafka consume`; Kafka→HTTP verified against a mock HTTP target; end-to-end chaining (HTTP call response → next kafka effect); `values` fan-out; `effect.error` → `mock run` exits 1 / `mock stop` raises `AssertionFailure`.
- **Back-compat regression:** existing `reaction` fixtures still emit `kafka.reacted` and the pre-change summary keys, unchanged.

---

## 14. Backward Compatibility & Docs Sync

- **Backward compatible.** `effects` is opt-in. The legacy `reaction` stays valid (= one bound kafka effect, emits `kafka.reacted`). New events and summary counters are additive. No existing config or event changes.
- **Docs sync (via `docs-watcher` at implementation finish):**
  - DESIGN §2.1 — `effects` under `mocks.http.stubs` / `mocks.kafka.reactors`; the two effect variants and field tables.
  - DESIGN §3.6 — effect lifecycle, ordering, events, failure semantics.
  - DESIGN §10 — move "cross-transport reactions" from deferred → done; add new deferred items (gRPC effects; effect registry; http-effect template reference; full http-effect response assertions).
  - ARCHITECTURE §3 — `mock/effects.py` in the module map.
  - ARCHITECTURE §4 — effect trace for both invocation points (HTTP handler semaphore section; reactor `_handle`).
  - ARCHITECTURE §6 — new events (`kafka.produced`, `http.called`, `effect.error`) + summary counters.
  - ARCHITECTURE §10 — note effects are an in-tree dispatch (not an entry-point registry), with the evolution path.

---

## 15. Open Questions / Deferred

- **gRPC stub effects** — `GrpcStub.effects` + a `grpc` (call) / `grpc-consume` effect variant. The union and executor admit this without config redesign; separate spec.
- **Pluggable effect registry (`agctl.mock_effects`)** — the Approach 3 evolution path, for db-write / log / delay / third-party effects. Not justified until a third variant lands.
- **Referencing a named HTTP `template` from an http effect** — inline fields cover v1; template reference is sugar for later.
- **Full assertion modes on http-effect responses** (`--status` / `--match`) — capture + `http.called.status_code` cover the common need; full modes deferred.

### 15.1 Resolved during the final-review fix wave (recorded for the ADR)

- **§5.9 fail-fast response-abort — implemented** (was an open runtime detail even after Decision 9 closed the semantics). `MockEngine` threads `fail_fast` into `make_handler`/`MockHTTPServer`; the handler consults `effect_executor.run(...)`'s outcome and, under fail-fast + `ok=False`, returns without writing a response (semaphore released in `finally`; no `http.hit` for an unserved response).
- **Service-mode `path` rendering — implemented** (spec §6.2 promised it; the initial executor passed `path` verbatim). `_run_http` applies `render_typed(path, namespace)` when `effect.service is not None`; url mode stays fully literal.
- **`values` item fallback to top-level `key`/`headers` — implemented** (spec §6.1 documented it as a fallback; the initial executor used items as-is). Per item: the item's own `key`/`headers` when set, else the effect-level ones.
- **`{{gen}}` generator pre-pass — implemented.** `EffectExecutor.run` builds ONE memo per call and runs `substitute_generators` over each effect's payload fields before `render_typed` (kafka `value`/`values[*]`/`key`/`headers`; http `body`/`headers`/ service-mode `path`). Known limitation (pre-existing, now documented in DESIGN §2.5): mock *response* bodies and kafka `reaction` payloads are NOT generator fill sites — tokens there are validated but served literally.
- **`--no-template-vars` does not reach the executor** — the global flag is read via `template_vars_enabled_from_ctx` by command callbacks, but `mock run` never threads it into the engine/executor; the pre-pass is therefore unconditional. Left as-is per fix-wave ruling (no new flag plumbing).
- **HttpClient pool keyed `(base_url, timeout)`** — the initial per-`base_url` cache silently reused the first effect's timeout for later effects on the same host; the key now includes the resolved timeout.
- **`agctl discover` on effects-only reactors** — the category-listing example and the item `reaction` serialization dereferenced `reaction.topic`/`model_dump` unguarded (legal `reaction=None` since T1); both guarded, with the effects list serialized under `effects` and the example naming the first kafka effect's topic (`<effects>` placeholder when only http effects).

---

## TL;DR

Add an ordered, polymorphic **`effects:`** list to `mocks.http.stubs.<name>` and `mocks.kafka.reactors.<name>`, closing the deferred HTTP↔Kafka cross-transport gap. Each effect is `type: kafka` (produce one message or a `values` list) or `type: http` (outbound call, service or url mode). Effects run synchronously and in order after a trigger matches: for HTTP, inside the handler's semaphore section before the response is sent; for Kafka, inside the reactor's per-message handler. Effects chain via per-effect `capture` (rooted in the effect's own result envelope; last-wins on name collision) and reuse the existing `resolve_captures` / `render_typed` pipeline, per-cluster `KafkaClient` (+ independent reaction-codec encode), and `HttpClient`. Effect failure is fatal (`effect.error` → exit 1 / `mock stop` raises; COMMIT-not-retry; `--fail-fast` stops on first). New events `kafka.produced` / `http.called` / `effect.error` are additive; the legacy `reaction` stays as back-compat sugar emitting `kafka.reacted`. gRPC effects, a pluggable effect registry, http-effect template references, and full http-effect response assertions are deferred.

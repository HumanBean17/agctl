# Design: Mock Server — `agctl mock`

**Status:** Approved (design) — ready for implementation plan
**Date:** 2026-07-03
**Author:** brainstorming session
**Affects:** new `agctl mock` command group, new `mocks` config section, config models/validator, new `agctl/mock/` subpackage, `KafkaClient` (one new method), discovery, `agctl-config` skill
**Relation to docs:** **Reverses** a documented DESIGN §1 non-goal. On implementation, DESIGN.md §1 / §2 / §3 / §4 / §10 and ARCHITECTURE.md §3 / §4 / §5 / §6 / §8 / §12 / §15 are synced via `docs-watcher`.

---

## 1. Background & Problem

`agctl` can drive the system under test (SUT) and assert on its state, but it
cannot *impersonate* the SUT's external integrations. In local testing the real
downstream dependencies are usually absent — the SUT calls an external HTTP API
that is not running, or publishes a Kafka message that no consumer handles. Today
users must stand up a separate mock tool (WireMock, LocalStack, etc.) and point
their config at it. That is exactly what DESIGN §1 prescribed: *"Mocking /
service virtualization. No built-in mock server. Use a dedicated tool … and point
`agctl` at it via config."*

This spec **deliberately reverses that non-goal** and adds first-class mock
support for HTTP and Kafka, so a local test session is self-contained.

Two reframes discovered during design make this tractable and consistent with
`agctl`'s principles:

- **No Kafka broker.** The SUT's local environment already runs a real broker
  (it is a Kafka-based system). What is missing is the *downstream consumer*. So
  `agctl` does not impersonate a broker — it joins the SUT's real broker as a
  consumer and **reacts** to messages, playing the role of the absent consumer.
- **HTTP mock is an embedded server** the SUT's outbound HTTP client points at.
  It is small enough to build on the Python standard library with no new
  dependency.

The result is a mock that is SUT-facing (the real application connects to it),
composed from existing `agctl` machinery, and run as a foreground process that
mirrors the one long-running command already in the codebase (`http ping`).

## 2. Goals

- Let an agent impersonate the SUT's external **HTTP** and **Kafka**
  integrations during local testing, with zero external tooling.
- Reuse existing matchers and substitution (`resolution.py` `{placeholder}`,
  `assertions.py::json_subset`, the jq engine, `KafkaClient.produce`) rather than
  reinventing them.
- Stay consistent with the stateless principle (ARCHITECTURE §2 / DESIGN §8): a
  foreground, killable process; no daemon, no pidfiles, no on-disk state.
- Stay consistent with **fail loudly**: every mock interaction is observable via
  a streaming event line; unmatched HTTP requests never pass silently.
- Keep the engine **stateless** (no cross-call state) so behavior is deterministic
  and trivially unit-testable.

## 3. Scope & Design Constraints

These constrain the whole design and are load-bearing for several decisions.

- **SUT-facing.** The mock is a real network endpoint (HTTP server) or a real
  consumer (Kafka) that the SUT's own clients talk to. It is *not* a client-side
  fake of `agctl`'s own calls — that would only cover `agctl`, not the SUT, and
  would not close the gap.
- **`agctl` is the sole reactor on a topic.** Where a topic has a real second
  consumer that also reacts, partition-ownership/dueling-replier conflicts are
  **out of scope**. The mock assumes it is the handler (via consumer-group /
  partition assignment) for the topics it reacts on.
- **Foreground process.** `mock run` blocks and is backgrounded by the agent with
  `&` / killed (the heartbeat pattern, DESIGN §3.1 / UC-3). A managed daemon is a
  deferred additive extension (§9, §16).
- **Within-transport reactions only.** An HTTP trigger yields an HTTP response; a
  Kafka trigger yields a Kafka produce. Cross-transport reactions (e.g. a Kafka
  message triggering an HTTP callback) are deferred (§16) but the model admits
  them later without a rewrite (D4).
- **Static + request-templated.** Reactions may interpolate values captured from
  the triggering request/message, but carry no state across calls (D5).

## 4. Non-Goals

- **Reimplementing the Kafka wire protocol / a broker.** The SUT's real broker is
  used; `agctl` only consumes and reacts.
- **Cross-transport reactions** in the first cut (D4; deferred §16).
- **Stateful / scenario mocks** — sequences, counters, "Nth call returns Y"
  (deferred §16).
- **Record / replay** of real traffic into stubs (deferred §16).
- **A managed daemon** (`mock start/stop/status` with pidfile + control socket)
  in the MVP (deferred §16).
- **Multiple HTTP servers / ports.** One server, many stubs (path-routed).
- **Exactly-once reactor delivery.** At-least-once with idempotent reactions is
  the contract (§8); exactly-once is out of scope.
- **Adversarial containment.** The mock binds to `127.0.0.1` by default and is a
  local-test convenience, not a security boundary.
- **New protocols** (gRPC, etc.). HTTP and Kafka only.

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | **A mock is a SUT-facing trigger→reaction rule.** Trigger = HTTP request matcher or Kafka message matcher; reaction = HTTP response or Kafka produce. | The real application's clients connect to the mock. Client-side faking would only cover `agctl`'s own calls and would not close the local-testing gap. |
| D2 | **No Kafka broker; `agctl` reacts as a committed consumer** on the SUT's real broker (reusing top-level `kafka.brokers` / `kafka.ssl`). | Reimplementing the Kafka wire protocol is impractical (that is why Redpanda exists). The real broker is already part of the SUT's local environment; the missing piece is the downstream consumer, which `agctl` plays. |
| D3 | **HTTP mock = an embedded server on the Python standard library** (`http.server`), no new dependency. | A mock HTTP server is a small, well-understood problem; stdlib suffices and preserves the packaging-minimalism invariant (no new core dependency, no new optional extra for HTTP-only mocks). |
| D4 | **Within-transport reactions only**, but the trigger→reaction model is shaped so cross-transport reactions are a future extension, not a rewrite. | Cross-transport covers comparatively rare async request/reply shapes; YAGNI for the MVP. Two focused engines ship sooner and stay simpler. |
| D5 | **Static + request-templated richness; no cross-call state.** Reactions interpolate values captured from the trigger; the engine is otherwise stateless. | Covers ~90% of integration mocking (including request-derived IDs). A stateless engine is deterministic and trivially unit-testable, matching `agctl`'s stateless ethos. Stateful scenarios are deferred (§16). |
| D6 | **Foreground long-running command** (`agctl mock run`), not a managed daemon. | Consistent with the `http ping` precedent (ARCHITECTURE §6) and the statelessness principle (DESIGN §8). No on-disk state, no CLI↔daemon IPC. A daemon is an additive later option (§16). |
| D7 | **NDJSON streaming output** — the second sanctioned exception to one-emit-per-invocation. | A live mock must surface traffic/events as they happen; mirrors `http ping`. Startup errors still emit one structured envelope before any line (the `http ping` startup pattern). |
| D8 | **Runtime-error policy: continue + emit + escalate at shutdown.** A reaction produce failure emits a `kafka.error` line and the reactor keeps running; the shutdown summary reflects error counts and sets the exit code. | A mock should not die on one bad message mid-test. The stream keeps failure observable (fail loudly) without being brittle. |
| D9 | **Reuse existing machinery** (`{placeholder}` substitution, `json_subset`, jq, `KafkaClient.produce`) and add **exactly one** new client surface — a committed consume loop on `KafkaClient` (§8). | No reinvention; consistency with the assertion engine and the existing Kafka client. The committed loop is genuinely new because existing `consume_window` re-seeks by time and ignores offsets. |

## 6. Command Contract — `agctl mock run`

```
agctl mock run
    [--config <path>]            # auto-discovered by default
    [--http-listen <host:port>]  # override mocks.http.listen
    [--only http|kafka]          # run a single engine
    [--duration <seconds>]       # stop after N s; mutually exclusive with --until-stopped
    [--until-stopped]            # default: run until SIGTERM/SIGINT
```

- Loads the full config pipeline (ARCHITECTURE §5), then starts the HTTP server
  and every Kafka reactor configured under `mocks`.
- `--only` restricts to one engine (e.g. HTTP-only mocks need no `kafka` extra).
- `--duration` / `--until-stopped` mirror `http ping`.
- **Not** wrapped by `@envelope`; it hand-reimplements the try/except → emit + exit
  shape, exactly as `http ping` does (ARCHITECTURE §4 / §6). It is the second
  command to use the streaming-output exception.

## 7. Config Schema — new `mocks:` section

A new optional top-level section, additive and backward-compatible (existing
configs without `mocks` are unaffected). Validated by new Pydantic v2 models in
`agctl/mock/models.py` and surfaced on `Config` (`agctl/config/models.py`).
Conventions match existing sections: `description` on every item, `${ENV}`
interpolation at load, `{placeholder}` at match/react time, jq predicates for
Kafka.

### 7.1 Config contract (YAML)

```yaml
mocks:
  # ── HTTP mock server: agctl serves these; the SUT points its outbound HTTP
  #    client at `listen`. One server, many stubs (path-routed).
  http:
    listen: "${AGCTL_MOCK_HTTP_HOST:-127.0.0.1}:${AGCTL_MOCK_HTTP_PORT:-18080}"
    stubs:
      create-order:
        description: "Mock the downstream order API"
        method: POST
        path: "/api/v1/orders"
        match:                          # optional; narrows when this stub fires
          body: { "priority": "high" }  # json_subset filter (reuses assertions.py)
        response:
          status: 201
          headers: { Content-Type: "application/json" }
          body:
            order_id: "{customer_id}-mock"   # templated from captured request data
            status: "PENDING"
        delay_ms: 0                    # optional latency simulation

      get-order:
        description: "Fetch a mocked order"
        method: GET
        path: "/api/v1/orders/{order_id}"   # {order_id} captured → usable below
        response:
          status: 200
          body: { order_id: "{order_id}", status: "CONFIRMED" }

  # ── Kafka reactors: agctl joins a consumer group on the SUT's REAL broker
  #    (reuses top-level kafka.brokers / kafka.ssl) and reacts to messages by
  #    producing a templated reply. Committed offsets → behaves like a real consumer.
  kafka:
    reactors:
      order-command-handler:
        description: "Mock the service that consumes order commands"
        topic: orders.commands            # consume from here
        consumer_group: agctl-mock-order-handler
        match: '.command == "CREATE_ORDER"'   # jq predicate; optional (omit = match all)
        reaction:
          topic: orders.events             # produce reply here
          key: "{orderId}"                 # templated from the matched message value
          value:
            eventType: "ORDER_CREATED"
            orderId: "{orderId}"
            status: "PENDING"
```

### 7.2 Pydantic model shape

| Model | Fields |
|---|---|
| `MocksConfig` | `http: HttpMockConfig \| None`, `kafka: KafkaMockConfig \| None` |
| `HttpMockConfig` | `listen: str` (host:port; supports `${ENV}`), `stubs: dict[str, HttpStub]` |
| `HttpStub` | `description: str`, `method: str` (upper-cased), `path: str` (with `{name}` segments), `match: HttpMatch \| None`, `response: HttpResponse`, `delay_ms: int = 0` |
| `HttpMatch` | `body: dict \| None` (subset filter) |
| `HttpResponse` | `status: int`, `headers: dict \| None`, `body: Any \| None` |
| `KafkaMockConfig` | `reactors: dict[str, KafkaReactor]` |
| `KafkaReactor` | `description: str`, `topic: str`, `consumer_group: str`, `match: str \| None` (jq predicate), `reaction: KafkaReaction` |
| `KafkaReaction` | `topic: str`, `key: str \| None`, `value: Any`, `headers: dict \| None` |

`method` is upper-cased at load (mirroring how `KafkaSSL.security_protocol` is
normalized, ARCHITECTURE §5). `status` is constrained to an integer HTTP status.

### 7.3 Capture / templating model (symmetric across transports)

Both transports resolve reaction templates via the existing `{placeholder}`
substitution in `resolution.py`, against a capture context built from the trigger:

- **HTTP** — context = path params (from `{name}` segments) ∪ top-level keys of
  the JSON request body (when the body parses as a JSON object). `match.body` is a
  *filter* (the subset must be present for the stub to match); it is not a capture
  source.
- **Kafka** — context = top-level keys of the matched message's parsed JSON value.

Unsupplied `{placeholder}` values are left literal — the inherited limitation
already documented for HTTP templates (ARCHITECTURE §15 / DESIGN §10 deferred).
Capturing nested fields and request *headers* is deferred (§16).

## 8. Trigger → Reaction Engine

Two focused engines under `agctl/mock/`, sharing the trigger→reaction mental model.

### 8.1 HTTP server (`agctl/mock/http_server.py`)

A stdlib `ThreadingHTTPServer` whose handler delegates to `MockEngine.handle_http`.

**Match contract** — a stub matches iff, in definition order (first match wins):
1. `method` equals the request method (case-insensitive), **and**
2. `path` matches the request path per the path-template contract
   (`agctl/mock/routing.py`): literal segments match exactly and each `{name}`
   segment captures a single path segment into the context (no wildcards/regex in
   the MVP), **and**
3. if `match.body` is present, `json_subset(match.body, request_body)` is true
   (reuses `assertions.py`).

**Reaction** — render `response.body` / `response.headers` via `{placeholder}`
against the capture context (§7.3); sleep `delay_ms`; return `status` + headers +
body; emit `http.hit`. **No match** → `404` with body
`{"mock_error":"no matching stub"}` and an `http.unmatched` line.

### 8.2 Kafka reactor (`agctl/mock/kafka_reactor.py`)

One thread per reactor. **Match contract** — a message is handled iff
`reactor.match` is omitted (match-all) **or** `jq_bool(match, message_value)` is
true (reuses the jq engine behind `--match`, lazy-imported under the `kafka`
extra). Non-matching messages are committed and skipped (like `kafka consume
--match`).

**Reaction** — render `reaction.value` / `key` / `headers` via `{placeholder}`
against the message-value context (§7.3); produce via `KafkaClient.produce`
(reused unchanged); commit the offset; emit `kafka.reacted`.

### 8.3 The one new client surface — committed consume loop

`KafkaClient` gains a committed consume primitive (e.g. `consume_loop` /
`poll_and_commit`) that the reactor drives: poll → invoke a callback per message →
commit. It is distinct from `consume_window`, which re-seeks by time and ignores
committed offsets (correct for one-shot `consume`/`assert` but wrong for a
long-lived reactor). Contract:

- **At-least-once.** A message may be re-delivered after a crash/restart if the
  commit did not land; reactions must therefore be **idempotent** (the author's
  responsibility, mirroring `db execute` idempotency, DESIGN §3.3).
- Commits after the reaction succeeds; a reaction failure does **not** block the
  partition (D8 — emit `kafka.error`, commit, continue).
- Reuses the existing confluent-kafka `Consumer`, top-level `kafka.brokers` /
`kafka.ssl`, and the `_kafka_ssl_conf` translation owned by the command layer.

## 9. Runtime & Lifecycle (`agctl/mock/engine.py`)

`MockEngine` owns the HTTP server thread, one thread per Kafka reactor, a shared
`stop` `Event`, and `SIGTERM`/`SIGINT` handlers (the `http ping` precedent).

- **Start** — validate config; bind HTTP; for each reactor, construct a committed
  consumer and start its thread; emit `started`.
- **Run** — serve HTTP; each reactor polls/matches/reacts/commits; every event
  emits one NDJSON line (§10). `--duration` arms a timer that sets `stop`.
- **Shutdown** (`SIGTERM`/`SIGINT` or `--duration`) — stop accepting HTTP; finish
  in-flight requests; flush producers; commit final offsets; close consumers;
  emit `summary`; exit `0` (clean, no runtime errors) or `1` (clean but runtime
  errors occurred, per D8).

The mock holds in-memory runtime state only (matched-stub bookkeeping for the
summary). It writes **nothing** to disk — the ARCHITECTURE §2 statelessness
invariant concerns `agctl`-local disk state, which still holds.

## 10. Output Contract (NDJSON — second streaming exception)

`mock run` emits one JSON object per line. Timestamps are ISO-8601 Z (matching
`http ping`).

```json
{"event":"started","http":{"listen":"127.0.0.1:18080","stubs":2},"kafka":{"reactors":[{"name":"order-command-handler","topic":"orders.commands","consumer_group":"agctl-mock-order-handler"}]},"timestamp":"…"}
{"event":"http.hit","stub":"create-order","method":"POST","path":"/api/v1/orders","status":201,"duration_ms":3,"timestamp":"…"}
{"event":"http.unmatched","method":"GET","path":"/api/v1/unknown","status":404,"timestamp":"…"}
{"event":"kafka.reacted","reactor":"order-command-handler","topic":"orders.events","key":"ord-789","duration_ms":1,"timestamp":"…"}
{"event":"kafka.error","reactor":"order-command-handler","topic":"orders.commands","error":"…","timestamp":"…"}
{"event":"summary","http_hits":7,"http_unmatched":1,"kafka_reactions":3,"kafka_errors":0,"duration_ms":45213}
```

As with `http ping`, all machine-readable output is on stdout; diagnostics only
on stderr. Startup failures emit **one** structured envelope (the §4.1 shape with
`command: "mock.run"`) **before** any event line, then exit 2.

## 11. Error & Exit-Code Model

No new error types; the existing `AgctlError` hierarchy covers every path.

| Failure | Type | Exit | When |
|---|---|---|---|
| Bad `mocks:` schema / unresolved `${ENV}` / version mismatch | `ConfigError` | 2 | startup, before serving (one envelope) |
| HTTP bind failure (port in use) | `ConfigError` | 2 | startup |
| `mocks.kafka` present but no top-level `kafka.brokers` | `ConfigError` | 2 | startup (cross-ref, §12) |
| Broker unreachable at startup | `ConnectionFailure` | 2 | startup (fail fast; retry/backoff deferred) |
| `mock run` with reactors but the `kafka` extra missing | `ConfigError` | 2 | startup (lazy-import → install hint) |
| Reaction produce failure at runtime | *(streamed `kafka.error` line)* | — | runtime (continue, D8) |
| Unmatched HTTP request | *(HTTP 404 + `http.unmatched` line)* | — | runtime (not an error) |
| Clean shutdown, no runtime errors | `summary` line | 0 | shutdown |
| Clean shutdown, runtime errors occurred | `summary` line | 1 | shutdown |

## 12. Validation Rules (`agctl/config/validator.py`)

Two new cross-reference rules alongside the existing checks:

1. **`mocks.kafka` requires `kafka.brokers`.** If any reactor is configured but
   the top-level `kafka` block is absent or has no brokers → config validation
   error (exit 2). (Evaluated by `agctl config validate`; the per-command load
   path does not run `validate_config`, mirroring the §8 honesty in the
   db-execute spec — the runtime startup failure is the per-invocation guard.)
2. **Missing `description` on any stub or reactor → warning** (not an error),
   consistent with templates/patterns (discovery degrades without it).

Topics are free-form (as they already are for `kafka.patterns`), so no
topic-existence cross-ref is added.

## 13. Discovery Changes (`agctl/commands/discover_commands.py`)

A new **`mocks`** category (one category, items carry a `type` discriminator).
Purely additive — existing categories and consumers ignoring unknown keys are
unaffected.

- **Level 0 summary** adds `http_stubs` and `kafka_reactors` counts (and the
  category appears in the hint's category list).
- **Level 1** lists stubs and reactors as `{name, description, type}` where
  `type` is `"http-stub"` or `"kafka-reactor"`.
- **Level 2** detail: an http-stub reports `method`/`path`/`match`/`response`;
  a kafka-reactor reports `topic`/`consumer_group`/`match`/`reaction.topic` plus
  a ready-to-use example.
- **Search** covers mock names/descriptions.

## 14. Packaging

- **HTTP server** = stdlib `http.server` → **no new dependency**; HTTP-only mocks
  work on a core install.
- **Reactors** reuse `confluent-kafka` + `jq`, already the `kafka` extra →
  `pip install 'agctl[kafka]'`. Missing extra at `mock run` → `ConfigError` with
  the install hint (existing lazy-import pattern, ARCHITECTURE §8).
- **No `pyproject.toml` dependency changes, no new entry-point group.** `mock` is
  a built-in command group registered in `cli.py` like `http`/`kafka`/`db` — not
  a plugin (ARCHITECTURE §10 extension surface is untouched).

## 15. Testing Strategy

Mirrors the existing test architecture and seams (ARCHITECTURE §12).

**Unit (no network):**
- `routing` path-template → param extraction (pure function).
- HTTP stub matching (method + path template + `json_subset`) and response
  templating as pure functions, with an injectable `handler_factory`.
- Kafka reactor match (jq) + reaction templating with injected
  `consumer_factory` / `producer_factory` fakes (the `kafka_commands` seam).
- `MockEngine` start/stop with injected fake engines; signal wiring.
- Pydantic model validation (bad `status`, missing required fields, `method`
  upper-casing).
- Config validation: `mocks.kafka` requires `kafka.brokers`; missing-`description`
  warning.

**HTTP integration (no Docker):** start the real `ThreadingHTTPServer` on port
`0` (ephemeral), drive it with `httpx` (the `http` extra, available in dev);
assert responses, unmatched-404, `delay_ms`, and NDJSON lines.

**Kafka integration (self-skipping):** under `AGCTL_TEST_LIVE=1`, reuse the
existing testcontainers Kafka harness; feed messages via a producer, assert a
templated reaction is produced and offsets advance. Self-skips otherwise.

New files: `tests/unit/test_mock_*.py`, `tests/integration/test_mock_commands.py`.

## 16. Open Questions / Out of Scope (deferred)

- **Cross-transport reactions** (HTTP trigger → Kafka produce; Kafka trigger →
  HTTP callback). The trigger→reaction model admits this later without a rewrite;
  deferred per D4.
- **Stateful / scenario mocks** (sequences, "Nth call → Y", reactor behavior
  change after N messages). Deferred per D5; revisit for idempotency/retry tests.
- **Managed daemon** (`mock start/stop/status` with pidfile + control socket).
  Deferred per D6; add behind `--detach` only if PID-management friction shows up.
- **Record / replay** of real traffic into stubs. Deferred; static+templated
  stubs cover local testing.
- **Nested-field / header capture** in templates (currently top-level keys only).
- **Exactly-once reactor delivery** and reaction retry/backoff on broker errors.
- **Schema Registry / Avro/Protobuf decoding** for reactor `match` (inherited
  limitation, ARCHITECTURE §15 — Kafka values are raw JSON).
- **Multiple HTTP servers / ports.**

## 17. Rejected Alternatives (ADR-style)

- **Reimplement a Kafka wire-protocol broker in-process.** Rejected (D2):
  impractically large and error-prone; the real broker is already in the SUT's
  local environment, so impersonating a broker solves a problem that does not
  exist. Reacting as a consumer closes the actual gap.
- **Managed daemon** (`mock start/stop/status`). Rejected for the MVP (D6): it
  reintroduces on-disk state and a CLI↔daemon IPC protocol the architecture
  deliberately avoids; the foreground loop is proven by `http ping`.
- **Cross-transport unified engine from the start.** Rejected for the MVP (D4):
  YAGNI; within-transport covers integration mocking; deferred without locking it
  out.
- **Stateful / scenario mocks.** Rejected for the MVP (D5): a stateless engine is
  deterministic and testable and matches `agctl`'s ethos.
- **HTTP framework** (FastAPI/uvicorn/starlette). Rejected (D3): stdlib suffices
  for mocking and avoids a new dependency, preserving packaging minimalism.
- **Record / replay.** Rejected: YAGNI; templated stubs cover local testing with
  less machinery.
- **Client-side faking** (replace `agctl`'s own HTTP/Kafka clients with fakes).
  Rejected (D1): covers only `agctl`'s calls, not the SUT's; does not close the
  gap. (`agctl` already fakes its own clients in its test suite via testcontainers
  and injected transports.)

## 18. Docs & Skill Impact

- **DESIGN.md** — **reverse the §1 mocking non-goal** (it is a §1 non-goal, not a
  §10 deferred item — reframe it as "now supported, see §3"); add a `mocks` schema
  subsection to §2; add an `agctl mock` subsection to §3 (command surface,
  lifecycle, NDJSON); add `mock.run` streaming output to §4 (alongside the
  `http ping` exception); add the new deferred items to §10 (cross-transport
  reactions, stateful/scenario mocks, managed daemon).
- **ARCHITECTURE.md** — §3 module map adds `commands/mock_commands.py` and the
  `mock/` subpackage; §4 notes `mock run` bypasses `@envelope` (second exception,
  like `http ping`); §5 documents the `mocks` config; §6 records the second
  streaming exception; §8 documents the new `KafkaClient` committed-consume
  method; §12 adds the new test files/seams; §15 updates limitations.
- **`skills/agctl-config`** — add a `reference/mocks.md` and surface mock/stub/
  reactor authoring (incl. capture/templating and idempotent-reaction guidance)
  in `SKILL.md`.
- **`skills/agctl/SKILL.md`** — add `agctl mock run` usage and the
  background-with-`&`/`kill` lifecycle pattern.

Doc sync is performed via the `docs-watcher` subagent after implementation
(CLAUDE.md "Docs Sync").

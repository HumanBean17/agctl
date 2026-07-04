# Design: Mock Server — `agctl mock`

**Status:** Approved (design, revised) — ready for implementation plan
**Date:** 2026-07-03
**Revised:** 2026-07-03 — applied fan-out spec-review findings (capture-value coercion; single-writer NDJSON emission; explicit startup broker probe; unique default `consumer_group` + resume hazard; `0.0.0.0` default bind; HTTP/1.1 + `Content-Length` + chunked-body handling; reactor-owned consumer lifecycle; `--fail-fast`; dependency-direction / module-location fixes; explicit "Known-wrong-result / not covered" section).
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

> **Scope honesty (load-bearing).** This MVP covers *stateless, single-consumer,
> value-keyed, plaintext* flows. Several common enterprise patterns are **not**
> covered, and — critically — for those the mock tends toward a *plausible-but-
> wrong (false-green)* result rather than a clear failure. Those patterns are
> enumerated explicitly in §16.2 so a user is never misled into trusting a green
> test that never exercised the integration. This is the fail-loudly principle
> (DESIGN §1) applied to the spec itself.

## 2. Goals

- Let an agent impersonate the SUT's external **HTTP** and **Kafka**
  integrations during local testing, with zero external tooling.
- Reuse existing matchers and substitution (`resolution.py` `{placeholder}`,
  `assertions.py::json_subset`, the jq engine, `KafkaClient.produce`) rather than
  reinventing them.
- Stay consistent with the stateless principle (ARCHITECTURE §2 / DESIGN §8): a
  foreground, killable process; no daemon, no pidfiles, no on-disk state.
- Stay consistent with **fail loudly**: every mock interaction is observable via
  a streaming event line; unmatched HTTP requests and unmatched/skipped Kafka
  messages never pass silently.
- Keep the engine **stateless across calls** (no cross-call rule state) so
  behavior is deterministic and trivially unit-testable.
- **Document, not hide**, the patterns the MVP does not cover (§16.2), so a user
  never gets a false green from an unmocked flow.

## 3. Scope & Design Constraints

These constrain the whole design and are load-bearing for several decisions.

- **SUT-facing.** The mock is a real network endpoint (HTTP server) or a real
  consumer (Kafka) that the SUT's own clients talk to. It is *not* a client-side
  fake of `agctl`'s own calls — that would only cover `agctl`, not the SUT.
- **Pointing the SUT at the mock is the operator's job.** `agctl` does not
  configure or launch the SUT; the operator sets the SUT's HTTP base URL to the
  mock's `listen` and its broker to `kafka.brokers`. (Stated explicitly per
  review — prevents an implementer over-building SUT wiring.)
- **`agctl` is the sole reactor on a topic.** Where a topic has a real second
  consumer that also reacts, partition-ownership/dueling-replier conflicts are
  **out of scope**. Because this is *asserted, not enforced*, the default
  `consumer_group` is generated **unique per run** (§8.3); pinning a stable group
  is opt-in and carries a documented hazard (§16.2).
- **Foreground process.** `mock run` blocks and is backgrounded by the agent with
  `&` / killed (the heartbeat pattern, DESIGN §3.1 / UC-3). A managed daemon is a
  deferred additive extension (§9, §16.1).
- **Within-transport reactions only.** An HTTP trigger yields an HTTP response; a
  Kafka trigger yields a Kafka produce. Cross-transport reactions are a hard
  MVP limitation (§16.2), not merely deferred.
- **Static + request-templated.** Reactions may interpolate values captured from
  the triggering request/message, but carry no state across calls (D5).
- **Plaintext HTTP only.** The HTTP mock binds a plaintext socket (default
  `0.0.0.0`). TLS is a non-goal for the MVP; SUT clients that pin certs or
  hardcode `https://` cannot be redirected here (§16.2).

## 4. Non-Goals

- **Reimplementing the Kafka wire protocol / a broker.** The SUT's real broker is
  used; `agctl` only consumes and reacts.
- **TLS / HTTPS mock.** Plaintext only; cert-pinned / `https://`-hardcoded SUT
  clients cannot connect (documented limitation §16.2).
- **Cross-transport reactions / sagas.** No Kafka-trigger→HTTP-callback
  causality (hard limitation §16.2).
- **Stateful / scenario mocks** — sequences, counters, "Nth call returns Y",
  OAuth token exchange, create-then-get lifecycle, idempotency-key replay,
  pagination cursors, 429-then-retry (documented limitation §16.2).
- **Record / replay** of real traffic into stubs (deferred §16.1).
- **A managed daemon** (`mock start/stop/status` with pidfile + control socket)
  in the MVP (deferred §16.1).
- **Multiple HTTP servers / ports.** One server, many stubs (path-routed).
- **Exactly-once reactor delivery.** At-least-once with idempotent reactions is
  the contract (§8.3); exactly-once is out of scope.
- **Adversarial containment.** The mock binds a plaintext dev socket; it is a
  local-test convenience, not a security boundary.
- **New protocols** (gRPC, etc.). HTTP and Kafka only.

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | **A mock is a SUT-facing trigger→reaction rule.** Trigger = HTTP request matcher or Kafka message matcher; reaction = HTTP response or Kafka produce. | The real application's clients connect to the mock. Client-side faking would only cover `agctl`'s own calls and would not close the local-testing gap. |
| D2 | **No Kafka broker; `agctl` reacts as a committed consumer** on the SUT's real broker (reusing top-level `kafka.brokers` / `kafka.ssl`). | Reimplementing the Kafka wire protocol is impractical. The real broker is already part of the SUT's local environment; the missing piece is the downstream consumer, which `agctl` plays. |
| D3 | **HTTP mock = an embedded server on the Python standard library** (`http.server`), no new dependency. | A mock HTTP server is a small, well-understood problem; stdlib suffices and preserves packaging minimalism. |
| D4 | **Within-transport reactions only.** | Cross-transport covers comparatively rare shapes; it is a hard MVP limitation (§16.2), not merely deferred. |
| D5 | **Static + request-templated richness; no cross-call state.** Reactions interpolate captured values; the engine is otherwise stateless. *(Revised:)* this covers **stateless, single-consumer, value-keyed** flows only — **not** "~90% of integration mocking." Stateful/correlated/non-JSON/cross-transport flows produce plausible-but-wrong results and are enumerated in §16.2. | A stateless engine is deterministic and testable and matches `agctl`'s ethos. The earlier "90%" claim overstated coverage for enterprise; honesty here prevents false greens. |
| D6 | **Foreground long-running command** (`agctl mock run`), not a managed daemon. | Consistent with the `http ping` precedent and the statelessness principle. A daemon is an additive later option (§16.1). |
| D7 | **NDJSON streaming output** — the second sanctioned exception to one-emit-per-invocation. | A live mock must surface traffic/events as they happen; mirrors `http ping`. Startup errors still emit one structured envelope before any line. |
| D8 | **Runtime-error policy: continue + emit + escalate at shutdown, with a `--fail-fast` opt-out.** A reaction produce failure emits a `kafka.error` line and (by default) the reactor keeps running; `--fail-fast` instead exits non-zero on the first runtime error. *(Revised:)* failures are **not** made silent — the agent failure-stream protocol (§10.1) is the load-bearing way a backgrounded mock's misbehavior is detected. | A mock should not die on one bad message mid-test, but a test harness whose worst failure mode is the silent false-positive must offer a fail-fast path and prescribe how to consume the stream. |
| D9 | **Reuse existing machinery** (`{placeholder}` substitution, `json_subset`, jq, `KafkaClient.produce`) and add **exactly one** new client surface — a committed consume loop on `KafkaClient` (§8.3). | No reinvention. The committed loop is genuinely new because existing `consume_window` re-seeks by time and ignores offsets. |
| D10 | **Default HTTP bind `0.0.0.0`.** | A containerized SUT (docker-compose — the dominant enterprise local-test topology) cannot reach the host's `127.0.0.1`. `0.0.0.0` makes the mock reachable via `host.docker.internal` (Mac/Win) or the host LAN IP (Linux). Mitigated by documenting it is a dev-only plaintext mock (§3, §16.2). |
| D11 | **Captured non-string values are stringified** before `{placeholder}` substitution. | `fill_placeholders` is typed `dict[str,str]` and `re.sub` requires its callable to return `str`; a numeric `orderId` (the most common capture) would otherwise raise `TypeError`. JSON-type pass-through is therefore **not** supported (§7.3, §16.2). |
| D12 | **NDJSON emission is single-writer** (a `threading.Lock` around the write-newline-flush sequence, or one dedicated writer thread all reactors/handlers enqueue onto). | The HTTP thread + N reactor threads emit concurrently; unlocked writes interleave into invalid NDJSON lines (a review blocker). |
| D13 | **Each reactor thread owns its consumer's lifecycle** (polls with a short timeout, exits on `stop`, closes its own consumer); `MockEngine` only `join()`s threads. | confluent-kafka `Consumer` is not thread-safe; closing it from the signal-handling main thread while a reactor is inside `poll()` races/deadlocks. |

## 6. Command Contract — `agctl mock run`

```
agctl mock run
    [--config <path>]            # auto-discovered by default
    [--http-listen <host:port>]  # literal string (NO ${} interpolation); overrides mocks.http.listen
    [--only http|kafka]          # run a single engine (HTTP-only needs no kafka extra)
    [--fail-fast]                # exit non-zero on the FIRST runtime error (default: continue + summarize)
    [--duration <seconds>]       # stop after N s; mutually exclusive with --until-stopped
    [--until-stopped]            # default: run until SIGTERM/SIGINT
```

- `--http-listen` is a **literal** `host:port` string — CLI args are not `${}`-interpolated (only YAML values are).
- `--only` restricts to one engine; the `kafka`-extra / `kafka.brokers` checks are gated on the engines actually being started (an `--only http` run is not blocked by a declared-but-unused `mocks.kafka`).
- `--fail-fast` flips D8 to exit `1` on the first `kafka.error` / unrecoverable runtime error (useful for synchronous `--duration` foreground runs).
- **No `mocks` section** → emit `started` + `summary` with zero counts and exit `0` (idempotent no-op). **`--only <engine>` with that engine absent** → `ConfigError` (exit 2).
- **Lifecycle hazard (documented):** when backgrounding, wrap with `nohup`/`setsid` so the mock is not killed by `SIGHUP` when the launching shell exits, and capture the PID. At startup, **refuse to bind if the port is already in use** (emit a `ConfigError` envelope with a hint to kill the stale mock) — this prevents a silent stale mock from serving the SUT.

`mock run` is **not** wrapped by `@envelope`; it hand-reimplements the try/except → emit + exit shape, exactly as `http ping` does (ARCHITECTURE §4 / §6). It is the second command to use the streaming-output exception.

## 7. Config Schema — new `mocks:` section

A new optional top-level section, additive and backward-compatible (existing
configs without `mocks` are unaffected). **No `version` bump** — additive under
major `1` (ARCHITECTURE §5 version guard). Conventions match existing sections:
`${ENV}` interpolation at load, `{placeholder}` at match/react time, jq predicates
for Kafka.

> **Module location (per review).** The Pydantic models for `mocks` live in
> **`agctl/config/models.py`** alongside every other section model
> (`KafkaConfig`, `DatabaseConfig`, `HttpTemplate`, …) — *not* in `agctl/mock/`.
> This preserves config's dependency isolation (`config/* → {errors}` only;
> ARCHITECTURE §3) and the one-place convention. The `mock/` subpackage holds
> only runtime engine/server/reactor code.

### 7.1 Config contract (YAML)

```yaml
mocks:
  # ── HTTP mock server: agctl serves these; the SUT points its outbound HTTP
  #    client at `listen`. One server, many stubs (path-routed).
  http:
    listen: "${AGCTL_MOCK_HTTP_HOST:-0.0.0.0}:${AGCTL_MOCK_HTTP_PORT:-18080}"
    stubs:
      create-order:
        description: "Mock the downstream order API"   # optional; missing → validate warning (§12)
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
        delay_ms: 0                    # optional latency simulation (concurrency-capped, §8.1)

      get-order:
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
        consumer_group: agctl-mock-order-handler   # OPTIONAL; omit → unique per-run group (§8.3)
        match: '.command == "CREATE_ORDER"'   # jq predicate; optional (omit = match all)
        reaction:
          topic: orders.events             # produce reply here
          key: "{orderId}"                 # templated from the matched message value
          value:
            eventType: "ORDER_CREATED"
            orderId: "{orderId}"
            status: "PENDING"
```

### 7.2 Pydantic model shape (`agctl/config/models.py`)

| Model | Fields |
|---|---|
| `MocksConfig` | `http: HttpMockConfig \| None`, `kafka: KafkaMockConfig \| None` |
| `HttpMockConfig` | `listen: str` (host:port; supports `${ENV}`), `stubs: dict[str, HttpStub]` |
| `HttpStub` | `description: str \| None = None`, `method: str` (upper-cased), `path: str` (with `{name}` segments), `match: HttpMatch \| None`, `response: HttpResponse`, `delay_ms: int = 0` |
| `HttpMatch` | `body: dict \| None` (subset filter) |
| `HttpResponse` | `status: int` (constrained `100–599`), `headers: dict \| None`, `body: Any \| None` (`None` → empty response body) |
| `KafkaMockConfig` | `reactors: dict[str, KafkaReactor]` |
| `KafkaReactor` | `description: str \| None = None`, `topic: str`, `consumer_group: str \| None` (omit → generated unique per run), `match: str \| None` (jq predicate), `reaction: KafkaReaction` |
| `KafkaReaction` | `topic: str`, `key: str \| None`, `value: Any` (must be JSON-serializable; reuses `KafkaClient.produce`), `headers: dict \| None` (values UTF-8-encoded; non-string values → error) |

- `description` is **optional** (`str | None = None`) on `HttpStub`/`KafkaReactor` — matching the existing convention (`HttpTemplate`, `KafkaPattern`, `DatabaseTemplate`) so the §12 warning path is reachable; absence is never a hard schema error.
- `method` is upper-cased at load (mirrors `KafkaSSL.security_protocol` normalization) and accepts any string (a mock may serve any verb).
- **`listen` parsing:** split with `rsplit(":", 1)`; IPv6 hosts must be bracketed (`[::1]:18080`); an empty or non-numeric port, or an unparseable value, is a `ConfigError` (exit 2) at startup.

### 7.3 Capture / templating model (symmetric across transports)

Both transports resolve reaction templates via the existing `{placeholder}`
substitution in `resolution.py`, against a capture context built from the trigger:

- **HTTP** — context = path params (from `{name}` segments) ∪ top-level keys of
  the JSON request body (**only when the body parses as a JSON object**).
  `match.body` is a *filter* (the subset must be present for the stub to match);
  it is not a capture source. When the body does not parse as a JSON object, the
  stub may still match on method+path but no body keys are captured (§8.1 emits a
  degraded-match signal if the response then has unresolved placeholders).
- **Kafka** — context = top-level keys of the matched message's parsed JSON
  value (**only when the value is a JSON object**; arrays/scalars yield no
  captures — §8.2).

**Capture value coercion (D11):** captured values are frequently non-string
(numeric IDs, bools). Before `{placeholder}` substitution, every captured value
is stringified via `str()`: numbers → decimal form, `True`/`False` →
`"True"`/`"False"`, `None` → `"None"`, lists/dicts → their `str()`. **JSON-type
pass-through is not supported** — `{placeholder}` lives inside strings, so a
reaction field always receives a string (e.g. `orderId` becomes `"42"`, never the
int `42`). Reactions must therefore be type-tolerant (§16.2).

Unsupplied `{placeholder}` values are left literal — the inherited limitation
already documented for HTTP templates (ARCHITECTURE §15 / DESIGN §10 deferred).
Capturing **nested** fields and request **headers** is deferred (§16.1) — note
this means header-borne correlation IDs are not mockable in the MVP (§16.2).

## 8. Trigger → Reaction Engine

Two focused engines under `agctl/mock/`, sharing the trigger→reaction model.

### 8.1 HTTP server (`agctl/mock/http_server.py`)

A stdlib `ThreadingHTTPServer` whose handler delegates to `MockEngine.handle_http`.

**HTTP/1.1 + Content-Length (per review).** The handler pins
`protocol_version = "HTTP/1.1"` and always sends `Content-Length` (otherwise
stdlib defaults to HTTP/1.0 and closes the connection per response, defeating the
SUT client's connection pool and changing timing/ordering).

**Match contract** — a stub matches iff, in **YAML mapping order (first match
wins; `dict` insertion order is relied upon — Pydantic v2 preserves it)**:
1. `method` equals the request method (case-insensitive), **and**
2. `path` matches the request path per the path-template contract
   (`agctl/mock/routing.py`): the **query string is stripped before matching**
   (`urlsplit(request.path).path`); literal segments match exactly and each
   `{name}` segment captures a single path segment; **trailing slash is
   significant** (`/orders` ≠ `/orders/`), **and**
3. if `match.body` is present, `json_subset(match.body, request_body)` is true.

**Reaction** — render `response.body` / `response.headers` via `{placeholder}`
against the capture context (§7.3); sleep `delay_ms`; return `status` + headers +
body; emit `http.hit`. **Response Content-Type default (per review):** when
`body` is a `dict`/`list`, it is JSON-encoded and `Content-Type` defaults to
`application/json` if not set explicitly; a scalar/`None` body defaults to
`text/plain`. Explicit `headers` always win. **No match** → `404` with body
`{"mock_error":"no matching stub"}` and an `http.unmatched` line.

**Chunked request bodies (per review).** `BaseHTTPRequestHandler` does not
de-chunk `Transfer-Encoding: chunked` request bodies (emitted by httpx/requests
when streaming or body length is unknown). The handler must de-chunk before
`match.body`/capture, **or** this is documented as a known limitation (a chunked
POST would otherwise 404 falsely).

**`delay_ms` concurrency cap (per review).** `ThreadingMixIn` has no thread cap;
delayed requests would occupy unbounded threads. Handling is gated by a
`threading.Semaphore` (configurable ceiling, default e.g. 64); overflow above the
cap fails fast (429 or immediately-served) rather than spawning unbounded threads.

### 8.2 Kafka reactor (`agctl/mock/kafka_reactor.py`)

One thread per reactor. **Match contract** — a message is handled iff
`reactor.match` is omitted (match-all) **or** `jq_bool(message_value, match)` is
true. *(Argument order per review: `jq_bool(value, expr)` — value first, jq
expression second.)* The reactor feeds the **normalized** parsed value (not raw
bytes) through `jq_bool`; a jq error on a malformed value is a **no-match**
(inherited safe behavior, ARCHITECTURE §9). Non-matching messages are committed
and skipped (like `kafka consume --match`).

**Non-JSON / non-object values (per review).** If the message value is not a JSON
object (Avro/Protobuf/text, or an array/scalar), no capture context can be built
and the message does not match — the reactor emits a distinct **`kafka.skipped`**
line (not `kafka.error`) so the stream surfaces "saw N messages, matched none"
rather than looking idle. This makes Schema-Registry-backed topics visibly
un-mockable instead of silently idle (§16.2).

**Reaction** — render `reaction.value` / `key` / `headers` via `{placeholder}`
against the message-value context (§7.3); produce via `KafkaClient.produce`
(reused unchanged; `value` must be JSON-serializable); emit `kafka.reacted`.

### 8.3 The one new client surface — committed consume loop

`KafkaClient` gains a committed consume primitive (e.g. `consume_loop` /
`poll_and_commit`) that the reactor drives: poll → invoke a callback per message
→ commit. It is distinct from `consume_window`, which re-seeks by time and ignores
committed offsets. Contract:

- **At-least-once.** A message may be re-delivered after a crash/restart if the
  commit did not land; reactions must be **idempotent** — e.g. derive the reaction
  key from the message key so a duplicate produce is a no-op for an idempotent
  downstream consumer, or embed a stable idempotency id in the value.
- **Commit only on success (per review).** The offset is committed only after the
  reaction succeeds; on a reaction failure the partition is **not** advanced past
  the message (bounded in-place retry, then — if still failing — a `kafka.error`
  line carrying the `offset`/`partition`, under D8). This avoids silently dropping
  poison messages (the false-green vector the review flagged); a skipped offset is
  always visible in the stream and summary.
- **Reactor owns its consumer (D13).** Each reactor thread polls with a short
  timeout in a loop, observes `stop`, then closes its own consumer; `MockEngine`
  only `join()`s. (confluent-kafka `Consumer` is not thread-safe — closing from
  the main signal thread while a reactor is in `poll()` is unsafe.)
- **Startup connectivity probe (per review).** confluent-kafka `Consumer`
  construction is lazy (no TCP until poll/subscribe). To make the §11 "broker
  unreachable at startup → exit 2" guarantee real, startup performs an explicit
  probe (e.g. `consumer.list_groups(timeout=…)` / `list_topics`) inside the
  fail-fast window.
- **Rebalance handling.** A rebalance callback (`on_assign`/`on_revoke`) is
  registered; a rebalance observed at runtime emits a warning line (a second
  consumer in the group is the §3/§16.2 hazard).
- **Consumer-group default & hazard (per review).** `consumer_group` is optional.
  If omitted, `agctl` generates a **unique per-run** group (so a restart starts
  fresh and never silently resumes past messages produced between runs). If
  pinned in config, resume semantics apply — documented as a hazard (§16.2):
  messages produced while the mock is down are never reacted to.

**Wiring (dependency direction, per review).** The `mock/` subpackage **never
imports** from `agctl/commands/` (ARCHITECTURE §3 forbids back-edges into the
command layer). `agctl/commands/mock_commands.py` (command layer) constructs the
`KafkaClient` via the existing `new_kafka_client(cfg.kafka)` / `_kafka_ssl_conf`
and **injects** it into the reactor; the reactor depends only on the `KafkaClient`
contract (plus `consumer_factory`/`producer_factory` seams for tests).

## 9. Runtime & Lifecycle (`agctl/mock/engine.py`)

`MockEngine` owns the HTTP server thread, one thread per Kafka reactor, a shared
`stop` `Event`, and `SIGTERM`/`SIGINT` handlers (the `http ping` precedent).

- **Single-writer emission (D12).** All NDJSON lines go through one locked
  emit path (write + newline + flush under one `threading.Lock`), or a single
  dedicated writer thread that all handlers/reactors enqueue onto. This keeps
  concurrent lines intact.
- **Start** — **construct + probe all engines first** (consumers probe brokers),
  **then bind + serve HTTP last**; on any startup failure, tear down
  (`server.shutdown()`/`server_close()`, close probed consumers) so no listening
  socket or non-daemon thread leaks. Emit `started`.
- **Run** — serve HTTP; each reactor polls/matches/reacts/commits; every event
  emits one NDJSON line (§10). `--duration` arms a timer that sets `stop`.
- **Shutdown** (`SIGTERM`/`SIGINT` or `--duration`) — stop accepting HTTP; finish
  in-flight requests; each reactor closes its own consumer (D13); flush producers;
  commit final offsets; emit `summary`; exit `0` (clean, no runtime errors) or `1`
  (clean but runtime errors occurred, D8). `--fail-fast` instead exits `1`
  immediately on the first runtime error.

**Stateless, precisely (per review).** "Stateless" means **no reaction depends on
a prior request and nothing is written to disk**. In-memory runtime state *does*
exist (threads, the listening socket, summary counters, broker-side committed
offsets under the consumer group) — these are process/protocol state, not
cross-call rule state. Summary counters are best-effort under concurrent requests
(a lock is optional; the race only affects final counts, never matching).

## 10. Output Contract (NDJSON — second streaming exception)

`mock run` emits one JSON object per line via the single-writer path (D12).
Timestamps are ISO-8601 Z (matching `http ping`).

```json
{"event":"started","http":{"listen":"0.0.0.0:18080","stubs":2},"kafka":{"reactors":[{"name":"order-command-handler","topic":"orders.commands","consumer_group":"agctl-mock-order-handler-<runid>"}]},"timestamp":"…"}
{"event":"http.hit","stub":"create-order","method":"POST","path":"/api/v1/orders","status":201,"duration_ms":3,"timestamp":"…"}
{"event":"http.unmatched","method":"GET","path":"/api/v1/unknown","status":404,"timestamp":"…"}
{"event":"http.body_parse_skipped","stub":"oauth-token","method":"POST","path":"/oauth/token","reason":"non-JSON body; response has unresolved placeholders","timestamp":"…"}
{"event":"kafka.reacted","reactor":"order-command-handler","topic":"orders.events","key":"ord-789","duration_ms":1,"timestamp":"…"}
{"event":"kafka.skipped","reactor":"order-command-handler","topic":"orders.commands","reason":"non-object message value","count":3,"timestamp":"…"}
{"event":"kafka.error","reactor":"order-command-handler","topic":"orders.commands","offset":1043,"partition":2,"error":"…","fatal":false,"timestamp":"…"}
{"event":"summary","http_hits":7,"http_unmatched":1,"http_body_parse_skipped":0,"kafka_reactions":3,"kafka_skipped":3,"kafka_errors":0,"duration_ms":45213}
```

As with `http ping`, all machine-readable output is on stdout; diagnostics only
on stderr. Startup failures emit **one** structured envelope (the §4.1 shape with
`command: "mock.run"`) **before** any event line, then exit 2.

### 10.1 Agent failure-stream protocol (load-bearing — per review)

The mock's failure signals (`http.unmatched`, `http.body_parse_skipped`,
`kafka.skipped`, `kafka.error`) live **only** on stdout, and the exit-1-at-
shutdown escalation arrives only on a clean `SIGTERM`. The background `&`/`kill`
pattern loses both by default. The spec therefore **prescribes** the protocol an
agent must follow (to be encoded in the `agctl-config` skill, §18):

1. Redirect the mock's stdout to a log file: `agctl mock run > mock.log 2>&1 &`.
2. **Poll** `mock.log` for the `started` line before running the SUT (do not
   sleep a fixed delay).
3. Terminate with `SIGTERM` and `wait` — **never `SIGKILL`** (which skips the
   shutdown handler, the summary line, and the exit code).
4. After the test, **grep the log for `http.unmatched` / `http.body_parse_skipped`
   / `kafka.skipped` / `kafka.error` regardless of the test result**, and treat
   any hit as a failure.

Without this, "fail loudly" is aspirational. `--fail-fast` (§6) is the
synchronous foreground alternative for `--duration` runs.

## 11. Error & Exit-Code Model

No new error types; the existing `AgctlError` hierarchy covers every path.

| Failure | Type | Exit | When |
|---|---|---|---|
| Bad `mocks:` schema / unresolved `${ENV}` / version mismatch | `ConfigError` | 2 | startup, before serving (one envelope) |
| HTTP bind failure — port in use | `ConfigError` | 2 | startup (refuse-to-bind; hint to kill stale mock, §6) |
| `mocks.kafka` present but no top-level `kafka.brokers` | `ConfigError` | 2 | startup (**runtime guard in `mock run`, not `validate_config`** — see §12) |
| Broker unreachable at startup | `ConnectionFailure` | 2 | startup — **made enforceable via the explicit probe (§8.3)** |
| `mock run` with reactors starting but the `kafka` extra missing | `ConfigError` | 2 | startup (lazy-import → install hint); gated on engines actually started (`--only`) |
| `--only <engine>` with that engine absent | `ConfigError` | 2 | startup |
| Runtime reaction produce failure (default) | `kafka.error` line, reactor continues | — | runtime (D8) |
| Runtime reaction failure under `--fail-fast` | `kafka.error` line | 1 | runtime (immediate exit) |
| Reactor consumer dies after `started` | `kafka.error` line, `fatal:true`, that reactor stops, others continue | (exit 1 at shutdown) | runtime |
| Unmatched HTTP request | HTTP 404 + `http.unmatched` line | — | runtime (not an error) |
| No `mocks` section configured | `started` + `summary`, zero counts | 0 | startup (idempotent no-op) |
| Clean shutdown, no runtime errors | `summary` line | 0 | shutdown |
| Clean shutdown, runtime errors occurred | `summary` line | 1 | shutdown |

## 12. Validation Rules (`agctl/config/validator.py`)

1. **`mocks.kafka` requires `kafka.brokers`.** If any reactor is configured but
   the top-level `kafka` block is absent or has no brokers → config validation
   error (exit 2). **Honesty note (per review):** this runs only in
   `agctl config validate` — the per-command load path does **not** run
   `validate_config` (ARCHITECTURE §5). So `mock run` performs its own equivalent
   runtime guard at startup (mirroring how `db execute` does its own write-safety
   gates); the §11 row is that runtime guard, not the cross-ref.
2. **Missing `description` on any stub or reactor → warning** (reachable because
   the field is optional, §7.2).
3. **Path-template shadowing warning (per review).** Warn when two stubs' path
   templates are ambiguous — e.g. a literal segment in one overlaps a `{name}`
   segment in another at the same position (`/orders/bulk` shadowed by
   `/orders/{order_id}` defined earlier) — so first-match-wins does not silently
   misroute.

Topics are free-form (as they already are for `kafka.patterns`), so no
topic-existence cross-ref is added.

## 13. Discovery Changes (`agctl/commands/discover_commands.py`)

A new **`mocks`** category (one category, items carry a `type` discriminator).
Purely additive. (Lowest-value MVP slice — may be deferred to shrink the MVP
without blocking the core goal; confirm at plan time.)

- **Level 0 summary** adds `http_stubs` and `kafka_reactors` counts.
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
  the install hint (existing lazy-import pattern, ARCHITECTURE §8); gated on
  reactors actually starting.
- **No `pyproject.toml` dependency changes, no new entry-point group, no `version`
  bump.** `mock` is a built-in command group registered in `cli.py` like
  `http`/`kafka`/`db` — not a plugin.

## 15. Testing Strategy

Mirrors the existing test architecture and seams (ARCHITECTURE §12).

**Unit (no network):**
- `routing` path-template → param extraction (pure function); query-string
  stripping; trailing-slash significance.
- HTTP stub matching and response templating; **capture value coercion**
  (non-string → `str()`); Content-Type defaulting; with an injectable
  `handler_factory`.
- Kafka reactor match (`jq_bool(message_value, match)` — correct arg order) +
  reaction templating; **non-object value → `kafka.skipped`**; with injected
  `consumer_factory`/`producer_factory` fakes.
- `MockEngine` start/stop; **single-writer emission under concurrency** (no
  interleaved lines); **startup probe-then-bind ordering** (HTTP not left bound on
  reactor failure); **reactor self-close** (D13); `--fail-fast`.
- Pydantic model validation (`status` 100–599, optional `description`, `listen`
  parsing incl. IPv6/empty-port).
- Config validation: `mocks.kafka` requires `kafka.brokers`; missing-`description`
  warning; path-template shadowing warning.

**HTTP integration (no Docker):** real `ThreadingHTTPServer` on port `0`, driven
with `httpx`; assert **HTTP/1.1 + Content-Length**, JSON Content-Type defaulting,
`delay_ms` under the concurrency cap, unmatched-404, **chunked request body**
handling, and NDJSON lines.

**Kafka integration (self-skipping):** under `AGCTL_TEST_LIVE=1`, reuse the
existing testcontainers Kafka harness; feed messages via a producer, assert a
templated reaction is produced, **offsets commit on success only**, and a
**unique default `consumer_group`** per run. Self-skips otherwise.

New files: `tests/unit/test_mock_*.py`, `tests/integration/test_mock_commands.py`.

## 16. Deferred & Not-Covered

### 16.1 Deferred (future enhancements)

- **Cross-transport reactions** (HTTP trigger → Kafka produce; Kafka trigger →
  HTTP callback). The trigger→reaction model admits this later without a rewrite.
- **Stateful / scenario mocks** (sequences, "Nth call → Y", reactor behavior
  change after N messages).
- **Managed daemon** (`mock start/stop/status` with pidfile + control socket),
  behind `--detach`.
- **Record / replay** of real traffic into stubs.
- **Nested-field / header capture** in templates (currently top-level keys only).
- **Exactly-once reactor delivery** and reaction retry/backoff on broker errors.
- **Schema Registry / Avro/Protobuf decoding** for reactor `match` (inherited
  limitation, ARCHITECTURE §15).
- **TLS / HTTPS mock** (cert-pinned SUT clients).
- **Multiple HTTP servers / ports.**

### 16.2 Known-wrong-result / Not Covered (MVP limitations)

These patterns are **not** mocked by the MVP. Critically, for each the failure
mode tends toward a **plausible-but-wrong (false-green) result, not a clear
failure** — so they must be documented, not silently omitted. Use an external
tool (WireMock/LocalStack) or wait for the deferred feature for these:

- **Stateful flows** — OAuth/token exchange, create-then-GET (201→200),
  idempotency-key replay, pagination cursors, 429-then-retry. The static engine
  returns the same canned response regardless of prior calls → state-propagation
  and dedupe logic go untested.
- **TLS / HTTPS-pinned or `https://`-hardcoded SUT clients.** Cannot connect to a
  plaintext mock at all → the integration is untested (payments/auth/healthcare
  integrations are disproportionately affected).
- **Cross-transport sagas** (Kafka trigger → HTTP callback). No causal linkage;
  requires manual orchestration (`agctl kafka produce` + HTTP mock separately).
- **Header-borne correlation IDs** (sync Kafka request/reply keyed by a header;
  CloudEvents `ce-*`, `traceparent`). The reactor cannot capture from headers, so
  it cannot produce a correlatable reply → routes through the SUT's reply-timeout
  fallback.
- **Non-JSON Kafka values** (Avro/Protobuf/schema-registry-backed topics).
  Emitted as `kafka.skipped` (visible), but the topic is effectively un-mockable
  until decoding lands.
- **JSON-type pass-through in reactions.** Captured values are stringified (D11);
  a strict downstream expecting a numeric field may reject the reaction.
- **Containerized SUT topology** now works by default (`0.0.0.0` bind, D10), but
  the operator must still target `host.docker.internal` / host LAN IP and avoid a
  SUT that swallows connection errors (which can still false-green).
- **Shared broker + pinned `consumer_group` reused across runs/devs.** Partition
  split or resume-past-messages → silently missing/old reactions. Mitigated by
  the unique-per-run default (§8.3); pinning carries this hazard.

## 17. Rejected Alternatives (ADR-style)

- **Reimplement a Kafka wire-protocol broker in-process.** Rejected (D2):
  impractically large; the real broker is already in the SUT's local environment.
- **Managed daemon** (`mock start/stop/status`). Rejected for the MVP (D6):
  reintroduces on-disk state and CLI↔daemon IPC the architecture avoids.
- **Cross-transport unified engine from the start.** Rejected for the MVP (D4):
  deferred as a hard limitation (§16.2), not merely "rare."
- **Stateful / scenario mocks.** Rejected for the MVP (D5): a stateless engine is
  deterministic and testable; the uncovered flows are documented (§16.2).
- **HTTP framework** (FastAPI/uvicorn/starlette). Rejected (D3): stdlib suffices
  and avoids a new dependency.
- **TLS/HTTPS mock in the MVP.** Rejected: plaintext covers the common local
  case; cert-pinned clients are documented as not-covered (§16.2).
- **Record / replay.** Rejected: YAGNI; templated stubs cover local testing.
- **Client-side faking** of `agctl`'s own clients. Rejected (D1): covers only
  `agctl`, not the SUT.

## 18. Docs & Skill Impact

- **DESIGN.md** — **reverse the §1 mocking non-goal** (reframe as "now supported,
  see §3"); add a `mocks` schema subsection to §2; add an `agctl mock` subsection
  to §3 (command surface, lifecycle, NDJSON, `--fail-fast`); add `mock.run`
  streaming output to §4 (alongside the `http ping` exception); add the new
  deferred items + the **not-covered limitations list** to §10.
- **ARCHITECTURE.md** — §3 module map adds `commands/mock_commands.py` and the
  `mock/` subpackage (and notes models live in `config/models.py`); §4 notes
  `mock run` bypasses `@envelope` (second exception); §5 documents the `mocks`
  config + no version bump; §6 records the second streaming exception + the
  single-writer emission; §8 documents the new `KafkaClient` committed-consume
  method, the startup probe, and reactor-owned lifecycle; §12 adds the new test
  files/seams; §15 updates limitations (cross-link §16.2).
- **`skills/agctl-config`** — add `reference/mocks.md`; surface stub/reactor
  authoring (capture/templating + coercion, idempotent reactions), the
  **agent failure-stream protocol (§10.1)**, and the **not-covered list** so
  authors don't trust false greens.
- **`skills/agctl/SKILL.md`** — add `agctl mock run` usage, the `nohup`/log/
  `SIGTERM`+`wait`/grep lifecycle, and `--fail-fast`.

Doc sync is performed via the `docs-watcher` subagent after implementation
(CLAUDE.md "Docs Sync").

# Mock Server (`agctl mock`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add first-class HTTP + Kafka mock support to `agctl` so a local test session is self-contained — an embedded stdlib HTTP server serves stubbed responses, and committed Kafka consumers react to messages on the SUT's real broker — both streamed as NDJSON events via a new `agctl mock run` foreground command.

**Architecture:** A new `agctl/mock/` subpackage holds four focused runtime units — a pure path-router, an HTTP server (stdlib `ThreadingHTTPServer`), a Kafka reactor (one thread per reactor), and a `MockEngine` that owns lifecycle, single-writer NDJSON emission, and signal-driven shutdown. A new `agctl/commands/mock_commands.py` Click command constructs the clients and injects them into the engine, hand-reimplementing the `try/except → emit + exit` shape (the second sanctioned streaming exception after `http ping`). Pydantic models for the new `mocks:` config section live in `agctl/config/models.py` (preserving config's dependency isolation). The `KafkaClient` gains exactly one new committed-consume surface (`consume_loop` + a startup `probe`). Everything else reuses existing machinery: `{placeholder}` substitution (`resolution.py`), `json_subset`/`jq_bool` (`assertions.py`), and `KafkaClient.produce`.

**Tech Stack:** Python ≥3.11, Click, Pydantic v2, stdlib `http.server`/`socketserver`/`threading`/`signal` (HTTP mock — no new dependency), `confluent-kafka` + `jq` (Kafka reactors — the existing `kafka` extra).

## Global Constraints

(Every task's requirements implicitly include this section. Values copied verbatim from the approved spec `docs/superpowers/specs/active/2026-07-03-agctl-mock-server-design.md`.)

- **Python `>=3.11`**; no new runtime dependency. HTTP mock uses **stdlib `http.server` only**.
- Reactors reuse **`confluent-kafka` + `jq`** under the existing **`kafka` extra** (`pip install 'agctl[kafka]'`). A missing extra at `mock run` startup (when reactors will actually start) → **`ConfigError` (exit 2)** with the install hint — via the existing lazy-import convention, never a bare `ModuleNotFoundError`.
- **No `version` bump**; `mocks:` is additive under config major `1`. **No `pyproject.toml` dependency changes, no new entry-point group.** `mock` is a built-in command group registered in `cli.py` like `http`/`kafka`/`db` — not a plugin.
- **Dependency direction (ARCHITECTURE §3):** the `agctl/mock/` subpackage imports only from `{stdlib, agctl.errors, agctl.resolution, agctl.assertions, agctl.clients.kafka_client}` and the `agctl.config.models` *types* — it **MUST NOT import from `agctl.commands.*`**. The command layer constructs `KafkaClient` and injects it; `mock/` never reaches back into commands.
- **Pydantic models for `mocks` live in `agctl/config/models.py`** (not `mock/`), so `config/* → {errors}` isolation holds.
- **Exit codes:** `0` success/clean-shutdown, `1` fail-fast or runtime-errors-occurred-at-clean-shutdown, `2` config/startup error (all via the existing `AgctlError` hierarchy — no new error types).
- **Default HTTP bind `0.0.0.0`**, default port `18080`.
- **Captured non-string values are stringified via `str()`** before `{placeholder}` substitution (D11). Capture context is always `dict[str, str]`.
- **NDJSON emission is single-writer** — every event line passes through one `threading.Lock`-guarded write-newline-flush (D12).
- **Each reactor thread owns its consumer's lifecycle** (polls, closes its own consumer in `finally`); `MockEngine` only `join()`s threads (D13).
- **At-least-once reactor delivery; reactions must be idempotent.** Offset is committed **only after the reaction succeeds** (or after bounded retries exhaust + a visible `kafka.error` line — never a silent drop).
- **Within-transport reactions only** (HTTP trigger → HTTP response; Kafka trigger → Kafka produce). Cross-transport is a documented hard limitation, not implemented.
- **Plaintext HTTP only** (no TLS). The mock is a dev convenience, not a security boundary.
- **Conventions:** `${ENV}` interpolation at config load; `{placeholder}` at match/react time; jq predicates are value-first — `jq_bool(value, expr)`. Match the surrounding code's docstring/comment density and naming.

---

## File Structure

**Create:**
- `agctl/mock/__init__.py` — package marker (empty).
- `agctl/mock/routing.py` — pure path-template parse + match (no deps beyond stdlib + `resolution`'s placeholder-name regex).
- `agctl/mock/http_server.py` — `MockHTTPServer` (stdlib `ThreadingHTTPServer`) + request handler: stub matching, capture, templated reaction, HTTP/1.1+Content-Length, Content-Type defaulting, chunked de-chunk, `delay_ms` semaphore, emits `http.hit`/`http.unmatched`/`http.body_parse_skipped`.
- `agctl/mock/kafka_reactor.py` — `KafkaReactor`: one thread; build capture from message value, `jq_bool` match, templated reaction, `KafkaClient.produce`, emits `kafka.reacted`/`kafka.skipped`/`kafka.error`; owns its consumer via `consume_loop`.
- `agctl/mock/engine.py` — `MockEngine`: stop `Event`, single-writer `emit_event`, summary counters, HTTP server thread + reactor threads, `SIGTERM`/`SIGINT` handlers, probe-then-bind `start()`, `run()`, `shutdown()`.
- `agctl/commands/mock_commands.py` — `mock run` Click command + hand-rolled envelope + `parse_listen`-based overrides + runtime guards + `new_mock_engine`/`new_kafka_client` injection seams.
- `tests/unit/test_mock_routing.py`, `tests/unit/test_mock_http_server.py`, `tests/unit/test_mock_kafka_reactor.py`, `tests/unit/test_mock_engine.py`, `tests/unit/test_mock_commands.py`, `tests/unit/test_mock_models.py`.
- `tests/integration/test_mock_commands.py` — real `ThreadingHTTPServer` on port 0 (no Docker); self-skipping Kafka reactor test under `AGCTL_TEST_LIVE=1`.

**Modify:**
- `agctl/config/models.py` — add the 8 `mocks` models; add `mocks: MocksConfig | None = None` to `Config`; add `parse_listen` helper (config must own it so the model validator and the command-layer `--http-listen` override share one parser, and so `config/*` doesn't import `mock/`).
- `agctl/config/validator.py` — add `mocks.kafka`-requires-`kafka.brokers` cross-ref error; missing-`description` warnings for stubs/reactors; path-template shadowing warning.
- `agctl/clients/kafka_client.py` — add `consume_loop(...)` committed-consume primitive + `probe(...)` one-shot connectivity check (the single new client surface; both belong to the committed-consume feature per spec §8.3).
- `agctl/cli.py` — add the `mock` group + register `mock_run`.
- `tests/unit/test_validator.py` — add mock cross-ref + warning tests.

**Deferred (explicit follow-up, NOT in this plan):** spec §13 Discovery changes. The spec flags it as the lowest-value slice; it does not block the core HTTP+Kafka mock goal. Add a separate plan when discovery of mocks is wanted.

---

## Task 1: Config models for the `mocks:` section

**Files:**
- Modify: `agctl/config/models.py`
- Test: `tests/unit/test_mock_models.py` (Create)

**Interfaces:**
- Consumes: existing `Config`/`BaseModel`/`Field`/`field_validator` patterns; the placeholder-name regex convention `[A-Za-z_][A-Za-z0-9_]*` (same as `resolution.py::_PLACEHOLDER_RE`).
- Produces (exact model tree — later tasks depend on these field names verbatim):
  - `parse_listen(listen: str) -> tuple[str, int]` — module-level function. Split with `rsplit(":", 1)`. IPv6 hosts **must be bracketed** (`[::1]:18080` → host `::1`, port `18080`): when the host part starts with `[`, strip the brackets. A missing port, a non-int port, or an empty/unparseable value raises `ValueError` with a clear message. (Reused by `--http-listen` in Task 8.)
  - `HttpResponse(BaseModel)`: `status: int = Field(default=200, ge=100, le=599)`; `headers: dict[str, str] | None = None`; `body: Any = None` (`None` ⇒ empty response body).
  - `HttpMatch(BaseModel)`: `body: dict | None = None` (the `json_subset` filter needle).
  - `HttpStub(BaseModel)`: `description: str | None = None`; `method: str` (normalized to **uppercase** via a `@field_validator("method")`, accepting any string); `path: str` (may contain `{name}` segments); `match: HttpMatch | None = None`; `response: HttpResponse`; `delay_ms: int = 0`.
  - `HttpMockConfig(BaseModel)`: `listen: str = "0.0.0.0:18080"` (validated by a `@field_validator("listen")` that calls `parse_listen` and raises `ValueError` on failure so a bad listen fails at config **load** as a `ConfigError`); `stubs: dict[str, HttpStub] = Field(default_factory=dict)`.
  - `KafkaReaction(BaseModel)`: `topic: str`; `key: str | None = None`; `value: Any` (required — must be JSON-serializable; consumed by `KafkaClient.produce`); `headers: dict[str, str] | None = None` (values UTF-8-encoded by `produce`; non-string values are a config error — validated by a `@field_validator("headers")` that rejects non-str values with `ValueError`).
  - `KafkaReactor(BaseModel)`: `description: str | None = None`; `topic: str`; `consumer_group: str | None = None` (omit ⇒ generated unique per run — resolved in Task 7, not here); `match: str | None = None` (jq predicate); `reaction: KafkaReaction`.
  - `KafkaMockConfig(BaseModel)`: `reactors: dict[str, KafkaReactor] = Field(default_factory=dict)`.
  - `MocksConfig(BaseModel)`: `http: HttpMockConfig | None = None`; `kafka: KafkaMockConfig | None = None`.
  - `Config` gains: `mocks: MocksConfig | None = None` (additive; absent ⇒ `None`; no `version` bump).

- [ ] **Step 1: Write the failing tests**

  Scenarios + expected results (implementer writes the test functions):
  - `MocksConfig()` with no data → both `http` and `kafka` are `None`. A `Config(version="1", mocks={})` validates (mocks present but empty is allowed).
  - `HttpStub(method="post", path="/x", response={"status": 201})` → `method == "POST"`, `response.status == 201`.
  - `HttpResponse(status=99)` and `HttpResponse(status=600)` → both raise `ValidationError`; `HttpResponse(status=599)` → ok; `HttpResponse()` → `status == 200`, `body is None`.
  - `KafkaReaction(topic="t", value={"a":1})` ok; `KafkaReaction(topic="t", value=42)` ok (JSON-serializable scalar); `KafkaReaction(topic="t", value=object())` is fine at model time (JSON-encoding failure surfaces later in `produce` — do not over-validate here).
  - `KafkaReaction(topic="t", value=1, headers={"x": 5})` → `ValidationError` (non-string header value rejected).
  - `parse_listen("0.0.0.0:18080") == ("0.0.0.0", 18080)`; `parse_listen("[::1]:18080") == ("::1", 18080)`; `parse_listen("host")`, `parse_listen("host:abc")`, `parse_listen("")` each raise `ValueError`.
  - `HttpMockConfig(listen="0.0.0.0:notaport")` → `ValidationError` (the `listen` validator runs `parse_listen`).
  - End-to-end via `load_config`: a YAML string containing the spec §7.1 `mocks:` block (with `${...}` already resolved to literals for the unit test, e.g. `listen: "0.0.0.0:18080"`) parses into a `Config` whose `mocks.http.stubs["create-order"].response.body["order_id"] == "{customer_id}-mock"`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_models.py -v`
  Expected: FAIL — `MocksConfig`/the stub models/`parse_listen` are not defined (`AttributeError`/`ImportError`).

- [ ] **Step 3: Write minimal implementation**

  In `agctl/config/models.py`: add `parse_listen`, the 8 models above with the stated validators (`method` upper-case, `status` range via `Field(ge=100, le=599)`, `listen` via `parse_listen`, `headers` non-str rejection), and the `mocks` field on `Config`. No method bodies beyond the validators; field order matters (referenced models defined before referrers, mirroring the existing file's bottom-up order). `dict` insertion order is preserved by Pydantic v2 — do not sort stubs/reactors.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_mock_models.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/config/models.py tests/unit/test_mock_models.py`
  Run: `git commit -m "feat(config): add mocks section Pydantic models + parse_listen"`

---

## Task 2: Validator cross-references and warnings for `mocks`

**Files:**
- Modify: `agctl/config/validator.py`
- Test: `tests/unit/test_validator.py` (add mock test cases)

**Interfaces:**
- Consumes: Task 1's `MocksConfig`/`HttpStub`/`KafkaReactor`/`HttpMockConfig`; the existing `validate_config(cfg) -> tuple[list[dict], list[dict]]` shape (each entry `{"path": str, "message": str}`); the existing `_missing_description` helper.
- Produces: `validate_config` gains three mock-aware checks (only consulted by `agctl config validate`; the per-command load path does NOT run it — the runtime equivalent is in Task 8):
  1. **`mocks.kafka` requires `kafka.brokers`.** If `cfg.mocks` and `cfg.mocks.kafka` and `cfg.mocks.kafka.reactors` are non-empty, but `cfg.kafka` is absent/has an empty `brokers` list → **error** `{"path": "mocks.kafka", "message": "kafka mocks require top-level kafka.brokers"}` (one error; mention is sufficient).
  2. **Missing `description` → warning** for every stub (`mocks.http.stubs.<name>`) and reactor (`mocks.kafka.reactors.<name>`), reusing the existing message text `"missing description (discovery degrades without it)"`.
  3. **Path-template shadowing warning.** For the HTTP stubs (in YAML/insertion order), warn when a later stub's path template is ambiguous relative to an earlier one at the same position: e.g. an earlier `/orders/{order_id}` shadows a later `/orders/bulk` (the literal would never be reached because the earlier `{name}` captures `bulk`). The warning path is `mocks.http.stubs.<later-name>` and the message names both stubs and explains first-match-wins. (A simple, conservative rule is acceptable: flag a literal segment at a position where an earlier stub has a `{name}` at the same position. Do not attempt a full router overlap solver.)

- [ ] **Step 1: Write the failing tests**

  Add to `tests/unit/test_validator.py`:
  - A `Config` with `mocks.kafka.reactors = {"r": KafkaReactor(topic="t", reaction=...)}` but **no** top-level `kafka.brokers` → `validate_config` returns one error whose `path == "mocks.kafka"`.
  - Same config but `cfg.kafka.brokers = ["localhost:9092"]` → no error.
  - A stub and a reactor each missing `description` → two warnings with the expected `path`s.
  - Two HTTP stubs `/orders/{order_id}` then `/orders/bulk` (in that order) → a shadowing warning naming the later stub.
  - Stubs `/a/{x}` and `/b/{y}` → no shadowing warning (no ambiguity). And `/orders/bulk` then `/orders/{order_id}` (literal first) → no warning.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_validator.py -k mock -v`
  Expected: FAIL — the mock checks don't exist yet (no errors/warnings returned).

- [ ] **Step 3: Write minimal implementation**

  Append the three checks to `validate_config` (guard every access on `cfg.mocks is not None` and the sub-section being non-None). Reuse `_missing_description`. Keep the function returning `(errors, warnings)` with errors appended before warnings (matching existing ordering).

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_validator.py -v`
  Expected: PASS (new mock cases + all pre-existing validator cases still green).

- [ ] **Step 5: Commit**

  Run: `git add agctl/config/validator.py tests/unit/test_validator.py`
  Run: `git commit -m "feat(config): validate mocks (kafka.brokers ref, descriptions, path shadowing)"`

---

## Task 3: Pure path-template router

**Files:**
- Create: `agctl/mock/__init__.py` (empty package marker), `agctl/mock/routing.py`
- Test: `tests/unit/test_mock_routing.py` (Create)

**Interfaces:**
- Consumes: stdlib `urllib.parse.urlsplit`; the placeholder-name regex convention `[A-Za-z_][A-Za-z0-9_]*` (compile a local `_PARAM_RE` matching `resolution.py::_PLACEHOLDER_RE` exactly — do **not** import `resolution` just for the regex, to keep the module dependency-light).
- Produces:
  - `split_segments(path: str) -> list[str]` — split a path on `/` (preserving leading/trailing empty segments so trailing slash is significant). E.g. `/orders` → `['', 'orders']`; `/orders/` → `['', 'orders', '']`.
  - `is_param_segment(seg: str) -> bool` — True iff `seg` is exactly `{name}` for a valid name.
  - `param_name(seg: str) -> str` — the `name` inside `{name}` (caller ensures `is_param_segment` first).
  - `match_path(template_path: str, request_path: str) -> dict[str, str] | None` — **strip the query string from the request path first** (`urlsplit(request_path).path`); compare segment-by-segment. Different segment counts → `None`. For each position: a `{name}` template segment captures `name → request_segment`; a literal template segment must `==` the request segment. Return the captures dict on full match, else `None`.

- [ ] **Step 1: Write the failing tests**

  Expected results:
  - `match_path("/api/v1/orders/{order_id}", "/api/v1/orders/42") == {"order_id": "42"}`.
  - Trailing slash is significant: `match_path("/orders", "/orders/") is None` and `match_path("/orders/", "/orders") is None`.
  - Query string stripped: `match_path("/api/v1/orders/{order_id}", "/api/v1/orders/42?x=1&y=2") == {"order_id": "42"}`.
  - Segment-count mismatch: `match_path("/orders/{id}", "/orders") is None`.
  - Literal must match exactly: `match_path("/orders/bulk", "/orders/BULK") is None`.
  - Multiple captures: `match_path("/{org}/{repo}", "/a/b") == {"org": "a", "repo": "b"}`.
  - Root: `match_path("/", "/") == {}`; `match_path("/", "/x") is None`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_routing.py -v`
  Expected: FAIL — module/functions undefined.

- [ ] **Step 3: Write minimal implementation**

  Implement the four functions per the contracts above. No networking, no state. Keep it pure (deterministic, trivially testable).

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_mock_routing.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/mock/__init__.py agctl/mock/routing.py tests/unit/test_mock_routing.py`
  Run: `git commit -m "feat(mock): pure path-template router with query-strip and trailing-slash significance"`

---

## Task 4: `KafkaClient.consume_loop` + `probe` (the one new client surface)

**Files:**
- Modify: `agctl/clients/kafka_client.py`
- Test: `tests/unit/test_kafka_client.py` (add `consume_loop`/`probe` cases)

**Interfaces:**
- Consumes: the existing `KafkaClient.__init__` (`brokers`, `group_id`, `extra_conf`, `consumer_factory`, `producer_factory`), `_import_kafka()`, `_build_consumer()` (reuse the conf it builds — manual commit: `enable.auto.commit=False`, `auto.offset.reset="earliest"`), `_normalize_message(msg)`, and the `ConnectionFailure`/`ConfigError` mappings.
- Produces (the committed-consume feature — spec §8.3):
  - `class ReactionResult(enum.Enum)` (define at module top-level in `kafka_client.py`): members `COMMIT = "commit"`, `RETRY = "retry"`, `STOP = "stop"`.
  - `KafkaClient.consume_loop(self, topic, *, group_id, stop_event, handle, poll_timeout=0.5, max_retries=3, on_assign=None, on_revoke=None) -> None`:
    - `group_id`: the consumer group id to use (the engine resolves unique-per-run vs. pinned before calling).
    - `stop_event`: a `threading.Event`; the loop exits when set (checked before each poll and is the only clean exit).
    - `handle`: `Callable[[dict], ReactionResult]` called once per *normalized* message, **with retry context**: the client calls it as `handle(msg, attempt=a, final=(a >= max_retries))`. Contract: `COMMIT` ⇒ `store_offset(msg)` + `commit()` then continue; `RETRY` ⇒ (only valid when `final is False`) seek the message's `TopicPartition(topic, partition, offset)` back and re-poll (the same message is re-delivered) for the next attempt; `STOP` ⇒ break the loop (reactor is done/dying). **The client never retries past `final`** — when `final is True` the handle is contractually required to return `COMMIT` or `STOP` (never `RETRY`), so the client cannot deadlock; a defensive `RETRY`-on-final is treated as `COMMIT`. Because the client owns the retry budget and passes `attempt`/`final`, the reactor needs **no** attempt counter of its own (see Task 6).
    - `on_assign`/`on_revoke`: optional rebalance callbacks registered on subscribe (the engine wires a warning-line emitter via `on_assign`).
    - **Consumer lifecycle (D13):** the consumer is built inside `consume_loop`, used only on this thread, and `close()`d in a `finally`. The engine never touches it.
  - `KafkaClient.probe(self, topic, *, group_id, timeout=5.0) -> None`:
    - Builds a consumer (same conf as `_build_consumer`, with the given `group_id`), calls `consumer.list_topics(topic, timeout=timeout)`. On a librdkafka error / timeout / raised `KafkaException` → raise `ConnectionFailure` (message includes broker list). On `_import_kafka()` raising `ConfigError` (missing `kafka` extra) → propagate. Always `close()` the consumer in `finally`. This is the one-shot connectivity check the engine calls **before** binding HTTP (spec §8.3 / §11 "broker unreachable at startup → exit 2").

- [ ] **Step 1: Write the failing tests**

  Use injected `consumer_factory`/`producer_factory` fakes (no broker). The test `handle` is a fake callable `handle(msg, *, attempt, final) -> ReactionResult`. Scenarios + expected results:
  - `consume_loop` with a `FakeConsumer` whose `poll()` yields two normalized messages then `None`: `handle` returns `COMMIT` for both (called with `attempt=1, final=False` each) → after the loop, `FakeConsumer.commit` was called twice and `FakeConsumer.close` called once; `store_offset` called for each.
  - `handle` returns `RETRY` for `attempt` 1 and 2 then `COMMIT` at `attempt=3` (`max_retries=3`, so `final=True` at 3): the consumer's `seek` is called twice (seeking back to the message's partition+offset) and `commit` once — message eventually committed after the successful attempt.
  - `handle` returns `RETRY` on every attempt, including `final=True` (`max_retries=2`): after 2 attempts the client treats the final `RETRY` as a forced `COMMIT` and commits exactly once (advances past) — verifies it does **not** loop forever and does commit (the visible-skip backstop).
  - `handle` returns `STOP`: loop exits immediately, consumer closed.
  - `stop_event` set before the first poll: loop exits without calling `handle`, consumer closed.
  - `probe` with a `FakeConsumer` whose `list_topics` returns normally → returns `None` (no raise), consumer closed. With a `FakeConsumer` whose `list_topics` raises → `ConnectionFailure` raised, consumer still closed (finally). With `_import_kafka` patched to raise `ConfigError` (simulate missing extra) → `ConfigError` propagates.
  - (Define the `FakeConsumer` to record `poll`/`commit`/`store_offset`/`seek`/`close`/`list_topics`/`assignment`/`subscribe` calls, mirroring the real `confluent_kafka.Consumer` surface used by the existing tests in this file.)

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_kafka_client.py -k "consume_loop or probe" -v`
  Expected: FAIL — `consume_loop`/`probe`/`ReactionResult` undefined.

- [ ] **Step 3: Write minimal implementation**

  Add `import enum`, define `ReactionResult`, and add `consume_loop` + `probe` to `KafkaClient`. In `consume_loop`: `_import_kafka()` first (raises `ConfigError` on missing extra), build consumer via `_build_consumer()` overriding `group.id` with the passed `group_id`, `subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)`, poll loop honoring `stop_event`, dispatch on `handle`'s `ReactionResult`, manage seek-back retries + forced-commit, `finally: consumer.close()`. In `probe`: build consumer, `list_topics(topic, timeout=timeout)` inside try/except mapping to `ConnectionFailure`, `finally: close()`. Map exceptions exactly as the existing methods do.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_kafka_client.py -v`
  Expected: PASS (new cases + all pre-existing client cases still green).

- [ ] **Step 5: Commit**

  Run: `git add agctl/clients/kafka_client.py tests/unit/test_kafka_client.py`
  Run: `git commit -m "feat(kafka): committed consume_loop + startup broker probe"`

---

## Task 5: HTTP mock server (stdlib `ThreadingHTTPServer`)

**Files:**
- Create: `agctl/mock/http_server.py`
- Test: `tests/unit/test_mock_http_server.py` (Create)

**Interfaces:**
- Consumes: Task 1 models (`HttpMockConfig`/`HttpStub`/`HttpResponse`); Task 3 router (`match_path`); `agctl.resolution.fill_placeholders(value, params: dict[str,str])`; `agctl.assertions.json_subset(needle, haystack)`; stdlib `http.server.ThreadingHTTPServer`/`BaseHTTPRequestHandler`, `socketserver`, `threading.Semaphore`, `json`, `urllib.parse`.
- Produces:
  - `MockHTTPServer(socketserver.ThreadingHTTPServer)` — constructed as `MockHTTPServer((host, port), Handler, stubs=..., emit_event=..., concurrency_cap=64)`. Stores `stubs` (a `dict[str, HttpStub]` in insertion order), `emit_event` (the engine's single-writer callable), and a `threading.Semaphore(concurrency_cap)`. The `Handler` (`BaseHTTPRequestHandler` subclass, pinned `protocol_version = "HTTP/1.1"`) is closures/attrs-bound to the server's `stubs`/`emit_event`/semaphore.
  - The handler, per request, performs (all logic described, not coded here):
    1. **Read the request body honoring `Content-Length` AND de-chunk `Transfer-Encoding: chunked`** bodies (stdlib does not de-chunk). If neither is present, body is empty.
    2. **Parse body as JSON** into `parsed_body` (dict/list/scalar) when `Content-Type` indicates JSON or the body parses as JSON; else `parsed_body = None`.
    3. **Match** the first stub (insertion order) where: `stub.method.upper() == request.command.upper()` AND `match_path(stub.path, request.path)` is not `None` AND (`stub.match.body is None` OR `json_subset(stub.match.body, parsed_body)`). On a match, the captures dict is the `match_path` result.
    4. **Build capture context** `dict[str,str]`: start from the path captures; if `parsed_body` is a `dict`, merge its **top-level** keys in via `str(v)` stringification (D11). (`match.body` is a filter, not a capture source.) If the body did not parse to a dict, no body keys are captured.
    5. **No match** → respond `404`, `Content-Type: application/json`, body `{"mock_error":"no matching stub"}`, send exactly one `Content-Length`, and emit `{"event":"http.unmatched","method":...,"path":...,"status":404}` via `emit_event`.
    6. **React** → render `response.body` and `response.headers` via `fill_placeholders(..., capture_context)`. If the response body has an unresolved `{placeholder}` AND the body did not parse to a dict, emit `{"event":"http.body_parse_skipped","stub":name,"method":...,"path":...,"reason":"non-JSON body; response has unresolved placeholders"}` (degraded-match signal). **Content-Type default:** if `body` (rendered) is a `dict`/`list`, JSON-encode it and default `Content-Type` to `application/json` when not set explicitly; a scalar/`None` body defaults to `text/plain`. Explicit `response.headers` always win. Acquire the semaphore before `delay_ms` sleep (or immediately 429 if the cap is exhausted — simple overflow handling), sleep `response.delay_ms`, release. Send `status` + headers + body, **always** with `Content-Length`. Emit `{"event":"http.hit","stub":name,"method":...,"path":...,"status":status,"duration_ms":...}`.
  - `make_handler(stubs, emit_event, semaphore) -> type` — a factory returning the bound `Handler` class (the test seam: tests build a handler bound to a list-capturing `emit_event` and stubs, then drive it via a real server on port 0 or by constructing the handler against a fake request).

- [ ] **Step 1: Write the failing tests**

  Drive a real `MockHTTPServer` bound to `("127.0.0.1", 0)` with a list-capturing `emit_event`, hit it with `httpx` (the `http` extra is available to tests). Scenarios + expected results:
  - A stub `POST /api/v1/orders` with `match.body={"priority":"high"}`, `response.status=201`, `response.body={"order_id":"{customer_id}-mock","status":"PENDING"}`, `delay_ms=0`. A `POST /api/v1/orders` with JSON body `{"customer_id":"c1","priority":"high"}` → status `201`, response body JSON `{"order_id":"c1-mock","status":"PENDING"}`, `Content-Type: application/json`, and an `http.hit` event with `stub=="create-order"` (or whatever name) was emitted.
  - Same stub, a body with `priority:"low"` → **404** + `http.unmatched` (the body filter failed). A `GET` to the same path → 404 + `http.unmatched` (method mismatch).
  - Path capture: `GET /api/v1/orders/{order_id}` with `response.body={"order_id":"{order_id}"}`; `GET /api/v1/orders/42` → body `{"order_id":"42"}`.
  - Query strip: `GET /api/v1/orders/42?x=1` still matches and captures `order_id=="42"`.
  - Chunked body: a `POST` sent with `Transfer-Encoding: chunked` (httpx streams this) with a JSON body still matches the `match.body` filter (de-chunk works).
  - HTTP/1.1 + Content-Length: the raw response line is `HTTP/1.1` and carries a `Content-Length` header (assert on the `httpx` response's raw connection or `response.http_version`/headers).
  - Concurrency cap: set `concurrency_cap=1` with a stub `delay_ms=50`; fire 2 concurrent requests; assert one is served normally and the overflow is handled (either 429 or serialized — pick 429 and assert the second gets a 429 / fast failure, documenting the choice).
  - `body_parse_skipped`: a stub whose response references `{customer_id}` but whose request body is non-JSON text → the stub still matches on method+path, responds with the literal `{customer_id}` unresolved, and emits a `http.body_parse_skipped` event.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_http_server.py -v`
  Expected: FAIL — `MockHTTPServer`/`make_handler` undefined.

- [ ] **Step 3: Write minimal implementation**

  Implement `MockHTTPServer` + `make_handler` per the contract. Pin `protocol_version="HTTP/1.1"`; always send `Content-Length`; implement chunked de-chunking for request bodies; implement the match/react/emit sequence; gate `delay_ms` on the semaphore. Match the surrounding code's style (docstrings, `from __future__ import annotations`).

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_mock_http_server.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/mock/http_server.py tests/unit/test_mock_http_server.py`
  Run: `git commit -m "feat(mock): stdlib HTTP mock server (HTTP/1.1, capture, templating, chunked, concurrency cap)"`

---

## Task 6: Kafka reactor

**Files:**
- Create: `agctl/mock/kafka_reactor.py`
- Test: `tests/unit/test_mock_kafka_reactor.py` (Create)

**Interfaces:**
- Consumes: Task 1 models (`KafkaReactor`, `KafkaReaction`); Task 4 `KafkaClient.consume_loop`/`probe` + `ReactionResult`; `agctl.assertions.jq_bool(value, expr)` (value-first); `agctl.resolution.fill_placeholders`; `KafkaClient.produce(topic, value, *, key, headers)`.
- Produces:
  - `KafkaReactor(name: str, config: KafkaReactor, client: KafkaClient, *, emit_event: Callable[[dict], None], stop_event: threading.Event, fail_fast: bool, run_id: str)`:
    - `resolved_group() -> str` — `config.consumer_group` if set, else `f"agctl-mock-{name}-{run_id}"` (unique per run; `run_id` supplied by the engine, e.g. `str(os.getpid())`).
    - `prepare() -> None` — calls `client.probe(config.topic, group_id=self.resolved_group())`; raises `ConnectionFailure`/`ConfigError` on unreachable broker / missing extra. Called by the engine **before** HTTP bind (probe-then-bind ordering).
    - `run() -> None` — calls `client.consume_loop(config.topic, group_id=self.resolved_group(), stop_event=stop_event, handle=self._handle, max_retries=3)`. (The consumer is built+closed inside `consume_loop` on this thread — D13.)
    - `_handle(msg: dict, *, attempt: int, final: bool) -> ReactionResult` — the per-message decision logic (the client passes `attempt`/`final`, so the reactor keeps **no** per-message counter):
      1. `value = msg.get("value")`. If `value` is **not a `dict`** (non-JSON, array, scalar, Avro/Protobuf-as-text) → emit `{"event":"kafka.skipped","reactor":name,"topic":config.topic,"reason":"non-object message value","count":1}` and return `ReactionResult.COMMIT` (the message is processed = not matchable; visible skip, not silent idle).
      2. **Match:** if `config.match` is set and `jq_bool(value, config.match)` is False → return `ReactionResult.COMMIT` (non-match; no event — like `kafka consume --match`). A `jq_bool` that errors → treat as no-match (inherited safe behavior).
      3. **Capture context** `dict[str,str]`: top-level keys of `value`, each stringified via `str(v)` (D11).
      4. **React:** render `config.reaction.value` via `fill_placeholders(..., capture_context)`; render `key` (if set) and `headers` likewise. Call `client.produce(config.reaction.topic, rendered_value, key=rendered_key, headers=rendered_headers)`. On success → emit `{"event":"kafka.reacted","reactor":name,"topic":config.reaction.topic,"key":rendered_key,"duration_ms":...}` and return `ReactionResult.COMMIT`.
      5. **Reaction failure** (the `produce` raised, or JSON-encoding failed): if **not** `final` → return `ReactionResult.RETRY` (the client seeks back and re-delivers; emit nothing yet). If `final` → emit the `kafka.error` line **exactly once**: `{"event":"kafka.error","reactor":name,"topic":config.topic,"offset":msg["offset"],"partition":msg["partition"],"error":str(exc),"fatal":fail_fast}`; then return `ReactionResult.COMMIT` if **not** `fail_fast` (visible skip, reactor continues) or `ReactionResult.STOP` if `fail_fast` (engine-wide stop). This guarantees the error surfaces once and the offset advances (never a silent drop), per spec §8.3.

- [ ] **Step 1: Write the failing tests**

  Inject a `FakeKafkaClient` (subclass/duck of `KafkaClient` overriding `consume_loop` to call `handle` for a scripted list of messages, and `produce`/`probe` as recorders) + a list-capturing `emit_event`. Scenarios + expected results:
  - Message value `{"orderId":"ord-1","command":"CREATE_ORDER"}`, reactor `match='.command == "CREATE_ORDER"'`, reaction `value={"eventType":"ORDER_CREATED","orderId":"{orderId}"}`, `key="{orderId}"` → `produce` called with `topic=reaction.topic`, `value={"eventType":"ORDER_CREATED","orderId":"ord-1"}`, `key="ord-1"`; an `http`... a `kafka.reacted` event emitted with `key=="ord-1"`.
  - Numeric capture coercion: value `{"orderId":42}` → reaction `value` renders `orderId` as `"42"` (stringified); `jq_bool(value, '.command=="X"')` returns False → `COMMIT`, no event.
  - Non-object value (string `"not-json"` is parsed by `_normalize_message` to a `str`; or an array `[1,2]`) → `kafka.skipped` event emitted with `count==1`, `_handle` returns `COMMIT`.
  - Reaction failure (FakeKafkaClient.produce raises): with `fail_fast=False`, after `max_retries` the reactor emits one `kafka.error` event with `fatal==False` and returns `COMMIT` (advances); with `fail_fast=True`, the final attempt emits `kafka.error` with `fatal==True` and returns `STOP`.
  - `prepare()` calls `client.probe(...)` with `group_id == resolved_group()`; when omitted, `resolved_group()` starts with `f"agctl-mock-{name}-"`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_kafka_reactor.py -v`
  Expected: FAIL — `KafkaReactor` undefined.

- [ ] **Step 3: Write minimal implementation**

  Implement `KafkaReactor` with `resolved_group`/`prepare`/`run`/`_handle` per the contract. Keep `_handle`'s attempt-tracking simple (a `dict[tuple[int,int], int]` of attempts per `(partition, offset)`, cleared on `COMMIT`). Match surrounding style.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_mock_kafka_reactor.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/mock/kafka_reactor.py tests/unit/test_mock_kafka_reactor.py`
  Run: `git commit -m "feat(mock): Kafka reactor (jq match, capture coercion, idempotent reaction, visible skip)"`

---

## Task 7: `MockEngine` — lifecycle, single-writer emission, shutdown

**Files:**
- Create: `agctl/mock/engine.py`
- Test: `tests/unit/test_mock_engine.py` (Create)

**Interfaces:**
- Consumes: Task 5 `MockHTTPServer`; Task 6 `KafkaReactor`; Task 1 models (`MocksConfig`); `parse_listen` from `config.models`; `KafkaClient`; stdlib `threading`, `signal`, `time`.
- Produces:
  - `MockEngine(mocks: MocksConfig | None, *, run_http: bool, run_kafka: bool, http_listen: str, kafka_client: KafkaClient | None, fail_fast: bool = False, duration: float | None = None, until_stopped: bool = True, emit_fn: Callable[[dict], None] = _default_emit, run_id: str | None = None)`:
    - `run_id` defaults to `str(os.getpid())` when `None` (shared across reactors so all generated groups share a suffix).
    - `start() -> None` — **probe-then-bind ordering:** if `run_kafka`, build the `KafkaReactor` objects and call `prepare()` on **each** (probes brokers) **first**; if any `prepare()` raises, close the already-prepared reactors' consumers and re-raise (no HTTP socket bound). Then if `run_http`, bind `MockHTTPServer(parse_listen(http_listen), ...)` — a bind failure (port in use) raises `ConfigError` with a hint to kill the stale mock. Then emit the `started` line (shape below). On any exception, run `shutdown()` to release what was acquired.
    - `run() -> int` — installs `SIGTERM`/`SIGINT` handlers that set the shared `stop` `Event` (guard for non-main-thread); starts the HTTP serve thread (`server.serve_forever`) and one thread per reactor (`reactor.run()`); if `duration` is set, arms a timer that sets `stop`. Blocks until `stop` is set (all threads observe it). Restores previous signal handlers in `finally`. Returns the exit code: `0` if no runtime errors occurred, else `1` (D8). Under `fail_fast`, returns `1` as soon as a reactor signals `STOP`.
    - `shutdown() -> None` — `server.shutdown()`/`server_close()` (if HTTP); each reactor's loop exits on `stop` (its consumer closes inside `consume_loop` finally); `join()` all threads with a timeout; emit the `summary` line.
    - **Single-writer emission (D12):** `emit_event(self, line: dict) -> None` acquires a `threading.Lock`, adds a `timestamp` field (ISO-8601 Z, `datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")`) if absent, writes `json.dumps(line) + "\n"` via the injected `emit_fn`/stdout, flushes, releases. Every HTTP handler and reactor emits through **this** method.
    - **Summary counters** (best-effort under a lock): `http_hits`, `http_unmatched`, `http_body_parse_skipped`, `kafka_reactions`, `kafka_skipped`, `kafka_errors`. The handlers/reactors increment them (the `emit_event` path can tally by `line["event"]` centrally — simplest: increment inside `emit_event` based on the event name). `duration_ms` = monotonic since `start()`.
  - `started` line shape: `{"event":"started","http":{"listen":"<host:port>","stubs":N}|None,"kafka":{"reactors":[{"name":..,"topic":..,"consumer_group":..},...]}|None,"timestamp":..}`.
  - `summary` line shape: `{"event":"summary","http_hits":N,"http_unmatched":N,"http_body_parse_skipped":N,"kafka_reactions":N,"kafka_skipped":N,"kafka_errors":N,"duration_ms":N}`.

- [ ] **Step 1: Write the failing tests**

  Use injectable fakes: a `FakeHTTPServer` (records serve/shutdown, calls a bound `emit_event` on demand) and `FakeKafkaClient` (whose `consume_loop` returns immediately / `probe` is a no-op), plus a list-capturing `emit_fn`. Scenarios + expected results:
  - `MockEngine(mocks=None, run_http=False, run_kafka=False, ...)` → `start()` emits a `started` line with `http==None` and `kafka==None`, `run()` returns `0`, `shutdown()` emits a `summary` with all-zero counts. (No-op engine.)
  - `run_http=True` with a `mocks.http` of 2 stubs, `run_kafka=False`: `started` line has `http.stubs==2`, `kafka==None`; the HTTP serve thread is started; on `stop` set, `shutdown()` stops/joins it.
  - Probe-then-bind ordering: with `run_kafka=True`, a `FakeKafkaClient.probe` that **raises** → `start()` raises and **no** `started` line was emitted and the fake HTTP server was **never** bound (assert its bind/serve was not called). When `probe` succeeds, HTTP is then bound and `started` emitted.
  - Single-writer under concurrency: spin N threads each calling `engine.emit_event({"event":"http.hit",...}))` in a tight loop; assert every captured line is valid JSON (parses) and no line is interleaved/corrupted.
  - `--fail-fast`: a reactor faking `STOP` (a `kafka.error` with `fatal=True`) → `run()` returns `1`.
  - Summary tally: emit a mix of `http.hit`/`http.unmatched`/`kafka.reacted`/`kafka.skipped`/`kafka.error` lines → `summary` counts match.
  - Port-in-use: constructing/binding with a `FakeHTTPServer` whose bind raises `OSError(EADDRINUSE)` → `start()` raises `ConfigError` whose message mentions killing the stale mock.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_engine.py -v`
  Expected: FAIL — `MockEngine` undefined.

- [ ] **Step 3: Write minimal implementation**

  Implement `MockEngine` per the contract. Reuse `http_commands._emit_stdout_line`'s pattern for the default `emit_fn` (write+newline+flush to stdout) but route through the lock. Keep threads daemon-friendly; `join` with a timeout so a stuck thread can't hang shutdown. Match surrounding style.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_mock_engine.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/mock/engine.py tests/unit/test_mock_engine.py`
  Run: `git commit -m "feat(mock): MockEngine lifecycle, single-writer NDJSON, probe-then-bind, fail-fast"`

---

## Task 8: `agctl mock run` command + CLI wiring + runtime guards

**Files:**
- Create: `agctl/commands/mock_commands.py`
- Modify: `agctl/cli.py`
- Test: `tests/unit/test_mock_commands.py` (Create); `tests/unit/test_cli.py` (add a `mock run --help` registration assertion)

**Interfaces:**
- Consumes: Task 7 `MockEngine`; Task 1 `parse_listen`/`MocksConfig`; `agctl.command.load_config_or_raise`; `agctl.output.emit`; `agctl.errors.ConfigError`/`ConnectionFailure`/`AgctlError`; `agctl.commands.kafka_commands.new_kafka_client` + `_kafka_ssl_conf` (imported — command→command is allowed; this is how the KafkaClient is built and the lazy-import/install-hint behavior is reused). Also `agctl.commands.http_commands._emit_stdout_line` may be reused (or a local copy).
- Produces:
  - `new_mock_engine(mocks, *, run_http, run_kafka, http_listen, kafka_client, fail_fast, duration, until_stopped) -> MockEngine` — **test seam** (tests monkeypatch `mock_commands.new_mock_engine` to return a fake). In production it constructs the real `MockEngine`.
  - `mock_run` Click command with options (verbatim names/flags from spec §6):
    - `--config <path>` (also read from `ctx.obj["config_path"]`), `--http-listen <host:port>` (literal override), `--only [http|kafka]` (`click.Choice`), `--fail-fast` (flag), `--duration <seconds>` (`type=float`), `--until-stopped` (flag). `--duration` and `--until-stopped` are mutually exclusive.
  - The command **hand-reimplements the `try/except → emit + exit` shape** (it is NOT wrapped by `@envelope` — the second streaming exception). Startup errors emit **one** `emit(ok=False, command="mock.run", error=..., duration_ms=...)` envelope then `raise SystemExit(2)`/the error's exit code (exactly like `http ping`).
  - **Runtime guards (in order):**
    1. Load config via `load_config_or_raise` (ConfigError → envelope + exit 2).
    2. Resolve engines to run: `--only http` ⇒ `run_http=mocks.http present, run_kafka=False`; `--only kafka` ⇒ the mirror; neither ⇒ `run_http = mocks and mocks.http is not None`, `run_kafka = mocks and mocks.kafka is not None and bool(mocks.kafka.reactors)`.
    3. **`--only <engine>` with that engine absent** → `ConfigError` ("--only %s but no mocks.%s configured") + exit 2.
    4. **No `mocks` section at all** (and no `--only`) → construct a no-op engine (`run_http=False, run_kafka=False`), let it emit `started`+`summary` with zero counts, exit `0` (idempotent no-op).
    5. If `run_kafka`: require top-level `kafka.brokers` non-empty (the §11 runtime guard — mirrors `db execute`'s own write-safety gates; `validate_config` is NOT on this path) else `ConfigError` + exit 2. Build the `KafkaClient` via `new_kafka_client(cfg.kafka)` (which applies `_kafka_ssl_conf`); missing `kafka` extra → `ConfigError` propagates from `_import_kafka` with the install hint. Pass it to the engine; else `kafka_client=None`.
    6. Resolve `http_listen`: `--http-listen` (parsed via `parse_listen`, literal — no `${}` on CLI args) if given, else `mocks.http.listen` (already `${}`-resolved at load).
  - After construction: `engine.start()` (probes + binds — may raise `ConfigError`/`ConnectionFailure`), then `code = engine.run()`, then `raise SystemExit(code)`. Wrap startup in try/except mapping `AgctlError` → `emit(...)` + `SystemExit(err.exit_code)`; any other `Exception` → `InternalError` envelope + `SystemExit(2)` (mirror `http ping`).
  - CLI wiring (modify `agctl/cli.py`): add `@cli.group(name="mock")` + `mock_group.add_command(mock_run)`, mirroring the existing groups; import `mock_run` from `.commands.mock_commands`.

- [ ] **Step 1: Write the failing tests**

  Use Click's `CliRunner` invoking `cli` (or `mock_run` directly) with `--config` pointing at a temp YAML; monkeypatch `mock_commands.new_mock_engine` to a fake that records calls and returns a canned exit code. Scenarios + expected results:
  - A config with a 2-stub `mocks.http`, `--only http` → `new_mock_engine` called with `run_http=True, run_kafka=False, kafka_client=None`; stdout's first line is the engine's `started`; process exits with the fake's code.
  - `--only kafka` with reactors + `kafka.brokers` set → engine called with `run_kafka=True`, `kafka_client` is a real `KafkaClient` (assert its type, not a live connection).
  - `--only http` with **no** `mocks.http` → exits `2`, single envelope `{"ok":false,"command":"mock.run","error":{"type":"ConfigError",...}}`.
  - Config with **no** `mocks` section, no `--only` → exits `0`, and the (fake) engine was constructed with `run_http=False, run_kafka=False`.
  - `mocks.kafka` reactors present but **no** `kafka.brokers` → exits `2` with a `ConfigError` envelope mentioning `kafka.brokers`.
  - `--duration` and `--until-stopped` together → exits `2` with a `ConfigError` envelope (mutually exclusive).
  - Engine `start()` raises `ConnectionFailure` (broker unreachable) → single `mock.run` envelope with `error.type=="ConnectionError"`, exit `2`.
  - `--http-listen "127.0.0.1:9999"` overrides config listen → engine called with `http_listen=="127.0.0.1:9999"`. `--http-listen "bad:no-port"` → exits `2` (parse_listen fails).
  - In `tests/unit/test_cli.py`: `CliRunner().invoke(cli, ["mock", "run", "--help"])` → exit `0`, output contains `--only`, `--fail-fast`, `--http-listen`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_commands.py tests/unit/test_cli.py -v`
  Expected: FAIL — `mock_run` undefined / `mock` group not registered.

- [ ] **Step 3: Write minimal implementation**

  Implement `mock_commands.py` (`new_mock_engine`, `_resolve_engines`, the Click command, the hand-rolled envelope, all guards). Wire the `mock` group + `mock_run` into `cli.py`. Reuse `parse_listen` from `config.models` and `new_kafka_client`/`_kafka_ssl_conf` from `kafka_commands`. Match the `http ping` structure for streaming + envelope.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_mock_commands.py tests/unit/test_cli.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/commands/mock_commands.py agctl/cli.py tests/unit/test_mock_commands.py tests/unit/test_cli.py`
  Run: `git commit -m "feat(mock): agctl mock run command, runtime guards, CLI wiring"`

---

## Task 9: Integration tests (HTTP live; Kafka self-skipping)

**Files:**
- Create: `tests/integration/test_mock_commands.py`

**Interfaces:**
- Consumes: the existing `tests/integration/conftest.py` fixtures (`require_kafka`, `AGCTL_TEST_LIVE=1` testcontainers harness). HTTP integration needs no Docker (real `ThreadingHTTPServer` on port 0).

- [ ] **Step 1: Write the tests**

  - **HTTP (always runs, no Docker):** write a temp `agctl.yaml` with a `mocks.http` of ≥2 stubs (one path-capture, one `match.body` filter, one with `delay_ms`); run `agctl mock run --config <yaml> --duration 1` via `subprocess` (or in-process via the Click command with stdout captured) against a real port; concurrently fire `httpx` requests; assert: the `started` line, the templated response body, `Content-Type: application/json`, an `http.unmatched` line for an unknown path, an `http.hit` line per served request, a `summary` line, and exit code `0`. Also assert the chunked-POST path matches.
  - **Kafka (self-skipping unless `AGCTL_TEST_LIVE=1`):** under the testcontainers Kafka harness, produce a command message to `orders.commands`, run `mock run --only kafka --duration 2`, then `kafka consume`/`assert` on `orders.events` for the templated reaction; assert the reaction was produced with the captured/coerced value, and (separately) that consuming the reactor's unique group shows the offset committed. `pytest.skip()` when no live broker — never fail because the service is absent.

- [ ] **Step 2: Run tests**

  Run: `pytest tests/integration/test_mock_commands.py -v` (HTTP cases run; Kafka cases skip without `AGCTL_TEST_LIVE=1`).
  Run: `AGCTL_TEST_LIVE=1 pytest tests/integration/test_mock_commands.py -v` (if Docker is available — Kafka case runs).
  Expected: HTTP PASS; Kafka PASS-under-live / SKIP otherwise.

- [ ] **Step 3: Commit**

  Run: `git add tests/integration/test_mock_commands.py`
  Run: `git commit -m "test(mock): HTTP integration + self-skipping Kafka reactor integration"`

---

## Deferred from this plan (explicit follow-up)

- **Spec §13 — Discovery changes** (`mocks` category in `agctl discover`). The spec flags it as the lowest-value MVP slice and permits deferral. It does not block the core HTTP+Kafka mock goal. Add a separate plan when discoverability of mocks is wanted.

## Post-implementation (per CLAUDE.md "Docs Sync")

After all tasks merge, invoke the **`docs-watcher`** subagent to sync DESIGN.md and ARCHITECTURE.md per spec §18 (reverse the DESIGN §1 mocking non-goal; add the `mocks` schema, `agctl mock` command, `mock.run` streaming output, deferred + not-covered lists; ARCHITECTURE §3/§4/§5/§6/§8/§12/§15 updates). Also update the `skills/agctl-config` reference (`reference/mocks.md`, the agent failure-stream protocol §10.1, the not-covered list) and `skills/agctl/SKILL.md` usage — though skill updates are the skill author's concern, flag them to the user.

---

## Coverage map (spec section → task)

| Spec section | Implemented by |
|---|---|
| §3 constraints (SUT-facing, plaintext, within-transport, static+templated) | Tasks 5, 6 (behavior) + Global Constraints |
| §6 Command contract `agctl mock run` | Task 8 |
| §7.1/§7.2 Config schema + models | Task 1 |
| §7.3 Capture/templating (coercion, symmetric) | Tasks 5 (HTTP) + 6 (Kafka) |
| §8.1 HTTP server (HTTP/1.1, Content-Length, chunked, concurrency cap, Content-Type default) | Task 5 |
| §8.2 Kafka reactor (jq match arg order, non-object skip, idempotent reaction) | Task 6 |
| §8.3 Committed consume loop + probe + reactor-owned lifecycle + unique group | Tasks 4 (client) + 6 (reactor) + 7 (group resolution) |
| §9 Runtime & lifecycle (single-writer, probe-then-bind, shutdown, fail-fast) | Task 7 |
| §10 NDJSON output + §10.1 failure-stream protocol | Tasks 7 (emission) + 8 (skill docs flagged post-impl) |
| §11 Error & exit-code model | Tasks 1 (load errors), 7 (startup/shutdown), 8 (runtime guards) |
| §12 Validation rules (kafka.brokers ref, description warn, path shadowing) | Task 2 (+ runtime guard in Task 8) |
| §13 Discovery | **Deferred** (separate plan) |
| §14 Packaging (stdlib HTTP, kafka extra, no version bump) | Global Constraints + Task 8 (no pyproject change) |
| §15 Testing strategy | Tasks 1–8 unit + Task 9 integration |
| §16.2 Not-covered (documented limitations) | Surfaced in skill docs post-impl (flag to user) |

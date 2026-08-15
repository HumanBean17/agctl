# Cross-Transport Mock Effects (HTTP ↔ Kafka) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an ordered, polymorphic `effects:` list (kafka produce / http call) to `mocks.http.stubs` and `mocks.kafka.reactors` so an HTTP trigger can produce Kafka messages and a Kafka trigger can make outbound HTTP calls.

**Architecture:** A Pydantic discriminated `Effect` union (`type: kafka | http`) rides on `HttpStub` and `KafkaReactor`. A new DI-based `EffectExecutor` (`mock/effects.py`) — constructed by `MockEngine` from command-layer resolvers — renders each effect via `render_typed`, dispatches kafka (encode via `_encode_payload_with_codec` + `KafkaClient.produce(_raw=True)`) or http (`HttpClient.request`), chains captures between effects, and emits `kafka.produced`/`http.called`/`effect.error`. Effects run synchronously and in order after a trigger matches; effect failure is fatal. Legacy `reaction` is preserved verbatim.

**Tech Stack:** Python 3.11+, Pydantic v2, Click, confluent-kafka (`kafka` extra), httpx (`http` extra), pytest.

## Global Constraints

- Config schema version stays `"3"` — `effects` is additive; no version bump, no `config migrate` change.
- **Lazy-import discipline:** `confluent_kafka`, `httpx`, `avro`, `protobuf` are never imported at module top of `mock/effects.py`; all heavy libs stay behind the injected clients/resolvers (preserves the extra-free import + optional-extras model). A missing `kafka`/`http` extra surfaces as `ConfigError` pointing at the right `pip install`.
- `${ENV}` interpolation applies to effect YAML values at load; `{placeholder}` resolves from the capture namespace at effect time; `{{uuid}}/{{ts}}/{{rand}}` generators work in effect fields via the existing render pipeline.
- **Fail loud:** validate-time `ConfigError` (exit 2) for unknown refs / structural violations; runtime `effect.error` is fatal (exit 1 at `mock run` shutdown / `AssertionFailure` at `mock stop`).
- The one-emit / NDJSON streaming contract is preserved; new events are additive; the legacy `reaction` path emitting `kafka.reacted` is unchanged byte-for-byte.
- TDD with `pytest`. Integration tests are gated on `AGCTL_TEST_LIVE=1` + a plaintext broker and self-skip otherwise (match the existing mock integration suite).
- Every commit lands on branch `feat/mock-cross-transport-effects`. End each task with `git add <files>` + `git commit -m "…"`.

## Spec refinements (read before implementing)

The approved spec is `docs/superpowers/specs/active/2026-08-07-mock-cross-transport-effects-design.md`. Three details are refined here for correctness; behavior matches the spec's intent:

1. **Legacy `reaction` stays a distinct `_handle` code path** emitting `kafka.reacted` — it is *not* literally expanded into an executor effect (which would emit `kafka.produced` and break existing fixtures/tests that grep `kafka.reacted`). Net behavior is identical to spec §5.6/§7.2/§8; this is the back-compat-safe implementation. Config validation guarantees `reaction` XOR `effects` on a reactor, so `_handle` has two mutually exclusive paths.
2. **`iter_mock_jq_expressions` (jq_precompile) IS extended** to yield http-effect `capture.<name>.from` paths (spec §11 said "unchanged"). Reason: a malformed jq `from` would otherwise silently capture empty (false-green). Startup + `config validate` must syntax-check it, exactly as trigger captures already are.
3. **`ServiceConfig` has no TLS fields** (`base_url`/`health_path`/`timeout_seconds` only). http effects reuse `HttpClient(base_url, timeout)`; TLS is implicit via the `https://` URL in service or url mode (corrects a spec §6.2 inaccuracy about `service.use_tls`/`service.tls`).

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `agctl/config/models.py` | `KafkaEffect`, `KafkaEffectMessage`, `HttpEffect`, `Effect` discriminated union; `effects` on `HttpStub`/`KafkaReactor`; `KafkaReactor.reaction` → optional | Modify |
| `agctl/mock/effects.py` | `EffectExecutor`: render + dispatch + capture-chain + first-failure short-circuit; DI-based, extra-free | Create |
| `agctl/mock/engine.py` | Accept effect resolvers; build executor; thread into server/reactor; `emit_event` branches; counters; summary; (construction call sites for server/reactor) | Modify |
| `agctl/mock/http_server.py` | `make_handler`/`MockHTTPServer` accept executor; run `stub.effects` in the semaphore section before the response | Modify |
| `agctl/mock/kafka_reactor.py` | `KafkaReactor` accepts executor; new `effects` path in `_handle`; legacy `reaction` path unchanged | Modify |
| `agctl/mock/daemon.py` | `EVENT_TO_COUNTER` += `kafka.produced`/`http.called`/`effect.error`; `FATAL_FAILURE_EVENTS` += `effect.error` | Modify |
| `agctl/mock/jq_precompile.py` | `iter_mock_jq_expressions` yields http-effect `capture.from` | Modify |
| `agctl/mock/capture_validate.py` | `collect_capture_placement_errors` walks http-effect `capture` | Modify |
| `agctl/config/validator.py` | Cross-refs: http-effect service; kafka-effect cluster/brokers + format-needs-SR; reaction+effects mutex; ≥1 of reaction/effects | Modify |
| `agctl/commands/config_commands.py` | `collect_unknown_template_errors` walks effect fields | Modify |
| `agctl/commands/mock_commands.py` | `mock_run` builds effect kafka/http resolvers, ensures effect-cluster clients, validates services, passes resolvers to engine | Modify |
| `tests/unit/test_mock_models.py` | Config model parsing + validators | Extend |
| `tests/unit/test_mock_effects.py` | `EffectExecutor` dispatch/chaining/failure (fakes) | Create |
| `tests/unit/test_mock_engine.py` | Executor construction, counters, summary, `effect.error` fatal | Extend |
| `tests/unit/test_mock_http_server.py` | HTTP-trigger effect invocation | Extend |
| `tests/unit/test_mock_kafka_reactor.py` | Kafka-trigger effects path + legacy back-compat | Extend |
| `tests/unit/test_mock_commands.py` | `mock_run` resolver/client/service wiring | Extend |
| `tests/unit/test_mock_capture_validate.py` | http-effect capture placement | Extend |
| `tests/unit/test_jq_precompile.py` | http-effect capture.from precompile | Extend |
| `tests/unit/test_config_validator.py` | Effect cross-ref errors | Extend |
| `tests/unit/test_config_commands.py` | Unknown-`{{…}}` in effect fields | Extend |
| `tests/integration/test_mock_effects.py` | Live HTTP→Kafka, Kafka→HTTP, chaining, multi-message, fatal | Create |

---

### Task 1: Config models — `Effect` union + `effects` on stubs/reactors

**Files:**
- Modify: `agctl/config/models.py` (add new models near the existing mock models, ~lines 304-378; add `effects` to `HttpStub` ~312 and `KafkaReactor` ~369; change `KafkaReactor.reaction` ~369 to optional)
- Test: `tests/unit/test_mock_models.py`

**Interfaces:**
- Consumes: existing `CaptureSpec` (`from_: str = Field(alias="from")`, `type: Literal["scalar","object","json"]="scalar"`, `model_config=ConfigDict(populate_by_name=True)`); existing `KafkaReaction` (`topic: str`, `key: str|None`, `value: Any`, `headers: dict[str,str]|None`); `HttpStub`, `KafkaReactor`.
- Produces (new models, exact fields):
  - `KafkaEffectMessage`: `value: Any`; `key: str | None = None`; `headers: dict[str, str] | None = None`. (One element of a `values` list.)
  - `KafkaEffect`: `type: Literal["kafka"]`; `topic: str`; `value: Any | None = None`; `values: list[KafkaEffectMessage] | None = None`; `key: str | None = None`; `headers: dict[str, str] | None = None`; `cluster: str | None = None`; `value_format: Literal["json","avro","protobuf"] | None = None`; `key_format: Literal["string","avro","protobuf"] | None = None`. `@model_validator(mode="after")`: exactly one of `value`/`values` set, else `ValidationError`("kafka effect requires exactly one of `value`/`values`"). A `@field_validator("headers")` mirrors `KafkaReaction._check_headers` (every value a `str`).
  - `HttpEffect`: `type: Literal["http"]`; `service: str | None = None`; `url: str | None = None`; `path: str = "/"`; `method: str = "GET"` (`@field_validator("method")` upper-cases, mirroring `HttpStub._normalize_method`); `headers: dict[str, str] | None = None`; `body: Any | None = None`; `timeout: float | None = None`; `capture: dict[str, CaptureSpec] | None = None`. `@model_validator(mode="after")`: exactly one of `service`/`url` set, else `ValidationError`("http effect requires exactly one of `service`/`url`").
  - `Effect = Annotated[Union[KafkaEffect, HttpEffect], Field(discriminator="type")]`. (First discriminated union in the codebase; idiomatic Pydantic v2.)
  - `HttpStub` gains: `effects: list[Effect] | None = None`.
  - `KafkaReactor`: `reaction: KafkaReaction | None = None` (was required); gains `effects: list[Effect] | None = None`. `@model_validator(mode="after")`: at least one of `reaction`/`effects` present AND not both — else `ValidationError`("reactor requires exactly one of `reaction`/`effects`").
- Validation rules: Pydantic raises `ValidationError` (surfaced as `ConfigError` exit 2 by the loader) on any structural violation above. `from` aliases and `populate_by_name` are inherited from `CaptureSpec` for `http` effect captures.

- [ ] **Step 1: Write the failing tests**

  Each test parses a minimal `agctl.yaml` fragment into the relevant model (or constructs the model directly) and asserts:
  - `KafkaEffect(type="kafka", topic="t", value={...})` parses; `.value` is the dict; `.type == "kafka"`.
  - `KafkaEffect(type="kafka", topic="t", values=[{value: 1}, {value: 2, key: "k"}])` parses; `.values` has 2 items.
  - `KafkaEffect(type="kafka", topic="t")` (neither value nor values) → `ValidationError`.
  - `KafkaEffect(type="kafka", topic="t", value=1, values=[...])` (both) → `ValidationError`.
  - `HttpEffect(type="http", service="s", path="/p", method="post")` parses; `.method == "POST"`.
  - `HttpEffect(type="http", url="https://x/y")` parses; `.url` set, `.service is None`.
  - `HttpEffect(type="http")` (neither service nor url) → `ValidationError`; both set → `ValidationError`.
  - `HttpStub(method="POST", path="/x", response={"status": 200}, effects=[{type: "kafka", topic: t, value: 1}])` parses; `.effects[0].topic == "t"`.
  - `KafkaReactor(topic="t", effects=[{type: "http", url: "https://x"}])` parses with `reaction is None`.
  - `KafkaReactor(topic="t")` (neither reaction nor effects) → `ValidationError`.
  - `KafkaReactor(topic="t", reaction={topic:t,value:1}, effects=[{type:kafka,topic:t,value:2}])` (both) → `ValidationError`.
  - `Effect` discriminator: a YAML/dict with `type: "kafka"` routes to `KafkaEffect`; `type: "http"` routes to `HttpEffect`; `type: "other"` → `ValidationError`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_models.py -v`
  Expected: FAIL — `KafkaEffect`/`HttpEffect`/`Effect` not defined; `effects` not an attribute.

- [ ] **Step 3: Write minimal implementation**

  Add the models above to `agctl/config/models.py` (near the existing mock block). Add the `type: Literal[...]` field to each variant, the two `@model_validator`s and the `headers`/`method` `@field_validator`s, and the `Effect` `Annotated` union. Add `effects: list[Effect] | None = None` to `HttpStub`; change `KafkaReactor.reaction` to `KafkaReaction | None = None`, add `effects: list[Effect] | None = None`, and add the reaction/effects `@model_validator`. Pydantic v2 resolves the forward references automatically (no `model_rebuild` needed unless an ordering issue arises; add one if a forward-ref error appears). No behavior beyond parsing + structural validation.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_mock_models.py -v`
  Expected: PASS. Also run `pytest tests/unit/test_mock_models.py tests/unit/test_config_models.py -q` to confirm no existing model test regressed.

- [ ] **Step 5: Commit**

  Run: `git add agctl/config/models.py tests/unit/test_mock_models.py`
  Run: `git commit -m "feat(mock): add Effect union + effects field on stubs/reactors"`

---

### Task 2: `EffectExecutor` — pure dispatch core (`mock/effects.py`)

**Files:**
- Create: `agctl/mock/effects.py`
- Test: `tests/unit/test_mock_effects.py`

**Interfaces:**
- Consumes (import, all extra-free at module top):
  - `from ..resolution import CaptureValue, render_typed` (`render_typed(value: Any, captures: dict[str, CaptureValue]) -> Any`; `CaptureValue` is a `@dataclass(value: Any, type: str)`).
  - `from .capture import resolve_captures` (`resolve_captures(envelope: dict, captures: dict[str, CaptureSpec] | None) -> tuple[dict[str, CaptureValue], list[tuple[str, str]]]`).
  - `from ..clients.kafka_client import _encode_payload_with_codec` (`_encode_payload_with_codec(codec, topic, value, key) -> tuple[bytes, bytes|None]`; `codec=None` ⇒ JSON encode via `json.dumps`).
  - `from ..config.models import KafkaEffect, HttpEffect` (type hints only).
- Produces:
  - `@dataclass(frozen=True) class EffectOutcome:` with `ok: bool` and `error: str | None = None`.
  - `class EffectExecutor:` constructed as `EffectExecutor(*, kafka_resolver, http_resolver, emit_event)` where:
    - `kafka_resolver: Callable[[KafkaEffect], tuple[Any, Any]]` returns `(client, codec)` for the effect's resolved cluster+topic+format. `client` has `.produce(topic, value, *, key=None, headers=None, _raw=False) -> dict`; `codec` is the encode codec dict or `None`.
    - `http_resolver: Callable[[HttpEffect], tuple[Any, str]]` returns `(client, path)`. `client` has `.request(method, path, *, headers=None, body=None) -> dict` returning `{status_code, response_time_ms, headers(lowercased), body, url, method}`.
    - `emit_event: Callable[[dict], None]`.
  - Method: `run(self, effects: list, namespace: dict[str, CaptureValue], trigger_label: str) -> EffectOutcome`.
    - Returns `EffectOutcome(ok=True)` immediately if `effects` is falsy.
    - Iterates `effects` in order. For each:
      - `type == "kafka"`: `client, codec = kafka_resolver(effect)`; build the message list = `effect.values` if set else `[KafkaEffectMessage(value=effect.value, key=effect.key, headers=effect.headers)]`; for each item: `render_typed` over `item.value`/`item.key`/`item.headers` against `namespace`; `value_bytes, key_bytes = _encode_payload_with_codec(codec, effect.topic, rendered_value, rendered_key)`; `client.produce(effect.topic, value_bytes, key=key_bytes, headers=rendered_headers, _raw=True)`; `emit_event({"event":"kafka.produced","trigger":trigger_label,"topic":effect.topic,"key":rendered_key,"duration_ms":<ms>})` (one per item).
      - `type == "http"`: `client, path = http_resolver(effect)`; `render_typed` over `effect.body`/`effect.headers`; `resp = client.request(effect.method, path, headers=rendered_headers, body=rendered_body)`; `emit_event({"event":"http.called","trigger":trigger_label,"service":effect.service,"url":effect.url,"method":effect.method,"path":path,"status_code":resp["status_code"],"duration_ms":resp["response_time_ms"]})` (emit exactly one of `service`/`url`, whichever is set); if `effect.capture`: `typed, missing = resolve_captures(resp, effect.capture)`; for each `(name, from_)` in `missing` `emit_event({"event":"capture.missing","trigger":trigger_label,"name":name,"from":from_})`; then `namespace.update(typed)` (last-wins chaining — later effects see earlier http-effect captures).
      - On any `Exception` during an effect: `emit_event({"event":"effect.error","trigger":trigger_label,"effect_type":<"kafka"|"http">,"error":str(exc),"fatal":True, ...transport context: "topic" for kafka / "url" or "service" for http})` and `return EffectOutcome(ok=False, error=str(exc))` (short-circuit; no further effects run).
- Error cases: a `kafka_resolver`/`http_resolver` that raises (e.g. unresolved cluster, connection refused, undelivered produce) surfaces as `effect.error` via the surrounding try/except. The executor itself never raises.

- [ ] **Step 1: Write the failing tests**

  Tests use fakes (a `FakeKafkaClient` with `.produce(...)` recording calls and optionally raising; a `FakeHttpClient` with `.request(...)` returning a canned response dict and optionally raising; an `emit` list capturing event dicts). Build `EffectExecutor` with closures returning these fakes. Assert:
  - **Kafka single value:** `run([KafkaEffect(type="kafka", topic="t", value={"a":"{x}"})], {"x": CaptureValue("v","scalar")}, "s")` returns `EffectOutcome(ok=True)`; the fake client recorded one `produce("t", <bytes>, key=None, headers=None, _raw=True)`; `emit` has one `kafka.produced` with `trigger="s"`, `topic="t"`.
  - **Kafka multi-message:** `values=[KafkaEffectMessage(value=1), KafkaEffectMessage(value=2, key="k")]` ⇒ two `produce` calls and two `kafka.produced` events, in order.
  - **Http call + response capture chaining:** an `http` effect with `capture={"ackId": CaptureSpec(from_=".body.id")}` against a fake returning `{"status_code":200,"response_time_ms":5,"headers":{},"body":{"id":"A1"},"url":"u","method":"POST"}` ⇒ one `http.called` event with `status_code=200`; `namespace["ackId"].value == "A1"`. A second kafka effect in the same list whose value references `"{ackId}"` renders to `"A1"` (proves chaining).
  - **Capture last-wins:** an http effect captures `x`, a later http effect captures `x` again to a different value ⇒ `namespace["x"]` holds the later value.
  - **First-failure short-circuit:** a kafka effect whose fake `produce` raises ⇒ `emit` contains `effect.error` (`effect_type="kafka"`, `fatal=True`, `trigger`, `topic`); a subsequent effect in the list never runs (its fake records no call); return `EffectOutcome(ok=False, error=...)`.
  - **Http failure:** http fake `request` raises ⇒ `effect.error` with `effect_type="http"` and the `url`/`service` context.
  - **Empty/no effects:** `run([], namespace, "s")` returns `ok=True` and emits nothing.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_effects.py -v`
  Expected: FAIL — module `agctl.mock.effects` does not exist (import error).

- [ ] **Step 3: Write minimal implementation**

  Create `agctl/mock/effects.py` with `EffectOutcome` and `EffectExecutor` per the Produces contract. No `confluent_kafka`/`httpx` imports anywhere in the module. Use `time.perf_counter()` for `duration_ms` around each produce/call. The executor must not mutate the caller's expectation other than extending `namespace` in place (documented).

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_mock_effects.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/mock/effects.py tests/unit/test_mock_effects.py`
  Run: `git commit -m "feat(mock): add EffectExecutor dispatch core"`

---

### Task 3: Engine wiring + daemon taxonomy

**Files:**
- Modify: `agctl/mock/daemon.py` (`EVENT_TO_COUNTER` ~226-236; `FATAL_FAILURE_EVENTS` ~213-222)
- Modify: `agctl/mock/engine.py` (`__init__` ~54-72; instance attrs ~112-183; `emit_event` ~185-236; `_emit_summary_line` ~669-705; add executor construction)
- Test: `tests/unit/test_mock_engine.py`, `tests/unit/test_mock_daemon.py`

**Interfaces:**
- Consumes: `EffectExecutor` from Task 2 (`run(effects, namespace, trigger_label) -> EffectOutcome`); existing `MockEngine.__init__` signature; existing counter fields; `emit_event(self, line)` tallies counters (some via `EVENT_TO_COUNTER`, with explicit runtime-error branches for `kafka.error`/`grpc.unmatched`/`grpc.error`).
- Produces:
  - `daemon.py`: `EVENT_TO_COUNTER` gains `"kafka.produced": "kafka_produced"`, `"http.called": "http_called"`, `"effect.error": "effect_errors"`. `FATAL_FAILURE_EVENTS` gains `"effect.error"` (so `ALL_FAILURE_EVENTS = FATAL_FAILURE_EVENTS | {"capture.missing"}` auto-includes it).
  - `engine.py` `__init__` gains two keyword-only params: `kafka_resolver: Callable | None = None`, `http_resolver: Callable | None = None` (defaults `None`). The `new_mock_engine` seam in `commands/mock_commands.py` is updated in Task 6 to forward these; for this task, `MockEngine.__init__` simply accepts and stores them.
  - New instance attrs (init to `0`): `self._kafka_produced`, `self._http_called`, `self._effect_errors`.
  - New instance attr: `self._effect_executor: EffectExecutor | None`. Constructed in `__init__` as `EffectExecutor(kafka_resolver=kafka_resolver, http_resolver=http_resolver, emit_event=self.emit_event)` when at least one resolver is not `None`, else `None`.
  - `emit_event`: ensure `kafka.produced`/`http.called`/`effect.error` are counted (if counting is `EVENT_TO_COUNTER`-driven, the daemon change suffices; if explicit branches exist, add them). Add an explicit `effect.error` branch that sets `self._runtime_error = True` (mirror the `grpc.unmatched`/`grpc.error` unconditional set), so a fatal effect failure fails the run regardless of `--fail-fast`.
  - `_emit_summary_line`: add `"kafka_produced": self._kafka_produced`, `"http_called": self._http_called`, `"effect_errors": self._effect_errors` to the summary dict.
- The executor is NOT yet threaded into the HTTP server / reactor constructions (Tasks 4 and 5 do that, each updating its own construction call site in `engine.py`).

- [ ] **Step 1: Write the failing tests**

  In `test_mock_daemon.py`: assert `"effect.error" in FATAL_FAILURE_EVENTS`; `"effect.error" in ALL_FAILURE_EVENTS`; `EVENT_TO_COUNTER["kafka.produced"] == "kafka_produced"`; `EVENT_TO_COUNTER["http.called"] == "http_called"`; `EVENT_TO_COUNTER["effect.error"] == "effect_errors"`.
  In `test_mock_engine.py` (construct `MockEngine` with `run_http=False, run_kafka=False, http_listen="..."` and fake resolvers): assert `engine._effect_executor is not None` when a resolver is passed and `None` when neither is. Feed `engine.emit_event({"event":"effect.error","trigger":"s","effect_type":"kafka","error":"x","fatal":True})` then drive `engine.run()` to completion (or assert `_runtime_error is True` directly + that `_effect_errors == 1`) ⇒ the run's exit code is `1`. Feed `{"event":"kafka.produced",...}` and `{"event":"http.called",...}` ⇒ `_kafka_produced`/`_http_called` increment. Assert the `summary` line emitted by `_emit_summary_line` contains keys `kafka_produced`, `http_called`, `effect_errors`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_daemon.py tests/unit/test_mock_engine.py -v`
  Expected: FAIL — new taxonomy keys absent; `_effect_executor` attribute missing; summary keys missing.

- [ ] **Step 3: Write minimal implementation**

  Apply the `daemon.py` constant additions. In `engine.py`: add the two resolver params + three counters + executor construction; extend `emit_event` (counting + `effect.error` runtime-error branch); extend `_emit_summary_line`. Do not touch the server/reactor construction call sites yet.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_mock_daemon.py tests/unit/test_mock_engine.py -v`
  Expected: PASS. Run `pytest tests/unit/test_mock_engine.py tests/unit/test_mock_lifecycle.py -q` to confirm no engine/lifecycle regression.

- [ ] **Step 5: Commit**

  Run: `git add agctl/mock/daemon.py agctl/mock/engine.py tests/unit/test_mock_daemon.py tests/unit/test_mock_engine.py`
  Run: `git commit -m "feat(mock): wire EffectExecutor into engine + new event taxonomy"`

---

### Task 4: HTTP trigger — run effects before the response

**Files:**
- Modify: `agctl/mock/http_server.py` (`make_handler` ~59-63; `MockHTTPServer.__init__` ~380-408; `_handle_request` flow ~313-371)
- Modify: `agctl/mock/engine.py` (the `MockHTTPServer(...)` construction ~330-336, to pass the executor)
- Test: `tests/unit/test_mock_http_server.py`

**Interfaces:**
- Consumes: `EffectExecutor.run(effects, namespace, trigger_label) -> EffectOutcome` (Task 2); `engine._effect_executor` (Task 3). Existing `_handle_request` builds `captures: dict[str, CaptureValue]` (path captures at ~260 + implicit body + `resolve_captures` at ~301), renders the response (~315-320), acquires the semaphore (~341), emits `http.hit` (~352), sends the response (~364), releases (~367).
- Produces:
  - `make_handler(stubs, emit_event, semaphore, *, effect_executor=None)` gains keyword-only `effect_executor: EffectExecutor | None = None`.
  - `MockHTTPServer.__init__(..., *, stubs, emit_event, concurrency_cap=64, effect_executor=None)` passes `effect_executor` into `make_handler`.
  - In `_handle_request`, after the response is rendered and the semaphore is acquired (inside the `if semaphore.acquire(blocking=False):` block, before `_send_json_response`), if `effect_executor is not None and stub.effects`: call `effect_executor.run(stub.effects, captures, stub_name)` (where `stub_name` is the matched stub's key and `captures` is the already-built namespace). Ignore the returned `EffectOutcome` for response purposes — the response is still sent (default mode: `effect.error` is fatal at the run level via Task 3; the request still completes). Emit ordering: effects run before `http.hit` is emitted is acceptable, or after — pick before `_send_json_response`; the `http.hit` `duration_ms` then honestly includes effect latency.
  - `engine.py`: update the `MockHTTPServer(...)` construction (~330-336) to pass `effect_executor=self._effect_executor`.
- Error cases: an `effect_executor.run` that emits `effect.error` (handled in Task 3 → runtime-error flag → exit 1). The handler must not let an executor exception escape; `EffectExecutor.run` does not raise (it catches and emits), so no extra guard is needed — but the handler must still send the response in a `finally`/normal path.

- [ ] **Step 1: Write the failing tests**

  Stand up an `MockHTTPServer` on an ephemeral port with one stub that has `effects=[KafkaEffect(type="kafka", topic="t", value={"id":"{cust_id}"})]` and `capture={"cust_id": CaptureSpec(from_=".body.id")}`, injecting a fake `effect_executor` whose `run` records `(effects, namespace, label)`. Drive a real `POST` with body `{"id":"c1"}` (use `http.client` or `urllib`). Assert: the fake `run` was called once with `label=<stub name>`, the namespace contained `cust_id.value == "c1"`, and the response status/body are the stub's configured `response`. Assert response is sent (status received) even when the fake returns `EffectOutcome(ok=False)`. Assert a stub with no `effects` does not invoke the fake. Assert concurrency: when the semaphore is exhausted, effects are not run and a 429 is returned (existing behavior preserved).

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_http_server.py -v`
  Expected: FAIL — `make_handler`/`MockHTTPServer` reject `effect_executor`; effects not invoked.

- [ ] **Step 3: Write minimal implementation**

  Add the `effect_executor` param to `make_handler` and `MockHTTPServer.__init__`; call `effect_executor.run(...)` inside the semaphore-held block before `_send_json_response` when `stub.effects` is present; thread the executor through the handler closure. Update the engine's `MockHTTPServer(...)` construction to pass `effect_executor=self._effect_executor`.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_mock_http_server.py tests/unit/test_mock_engine.py -v`
  Expected: PASS (engine test confirms the executor is forwarded to the server).

- [ ] **Step 5: Commit**

  Run: `git add agctl/mock/http_server.py agctl/mock/engine.py tests/unit/test_mock_http_server.py`
  Run: `git commit -m "feat(mock): run HTTP-stub effects before the response"`

---

### Task 5: Kafka reactor — effects path (legacy `reaction` unchanged)

**Files:**
- Modify: `agctl/mock/kafka_reactor.py` (`__init__` ~43-54; `_handle` ~179-344, the react block ~260-320)
- Modify: `agctl/mock/engine.py` (the `KafkaReactor(...)` construction ~292-303, to pass the executor)
- Test: `tests/unit/test_mock_kafka_reactor.py`

**Interfaces:**
- Consumes: `EffectExecutor.run(effects, namespace, trigger_label) -> EffectOutcome` (Task 2); `engine._effect_executor` (Task 3). Existing `_handle(msg, *, attempt, final) -> ReactionResult`: decode-error skip → non-object skip → `jq_bool` match → capture (implicit value + `resolve_captures(msg, config.capture)` at ~248, building `capture_context`) → react block (render + `_encode_reaction` + `client.produce(_raw=True)` + emit `kafka.reacted`) → `COMMIT`; on exception `RETRY` (if not final) or emit `kafka.error` + `STOP if fail_fast else COMMIT`. `ReactionResult` enum: `COMMIT`/`RETRY`/`STOP`.
- Produces:
  - `KafkaReactor.__init__(..., reaction_codec=None, *, effect_executor=None)` gains keyword-only `effect_executor: EffectExecutor | None = None`; store as `self._effect_executor`.
  - In `_handle`, after the capture block builds `capture_context`, branch on config shape (guaranteed mutually exclusive by Task 1's validator):
    - **If `self._config.reaction is not None`:** the existing react block runs unchanged (render → `_encode_reaction` → `produce(_raw=True)` → emit `kafka.reacted` → `COMMIT`; existing try/except `RETRY`/`kafka.error`). Byte-for-byte unchanged.
    - **Elif `self._config.effects`:** call `outcome = self._effect_executor.run(self._config.effects, capture_context, self._name)`. If `outcome.ok`: `return ReactionResult.COMMIT`. If not `outcome.ok`: the executor already emitted `effect.error` (fatal, sets runtime flag via the engine); `return ReactionResult.STOP if self._fail_fast else ReactionResult.COMMIT` (mirror the existing failure branch's fail-fast choice). No retry on effect failure.
  - `engine.py`: update the `KafkaReactor(...)` construction (~292-303) to pass `effect_executor=self._effect_executor`.
- Error cases: executor failure ⇒ `effect.error` (fatal) + `COMMIT` (or `STOP` under `--fail-fast`); the trigger message is never retried. A reactor with `effects` but no `effect_executor` wired (should not happen — engine always builds one when effects exist) is an internal error; defensive: if `self._effect_executor is None and self._config.effects`, emit `kafka.error` and `COMMIT`.

- [ ] **Step 1: Write the failing tests**

  Build a `KafkaReactor` with a `KafkaReactorConfig` that has `effects=[HttpEffect(type="http", url="https://x", method="POST", body={"id":"{orderId}"})]`, `capture={"orderId": CaptureSpec(from_=".value.id")}`, `match=None`, `reaction=None`; inject a fake `client` (with `consume_loop`/`probe` no-ops) and a fake `effect_executor` whose `run` records calls. Call `reactor._handle({"value":{"id":"o1"}, "offset":0, "partition":0, "key":None, "timestamp":0, "headers":{}}, attempt=1, final=True)` ⇒ assert fake `run` was called with `label=reactor name`, namespace `orderId.value=="o1"`; returns `ReactionResult.COMMIT`. Repeat with the fake returning `EffectOutcome(ok=False)`, `fail_fast=False` ⇒ `COMMIT`; `fail_fast=True` ⇒ `STOP`. **Back-compat:** a reactor with a real `reaction` (no effects) and `effect_executor=None` ⇒ `_handle` produces via the existing path and the fake executor is never called; emit is `kafka.reacted` (not `kafka.produced`).

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_kafka_reactor.py -v`
  Expected: FAIL — `effect_executor` param absent; effects branch absent.

- [ ] **Step 3: Write minimal implementation**

  Add the `effect_executor` param + attribute; add the `reaction`/`effects` branch in `_handle` leaving the legacy react block untouched; update the engine's reactor construction to pass `effect_executor=self._effect_executor`.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_mock_kafka_reactor.py tests/unit/test_mock_reactor_codec.py tests/unit/test_mock_engine.py -v`
  Expected: PASS (reactor-codec back-compat tests unaffected — they exercise the legacy `reaction` path).

- [ ] **Step 5: Commit**

  Run: `git add agctl/mock/kafka_reactor.py agctl/mock/engine.py tests/unit/test_mock_kafka_reactor.py`
  Run: `git commit -m "feat(mock): Kafka-reactor effects path (reaction unchanged)"`

---

### Task 6: Command-layer wiring — resolvers, effect clients, service checks

**Files:**
- Modify: `agctl/commands/mock_commands.py` (`mock_run` body ~504-626; `new_mock_engine` seam ~59-102; imports ~36-49)
- Test: `tests/unit/test_mock_commands.py`

**Interfaces:**
- Consumes (from `kafka_commands`, already imported): `resolve_cluster_name(cfg.kafka, *, explicit=, binding_cluster=) -> str`; `_resolve_codec(cfg, topic, cluster_name, cli_value_fmt, cli_key_fmt, *, probe=True) -> (codec, value_fmt, key_fmt)`; `new_kafka_client(cluster, group_id=None, *, codec=None)`. Existing `clients_by_cluster: dict[str, KafkaClient]` and `probed_clusters: set[str]` built in the reactor loop (~508-581). From `http_commands`: `_split_url(url) -> (base_url, path)` (url-mode split; if not importable, use `urllib.parse.urlsplit` to derive scheme+netloc → base_url and path+query → path). `HttpClient(base_url, timeout_seconds, *, transport=None, headers=None)` with `.request(method, path, *, headers=None, body=None)`.
- Produces (inside `mock_run`, after the existing reactor client/codec block):
  - Walk every effect across `cfg.mocks.http.stubs.<s>.effects` and `cfg.mocks.kafka.reactors.<r>.effects`.
  - **kafka resolver** (a closure assigned to `kafka_resolver`, capturing `cfg`, `clients_by_cluster`, `probed_clusters`): given a `KafkaEffect`, derive the binding cluster = `cfg.kafka.topics[effect.topic].cluster` if `effect.topic` is declared in `cfg.kafka.topics` else `None`; `cluster = resolve_cluster_name(cfg.kafka, explicit=effect.cluster, binding_cluster=binding)`; if `cluster` not in `clients_by_cluster`: `clients_by_cluster[cluster] = new_kafka_client(cfg.kafka.clusters[cluster])` (codec=None — produce is `_raw`, encode done by the executor); `codec, _vf, _kf = _resolve_codec(cfg, effect.topic, cluster, effect.value_format, effect.key_format, probe=(cluster not in probed_clusters))`; add `cluster` to `probed_clusters` if the codec probe ran; cache `(client, codec)` keyed by `(cluster, effect.topic, effect.value_format, effect.key_format)`; return `(clients_by_cluster[cluster], codec)`.
  - **http resolver** (`http_resolver`, capturing `cfg`): given an `HttpEffect`, if `effect.service` is not None: require `effect.service in cfg.services` else raise `ConfigError` (path `mocks.{http.stubs,kafka.reactors}.<name>.effects[i].service`); `base_url = cfg.services[effect.service].base_url`; `path = effect.path`; `timeout = effect.timeout or cfg.services[effect.service].timeout_seconds or cfg.defaults.timeout_seconds`. Else (url mode): `(base_url, path) = _split_url(effect.url)`; `timeout = effect.timeout or cfg.defaults.timeout_seconds`. Cache an `HttpClient(base_url, timeout)` per `base_url` (reuse for connection pooling); return `(client, path)`.
  - Pass `kafka_resolver=kafka_resolver or None`, `http_resolver=http_resolver or None` to `new_mock_engine(...)` (only when any effects exist; else omit/None so the engine builds no executor).
  - `new_mock_engine` seam (~59-102): add the two keyword-only params and forward to `MockEngine(...)`.
- Error cases: unknown http-effect `service` → `ConfigError` (exit 2) with the dotted path. Unresolvable kafka-effect cluster (via `resolve_cluster_name`) → `ConfigError`. These surface during `mock_run` startup (before the engine binds), consistent with the existing reactor cluster resolution (~518-525).

- [ ] **Step 1: Write the failing tests**

  In `test_mock_commands.py`, test `mock_run`'s resolver-building via monkeypatches (monkeypatch `new_mock_engine` to capture kwargs; monkeypatch `new_kafka_client`/`_resolve_codec` with fakes; provide a minimal `cfg` via a fixture or `load_config`). Assert:
  - A config with an HTTP stub carrying a kafka effect whose `cluster` is not used by any reactor ⇒ `new_kafka_client` was called for that cluster and `kafka_resolver` is passed to the engine.
  - `kafka_resolver(effect)` returns a client whose cluster matches and a codec matching `_resolve_codec`'s return; calling it twice for the same `(cluster, topic, fmt)` returns the cached client/codec and does not re-probe.
  - A config with an http effect `service="order-service"` where `order-service` exists ⇒ `http_resolver` passed; the resolver returns an `HttpClient` bound to `services.order-service.base_url` and the effect's `path`.
  - An http effect referencing an unknown service ⇒ `mock_run` raises `ConfigError` before constructing the engine.
  - A config with no effects anywhere ⇒ neither resolver is passed (`None`).

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_mock_commands.py -v`
  Expected: FAIL — resolvers not built/passed.

- [ ] **Step 3: Write minimal implementation**

  In `mock_run`, after the reactor client/codec block, add the effect walk + the two resolver closures + the unknown-service guard; pass the resolvers to `new_mock_engine`. Extend the `new_mock_engine` seam signature and forwarding. Use the existing `_resolve_codec` probe dedup via `probed_clusters`.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_mock_commands.py tests/unit/test_mock_engine.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/commands/mock_commands.py tests/unit/test_mock_commands.py`
  Run: `git commit -m "feat(mock): build effect resolvers + clients in mock_run"`

---

### Task 7: Cross-reference validation (`config/validator.py`)

**Files:**
- Modify: `agctl/config/validator.py` (append checks before `return errors, warnings` ~407; mirror the reactor check ~239-290)
- Test: `tests/unit/test_config_validator.py`

**Interfaces:**
- Consumes: `validate_config(cfg) -> tuple[list[dict], list[dict]]` (entries `{"path","message"}`); the reactor cluster-resolution block (~239-290) and topic block (~123-199) as templates. **`config/` may not import `commands/`** — cluster-name resolution (`explicit → topic.cluster → default_cluster → single`) must be re-inlined (as the existing checks do).
- Produces (new checks appended before `return`):
  - For each `mocks.http.stubs.<s>.effects[i]` where `effect.type == "http"` and `effect.service is not None`: if `effect.service not in cfg.services` → error at `mocks.http.stubs.<s>.effects[i].service` ("http effect references unknown service '<s>'").
  - For each `mocks.kafka.reactors.<r>.effects[i]` where `effect.type == "http"`: same unknown-service check at `mocks.kafka.reactors.<r>.effects[i].service`.
  - For each kafka effect (http stubs + kafka reactors) at index `i`: resolve its cluster inline (explicit `effect.cluster` → `cfg.kafka.topics[effect.topic].cluster` if declared → `cfg.kafka.default_cluster` → single-cluster auto-default); if unresolvable → error at `<trigger>.effects[i]` ("kafka effect requires a resolvable cluster"); elif `cfg.kafka.clusters[resolved].brokers` empty → error at `<trigger>.effects[i]` ("kafka effect requires kafka.clusters.<resolved>.brokers"). If the resolved value format (effect `value_format`/`key_format` else `cfg.kafka.topics[effect.topic].value_format`/`key_format` else cluster defaults) is `avro`/`protobuf` and the cluster has no `schema_registry_url` → error at `<trigger>.effects[i]` (or at the cluster, mirroring the topic check ~154-182).
  - (The reaction/effects mutex and ≥1 rule are enforced on the model in Task 1; no duplicate check needed here.)
- Error cases: every violation appends one `{"path","message"}` to `errors`. `mocks is None` ⇒ no checks (guarded).

- [ ] **Step 1: Write the failing tests**

  Build `Config` fixtures (or load minimal YAML) and call `validate_config(cfg)`:
  - http effect with `service="missing"` ⇒ one error at `mocks.http.stubs.<s>.effects[0].service`.
  - kafka effect whose `cluster="nope"` not in `cfg.kafka.clusters` ⇒ error at `mocks.http.stubs.<s>.effects[0]` (or reactor path).
  - kafka effect resolving to a cluster with empty `brokers` ⇒ error.
  - kafka effect with `value_format: avro` whose resolved cluster lacks `schema_registry_url` ⇒ error.
  - Valid config (all refs resolve, JSON format) ⇒ no effect-related errors.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_config_validator.py -v`
  Expected: FAIL — no effect cross-ref checks.

- [ ] **Step 3: Write minimal implementation**

  Append the effect cross-ref blocks (http-effect service, kafka-effect cluster/brokers/format) to `validate_config`, re-inlining cluster resolution. Reuse the existing `_missing_description`/append style.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_config_validator.py tests/unit/test_validator.py -v`
  Expected: PASS (no existing cross-ref regression).

- [ ] **Step 5: Commit**

  Run: `git add agctl/config/validator.py tests/unit/test_config_validator.py`
  Run: `git commit -m "feat(mock): validate effect service/cluster/format cross-refs"`

---

### Task 8: Startup walks — jq precompile + capture placement

**Files:**
- Modify: `agctl/mock/jq_precompile.py` (`iter_mock_jq_expressions` ~28-79)
- Modify: `agctl/mock/capture_validate.py` (`collect_capture_placement_errors` ~76, http-stub walk ~99-116)
- Test: `tests/unit/test_jq_precompile.py`, `tests/unit/test_mock_capture_validate.py`

**Interfaces:**
- Consumes: `iter_mock_jq_expressions(mocks) -> Iterator[tuple[str,str]]` (yields `(label, expr)`; currently yields http stub `match.jq` + `capture.<name>.from`, reactor `match` + `capture.<name>.from`, grpc `match.jq` + `capture.<name>.from`). `collect_capture_placement_errors(mocks) -> list[dict]` (walks http stub `capture`/`response.body`, reactor `capture`/`reaction.*`, grpc).
- Produces:
  - `iter_mock_jq_expressions`: additionally yield each http effect's `capture.<name>.from` (label `mocks.http.stubs.<s>.effects[i].capture.<name>.from`) and each http effect on kafka reactors likewise (`mocks.kafka.reactors.<r>.effects[i].capture.<name>.from`). (kafka effects have no capture; nothing to yield for them.)
  - `collect_capture_placement_errors`: for each http effect with a `capture` entry whose `type == "object"`, validate placement against the fields of *subsequent* effects in the same list (the whole-object `{name}` placeholder may appear in a later effect's `value`/`body`/`path`/`key`/`headers`, mirroring how trigger object-captures are checked against `response.body`). Report a placement error per violation at `...effects[i].capture.<name>` with the existing message style. If a whole-object capture is unused by any later effect, that is allowed (not an error) — matches the soft posture for chained captures.
- Error cases: a malformed jq `from` in an http-effect capture → `compile_jq` raises `ConfigError` at startup (Step 0) / surfaces via `collect_jq_compile_errors` in `config validate`. Never raises from the walker itself.

- [ ] **Step 1: Write the failing tests**

  In `test_jq_precompile.py`: a config with an http effect `capture={"x": CaptureSpec(from_=".body.id")}` ⇒ `iter_mock_jq_expressions` yields `("mocks.http.stubs.<s>.effects[0].capture.x.from", ".body.id")`. In `test_mock_capture_validate.py`: an http effect `capture={"ctx": CaptureSpec(from_=".body.ctx", type="object")}` whose `ctx` is used as `{ctx}` in a later effect's `body` ⇒ no error; the same object capture used in a nested position (e.g. `"v": "{ctx.inner}"`) ⇒ one placement error at the capture path.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_jq_precompile.py tests/unit/test_mock_capture_validate.py -v`
  Expected: FAIL — walker/validator do not cover effects.

- [ ] **Step 3: Write minimal implementation**

  Extend `iter_mock_jq_expressions` to walk `mocks.http.stubs.<s>.effects` and `mocks.kafka.reactors.<r>.effects`, yielding http-effect `capture.<name>.from`. Extend `collect_capture_placement_errors` to walk http-effect captures and check object-capture placement against subsequent effects' fields.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_jq_precompile.py tests/unit/test_mock_capture_validate.py tests/unit/test_mock_grpc_dispatch.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/mock/jq_precompile.py agctl/mock/capture_validate.py tests/unit/test_jq_precompile.py tests/unit/test_mock_capture_validate.py`
  Run: `git commit -m "feat(mock): precompile + validate http-effect capture paths"`

---

### Task 9: `config validate` — unknown `{{…}}` tokens in effect fields

**Files:**
- Modify: `agctl/commands/config_commands.py` (`collect_unknown_template_errors` mock walk ~183-232)
- Test: `tests/unit/test_config_commands.py`

**Interfaces:**
- Consumes: `collect_unknown_template_errors(cfg) -> list[dict]`; the inner `check(path, value)` closure (uses `find_unknown_templates(value) -> list[str]`).
- Produces: extend the `mocks.http.stubs` loop to `check` each effect's fields; extend the `mocks.kafka.reactors` loop likewise. Per effect (by index `i`):
  - kafka effect: `check(f"mocks.http.stubs.<s>.effects[i].topic", effect.topic)`; `check(...effects[i].key", effect.key)`; `check(...effects[i].value", effect.value)`; for each `values[j]`: `check(...effects[i].values[j].value", ...)`, `.key`, `.headers`; `check(...effects[i].headers", effect.headers)`.
  - http effect: `check(...effects[i].url", effect.url)`; `check(...effects[i].path", effect.path)`; `check(...effects[i].body", effect.body)`; `check(...effects[i].headers", effect.headers)`.
  - (Same paths under `mocks.kafka.reactors.<r>.effects[i]...` for reactor effects.)
- Error cases: any unknown generator token (e.g. `{{foo}}`) → one `{"path","message"}` per token, naming the valid generators (existing `_template_token_message`). Never raises.

- [ ] **Step 1: Write the failing tests**

  In `test_config_commands.py`, load a config with a kafka effect `value: {"id":"{{uuid}}","bad":"{{foo}}"}` and an http effect `url: "https://x/{{bar}}"`. Run `collect_unknown_template_errors(cfg)`. Assert errors at `mocks.http.stubs.<s>.effects[0].value` (token `{{foo}}`) and `...effects[1].url` (token `{{bar}}`); assert `{{uuid}}` is not flagged. Add a reactor-effects variant asserting the `mocks.kafka.reactors.<r>.effects[i]...` paths.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_config_commands.py -v`
  Expected: FAIL — effect fields not walked.

- [ ] **Step 3: Write minimal implementation**

  Add the effect-field `check(...)` calls inside the existing stub/reactor loops in `collect_unknown_template_errors`.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_config_commands.py tests/unit/test_config_validate.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/commands/config_commands.py tests/unit/test_config_commands.py`
  Run: `git commit -m "feat(mock): scan effect fields for unknown generator tokens"`

---

### Task 10: Integration — live HTTP→Kafka, Kafka→HTTP, chaining, fatal

**Files:**
- Create: `tests/integration/test_mock_effects.py`

**Interfaces:**
- Consumes: the full stack (Tasks 1-9); a live plaintext Kafka broker (`AGCTL_TEST_LIVE=1`); the existing mock integration harness pattern (`tests/integration/test_mock_commands.py`). `agctl mock run` (streaming NDJSON), `agctl kafka consume`, `agctl http request`.
- Produces: end-to-end coverage that the unit-tested seams compose correctly over real sockets and a real broker.
- Self-skip: the module skips (not fails) when `AGCTL_TEST_LIVE` is unset, exactly like the existing integration suite.

- [ ] **Step 1: Write the failing tests**

  Each test starts `agctl mock run` (background, stdout→log), polls the log for `started`, drives the SUT-facing side, then `SIGTERM` + `wait` and greps the log:
  - **HTTP→Kafka (single):** a stub `POST /fire` with a kafka effect → `orders.events`. `agctl http request --url http://<mock>/fire --method POST --body '{"id":"o1"}'`; then `agctl kafka consume --topic orders.events --timeout 10 --expect-count 1 --match '.value.id == "o1"'` ⇒ exit 0. The mock log contains one `kafka.produced` with `topic=orders.events`.
  - **HTTP→Kafka (multi-message `values`):** one `values` list of two ⇒ `agctl kafka consume --expect-count 2`; log has two `kafka.produced`.
  - **Kafka→HTTP (callback):** a reactor on `commands.t` with an http effect calling a second mock stub (a capture sink stub on the same mock) that records the body. Produce to `commands.t` via `agctl kafka produce`; assert the sink stub saw a hit with the rendered body (via its `http.hit` in the log) and the log has one `http.called`.
  - **Chaining:** a reactor with `[http effect (capture ackId from response), kafka effect (value uses {ackId})]` ⇒ after producing the trigger, `kafka consume` on the audit topic sees a message carrying the captured `ackId`.
  - **Fatal `effect.error`:** a kafka effect targeting a non-existent topic on a broker that rejects it (or an http effect whose service is unreachable) ⇒ the log contains `effect.error` (`fatal:true`); `mock run` exits `1`; `mock stop` (daemon mode) raises `AssertionFailure`.
  - **Back-compat regression:** a reactor with a legacy `reaction` (no effects) ⇒ the log emits `kafka.reacted` (not `kafka.produced`) and the `summary` still contains the pre-change keys.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `AGCTL_TEST_LIVE=1 pytest tests/integration/test_mock_effects.py -v`
  Expected: FAIL (or skip if the flag is unset — confirm skip logic first).

- [ ] **Step 3: Write minimal implementation**

  No production code in this task — only the integration tests. If a test reveals a wiring gap, fix the offending unit-level seam (Tasks 1-9) rather than papering over it here. Ensure the module's skip guard matches the existing integration suite.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `AGCTL_TEST_LIVE=1 pytest tests/integration/test_mock_effects.py -v`
  Expected: PASS. Also run the full mock unit + integration suites: `pytest tests/unit/test_mock_*.py tests/integration/test_mock_*.py -q` ⇒ all green, confirming no regression.

- [ ] **Step 5: Commit**

  Run: `git add tests/integration/test_mock_effects.py`
  Run: `git commit -m "test(mock): integration for cross-transport effects"`

---

## Docs sync (post-implementation)

Per project `CLAUDE.md`, invoke the `docs-watcher` subagent at implementation finish to sync DESIGN.md (§2.1 `effects` schema, §3.6 lifecycle/events, §10 cross-transport → done) and ARCHITECTURE.md (§3 `mock/effects.py`, §4 effect trace, §6 new events, §10). The spec already records the intended doc deltas in its §14.

---

## TL;DR

Ten TDD tasks on branch `feat/mock-cross-transport-effects`: (1) `Effect` union + `effects` on stubs/reactors in `config/models.py`; (2) `EffectExecutor` dispatch core in `mock/effects.py`; (3) engine wiring + daemon taxonomy; (4) HTTP-trigger invocation before the response; (5) Kafka-reactor effects path with legacy `reaction` unchanged; (6) `mock_run` resolver/client/service wiring; (7) cross-ref validation; (8) jq-precompile + capture-placement walks; (9) `config validate` generator-token scan; (10) live integration. Three spec refinements are recorded up front (legacy `reaction` stays a distinct path; jq_precompile does cover effect captures; `ServiceConfig` has no TLS fields).

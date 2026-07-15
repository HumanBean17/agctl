# `agctl kafka listen` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `kafka listen` command group — a long-lived capture daemon that drains Kafka topics live from `start`, persists captures to disk (immune to scan windows and broker retention), and lets an agent attach assertions and collect results at the end of a runbook or debug session.

**Architecture:** A managed-daemon pattern mirroring `agctl mock start/stop/status`. The daemon is a pure capture writer (one `consume_loop` per topic, seek-to-end on assignment); all other subcommands read on-disk state with no IPC. Generic daemon primitives are extracted to a shared `agctl/daemon.py`; listen-specific lifecycle/capture/assert logic lives in a new `agctl/listen/` subpackage; the seven Click commands live in `agctl/commands/kafka_listen_commands.py`.

**Tech Stack:** Python ≥3.11, Click, Pydantic v2, confluent-kafka (lazy, `kafka` extra), jq (lazy), pytest. Stateless-per-invocation except the new `listen` daemon carve-out (second after `mock`).

## Global Constraints

- Every non-streaming command emits exactly one JSON envelope via `@envelope` and exits 0/1/2; `kafka listen run` is the sole streaming exception (NDJSON, like `mock run`).
- Lazy-import `confluent_kafka`/`jq`; a missing library surfaces as `ConfigError` (exit 2) pointing at `pip install 'agctl[kafka]'`, never a crash.
- `start`/`stop`/`status` are gated to POSIX/WSL via `require_posix_daemon()`; `run` is cross-platform foreground (native-Windows fallback).
- No new config section. Subscriptions come from `--topic` / `--pattern` (patterns reused from `kafka.patterns`: `KafkaPattern{description, topic, match, cluster}`). No `models.py` changes.
- Inward-only dependency direction is preserved: the new shared `agctl/daemon.py` imports only `{errors}`; `listen/*` imports `{errors, assertions, clients, config.models}`; commands import as mock_commands does.

## Shared Types & Paths (referenced by every task)

- **State layout** under `--state-dir` (default `./.agctl`):
  - pidfile: `<state_dir>/listen-<run_id>.pid` — JSON `{pid, run_id, topics:[str], group:str, cluster:str, started_at:str(ISO-Z), state_dir:str, log_path:str}`
  - run dir: `<state_dir>/listen-<run_id>/`
  - `<run_dir>/meta.json` — `{run_id, topics:[str], group:str, cluster:str, started_at:str, capture_match:str|null, max_bytes_per_topic:int}`
  - `<run_dir>/asserts.jsonl` — one **ExpectationSpec** per line (written by `assert`)
  - `<run_dir>/<topic>.ndjson` — one **CapturedEnvelope** per line (written by the daemon)
  - `<run_dir>/events.log` — daemon stdout (NDJSON lifecycle events; parsed by `stop`/`status`)
- **run_id**: `secrets.token_hex(4)` (8 hex chars). Consumer group: `f"agctl-listen-{run_id}"`.
- **CapturedEnvelope** (one NDJSON line): `{topic:str, key:str|null, value:any, partition:int, offset:int, timestamp:str|null, headers:dict[str,str], captured_at:str(ISO-Z)}`. `value` is JSON-decoded when parseable else the raw decoded string (mirrors `KafkaClient._normalize_message`, which returns the same minus `topic`/`captured_at`).
- **ExpectationSpec** (one asserts.jsonl line): `{id:str, topic:str, modes:{"contains":any|None, "match":str|None, "pattern":str|None, "path":str|None}, params:dict[str,str], expect_count:int}`.
- **ExpectationResult**: `{id:str, topic:str, passed:bool, matched_count:int, expect_count:int, modes:list[dict], detail:dict|None}`. `passed` is `matched_count >= expect_count`.
- **max-bytes-per-topic default**: 268435456 (256 MiB), default-ON (D4). `0` means unlimited.

---

## Task 1: Extract generic daemon primitives to `agctl/daemon.py` (D8)

**Files:**
- Create: `agctl/daemon.py`
- Modify: `agctl/commands/mock_commands.py` (remove `spawn_daemon`, `_terminate`, `_require_posix_daemon`; import them from `agctl.daemon`)
- Modify: `agctl/mock/daemon.py` (remove `is_alive`, `read_pidfile`, `write_pidfile`, `remove_pidfile`; import them from `agctl.daemon`)
- Test: `tests/unit/test_daemon_primitives.py`

**Interfaces:**
- Produces `agctl/daemon.py` exporting (signatures unchanged from their current mock homes):
  - `spawn_daemon(argv: list[str], log_path: str, env: dict | None = None) -> int` (current `mock_commands.spawn_daemon`)
  - `terminate(pid: int, timeout: float) -> str` (current `_terminate`; returns `"SIGTERM"|"SIGKILL"`)
  - `require_posix_daemon() -> None` (current `_require_posix_daemon`; raises `ConfigError` when `os.name == "nt"`)
  - `is_alive(pid: int) -> bool`
  - `read_pidfile(path: Path) -> dict | None`, `write_pidfile(path: Path, data: dict) -> None`, `remove_pidfile(path: Path) -> None`
- Consumes: `errors.ConfigError` (only). No mock/listen imports.
- mock modules re-export the moved names (e.g. `from ..daemon import spawn_daemon, terminate as _terminate, require_posix_daemon as _require_posix_daemon`) so existing mock call sites and tests that reference them keep working.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_daemon_primitives.py` verifies the new module is the source of the generic primitives and they still behave: `from agctl import daemon` exposes `spawn_daemon`, `terminate`, `require_posix_daemon`, `is_alive`, `read_pidfile`, `write_pidfile`, `remove_pidfile`; `write_pidfile` then `read_pidfile` round-trips a dict in a `tmp_path`; `remove_pidfile` on a missing file is a no-op; `is_alive(os.getpid())` is True; `require_posix_daemon()` on `os.name != "nt"` returns None (monkeypatch `agctl.daemon.os.name` to `"posix"`).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_daemon_primitives.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'agctl.daemon'`.

- [ ] **Step 3: Write minimal implementation**

Create `agctl/daemon.py` by moving the five function bodies verbatim from `mock_commands.py` (`spawn_daemon`, `_terminate`, `_require_posix_daemon`) and `mock/daemon.py` (`is_alive`, `read_pidfile`, `write_pidfile`, `remove_pidfile`), renaming `_terminate`→`terminate` and `_require_posix_daemon`→`require_posix_daemon`. In `mock_commands.py`, delete the three definitions and add `from ..daemon import spawn_daemon, terminate as _terminate, require_posix_daemon as _require_posix_daemon`. In `mock/daemon.py`, delete the four definitions and add `from ..daemon import is_alive, read_pidfile, write_pidfile, remove_pidfile`. Keep `mock/daemon.py`'s mock-specific code (`RunningMock`, `pidfile_path`, `log_path`, `list_running_mocks`, `resolve_target`, `parse_log`, failure taxonomies) untouched.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_daemon_primitives.py tests/unit/ -k mock -v`
Expected: PASS — new test passes AND every existing mock unit test still passes (behavior-preserving move).

- [ ] **Step 5: Commit**

Run: `git add agctl/daemon.py agctl/commands/mock_commands.py agctl/mock/daemon.py tests/unit/test_daemon_primitives.py`
Run: `git commit -m "refactor: extract generic daemon primitives to agctl/daemon.py (D8)"`

---

## Task 2: Listen lifecycle helpers (`agctl/listen/daemon.py`)

**Files:**
- Create: `agctl/listen/__init__.py`, `agctl/listen/daemon.py`
- Test: `tests/unit/test_listen_daemon.py`

**Interfaces:**
- Consumes (from Task 1): `agctl.daemon.{is_alive, read_pidfile, write_pidfile, remove_pidfile}`, `errors.ConfigError`.
- Produces pure functions (no Kafka, no network — unit-testable with temp dirs), mirroring `mock/daemon.py`'s shape but keyed by `run_id`:
  - `new_run_id() -> str` — `secrets.token_hex(4)`.
  - `pidfile_path(state_dir: Path, run_id: str) -> Path` → `state_dir / f"listen-{run_id}.pid"`.
  - `run_dir(state_dir: Path, run_id: str) -> Path` → `state_dir / f"listen-{run_id}"`.
  - `events_log_path(run_dir: Path) -> Path` → `run_dir / "events.log"`.
  - `capture_path(run_dir: Path, topic: str) -> Path` → `run_dir / f"{topic}.ndjson"`.
  - `meta_path(run_dir: Path) -> Path` → `run_dir / "meta.json"`.
  - `asserts_path(run_dir: Path) -> Path` → `run_dir / "asserts.jsonl"`.
  - `RunningListener` frozen dataclass: `{pid:int, run_id:str, topics:list[str], group:str, cluster:str, started_at:str, state_dir:str, log_path:str, pidfile_path:Path}`.
  - `write_meta(run_dir: Path, meta: dict) -> None`; `read_meta(run_dir: Path) -> dict | None`.
  - `list_running_listeners(state_dir: Path) -> list[RunningListener]` — glob `listen-*.pid`, build `RunningListener` from each live pidfile, clean stale (dead-pid) pidfiles; no error on missing/empty dir.
  - `resolve_listener_target(state_dir, *, run_id: str|None, pid: int|None, all_: bool) -> list[RunningListener]` — `all_`→all; `pid`→match; `run_id`→match; else implicit-singleton; raise `ConfigError` when multiple running and no selector, or selector matches nothing.
  - `append_expectation(run_dir: Path, spec: ExpectationSpec) -> None` — append one JSON line to asserts.jsonl (mkdir parents).
  - `read_expectations(run_dir: Path) -> list[dict]` — parse asserts.jsonl, skip blank/unparseable lines.
  - `parse_events_log(path: Path) -> ParsedEvents` where `ParsedEvents` = `{started: dict|None, startup_error: dict|None, summary: dict|None, overflow_topics: list[str], errors: list[dict]}`. Recognizes event types `started`, `summary`, `capture.overflow` (collects the `topic`), `kafka.error`; a line with `ok is False` is `startup_error`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_listen_daemon.py` covers: `pidfile_path`/`run_dir`/`capture_path`/`events_log_path` derive the exact paths from the Shared Types section; `write_meta`/`read_meta` round-trip; `append_expectation` then `read_expectations` round-trips two specs and preserves order; `list_running_listeners` on a temp dir with one live-pid pidfile (`os.getpid()`) returns one `RunningListener` and cleans a stale (dead-pid) pidfile; `resolve_listener_target` returns the singleton when one running, raises `ConfigError` when two running and no selector; `parse_events_log` on a canned NDJSON string (a `started` line, a `capture.overflow` line for topic `T`, a `summary` line, and a startup-error envelope) populates `started`, `overflow_topics==["T"]`, `summary`, `startup_error` correctly.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_listen_daemon.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'agctl.listen'`.

- [ ] **Step 3: Write minimal implementation**

Implement `agctl/listen/daemon.py` to satisfy the Produces contracts above using only stdlib (`json`, `secrets`, `pathlib`, `errno`, `os`) plus the Task-1 primitives and `ConfigError`. No Kafka imports. `parse_events_log` mirrors `mock/daemon.parse_log`'s line-by-line JSON parse with the listen event vocabulary.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_listen_daemon.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/listen/__init__.py agctl/listen/daemon.py tests/unit/test_listen_daemon.py`
Run: `git commit -m "feat(listen): add lifecycle helpers (run_id keying, state paths, events-log parser)"`

---

## Task 3: Capture-file reader (`agctl/listen/capture_file.py`)

**Files:**
- Create: `agctl/listen/capture_file.py`
- Test: `tests/unit/test_listen_capture_file.py`

**Interfaces:**
- Consumes: `agctl.commands.kafka_commands._build_assert_predicate(*, needle, match, path, filled_pattern_match) -> Callable[[dict], bool]` (predicate roots `--contains`/`--path` at `msg["value"]`, `--match`/`--pattern` at the whole `msg`); `agctl.assertions.compile_jq(expr, *, label)`; `agctl.resolution.fill_placeholders(template, params)`; `agctl.params.parse_params`.
- Produces pure functions over a `<topic>.ndjson` file:
  - `iter_messages(path: Path) -> Iterator[dict]` — yield each parsed CapturedEnvelope; skip blank/unparseable lines.
  - `count_matching(path: Path, predicate: Callable[[dict], bool]) -> tuple[int, int]` → `(matched_count, scanned_count)`, first-match short-circuit NOT used here (counts all).
  - `first_matching(path: Path, predicate) -> tuple[dict | None, int]` → first match (or None) + scanned count; stops at first match.
  - `read_messages(path: Path, *, predicate: Callable[[dict],bool] | None, limit: int) -> dict` → `{matched:int, truncated:bool, messages:list[dict]}` applying optional predicate then capping at `limit` (truncated True if more matched).
  - `build_predicate(spec: dict) -> Callable[[dict], bool]` — translate an ExpectationSpec into the args for `_build_assert_predicate`: parse `modes.contains` (JSON) into `needle`; fill `modes.pattern` by looking up... (pattern resolution happens in the command layer, not here — `build_predicate` receives already-resolved modes). Contract: `build_predicate` takes `{contains:any|None, match:str|None, path:str|None, filled_pattern_match:str|None}` and returns the predicate, after `compile_jq`-validating each present jq expression (loud-on-typo, exit 2).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_listen_capture_file.py` writes a temp `<topic>.ndjson` with 4 CapturedEnvelope lines (two with `value.eventType=="ORDER_CREATED"`, two with other types) and asserts: `iter_messages` yields 4; `count_matching` with predicate `lambda m: m["value"]["eventType"]=="ORDER_CREATED"` returns `(2, 4)`; `first_matching` returns the first matching envelope and scanned count; `read_messages` with `limit=1` returns `{matched:2, truncated:True, messages:[<one>]}`. A second test feeds `build_predicate({match: '.value.eventType == "X"'})` and asserts the returned predicate is True for a matching envelope and False for a non-match; feeding a syntactically bad `match` raises `ConfigError`.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_listen_capture_file.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Write minimal implementation**

Implement the functions above. `build_predicate` parses `contains` via `json.loads`, `compile_jq`-validates `match`/`path`/`filled_pattern_match` (raising `ConfigError` on a malformed expression), then delegates to `_build_assert_predicate`. Predicate exceptions per-message are swallowed → treated as non-match (consistent with `kafka assert`).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_listen_capture_file.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/listen/capture_file.py tests/unit/test_listen_capture_file.py`
Run: `git commit -m "feat(listen): add capture-file reader (filter/count/first/paginate)"`

---

## Task 4: Expectation evaluation (`agctl/listen/assert_eval.py`)

**Files:**
- Create: `agctl/listen/assert_eval.py`
- Test: `tests/unit/test_listen_assert_eval.py`

**Interfaces:**
- Consumes (Task 2): `agctl.listen.daemon.{read_expectations, capture_path}`; (Task 3): `agctl.listen.capture_file.{build_predicate, count_matching}`; `agctl.commands.kafka_commands` nothing new (pattern fill done here); `agctl.resolution.fill_placeholders`; `agctl.params.parse_params`.
- Produces:
  - `resolve_spec_modes(spec: dict, patterns: dict[str, KafkaPattern]) -> dict` — expand an ExpectationSpec into `{contains, match, path, filled_pattern_match}`: if `spec["modes"]["pattern"]` is set, look it up in `patterns` (missing → `ConfigError`/`TemplateNotFound` style), fill its `match` with `spec["params"]` via `fill_placeholders`; merge with explicit `contains`/`match`/`path`. Returns the resolved mode dict for `build_predicate`.
  - `evaluate_expectations(run_dir: Path, patterns: dict) -> list[ExpectationResult]` — read `asserts.jsonl`; for each spec, resolve modes, build predicate, `count_matching` over `capture_path(run_dir, spec["topic"])`; build an `ExpectationResult` (`passed = matched_count >= expect_count`). On a failed expectation, attach `detail` with `messages_scanned` and a per-mode `root` list (same shape as `kafka assert`: `contains`/`path`→`"message value"`, `match`/`pattern`→`"message envelope"`) so the result is self-debugging.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_listen_assert_eval.py` builds a temp run dir with an `asserts.jsonl` (two specs: one `--contains` on topic A expecting 1, one `--match` on topic B expecting 1) and the two `<topic>.ndjson` files (A has a matching envelope; B has none). `evaluate_expectations` returns two `ExpectationResult`s: A `passed:True, matched_count:1`; B `passed:False, matched_count:0` with a `detail` containing `messages_scanned` and the `root` for the `match` mode. A second test verifies `resolve_spec_modes` fills a named pattern's `{orderId}` placeholder from params and raises on an unknown pattern name.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_listen_assert_eval.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Write minimal implementation**

Implement `resolve_spec_modes` and `evaluate_expectations` per the contracts. No wall-clock deadline anywhere (the scan is bounded by file size only).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_listen_assert_eval.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/listen/assert_eval.py tests/unit/test_listen_assert_eval.py`
Run: `git commit -m "feat(listen): add expectation evaluation over captured files"`

---

## Task 5: Per-topic capture loop (`agctl/listen/capture.py`)

**Files:**
- Create: `agctl/listen/capture.py`
- Test: `tests/unit/test_listen_capture.py`

**Interfaces:**
- Consumes: `agctl.clients.kafka_client.KafkaClient.consume_loop(topic, *, group_id, stop_event, handle, poll_timeout=0.5, max_retries=3, on_assign=None, on_revoke=None)` and `ReactionResult` (`COMMIT`/`RETRY`/`STOP`); `agctl.assertions.jq_bool` (for optional `--capture-match`).
- Produces `class CaptureLoop`:
  - `__init__(self, *, topic: str, client: KafkaClient, group_id: str, capture_path: Path, capture_match: str | None, max_bytes: int, emit_event: Callable[[dict], None], ready_event: threading.Event, stop_event: threading.Event)` — `capture_match` is an optional jq predicate over the message envelope (filter at capture); `max_bytes=0` disables the overflow valve.
  - `run(self) -> None` — call `client.consume_loop(self._topic, group_id=..., stop_event=..., handle=self._handle, on_assign=self._on_assign)`.
  - `_on_assign(self, consumer, partitions)` — for each assigned `TopicPartition` call `consumer.seek(TopicPartition(tp.topic, tp.partition, OFFSET_END))` so the listener starts at the head (no backlog; this is the load-bearing seek-to-latest); then set `ready_event` once (guard so it only fires on the first assignment).
  - `_handle(self, msg, *, attempt, final) -> ReactionResult` — if `capture_match` set and `jq_bool(msg, capture_match)` is False → `COMMIT` (skip). If the capture file's current byte size ≥ `max_bytes` (and `max_bytes > 0`) and not already overflowed → `emit_event({"event":"capture.overflow","topic":..., "bytes":...})` once and return `STOP` (cease capturing this topic). Else append one CapturedEnvelope line (`topic` + `_normalize`-style fields + `captured_at`) to `capture_path` under a per-topic `threading.Lock` and return `COMMIT`.
- Test seam: tests inject a fake `KafkaClient` whose `consume_loop` invokes `on_assign` then calls `handle` with canned normalized dicts and stops (mirrors the existing `consume_loop` fake used in reactor tests).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_listen_capture.py` uses a fake client: `consume_loop(topic, *, group_id, stop_event, handle, on_assign)` records the `on_assign` callback, calls `on_assign(fake_consumer, [tp])` (asserting the callback seeks each partition to end via a mock `consumer.seek` spy), then calls `handle` with three normalized messages (one failing `capture_match`, two passing) and returns. After `CaptureLoop(...).run()`, asserts: the capture file has exactly 2 lines (the matching ones), each a valid CapturedEnvelope with `topic` set; `ready_event.is_set()` is True; `consumer.seek` was called with `OFFSET_END` for the assigned partition. A second test sets `max_bytes` just above one line's size and asserts the second matching message triggers exactly one `capture.overflow` event (via a recording `emit_event`) and the loop stops (handle returned `STOP`).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_listen_capture.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Write minimal implementation**

Implement `CaptureLoop` per the contracts, reusing `KafkaClient.consume_loop`. `_on_assign` imports `OFFSET_END`/`TopicPartition` lazily (`from ..clients.kafka_client import _import_kafka` is internal — instead mirror the lazy pattern: import inside the method). Append via `json.dumps(..., ensure_ascii=False)` + newline under the per-topic lock.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_listen_capture.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/listen/capture.py tests/unit/test_listen_capture.py`
Run: `git commit -m "feat(listen): add per-topic capture loop (seek-to-end, overflow valve)"`

---

## Task 6: ListenEngine lifecycle (`agctl/listen/engine.py`)

**Files:**
- Create: `agctl/listen/engine.py`
- Test: `tests/unit/test_listen_engine.py`

**Interfaces:**
- Consumes (Task 5): `agctl.listen.capture.CaptureLoop`; `threading`, `signal`, `time`, `datetime`; `errors.ConfigError`, `errors.ConnectionFailure`.
- Produces `class ListenEngine` (mirrors `MockEngine`'s lifecycle shape, HTTP-free):
  - `__init__(self, *, topics: list[str], client: KafkaClient, run_id: str, group: str, cluster: str, run_dir: Path, capture_match: str | None, max_bytes: int, duration: float | None, emit_fn: Callable[[dict], None] = _default_emit)` — store config; create `self._stop = threading.Event()`; one `CaptureLoop` per topic, each with its own `ready_event`; single-writer `self._emit_lock`; counters `captured_per_topic: dict[str,int]`, `overflowed_topics: list[str]`, `errors: int`; `self._started = False`.
  - `emit_event(self, line: dict) -> None` — under `self._emit_lock`: add `timestamp` if missing, tally counters (`capture.overflow`→append topic, `kafka.error`→`errors+=1`), write via `emit_fn`. (Single-writer discipline from `MockEngine.emit_event`.)
  - `start(self) -> None` — for each topic build a `CaptureLoop`; start each on its own daemon thread whose target runs `capture_loop.run()` wrapped so any exception emits `{"event":"kafka.error","topic":...,"error":str(exc),"fatal":True}` (mirrors `MockEngine`'s reactor-thread error handling); wait until every topic's `ready_event` is set OR a startup budget (e.g. 30s) elapses (→ raise `ConnectionFailure` "listener did not become ready for topic …"); then set `self._started = True` and emit `started`: `{"event":"started","run_id":...,"topics":[...],"group":...,"cluster":...,"started_at":<ISO-Z>}`.
  - `run(self) -> int` — install `SIGTERM`/`SIGINT` handlers that set `self._stop` (guard for non-main thread); arm a `threading.Timer` if `duration` set; block on `self._stop`; join capture threads (timeout 2s); return `1` if `errors>0` else `0`; restore prior handlers in `finally`.
  - `shutdown(self) -> None` — set stop, cancel duration timer, join threads, emit `summary` only if `self._started`: `{"event":"summary","topics":[{"topic":t,"captured":n,"overflowed": t in overflowed_topics}...],"errors":...,"duration_ms":...}`.
- `_default_emit(line)`: `json.dumps(ensure_ascii=False)` + newline + flush to stdout (copy of `mock.engine._default_emit`).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_listen_engine.py` injects a fake `CaptureLoop` class (via a `capture_loop_factory` seam on the engine, like `new_mock_engine`) whose `run()` sets its `ready_event` immediately and appends one canned line to its capture path, and a recording `emit_fn`. After `engine.start()` then immediate `engine.shutdown()`, assert: exactly one `started` event was emitted (with the right topics/group), followed by one `summary` event whose `topics[].captured` reflects the canned line; `run()` returns 0. A second test: a `CaptureLoop` whose `run()` raises `ConnectionFailure` → `start()` (or `run()`'s thread wrapper) emits a `kafka.error` with `fatal:True`, and `run()` returns 1. A third: if `ready_event` never sets, `start()` raises `ConnectionFailure` within the budget (use a tiny budget via a seam).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_listen_engine.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Write minimal implementation**

Implement `ListenEngine` per the contracts, structurally mirroring `MockEngine` (start→run→shutdown; signal handlers; single-writer emit; started-gates-summary) but with `CaptureLoop` threads instead of HTTP+reactor. Provide a `capture_loop_factory` class attribute defaulting to `CaptureLoop` so tests inject a fake.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_listen_engine.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/listen/engine.py tests/unit/test_listen_engine.py`
Run: `git commit -m "feat(listen): add ListenEngine (lifecycle, threads, NDJSON emit, summary)"`

---

## Task 7: `kafka listen run` foreground command + CLI group registration

**Files:**
- Create: `agctl/commands/kafka_listen_commands.py`
- Modify: `agctl/cli.py` (import `kafka_listen_group`, register under `kafka`)
- Test: `tests/unit/test_kafka_listen_run.py`

**Interfaces:**
- Consumes: `agctl.command.load_config_or_raise`; `agctl.commands.kafka_commands.{resolve_cluster_name, new_kafka_client}`; `agctl.config.models.KafkaPattern`; `agctl.output.emit`; `agctl.listen.engine.ListenEngine`; `agctl.listen.daemon` path helpers.
- Produces:
  - `kafka_listen_group` — `@click.group(name="listen")` with docstring "Kafka long-lived capture listener."
  - `kafka_listen_run` — `@click.command("run")` (NOT `@envelope`; hand-rolled try/except→emit+SystemExit, structurally identical to `mock_run`): flags `--topic` (multiple), `--pattern` (multiple), `--cluster`, `--capture-match`, `--max-bytes-per-topic` (int, default 268435456), `--duration` (float), `--until-stopped` (flag; mutually exclusive with `--duration`), plus the standard `--config/--env-file/--overlay` (read off `ctx.obj`). Behavior: load config; resolve the topic set (each `--pattern` contributes its `KafkaPattern.topic` and, if `--capture-match` unset, its `match` as the capture filter, and its `cluster` as a binding; `--topic` adds bare topics); resolve one cluster via `resolve_cluster_name(cfg.kafka, explicit=cluster, binding_cluster=<first pattern's cluster>)`; build one `KafkaClient` via `new_kafka_client(cluster)`; generate `run_id` (`new_run_id()`); compute `run_dir`; build `ListenEngine`; `engine.start()` (startup errors → structured `kafka.listen.run` envelope with `ok:False` + exit code, before any event line); `code = engine.run()`; `engine.shutdown()` in `finally`; `raise SystemExit(code)`.
  - Resolve-subscriptions helper `resolve_subscriptions(cfg, topics, patterns, capture_match) -> tuple[list[str], str|None, str|None]` returning `(topics, effective_capture_match, binding_cluster)` — pure, unit-tested.
- cli.py change: `from .commands.kafka_listen_commands import kafka_listen_group`; add `kafka_group.add_command(kafka_listen_group)` next to the existing kafka subcommand registrations.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_kafka_listen_run.py` unit-tests `resolve_subscriptions` directly: given a config with pattern `order-created` (`topic: orders.created`, `match: '.value.eventType == "ORDER_CREATED"'`, `cluster: default`) plus a bare `--topic payments.events` and no explicit `--capture-match`, it returns `(["orders.created","payments.events"], '.value.eventType == "ORDER_CREATED"', "default")`; with an explicit `--capture-match`, the pattern's match is NOT used. A second test uses Click `CliRunner` invoking `kafka listen run` with a `new_listen_engine` seam (monkeypatchable factory attribute in `kafka_listen_commands`, like `new_mock_engine`) returning a fake engine whose `start/run/shutdown` emit canned NDJSON, and asserts stdout contains a `started` line and a `summary` line and the process exits 0. A third test asserts a startup `ConnectionFailure` from the fake engine produces a single `{"ok":False,"command":"kafka.listen.run",...}` envelope and exit 2, with no event lines.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_kafka_listen_run.py -v`
Expected: FAIL — module missing / `kafka listen` not registered.

- [ ] **Step 3: Write minimal implementation**

Implement `kafka_listen_commands.py`: the Click group, `resolve_subscriptions`, and `kafka_listen_run` (mirroring `mock_run`'s streaming structure). Add a module-level `new_listen_engine(...)` test seam defaulting to `ListenEngine`. Register the group in `cli.py`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_kafka_listen_run.py -v`
Expected: PASS. Also run `python -m agctl kafka listen --help` (manual smoke) to confirm the subgroup is wired.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/kafka_listen_commands.py agctl/cli.py tests/unit/test_kafka_listen_run.py`
Run: `git commit -m "feat(listen): add kafka listen run (foreground streaming) + CLI group"`

---

## Task 8: `kafka listen start` / `status` / `stop` managed-daemon commands

**Files:**
- Modify: `agctl/commands/kafka_listen_commands.py` (add three commands)
- Test: `tests/unit/test_kafka_listen_daemon_cmds.py`

**Interfaces:**
- Consumes (Task 1): `agctl.daemon.{spawn_daemon, terminate, require_posix_daemon}`; (Task 2): `agctl.listen.daemon.{new_run_id, pidfile_path, run_dir, events_log_path, write_meta, read_meta, list_running_listeners, resolve_listener_target, parse_events_log, RunningListener}`; (Task 7): `resolve_subscriptions(cfg, topics: list[str], patterns: list[str], capture_match: str | None) -> tuple[list[str], str | None, str | None]` (returns topics, effective capture match, binding cluster); `agctl.command.envelope, load_config_or_raise`; `agctl.commands.kafka_commands.resolve_cluster_name`; `errors.{ConfigError, ConnectionFailure, AssertionFailure}`; `shutil` (cleanup).
- Produces three `@envelope`-wrapped `_core` functions + Click commands (`kafka.listen.start` / `.status` / `.stop`):
  - `_kafka_listen_start_core(config_path, topics, patterns, cluster, capture_match, max_bytes, state_dir, overlay_paths, env_file) -> dict`: `require_posix_daemon()`; load config; resolve subscriptions + cluster (reuse Task 7 helper, but for `start` the daemon re-resolves from argv — `start` only needs cluster/topics to write the pidfile/meta and to build the spawn argv); generate `run_id`; compute paths; already-running pre-check (read pidfile, `is_alive` → `ConfigError` "listener already running; run 'agctl kafka listen stop' first"); `write_meta`; build the daemon argv (`[kafka listen run --topic/--pattern ... --cluster ... --capture-match ... --max-bytes ...]`, forwarding `--config/--env-file/--overlay` as absolute paths, exactly as `_mock_start_core` does); `spawn_daemon(argv, events_log_path)`; write pidfile; readiness-poll `events_log_path` via `parse_events_log` until `started` (budget 30s → `ConfigError`) or `startup_error` (→ terminate + remove pidfile + `ConfigError`); return `{pid, run_id, state_dir, topics, group, cluster, started_at}`.
  - `_kafka_listen_status_core(run_id, pid, state_dir) -> dict`: `require_posix_daemon()`; `resolve_listener_target`; not-running → `{running:False}`; else parse events.log for `summary_so_far`-style counts + `overflow_topics`; compute `uptime_ms` from `started_at`; return `{running:True, pid, run_id, uptime_ms, topics:[{topic, captured, bytes, overflowed}]}` (captured/bytes by counting lines/size of each `<topic>.ndjson`).
  - `_kafka_listen_stop_core(run_id, pid, all_, timeout, state_dir) -> dict`: `require_posix_daemon()`; resolve targets; not-running → `{stopped:False}` (or `{stopped:[]}` for `--all`); for each target `terminate(pid, timeout)`, `parse_events_log` for summary/errors, `shutil.rmtree(run_dir)`, remove pidfile; build verdict `{stopped:True, pid, signal, summary, cleaned:True, failures:[...]}`; raise `AssertionFailure` (exit 1) if any fatal event present (`kafka.error`, or `capture.overflow` on a topic that has an attached expectation in `asserts.jsonl`).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_kafka_listen_daemon_cmds.py` monkeypatches `kafka_listen_commands.spawn_daemon` to return `os.getpid()` (or a fake pid) AND pre-write a canned `events.log` containing a `started` line, then asserts `_kafka_listen_start_core` writes the pidfile + meta and returns a dict with `run_id`/`topics`/`started_at`. A `status` test: given a written pidfile + run dir with two `<topic>.ndjson` files (N and M lines), returns `running:True` with per-topic `captured==N`/`M`. A `stop` test: with a canned `events.log` containing a `kafka.error` fatal event, `_kafka_listen_stop_core` raises `AssertionFailure` and the run dir is removed (cleanup). A `stop` clean test: no fatal events → returns `{stopped:True, cleaned:True}`, run dir gone. A multiple-running test: `start`/`status`/`stop` with no selector and two pidfiles → `ConfigError`.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_kafka_listen_daemon_cmds.py -v`
Expected: FAIL — commands not defined.

- [ ] **Step 3: Write minimal implementation**

Add the three Click commands + `_core` functions, register them on `kafka_listen_group`. Follow `_mock_start_core`/`_mock_stop_core`/`_mock_status_core` structure closely (readiness poll, terminate+cleanup, selector resolution). Use `@envelope("kafka.listen.start")` etc.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_kafka_listen_daemon_cmds.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/kafka_listen_commands.py tests/unit/test_kafka_listen_daemon_cmds.py`
Run: `git commit -m "feat(listen): add kafka listen start/status/stop (managed daemon)"`

---

## Task 9: `kafka listen assert` / `results` / `messages` commands

**Files:**
- Modify: `agctl/commands/kafka_listen_commands.py` (add three commands)
- Test: `tests/unit/test_kafka_listen_assert_msgs.py`

**Interfaces:**
- Consumes (Task 2): `agctl.listen.daemon.{resolve_listener_target, append_expectation, read_expectations, capture_path, new_run_id (for id defaults)}`; (Task 3): `agctl.listen.capture_file.{build_predicate, read_messages}`; (Task 4): `agctl.listen.assert_eval.{evaluate_expectations}`; `agctl.command.envelope, load_config_or_raise`; `agctl.params.parse_params`; `errors.{ConfigError, AssertionFailure, TemplateNotFound}`.
- Produces three `@envelope`-wrapped commands (`kafka.listen.assert` / `.results` / `.messages`):
  - `assert` flags: `--topic` (required), `--contains`, `--match`, `--pattern`, `--path`, `--param` (multiple), `--expect-count` (int, default 1), `--id` (optional; default `f"exp-{n}"` from current asserts count), `--run-id`/`--pid`. `_core` resolves the listener's run_dir via `resolve_listener_target(state_dir, run_id=, pid=, all_=False)` (→ `ConfigError` if not running), validates that ≥1 mode is supplied (else `ConfigError`, mirroring `kafka assert`'s "zero modes" rule), builds an ExpectationSpec (modes raw, params parsed), `append_expectation(run_dir, spec)`, returns `{attached:True, id, topic, modes:[...], expect_count}`.
  - `results` flags: `--run-id`/`--pid`. `_core` resolves run_dir; loads config (for `kafka.patterns`); `evaluate_expectations(run_dir, cfg.kafka.patterns)`; if no expectations attached → `ConfigError` "no expectations attached; run 'kafka listen assert' first". Build `{evaluated, passed, failed, results:[ExpectationResult]}`; if `failed>0` raise `AssertionFailure("kafka listen: {failed}/{evaluated} expectation(s) failed", {"results": results})` (the per-result `detail` flows out via `error.detail`).
  - `messages` flags: `--topic` (required), `--match`, `--param` (multiple), `--limit` (int, default 50), `--run-id`/`--pid`. `_core` resolves run_dir; if `--match`, fill placeholders + `build_predicate({match: ...})` else predicate None; `read_messages(capture_path(run_dir, topic), predicate=predicate, limit=limit)`; return `{topic, matched, truncated, messages}`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_kafka_listen_assert_msgs.py` sets up a temp state dir with one running-listener pidfile (live pid) + run dir containing `orders.created.ndjson` (3 envelopes, 2 matching `--match '.value.eventType=="ORDER_CREATED"'`). `assert` then `results`: after attaching one expectation (expect-count 1) and calling `results`, the envelope result has `passed:True, failed:0`; with expect-count 3 it raises `AssertionFailure` with `error.detail.results[0].matched_count==2`. `messages --topic orders.created --match '...' --limit 1` returns `{matched:2, truncated:True, messages:[<one>]}`. An `assert` with zero modes → `ConfigError`. `results` with no asserts.jsonl → `ConfigError`.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_kafka_listen_assert_msgs.py -v`
Expected: FAIL — commands not defined.

- [ ] **Step 3: Write minimal implementation**

Add the three Click commands + `_core` functions and register on `kafka_listen_group`. `results` reuses `evaluate_expectations` and raises `AssertionFailure` on any failure so the self-debugging detail reaches `error.detail`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_kafka_listen_assert_msgs.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/kafka_listen_commands.py tests/unit/test_kafka_listen_assert_msgs.py`
Run: `git commit -m "feat(listen): add kafka listen assert/results/messages"`

---

## Task 10: Full test suite, help text, and docs sync

**Files:**
- Modify (docs, via the `docs-watcher` subagent per `CLAUDE.md`): `docs/ARCHITECTURE.md`, `docs/DESIGN.md`, `skills/` consumer reference
- Test: `tests/integration/test_kafka_listen_commands.py`

**Interfaces:** None new — this task verifies the whole and syncs docs.

- [ ] **Step 1: Write the integration test**

`tests/integration/test_kafka_listen_commands.py` (self-skipping without a live broker, mirroring `tests/integration/test_mock_commands.py` / the kafka fixtures in `tests/integration/conftest.py`): under `AGCTL_TEST_LIVE=1` with the Kafka+KRaft testcontainer — produce a prior message, `kafka listen start --topic T`, produce a new matching message, `kafka listen assert --topic T --match '...' --expect-count 1`, `kafka listen results` (expect pass), `kafka listen messages --topic T`, `kafka listen stop` (clean). A second case asserts seek-to-latest: a message produced BEFORE `start` is NOT captured (no prior-run match), confirming the head positioning. A third case asserts retention-immunity is structurally present (the assert reads the capture file, not the live topic) by detaching/verifying the file survives independent of a fresh windowed read.

- [ ] **Step 2: Run unit suite**

Run: `pytest tests/unit -v`
Expected: PASS — all listen unit tests + no regressions in mock/kafka/other suites.

- [ ] **Step 3: Run integration suite (if a broker is available)**

Run: `AGCTL_TEST_LIVE=1 pytest tests/integration/test_kafka_listen_commands.py -v`
Expected: PASS (or SKIP if no Docker/broker — acceptable; the self-skip must never FAIL).

- [ ] **Step 4: Sync docs**

Invoke the `docs-watcher` subagent to update (per the spec's §14): `docs/ARCHITECTURE.md` (§3 module map add `commands/kafka_listen_commands.py`, `listen/`, `agctl/daemon.py`; §6 sixth streaming exception `kafka listen run` + the managed-daemon start/stop/status; §8 capture reuses `consume_loop`; §9 assert_eval reuses predicates; §15 second statelessness carve-out alongside mock); `docs/DESIGN.md` (§3.2 add the `kafka listen` command group and the listener/offset model; §1 note the listener as the long-saga verification primitive; §10 roadmap); `skills/` consumer reference (add `kafka listen` + the runbook protocol "start before trigger → attach → results → stop" and the "stop does not auto-assert" caveat). Preserve each doc's altitude (DESIGN=WHAT/WHY, ARCHITECTURE=HOW).

- [ ] **Step 5: Commit**

Run: `git add docs/ARCHITECTURE.md docs/DESIGN.md skills/ tests/integration/test_kafka_listen_commands.py`
Run: `git commit -m "feat(listen): integration tests + docs sync for kafka listen"`

---

## Notes for the implementer

- **Seek-to-latest is the core invariant.** `CaptureLoop._on_assign` MUST seek every assigned partition to `OFFSET_END` before any poll yields data. `_build_consumer` hardcodes `auto.offset.reset=earliest`, so the seek is mandatory (do not rely on the reset policy). The readiness `started` event fires only after assignment + seek.
- **No deadline on assert scans.** `capture_file.count_matching` / `first_matching` read the whole file (bounded by runbook duration, not a timeout). Never introduce a wall-clock cutoff there — that would reintroduce the false negative this feature exists to kill.
- **`stop` cleans the run dir** (`shutil.rmtree`). `results`/`messages`/`assert` never delete anything.
- **`@envelope` tag names**: `kafka.listen.start`, `kafka.listen.status`, `kafka.listen.stop`, `kafka.listen.assert`, `kafka.listen.results`, `kafka.listen.messages`. `kafka.listen.run` is unwrapped (streaming).
- **Reuse, don't duplicate**: `_build_assert_predicate`, `resolve_cluster_name`, `new_kafka_client`, `consume_loop`, `KafkaPattern`, and the Task-1 daemon primitives. The listen subpackage should not re-implement these.

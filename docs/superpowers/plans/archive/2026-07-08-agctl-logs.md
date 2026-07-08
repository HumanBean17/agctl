# `agctl logs` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `agctl logs` command group (`query` / `assert` / `tail`) backed by a pluggable `LogBackend` Protocol + `agctl.logs_backends` entry-point, shipping one NDJSON `file` backend (logstash) in v1 — mirroring the existing `db_drivers` seam so Victoria is a future external package.

**Architecture:** A `LogClient` selects a `LogBackend` by `source.type` (built-in `file` in `BUILTIN_BACKENDS`, third-party via `agctl.logs_backends`), exactly as `DbClient` selects a `DBDriver`. Every backend emits one **canonical entry** (`{timestamp, level, logger, message, thread, service, stack_trace, tags, fields}`) so `--level`/`--logger`/`--message`/`--match`/`discover` are transport-agnostic. `query`/`assert` use the standard `@envelope` core split; `tail` is the streaming exception (mirrors `http ping`: NDJSON lines, signal-driven stop, summary line). `assert` is one-shot (`--timeout` omitted/`0`) or poll (`--timeout N>0`); `--not` inverts. jq is reused (`jq_bool`/`compile_jq` from `agctl/assertions.py`); `--match` is `{placeholder}`-filled then compile-checked up front.

**Tech Stack:** Python ≥3.11, Click, Pydantic v2, PyYAML, `jq` (optional extra, lazy-imported), pytest + `click.testing.CliRunner`.

## Global Constraints

These apply to every task; each task's requirements implicitly include them. They are the spec's project-wide rules (D1–D12), copied verbatim where exact.

- **Additive only.** No existing flag, field, exit code, output shape, command, or entry-point changes. New `logs` group, new `logs:` config section, new discover category, new `logs` extra, new `agctl.logs_backends` entry-point group.
- **Canonical entry shape (D2/D3).** Every backend emits, and every flag/`--match`/output operates on, this dict: `{timestamp: str(ISO-8601), level: str(UPPER), logger: str, message: str, thread: str|None, service: str|None, stack_trace: str|None, tags: list[str]|None, fields: dict[str,Any]}`. `fields` holds **every original top-level key not mapped to a canonical slot** (MDC + `StructuredArguments` + `@version`/`level_value`/etc.). No `.raw` blob.
- **`source.type` selects the backend (D4); default `"file"`. `format` is a file-backend parser hint; default `"logstash"` (the only v1 parser).**
- **`assert` is one-shot + poll (D5).** `--timeout` is optional: omitted or `0` → one-shot scan of the `--since` window, exit immediately; `N > 0` → poll until first match or `N` elapses. `--since` is required for `assert` (bounds the window in both modes); optional for `query`.
- **`--not` inverts `assert` (D6):** no match in window → exit 0; a match found → `AssertionFailure` (exit 1) with the offending entry.
- **`--match` is `{placeholder}`-filled via `--param`, then `compile_jq`-validated up front (D7).** A malformed expression or missing jq is a `ConfigError` (exit 2), not a silent no-match.
- **Structural flags are push-down hints; `--match` is always client-side on the canonical entry (D8).** The file backend applies all filters client-side.
- **`tail` streams NDJSON without an envelope wrapper (D9)** — mirrors `http ping`.
- **Missing log file = empty result, not an error (D10).**
- **New `logs` extra bundles `jq>=1.6`; jq is lazy-imported only when `--match` is used (D11).** Structural-flag-only usage needs no jq. Missing-library `ConfigError` hint: `pip install 'agctl[logs]'`.
- **Built-in `file` in `BUILTIN_BACKENDS`; third-party via `agctl.logs_backends` (D12)** — symmetric with `postgresql` in `BUILTIN_DRIVERS`.
- **Plan-level refinement (note):** `invert` is a **command-layer** concern, not a backend one. Both `--not` and positive `assert` early-stop on the first match, so `await_one` returns first-match-or-`None` regardless of intent; the command interprets. (The spec's §8.1 listed an `invert` parameter on `await_one`; this plan drops it as redundant. Flagged for review.)
- **Exit codes unchanged:** `0` ok / `1` assertion / `2` config-or-env.
- **Reuse, do not re-implement:** `jq_bool`/`compile_jq`/`_parse_iso_datetime`/`_to_utc` from `agctl/assertions.py`; `fill_placeholders` from `agctl/resolution.py`; `parse_params` from `agctl/params.py`; `envelope`/`load_config_or_raise` from `agctl/command.py`; the `ping_loop`/`_emit_stdout_line`/signal-handler pattern from `agctl/commands/http_commands.py`.
- **Branch:** `feat/add-service-logging-discovery` (worktree at `.worktrees/feat-add-service-logging-discovery`). Commit after every task. End commit messages with `Co-Authored-By: Claude <noreply@anthropic.com>`.
- **Run tests with the shared repo venv** (`pytest` is not on PATH): `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`. No broker/DB/HTTP services needed for logs (file-based). Importable `agctl` resolves to the worktree's code when pytest runs from the worktree root.

---

## File Structure

**Created:**
- `agctl/clients/log_backend_protocol.py` — `LogBackend` Protocol + DTOs (`CanonicalEntry`, `LogFilter`, `ScanResult`, `AwaitResult`, `SchemaDescriptor`).
- `agctl/clients/log_client.py` — `LogClient` (selects backend by `source.type`) + `BUILTIN_BACKENDS` + `load_backends()` + `LOG_BACKEND_ENTRY_POINT_GROUP`.
- `agctl/clients/log_backends/__init__.py` — package marker.
- `agctl/clients/log_backends/ndjson_file.py` — `NdjsonFileBackend` (logstash normalizer, tail read, `scan`/`await_one`/`follow`/`sample_schema`/`validate_config`).
- `agctl/commands/logs_commands.py` — `logs_query` / `logs_assert` / `logs_tail` Click commands + cores/envelopes + `new_logs_client` factory + `_parse_since_until` + `_build_log_filter`.
- `tests/unit/test_logs_client.py` — protocol/DTO, backend, and `LogClient` tests.
- `tests/unit/test_logs_commands.py` — `query`/`assert`/`tail` command tests.
- `tests/integration/test_logs_commands.py` — real-temp-file E2E (always-on).

**Modified:**
- `agctl/config/models.py` — add `LogSource`, `LogsDefaults`, `LogsConfig`; add `logs` field to `Config`.
- `agctl/config/validator.py` — validate each `logs.sources` entry via its backend's `validate_config`.
- `agctl/commands/discover_commands.py` — add `log-sources` category (summary/category/item/search).
- `agctl/cli.py` — register the `logs` group + subcommands.
- `agctl/data/sample-config.yaml` + `tests/fixtures/agctl.yaml` — add a `logs:` section.
- `pyproject.toml` — add `logs` extra + `agctl.logs_backends` entry-point group.
- `tests/unit/test_discover_command.py` — add `log-sources` tests.
- `tests/unit/test_models.py` — add logs-model tests.
- `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, `skills/agctl-config/`, `README.md` — docs sync (final task, via `docs-watcher` + targeted edits).

---

### Task 1: Config models — `LogSource` / `LogsDefaults` / `LogsConfig`

Foundation: the pydantic models every later task reads. `${ENV}` interpolation and `AGCTL_LOGS__*` env overrides apply automatically via the existing loader; no loader change here.

**Files:**
- Modify: `agctl/config/models.py` (add three models after `MocksConfig`, before `Config`; add one field to `Config`)
- Test: `tests/unit/test_models.py`

**Interfaces:**
- Produces (exact field names + types + defaults):
  - `LogSource(BaseModel)`: `type: str = "file"`; `path: str | None = None`; `format: str = "logstash"`; `service: str | None = None`.
  - `LogsDefaults(BaseModel)`: `tail_lines: int = 200`; `limit: int = 50`; `timeout_seconds: int = 10`; `poll_interval_ms: int = 100`.
  - `LogsConfig(BaseModel)`: `sources: dict[str, LogSource] = Field(default_factory=dict)`; `defaults: LogsDefaults = Field(default_factory=LogsDefaults)`.
  - `Config` gains: `logs: LogsConfig = Field(default_factory=LogsConfig)` (placed after `mocks`).
- Consumes: `BaseModel`, `Field` from pydantic (already imported in `models.py`).

- [ ] **Step 1: Write the failing model tests**

Add to `tests/unit/test_models.py`:
1. `test_log_source_defaults`: `LogSource()` has `type=="file"`, `path is None`, `format=="logstash"`, `service is None`.
2. `test_logs_defaults_defaults`: `LogsDefaults()` has `tail_lines==200`, `limit==50`, `timeout_seconds==10`, `poll_interval_ms==100`.
3. `test_logs_config_empty_default`: `LogsConfig()` has `sources=={}` and a `LogsDefaults` instance.
4. `test_config_has_logs_field`: `Config(version="2")` has `.logs` being a `LogsConfig` (empty sources). Also: constructing `Config(version="2", logs={"sources": {"svc": {"path": "/tmp/x.log"}}})` (via `model_validate` with a dict) yields `cfg.logs.sources["svc"].path == "/tmp/x.log"` and `cfg.logs.sources["svc"].type == "file"` (default applied).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_models.py -q`
Expected: FAIL — `LogSource`/`LogsConfig` are not defined (ImportError or AttributeError).

- [ ] **Step 3: Add the three models + the Config field**

In `agctl/config/models.py`: define `LogSource`, `LogsDefaults`, `LogsConfig` (after `MocksConfig`, before `Config`), and add `logs: LogsConfig = Field(default_factory=LogsConfig)` to `Config`. No validators needed in v1 (structural source validation is the backend's job, Task 6).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_models.py -q`
Expected: PASS.

- [ ] **Step 5: Run the full unit suite (regression)**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS — adding an optional config field with a default is backward-compatible.

- [ ] **Step 6: Commit**

Run: `git add -A && git commit -m "feat(config): add logs.sources/logs.defaults models"`

---

### Task 2: Backend contract — Protocol + DTOs + packaging

The cross-backend contract (D2/D3) and the packaging seam (D11/D12). Pure types + pyproject; no behavior yet. Tests construct DTOs and assert the Protocol is structural.

**Files:**
- Create: `agctl/clients/log_backend_protocol.py`
- Modify: `pyproject.toml` (add `logs` extra under `[project.optional-dependencies]`; add `[project.entry-points."agctl.logs_backends"]` section, empty/commented like `agctl.plugins`)
- Test: `tests/unit/test_logs_client.py` (new file)

**Interfaces:**
- Produces (exact names; `dataclasses.dataclass` with the listed fields):
  - `CanonicalEntry`: `timestamp: str`, `level: str`, `logger: str`, `message: str`, `thread: str | None = None`, `service: str | None = None`, `stack_trace: str | None = None`, `tags: list[str] | None = None`, `fields: dict = field(default_factory=dict)`.
  - `LogFilter`: `level: str | None = None`, `logger_glob: str | None = None`, `message_substring: str | None = None`, `match_jq: str | None = None`, `params: dict = field(default_factory=dict)`.
  - `ScanResult`: `entries: list` (list[CanonicalEntry]), `matched: int`, `scanned: int`, `truncated: bool`.
  - `AwaitResult`: `entry` (CanonicalEntry | None), `scanned: int`, `elapsed_ms: int`.
  - `SchemaDescriptor`: `standard: list[str]`, `conditional: list[str]`, `observed: list[str]`.
  - `LogBackend(Protocol)` (`@runtime_checkable`, `from typing import Protocol, runtime_checkable`, `from __future__ import annotations`) with methods (signatures only — bodies are `...`):
    - `def validate_config(self) -> None: ...`
    - `def scan(self, filt: LogFilter, *, since, until, limit: int, tail_lines: int) -> ScanResult: ...` (`since`/`until` are `datetime|None`)
    - `def await_one(self, filt: LogFilter, *, since, timeout_s: float, poll_interval_ms: int) -> AwaitResult: ...`
    - `def follow(self, filt: LogFilter, *, stop_event) -> "Iterator[CanonicalEntry]": ...` (`stop_event: threading.Event`)
    - `def sample_schema(self, *, sample_lines: int = 100) -> SchemaDescriptor: ...`
  - pyproject `[project.optional-dependencies]` gains `logs = ["jq>=1.6"]`.
  - pyproject gains `[project.entry-points."agctl.logs_backends"]` with a comment (no entries; the built-in `file` backend is registered in code, not via entry-point — symmetric with `postgresql`).
- Consumes: nothing (pure types).

- [ ] **Step 1: Write the failing DTO/Protocol tests**

Create `tests/unit/test_logs_client.py`:
1. `test_canonical_entry_required_fields`: `CanonicalEntry(timestamp="t", level="ERROR", logger="l", message="m")` constructs; default `thread/service/stack_trace/tags is None` and `fields == {}`.
2. `test_log_filter_defaults_all_none`: `LogFilter()` has all filter fields `None` and `params == {}`.
3. `test_scanresult_and_awaitresult_hold_fields`: construct a `ScanResult(entries=[], matched=0, scanned=5, truncated=False)` and an `AwaitResult(entry=None, scanned=5, elapsed_ms=12)`; assert field values.
4. `test_log_backend_is_protocol`: `isinstance` check that a class implementing all five methods satisfies `isinstance(obj, LogBackend)` (build a minimal `class _Fake` with the five methods returning dummy values; assert `isinstance(_Fake(), LogBackend) is True` and `isinstance(object(), LogBackend) is False`).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -q`
Expected: FAIL — module/symbols not defined (ImportError).

- [ ] **Step 3: Implement the Protocol + DTOs**

Create `agctl/clients/log_backend_protocol.py` with the five dataclasses and the `LogBackend` Protocol per the Produces contract. Use `from __future__ import annotations`, `dataclasses.dataclass`/`field`, `typing.Protocol`/`runtime_checkable`, and `collections.abc.Iterator` (or `typing.Iterator`) for `follow`'s return.

- [ ] **Step 4: Add packaging (logs extra + entry-point group)**

In `pyproject.toml`: under `[project.optional-dependencies]` add `logs = ["jq>=1.6"]`. Add a new `[project.entry-points."agctl.logs_backends"]` section with only a comment (mirror the existing `[project.entry-points."agctl.plugins"]` form). Do NOT register the built-in `file` backend here.

- [ ] **Step 5: Run the tests + an install-resolution check**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -q`
Expected: PASS.
Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 6: Commit**

Run: `git add -A && git commit -m "feat(logs): add LogBackend protocol, canonical DTOs, logs extra + entry-point group"`

---

### Task 3: `NdjsonFileBackend` — logstash normalizer + `scan`

The core read path: tail-read a local NDJSON file, normalize each logstash JSON line to a `CanonicalEntry` (D3), and apply filters client-side (D8). This is the biggest task; the normalizer is the part most likely to be wrong, so it gets the most tests.

**Files:**
- Create: `agctl/clients/log_backends/__init__.py` (empty package marker)
- Create: `agctl/clients/log_backends/ndjson_file.py`
- Test: `tests/unit/test_logs_client.py`

**Interfaces:**
- Consumes: `CanonicalEntry`, `LogFilter`, `ScanResult` from `agctl/clients/log_backend_protocol.py`; `LogSource` from `agctl/config/models.py`; `jq_bool` and `_parse_iso_datetime`/`_to_utc` from `agctl/assertions.py`; `fnmatch.fnmatch` (stdlib) for the logger glob.
- Produces:
  - `class NdjsonFileBackend` with:
    - `__init__(self, source)`: store `self._path = source.path`, `self._format = source.format`. (No file I/O at construction.)
    - `validate_config(self) -> None`: raise `ConfigError(f"logs source of type 'file' requires 'path'", {"type": "file"})` if `self._path is None`.
    - `scan(self, filt, *, since, until, limit, tail_lines) -> ScanResult`: read the last `tail_lines` newline-delimited lines from `self._path` (backward, without loading the whole file); for each line, `json.loads` → normalize → apply window + filters; return matches (capped at `limit`) plus counts. Details below.
    - `_normalize(self, raw: dict) -> CanonicalEntry`: map a parsed logstash JSON object to canonical (contract below).
    - (Other methods `await_one`/`follow`/`sample_schema` raise `NotImplementedError` for now — added in Tasks 4/5.)
  - **`_normalize` contract (logstash → canonical):**
    - `timestamp = raw.get("@timestamp")` (str; may be `None`).
    - `level = str(raw.get("level", "")).upper()` (UPPER-normalized).
    - `logger = raw.get("logger_name")` (str|None).
    - `message = raw.get("message")` (str|None).
    - `thread = raw.get("thread_name")`.
    - `service = raw.get("service")`.
    - `stack_trace = raw.get("stack_trace")`.
    - `tags = raw.get("tags")`.
    - `fields = {k: v for k, v in raw.items() if k not in the slot-source set}` where the slot-source set is `{"@timestamp","level","logger_name","thread_name","message","service","stack_trace","tags"}`. (Every other top-level key — MDC, `StructuredArguments`, `@version`, `level_value` — lands in `fields`.)
  - **`scan` semantics:**
    - If the file does not exist → return `ScanResult(entries=[], matched=0, scanned=0, truncated=False)` (D10), no error.
    - Read the last `tail_lines` lines (a line = bytes ending in `\n`; the final line may be partial — include it if non-empty).
    - For each line: attempt `json.loads`; on `JSONDecodeError`, write one line to stderr (`agctl: skipping non-JSON log line`) and continue (never raise). On success, `_normalize`.
    - Window: parse the entry's `timestamp` via `_parse_iso_datetime` then `_to_utc`; skip entries outside `[since, until]` when those bounds are non-`None`. Entries with an unparseable/`None` timestamp are kept only if both bounds are `None`; otherwise skipped.
    - `scanned` = number of entries that passed the window check (the candidates evaluated against filters).
    - Apply, in order, each active filter (AND): `filt.level` → `entry.level == filt.level.upper()`; `filt.logger_glob` → `fnmatch(entry.logger or "", filt.logger_glob)`; `filt.message_substring` → `filt.message_substring in (entry.message or "")`; `filt.match_jq` → `jq_bool(dataclasses.asdict(entry), filt.match_jq)`.
    - `matched` = total count passing all filters. `entries` = first `limit` matches (canonical entries). `truncated = matched > limit`.

- [ ] **Step 1: Write the failing normalizer tests**

Add to `tests/unit/test_logs_client.py`:
1. `test_normalize_maps_logstash_slots`: `_normalize` of `{"@timestamp":"2026-07-08T10:00:00Z","level":"info","logger_name":"c.Foo","thread_name":"t1","message":"hi","service":"svc","@version":"1","level_value":20000,"orderId":"ord-1"}` yields `level=="INFO"`, `logger=="c.Foo"`, `message=="hi"`, `thread=="t1"`, `service=="svc"`, `timestamp=="2026-07-08T10:00:00Z"`, `stack_trace is None`, `tags is None`, and `fields == {"@version":"1","level_value":20000,"orderId":"ord-1"}` (non-slot keys only).
2. `test_normalize_stack_trace_and_tags`: input with `"stack_trace":"java.lang.RuntimeException: ..."` and `"tags":["hot"]` populates those slots and keeps them OUT of `fields`.
3. `test_normalize_missing_fields_are_none`: input `{}` yields `level==""`, all slot strings `None`/empty per defaults, `fields == {}`.

- [ ] **Step 2: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -k normalize -q`
Expected: FAIL — `NdjsonFileBackend`/`_normalize` undefined.

- [ ] **Step 3: Implement `NdjsonFileBackend.__init__`, `validate_config`, `_normalize`**

Create `agctl/clients/log_backends/__init__.py` (empty) and `agctl/clients/log_backends/ndjson_file.py` per the Produces contract. Implement `_normalize` exactly as specified.

- [ ] **Step 4: Run the normalizer tests to verify they pass**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -k normalize -q`
Expected: PASS.

- [ ] **Step 5: Write the failing `scan` tests**

Add to `tests/unit/test_logs_client.py` (each writes a real temp file via `tmp_path`):
1. `test_scan_missing_file_is_empty`: a `LogSource(path=str(tmp_path/"nope.log"))` backend `.scan(LogFilter(), since=None, until=None, limit=50, tail_lines=200)` returns `ScanResult` with `entries==[]`, `matched==0`, `scanned==0`, `truncated==False` — no exception.
2. `test_scan_reads_tail_and_filters_level`: write 5 NDJSON lines (2 `level:"ERROR"`, 3 `level:"INFO"`); `scan(LogFilter(level="ERROR"), since=None, until=None, limit=50, tail_lines=200)` returns `matched==2`, `scanned==5`, `truncated==False`, and both entries have `level=="ERROR"`.
3. `test_scan_skips_non_json_lines`: file with one valid JSON line, one garbage line, one valid line; `scan(LogFilter(), …)` yields `scanned==2` (garbage skipped, no exception).
4. `test_scan_logger_glob_and_message`: a line with `logger_name:"com.myco.order.Svc"` and `message:"Order persisted"`; `LogFilter(logger_glob="com.myco.order.*", message_substring="persisted")` matches it; `logger_glob="com.other.*"` does not.
5. `test_scan_match_jq_on_fields`: a line whose `fields` includes `orderId:"ord-9"`; `LogFilter(match_jq='.fields.orderId == "ord-9"')` matches; `.fields.orderId == "other"` does not.
6. `test_scan_window_since_until`: two lines with timestamps 10 minutes apart; `since`/`until` bounding the recent one includes only it (use `_parse_iso_datetime`/`_to_utc` to build bounds, or pass ISO strings via the command helper — here pass aware `datetime` bounds directly).
7. `test_scan_limit_truncates`: 5 matching lines, `limit=2` → `entries` length 2, `matched==5`, `truncated==True`.
8. `test_scan_tail_lines_bounds_read`: file with 10 lines; `tail_lines=3` → only the last 3 are considered (`scanned <= 3`).

- [ ] **Step 6: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -k scan -q`
Expected: FAIL — `scan` raises `NotImplementedError` (or is absent).

- [ ] **Step 7: Implement `scan`**

Implement `scan` per the Produces semantics. Use a backward read strategy that seeks near the end and collects up to `tail_lines` newline-terminated fragments; be robust to a file smaller than the estimate and to no-trailing-newline. Reuse `_parse_iso_datetime`/`_to_utc` for window comparison and `jq_bool` for `match_jq`.

- [ ] **Step 8: Run the scan tests + full suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -q`
Expected: PASS.
Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 9: Commit**

Run: `git add -A && git commit -m "feat(logs): NdjsonFileBackend logstash normalizer + scan with filters"`

---

### Task 4: `NdjsonFileBackend` — `await_one` (one-shot + poll) + `sample_schema`

The assert engine (D5) and the discover field enumerator. `await_one` returns first-match-or-`None`; the command layer interprets `--not` (see Global Constraints refinement).

**Files:**
- Modify: `agctl/clients/log_backends/ndjson_file.py` (replace the `NotImplementedError` stubs for `await_one` and `sample_schema`)
- Test: `tests/unit/test_logs_client.py`

**Interfaces:**
- Consumes: Task 3 (`scan`, `_normalize`); `time.monotonic` (stdlib); `CanonicalEntry`, `AwaitResult`, `SchemaDescriptor`, `LogFilter`.
- Produces:
  - `await_one(self, filt, *, since, timeout_s, poll_interval_ms) -> AwaitResult`:
    - `deadline = monotonic() + timeout_s` when `timeout_s > 0`; one-shot when `timeout_s <= 0`.
    - Loop: call the same window+filter read used by `scan` with `until = datetime.now(UTC)` and `limit = 1` (need only the first match); if any match → return `AwaitResult(entry=<first>, scanned=<cumulative>, elapsed_ms=<elapsed>)`. If `timeout_s <= 0` (one-shot) and no match → return `AwaitResult(entry=None, scanned, elapsed_ms)` immediately. If `timeout_s > 0` and no match and `monotonic() < deadline` → sleep `poll_interval_ms/1000` and retry; on deadline → return `AwaitResult(entry=None, scanned, elapsed_ms≈timeout*1000)`.
    - `scanned` is cumulative across poll iterations.
  - `sample_schema(self, *, sample_lines=100) -> SchemaDescriptor`:
    - If file missing → `SchemaDescriptor(standard=[], conditional=[], observed=[])` (D10).
    - Read last `sample_lines` lines, parse + normalize (skip non-JSON).
    - `standard` = sorted names from `["timestamp","level","logger","message","thread","service"]` that were non-`None`/non-empty on at least one entry (always include `timestamp`,`level`,`logger`,`message` if any entry parsed).
    - `conditional` = sorted subset of `["stack_trace","tags"]` seen non-`None`.
    - `observed` = sorted union of every key in every entry's `fields`, EXCLUDING the well-known logstash noise set `{"@version","level_value"}`.
- Consumes (test seam): the loop must use an injectable `monotonic`/`sleep` so tests can drive polling deterministically — expose them as `__init__` kwargs `monotonic=time.monotonic` and `sleep=time.sleep` stored on `self`, OR accept them as `await_one` kwargs `monotonic=None, sleep=None` defaulting to the stdlib functions. Tests pass fakes.

- [ ] **Step 1: Write the failing `await_one` tests**

Add to `tests/unit/test_logs_client.py` (each uses a temp file; inject fakes for monotonic/sleep where polling is exercised):
1. `test_await_one_shot_match`: file with an `ERROR` line; `await_one(LogFilter(level="ERROR"), since=None, timeout_s=0, poll_interval_ms=100)` returns `entry` non-`None`, `entry.level=="ERROR"`.
2. `test_await_one_shot_no_match`: file with only `INFO`; `timeout_s=0` → `entry is None`.
3. `test_await_one_poll_finds_after_delay`: file initially empty; using a fake `sleep` + a fake `monotonic` that advances, and a side effect that appends a matching line after the first poll, `await_one(..., timeout_s=2, poll_interval_ms=100)` returns the matching `entry` once it appears (proves polling re-reads).
4. `test_await_one_poll_times_out`: file with no match; `timeout_s=0.2`, real or fake clock; `entry is None`, `elapsed_ms >= ~200`.
5. `test_await_one_cumulative_scanned`: two polls each scanning 3 entries → `scanned` reflects the total across iterations.

- [ ] **Step 2: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -k await_one -q`
Expected: FAIL — `await_one` raises `NotImplementedError`.

- [ ] **Step 3: Implement `await_one`**

Implement per the Produces contract, reusing the Task-3 read/filter logic (factor a private `_read_window(filt, since, until, limit, tail_lines)` helper shared by `scan` and `await_one` if it keeps the code DRY). Honor the injectable clock/sleep test seam.

- [ ] **Step 4: Run `await_one` tests to verify they pass**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -k await_one -q`
Expected: PASS.

- [ ] **Step 5: Write the failing `sample_schema` tests**

Add to `tests/unit/test_logs_client.py`:
1. `test_sample_schema_missing_file_empty`: missing path → all three lists empty.
2. `test_sample_schema_enumerates`: write 3 lines whose `fields` include `orderId`, `status`, plus `@version` and `level_value`; `sample_schema(sample_lines=100)` returns `observed` containing `orderId` and `status` but NOT `@version`/`level_value`; `standard` includes `timestamp`,`level`,`logger`,`message`; `conditional` includes `stack_trace` if one line had it.

- [ ] **Step 6: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -k sample_schema -q`
Expected: FAIL — `NotImplementedError`.

- [ ] **Step 7: Implement `sample_schema`**

Implement per the Produces contract (reuse the tail-read + normalize path).

- [ ] **Step 8: Run the full client suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 9: Commit**

Run: `git add -A && git commit -m "feat(logs): NdjsonFileBackend await_one (one-shot+poll) + sample_schema"`

---

### Task 5: `NdjsonFileBackend` — `follow` (tail streaming source)

The generator that `logs tail` consumes: size-poll the file and yield new matching canonical entries until the stop event is set.

**Files:**
- Modify: `agctl/clients/log_backends/ndjson_file.py` (replace the `follow` stub)
- Test: `tests/unit/test_logs_client.py`

**Interfaces:**
- Consumes: Task 3 (`_normalize`, filter application); `os.stat`/file I/O; `time.sleep`; `threading.Event`.
- Produces:
  - `follow(self, filt, *, stop_event) -> Iterator[CanonicalEntry]` (a generator):
    - Accept `__init__`-injected `monotonic`/`sleep`/`stat_fn` test seams (default to stdlib/`os.stat`) so tests advance time and growth deterministically.
    - Track the byte size last read (start: 0). Each iteration: if `stop_event.is_set()` → return. Stat the path: if missing → sleep `poll_interval` and continue (D10: a not-yet-started service has no file yet; keep waiting). If size grew since last read → open, seek to last offset, read new bytes, split into lines, normalize each (skip non-JSON), apply `filt` (level/logger/match — no window/tail_lines for tail), `yield` each match. Update last offset. Sleep `poll_interval` (use `stop_event.wait(poll_interval)` so a signal during sleep wakes promptly — mirror `ping_loop`).
    - Handle file truncation/rollover (size < last offset): reset last offset to 0 (the active file was rotated).

- [ ] **Step 1: Write the failing `follow` test**

Add `test_follow_yields_new_matches_then_stops` to `tests/unit/test_logs_client.py`: create a backend over a temp file with one initial `INFO` line; drive `follow(LogFilter(level="ERROR"), stop_event=event)` with injected fake `sleep` and a fake `stat`/growth sequence that (a) first reports the initial size, (b) then appends an `ERROR` line and reports the larger size, (c) then the test sets `stop_event`; assert the generator yields exactly the one `ERROR` entry and then stops (does not block forever). Also `test_follow_missing_file_waits`: a missing path with a fake that never grows + a `stop_event` pre-set after one iteration → generator yields nothing and returns.

- [ ] **Step 2: Run to verify it fails**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -k follow -q`
Expected: FAIL — `NotImplementedError`.

- [ ] **Step 3: Implement `follow`**

Implement the generator per the Produces contract, using `stop_event.wait(poll_interval)` for interruptible sleep (the `ping_loop` discipline).

- [ ] **Step 4: Run the client suite + full suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add -A && git commit -m "feat(logs): NdjsonFileBackend follow generator for tail streaming"`

---

### Task 6: `LogClient` — backend selection + config validation wiring

The `DbClient` analog: select a backend by `source.type`, validate the source, and delegate. Also wires `logs.sources` validation into `agctl config validate`.

**Files:**
- Create: `agctl/clients/log_client.py`
- Modify: `agctl/config/validator.py` (add a `logs.sources` validation pass in `validate_config`)
- Test: `tests/unit/test_logs_client.py`

**Interfaces:**
- Consumes: `NdjsonFileBackend` from `agctl/clients/log_backends/ndjson_file.py`; `LogBackend`/DTOs from `log_backend_protocol.py`; `ConfigError` from `agctl/errors.py`; `importlib.metadata.entry_points`.
- Produces:
  - `agctl/clients/log_client.py`:
    - `LOG_BACKEND_ENTRY_POINT_GROUP = "agctl.logs_backends"`.
    - `BUILTIN_BACKENDS: dict[str, type] = {"file": NdjsonFileBackend}`.
    - `class LogClient`:
      - `__init__(self, source, *, backend=None, backends=None)`: if `backend` injected (DI), use it; else `available = backends if backends is not None else LogClient.load_backends()`; `src_type = source.type`; if `not src_type or src_type not in available` → `raise ConfigError(f"Unknown logs backend type: {src_type}", {"type": src_type})`; construct `self._backend = available[src_type](source)`; then call `self._backend.validate_config()` (command-entry guard — surfaces e.g. missing `path` as `ConfigError`).
      - `@classmethod load_backends(cls) -> dict[str, type]`: mirror `DbClient.load_drivers` exactly — read `agctl.logs_backends` entry points (skip any that fail `.load()`), then `backends.update(BUILTIN_BACKENDS)`. Return the merged mapping.
      - Delegating methods: `validate_config(self)` (calls `self._backend.validate_config()`), `scan`, `await_one`, `follow`, `sample_schema` — each forwards to `self._backend.<method>(...)` with the same signature.
  - `agctl/config/validator.py`: in `validate_config(cfg)`, after the existing checks, add a pass: `from ..clients.log_client import LogClient` (local import to avoid an import cycle at module load); for each `(name, source)` in `cfg.logs.sources.items()`, attempt `LogClient(source)` inside try/except `ConfigError`; on `ConfigError`, append `{"path": f"logs.sources.{name}", "message": str(err)}` to `errors`. (Construction runs `validate_config`, surfacing e.g. a missing `path` or unknown `type`.)
- Consumes (validator): `Config`, `ConfigError`.

- [ ] **Step 1: Write the failing `LogClient` tests**

Add to `tests/unit/test_logs_client.py`:
1. `test_log_client_selects_file_backend`: `LogClient(LogSource(path="/tmp/x.log"))` constructs without error; `isinstance(client._backend, NdjsonFileBackend)`; `client.validate_config()` returns `None`.
2. `test_log_client_unknown_type_raises`: `LogClient(LogSource(type="victoria", path=None))` → `ConfigError` whose message contains `Unknown logs backend type` and `detail["type"]=="victoria"`.
3. `test_log_client_missing_path_raises`: `LogClient(LogSource(type="file"))` (path `None`) → `ConfigError` mentioning `path`.
4. `test_log_client_injected_backend_skips_lookup`: pass `backend=<fake>`; `load_backends` is NOT called and the fake is used (verify with a fake whose `validate_config` is a no-op and a `scan` returns a sentinel `ScanResult`; assert `client.scan(...)` returns the sentinel). This proves the DI test seam.
5. `test_load_backends_includes_file_and_skips_broken`: `LogClient.load_backends()["file"] is NdjsonFileBackend`; monkeypatch `importlib.metadata.entry_points` to return one broken entry point (`.load` raises) + the built-ins still win → `file` present, no exception.

- [ ] **Step 2: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -k log_client -q`
Expected: FAIL — `LogClient` undefined.

- [ ] **Step 3: Implement `LogClient` + `load_backends` + `BUILTIN_BACKENDS`**

Create `agctl/clients/log_client.py` per the Produces contract, mirroring `agctl/clients/db_client.py` structure (`BUILTIN_BACKENDS` like `BUILTIN_DRIVERS`; `load_backends` like `load_drivers`; `__init__` DI like `DbClient.__init__`).

- [ ] **Step 4: Run the LogClient tests to verify they pass**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -k log_client -q`
Expected: PASS.

- [ ] **Step 5: Write the failing validator-wiring test**

Add to `tests/unit/test_logs_client.py` (or `test_config_validator.py` if it exists — check; otherwise here): build a `Config` with `logs.sources.svc` having `type="file"` and no `path`; call `validate_config(cfg)`; assert the returned `errors` contains an entry with `path=="logs.sources.svc"` and a message mentioning `path`. Also assert a well-formed source (`type="file", path="/tmp/x.log"`) contributes no error.

- [ ] **Step 6: Run to verify it fails**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_client.py -k validator -q` (adjust `-k` to the test name)
Expected: FAIL — no logs validation present yet.

- [ ] **Step 7: Wire logs validation into `validator.py`**

Add the `logs.sources` validation pass to `validate_config` per the Produces contract.

- [ ] **Step 8: Run the full suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS (existing validator tests unaffected — the new pass only adds errors for malformed logs sources, which no existing fixture has).

- [ ] **Step 9: Commit**

Run: `git add -A && git commit -m "feat(logs): LogClient backend selector + logs.sources config validation"`

---

### Task 7: `logs query` + `logs assert` commands + CLI group wiring

The two `@envelope` commands and their shared helpers, plus wiring the `logs` Click group into `cli.py` so `CliRunner.invoke(cli, ["logs", …])` works (command tests go through the wired CLI, matching the kafka test style). `tail` is added to the same group in Task 8.

**Files:**
- Create: `agctl/commands/logs_commands.py`
- Modify: `agctl/cli.py` (import the logs commands; add `@cli.group(name="logs")`; `logs_group.add_command(logs_query)`/`logs_assert`)
- Modify: `tests/fixtures/agctl.yaml` (add a `logs:` section)
- Test: `tests/unit/test_logs_commands.py` (new file)

**Interfaces:**
- Consumes: `envelope`, `load_config_or_raise` from `agctl/command.py`; `compile_jq` from `agctl/assertions.py`; `fill_placeholders` from `agctl/resolution.py`; `parse_params` from `agctl/params.py`; `ConfigError`, `AssertionFailure`, `TemplateNotFound` from `agctl/errors.py`; `LogClient` from `agctl/clients/log_client.py`; `LogSource` from `agctl/config/models.py`; `LogFilter` from `agctl/clients/log_backend_protocol.py`; `datetime`, `re`.
- Produces:
  - `new_logs_client(source) -> LogClient` — test seam (tests monkeypatch `logs_commands.new_logs_client`); body: `from ..clients.log_client import LogClient; return LogClient(source)`.
  - `_parse_since_until(value: str) -> datetime` — parse `value` to an aware UTC `datetime`: if it matches `^(\d+)([smh])$`, compute `now(UTC) − duration` (s/m/h); else if it parses as ISO-8601 (contains `"T"`), parse and normalize via `_to_utc`; else raise `ConfigError(f"invalid --since/--until value: {value!r}", {"value": value})`. (`_to_utc`/`_parse_iso_datetime` reused from `agctl/assertions.py`.)
  - `_build_log_filter(*, level, logger, message, match, params) -> LogFilter`:
    - `level = level.upper() if level else None`.
    - `match_jq`: if `match is not None`: `filled = fill_placeholders(match, params)`; `compile_jq(filled, label="logs --match")` (up-front loud guard — raises `ConfigError` on a malformed expr OR a missing jq library); store `filled`. Else `None`.
    - Return `LogFilter(level=level, logger_glob=logger, message_substring=message, match_jq=match_jq, params=params)`.
  - `_resolve_source(cfg, name) -> LogSource` — `if name not in cfg.logs.sources: raise ConfigError(f"Unknown logs source: {name}", {"source": name})`; return `cfg.logs.sources[name]`.
  - `_logs_query_core(config_path, source, level, logger, message, match, param, since, until, limit) -> dict`:
    - `cfg = load_config_or_raise(config_path)`; `src = _resolve_source(cfg, source)`; `params = parse_params(param)`; `filt = _build_log_filter(level=level, logger=logger, message=message, match=match, params=params)`.
    - `since_dt = _parse_since_until(since) if since else None`; `until_dt = _parse_since_until(until) if until else datetime.now(UTC)`.
    - `client = new_logs_client(src)`; `res = client.scan(filt, since=since_dt, until=until_dt, limit=limit or cfg.logs.defaults.limit, tail_lines=cfg.logs.defaults.tail_lines)`.
    - Return `{"source": source, "matched": res.matched, "scanned": res.scanned, "truncated": res.truncated, "entries": [dataclasses.asdict(e) for e in res.entries]}`.
  - `_logs_query_envelope = envelope("logs.query")(_logs_query_core)`.
  - `_logs_assert_core(config_path, source, level, logger, message, match, param, since, not_, timeout) -> dict`:
    - Load cfg, resolve source, build filter (same as query). `--since` is required: if `since is None` → `ConfigError("--since is required for logs assert", {})`.
    - `since_dt = _parse_since_until(since)`. `timeout_s = float(timeout) if timeout is not None else 0.0`.
    - `res = client.await_one(filt, since=since_dt, timeout_s=timeout_s, poll_interval_ms=cfg.logs.defaults.poll_interval_ms)`.
    - `matched = res.entry is not None`; `succeeded = (not not_) if matched else (not_ )` — i.e. `succeeded = matched if not not_ else (not matched)`.
    - If `not succeeded`: `raise AssertionFailure(message, detail)` where message is `"Matching log entry found" if not_ else "No matching log entry found within <timeout>s"` and detail is `{"source": source, "not": bool(not_), "filter": {"level": filt.level, "logger": filt.logger_glob, "message": filt.message_substring, "match": filt.match_jq}, "since": <iso of since_dt>, "entries_scanned": res.scanned, "elapsed_ms": res.elapsed_ms}` plus `"matching_entry": dataclasses.asdict(res.entry)` when `res.entry is not None`.
    - On success return `{"source": source, "matched": True, "matching_entry": (dataclasses.asdict(res.entry) if res.entry is not None else None), "entries_scanned": res.scanned, "elapsed_ms": res.elapsed_ms}`.
  - `_logs_assert_envelope = envelope("logs.assert")(_logs_assert_core)`.
  - `logs_query` and `logs_assert` — `@click.command(...)` with the flags from the spec §6.2/§6.3 (`--source` required, `--level`/`--logger`/`--message`/`--match`/`--param`(multiple)/`--since`/`--until`(query only)/`--limit`(query only)/`--not`(assert only, flag)/`--timeout`(assert only, optional float)/`--config`), `@click.pass_context`, resolving `config_path = ctx.obj.get("config_path") if ctx.obj else None` and calling the envelope.
  - `logs_group` is created in `cli.py` (see below).
- Consumes (test): `CliRunner`, `from agctl.cli import cli`, the fixture config + `ENV` (mirror `test_kafka_commands.py`).

- [ ] **Step 1: Add the `logs:` section to the test fixture**

Append to `tests/fixtures/agctl.yaml` (after `mocks:`): a `logs:` section with `sources.order-service` (`path: "logs/order-service.log"`, `format: logstash`, `service: order-service`) and `sources.payment-service` (same shape), and a `defaults:` block (`tail_lines: 200`, `limit: 50`, `timeout_seconds: 10`, `poll_interval_ms: 100`). `type` omitted → defaults to `"file"`. (Paths need not exist — D10.)

- [ ] **Step 2: Write the failing `query`/`assert` tests**

Create `tests/unit/test_logs_commands.py` mirroring `test_kafka_commands.py`: `from agctl.cli import cli`, `FIXTURE = .../fixtures/agctl.yaml`, the same `ENV` dict, `CliRunner`. Define a `_FakeLogsClient` (has `scan`/`await_one`/`sample_schema`/`follow`/`validate_config`) whose `scan`/`await_one` return canned `ScanResult`/`AwaitResult`; a helper `_install_fake(monkeypatch, scan=None, await_one=None)` that does `monkeypatch.setattr(logs_commands, "new_logs_client", lambda src: _FakeLogsClient(scan=scan, await_one=await_one))`.
1. `test_logs_query_returns_entries`: fake `scan` returns 1 entry; `cli` invoke `["logs","query","--source","order-service","--config",FIXTURE]` with `env=ENV` → exit 0, `result["command"]=="logs.query"`, `result["result"]["matched"]==1`, `result["result"]["entries"]` length 1.
2. `test_logs_query_level_filter_passed`: assert the `LogFilter` handed to the fake had `level=="ERROR"` when `--level error` given (case-insensitive → UPPER).
3. `test_logs_query_truncated_flag`: fake `scan` returns `matched=5, entries=[…2…]` with `truncated=True` → result `truncated==True`.
4. `test_logs_assert_match_success`: fake `await_one` returns an entry → exit 0, `result["result"]["matched"]==True`.
5. `test_logs_assert_no_match_is_assertion_error`: fake `await_one` returns `entry=None` → exit 1, `error.type=="AssertionError"`, `error.detail` carries `filter`/`entries_scanned`/`since`.
6. `test_logs_assert_not_success_when_no_match`: `--not` + fake `await_one` returns `None` → exit 0 (no match is the good outcome under `--not`).
7. `test_logs_assert_not_failure_when_match_found`: `--not` + fake returns an entry → exit 1, `error.detail.matching_entry` present.
8. `test_logs_assert_since_required`: `logs assert --source order-service` with no `--since` → exit 2, `error.type=="ConfigError"` mentioning `--since`.
9. `test_logs_assert_timeout_optional_default_oneshot`: omit `--timeout`; assert the fake's `await_one` was called with `timeout_s == 0.0` (one-shot). With `--timeout 5`, called with `timeout_s == 5.0`.
10. `test_logs_match_placeholder_fill_and_compile`: `--match '.fields.orderId == "{orderId}"' --param orderId=ord-1` → the fake's `LogFilter.match_jq == '.fields.orderId == "ord-1"'` and exit 0.
11. `test_logs_match_compile_loud_fail`: `--match '.fields.orderId == '` (malformed) → exit 2, `error.type=="ConfigError"`.
12. `test_logs_unknown_source`: `--source nope` → exit 2 `ConfigError`.
13. `test_logs_match_missing_jq_extra`: monkeypatch `agctl.assertions._jq` to raise `ConfigError` (or simulate); `--match '.x'` → exit 2 `ConfigError` whose message contains `agctl[logs]`.

- [ ] **Step 3: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_commands.py -q`
Expected: FAIL — `logs_commands`/`logs` group undefined (`Error: No such command 'logs'` or ImportError).

- [ ] **Step 4: Implement `logs_commands.py`**

Implement `new_logs_client`, `_parse_since_until`, `_build_log_filter`, `_resolve_source`, the two cores + envelopes, and the two Click commands per the Produces contract. Match the kafka command style (`@click.option(...)` declarations, `@click.pass_context`, config_path resolution, envelope call).

- [ ] **Step 5: Wire the `logs` group into `cli.py`**

In `agctl/cli.py`: `from .commands.logs_commands import logs_assert, logs_query`; add `@cli.group(name="logs") def logs_group() -> None: """Log query/assert/tail commands."""`; add `logs_group.add_command(logs_query)` and `logs_group.add_command(logs_assert)` (tail is added in Task 8).

- [ ] **Step 6: Run the command tests + full suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_commands.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 7: Commit**

Run: `git add -A && git commit -m "feat(logs): add logs query + assert commands and logs CLI group"`

---

### Task 8: `logs tail` — streaming command

The streaming exception (D9): NDJSON lines, signal-driven stop, summary line, manual startup-error envelopes — mirroring `http ping`.

**Files:**
- Modify: `agctl/commands/logs_commands.py` (add `logs_tail` + a small `_emit_stdout_line` + `_tail_run` helper; reuse `_build_log_filter`/`_resolve_source`)
- Modify: `agctl/cli.py` (`logs_group.add_command(logs_tail)`)
- Test: `tests/unit/test_logs_commands.py`

**Interfaces:**
- Consumes: the Task-7 helpers; `emit` from `agctl/output.py`; `signal`, `threading`, `time`, `json`, `sys`; `ping_loop`'s signal-installing pattern from `http_commands.py` (read it; mirror the `signal.signal(SIGTERM/SIGINT, _handler)` + restore-in-`finally` + `stop_event.wait(interval)` discipline).
- Produces:
  - `_emit_stdout_line(line: dict) -> None` — write one NDJSON line to stdout (mirror `http_commands._emit_stdout_line`).
  - `_tail_run(client, filt, *, stop_event, emit_line) -> tuple[int, int]` — drive `client.follow(filt, stop_event=stop_event)`, emit each yielded entry (as `dataclasses.asdict(entry)`) via `emit_line`, count emitted; return `(emitted, duration_ms)`. (Keeps the signal/handler plumbing testable separately, like `_run_pings`.)
  - `logs_tail` — `@click.command("tail")` with `--source` (required), `--level`/`--logger`/`--match`/`--param`(multiple), `--duration`(float)/`--until-stopped`(flag), `--config`; `@click.pass_context`. NOT `@envelope`-wrapped. Behavior:
    - Resolve `config_path`; `start = time.monotonic()`.
    - Mutex: if `--duration` and `--until-stopped` both set → `emit(ok=False, command="logs.tail", error={"type":"ConfigError","message":"--duration and --until-stopped are mutually exclusive","detail":{}}, duration_ms=...)` then `raise SystemExit(2)`.
    - Try: `cfg = load_config_or_raise(config_path)`; `src = _resolve_source(cfg, source)`; `params = parse_params(param)`; `filt = _build_log_filter(level=level, logger=logger, message=None, match=match, params=params)`. (Tail takes no `--message` per spec §6.4.) On `AgctlError` → `emit(ok=False, command="logs.tail", error=err.to_dict(), duration_ms=...)` + `raise SystemExit(err.exit_code)`; on other `Exception` → `InternalError` envelope + `raise SystemExit(2)` (mirror `http_ping` startup-error handling).
    - `client = new_logs_client(src)`.
    - Install `stop_event = threading.Event()` + SIGTERM/SIGINT handlers that `stop_event.set()` (guard for non-main-thread); restore in `finally` (mirror `http_ping` exactly).
    - `emitted, total_ms = _tail_run(client, filt, stop_event=stop_event, emit_line=_emit_stdout_line)` (honoring `--duration` by also setting a deadline via `stop_event` if given — e.g. a timer thread, or check elapsed inside `_tail_run`; simplest: if `duration` set, `stop_event.set()` after `duration` via a daemon `threading.Timer`).
    - Emit summary `{"summary": True, "total_emitted": emitted, "duration_ms": total_ms}` via `_emit_stdout_line`; `raise SystemExit(0)`.
  - `cli.py`: `from .commands.logs_commands import ..., logs_tail`; `logs_group.add_command(logs_tail)`.
- Consumes (test seam): `_tail_run` takes `emit_line` and `client.follow` is driven by `stop_event`, so tests inject a fake client whose `follow` yields canned entries then checks `stop_event`.

- [ ] **Step 1: Write the failing `tail` tests**

Add to `tests/unit/test_logs_commands.py`:
1. `test_logs_tail_streams_entries_and_summary`: a fake client whose `follow` yields 2 entries then blocks until `stop_event` is set (test sets it after collecting 2 lines via a background thread or by making `follow` yield-then-return when stop_event set); invoke `["logs","tail","--source","order-service","--config",FIXTURE]` with `env=ENV` and `CliRunner` (the fake is installed via `new_logs_client`); assert stdout has 2 entry JSON lines + 1 summary line (`json.loads` each: entries lack a `summary` key; summary has `summary==True`, `total_emitted==2`); exit 0.
2. `test_logs_tail_mutex_duration_until_stopped`: `--duration 5 --until-stopped` → exit 2, one envelope line with `error.type=="ConfigError"`, no entry lines streamed.
3. `test_logs_tail_unknown_source_startup_error`: `--source nope` → exit 2, `error.type=="ConfigError"`, emitted as a single envelope (not NDJSON entries).
4. `test_logs_tail_match_compile_loud_fail`: `--match '.fields.x =='` (malformed) → exit 2 `ConfigError` envelope at startup, before streaming.

- [ ] **Step 2: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_commands.py -k tail -q`
Expected: FAIL — `logs_tail` undefined / `No such command 'tail'` on the `logs` group.

- [ ] **Step 3: Implement `logs_tail` + `_emit_stdout_line` + `_tail_run`**

Implement per the Produces contract, reading `agctl/commands/http_commands.py` (`http_ping`) and mirroring its signal-install/restore + manual-envelope + summary + `SystemExit` discipline.

- [ ] **Step 4: Register `logs_tail` on the group in `cli.py`**

Add `logs_tail` to the import and `logs_group.add_command(logs_tail)`.

- [ ] **Step 5: Run the command tests + full suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_logs_commands.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 6: Commit**

Run: `git add -A && git commit -m "feat(logs): add logs tail streaming command"`

---

### Task 9: `discover` — `log-sources` category

Add the category to the four discover modes (summary/category/item/search). Item detail constructs a `LogClient` and calls `sample_schema`.

**Files:**
- Modify: `agctl/commands/discover_commands.py` (`_VALID_CATEGORIES`, `_SUMMARY_HINT`, `_summary_core`, `_category_core`, `_item_core`, `_search_core`)
- Test: `tests/unit/test_discover_command.py`

**Interfaces:**
- Consumes: `LogClient` from `agctl/clients/log_client.py` (local import inside `_item_core` to avoid a module-load cycle); existing discover helpers.
- Produces:
  - `_VALID_CATEGORIES`: append `"log-sources"`.
  - `_SUMMARY_HINT`: append `, log-sources` to the category list in the hint string.
  - `_summary_core`: add `"log_sources": len(cfg.logs.sources)`.
  - `_category_core`: `elif category == "log-sources":` → for `name, src in cfg.logs.sources.items()`: `items.append({"name": name, "description": f"{src.type} logs for {name} ({src.path or '?'})"})`.
  - `_item_core`: `if category == "log-sources":` → if `name not in cfg.logs.sources`: `raise TemplateNotFound(f"Unknown logs source: {name}", {"path": f"logs.sources.{name}"})`; `src = cfg.logs.sources[name]`; `schema = LogClient(src).sample_schema(sample_lines=100)`; return `{"category":"log-sources","name":name,"description":f"{src.type} logs for {name}","path":src.path,"type":src.type,"format":src.format,"schema_fields":{"standard":schema.standard,"conditional":schema.conditional,"observed":schema.observed},"example":f"agctl logs query --source {name} --level ERROR --since 5m"}`.
  - `_search_core`: add a loop over `cfg.logs.sources.items()` matching `name` (lowercased substring) or `src.path` (lowercased); append `{"category":"log-sources","name":name,"description":<same as category listing>}`.

- [ ] **Step 1: Write the failing discover tests**

Add to `tests/unit/test_discover_command.py`:
1. `test_discover_summary_includes_log_sources`: invoke `agctl discover --config FIXTURE` (env `ENV`); `result["result"]["log_sources"] == <count of logs sources in the fixture>` and the hint contains `log-sources`.
2. `test_discover_category_log_sources`: `agctl discover --category log-sources --config FIXTURE` → items include `order-service` with a description naming `file` and the path; `count` matches.
3. `test_discover_item_log_sources_with_schema`: write a temp `agctl.yaml` (version `"2"`) whose `logs.sources.svc.path` points at a temp `.log` file with 2 NDJSON lines (one carrying `orderId:"o1"` and a `stack_trace`); invoke `discover --category log-sources --name svc --config <temp>`; assert `schema_fields.standard` includes `timestamp`/`level`/`logger`/`message`, `conditional` includes `stack_trace`, `observed` includes `orderId` (and not `@version`), and `example` starts with `agctl logs query --source svc`.
4. `test_discover_search_finds_log_source`: `agctl discover --search order --config FIXTURE` → matches include a `log-sources` entry for `order-service`.
5. `test_discover_item_unknown_log_source`: `--category log-sources --name nope` → exit 2, `error.type` per `TemplateNotFound` handling.

- [ ] **Step 2: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_discover_command.py -k "log_sources or log or discover_summary" -q`
Expected: FAIL — `log-sources` not a valid category.

- [ ] **Step 3: Add the `log-sources` category across the four modes**

Edit `discover_commands.py` per the Produces contract.

- [ ] **Step 4: Run the discover tests + full suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_discover_command.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add -A && git commit -m "feat(discover): add log-sources category with sampled schema_fields"`

---

### Task 10: Integration tests (always-on, real temp files)

File logs need no broker/DB/HTTP service, so integration coverage runs always-on (not gated behind `AGCTL_TEST_LIVE`) — driving the real `NdjsonFileBackend` end-to-end through the Click CLI.

**Files:**
- Create: `tests/integration/test_logs_commands.py`

**Interfaces:**
- Consumes: Tasks 3–9 (real backend, real commands, real discover). `CliRunner`, `tmp_path`, the fixture `ENV` where useful.
- Produces: E2E confidence that the file backend + commands + discover work on real NDJSON files. No fakes; no monkeypatch of `new_logs_client`.

- [ ] **Step 1: Write the integration tests**

Create `tests/integration/test_logs_commands.py`. Each test writes a temp `agctl.yaml` whose `logs.sources.svc.path` is a temp `.log` file it also writes (real logstash NDJSON lines), then invokes the real `cli`:
1. `test_query_real_file_filters_and_window`: write lines spanning two timestamps; `logs query --source svc --level ERROR --since 1h` returns only the ERROR rows in the window; assert `matched`/`entries`.
2. `test_assert_positive_then_not`: write an INFO line; `logs assert --source svc --level INFO --since 1h` exits 0; `logs assert --source svc --level ERROR --since 1h` exits 1; `logs assert --source svc --level INFO --since 1h --not` exits 1 (an INFO was present — forbidden under `--not`); `logs assert --source svc --level FATAL --since 1h --not` exits 0 (no FATAL — good).
3. `test_assert_poll_finds_appended_line`: write an empty file; start `logs assert --source svc --match '.fields.orderId == "ord-2"' --since 1h --timeout 3` in a thread/process, append the matching line after ~0.5s; assert exit 0 (poll found it). (Use `subprocess` or a background `CliRunner.invoke` with a writer thread; keep it deterministic and fast.)
4. `test_tail_streams_appended_lines`: start `logs tail --source svc --duration 1` (real streaming), append 2 lines during the run; assert stdout has 2 entry lines + 1 summary line with `total_emitted==2`, exit 0.
5. `test_missing_file_is_empty_not_error`: source path that doesn't exist; `logs query --source svc` exits 0 with `matched==0` (D10).
6. `test_discover_item_real_schema`: reuse the temp file; `discover --category log-sources --name svc` returns populated `schema_fields`.

- [ ] **Step 2: Run the integration tests**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/integration/test_logs_commands.py -q`
Expected: PASS (always-on; no `AGCTL_TEST_LIVE` gate).

- [ ] **Step 3: Run the entire suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest -q`
Expected: PASS.

- [ ] **Step 4: Commit**

Run: `git add -A && git commit -m "test(logs): always-on integration tests over real NDJSON files"`

---

### Task 11: Documentation sync

Sync DESIGN.md, ARCHITECTURE.md, the consumer skill, the sample config, and README to the as-built feature. Per CLAUDE.md, invoke the `docs-watcher` subagent for DESIGN/ARCHITECTURE altitude; make the targeted concrete edits (examples, sample config, skill reference) directly.

**Files:**
- Modify: `agctl/data/sample-config.yaml` (add a `logs:` section mirroring the fixture)
- Modify: `docs/DESIGN.md`, `docs/ARCHITECTURE.md` (via `docs-watcher` + targeted edits)
- Modify: `skills/agctl-config/reference/` (logs authoring reference)
- Modify: `README.md` (surface the `logs` group + `pip install 'agctl[logs]'`)

**Interfaces:**
- Consumes: Tasks 1–10 (as-built behavior).
- Produces: docs matching as-built.

- [ ] **Step 1: Add a `logs:` section to the packaged sample config**

In `agctl/data/sample-config.yaml`: add a `logs:` section (commented example with one `file`/`logstash` source and the `defaults:` block), mirroring the test fixture. This is the `agctl config init` starter.

- [ ] **Step 2: Invoke `docs-watcher` for DESIGN/ARCHITECTURE altitude**

Dispatch the `docs-watcher` subagent (CLAUDE.md "Docs Sync") to reconcile:
- DESIGN.md: §2 add `logs:` config (`sources` with `type`/`path`/`format`/`service`, `defaults`); §3 add `logs query`/`assert`/`tail` (flags, `--not`, optional `--timeout`, one-shot+poll, `--match` placeholder-fill); add `agctl.logs_backends` to the extension-points list alongside `agctl.db_drivers`.
- ARCHITECTURE.md: clients layout (`log_backend_protocol.py`, `log_client.py`, `log_backends/ndjson_file.py`); the `LogBackend` Protocol + `CanonicalEntry` contract + `LogClient` selection; note `logs tail` as a third streaming exception (`http ping`, `mock run`); note the `logs` extra and the lazy-jq rule.

- [ ] **Step 3: Update the consumer skill + README**

In `skills/agctl-config/reference/`: add a `logs.sources` authoring reference (`type` default `file`, `path`, `format`, the canonical-entry/`.fields.*` model, `--not` and one-shot+poll semantics, missing-file-is-empty, the rolled-over-history limitation). In `README.md`: surface the `logs` group and `pip install 'agctl[logs]'`.

- [ ] **Step 4: Verify the suite still passes + commit**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS (no test should depend on the prose; if a drift-guard asserts the sample-config content, the Task-11 Step 1 edit satisfies it).

Run: `git add -A && git commit -m "docs: sync logs feature — DESIGN/ARCHITECTURE, sample config, skill, README"`

---

## Self-Review (completed)

**1. Code scan:** No method bodies, algorithms, or test code appear. Each step states behavior, exact expected results, signatures, and data shapes (e.g. the `_normalize` field mapping, `scan` semantics, `_build_log_filter` rules). The dict-literal contract snippets (e.g. `_item_core` return shape) are data contracts, not implementation.

**2. Self-containment:** Every task lists exact files (with line anchors where stable), Consumes/Produces contracts (signatures, fields, defaults, error cases), and per-test scenarios with expected results. A zero-context implementer can do any task alone — e.g. Task 4's `await_one` contract fully defines behavior without reading Task 3 beyond the named `_read_window`/`scan` reuse note.

**3. Spec coverage:** D1 (backend seam) → Tasks 2,6; D2 (canonical entry) → Task 2 + 3; D3 (`.fields.*`) → Task 3 (`_normalize`); D4 (`type`/`format`) → Task 1 + 6; D5 (one-shot+poll) → Task 4 + 7; D6 (`--not`) → Task 7; D7 (`--match` fill+compile) → Task 7 (`_build_log_filter`); D8 (push-down/client-side) → Task 3; D9 (tail streaming) → Task 8; D10 (missing file empty) → Tasks 3,4,5; D11 (`logs` extra, lazy jq) → Task 2 + 7; D12 (entry-point/built-in) → Task 2 + 6. Spec §6 commands → Tasks 7,8; §7 config → Task 1; §8 backend contract → Tasks 2,3,4,5,6; §9 error model → Tasks 3,6,7,8; §10 testing → Tasks 3–10; §11 validation → Task 6; §12 discover → Task 9; §13 packaging → Task 2; §15 deferred = none implemented (correct); §16/17 docs → Task 11.

**4. Placeholder scan:** No TBD/TODO/"add error handling"/"similar to Task N". Each test step names the scenario + expected result; each impl step names the behavior. (The one "see Task N" reference is the explicit, named reuse of `_read_window` in Task 4, which restates the relevant contract rather than deferring.)

**5. Type consistency:** `CanonicalEntry`/`LogFilter`/`ScanResult`/`AwaitResult`/`SchemaDescriptor` field names are identical across Tasks 2 (definition), 3/4/5 (backend), 7 (command `dataclasses.asdict` usage), 9 (discover `schema_fields`). `LogClient.__init__(source, *, backend=None, backends=None)` + `load_backends()` consistent across Tasks 6, 7 (`new_logs_client`), 9 (`LogClient(src)` in `_item_core`). `_parse_since_until`, `_build_log_filter`, `_resolve_source`, `new_logs_client` consistent across Task 7's definition and Task 8's reuse. `await_one` signature `(filt, *, since, timeout_s, poll_interval_ms)` consistent across Tasks 2 (Protocol), 4 (impl), 7 (call) — with the Global-Constraints refinement note that `invert` is command-layer, not a parameter.

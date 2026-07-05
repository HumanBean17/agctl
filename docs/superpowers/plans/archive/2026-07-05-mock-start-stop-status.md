# `agctl mock` start / stop / status — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `agctl mock start` (detached daemon), `agctl mock stop` (stop + verdict), and `agctl mock status` (live introspection) as three new `@envelope` commands on the existing `mock` group, leaving `mock run` unchanged.

**Architecture:** `start` spawns the existing `mock run` engine as a detached subprocess (`python -m agctl mock run …`, new session, stdout→log file), writes a port-keyed pidfile, and polls the log until the daemon's `started` line appears. `stop`/`status` resolve the target via the pidfile, then parse the daemon's NDJSON log (shared pure-function parser) for the `summary` line and failure events. `stop` raises `AssertionFailure` (exit 1) with the verdict in `error.detail` when any fatal failure event is found. All daemon state is confined to `<state-dir>/` (default `.agctl/`).

**Tech Stack:** Python ≥3.11, stdlib only (`os`, `signal`, `json`, `subprocess`, `time`, `pathlib`, `datetime`). Click (already a dep). No new dependencies. Tests: `pytest` + `click.testing.CliRunner`.

**Spec:** [`docs/superpowers/specs/active/2026-07-05-mock-start-stop-status-design.md`](../../specs/active/2026-07-05-mock-start-stop-status-design.md)

## Global Constraints

- **Python ≥3.11.** No new third-party dependencies; stdlib + existing `click`/`pyyaml`/`pydantic` only.
- **Follow existing patterns exactly:** `@envelope("cmd.name")(_core)` split (`agctl/command.py`); `_core(...) -> dict` returns the `result` payload and raises `AgctlError` subclasses on failure; `ConfigError(message, detail_dict)` (exit 2) for misuse; `AssertionFailure(message, detail_dict)` (exit 1) for the stop-verdict-failed case. One JSON object per command on stdout (the `@envelope` wrapper handles emission — the new cores never call `emit` directly).
- **`mock run` must remain unchanged** — no edits to its flags, output, or to `agctl/mock/engine.py`.
- **All daemon state confined to `<state-dir>/` (default `.agctl/`)** — pidfile + log only, no DB, no lock files, no state elsewhere.
- **POSIX-only:** uses `start_new_session=True`, `signal.SIGTERM`/`SIGKILL`, `os.kill(pid, 0)` for liveness. (Windows support is an explicit non-goal — spec §12.)
- **TDD discipline:** write the failing test → run it (confirm it fails for the right reason) → implement minimal code → run (confirm pass) → commit. Frequent commits; one logical change per commit.
- **No code in this plan:** every step describes behavior, exact signatures, data shapes, and expected test results. The implementer writes the code.

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `agctl/__main__.py` | **Create** | One-line `python -m agctl` entry — required so `start` can spawn the daemon via `[sys.executable, "-m", "agctl", …]`. |
| `agctl/mock/daemon.py` | **Create** | Pure functions for daemon orchestration state: pidfile read/write/remove, liveness, enumerate/select targets, and the shared NDJSON log parser + failure-event taxonomy. No engine dependency; fully unit-testable with temp dirs. |
| `agctl/commands/mock_commands.py` | **Modify** | Add `mock_start`, `mock_stop`, `mock_status` Click commands + their `@envelope`-wrapped `_core` functions + the injectable `spawn_daemon` test seam. Reuses existing `_resolve_engines`. |
| `agctl/cli.py` | **Modify** | Register `mock_start`, `mock_stop`, `mock_status` on `mock_group` (one `add_command` per task). |
| `tests/unit/test_mock_daemon.py` | **Create** | Unit tests for `agctl/mock/daemon.py` pure functions (Tasks 2–3). |
| `tests/unit/test_mock_lifecycle.py` | **Create** | Unit tests for `start`/`stop`/`status` command cores via the spawn seam + real short-lived subprocesses as stand-in daemons (Tasks 4–6). |
| `tests/unit/test_packaging.py` | **Modify** | Add the `python -m agctl` test (Task 1). |
| `tests/integration/test_mock_daemon.py` | **Create** | End-to-end round-trip on a free local port (Task 7). No Kafka, no Docker. |
| `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, `skills/` | **Modify** | Doc sync per spec §14 (Task 8), via the `docs-watcher` subagent per project `CLAUDE.md`. |

**Dependency direction** (inward-only, no cycles — ARCHITECTURE §3): `commands/mock_commands → {mock/daemon, command, config, errors, output}`; `mock/daemon → {errors}` only. `mock/engine.py` is unchanged.

**Task dependency order:** Task 1 → {2, 3} → 4 → {5, 6} → 7 → 8. Tasks 2 and 3 are independent pure-function foundations. Task 4 (start) consumes 1+2+3. Tasks 5 (stop) and 6 (status) consume 2+3 and may be done in either order. Task 7 exercises 4–6 end-to-end. Task 8 is the final doc sync.

---

## Task 1: `python -m agctl` module entry

**Files:**
- Create: `agctl/__main__.py`
- Test: `tests/unit/test_packaging.py` (add one test — follow the file's existing subprocess-based convention)

**Interfaces:**
- Consumes: `agctl.cli.cli` (the existing Click root group).
- Produces: a module that, when invoked as `python -m agctl <args>`, runs the CLI with `sys.argv` intact. This is the mechanism the Task 4 `spawn_daemon` seam uses to launch the daemon: `subprocess.Popen([sys.executable, "-m", "agctl", "mock", "run", …], …)`. Without `__main__.py`, that invocation fails with `No module named agctl.__main__`.

- [ ] **Step 1: Write the failing test**

Add a test to `tests/unit/test_packaging.py` (match the file's existing style for invoking the CLI). Scenario: run `python -m agctl --help` as a subprocess (using `sys.executable`); assert the process exits 0 and that `stdout` contains the substring `"mock"` (the mock group appears in root help). The test must run the real interpreter, not import in-process — it is verifying the module entry works as a subprocess.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_packaging.py -v -k "module or main or m_agctl"` (use whatever name you gave the test)
Expected: FAIL — the subprocess exits non-zero with stderr containing `No module named agctl.__main__` (or `can't find '__main__' module`).

- [ ] **Step 3: Write minimal implementation**

Create `agctl/__main__.py`. Behavior: import `cli` from `agctl.cli` and invoke it (so `python -m agctl` is equivalent to the `agctl` console script). The file is a couple of lines — a docstring noting it enables `python -m agctl` for subprocess spawning (used by `mock start`), the import, and the call. Guard the call under `if __name__ == "__main__":` per Python module-entry convention.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_packaging.py -v -k "<test name>"`
Expected: PASS (subprocess exits 0, stdout contains `"mock"`).

- [ ] **Step 5: Commit**

Run: `git add agctl/__main__.py tests/unit/test_packaging.py`
Run: `git commit -m "feat(agctl): add python -m agctl module entry (enables mock daemon spawn)"`

---

## Task 2: Pidfile + liveness + target resolution helpers

**Files:**
- Create: `agctl/mock/daemon.py`
- Test: `tests/unit/test_mock_daemon.py`

**Interfaces:**
- Consumes: `agctl.errors.ConfigError` (`ConfigError(message: str, detail: dict)`).
- Produces (all in `agctl/mock/daemon.py`):
  - `pidfile_path(state_dir, port) -> Path` — `<state_dir>/mock-<port>.pid` when `port` is an `int`; `<state_dir>/mock-kafka.pid` when `port` is `None` (kafka-only mock).
  - `log_path(state_dir, port) -> Path` — same naming rule with `.log`.
  - `read_pidfile(path) -> dict | None` — `json.load`; return `None` if the file is missing or unparseable (never raises).
  - `write_pidfile(path, data: dict) -> None` — write `data` as JSON. `data` shape: `{pid: int, listen: str | None, port: int | None, log_path: str, config_path: str | None, started_at: str (ISO-8601 Z), run_id: str}`.
  - `remove_pidfile(path) -> None` — unlink if present, ignore `FileNotFoundError`.
  - `is_alive(pid: int) -> bool` — `os.kill(pid, 0)`; return `False` on `ProcessLookupError`/`OSError` with `errno.ESRCH`; return `True` on `PermissionError` (the process exists but is not ours); return `True` otherwise.
  - `RunningMock` — a dataclass (or typed dict) with the pidfile fields plus `pidfile_path: Path`.
  - `list_running_mocks(state_dir) -> list[RunningMock]` — glob `mock-*.pid` in `state_dir`; for each, `read_pidfile`; if the file yields `None`, skip; build a `RunningMock`; if `is_alive(pid)` keep it, else `remove_pidfile` its pidfile (stale cleanup) and skip it. Return the live list (empty if none). Create `state_dir` if missing (no error on an empty/absent dir — return `[]`).
  - `resolve_target(state_dir, listen: str | None, pid: int | None, all_: bool) -> list[RunningMock]` — returns the list of mocks to operate on:
    - `all_=True` → `list_running_mocks(state_dir)` (all of them; may be empty).
    - `pid` set → the single `RunningMock` whose `pid` matches; if none, raise `ConfigError(f"no running mock with pid {pid}", {"pid": pid})`.
    - `listen` set → the single `RunningMock` whose `listen` string equals `listen`; if none, raise `ConfigError(f"no running mock on {listen}", {"listen": listen})`.
    - none set → `candidates = list_running_mocks(state_dir)`; `len==0` → return `[]` (caller treats as not-running, idempotent); `len==1` → `[candidates[0]]`; `len>1` → raise `ConfigError("multiple mocks running; specify --listen or --pid", {"candidates": [r.listen for r in candidates]})`.

- [ ] **Step 1: Write the failing tests**

In `tests/unit/test_mock_daemon.py`, write these scenarios (one test function each, using the `tmp_path` fixture for `state_dir`):

1. **`is_alive` on live pid:** `is_alive(os.getpid())` returns `True`.
2. **`is_alive` on dead pid:** `is_alive(999_999)` (or a known-unused pid) returns `False`.
3. **path naming:** `pidfile_path(d, 18080)` ends with `mock-18080.pid`; `pidfile_path(d, None)` ends with `mock-kafka.pid`; same for `log_path` with `.log`.
4. **pidfile round-trip:** write a sample `data` dict via `write_pidfile`, then `read_pidfile` returns an equal dict; `read_pidfile` on a non-existent path returns `None`; `remove_pidfile` deletes it (and does not raise on a missing path).
5. **`list_running_mocks` live + stale:** write three pidfiles into `state_dir` — two whose `pid` is `os.getpid()` (live), one whose `pid` is `999_999` (stale). Assert the returned list has length 2 (only live) and that the stale pid's pidfile was removed from disk.
6. **`resolve_target` matrix:** seed `state_dir` with live pidfiles for two distinct ports, then assert: no-arg + 2 running → raises `ConfigError`; no-arg + 1 running (re-seed with one) → returns a 1-element list; no-arg + 0 running (empty dir) → returns `[]`; `listen=` matching one → returns `[that one]`; `listen=` non-matching → raises `ConfigError`; `pid=` matching → returns `[that one]`; `pid=` non-matching → raises `ConfigError`; `all_=True` with 2 running → returns both.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_mock_daemon.py -v`
Expected: FAIL — `ImportError: cannot import name '...' from agctl.mock.daemon` (module/functions do not exist yet).

- [ ] **Step 3: Write minimal implementation**

Create `agctl/mock/daemon.py`. Implement the functions and the `RunningMock` dataclass to the contracts above. Use `pathlib.Path`, `json`, `os`, `errno`. Stale-pidfile cleanup happens inside `list_running_mocks` (the single place that enumerates). `resolve_target` delegates to `list_running_mocks` for the no-arg and `all_` paths. Do not implement the log parser here (Task 3).

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_mock_daemon.py -v`
Expected: PASS — all scenarios green.

- [ ] **Step 5: Commit**

Run: `git add agctl/mock/daemon.py tests/unit/test_mock_daemon.py`
Run: `git commit -m "feat(mock): pidfile, liveness, and target-resolution helpers for daemon lifecycle"`

---

## Task 3: NDJSON log parser + failure taxonomy

**Files:**
- Modify: `agctl/mock/daemon.py` (append the parser section)
- Test: `tests/unit/test_mock_daemon.py` (append parser tests)

**Interfaces:**
- Consumes: nothing new (pure stdlib).
- Produces (append to `agctl/mock/daemon.py`):
  - `FATAL_FAILURE_EVENTS: frozenset[str]` = `{"http.unmatched", "http.body_parse_skipped", "kafka.skipped", "kafka.error"}`.
  - `ALL_FAILURE_EVENTS: frozenset[str]` = `FATAL_FAILURE_EVENTS | {"capture.missing"}`.
  - `EVENT_TO_COUNTER: dict[str, str]` — maps each tallyable event name to its `summary` counter field: `http.hit→http_hits`, `http.unmatched→http_unmatched`, `http.body_parse_skipped→http_body_parse_skipped`, `kafka.reacted→kafka_reactions`, `kafka.skipped→kafka_skipped`, `kafka.error→kafka_errors`. (Mirrors `MockEngine.emit_event`'s counter increments in `agctl/mock/engine.py`.)
  - `ParsedLog` — a dataclass with fields: `started: dict | None`, `startup_error: dict | None`, `summary: dict | None`, `summary_so_far: dict`, `failures: list[dict]`.
  - `parse_log(path) -> ParsedLog` — read every line of the file at `path`; for each non-blank line, `json.loads` it (skip silently if it does not parse). For each parsed object:
    - if it has an `"event"` key: `event=="started"` → set `started`; `event=="summary"` → set `summary` (last one wins); `event` in `ALL_FAILURE_EVENTS` → append the whole object to `failures`; and for every event, if `event` is in `EVENT_TO_COUNTER`, increment `summary_so_far[<field>]`.
    - else (no `"event"` key) and `obj.get("ok") is False` → set `startup_error` to `obj` (this is the daemon's startup-failure envelope — emitted by `mock run`'s hand-rolled path before any event line).
    - `summary_so_far` starts as `{field: 0 for field in EVENT_TO_COUNTER.values()}`.
    - Missing/unreadable file → return `ParsedLog(started=None, startup_error=None, summary=None, summary_so_far=<zeros>, failures=[])`.
  - `has_fatal_failure(parsed: ParsedLog) -> bool` — `True` iff any entry in `parsed.failures` has `event` in `FATAL_FAILURE_EVENTS`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_mock_daemon.py`:

1. **taxonomy constants:** `FATAL_FAILURE_EVENTS` contains exactly the four fatal names (not `capture.missing`); `ALL_FAILURE_EVENTS` equals the four plus `capture.missing`.
2. **`parse_log` happy path:** write an NDJSON file (via `tmp_path`) containing, in order: a `started` line `{"event":"started","http":{"listen":"0.0.0.0:18080","stubs":2},"kafka":null}`, an `http.hit` line, an `http.unmatched` line, a `kafka.error` line, a `capture.missing` line, and a `summary` line `{"event":"summary","http_hits":1,"http_unmatched":1,"http_body_parse_skipped":0,"kafka_reactions":0,"kafka_skipped":0,"kafka_errors":1,"duration_ms":500}`. Assert: `parsed.started == <the started line>`, `parsed.summary == <the summary line>`, `parsed.summary_so_far == {"http_hits":1,"http_unmatched":1,"http_body_parse_skipped":0,"kafka_reactions":0,"kafka_skipped":0,"kafka_errors":1}`, `parsed.failures` has length 3 (the unmatched, kafka.error, and capture.missing entries, in that order).
3. **`parse_log` startup-error path:** write a file with one line `{"ok":false,"command":"mock.run","error":{"type":"ConfigError","message":"bad"}}` (no `event`, no `started`/`summary`). Assert `parsed.startup_error == <that line>`, `parsed.started is None`, `parsed.summary is None`, `parsed.failures == []`.
4. **`parse_log` missing file:** `parse_log(<non-existent path>)` returns a `ParsedLog` with `started=None`, `summary=None`, `summary_so_far` all-zero, `failures=[]`.
5. **`has_fatal_failure`:** build a `ParsedLog` whose `failures` contains only a `capture.missing` entry → `False`; add an `http.unmatched` entry → `True`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_mock_daemon.py -v -k "parse_log or taxonomy or fatal"`
Expected: FAIL — `AttributeError: module 'agctl.mock.daemon' has no attribute 'parse_log'` (etc.).

- [ ] **Step 3: Write minimal implementation**

Append the constants, `ParsedLog`, `parse_log`, and `has_fatal_failure` to `agctl/mock/daemon.py` per the contracts above. `parse_log` is a single forward pass over the lines; it must not import anything from `agctl.mock.engine` (the event vocabulary is hard-coded here as the contract, so the parser stands alone).

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_mock_daemon.py -v`
Expected: PASS — all parser + helper tests green (Task 2 tests still green).

- [ ] **Step 5: Commit**

Run: `git add agctl/mock/daemon.py tests/unit/test_mock_daemon.py`
Run: `git commit -m "feat(mock): shared NDJSON log parser and failure-event taxonomy"`

---

## Task 4: `mock start` command + spawn seam + readiness poll

**Files:**
- Modify: `agctl/commands/mock_commands.py` (add `spawn_daemon`, `_mock_start_core`, `mock_start`)
- Modify: `agctl/cli.py` (register `mock_start` on `mock_group`)
- Test: `tests/unit/test_mock_lifecycle.py` (new)

**Interfaces:**
- Consumes:
  - Task 1: `python -m agctl` entry.
  - Task 2: `pidfile_path`, `log_path`, `write_pidfile`, `read_pidfile`, `remove_pidfile`, `is_alive`.
  - Task 3: `parse_log`.
  - Existing (from `mock_commands.py`): `_resolve_engines(only: str | None, mocks) -> tuple[bool, bool]` (raises `ConfigError` on `--only` misuse / missing section).
  - Existing (from `config/models.py`): `parse_listen(listen: str) -> tuple[str, int]` (raises `ValueError` on a bad `host:port`).
  - Existing (from `command.py`): `load_config_or_raise(config_path)`, `envelope("mock.start")`.
  - Existing (from `errors.py`): `ConfigError(message, detail)`.
- Produces:
  - `spawn_daemon(argv: list[str], log_path: str, env: dict | None = None) -> int` — module-level function in `mock_commands.py` (the test seam). Behavior: open the file at `log_path` for append; spawn a detached child running `python -m agctl` (interpreter = `sys.executable`, module form `-m agctl`) with the supplied `argv`, with the child's stdout **and** stderr both redirected to that log file (`stderr=subprocess.STDOUT`), started in a **new session/process-group** (`start_new_session=True`) so it survives the launching shell, inheriting the parent environment when `env is None` (else the supplied mapping). Return the child's pid. **Tests monkeypatch this** to return a fake pid and (optionally) write a canned log so the readiness poll resolves deterministically.
  - `_mock_start_core(config_path, http_listen, only, fail_fast, duration, state_dir) -> dict` — wrapped as `_mock_start_envelope = envelope("mock.start")(_mock_start_core)`. Returns `{pid, listen, log_path, stubs, reactors, started_at}`.
  - `mock_start` Click command: `@click.command("start")` with `--config`, `--http-listen`, `--only` (`click.Choice(["http","kafka"])`), `--fail-fast` (flag), `--duration` (float), `--state-dir` (default `"./.agctl"`); `@click.pass_context`; delegates to `_mock_start_envelope` like `check_ready` does.
  - `_START_BUDGET_SECONDS: float = 30.0` — module constant for the readiness-poll timeout.

**`_mock_start_core` behavior:**
1. `cfg = load_config_or_raise(config_path)`.
2. `run_http, run_kafka = _resolve_engines(only, cfg.mocks)` (reused unchanged — its `ConfigError`s propagate).
3. Resolve `http_listen`: if `--http-listen` given, `parse_listen` it (raise `ConfigError` on `ValueError`); elif `cfg.mocks` and `cfg.mocks.http`, use `cfg.mocks.http.listen`; else default `"0.0.0.0:18080"`. If `run_http`, parse the resolved listen to get `(host, port)`; require `port > 0` else `ConfigError("start requires a concrete --http-listen port (got 0)", {})`. Derive `port` for pidfile keying (set `port = None` when `not run_http` — kafka-only).
4. `state_dir` default `"./.agctl"`. Compute `pid = pidfile_path(state_dir, port)`, `logp = log_path(state_dir, port)`.
5. **Already-running pre-check:** read existing pidfile at `pid`; if it yields a dict and `is_alive(dict["pid"])` → `ConfigError(f"mock already running on {http_listen} (pid {dict['pid']}); run 'agctl mock stop' first or use a different --http-listen", {"pid": dict["pid"], "listen": http_listen})`.
6. Build the daemon `argv`: start with `["mock", "run"]`; if `config_path is not None` append `["--config", os.path.abspath(config_path)]` (if `None`, omit so the daemon auto-discovers in the same cwd); if `run_http` append `["--http-listen", http_listen]`; if `only` append `["--only", only]`; if `fail_fast` append `["--fail-fast"]`; if `duration is not None` append `["--duration", str(duration)]`.
7. `child_pid = spawn_daemon(argv, str(logp))`.
8. `write_pidfile(pid, {pid: child_pid, listen: http_listen if run_http else None, port, log_path: str(logp), config_path, started_at: <now ISO-8601 Z>, run_id: str(child_pid)})`.
9. **Readiness poll:** loop until monotonic elapsed exceeds `_START_BUDGET_SECONDS`:
   - `parsed = parse_log(logp)`
   - if `parsed.started` is not None → success; break with `started = parsed.started`.
   - elif `parsed.startup_error` is not None → cleanup (`os.kill(child_pid, SIGTERM)` best-effort ignoring errors; `remove_pidfile(pid)`) and raise `ConfigError(startup_error["error"]["message"], startup_error.get("error", {}).get("detail", {}) | {"listen": http_listen})`.
   - else `time.sleep(0.05)` and continue.
   - on budget exhaustion → cleanup as above, raise `ConfigError(f"mock daemon did not become ready within {_START_BUDGET_SECONDS}s", {"pid": child_pid, "log_path": str(logp)})`.
10. Build result: `stubs = started["http"]["stubs"] if started.get("http") else None`; `reactors = [r["name"] for r in started["kafka"]["reactors"]] if started.get("kafka") else []`. Return `{pid: child_pid, listen: http_listen if run_http else None, log_path: str(logp), stubs, reactors, started_at: <the written started_at>}`.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_mock_lifecycle.py`. Use a minimal `agctl.yaml` fixture (version `"2.0"`, `mocks.http.listen` set, one GET stub returning 200). Use `tmp_path` for both the config file and `--state-dir`. The spawn seam is monkeypatched to (a) return a fake pid and (b) write a canned `started` line to the log path it was given, so the readiness poll resolves without a real daemon. Write these scenarios:

1. **start success (HTTP):** patch `agctl.commands.mock_commands.spawn_daemon` with a fake that writes `{"event":"started","http":{"listen":"127.0.0.1:18080","stubs":1},"kafka":null}` to the given log path then returns `12345`. Invoke `cli` with `["--config", cfg, "mock", "start", "--only", "http", "--http-listen", "127.0.0.1:18080", "--state-dir", str(tmp_path)]`. Assert: `exit_code == 0`; output is a single JSON envelope with `ok:true`, `command=="mock.start"`, `result.pid == 12345`, `result.listen == "127.0.0.1:18080"`, `result.stubs == 1`, `result.log_path` ends with `mock-18080.log`; and a pidfile `mock-18080.pid` exists in `tmp_path` whose JSON `pid` is `12345`.
2. **start already-running:** pre-write `mock-18080.pid` in `tmp_path` with `pid = os.getpid()` (a live pid). Invoke start with the same listen. Assert `exit_code == 2`, `error.type == "ConfigError"`, message mentions "already running". (The fake spawn must not be called.)
3. **start startup-error:** patch `spawn_daemon` to write `{"ok":false,"command":"mock.run","error":{"type":"ConfigError","message":"bind failed","detail":{}}}` to the log (no started line) then return `12345`. Invoke start. Assert `exit_code == 2`, `error.type == "ConfigError"`, message contains "bind failed"; and the pidfile was removed (cleanup).
4. **start readiness timeout:** monkeypatch `agctl.commands.mock_commands._START_BUDGET_SECONDS = 0.2`; patch `spawn_daemon` to return `12345` and write nothing. Invoke start. Assert `exit_code == 2`, `error.type == "ConfigError"`, message mentions "did not become ready"; pidfile removed.
5. **start `--only http` without `mocks.http`:** config with no `mocks.http`. Invoke `mock start --only http`. Assert `exit_code == 2` (`ConfigError` from `_resolve_engines`, "no mocks.http configured").
6. **start forwards flags:** patch `spawn_daemon` to record the `argv` it received (and write a started line). Invoke `mock start --only http --http-listen 127.0.0.1:18080 --fail-fast --duration 5`. Assert the recorded argv contains `"mock","run"`, `"--only","http"`, `"--http-listen","127.0.0.1:18080"`, `"--fail-fast"`, `"--duration","5"`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_mock_lifecycle.py -v`
Expected: FAIL — `Error: No such command 'start'` (Click), or `AttributeError` on the missing command/seam.

- [ ] **Step 3: Write minimal implementation**

In `agctl/commands/mock_commands.py`: add imports (`os`, `signal`, `sys`, `time`, `subprocess`, `datetime`/`timezone`, and the needed names from `..mock.daemon`, plus `envelope`/`load_config_or_raise`/`ConfigError`/`parse_listen`). Add the `spawn_daemon` seam, the `_START_BUDGET_SECONDS` constant, `_mock_start_core` (behavior above), and the `mock_start` Click command (`_mock_start_envelope = envelope("mock.start")(_mock_start_core)`). In `agctl/cli.py`: add `from .commands.mock_commands import mock_start` (extend the existing import line) and `mock_group.add_command(mock_start)` next to the existing `mock_group.add_command(mock_run)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_mock_lifecycle.py -v`
Expected: PASS — all six start scenarios green.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/mock_commands.py agctl/cli.py tests/unit/test_mock_lifecycle.py`
Run: `git commit -m "feat(mock): add mock start — detached daemon with pidfile + readiness gate"`

---

## Task 5: `mock stop` command

**Files:**
- Modify: `agctl/commands/mock_commands.py` (add `_mock_stop_core`, `mock_stop`)
- Modify: `agctl/cli.py` (register `mock_stop`)
- Test: `tests/unit/test_mock_lifecycle.py` (append)

**Interfaces:**
- Consumes:
  - Task 2: `resolve_target`, `remove_pidfile`, `read_pidfile`.
  - Task 3: `parse_log`, `has_fatal_failure`.
  - Existing (from `errors.py`): `ConfigError(message, detail)` and `AssertionFailure(message, detail)` (exit 1, `type_name == "AssertionError"`).
  - Existing (from `command.py`): `envelope("mock.stop")`.
- Produces:
  - `_mock_stop_core(listen, pid, all_, timeout, state_dir) -> dict` — wrapped as `_mock_stop_envelope = envelope("mock.stop")(_mock_stop_core)`. Returns the verdict on a clean stop / not-running; **raises `AssertionFailure(message, detail=verdict)`** when any stopped mock had a fatal failure (the `@envelope` wrapper turns this into `ok:false`, `result:null`, `error.type=="AssertionError"`, exit 1).
  - `mock_stop` Click command: `@click.command("stop")` with `--listen`, `--pid` (int), `--all` (flag), `--timeout` (float, default `10.0`), `--state-dir` (default `"./.agctl"`); `@click.pass_context`; delegates to `_mock_stop_envelope`.

**`_mock_stop_core` behavior:**
1. `targets = resolve_target(state_dir, listen, pid, all_)`. If empty → return `{stopped: False}` when `not all_` else `{stopped: []}` (idempotent not-running; `ok:true`, exit 0).
2. For each `target` in `targets` (one, or many when `all_`):
   a. `sig = "SIGTERM"`. Best-effort `os.kill(target.pid, signal.SIGTERM)` (guard `ProcessLookupError` — already dead is fine).
   b. Wait loop: poll `is_alive(target.pid)` until False or monotonic elapsed ≥ `timeout` (sleep 0.05 between checks).
   c. If still alive: `os.kill(target.pid, signal.SIGKILL)` (guard `ProcessLookupError`); `sig = "SIGKILL"`; set `warning = f"process did not exit on SIGTERM within {timeout}s; sent SIGKILL; summary may be incomplete"`.
   d. `parsed = parse_log(target.log_path)`.
   e. `entry = {stopped: True, pid: target.pid, signal: sig, summary: parsed.summary or {}, failures: parsed.failures}`; if `warning` set, add `entry["warning"] = warning`.
   f. `remove_pidfile(target.pidfile_path)`.
3. Aggregate:
   - **Single target (`not all_`):** `verdict = entries[0]`. Let `fatal = [f for f in verdict["failures"] if f.get("event") in FATAL_FAILURE_EVENTS]`. If `fatal` is non-empty → `raise AssertionFailure(f"mock run had {len(fatal)} fatal failure event(s)", verdict)`. Else return `verdict`.
   - **`all_`:** let `bad = [e for e in entries if any(f.get("event") in FATAL_FAILURE_EVENTS for f in e["failures"])]`. If `bad` is non-empty → `raise AssertionFailure(f"{len(bad)} of {len(entries)} mock(s) had fatal failures", {"stopped": entries})` (note: `detail["stopped"]` is the array, per spec §6.2). Else return `{"stopped": entries}`.

(Note: `FATAL_FAILURE_EVENTS` is imported from `..mock.daemon` for the fatal check.)

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_mock_lifecycle.py`. **Stand-in daemon pattern:** for tests that need a "running" mock, spawn a real short-lived subprocess — `subprocess.Popen([sys.executable, "-c", "import time; signal stuff; time.sleep(30)"])` (a plain sleeper; for the SIGKILL test, a sleeper that installs `signal.signal(signal.SIGTERM, signal.SIG_IGN)` first). Write a pidfile into `tmp_path` whose `pid` is the sleeper's pid, `listen` set, `log_path` pointing at a pre-written NDJSON log file you control, plus `pidfile_path`. This exercises real signal delivery without a real agctl daemon or port. Scenarios:

1. **stop clean:** spawn a sleeper; write pidfile; pre-write a log with a `started` line and a clean `summary` (`http_unmatched:0`, `kafka_errors:0`, etc.), no failure events. Invoke `["mock", "stop", "--state-dir", str(tmp_path)]` (no selector — exactly one running). Assert `exit_code == 0`, envelope `ok:true`, `result.stopped == True`, `result.signal == "SIGTERM"`, `result.summary` equals the summary line, `result.failures == []`. Assert the sleeper process has terminated and the pidfile was removed.
2. **stop with fatal failures:** same setup, but pre-write the log with a `summary` plus an `http.unmatched` line and a `kafka.error` line. Assert `exit_code == 1`, `ok == False`, `result is None`, `error.type == "AssertionError"`, `error.detail.stopped == True`, `error.detail.failures` has 2 entries, `error.detail.summary` present.
3. **stop `capture.missing` is non-fatal:** log has a `capture.missing` line plus a clean summary, no fatal events. Assert `exit_code == 0`, `ok:true`, `result.failures` contains the `capture.missing` entry (for visibility), but no fatal.
4. **stop not-running:** empty `tmp_path` (no pidfiles), no selector. Assert `exit_code == 0`, `result.stopped == False`.
5. **stop ambiguous:** spawn two sleepers on two ports; write two pidfiles; no selector. Assert `exit_code == 2`, `error.type == "ConfigError"`, message mentions "multiple"; `error.detail.candidates` lists both listens.
6. **stop --all:** spawn two sleepers; write two pidfiles with clean-summary logs. Invoke `mock stop --all --state-dir <tmp>`. Assert `exit_code == 0`, `result.stopped` is a list of length 2; both pidfiles removed; both sleepers dead.
7. **stop SIGKILL fallback:** spawn a sleeper that ignores SIGTERM (`signal.signal(signal.SIGTERM, signal.SIG_IGN)`); write pidfile with a log containing only a `started` line (no `summary` — it never shuts down cleanly). Invoke `mock stop --timeout 1 --state-dir <tmp>`. Assert `exit_code == 0`, `result.signal == "SIGKILL"`, `result.warning` set and mentions SIGKILL, the sleeper is dead, pidfile removed.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_mock_lifecycle.py -v -k "stop"`
Expected: FAIL — `No such command 'stop'` (Click) or missing core.

- [ ] **Step 3: Write minimal implementation**

In `mock_commands.py`: add `_mock_stop_core` (behavior above) and the `mock_stop` Click command (`_mock_stop_envelope = envelope("mock.stop")(_mock_stop_core)`); import `AssertionFailure` and the needed daemon helpers. In `cli.py`: extend the `mock_commands` import to include `mock_stop` and add `mock_group.add_command(mock_stop)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_mock_lifecycle.py -v -k "stop"`
Expected: PASS — all seven stop scenarios green (start tests still green).

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/mock_commands.py agctl/cli.py tests/unit/test_mock_lifecycle.py`
Run: `git commit -m "feat(mock): add mock stop — SIGTERM/SIGKILL + parsed verdict, strict failure exit"`

---

## Task 6: `mock status` command

**Files:**
- Modify: `agctl/commands/mock_commands.py` (add `_mock_status_core`, `mock_status`)
- Modify: `agctl/cli.py` (register `mock_status`)
- Test: `tests/unit/test_mock_lifecycle.py` (append)

**Interfaces:**
- Consumes:
  - Task 2: `resolve_target`, `read_pidfile`.
  - Task 3: `parse_log`.
  - Existing (from `command.py`): `envelope("mock.status")`.
- Produces:
  - `_mock_status_core(listen, state_dir) -> dict` — wrapped as `_mock_status_envelope = envelope("mock.status")(_mock_status_core)`. Always returns a dict (never raises on the not-running case — `ok:true`, exit 0).
  - `mock_status` Click command: `@click.command("status")` with `--listen`, `--state-dir` (default `"./.agctl"`); `@click.pass_context`; delegates to `_mock_status_envelope`.

**`_mock_status_core` behavior:**
1. `targets = resolve_target(state_dir, listen, None, all_=False)`. If empty → return `{"running": False}`. (Ambiguous → `resolve_target` raises `ConfigError`; `--listen` non-match → `ConfigError` — both exit 2, correct.)
2. `target = targets[0]`. `parsed = parse_log(target.log_path)`.
3. Compute `uptime_ms`: parse `target.started_at` (ISO-8601 Z) to a UTC datetime, `int((now_utc - started).total_seconds() * 1000)`; if parsing fails, `uptime_ms = None`.
4. Return `{"running": True, "pid": target.pid, "listen": target.listen, "uptime_ms": uptime_ms, "summary_so_far": parsed.summary_so_far, "failures_so_far": parsed.failures}`.
5. `status` does **not** signal the daemon and does **not** remove the pidfile.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_mock_lifecycle.py` (use the same sleeper-stand-in pattern, or simply write a pidfile with the current `os.getpid()` since `status` never kills anything — either is fine):

1. **status running:** write a pidfile (pid = a spawned sleeper, or `os.getpid()`) into `tmp_path`, plus a log file containing a `started` line, one `http.hit`, and one `http.unmatched`. Invoke `mock status --state-dir <tmp>`. Assert `exit_code == 0`, `result.running == True`, `result.pid` matches, `result.listen` matches, `result.uptime_ms` is a non-negative int, `result.summary_so_far == {"http_hits":1,"http_unmatched":1,...}`, `result.failures_so_far` has the `http.unmatched` entry. Assert the daemon process (if a real sleeper) is still alive afterward and the pidfile still exists.
2. **status not-running:** empty `tmp_path`. Assert `exit_code == 0`, `result.running == False`.
3. **status --listen selects:** write two pidfiles for two listens; invoke `mock status --listen <one>`. Assert `result.listen` is the selected one. (No-selector with two → `ConfigError` exit 2, mirroring `resolve_target`.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_mock_lifecycle.py -v -k "status"`
Expected: FAIL — `No such command 'status'`.

- [ ] **Step 3: Write minimal implementation**

In `mock_commands.py`: add `_mock_status_core` (behavior above) and the `mock_status` Click command (`_mock_status_envelope = envelope("mock.status")(_mock_status_core)`); import `datetime`/`timezone` for the uptime parse. In `cli.py`: extend the import to include `mock_status` and add `mock_group.add_command(mock_status)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_mock_lifecycle.py -v -k "status"`
Expected: PASS — all three status scenarios green (start + stop tests still green).

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/mock_commands.py agctl/cli.py tests/unit/test_mock_lifecycle.py`
Run: `git commit -m "feat(mock): add mock status — live introspection via log tail (no signal)"`

---

## Task 7: Integration round-trip test

**Files:**
- Create: `tests/integration/test_mock_daemon.py`

**Interfaces:**
- Consumes: Tasks 4–6 (the three commands), Task 1 (`python -m agctl`), a real free local port, stdlib `urllib.request` for the HTTP hit (no extra dependency).

**Behavior under test (one cohesive round-trip, two parameterized variants):**
- Allocate a free port: bind a socket to `("127.0.0.1", 0)`, read `getsockname()[1]`, close the socket.
- Write a temp `agctl.yaml` (in `tmp_path`) with `version: "2.0"`, `mocks.http.listen: "127.0.0.1:<free>"`, one stub: `GET /ping` → `{status: 200, body: {ok: true}}`.
- Run `agctl mock start --config <yaml> --only http --state-dir <tmp>` via `CliRunner` (in-process is fine). Assert `exit_code == 0`, `result.pid` is an int, `result.listen == "127.0.0.1:<free>"`, `result.stubs == 1`, and a pidfile exists in `tmp_path`.
- **Variant A (clean):** `urllib.request.urlopen("http://127.0.0.1:<free>/ping")` → assert HTTP 200 and body contains `ok`.
- **Variant B (with failure):** same `ping` hit, then `urllib.request.urlopen("http://127.0.0.1:<free>/no-such-path")` and accept the 404 (this generates an `http.unmatched` event).
- Run `agctl mock status --state-dir <tmp>`. Assert `exit_code == 0`, `result.running == True`, `result.summary_so_far["http_hits"] >= 1`. In Variant B, `result.failures_so_far` includes an `http.unmatched` entry.
- Run `agctl mock stop --state-dir <tmp>`. **Variant A:** assert `exit_code == 0`, `ok:true`, `result.stopped == True`, `result.failures == []`. **Variant B:** assert `exit_code == 1`, `ok == False`, `error.type == "AssertionError"`, `error.detail.failures` contains an `http.unmatched` entry.
- Assert the pidfile was removed and the daemon process is gone after stop.

- [ ] **Step 1: Write the test**

Create `tests/integration/test_mock_daemon.py` with the two variants above (parameterized or two test functions). Use `CliRunner` for the three command invocations and stdlib `urllib.request` for the HTTP hit. Use `tmp_path` for both the config and `--state-dir`. Add a short final guard: after stop, assert the port is no longer accepting connections (a connection attempt raises) — confirms the daemon actually exited.

- [ ] **Step 2: Run the test**

Run: `pytest tests/integration/test_mock_daemon.py -v`
Expected: PASS (implementation from Tasks 4–6 is in place). If anything fails, the gap is in the integration surface (argv forwarding, real-subprocess readiness timing, log flushing) — fix the implementation, not the test, then re-run. The readiness poll uses `_START_BUDGET_SECONDS` (30s default) so a real daemon on a free port becomes ready well within budget.

- [ ] **Step 3: Commit**

Run: `git add tests/integration/test_mock_daemon.py`
Run: `git commit -m "test(mock): integration round-trip — start, hit, status, stop (clean + failure)"`

---

## Task 8: Full-suite green + doc sync

**Files:**
- Modify: `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, `skills/` (per spec §14)
- Per project `CLAUDE.md`: the doc sync is performed by the `docs-watcher` subagent.

- [ ] **Step 1: Run the full suite**

Run: `pytest tests/unit tests/integration -v`
Expected: all tests PASS — the new daemon/lifecycle tests plus every pre-existing test (no regressions; `mock run` is unchanged).

- [ ] **Step 2: Sync docs (spec §14)**

Invoke the `docs-watcher` subagent (per project `CLAUDE.md`) to apply the spec §14 edits and preserve each doc's altitude. The required changes:
- **`docs/DESIGN.md`** — §3.5: add `mock start` / `mock stop` / `mock status` references and the simplified lifecycle (the old four-step agent protocol becomes "use `mock start` … `mock stop`"); §8: record the bounded statelessness carve-out (pidfile + log under `.agctl/`); §10: move "Mock: managed daemon" from *Open Questions* into the released set (control-socket portion stays deferred); §14: note the divergence that the daemon introduces on-disk state.
- **`docs/ARCHITECTURE.md`** — §3: add `agctl/mock/daemon.py` and `agctl/__main__.py` to the module map, note `mock_commands.py` now hosts four commands; §6: record that `mock start`/`stop`/`status` are `@envelope` commands (NOT streaming exceptions) so the "two streaming exceptions" statement still reads correctly; §15: record the daemon state carve-out as a known, bounded deviation from statelessness.
- **`skills/` (consumer skills tree)** — add `mock start`/`stop`/`status` to the operational CLI reference an agent follows, including the readiness-gate and failure-surfacing semantics and the `error.detail` verdict placement for `stop`.

- [ ] **Step 3: Commit doc changes**

Run: `git add docs/ skills/`
Run: `git commit -m "docs: sync DESIGN/ARCHITECTURE/skills for mock start/stop/status"`

---

## Definition of Done

- `agctl mock start`, `mock stop`, `mock status` work end-to-end (Task 7 integration test green, both variants).
- `agctl mock run` is unchanged and its tests still pass.
- All unit + integration tests pass; no new dependencies.
- `python -m agctl` works.
- DESIGN.md, ARCHITECTURE.md, and the consumer `skills/` tree reflect the new commands and the statelessness carve-out.

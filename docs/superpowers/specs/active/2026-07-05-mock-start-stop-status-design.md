# Design: `agctl mock` — start / stop / status (managed daemon)

**Status:** Approved design — ready for implementation plan.
**Author:** brainstorming session (2026-07-05)
**Companion docs:** [`DESIGN.md`](../../../DESIGN.md) §3.5, §8, §10, §14 · [`ARCHITECTURE.md`](../../../ARCHITECTURE.md) §3, §6, §15

---

## 1. Background & Problem

`agctl mock run` (DESIGN §3.5) is a **foreground streaming process**: it emits NDJSON
events to stdout and blocks until `SIGTERM`/`SIGINT`. The documented way to use it from
an agent is a four-step protocol (DESIGN §3.5 "Agent failure-stream protocol"):

1. Redirect stdout to a log file: `agctl mock run > mock.log 2>&1 &`.
2. **Poll** `mock.log` for the `started` line before running the SUT.
3. Terminate with `SIGTERM` and `wait` — never `SIGKILL`.
4. After the test, **grep the log** for `http.unmatched` / `http.body_parse_skipped` /
   `kafka.skipped` / `kafka.error` / `capture.missing`, treating any hit as a failure.

This works, but it pushes lifecycle plumbing (PID tracking, log redirection, readiness
polling, failure grepping) onto every caller. The failure signals live **only** on
stdout (ARCHITECTURE §6), and the background `&` + `kill` pattern loses them by default.

DESIGN §10 already pre-sanctions the fix as deferred future work:

> **Mock: managed daemon** — `mock start/stop/status` with pidfile + control socket,
> behind `--detach`.

This design specifies that daemon lifecycle — the `start` / `stop` / `status` command
trio — collapsing the four-step protocol into a small set of one-envelope commands.

---

## 2. Goals

- **Start a mock without managing a PID or log redirect by hand.** `agctl mock start`
  runs the existing mock engine detached, redirects its NDJSON to a log file, and
  returns one JSON envelope once the mock is **ready** (the `started` line has appeared).
- **Stop a mock and get the verdict in one command.** `agctl mock stop` signals the
  daemon, waits for graceful shutdown, parses the log, and returns the `summary` plus
  the surfaced failure events — `ok:false` / exit 1 if any runtime failure occurred.
- **Live introspection without stopping.** `agctl mock status` reports whether the mock
  is alive and the running tally of hits/failures so far, by reading the live log.
- **No change to `mock run`.** The foreground streaming command keeps its current
  contract byte-for-byte.
- **Reuse `run`'s engine and event vocabulary.** The daemon is the `run` engine in a
  new session; `start`/`stop`/`status` are orchestration over its existing
  `started` / per-event / `summary` / SIGTERM-driven-exit semantics.

---

## 3. Scope & Design Constraints

- **One JSON object per invocation.** `start`/`stop`/`status` are normal `@envelope`
  commands (`agctl/command.py`), each emitting exactly one envelope and exiting 0/1/2.
  They are **not** streaming exceptions (unlike `mock run` and `http ping`).
- **Detached daemon = subprocess of `run`.** `start` spawns
  `agctl mock run <same flags>` in a new session (`start_new_session=True`) with
  stdout/stderr redirected to the log file, then writes the pidfile and polls the log
  for `started`. The parent `start` invocation returns; the child keeps running.
- **On-disk state is the pidfile + the log, nothing else.** Under `.agctl/` in the
  current working directory. No database, no lock files, no control socket in v1.
- **`MockEngine` (`agctl/mock/engine.py`) is reused unchanged.** Its existing
  behavior is the contract the new commands rely on: it emits `started` after
  probe+bind, emits one NDJSON line per runtime event, installs `SIGTERM`/`SIGINT`
  handlers that emit a final `summary` and exit `0` (clean) or `1` (runtime errors).
- **Addressable by HTTP port.** The pidfile is keyed by the listen port so multiple
  mocks can coexist; kafka-only mocks (no port) fall back to a fixed name and are
  addressed via `--pid`.

---

## 4. Non-Goals

- **No in-process daemonization** (double-`fork`/`setsid` inside one process). The
  subprocess approach is used instead — simpler, testable, and reuses `run` verbatim.
- **No control socket / runtime RPC.** `stop` and `status` communicate with the daemon
  solely via signal (`SIGTERM`) + log-file parsing. A control socket (DESIGN §10's
  "control socket") is deferred until a feature needs live runtime *control* (e.g.
  adding a stub at runtime), not just observation.
- **No change to `mock run` semantics, flags, or output.**
- **No stateful-mock / record-replay / TLS-mock features** (separate DESIGN §10 items).
- **No new failure event types.** The daemon emits the existing vocabulary
  (DESIGN §4.2 / ARCHITECTURE §6); `stop`/`status` only parse it.

---

## 5. Decisions (recorded with rationale)

**D1 — New `start` verb, not `run --detach`.**
`run` is a clean streaming command (NDJSON to stdout). Overloading it with a detach
mode would muddy that contract. A separate `start` verb gives clean orthogonality:
`run` = foreground streaming, `start` = detached daemon. (Rejects the roadmap's literal
"behind `--detach`" phrasing in favor of the verb it lists alongside it.)

**D2 — `start` spawns `run` as a detached subprocess.**
Alternatives rejected: in-process double-fork (Unix arcana, harder to test and to get
log redirection right); making `run` always write a pidfile (couples foreground
streaming with daemon state). The subprocess approach inherits `run`'s
`started`/`summary`/exit-code semantics for free, and the detach is one
`Popen(..., start_new_session=True)` with `stdout` → log fd.

**D3 — `stop` surfaces failures, using the *strict* protocol-level exit rule.**
`run`'s own exit-1 rule is narrow: exit 1 only when `kafka_errors > 0` (or fatal /
`--fail-fast`). The DESIGN §3.5 agent protocol is stricter — it tells agents to treat
*any* of the five failure events as a failure. `stop` adopts the **strict** rule (any
failure event → `ok:false`, exit 1), matching what agents already do by hand and what
the approved UX preview specified. Consequence: `stop`'s exit code can be **stricter
than `run`'s** for the same run. This is deliberate and more useful; the full event
list is always present in `failures[]` so the caller sees the whole picture.
`capture.missing` stays **non-fatal** (per DESIGN — included in `failures[]` for
visibility, does not drive exit 1).

**D4 — Pidfile + log under `.agctl/` in cwd, keyed by port.**
The agent is working in a repo; state colocated with the work is discoverable and
trivial to clean (`rm -rf .agctl`). It avoids `~/.cache` cross-repo collisions. Port
keying lets multiple mocks coexist. Configurable via `--state-dir`.

**D5 — Scoped exception to "stateless invocations" (DESIGN §8).**
DESIGN §8 says "no session files, no lock files." A daemon requires a pidfile. This is
a deliberate, bounded carve-out — exactly the one DESIGN §10 envisioned — limited to a
pidfile + a log under `.agctl/`, no other state. It must be recorded as a delta in
DESIGN §14 and ARCHITECTURE §15.

**D6 — `start` is itself the readiness gate.**
`start` blocks until the daemon's `started` line appears in the log (or a startup error,
or a timeout), then returns. The caller no longer polls for readiness — step 2 of the
old protocol is absorbed into `start`.

**D7 — Idempotent "not running" semantics.**
`stop` on a mock that is not running returns `ok:true`, `{stopped:false}`, exit 0 — a
no-op, not an error. `status` on a not-running mock returns `{running:false}`. Stale
pidfiles (dead pid) are detected via `os.kill(pid, 0)` and cleaned up.

---

## 6. Command Contracts

All three are registered on the existing `mock` group in `agctl/cli.py` (alongside
`mock_run`), implemented as `@envelope`-wrapped cores in `agctl/commands/mock_commands.py`
(following the `_core` split + `@envelope` pattern used by `db`/`kafka`/`http` commands).

### 6.1 `agctl mock start`

```
agctl mock start
    [--config <path>]            # auto-discovered by default
    [--http-listen <host:port>]  # literal (NO ${} interpolation); overrides mocks.http.listen
    [--only http|kafka]          # run a single engine
    [--fail-fast]                # forwarded to the daemon
    [--duration <seconds>]       # forwarded; daemon self-stops after N s
    [--state-dir <path>]         # default ./.agctl
```

`--until-stopped` is dropped (it is the daemon's default). The flag set is `run`'s minus
the streaming-lifecycle flags.

**Result shape (`mock.start`):**

```json
{
  "ok": true,
  "command": "mock.start",
  "result": {
    "pid": 12345,
    "listen": "0.0.0.0:18080",
    "log_path": "./.agctl/mock-18080.log",
    "stubs": 2,
    "reactors": ["order-command-handler"],
    "started_at": "2026-07-05T09:00:00Z"
  },
  "duration_ms": 312
}
```

### 6.2 `agctl mock stop`

```
agctl mock stop
    [--listen <host:port>]       # select when >1 running
    [--pid <pid>]                # explicit selector
    [--all]                      # stop every running mock in --state-dir
    [--timeout <seconds>]        # graceful-wait budget (default 10); SIGKILL after
    [--state-dir <path>]
```

Selector resolution: no-arg works when exactly one mock is running in `--state-dir`;
otherwise `--listen`, `--pid`, or `--all` is required (else `ConfigError` exit 2).

**`--all` result shape:** with `--all`, `result.stopped` is an **array** of per-mock
stop results (each shaped like the single-mock `result` above, minus the outer wrapper),
and the envelope's `ok` is `false` if **any** stopped mock had a fatal failure (exit 1).
With a single selector (`--listen`/`--pid`/no-arg), `result.stopped` stays the single
boolean shown in the example above.

**Result shape (`mock.stop`) — example with failures:**

```json
{
  "ok": false,
  "command": "mock.stop",
  "result": {
    "stopped": true,
    "pid": 12345,
    "signal": "SIGTERM",
    "summary": {
      "http_hits": 7,
      "http_unmatched": 2,
      "http_body_parse_skipped": 0,
      "kafka_reactions": 3,
      "kafka_skipped": 0,
      "kafka_errors": 1,
      "duration_ms": 45213
    },
    "failures": [
      {"event": "http.unmatched", "method": "GET", "path": "/x", "timestamp": "..."},
      {"event": "kafka.error", "reactor": "order-command-handler", "error": "...", "timestamp": "..."}
    ]
  },
  "duration_ms": 8
}
```

A clean stop (no failure events) returns `ok:true` and an empty `failures: []`, exit 0.

### 6.3 `agctl mock status`

```
agctl mock status
    [--listen <host:port>]
    [--state-dir <path>]
```

**Result shape (`mock.status`) — running:**

```json
{
  "ok": true,
  "command": "mock.status",
  "result": {
    "running": true,
    "pid": 12345,
    "listen": "0.0.0.0:18080",
    "uptime_ms": 12034,
    "summary_so_far": {
      "http_hits": 3, "http_unmatched": 0, "http_body_parse_skipped": 0,
      "kafka_reactions": 1, "kafka_skipped": 0, "kafka_errors": 0
    },
    "failures_so_far": []
  },
  "duration_ms": 2
}
```

`status` does **not** signal the daemon. Its `summary_so_far` is derived from the live
log (the running tally up to the last flushed line); since `run` only emits a `summary`
line at shutdown, the running tally is reconstructed by counting events seen so far.
A not-running mock returns `{running:false}` (and `ok:true`, exit 0).

---

## 7. On-disk State

Location: `<state-dir>/` (default `./.agctl/`). Two artifacts per running mock:

- **`mock-<port>.pid`** — JSON pidfile:
  ```json
  {"pid": 12345, "listen": "0.0.0.0:18080",
   "log_path": "./.agctl/mock-18080.log",
   "config_path": "./agctl.yaml",
   "started_at": "2026-07-05T09:00:00Z",
   "run_id": "<daemon run id>"}
  ```
  Keyed by HTTP **port**. Kafka-only mocks (no HTTP engine) use the fixed name
  `mock-kafka.pid` and are addressed via `--pid`.
- **`mock-<port>.log`** — the daemon's NDJSON stream (verbatim `run` stdout, including
  the `started` line, per-event lines, and the final `summary`).

**Liveness:** every command probes the pid via `os.kill(pid, 0)`. A dead pid is treated
as "not running": `start` reclaims the pidfile, `stop`/`status` report not-running and
remove the stale pidfile.

**Statelessness reconciliation (D5):** the pidfile + log are the daemon *service's*
state, not command-invocation state. The DESIGN §8 principle (command invocations are
self-contained, no cross-invocation state required to function) still holds for every
non-daemon command. This carve-out is bounded to `.agctl/` and recorded in §14.

---

## 8. Behavior & Semantics

### 8.1 `start` → ready

1. Load config and resolve engines + `http_listen` via the **same code path** as
   `mock run` (`_resolve_engines`, listen resolution). Misuse fails fast here, before
   any spawn, as a `ConfigError` envelope (exit 2).
2. Pre-checks: no live pidfile for the resolved port (else `ConfigError` "already
   running, pid N; stop it first or use `--listen`").
3. Open the log fd, spawn `agctl mock run <same flags>` detached in a new session
   with stdout/stderr → log fd.
4. Write the pidfile.
5. Poll the log for the `started` line within a startup budget. Outcomes:
   - `started` appears → emit `mock.start` success envelope, exit 0.
   - A startup-error envelope appears in the log (config/bind/probe failure) → read
     it, emit `ok:false` with the daemon's error, exit 2, clean up the pidfile.
   - Budget elapses with neither → emit `ok:false` (`InternalError` / timed-out
     waiting for readiness), exit 2, `SIGKILL` the orphaned child, clean up.

### 8.2 `stop` → verdict

1. Resolve the target (selector rules in §6.2).
2. Send `SIGTERM`. The daemon's existing handler emits the final `summary` line and
   exits `0` (clean) or `1` (`run`'s own rule).
3. Wait up to `--timeout` for the process to exit. If still alive → `SIGKILL`; report
   `signal:"SIGKILL"`, mark `summary` as incomplete (a `warning` field is set, since
   `SIGKILL` skips the shutdown path that emits `summary`).
4. Parse the log: take the **last** `summary` line; collect every event line whose
   `event` is in the failure set (§8.4) into `failures[]`, preserving order.
5. Apply the strict exit rule (D3): `ok:false` / exit 1 iff `failures[]` contains any
   **fatal** failure event (i.e. anything other than `capture.missing`); else
   `ok:true` / exit 0.

### 8.3 `status` → live snapshot

Read pidfile → liveness probe → parse the live log up to the last flushed line.
Reconstruct `summary_so_far` by counting each event type seen so far (the `summary`
line only exists at shutdown). Collect `failures_so_far` the same way `stop` does.
`status` never signals the daemon and never removes the pidfile.

### 8.4 Failure taxonomy

The failure event set (from DESIGN §4.2 / ARCHITECTURE §6) and their `stop`/`status`
handling:

| Event | Drives exit 1? | In `failures[]`? |
|---|---|---|
| `http.unmatched` | yes | yes |
| `http.body_parse_skipped` | yes | yes |
| `kafka.skipped` | yes | yes |
| `kafka.error` | yes | yes |
| `capture.missing` | **no** (non-fatal, per DESIGN) | yes (for visibility) |

This set is the single source of truth shared by `stop` and `status` (a constant in
the new daemon module), so the two commands never disagree about what counts as a
failure.

---

## 9. Error & Exit-Code Model

| Situation | Outcome |
|---|---|
| `start`: a live mock already owns the resolved port | `ConfigError`, exit 2 ("already running, pid N; stop it first or use `--listen`") |
| `start`: daemon dies before `started` (bind/probe/config error) | `start` reads the log, surfaces the daemon's structured error, exit 2; pidfile cleaned |
| `start`: readiness budget elapses | `ok:false`, exit 2; orphaned child `SIGKILL`ed; pidfile cleaned |
| `start`: flag misuse (`--only` bad value, etc.) | `ConfigError`, exit 2 (same as `run`) |
| `stop`: no running mock | `ok:true`, `{stopped:false}`, exit 0 (idempotent); stale pidfile cleaned |
| `stop`: ambiguous target (>1 running, no selector) | `ConfigError`, exit 2 (list candidates) |
| `stop`: daemon ignores `SIGTERM` past `--timeout` | `SIGKILL`; `signal:"SIGKILL"`, `warning` set, `summary` absent |
| `status`: no running mock | `ok:true`, `{running:false}`, exit 0; stale pidfile cleaned |
| `status`: ambiguous target | `ConfigError`, exit 2 (list candidates) |

Exit codes follow the existing model (DESIGN §4.1 / ARCHITECTURE §7): `0` success,
`1` (assertion-style) runtime failures detected by `stop`, `2` tool/config/env error.

---

## 10. Module Layout

New and changed files (implementation owned by the plan, not this spec):

- **`agctl/mock/daemon.py`** (new) — pure functions for daemon orchestration state:
  pidfile read/write/validate, liveness check, and the shared NDJSON log parser
  (`last summary line` + `failure-event tally`). No I/O coupling to the engine;
  testable directly with temp dirs.
- **`agctl/commands/mock_commands.py`** (extended) — add `mock_start`, `mock_stop`,
  `mock_status` Click commands and their `@envelope`-wrapped `_core` functions, plus
  an injectable daemon-spawn seam (mirroring the existing `new_mock_engine` test seam)
  so the orchestration is unit-testable without real subprocesses.
- **`agctl/cli.py`** (extended) — register the three new subcommands on `mock_group`.
- **`agctl/mock/engine.py`** — **unchanged.** `MockEngine`'s existing
  `started`/per-event/`summary`/SIGTEM-exit behavior is the contract `start`/`stop`
  rely on.

Dependency direction is preserved (inward-only, no cycles): `commands → {mock/daemon,
config, errors, command, output}`; `mock/daemon → {errors}` only.

---

## 11. Testing Strategy

- **Unit (default `pytest` suite, no network/Docker):**
  - `mock/daemon.py` pure functions — pidfile round-trip, stale-pidfile detection,
    log parsing (last `summary` extraction, failure-event tally across all five event
    types, `capture.missing` non-fatal handling, missing-`summary` edge).
  - `mock_start`/`mock_stop`/`mock_status` `_core` logic via the injectable spawn seam
    — covering: not-running idempotency, ambiguous-target `ConfigError`, already-running
    `ConfigError`, startup-error propagation, strict exit rule.
- **Integration (default suite — uses a free local port, no Docker):**
  - Full round-trip: `mock start --http-listen 127.0.0.1:<free>` → real daemon serves
    an HTTP stub from the sample config → `mock status` shows the hit → `mock stop`
    returns the verdict (clean → `ok:true`; an injected `http.unmatched` → `ok:false`
    exit 1). Uses the existing HTTP-server test seams; no live Kafka broker required
    for the HTTP-only path.
- **Self-skipping live suite** remains for any Kafka-reactor daemon test (real broker),
  following `tests/integration/conftest.py`'s `AGCTL_TEST_LIVE` pattern.

---

## 12. Deferred & Not-Covered

- **Control socket / runtime RPC** (add stub at runtime, live counter queries beyond
  log-tail). Deferred until a feature needs *control*, not just observation.
- **Multiple-mock ergonomics beyond port keying** (named mocks, `mock list`). The
  `--listen`/`--pid`/`--all` selectors cover v1; a `mock list` command can follow if
  multi-mock use grows.
- **`mock run --detach`** — explicitly rejected (D1); the verb form is preferred.
- **Windows service / non-Unix daemon model** — v1 assumes POSIX process groups
  (`start_new_session`, `SIGTERM`/`SIGKILL`). Windows support is out of scope.
- **Daemon log rotation / size limits** — the log grows for the life of the daemon;
  long-running daemons are not the primary use case (testing is). Rotation deferred.

---

## 13. Rejected Alternatives (ADR-style)

- **`run --detach` flag.** Overloads a clean streaming command with a second output
  model (one envelope vs. NDJSON), muddying `run`'s contract. Replaced by a dedicated
  `start` verb (D1).
- **`run` always writes a pidfile.** Couples foreground streaming with daemon state;
  `stop` only works if the caller backgrounded `run` with `&` anyway (still manual).
  Rejected in favor of `start` owning the daemon path entirely.
- **In-process double-fork daemon.** Unix arcana, hard to test, hard to redirect logs
  correctly. Replaced by the subprocess approach (D2), which reuses `run` verbatim.
- **`stop` returns ack-only (no log parsing).** Would preserve the documented pain
  point (agents must still grep the log for failures). Replaced by the
  failure-surfacing design (D3), which collapses the whole agent protocol.
- **Pidfile under `~/.cache/agctl/` (XDG).** Cross-repo collisions, less discoverable
  than colocated `.agctl/`. Rejected in favor of cwd-local state (D4).

---

## 14. Docs & Skill Impact

Implementation must sync (the `docs-watcher` subagent checks this; a correct no-op is
fine if the wording already fits):

- **DESIGN.md**
  - §3.5 — add `mock start` / `mock stop` / `mock status` command references and the
    simplified lifecycle (the old four-step protocol becomes "use `mock start` …
    `mock stop`").
  - §8 — record the bounded statelessness carve-out (D5): daemon pidfile + log under
    `.agctl/`.
  - §10 — move "Mock: managed daemon" from *Open Questions* into the released set
    (control-socket portion remains deferred).
  - §14 — note the divergence that the daemon introduces on-disk state.
- **ARCHITECTURE.md**
  - §3 — add `mock/daemon.py` to the module map; note `mock_commands.py` now hosts
    four commands.
  - §6 — record that `mock start/stop/status` are *not* streaming exceptions (they are
    `@envelope` commands), clarifying the "two streaming exceptions" statement.
  - §15 — record the daemon state carve-out as a known, bounded deviation from
    statelessness.
- **`skills/` (consumer skills tree)** — add `mock start/stop/status` to the
  operational CLI reference an agent follows, including the readiness-gate and
  failure-surfacing semantics.

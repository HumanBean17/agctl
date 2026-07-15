# Design: `agctl kafka listen` — long-lived capture daemon

**Status:** Approved design — ready for implementation plan
**Date:** 2026-07-15
**Precedent:** Mirrors the managed-daemon pattern of [`2026-07-05-mock-start-stop-status-design.md`](./archive/2026-07-05-mock-start-stop-status-design.md); reuses its lifecycle machinery.

---

## 1. Background & Problem

`agctl kafka assert` / `kafka consume` are **stateless, one-shot, windowed** scans
(ARCHITECTURE §8 `KafkaClient`; DESIGN §3.2 offset model). Each invocation seeks
every partition to `now − --lookback` (`--lookback` defaults to `--timeout`) and
polls forward until a wall-clock deadline or the first match. The scanned window is
roughly `[now − lookback, now + timeout]`.

This model is deliberately tuned for the **send-then-assert** pattern (produce →
assert moments later). It produces **false negatives** when an agent-driven runbook
verifies a Kafka message at the *end* of a long runbook on a high-volume topic, for
three compounding reasons:

1. **Window miss.** A message produced at runbook step 1, asserted at step N where
   `elapsed(step1 → stepN) > lookback`, sits *before* the seek point and is never
   read. Default lookback is small (= `--timeout`, often ~10s), so a multi-minute
   runbook guarantees the miss.
2. **Volume-induced timeout truncation.** Widening `--lookback` to cover the runbook
   makes the backlog in `[now − lookback, now]` huge; `find_in_window` polls in
   0.5s chunks single-threaded with no "keep up with the tail" guarantee, and
   returns `(None, scanned)` when the deadline expires before reaching the match —
   a false negative even though the message is in-window. Widening `--timeout` to
   compensate makes tests slow.

3. **Broker retention cleanup.** In long, open-ended debug sessions the session
   outlives the topic's retention: Kafka deletes the message before the agent
   asserts. The message is physically gone, so a windowed scan (which reads the
   live topic) fails permanently — the most insidious mode for debugging, and
   distinct from (1) and (2), where the message still exists but is unreachable.

Diagnosis confirmed with the user on two fronts: the failing topics are
**very-high-volume — the windowed scan cannot keep up (2)**, and long debug
sessions **outlive topic retention, so messages are deleted before assertion (3)**.
A stateless "assert only over the runbook window" fix addresses neither (2) nor
(3); the fix must **drain live and persist captures at arrival time**, decoupling
assertion from both the scan window and broker retention.

## 2. Goals

- Let an agent **register a Kafka listener at the start of a runbook and check
  results at the end**, so messages produced any time during the runbook are
  captured regardless of volume or trigger-to-assert gap.
- **Eliminate all three false-negative causes** for the listener path: no lookback
  window to fall outside of (seek-to-latest at start), no scan deadline to truncate
  against (assert reads a bounded on-disk file with no wall-clock budget), and no
  dependence on broker retention (captures persist to disk at arrival time,
  surviving the topic's retention cleanup).
- Support **multiple topics per listener** and **multiple attached assertions**,
  collected once at the end.
- Provide a **debug tap** ("filter and just get messages") over the captured set.
- **Reuse** the mock managed-daemon machinery (`spawn_daemon`, `_terminate`,
  `_require_posix_daemon`, pidfile/log helpers, `@envelope` lifecycle) rather than
  invent a parallel daemon model.
- Preserve agctl's output contract (one JSON envelope per non-streaming command;
  deterministic exit codes; NDJSON only for the foreground streaming target).

## 3. Scope & Design Constraints

- **Daemon = pure capture writer; all other subcommands read files.** No IPC to the
  daemon process. This is the mock model (`mock start` spawns a writer; `mock
  stop`/`status` read its NDJSON log + signal it). `listen assert`/`results`/
  `messages`/`status` are `@envelope`-wrapped commands operating on on-disk state.
- **Seek-to-latest-at-start** is the load-bearing property: the consumer is
  assigned and `seek_to_end`-ed on every partition *before* `start` reports ready.
  The runbook triggers *after* `start` returns, so the message is produced after the
  head position and is always captured; prior-run/stale messages are before the head
  and never read (this also kills the stale-event false-positive risk).
- **Capture-all to disk; cleanup at stop** (per approved decision). The on-disk
  NDJSON log *is* the buffer. Because capture is live from `start`, the log holds
  every message produced since `start`, with no window and no scan deadline.
- **Filter/evaluate at read time**, reusing `kafka_commands._build_assert_predicate`
  and the envelope-rooted jq semantics (`.value.x`, `.key`, `.headers.x`) — zero new
  predicate logic.
- **Second carve-out to the stateless invariant** (mock is the first; ARCHITECTURE
  §15). State is confined to the `listen` daemon lifecycle under `--state-dir`;
  nothing else reads/writes cross-invocation state.
- Same lazy-import convention (`confluent-kafka`/`jq` under the `kafka` extra); a
  missing library surfaces as `ConfigError` (exit 2), not a crash.

## 4. Non-Goals

- **No capture-time predicate required.** Capture-all is the default; a capture
  filter is an optional volume guardrail, not a prerequisite.
- **No new config section for v1.** Subscriptions are CLI-driven (`--topic` /
  `--pattern`); `kafka.patterns` is reused by name. A future `kafka.listeners`
  config section is explicitly deferred.
- **No Schema Registry / Avro/Protobuf decoding** (inherits the existing Kafka
  limitation; non-JSON values are captured raw and never match jq predicates).
- **No committed-offset persistence.** The consumer group is unique per run and
  ephemeral; offsets are not committed.
- **No multi-step scenario primitive** and **no cross-run replay** of captured logs
  (logs are deleted at `stop`).

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | Lifecycle is `start → assert (attach) → results (collect) → stop`, plus `messages` (debug), `status` (peek), `run` (foreground daemon target). | Matches the user's stated workflow and "register at start, check at end" mental model; `run` must exist as the daemon spawn target and is exposed (cross-platform foreground) exactly as `mock run` is. |
| D2 | `assert` **attaches** (writes a spec, returns ack, exit 0); `results` **collects** and exits 1 on any failure. | Keeps the exit-1-on-failure semantics on the collection step while honoring "attach asserts, then get results" as separate phases (valuable for eventually-consistent scenarios: attach early, collect after the message has had time to arrive). |
| D3 | Capture-all to disk; cleanup at `stop`. | User-approved. Removes ring-buffer eviction complexity; the file is the buffer; assert reads it with no deadline. |
| D4 | `--max-bytes-per-topic` safety valve **defaults ON** (generous cap), emitting a `capture.overflow` warning and stopping capture for that topic when exceeded. | Fail-loudly principle: on very-high-volume topics a runaway must not silently fill the disk. The cap is a guardrail, not a correctness limit. |
| D5 | `stop` does **not** auto-run `results`; it terminates and cleans up only. | Separation of concerns; `results` owns assertion. (Trade-off: an uncollected assertion is silently dropped — documented in the runbook protocol.) |
| D6 | `start`/`stop`/`status` gated to POSIX/WSL via the existing `_require_posix_daemon()`; `run` is cross-platform foreground (streaming NDJSON), giving native Windows a fallback path. | Faithful mirror of mock's platform split (ARCHITECTURE §15). Resolves the native-Windows question without deferring a usable path. |
| D7 | Listener keyed by a run-scoped `run_id` (not an HTTP port). | A Kafka listener has no listen address; the pidfile/state-dir key is the run id, selected via `--run-id`/`--pid`/implicit-singleton. |
| D8 | Extract truly-generic daemon primitives into a shared module used by both mock and listen. | Avoids `listen → mock` coupling (semantic smell) and is a targeted improvement that serves the work; respects the inward-only dependency direction. |

## 6. Command Contracts

All commands accept the global flags (`--config`, `--env-file`, `--overlay`,
`--timeout`) and the listener selector (`--run-id <id>` / `--pid <pid>` /
implicit-singleton when exactly one listener is running in `--state-dir`; mirrors
`resolve_target`). Envelope command tags are prefixed `kafka.listen.*`.

### 6.1 `agctl kafka listen start`

Spawn a detached capture daemon; block until ready (assigned + seeked-to-end);
return the start envelope.

```
agctl kafka listen start
    (--topic <name> | --pattern <name>)+   # repeatable; pattern reuses kafka.patterns (topic+match+cluster)
    [--cluster <name>]                      # overrides pattern-bound cluster (default: kafka.default_cluster / single-cluster)
    [--capture-match <jq>]                  # optional coarse capture filter (volume guardrail; default: capture all)
    [--max-bytes-per-topic <bytes>]         # safety valve; default ON at a generous cap (e.g. 256 MiB); 0 = unlimited
    [--state-dir <path>]                    # default ./.agctl
    [--config/--env-file/--overlay ...]
```

**Result (`kafka.listen.start`):**

```json
{
  "pid": 24001,
  "run_id": "9f3c…",
  "state_dir": "./.agctl/listen-9f3c",
  "topics": ["orders.created", "payments.events"],
  "group": "agctl-listen-9f3c",
  "cluster": "default",
  "started_at": "2026-07-15T09:00:00Z"
}
```

Exits 2 if a listener is already running for the resolved key, the broker is
unreachable at startup, or no/ambiguous cluster resolves.

### 6.2 `agctl kafka listen assert`

Attach an expectation to the listener's `asserts.jsonl`. Does **not** evaluate and
does **not** stop the listener. Callable repeatedly across topics.

```
agctl kafka listen assert
    --topic <name>
    [--contains '{...}'] [--match <jq>] [--pattern <name>]   # ≥1 mode; all active modes AND together (same composition as kafka assert)
    [--expect-count <n>]            # minimum matching count required (at-least; default 1), as kafka consume --expect-count
    [--path <jq>]                   # narrows --contains (as in kafka assert)
    [--param key=value]             # fills {placeholder} in --pattern match
    [--id <name>]                   # optional stable id; defaults to an incrementing ordinal
    [--run-id/--pid ...]
```

**Result (`kafka.listen.assert`):** `{attached: true, id: "<id>", topic, modes: [...], expect_count}`. Exit 0 / 2.

### 6.3 `agctl kafka listen results`

Evaluate every attached expectation against the captured files and return the
outcomes. **Exits 1 if any expectation fails.**

```
agctl kafka listen results
    [--run-id/--pid ...]
```

**Result (`kafka.listen.results`) — all pass:**

```json
{
  "evaluated": 2,
  "passed": 2,
  "failed": 0,
  "results": [
    {"id": "ord", "topic": "orders.created", "passed": true, "matched_count": 1, "expect_count": 1},
    {"id": "pay", "topic": "payments.events", "passed": true, "matched_count": 1, "expect_count": 1}
  ]
}
```

**Failure (`AssertionFailure`, exit 1):** `error.detail.results[]` carries, per
failed expectation, the same self-debugging payload as `kafka assert`
(`messages_scanned`, per-mode `root`, size-capped payload snapshot) so an agent
self-corrects a mis-rooted expression in one shot.

### 6.4 `agctl kafka listen messages`

Debug tap: dump captured messages for a topic, optionally further filtered/limited.
Reads the topic's capture file directly.

```
agctl kafka listen messages
    --topic <name>
    [--match <jq>] [--param key=value]      # further filter the captured set
    [--limit <n>]                            # default 50
    [--run-id/--pid ...]
```

**Result (`kafka.listen.messages`):** `{topic, matched, truncated, messages: [<envelope>…]}`.
Exit 0 / 2.

### 6.5 `agctl kafka listen status`

Live, read-only snapshot (never signals the daemon, never removes the pidfile).

```
agctl kafka listen status [--run-id/--pid ...] [--state-dir <path>]
```

**Result (`kafka.listen.status`):**

```json
{
  "running": true,
  "pid": 24001,
  "run_id": "9f3c…",
  "uptime_ms": 12034,
  "topics": [
    {"topic": "orders.created", "captured": 412, "bytes": 183402, "overflowed": false},
    {"topic": "payments.events", "captured": 7, "bytes": 2104, "overflowed": false}
  ]
}
```

A not-running listener returns `{running: false}` (`ok:true`, exit 0). Stale
pidfiles (dead pid) are detected and cleaned up automatically (as in mock).

### 6.6 `agctl kafka listen stop`

SIGTERM the daemon (SIGKILL after `--timeout`), parse the log for the final
summary, **delete the run state dir** (capture files + asserts + meta + pidfile).

```
agctl kafka listen stop [--run-id/--pid ...] [--all] [--timeout <seconds>] [--state-dir <path>]
```

**Result (`kafka.listen.stop`):** `{stopped: true, pid, signal, summary: {…}, cleaned: true}`.
Fatal capture errors found in the log (`kafka.error`, `capture.overflow` on a topic
that had an attached expectation) raise `AssertionFailure` (exit 1), mirroring
`mock stop`. With `--all`, one verdict per listener; any fatal → exit 1.

### 6.7 `agctl kafka listen run` (foreground daemon target; streaming)

The capture engine in the foreground, emitting NDJSON — the spawn target of
`start` and the cross-platform / native-Windows fallback. Parallels `mock run`.

```
agctl kafka listen run
    (--topic <name> | --pattern <name>)+
    [--cluster <name>] [--capture-match <jq>] [--max-bytes-per-topic <bytes>]
    [--duration <seconds>] [--until-stopped]    # mutually exclusive (until-stopped default)
    [--config/--env-file/--overlay ...]
```

**NDJSON stream:** `started`, `capture.message` (optional, off by default to avoid
doubling disk — controlled by `--emit-messages`), `capture.overflow`, `kafka.error`,
`summary`. Installs `SIGTERM`/`SIGINT` handlers that flush a final `summary` and
exit 0 (clean) / 1 (runtime errors). Startup errors emit one structured envelope
*before* any event line (mock pattern). **Not** wrapped by `@envelope` (streaming
exception, sixth after `http ping` / `mock run` / `logs tail` / `grpc` stream).

## 7. On-disk State

Under `--state-dir` (default `./.agctl`):

```
.agctl/
├── listen-<run_id>.pid                         # JSON: {pid, run_id, topics[], group, cluster, started_at, log_path, state_dir}
└── listen-<run_id>/
    ├── meta.json                               # {run_id, topics[], group, cluster, started_at, capture_match, max_bytes_per_topic}
    ├── asserts.jsonl                           # one attached-expectation spec per line (written by `assert`)
    ├── <topic>.ndjson                          # one captured message envelope per line (written by the daemon)
    └── events.log                              # NDJSON lifecycle/error events (the log `stop` parses for a verdict)
```

**Captured message line contract** (envelope-rooted, identical root to `kafka assert`
so predicates are reusable):

```json
{"topic":"orders.created","key":"ord-789","value":{"eventType":"ORDER_CREATED","orderId":"ord-789"},
 "partition":2,"offset":10043,"timestamp":"2026-07-15T09:00:01Z","headers":{"rqUID":"…"},"captured_at":"…"}
```

Non-JSON values are stored as `"value": "<raw string>"`; jq predicates against them
evaluate no-match (consistent with `kafka assert`).

**Attached-expectation line contract** (`asserts.jsonl`):

```json
{"id":"ord","topic":"orders.created","modes":{"pattern":"order-created"},"params":{"orderId":"ord-789"},"expect_count":1}
```

## 8. Behavior & Semantics

### 8.1 `start` → ready

1. Resolve the cluster (`--cluster` > pattern's `.cluster` > `kafka.default_cluster`
   > single-cluster auto-default); build one `KafkaClient` (via `new_kafka_client`).
2. Spawn the daemon (`spawn_daemon` → `python -m agctl kafka listen run …`),
   redirecting stdout to `events.log`; write the pidfile + `meta.json`.
3. The daemon: subscribes to the resolved topic set with a unique per-run group
   (`agctl-listen-<run_id>`), waits for assignment, `seek_to_end`s every partition,
   **then** emits `started` and begins the capture loop.
4. `start` polls `events.log` (as `mock start` does) until `started` appears (or a
   startup-error envelope, or the readiness budget expires → `ConfigError`).

### 8.2 Capture loop (daemon)

Reuses `KafkaClient.consume_loop`'s lifecycle (assignment → seek → `stop_event` →
close-in-`finally`) with a capture handle: each polled message is appended to
`<topic>.ndjson` (single writer — one consumer thread, no interleaving concern),
optionally gated by `--capture-match`. The per-topic byte counter triggers
`capture.overflow` + stops further writes for that topic at `--max-bytes-per-topic`.
`SIGTERM`/`SIGINT` flips the stop event; the loop closes the consumer in `finally`
and the run loop flushes a `summary`.

### 8.3 `assert` (attach) and `results` (collect)

`assert` appends one spec to `asserts.jsonl` and returns immediately (exit 0).
`results` reads `asserts.jsonl` and, per spec, scans the topic's capture file:
`find_in_window`-style first-match for `--contains`/`--match`/`--pattern`, count for
`--expect-count`. The scan has **no wall-clock deadline** — bounded only by file
size — so there is no timeout-truncation false negative. Predicates and roots are
reused verbatim from `kafka_commands` / `assertions`.

### 8.4 `stop` → verdict + cleanup

Resolve target(s) (`resolve_target` analog keyed by `run_id`); SIGTERM → wait →
SIGKILL on `--timeout`; parse `events.log` for `summary` + fatal events; **delete
the run dir**; remove the pidfile. Fatal events (`kafka.error`; `capture.overflow`
on a topic with an attached expectation) → `AssertionFailure` (exit 1).

### 8.5 Failure taxonomy

| Event | Severity | Surface |
|---|---|---|
| broker unreachable at start | fatal | `start` → `ConnectionFailure` (exit 2) |
| `kafka.error` during capture | fatal | `stop`/`status` verdict (exit 1 at `stop`) |
| `capture.overflow` on an asserted topic | fatal | `stop`/`status` verdict (exit 1 at `stop`) |
| `capture.overflow` on a non-asserted topic | warning | `status` only |
| uncollected expectations at `stop` | none (documented) | dropped silently with cleanup |

## 9. Error & Exit-Code Model

Inherits the standard table (ARCHITECTURE §7). Envelope tags: `kafka.listen.start`
/ `.assert` / `.results` / `.messages` / `.status` / `.stop`. `kafka.listen.run`
is the streaming exception (not `@envelope`-wrapped). Notable mappings:

- Bad invocation (no topics/patterns; ambiguous cluster; already-running for the
  key) → `ConfigError` (exit 2).
- Broker unreachable at start → `ConnectionFailure` (exit 2).
- `results` expectation failure → `AssertionFailure` (exit 1), self-debugging
  `error.detail.results[]`.
- `stop` fatal capture events → `AssertionFailure` (exit 1).

## 10. Module Layout

New / changed modules (dependency direction preserved: `cli → commands → {config,
clients, …}`; the new shared daemon module imports `{errors}` only):

```
agctl/
├── daemon.py                         # NEW (D8): generic primitives extracted from mock/daemon.py —
│                                     #   pidfile read/write/remove keyed by an arbitrary id, is_alive,
│                                     #   list_running(key glob)/resolve_target by id, parse_log pattern.
│                                     #   Imported by both commands/mock_commands.py and kafka_listen_commands.py.
├── commands/
│   ├── mock_commands.py              # CHANGED: re-import the generic primitives from agctl/daemon.py
│   │                                 #   (spawn_daemon/_terminate/_require_posix_daemon move here or to daemon.py).
│   └── kafka_listen_commands.py      # NEW: the seven _core fns + Click commands; @envelope-wrapped except run.
├── listen/                           # NEW subpackage (parallels mock/):
│   ├── capture.py                    # capture writer: consume_loop handle → append <topic>.ndjson; overflow valve.
│   ├── assert_eval.py                # file-scan evaluation; reuses kafka_commands._build_assert_predicate + assertions.
│   ├── capture_file.py               # read/filter/count per-topic NDJSON (pure functions; unit-testable with temp dirs).
│   └── daemon.py                     # listen-specific lifecycle: run_id keying, meta.json, RunningListener, verdict parse.
└── cli.py                            # CHANGED: register `listen` subgroup under `kafka` (produce/consume/assert/listen).
```

Reuse, not duplication: `spawn_daemon`, `_terminate`, `_require_posix_daemon`,
`is_alive`, and the pidfile/parse_log *patterns* come from the extracted
`agctl/daemon.py`; the Kafka consumer comes from `clients/kafka_client.py`
(`consume_loop`, `probe`); predicates from `assertions.py` / `kafka_commands.py`.

## 11. Testing Strategy

Mirrors the mock-daemon test architecture (ARCHITECTURE §12):

- **Unit (no network):**
  - `listen/capture_file.py` and `listen/daemon.py` pure helpers tested directly
    with temp dirs (write canned NDJSON, assert read/filter/count/cleanup) — exactly
    as `mock/daemon.py`'s helpers are tested.
  - `listen/capture.py` capture handle tested via `consumer_factory` fakes sharing
    the real `consume_loop` contract (reuse the kafka-client fake seam).
  - `listen/assert_eval.py` tested by writing canned capture files + attached specs,
    asserting pass/fail and self-debugging detail.
  - Lifecycle (`start`/`stop`/`status`) tested by monkeypatching `spawn_daemon` to
    return a fake pid + canned `events.log` lines (the mock-start test technique).
  - `cli._entry_points` and the new group registration monkeypatchable.
- **Integration (self-skipping):** `tests/integration/test_kafka_listen_commands.py`
  — end-to-end against a live Kafka+KRaft testcontainer under `AGCTL_TEST_LIVE=1`:
  start listener → produce → attach → results (pass and fail) → stop; verify
  seek-to-latest (no prior-run match) and cleanup. Skips when no broker.

## 12. Deferred & Not-Covered

- A `kafka.listeners` config section (named, reusable subscriptions) — v1 is
  CLI/pattern-driven.
- Capture-file index for sub-linear assert scans — MVP scans the file; fine because
  capture is bounded by runbook duration and asserts short-circuit on first match.
- Cross-run replay / persistence of captured logs beyond `stop`.
- A `--capture-match` that is required (currently optional; capture-all is default).
- Foreground `run` polish as a documented native-Windows fallback (it works today;
  formal doc/skill coverage is a follow-up).

## 13. Rejected Alternatives (ADR-style)

- **Stateless offset-bookmark then windowed assert over the runbook range.** Record
  the end offset at runbook start; assert only over `[bookmark, now]`. Rejected: the
  user's topics are very-high-volume — even the runbook-duration range can't be
  drained within a timeout, so the volume-induced truncation false negative
  remains. It also reads the live topic at assert time, so a message deleted by
  retention between bookmark and assert is gone — it does not survive failure mode
  (3). (Would be the right answer only for *bounded* volume where the gap, not
  throughput or retention, is the problem.)
- **Bounded in-memory ring buffer.** Capture into a size+age-capped ring; attached
  asserts refine against it. Rejected by the user in favor of capture-to-disk: on
  extreme volume the target message could evict from the ring before the assert is
  attached, reintroducing a miss. A size/age-capped ring also reproduces failure
  mode (3) in-process — a message captured at session start evicts long before a
  late-session assertion, the retention problem self-inflicted. Disk has no
  eviction.
- **Capture-to-log with agent-side grep (the raw `mock run` protocol).** Listener
  writes events; the agent asserts by grepping. Rejected: pushes assertion work and
  the self-debugging failure detail onto the agent, and loses the exit-1 discipline.
- **Generalize the mock Kafka reactor into a capture reactor.** Reuse `consume_loop`
  as a "capture reactor" exposing buffered bodies via `mock status`. Rejected:
  conflates *mocking the SUT's dependencies* with *observing the SUT's outputs* — a
  semantic mismatch — and the reactor stores only event counters today, not message
  bodies.

## 14. Docs & Skill Impact

After implementation, the `docs-watcher` subagent (per `CLAUDE.md`) syncs:

- **ARCHITECTURE.md:** add `kafka listen` to §3 module map (`commands/kafka_listen_commands.py`,
  `listen/`, extracted `agctl/daemon.py`); §4 request-lifecycle note; §6 sixth
  streaming exception (`kafka listen run`) and the managed-daemon `start/stop/status`;
  §8 client-layer note (capture reuses `consume_loop`); §9 assertion-engine note
  (assert_eval reuses predicates); §15 a **second** bounded-statelessness carve-out
  alongside mock, and removal of the implied "Kafka has no listener" limitation.
- **DESIGN.md:** §3.2 add the `kafka listen` command group (start/assert/results/
  messages/status/stop/run) and the offset/listener model; §1 note the listener as
  the long-saga verification primitive; §10 roadmap update.
- **skills/** (consumer skills): add `kafka listen` to the operational CLI/config
  reference an agent follows, including the runbook protocol (start before trigger →
  attach → results → stop) and the "stop does not auto-assert" caveat.

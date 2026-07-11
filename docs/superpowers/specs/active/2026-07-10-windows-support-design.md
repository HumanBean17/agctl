# Design: Native Windows & macOS Support (core parity; managed daemon WSL-only)

**Status:** Draft v1 — design approved in brainstorming session; pending spec review
**Date:** 2026-07-10
**Author:** brainstorming session
**Affects:** `agctl/commands/mock_commands.py` (daemon platform gate), `.github/workflows/test.yml` (OS matrix), `tests/unit/test_mock_lifecycle.py` (skip on Windows), several `/tmp`-path unit tests (convert to `tmp_path`), one new gate unit test, `docs/DESIGN.md` §3.6, `docs/ARCHITECTURE.md` §15, `README.md`, `skills/agctl*`. No new modules, no new dependencies, no new commands/flags/fields/exit codes (one new `ConfigError` message path only).
**Relation to docs:** Additive plus a recorded platform limitation. The `Operating System :: OS Independent` classifier (already in `pyproject.toml`) becomes honestly backed by CI. DESIGN §3.6 gains a *Platform support* note; ARCHITECTURE §15 gains the native-Windows daemon limitation and the streaming graceful-stop contract. Synced via `docs-watcher`. **No `version` bump** (additive under major `2`; no config-schema change).

---

## 1. Background & Problem

`agctl` declares `Operating System :: OS Independent` in `pyproject.toml`, yet its CI
(`.github/workflows/test.yml`) runs only `ubuntu-latest`, and the codebase was never exercised
on Windows or macOS. For an agent-facing CLI meant to run on a developer's actual machine, that
is an unbacked claim: an agent or human on native Windows has no assurance any command works, and
regressions specific to Windows/macOS are invisible.

A naive reading would block on three suspected POSIX-only surfaces:

1. **Managed-daemon process management** — `mock start`/`stop`/`status` use `os.kill(pid, SIGTERM/SIGKILL)`,
   `os.waitpid(pid, WNOHANG)`, `start_new_session=True` (`setsid`), and `is_alive()` via
   `os.kill(pid, 0)`. All are POSIX in semantics: on Windows, `os.kill(pid, 0)` *terminates* the
   process, `os.waitpid` only reaps the caller's own children, and `setsid` is a no-op.
2. **Streaming graceful shutdown** — `http ping`, `mock run`, `logs tail`, and `grpc` streaming
   install `SIGTERM`/`SIGINT` handlers to flush a final summary line. On Windows, a `SIGTERM`
   delivered via `os.kill`/`kill` hard-terminates (the handler never fires); only `SIGINT`/Ctrl+C
   reaches the handler.
3. **Heavy dependencies** — `jq`, `confluent-kafka`, `psycopg[binary]`, `grpcio` are native. A common
   belief is that `jq` (the `mwilliamson/jq.py` binding) has no Windows wheels, which would make
   `http call --match` / `kafka` / `db` / `grpc` uninstallable on Windows.

Surface #3 turned out to be **false** (verified against PyPI on 2026-07-10 — §11): all four ship
`win_amd64` wheels for CPython 3.11/3.12/3.13. Surface #2 is **already handled** for the realistic
case: the `SIGINT` path works on Windows unchanged. Only surface #1 is genuinely POSIX-bound, and
it is confined to the managed daemon — a convenience layer that is *not* on the critical path of an
agent's assertion chain (the agent backgrounds `mock run` directly).

The design therefore delivers **native Windows + macOS parity for every command an agent chains**,
gates only the managed daemon to POSIX, and backs the "OS Independent" claim with CI.

> **Scope honesty (load-bearing).** The managed daemon (`mock start`/`stop`/`status`) remains
> Linux/macOS/WSL-only. In WSL `os.name == "posix"`, so the daemon works there **unmodified** — no
> WSL detection is added. On native Windows the three daemon commands fail fast with an actionable
> `ConfigError`. Every other command — including `mock run` foreground and all jq-powered assertions
> — runs natively on Windows.

## 2. Goals

- Make every agent-facing command runnable **natively on Windows and macOS**: `http`, `db`, `kafka`,
  `logs`, `grpc`, `check`, `config`, `discover`, and `mock run` (foreground / backgrounded by the
  agent's shell) — including all jq-powered features (`--match`, `--jq-path`, kafka `--pattern`,
  db `--expect-value --path`, mock `match.jq`).
- **Back the `Operating System :: OS Independent` classifier with CI** by adding `windows-latest`
  and `macos-latest` to the unit-test matrix.
- Make the POSIX-only managed daemon **fail loudly and helpfully on native Windows** — an actionable
  `ConfigError` (exit 2) pointing at `mock run` or WSL, never an opaque `OSError`/`AttributeError`.
- **Preserve every existing behavior on Linux** byte-for-byte. No command, flag, field, exit code, or
  output shape changes anywhere; the only new runtime path is the Windows daemon gate.
- **No new dependencies, no new modules, no schema change.** The change is a localized gate + CI +
  test/docs work.

## 3. Scope & Design Constraints

- **Additive only, with one failure path added.** No command/flag/field/exit-code/output-shape change
  except the new `ConfigError` raised by the daemon gate on `os.name == "nt"`.
- **Gating is a single helper, at the entry of each daemon command.** One check, three call sites
  (`mock start`, `mock stop`, `mock status`). It runs *before* any pidfile read, `spawn_daemon`,
  `is_alive`, or `_terminate` work, so no POSIX process call is reached on Windows.
- **Streaming commands are not modified.** `http ping`, `mock run`, `logs tail`, and `grpc` streaming
  already register `SIGTERM`/`SIGINT` handlers from the main thread. On Windows those constants
  register cleanly and **Ctrl+C → `SIGINT` → handler → stop event → summary** already works.
- **Native Windows graceful stop of a *backgrounded* streaming command is Ctrl+C, not `kill`.**
  `SIGTERM` delivered via `os.kill`/`kill` cannot reach a handler on Windows (it hard-terminates);
  the SIGTERM-driven graceful-stop pattern is therefore POSIX/WSL. This is documented, not coded.
- **Detection is `os.name == "nt"`.** WSL is `"posix"` and is therefore *not* gated — the daemon runs
  there unchanged. No `platform`/registry sniffing; no false positives.
- **CI cross-platform coverage is the unit suite only.** Integration tests stay `ubuntu-latest` and
  opt-in (`AGCTL_TEST_LIVE`); Docker/testcontainers on Windows/macOS runners is slow and flaky, and
  the unit suite gives the real cross-platform signal.
- **All extras must install via prebuilt wheels on all three OSes.** Verified (§11); this is what
  makes core parity feasible rather than aspirational.

## 4. Non-Goals

- **A native Windows managed daemon.** Implementing `mock start`/`stop`/`status` on Windows would need
  job objects / named events / `taskkill` graceful semantics and a rethought SIGTERM contract. Explicitly
  declined in brainstorming; deferred (§13).
- **A pure-Python jq or a jq replacement.** Not needed — `jq` ships Windows wheels (§11). Replacing the
  jq engine would be a far larger change touching the dialect, migration tooling, and every `match` site.
- **A unified signal-helper refactor** (extracting the SIGTERM/SIGINT install/restore duplicated across
  four modules into one helper). Considered as "Approach B" and deferred (§12, §13) — it is not required
  for Windows support and widens the blast radius.
- **macOS-specific work beyond running the unit matrix.** macOS is already POSIX and already worked
  unverified; this change verifies it, it does not add macOS features.
- **Changing any existing Linux behavior**, including the daemon's signal/exit semantics on POSIX.
- **A Windows installer / `.exe` distribution / code signing.** `pip install` is the delivery path.

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Gate the managed daemon to POSIX rather than port it.** `mock start`/`stop`/`status` raise `ConfigError` (exit 2) on `os.name == "nt"`; everything else runs natively on Windows. | The daemon is the *only* genuinely POSIX-bound surface, and it is not on the critical path — an agent backgrounds `mock run` directly. A full Windows port (D-rejected, §12) costs far more than its value and adds a fragile surface. User-chosen scope ("core parity, daemon WSL-only"). |
| D2 | **Detection is `os.name == "nt"`.** | Native Windows is the only platform to gate. WSL reports `"posix"` and therefore runs the daemon unmodified — no WSL sniffing, no false positives, no `sys.platform`/registry heuristics. |
| D3 | **The gate raises `ConfigError` (exit 2) with an actionable `detail`** (`platform`, `hint`) pointing at `mock run` or WSL. | Consistent with the fail-loud model (ARCHITECTURE §7) and the self-debugging failure contract (§9): an agent reads `error.detail.hint` and self-corrects to `mock run` or WSL in one step. `ConfigError`/exit 2 matches "bad invocation" semantics exactly. |
| D4 | **Streaming commands (`http ping`, `mock run`, `logs tail`, `grpc` streaming) are not modified.** | Their `SIGINT`/Ctrl+C graceful-stop path already works on Windows; `SIGTERM` registration does not crash (the constant exists). Hardening beyond Ctrl+C would require the D-rejected Windows process primitives. The POSIX/WSL SIGTERM graceful-stop pattern is documented instead. |
| D5 | **CI cross-platform matrix covers the unit suite; integration stays `ubuntu-latest` + opt-in.** | The unit suite is network-free and gives the real cross-platform signal. Integration needs Docker/testcontainers, which is slow/flaky on Windows/macOS runners and provides little beyond what ubuntu integration already covers. |
| D6 | **`jq` is not replaced.** All heavy extras install via prebuilt Windows wheels (§11). | Reverses the common "jq has no Windows wheels" belief with verified PyPI data (jq ≥ 1.9.1 ships `win_amd64` cp311/312/313). Replacing jq would be a sprawling, out-of-scope change with no benefit. |
| D7 | **`/tmp`-using unit tests that touch the filesystem are converted to the `tmp_path` fixture.** String-only config-value usages are left as-is. | `/tmp` does not exist on Windows; tests that actually open files there fail on Windows. `tmp_path` is the cross-platform pytest idiom. String-only usages are inert. |
| D8 | **The daemon lifecycle unit suite is skipped on Windows** (module-level `skipif(os.name == "nt")`). | `tests/unit/test_mock_lifecycle.py` exercises `os.kill`/`os.waitpid`/real signals/spawned sleepers — the POSIX surface the daemon is gated away from. Skipping matches the WSL-only scope honestly; faking these on Windows would test nothing real. |
| D9 | **One new unit test verifies the gate** by exercising the platform seam (monkeypatched to `"nt"`) and asserting each daemon command raises `ConfigError` with the Windows hint — without needing a real Windows run. | The gate is the entire behavior change on Windows; it deserves direct coverage. The seam-based test runs green on every OS, so ubuntu CI proves the gate works before any Windows runner does. |
| D10 | **No `version` bump, no config-schema change.** | The change adds no config field and changes no contract visible to `agctl.yaml`. The Windows `ConfigError` is an invocation-time failure, not a schema event. |

## 6. Platform Behavior Contract

The supported/unsupported matrix per platform. "✅ native" = supported and CI-tested; "⚠ Ctrl+C" =
supported via Ctrl+C/SIGINT graceful stop (SIGTERM graceful stop is POSIX/WSL only); "🚫 exit 2" =
raises the actionable `ConfigError`.

| Command group | Linux | macOS | Native Windows | WSL |
|---|---|---|---|---|
| `http`, `check` | ✅ | ✅ | ✅ | ✅ |
| `db`, `kafka`, `logs`, `grpc` | ✅ | ✅ | ✅ (incl. jq features) | ✅ |
| `config`, `discover` | ✅ | ✅ | ✅ | ✅ |
| `mock run` (foreground) | ✅ | ✅ | ⚠ Ctrl+C stop | ✅ |
| `mock start` / `stop` / `status` (daemon) | ✅ | ✅ | 🚫 exit 2 → hint | ✅ |

The single new behavior on Windows is the last row; every other cell is pre-existing behavior, now
verified rather than assumed.

## 7. Component Changes

- **`agctl/commands/mock_commands.py` — daemon platform gate.** A single guard helper
  (`_require_posix_daemon()` by convention) is called at the entry of each daemon command's core
  logic: `_mock_start_core` and the corresponding entries for `mock stop` / `mock status`, *before*
  any pidfile/`spawn_daemon`/`is_alive`/`_terminate` work. It raises `ConfigError` when the platform
  is native Windows. **Testability requirement (design constraint, not code):** the gate must consult
  the platform through a module-level indirection (e.g. read `os.name` at *call* time via the module's
  own `os` reference, not a constant captured at import) so a unit test can force the Windows branch
  without mutating global `os` state or running on Windows. The helper is the **only** production code
  change.
- **`agctl/mock/daemon.py` — no change.** `is_alive` (`os.kill(pid, 0)`) and the parser helpers are
  reached only via the gated commands; they never execute on Windows.
- **Streaming commands — no change.** `agctl/commands/http_commands.py` (ping),
  `agctl/mock/engine.py` (run), `agctl/commands/logs_commands.py` (tail), and
  `agctl/commands/grpc_commands.py` (server-stream/bidi) keep their existing `SIGTERM`/`SIGINT`
  install/restore. `signal.signal(SIGTERM, …)` registers cleanly on Windows; Ctrl+C/SIGINT already
  drives graceful shutdown.
- **CLI bootstrap, config, clients — no change.** `_ensure_utf8_streams` (`agctl/cli.py`), the
  walk-up config discovery, `importlib.resources`, `ThreadingHTTPServer`, pathlib paths, and the
  `0.0.0.0` default bind are already cross-platform.
- **No new modules, no new dependencies, no new entry points, no new `Protocol`.**

## 8. CI Changes (`.github/workflows/test.yml`)

The job matrix expands to three OSes × three Pythons; the integration suite stays ubuntu-only.
Structure (config/contract):

```yaml
jobs:
  test:
    strategy:
      fail-fast: false
      matrix:
        os: [ubuntu-latest, windows-latest, macos-latest]
        python-version: ['3.11', '3.12', '3.13']
    runs-on: ${{ matrix.os }}
    steps:
      # checkout + setup-python (cache: pip) as today
      - name: Install agctl with all client extras and test deps
        run: pip install -e ".[dev,http,jq,kafka,db,logs,grpc]"
      - name: Run unit tests
        run: pytest tests/unit
```

Notes:
- `runs-on` becomes `${{ matrix.os }}`.
- The install command is unchanged — every extra resolves a prebuilt wheel on all three OSes (§11).
- The run command becomes `pytest tests/unit` (explicit). On ubuntu this is behaviorally identical to
  today (integration self-skips without `AGCTL_TEST_LIVE`, which CI never sets).
- `paths-ignore`, `concurrency`, and `workflow_dispatch` are unchanged.
- Integration remains an opt-in, ubuntu-only concern (out of this workflow's scope, same as today).

## 9. Error & Exit-Code Model

The gate adds exactly one new failure path — the daemon invoked on native Windows. It reuses the
existing `ConfigError` (`type_name` `"ConfigError"`, exit `2`) raised from inside an `@envelope`-wrapped
core, so it emits one structured envelope and exits 2, identical to every other bad invocation.

Envelope (contract):

```json
{
  "ok": false,
  "command": "mock.start",
  "error": {
    "type": "ConfigError",
    "message": "the managed mock daemon (mock start/stop/status) is supported on Linux, macOS, and WSL; on native Windows use 'agctl mock run' or run inside WSL",
    "detail": {
      "platform": "win32",
      "hint": "use 'agctl mock run' (foreground) or run agctl inside WSL for the managed daemon"
    }
  },
  "duration_ms": 0
}
```

`command` is `mock.start` / `mock.stop` / `mock.status` per invocation. `detail.platform` is
`sys.platform` at runtime. `detail.hint` is the actionable correction an agent reads to self-recover.
No new error subclass, no new exit code.

## 10. Testing Strategy

- **Gate unit test (new).** One test per daemon command (`start`/`stop`/`status`) that monkeypatches the
  platform seam to `"nt"` and asserts `ConfigError` is raised with `detail.hint` populated. Runs green
  on every OS, so ubuntu CI proves the gate.
- **Daemon lifecycle suite — skip on Windows.** `tests/unit/test_mock_lifecycle.py` gains a module-level
  `pytestmark = pytest.mark.skipif(os.name == "nt", reason="managed daemon is POSIX-only")`. The file's
  `os.kill`/`os.waitpid`/real-signal/spawned-sleeper tests are exactly the POSIX surface gated away.
- **`/tmp`-path tests — convert the filesystem-touching ones to `tmp_path`.** Audit `tests/unit/` for
  literal `/tmp/...`. Only tests that **open/read** such a path (notably `tests/unit/test_logs_client.py`)
  need conversion; string-only config-value usages (`test_overlay.py`, `test_models.py`,
  `test_resolver.py`) pass unchanged.
- **CI matrix** (§8) runs `pytest tests/unit` on ubuntu/windows/macOS × 3.11/3.12/3.13. A green Windows
  run is the acceptance signal for core parity.
- **No new integration tests.** Integration stays ubuntu/`AGCTL_TEST_LIVE`-gated and is out of scope.

## 11. Dependency Verification (load-bearing evidence)

Verified against PyPI release JSON on 2026-07-10. Each extra's heavy native dep has a prebuilt
`win_amd64` wheel for CPython 3.11/3.12/3.13 — so `pip install -e ".[http,jq,kafka,db,logs,grpc]"`
succeeds on Windows and macOS without a compiler:

| Dependency | Latest with `win_amd64` cp311/312/313 wheel | Extra |
|---|---|---|
| `jq` | 1.12.0 (present since ≥ 1.9.1) | `jq`, `kafka`, `db`, `logs`, `grpc` |
| `psycopg-binary` | 3.3.4 | `db` |
| `grpcio` | 1.82.1 | `grpc` |
| `confluent-kafka` | 2.15.0 | `kafka` |
| `httpx` | 0.28.1 (`py3-none-any`, pure-Python) | `http` |

This reverses the oft-repeated claim that `jq` lacks Windows wheels. Because agctl pins `jq>=1.6`,
pip on Windows resolves to a wheeled release (1.12.0) automatically. **jq-powered features therefore
work natively on Windows**; no degradation, no env-marker exclusion, no replacement is needed.

## 12. Rejected Alternatives (ADR-style)

- **Full native managed daemon on Windows** (job objects for child tracking, named events / a sentinel
  for graceful stop, `TerminateProcess`/`taskkill` for force, rethought SIGTERM contract). Rejected in
  brainstorming as the explicit scope choice: high cost, fragile surface, low value (the daemon is a
  convenience layer; `mock run` covers the foreground case). This is §13 future work.
- **Replace `jq` with a pure-Python evaluator** (JSONPath/JMESPath, or a hand-rolled jq subset).
  Rejected: unnecessary once Windows wheels were confirmed (§11), and a jq replacement would
  destabilize the dialect, `config migrate` prefix rules, and every `match` site for zero gain.
- **Signal-helper refactor ("Approach B")** — extract the SIGTERM/SIGINT install/restore duplicated in
  `mock/engine.py`, `grpc_commands.py`, `logs_commands.py`, `http_commands.py` into one Windows-aware
  `agctl/signals.py`. Rejected for this round: not required for Windows support (the `SIGINT` path
  already works), and it widens the blast radius across four modules. Recorded as a possible follow-up.
- **Run the integration suite on Windows/macOS CI.** Rejected: Docker/testcontainers on non-Linux
  runners is slow and flaky and adds little beyond ubuntu integration. Unit coverage is the
  cross-platform signal that matters.
- **"Windows support = WSL only" (docs + a CI smoke, no native work).** Rejected: the user wants real
  native-Windows value for the assertion commands; this spec delivers it.
- **Env-marker-exclude `jq` on Windows** (`jq>=1.6; sys_platform != 'win32'`). Mooted by §11 — jq
  installs cleanly on Windows, so degrading jq features there is unnecessary.

## 13. Deferred & Not-Covered

- **Native Windows managed daemon** — `mock start`/`stop`/`status` graceful lifecycle via Windows
  primitives. Future work; would revisit D1/D4 if native-Windows daemon demand materializes.
- **Approach B signal consolidation** — a single Windows-aware signal helper de-duplicating the four
  streaming-command install/restore sites. Optional follow-up refactor (behavior-preserving).
- **Windows/macOS integration in CI** — deferred until testcontainers-on-Windows stabilizes or an
  alternative live-service strategy exists.
- **`.exe` / MSI distribution, code signing, Store packaging** — `pip install` remains the path.
- **Sandboxing/process-isolation hardening** of `mock run` on Windows beyond Ctrl+C graceful stop.

## 14. Docs & Skill Impact

- **`docs/DESIGN.md` §3.6 (`agctl mock`)** — add a *Platform support* note: the managed daemon
  (`start`/`stop`/`status`) is Linux/macOS/WSL; `mock run` and all other command groups are
  cross-platform including native Windows. Note the Ctrl+C-vs-SIGTERM graceful-stop distinction.
- **`docs/ARCHITECTURE.md` §15 (Known Limitations)** — record the native-Windows daemon limitation,
  the streaming graceful-stop contract (Ctrl+C on Windows; SIGTERM on POSIX/WSL), and that
  `os.name == "nt"` gates the daemon (WSL passes through as `"posix"`).
- **`README.md`** — a short *Requirements / Platforms* line (Python ≥ 3.11; Linux/macOS/Windows/WSL;
  daemon is POSIX/WSL).
- **`skills/agctl*`** — light touch where a reference implies the daemon is universal (point at
  `mock run` / WSL on Windows). The consumer skill that documents `mock start`/`stop` is the main one.
- **`pyproject.toml`** — no edit; `Operating System :: OS Independent` is now honestly backed by CI.

# Native Windows & macOS Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every agent-facing `agctl` command run natively on Windows and macOS (incl. all jq-powered features), gate only the managed mock daemon (`mock start`/`stop`/`status`) to POSIX, and back the `Operating System :: OS Independent` classifier with a Windows+macOS CI matrix.

**Architecture:** One localized production change — a platform gate at the entry of the three daemon `_core` functions that raises an actionable `ConfigError` (exit 2) on `os.name == "nt"`. Every other command already runs cross-platform (all heavy deps ship `win_amd64` wheels, verified against PyPI; streaming commands' Ctrl+C/SIGINT graceful-stop already works on Windows). Plus a CI matrix expansion, a test skip for the POSIX-only daemon lifecycle suite, and docs. No new modules, no new deps, no schema change.

**Tech Stack:** Python ≥ 3.11, Click, pytest, GitHub Actions matrix (`ubuntu/windows/macOS × 3.11/3.12/3.13`).

## Global Constraints

(Copied verbatim from the approved spec `docs/superpowers/specs/active/2026-07-10-windows-support-design.md`; every task's requirements implicitly include these.)

- **Additive only.** No command/flag/field/exit-code/output-shape change except the single new `ConfigError` raised by the daemon gate on Windows.
- **No `version` bump; no config-schema change.** No new dependencies, no new modules, no new entry points, no new `Protocol`.
- **Detection is `os.name == "nt"`.** WSL reports `"posix"` and must pass through ungated (the daemon runs there unmodified). No `platform`/registry sniffing; no `sys.platform` heuristics for gating.
- **The gate raises `ConfigError` (exit 2)** — `type_name` `"ConfigError"`, the existing class from `agctl/errors.py`. Exact message (copy verbatim): `the managed mock daemon (mock start/stop/status) is supported on Linux, macOS, and WSL; on native Windows use 'agctl mock run' or run inside WSL`. Exact `detail` shape: `{"platform": <sys.platform at runtime>, "hint": "use 'agctl mock run' (foreground) or run agctl inside WSL for the managed daemon"}`.
- **Streaming commands must NOT be modified:** `agctl/commands/http_commands.py` (ping), `agctl/mock/engine.py` (run), `agctl/commands/logs_commands.py` (tail), `agctl/commands/grpc_commands.py` (server-stream/bidi). Their existing `SIGTERM`/`SIGINT` install/restore is left as-is.
- **Testability requirement (design constraint):** the gate must consult the platform through the module-level `os` binding in `agctl/commands/mock_commands.py` (read `os.name` at call time), so a unit test can force the Windows branch by replacing that module's `os` reference — without mutating the global `os` module and without running on Windows.
- **CI matrix:** `os: [ubuntu-latest, windows-latest, macos-latest]` × `python-version: ['3.11','3.12','3.13']`, `fail-fast: false`; the run command is `pytest tests/unit`; integration stays ubuntu-only and opt-in (`AGCTL_TEST_LIVE`, which CI never sets).
- **Conventional-commit messages** matching repo history (`feat:`, `test:`, `ci:`, `docs:`). Work happens on branch `feat/windows-support`.

---

## File Structure

- **`agctl/commands/mock_commands.py`** (modify) — add the platform gate; the only production code change.
- **`tests/unit/test_mock_commands.py`** (modify) — add the gate unit tests (runs on all OSes).
- **`tests/unit/test_mock_lifecycle.py`** (modify) — add a module-level skip on Windows.
- **`.github/workflows/test.yml`** (modify) — expand the OS matrix; run `pytest tests/unit`.
- **`docs/DESIGN.md`** (modify) — §3.6 Platform-support note.
- **`docs/ARCHITECTURE.md`** (modify) — §15 Known-Limitations entry.
- **`README.md`** (modify) — Requirements/Platforms line.
- **`skills/agctl-config/reference/mocks.md`** (modify) — note that the managed daemon is POSIX/WSL on Windows.

No new files in the `agctl/` package.

---

## Task 1: Daemon platform gate

Gates `mock start`/`stop`/`status` to POSIX. The whole production change for this feature.

**Files:**
- Modify: `agctl/commands/mock_commands.py` — add a module-level helper; call it as the first statement of three existing `_core` functions.
- Test: `tests/unit/test_mock_commands.py` — add gate tests.

**Existing symbols this task consumes (already present, do not change):**
- `agctl/commands/mock_commands.py` line 11 `import os`; line 14 `import sys`; line 24 imports `ConfigError` from `..errors`.
- `_mock_start_core(config_path, http_listen, only, fail_fast, duration, state_dir, overlay_paths=None)` at line 215 (wrapped by `_mock_start_envelope = envelope("mock.start")(_mock_start_core)` at line 417).
- `_mock_stop_core(listen, pid, all_, timeout, state_dir)` at line 571 (wrapped at line 692).
- `_mock_status_core(listen, state_dir)` at line 700 (wrapped at line 771).
- `ConfigError(message: str, detail: dict)` constructor — already used throughout this file.

**Interfaces:**
- **Produces:** a new module-level helper `_require_posix_daemon() -> None` in `agctl/commands/mock_commands.py`. Contract: when `os.name == "nt"`, raise `ConfigError` with the exact message and `detail` from Global Constraints; otherwise return `None`. It reads `os.name` via the module's `os` binding at call time. Exported only by module attribute (not added to `__all__` unless the implementer finds the existing `__all__` lists private helpers — it does not; leave `__all__` unchanged).
- **Wiring contract:** the helper is called as the **first executable statement** of `_mock_start_core`, `_mock_stop_core`, and `_mock_status_core` (immediately after each function's docstring, before any pidfile/`spawn_daemon`/`is_alive`/`_terminate`/`resolve_target` work). Because each `_core` is wrapped by `@envelope`, a raised `ConfigError` becomes the standard `ok:false` envelope + exit 2 — no envelope changes needed.

- [ ] **Step 1: Write the failing tests**

Add four tests to `tests/unit/test_mock_commands.py`. Import the three `_core` functions and the new helper from `agctl.commands.mock_commands`. The tests force the platform by replacing the module's `os` reference (the seam named in Global Constraints): monkeypatch `agctl.commands.mock_commands.os` with a stand-in object exposing only a `name` attribute (e.g. a `types.SimpleNamespace(name=...)`). Because the gate runs first and raises before any other `os.*` call, the stand-in needs nothing beyond `name`.

1. `test_require_posix_daemon_raises_on_windows` — force `os.name = "nt"`; call `_require_posix_daemon()`; assert it raises `ConfigError` whose `.message` equals the exact message string from Global Constraints, whose `.detail` is a dict containing key `"hint"` equal to the exact hint string, and containing key `"platform"`.
2. `test_require_posix_daemon_noop_on_posix` — force `os.name = "posix"`; call `_require_posix_daemon()`; assert it returns `None` (no exception).
3. `test_mock_start_core_gated_on_windows` — force `os.name = "nt"`; call `_mock_start_core(None, None, None, False, None, "./.agctl")`; assert it raises `ConfigError` with the exact message (dummy args are never reached).
4. `test_mock_stop_and_status_core_gated_on_windows` — force `os.name = "nt"`; call `_mock_stop_core(None, None, False, 10.0, "./.agctl")` and `_mock_status_core(None, "./.agctl")`; assert each raises `ConfigError` with the exact message.

(Tests 3–4 verify the gate is actually wired into each command's entry, not just that the helper exists.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_mock_commands.py -v -k "posix_daemon or gated_on_windows"`
Expected: FAIL — tests 1/3/4 fail because no gate exists (`_require_posix_daemon` is undefined; the `_core` functions do not raise on the forced Windows branch). Test 2 fails on `ImportError` for the undefined helper.

- [ ] **Step 3: Implement the gate**

Add module-level helper `_require_posix_daemon() -> None` to `agctl/commands/mock_commands.py`. Behavior (not code): if `os.name == "nt"`, raise `ConfigError` using the exact message and the exact `detail` dict from Global Constraints, where `detail["platform"]` is `sys.platform` read at call time; otherwise return `None`. Then add a call to `_require_posix_daemon()` as the first executable statement of `_mock_start_core`, `_mock_stop_core`, and `_mock_status_core` (right after each docstring). Do not modify any streaming command, the `mock_run` callback, `mock/daemon.py`, or the `@envelope` wrappers.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_mock_commands.py -v -k "posix_daemon or gated_on_windows"`
Expected: PASS (all four tests green).

- [ ] **Step 5: Run the full unit suite to confirm no regression**

Run: `pytest tests/unit -q`
Expected: PASS — no existing test breaks (the gate is a no-op on the Linux dev host where `os.name == "posix"`).

- [ ] **Step 6: Commit**

Run: `git add agctl/commands/mock_commands.py tests/unit/test_mock_commands.py`
Run: `git commit -m "feat(mock): gate managed daemon to POSIX (ConfigError on native Windows)"`

---

## Task 2: Skip the POSIX-only daemon lifecycle suite on Windows

`tests/unit/test_mock_lifecycle.py` exercises `os.kill`/`os.waitpid`/real signal handlers/spawned sleepers — the exact POSIX surface the daemon is gated away from. It must skip cleanly on Windows rather than fail.

**Files:**
- Modify: `tests/unit/test_mock_lifecycle.py` — add a module-level skip marker after the imports.

**Existing symbols this task consumes:**
- Line 4 `import os` and line 8 `import pytest` are already present (no new imports).

**Interfaces:**
- **Produces:** a module-level `pytestmark = pytest.mark.skipif(os.name == "nt", reason="managed mock daemon is POSIX-only; use mock run or WSL on Windows")`. Placed after the imports (after line 8) and before the first constant (`MINIMAL_CONFIG`). Effect: on `os.name == "nt"` the entire module is skipped during collection; on posix it runs unchanged.

- [ ] **Step 1: Add the skip marker**

Add the module-level `pytestmark` line described above to `tests/unit/test_mock_lifecycle.py`, after the imports and before `MINIMAL_CONFIG`. No other change to the file.

- [ ] **Step 2: Run the file on the dev host to confirm it still collects and runs**

Run: `pytest tests/unit/test_mock_lifecycle.py -q`
Expected: PASS — on the Linux/macOS dev host `os.name != "nt"`, so the skipif is a no-op and the suite runs as before (same pass count as before this task). (The Windows-skip is verified structurally by the marker and will be confirmed by Windows CI in Task 3.)

- [ ] **Step 3: Commit**

Run: `git add tests/unit/test_mock_lifecycle.py`
Run: `git commit -m "test(mock): skip POSIX daemon lifecycle suite on Windows"`

---

## Task 3: Expand the CI matrix to Windows + macOS (unit suite)

Backs the `Operating System :: OS Independent` classifier. Integration stays ubuntu-only.

**Files:**
- Modify: `.github/workflows/test.yml`.

**Existing structure this task consumes:**
- The `test` job currently has `runs-on: ubuntu-latest` (line 18), a `matrix.python-version: ['3.11','3.12','3.13']` (line 22), an install step `pip install -e ".[dev,http,jq,kafka,db,logs,grpc]"`, and a run step `pytest`.

**Interfaces:**
- **Produces:** the matrix gains `os: [ubuntu-latest, windows-latest, macos-latest]`; `runs-on` becomes `${{ matrix.os }}`; `fail-fast: false` is preserved; the run step becomes `pytest tests/unit`. `paths-ignore`, `concurrency`, `workflow_dispatch`, the `cache: pip` setup, and the install command are unchanged.

- [ ] **Step 1: Edit the workflow**

In `.github/workflows/test.yml`, under the `test` job:
1. Move `runs-on: ubuntu-latest` to `runs-on: ${{ matrix.os }}`.
2. Add `os: [ubuntu-latest, windows-latest, macos-latest]` to the `strategy.matrix` (alongside the existing `python-version`).
3. Keep `fail-fast: false`.
4. Change the "Run tests" step from `pytest` to `pytest tests/unit`.
Leave every other line unchanged.

- [ ] **Step 2: Validate the YAML**

Run: `python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/test.yml')); print('yaml ok')"`
Expected: prints `yaml ok` (no parse error). Visually confirm the matrix has 9 cells (3 os × 3 python) and `runs-on: ${{ matrix.os }}`.

- [ ] **Step 3: Audit note for the implementer (no file change — context only)**

Before relying on CI, know the expected failure surface so surprises are diagnosable: an audit of `tests/` found that **all literal `/tmp` and `/var` paths are string-only** (config values and inert constructor args — e.g. `test_logs_client.py` lines 121/158/183/1104/1228 call `_normalize`/use a `FakeBackend`/`validate_config`, never opening the path; `test_overlay.py`, `test_models.py`, `test_resolver.py` store the string only). The file-touching scan/await/follow tests **already use `tmp_path`**. No `/dev/null`/`fcntl`/`resource`/`termios`/`os.symlink` usage exists. Therefore the only Windows-incompatible suite is `test_mock_lifecycle.py` (skipped in Task 2). The spec's D7 (`/tmp` → `tmp_path` conversion) is thus a no-op: there is nothing to convert.

- [ ] **Step 4: Commit**

Run: `git add .github/workflows/test.yml`
Run: `git commit -m "ci: run unit tests on windows-latest and macos-latest"`

- [ ] **Step 5: Push and observe the matrix**

Run: `git push -u origin feat/windows-support`
Expected: the GitHub Actions "tests" workflow runs 9 matrix jobs (ubuntu/windows/macOS × 3.11/3.12/3.13). All should go green: the gate test (Task 1) runs on every OS; `test_mock_lifecycle.py` is skipped on Windows (Task 2); all other unit tests are cross-platform. **If any Windows/macOS job fails**, triage the specific test: if it is a genuine platform assumption not anticipated above, fix it minimally and re-push. Do not mark this task done until at least the ubuntu, windows-latest, and macos-latest jobs for Python 3.12 are green.

---

## Task 4: Document the platform contract

Record the supported/unsupported matrix and the streaming graceful-stop distinction so the `OS Independent` claim and the daemon limitation are honest in the docs and consumer skill.

**Files:**
- Modify: `docs/DESIGN.md` (§3.6 `agctl mock`), `docs/ARCHITECTURE.md` (§15 Known Limitations), `README.md`, `skills/agctl-config/reference/mocks.md`.

**Interfaces:**
- **Produces (prose only — no schema/contract change):**
  - DESIGN.md §3.6 — a short *Platform support* note: the managed daemon (`mock start`/`stop`/`status`) is supported on Linux, macOS, and WSL; on native Windows it exits 2 with a `ConfigError` pointing at `mock run` or WSL. `mock run` (foreground) and every other command group are cross-platform including native Windows. Add the Ctrl+C-vs-SIGTERM graceful-stop distinction: on native Windows, backgrounded streaming commands (`http ping`, `mock run`, `logs tail`, `grpc` streaming) stop gracefully on Ctrl+C/SIGINT; the SIGTERM-driven graceful-stop pattern is POSIX/WSL.
  - ARCHITECTURE.md §15 (Known Limitations) — a bullet recording: native-Windows managed-daemon limitation; that `os.name == "nt"` gates the three daemon `_core`s (WSL passes through as `"posix"`); and the streaming graceful-stop contract above. No change to §1–14.
  - README.md — a one- to two-line *Requirements / Platforms* note: Python ≥ 3.11; runs on Linux, macOS, native Windows, and WSL; the managed mock daemon is Linux/macOS/WSL-only.
  - `skills/agctl-config/reference/mocks.md` — where it describes the managed daemon, add that on native Windows users should use `mock run` or WSL.

- [ ] **Step 1: Add the DESIGN.md note**

Edit `docs/DESIGN.md` §3.6 (the `agctl mock` section). Add the *Platform support* note described above, placed near the daemon (`start`/`stop`/`status`) sub-section so a reader encountering `mock start` sees the limitation immediately.

- [ ] **Step 2: Add the ARCHITECTURE.md limitation**

Edit `docs/ARCHITECTURE.md` §15 (Known Limitations). Append a bullet capturing the native-Windows daemon gate, the `os.name == "nt"` detection (WSL = `"posix"` passes through), and the streaming graceful-stop contract (Ctrl+C on Windows; SIGTERM on POSIX/WSL).

- [ ] **Step 3: Add the README line**

Edit `README.md`. Add a short *Requirements / Platforms* note (Python ≥ 3.11; Linux/macOS/Windows/WSL; managed mock daemon is POSIX/WSL on Windows). Place it in the existing installation/requirements area.

- [ ] **Step 4: Update the mocks skill reference**

Edit `skills/agctl-config/reference/mocks.md`. Where the managed daemon (`mock start`/`stop`/`status`) is described, add that on native Windows it is unavailable and users should use `mock run` or WSL.

- [ ] **Step 5: Commit**

Run: `git add docs/DESIGN.md docs/ARCHITECTURE.md README.md skills/agctl-config/reference/mocks.md`
Run: `git commit -m "docs: record Windows/macOS support and the POSIX-only managed daemon"`

- [ ] **Step 6: Docs sync (per project CLAUDE.md)**

Invoke the `docs-watcher` subagent to confirm the DESIGN.md/ARCHITECTURE.md edits preserve each doc's altitude (DESIGN = WHAT/WHY contract; ARCHITECTURE = HOW as-built) and that no other sync is needed. A correct no-op confirmation is the desired outcome.

---

## Self-Review (completed during authoring)

- **Code scan:** No implementation logic, method bodies, or test code in any task. Each step states behavior, exact expected results, signatures, and data shapes only.
- **Self-containment:** Each task lists the exact symbols/files/lines it consumes and the exact contract it produces; an implementer needs only their own task.
- **Spec coverage:** §2 Goals → Tasks 1+3; §3 Scope (gate; streaming untouched; CI unit-only) → Tasks 1+3 (+ Global Constraints); §5 D1–D10 → Task 1 (D1–D4), Task 2 (D8), Task 3 (D5, D7 resolved as no-op per the audit), Task 1 test seam (D9), Global Constraints (D2/D3/D10); §6 matrix → implied by Tasks 1–2; §7 components → Task 1; §8 CI → Task 3; §9 error model → Task 1 + Global Constraints; §10 testing → Tasks 1–2 + Task 3 verification; §11 dependency evidence → recorded in spec, no task needed (informational); §12/§13 → no task (rejected/deferred); §14 docs → Task 4. **One spec correction surfaced:** spec D7 (convert `/tmp` tests) has no concrete offenders on audit — documented in Task 3 Step 3; no conversion task is needed.
- **Placeholder scan:** No TBD/TODO/"add error handling"/"similar to" patterns. Each test step names its scenario and exact expected result.
- **Type consistency:** `_require_posix_daemon()` spelled identically across Task 1; `ConfigError` constructor `(message, detail)` matches existing usage; `_core` signatures match lines 215/571/700.

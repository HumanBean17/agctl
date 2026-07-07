# Config Overlay (`--overlay`) + Runbook Sidecar Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Layer runbook-specific fixtures and overrides onto the main `agctl.yaml` via a core repeatable `--overlay` flag and a `<runbook>.agctl.yaml` sidecar convention, keeping the runbook pure markdown.

**Architecture:** Add a `PartialConfig` model (Config with `version` optional) and a raw-dict deep-merge step in the load pipeline — each file is interpolated independently, overlays are merged onto the base dict with sidecar-wins semantics, then the merged dict is validated once as `Config` (preserving original YAML keys, so the `CaptureSpec.from_` alias is never round-tripped). Expose `--overlay` globally (mirroring `--config`), thread it through `config validate/show/discover` and the runtime commands, teach the runbook skills to author/consume sidecars, and update DESIGN/ARCHITECTURE.

**Tech Stack:** Python 3.11+ (PEP 604 unions, 3.11 importlib shim), Pydantic v2, Click 8, pytest ≥ 8.

## Global Constraints

- **No schema version bump, no `config migrate` work.** The overlay is load-time composition over the existing v2 schema; `version` stays `"2"`. `config init`/`migrate` gain no `--overlay`.
- **Byte-unchanged invariants.** `agctl/data/sample-config.yaml` must not change — drift-guard tests `tests/unit/test_cli.py::test_sample_matches_readme_block` and `test_config_init_writes_sample` must stay green. *(Pre-flight resolution 2026-07-07: `runbook-template.md` is **not** pinned — nothing tests it, and Task 8 / spec §8 intend its Preconditions to gain the optional `Requires overlay:` line. The earlier "byte-unchanged" wording for the template is superseded; only `sample-config.yaml` is frozen.)*
- **Exit codes unchanged.** `0` success; `2` config/tool/env error (overlay parse failure, version mismatch, type mismatch, dangling ref).
- **Runbook stays pure markdown.** No YAML front-matter, no embedded config block (honors prior design §4 no-registry, §9 no-YAML-runbook). The runbook gains at most one `Preconditions` line naming the sidecar.
- **Sidecar extension is exactly `.agctl.yaml`** (reserved, greppable); discovery by sibling convention `<runbook-base>.agctl.yaml`.
- **`--overlay` is repeatable; later wins.** Precedence: base `<` overlays (in flag order) `<` `AGCTL_*` env overrides (still highest).
- **Out of scope (do not build):** no `AGCTL_OVERLAY` env var, no directory auto-include, no include/imports directives.
- **Test conventions.** pytest; new unit tests go in `tests/unit/` (flat, one module per source area — use `tests/unit/test_overlay.py` for the merge/compose logic). Build temp configs with the `tmp_path` fixture and `yaml.safe_load` round-trips; drive Click commands with `click.testing.CliRunner`. The shared fixture is `tests/fixtures/agctl.yaml` (`FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"`).
- **Commits.** Conventional style (`feat(config): …`, `docs(skill): …`); end every commit message with `Co-Authored-By: Claude <noreply@anthropic.com>`. Commit test + implementation together per task.

---

## File Structure

**Create:**
- `tests/unit/test_overlay.py` — merge, compose, and CLI-threading unit tests.

**Modify (core plumbing):**
- `agctl/config/models.py` — add `PartialConfig` (Config subclass, `version` optional).
- `agctl/config/loader.py` — add `ComposedConfig` type, `deep_merge()` helper, `compose_config()`; extend `load_config()` with an `overlays` parameter.
- `agctl/command.py` — extend `load_config_or_raise()` with an `overlay_paths` parameter.

**Modify (CLI surface):**
- `agctl/cli.py` — add global `--overlay` option → `ctx.obj["overlay_paths"]`.
- `agctl/commands/config_commands.py` — `validate` and `show` gain `--overlay`; `validate` surfaces override warnings; `show` emits composed config + override list when `--overlay` is used.
- `agctl/commands/discover_commands.py` — `discover` gains `--overlay`; thread through the four cores.
- `agctl/commands/http_commands.py`, `db_commands.py`, `kafka_commands.py`, `check_commands.py` — read `ctx.obj["overlay_paths"]`, forward to `load_config_or_raise`.
- `agctl/commands/mock_commands.py` — `mock run` and `mock start` forward `ctx.obj["overlay_paths"]`; `mock start` also injects `--overlay` into the rebuilt daemon argv.

**Modify (skills — markdown):**
- `skills/agctl-write-test-runbook/SKILL.md` (+ reference `runbook-template.md` Preconditions) — author runbook-specific fixtures into a sidecar.
- `skills/agctl-run-test-runbook/SKILL.md` — auto-discover sidecar; inject `--overlay` into every invocation; pre-run `config validate --overlay`.
- `skills/agctl-config/SKILL.md` — document `--overlay` and the sidecar convention.

**Modify (docs):**
- `docs/DESIGN.md` §2, §5 — overlay model, merge contract, `--overlay` flag.
- `docs/ARCHITECTURE.md` §5 — as-built load pipeline with overlay + merge steps.

---

### Task 1: `PartialConfig` model

**Files:**
- Modify: `agctl/config/models.py` (append after the `Config` class, ~line 287)
- Test: `tests/unit/test_overlay.py` (create)

**Interfaces:**
- Consumes: existing `Config` class (`models.py:280`).
- Produces: `class PartialConfig(Config)` — identical to `Config` except `version: str | None = None` (override the one required field to optional). Used by Task 3 to validate each overlay fragment in isolation so type errors attribute to the overlay file. All other fields inherit their defaults unchanged (sections already default to empty). `PartialConfig.model_validate(dict)` accepts a dict with no `version` key; accepts `version: "2"`; accepts any string `version` (the major-must-match-base rule is enforced in the loader, not the model).

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_overlay.py`. Imports: `from agctl.config.models import Config, PartialConfig`. Three scenarios:
1. `PartialConfig.model_validate({})` returns an instance with `.version is None` and default-empty sections (e.g. `.templates == {}`).
2. `PartialConfig.model_validate({"version": "2"})` returns `.version == "2"`.
3. `PartialConfig.model_validate({"templates": {"t": {"method": "GET", "service": "svc", "path": "/"}}})` returns an instance whose `.templates["t"]` is an `HttpTemplate` (proves section validation still works without a version).
Also add a guard: `Config.model_validate({})` raises `ValidationError` (version still required on the base model — the override is on `PartialConfig` only).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_overlay.py -v`
Expected: FAIL — `ImportError: cannot import name 'PartialConfig'`.

- [ ] **Step 3: Write minimal implementation**

Add `PartialConfig` to `agctl/config/models.py` as a subclass of `Config` that overrides only the `version` field to `str | None = None`. Add a one-line docstring: "Overlay fragment — Config with version optional; version is inherited from the base at merge time (spec D5)." Do not change `Config`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_overlay.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

Run: `git add agctl/config/models.py tests/unit/test_overlay.py && git commit -m "feat(config): add PartialConfig overlay model"`
(message ends with the Co-Authored-By trailer — same for every commit below).

---

### Task 2: `deep_merge` helper

**Files:**
- Modify: `agctl/config/loader.py` (add module-level helper)
- Test: `tests/unit/test_overlay.py` (append)

**Interfaces:**
- Consumes: nothing from other tasks (pure dict utility).
- Produces: `def deep_merge(base: dict, overlay: dict, overlay_name: str, overrides: list[dict], path: str = "") -> dict`. Mutates and returns `base`. Contract:
  - For each `key` in `overlay`:
    - If `key` absent from `base`: `base[key] = overlay[key]` (an addition — no override recorded).
    - If `key` in `base` and **both** `base[key]` and `overlay[key]` are `dict`: recurse with `path = f"{path}.{key}"` (or `key` if path empty) — nested dicts merge key-by-key.
    - Else (scalar/list leaf, or dict-vs-scalar type clash): append `{"path": <dotted>, "overlay": overlay_name}` to `overrides`, then `base[key] = overlay[key]` (overlay wins; a clash is an override, recorded).
  - The dotted `path` uses `.` separators matching `validate_config`'s `{"path": …}` shape (e.g. `templates.create-order`), so override records drop straight into the warnings list later.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_overlay.py`. Import `from agctl.config.loader import deep_merge`. Scenarios (each builds two plain dicts, an empty `overrides` list, calls `deep_merge(base, overlay, "sidecar.yaml", overrides)`, asserts on the returned dict and `overrides`):
1. **Addition:** overlay key absent in base → merged has it; `overrides` empty.
2. **Scalar override:** both have `{"a": 1}` vs `{"a": 2}` → merged `a == 2`; `overrides == [{"path": "a", "overlay": "sidecar.yaml"}]`.
3. **List replace:** base `{"brokers": ["x"]}`, overlay `{"brokers": ["y","z"]}` → merged `brokers == ["y","z"]` (lists replace, not extend); one override recorded at path `brokers`.
4. **Nested dict merge:** base `{"templates": {"keep": 1, "shared": {"m": "GET"}}}`, overlay `{"templates": {"new": 2, "shared": {"m": "POST"}}}` → merged `templates` has `keep`, `new`, and `shared.m == "POST"`; exactly one override recorded at path `templates.shared.m`.
5. **Type clash = override:** base `{"x": {"a": 1}}`, overlay `{"x": "scalar"}` → merged `x == "scalar"`; one override at path `x`.
6. **Dotted path nesting:** confirm the path string for a nested override is dot-joined (e.g. `kafka.patterns.foo.match`), built correctly across two levels.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_overlay.py -v`
Expected: FAIL — `ImportError: cannot import name 'deep_merge'`.

- [ ] **Step 3: Write minimal implementation**

Add `deep_merge` to `loader.py` implementing the contract above. Behavior, not algorithm: walk overlay keys; recurse on dict/dict; record-and-replace on everything else; dot-join the path. No Pydantic, no I/O.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_overlay.py -v`
Expected: PASS (all merge tests).

- [ ] **Step 5: Commit**

Run: `git add agctl/config/loader.py tests/unit/test_overlay.py && git commit -m "feat(config): add sidecar-wins deep_merge helper"`

---

### Task 3: `compose_config` + `load_config(overlays=)` + `load_config_or_raise` forwarding

**Files:**
- Modify: `agctl/config/loader.py` (add `ComposedConfig`, `compose_config`; extend `load_config`)
- Modify: `agctl/command.py` (`load_config_or_raise` signature)
- Test: `tests/unit/test_overlay.py` (append)

**Interfaces:**
- Consumes: Task 1 `PartialConfig`, Task 2 `deep_merge`, existing `discover_config_path`, `interpolate`, `apply_env_overrides`, `_check_version`, `Config`, and `ConfigError` (from `..errors`; constructor `(message: str, detail: dict)`).
- Produces:
  - `class ComposedConfig(NamedTuple)` with fields `config: Config` and `overrides: list[dict]` (each override dict is `{"path": str, "overlay": str}`).
  - `def compose_config(path: str | None = None, overlays: list[str] | None = None, env: dict[str, str] | None = None) -> ComposedConfig`. Pipeline contract:
    1. `env = env if env is not None else os.environ`.
    2. `base_path = discover_config_path(explicit=path, env=env)` (existing discovery; raises `ConfigError` if none found).
    3. `base_raw = interpolate(yaml.safe_load(base_path.read_text()) or {}, env)`.
    4. `_check_version(base_raw)` (base must be v2 — reuse existing helper unchanged).
    5. `overrides: list[dict] = []`.
    6. For each `ov` in `(overlays or [])`, in order:
       - If `pathlib.Path(ov).is_file()` is false → `raise ConfigError(f"Overlay file not found: {ov}", {"path": ov})`.
       - `raw_ov = interpolate(yaml.safe_load(pathlib.Path(ov).read_text()) or {}, env)`.
       - Validate the fragment: `PartialConfig.model_validate(raw_ov)`; on `ValidationError` → `raise ConfigError(f"Invalid overlay: {ov}", {"overlay": ov, "validation_errors": exc.errors()}) from exc`.
       - If `raw_ov.get("version")` is not None: its major (string before the first `.`) must equal `TOOL_MAJOR_VERSION` (`"2"`), else `raise ConfigError(f"Overlay version mismatch in {ov}: major must be {TOOL_MAJOR_VERSION}", {"overlay": ov, "found": <major>})`.
       - `deep_merge(base_raw, raw_ov, ov, overrides)`.
    7. `with_env = apply_env_overrides(base_raw, env)` (env applied to the **merged** dict, so `AGCTL_*` wins over overlays — precedence per Global Constraints).
    8. `try: config = Config.model_validate(with_env)`; on `ValidationError` → `raise ConfigError("Invalid configuration", {"validation_errors": exc.errors()}) from exc` (this is the single final validation that catches type mismatches surviving the merge).
    9. `return ComposedConfig(config, overrides)`.
  - `def load_config(path: str | None = None, env: dict[str, str] | None = None, overlays: list[str] | None = None) -> Config` → returns `compose_config(path, overlays, env).config`. (Existing 1- and 2-arg callers keep working; `overlays` defaults None.)
  - `agctl/command.py`: `def load_config_or_raise(config_path: str | None = None, overlay_paths: list[str] | None = None) -> Config` → body does the lazy `from .config import load_config` then `return load_config(config_path, overlays=overlay_paths)`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_overlay.py`. Imports: `from agctl.config import load_config` and `from agctl.config.loader import compose_config, ComposedConfig` and `from agctl.errors import ConfigError`. Use `tmp_path` to write a base `agctl.yaml` (minimal v2: `version: "2"`, one service `orders` + one template `create-order` referencing it) and overlay files. Scenarios:
1. **Overlay adds a template:** `compose_config(str(base), [str(ov)])` where `ov` adds `templates.extra` → `.config.templates` has both base and `extra`; `.overrides == []`.
2. **Override recorded:** base and overlay both define `templates.create-order` (different `path`) → `.config.templates["create-order"].path` equals the overlay value; `.overrides` has one entry `{"path": "templates.create-order", "overlay": <ov path>}`.
3. **Version inherited:** overlay has no `version` → no error; `.config.version == "2"`.
4. **Overlay version mismatch:** overlay has `version: "3"` → `compose_config` raises `ConfigError` whose `detail["overlay"]` names the overlay and message mentions mismatch.
5. **Bad overlay fragment:** overlay has `templates.bad` missing required `method` → `ConfigError` with `detail["overlay"]` set (attributed to the overlay, not "Invalid configuration").
6. **Type clash caught at final validate:** base `templates.x` is a valid HTTP template dict; overlay `templates.x` replaced with a non-template dict that fails `Config` validation → raises `ConfigError("Invalid configuration", …)`.
7. **Env wins over overlay:** base `templates.x.path == "/a"`, overlay sets it `"/b"`, env `{"AGCTL_TEMPLATES__X__PATH": "/env"}` → `compose_config(str(base), [str(ov)], env=...)` → `.config.templates["x"].path == "/env"` (env beats overlay).
8. **Missing overlay file:** `compose_config(str(base), [str(tmp_path/"nope.yaml")])` → `ConfigError` with `detail["path"]` set.
9. **`load_config` forwards overlays:** `load_config(str(base), overlays=[str(ov)])` returns a `Config` with the overlay's added template (proves the 3-arg path).
10. **No-overlay back-compat:** `load_config(str(base))` and `load_config(str(base), env={})` behave exactly as before (return the base `Config`).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_overlay.py -v`
Expected: FAIL — `ImportError: cannot import name 'compose_config'` (and `load_config` rejects the `overlays` kwarg).

- [ ] **Step 3: Write minimal implementation**

Add `ComposedConfig` (NamedTuple), `compose_config`, and the `overlays` parameter to `load_config` in `loader.py` per the contract above. Update `load_config_or_raise` in `command.py` to accept and forward `overlay_paths`. Do not change `_check_version`, `interpolate`, `apply_env_overrides`, or `discover_config_path`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_overlay.py -v`
Expected: PASS (all tests, including Tasks 1–2's). Also run `pytest tests/unit/test_loader.py -v` — Expected: PASS (no regressions; existing 1/2-arg calls still work).

- [ ] **Step 5: Commit**

Run: `git add agctl/config/loader.py agctl/command.py tests/unit/test_overlay.py && git commit -m "feat(config): compose base+overlays via deep-merge in load pipeline"`

---

### Task 4: Global `--overlay` flag + `config show --overlay`

**Files:**
- Modify: `agctl/cli.py` (root group option, ~line 109-117)
- Modify: `agctl/commands/config_commands.py` (`config_show`, lines 177-193)
- Test: `tests/unit/test_overlay.py` (append) and `tests/unit/test_cli.py` (append)

**Interfaces:**
- Consumes: Task 3 `compose_config`, existing `emit` (from `..output`), existing `_mask` helper in `config_commands.py`.
- Produces:
  - `cli.py` root group gains `@click.option("--overlay", "overlay_paths", multiple=True, help="Overlay config fragment (repeatable; later wins)")`; `cli` signature becomes `(ctx, config_path, overlay_paths)`; body stores `ctx.obj["overlay_paths"] = tuple(overlay_paths) or None`.
  - `config_show` gains `@click.option("--overlay", "overlay_paths", multiple=True, default=None)`; precedence `ovs = tuple(overlay_paths) or ctx.obj.get("overlay_paths")`; replaces `load_config(path)` with `composed = compose_config(path, list(ovs) if ovs else None)`; emits:
    - **Without `--overlay`:** `result = <masked cfg.model_dump()>` — byte-for-byte the same shape as today (back-compat).
    - **With `--overlay`:** `result = {"config": <masked cfg.model_dump()>, "overrides": composed.overrides}` (overrides is `[]` when none). Masking (`_mask` / `--unmask`) applies to the `config` subtree exactly as today.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_overlay.py` (CliRunner path) and/or `tests/unit/test_cli.py`. Use `CliRunner().invoke(cli, [...])` and `json.loads(result.output)`. Scenarios:
1. **Global flag threads to show:** write base + overlay (overlay adds `templates.extra`); invoke `cli(["--overlay", str(ov), "config", "show", "--config", str(base)])`; `result.exit_code == 0`; `payload["result"]["config"]["templates"]` contains `extra` (and base templates); `payload["result"]["overrides"] == []`.
2. **Override surfaced in show:** base + overlay both define `templates.create-order` differently; invoke with `--overlay`; `payload["result"]["config"]["templates"]["create-order"]` reflects the overlay value; `payload["result"]["overrides"]` has one entry with `path == "templates.create-order"`.
3. **No-overlay shape unchanged:** invoke `cli(["config", "show", "--config", str(base)])` (no `--overlay`); `payload["result"]` is the config dict directly (no `"config"`/`"overrides"` wrapper) — i.e. `"templates" in payload["result"]` is True and `"config" not in payload["result"]`.
4. **Post-command form:** `cli(["config", "show", "--config", str(base), "--overlay", str(ov)])` works the same as the global form (own option precedence).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_overlay.py tests/unit/test_cli.py -v`
Expected: FAIL — `--overlay` unknown option (Click `UsageError`), and `result` lacks the wrapper.

- [ ] **Step 3: Write minimal implementation**

Add the `--overlay` option to the root `cli` group and store it on ctx.obj. Add the `--overlay` option to `config_show`, compute `ovs`, call `compose_config`, and branch the emitted `result` shape on whether `--overlay` was supplied (detect by `bool(ovs)` — own option or ctx.obj). Keep `_mask`/`--unmask` behavior identical on the config subtree.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_overlay.py tests/unit/test_cli.py -v`
Expected: PASS. Also `pytest tests/unit/test_cli.py::test_sample_matches_readme_block tests/unit/test_cli.py::test_config_init_writes_sample -v` — Expected: PASS (drift guards intact).

- [ ] **Step 5: Commit**

Run: `git add agctl/cli.py agctl/commands/config_commands.py tests/unit/test_overlay.py tests/unit/test_cli.py && git commit -m "feat(config): add global --overlay flag and config show --overlay"`

---

### Task 5: `config validate --overlay` (override warnings)

**Files:**
- Modify: `agctl/commands/config_commands.py` (`config_validate`, lines 129-171)
- Test: `tests/unit/test_overlay.py` (append) and `tests/unit/test_config_commands.py` (append)

**Interfaces:**
- Consumes: Task 3 `compose_config`, existing `validate_config` (returns `(errors, warnings)` each entry `{"path","message"}`), existing `_plugin_validation_errors`, `collect_jq_compile_errors`, `collect_capture_placement_errors`, `_emit_config_error`.
- Produces: `config_validate` gains `@click.option("--overlay", "overlay_paths", multiple=True, default=None)`; precedence `ovs = tuple(overlay_paths) or ctx.obj.get("overlay_paths")`; replaces `load_config(path)` with `composed = compose_config(path, list(ovs) if ovs else None)` wrapped in the existing `try/except ConfigError`. After `errors, warnings = validate_config(composed.config)`, **append override warnings**: for each `{"path","overlay"}` in `composed.overrides`, append `{"path": p, "message": f"overridden by overlay {pathlib.Path(ov).name}"}` to `warnings`. The rest of `validate` (plugin/jq/capture folds, exit-2 on errors, emit with `valid`/`errors`/`warnings`) is unchanged.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_config_commands.py` (mirror its `_validate` helper that returns the `CliRunner` result). Scenarios:
1. **Override warning emitted:** base + overlay where overlay overrides `templates.create-order`; `cli(["config","validate","--config",base,"--overlay",ov])` → `exit_code == 0`; `payload["result"]["valid"] is True`; one warning whose `path == "templates.create-order"` and whose `message` contains `"overridden by overlay"` and the overlay filename.
2. **No override, no warning:** overlay only adds a new template; validate with `--overlay` → `valid is True`, no `overridden` warnings (other warnings like missing-description may appear — assert specifically that no warning message contains `"overridden by overlay"`).
3. **Cross-file dangling ref is still an error:** overlay adds `templates.x` referencing `service: ghost` (not in base or overlay) → `exit_code == 2`; `payload["result"]["valid"] is False`; an error with `path == "templates.x.service"`.
4. **Global form threads:** `cli(["--overlay", ov, "config", "validate", "--config", base])` behaves identically to the post-command form in scenario 1.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_config_commands.py -v`
Expected: FAIL — `--overlay` unknown to `validate`; override warnings absent.

- [ ] **Step 3: Write minimal implementation**

Add the `--overlay` option to `config_validate`; compute `ovs`; switch to `compose_config`; map `composed.overrides` into warnings (filename via `pathlib.Path(ov).name`); leave the error/emit logic untouched.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_config_commands.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/config_commands.py tests/unit/test_config_commands.py && git commit -m "feat(config): config validate --overlay with override warnings"`

---

### Task 6: `discover --overlay`

**Files:**
- Modify: `agctl/commands/discover_commands.py` (`discover` command lines 508-520, the four envelope cores at 193-481, and `_emit_argument_error` 493-505)
- Test: `tests/unit/test_discover_command.py` (append)

**Interfaces:**
- Consumes: Task 3 `load_config_or_raise` (now overlay-aware); the four cores currently call `load_config_or_raise(config_path)`.
- Produces: `discover` gains `@click.option("--overlay", "overlay_paths", multiple=True, default=None)`; resolves `ovs = tuple(overlay_paths) or (ctx.obj.get("overlay_paths") if ctx.obj else None)`; passes `overlay_paths=list(ovs) if ovs else None` into each of the four `@envelope`-wrapped cores — `_summary_envelope`, `_category_envelope`, `_item_envelope`, `_search_envelope` (defined at lines 193-481; `discover` dispatches to one of them based on `--category`/`--name`/`--search`). Each core's current `load_config_or_raise(config_path)` call becomes `load_config_or_raise(config_path, overlay_paths)`. The cores then list composed entries automatically (they iterate the composed `Config`). Categories, item shapes, and the `--category`/`--name`/`--search` output contracts are unchanged.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_discover_command.py` (mirror its CliRunner pattern; `FIXTURE` is `tests/fixtures/agctl.yaml`). Build a base config + an overlay that **adds** an `http-templates` entry and a `mock-http-stubs` entry not in the base. Scenarios:
1. **Composed category listing:** `cli(["discover","--category","http-templates","--config",base,"--overlay",ov])` → `exit_code == 0`; the `items` list includes both base templates and the overlay-added template by name.
2. **Mock stubs visible across files:** same invocation with `--category mock-http-stubs` → includes the overlay-added stub.
3. **`--name` resolves an overlay-only item:** `cli(["discover","--category","http-templates","--name",<overlay-only-name>,"--config",base,"--overlay",ov])` → `exit_code == 0`; returns full detail (proves the overlay-only template is fully present in the composed config).
4. **No overlay = today's behavior:** without `--overlay`, the overlay-only name is absent (or a not-found error) — i.e. the base-only listing is unchanged.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_discover_command.py -v`
Expected: FAIL — `--overlay` unknown to `discover`; overlay-only items absent.

- [ ] **Step 3: Write minimal implementation**

Add the `--overlay` option to `discover`; resolve `ovs`; thread `overlay_paths` through the four cores' `load_config_or_raise` calls. Do not alter category logic or output shapes.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_discover_command.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/discover_commands.py tests/unit/test_discover_command.py && git commit -m "feat(config): discover --overlay lists composed entries"`

---

### Task 7: Thread `--overlay` into runtime commands (http/db/kafka/check/mock)

**Files:**
- Modify: `agctl/commands/http_commands.py`, `db_commands.py`, `kafka_commands.py`, `check_commands.py` (each command callback)
- Modify: `agctl/commands/mock_commands.py` (`mock_run` ~411-433; `mock_start` core `_mock_start_core` ~215-374, daemon-argv build ~290-300)
- Test: `tests/unit/test_overlay.py` (append), `tests/unit/test_mock_commands.py` (append)

**Interfaces:**
- Consumes: Task 3 `load_config_or_raise(config_path, overlay_paths)`. Today each runtime callback resolves `config_path` then calls `load_config_or_raise(config_path)`; `mock_run`/`mock_start` resolve via `ctx.obj.get("config_path")`.
- Produces (uniform one-line-per-callback change): each runtime callback additionally reads `ovs = ctx.obj.get("overlay_paths") if ctx.obj else None` and calls `load_config_or_raise(config_path, overlay_paths=list(ovs) if ovs else None)`. No new per-command `--overlay` option (the run skill uses the global form `agctl --overlay X.agctl.yaml <group> <cmd>`). Additionally:
  - `mock_run`: forward `overlay_paths` into its `load_config_or_raise` call (~line 457).
  - `mock_start`: forward into `_mock_start_core`'s `load_config_or_raise` (~line 234), **and** in the rebuilt daemon argv (~lines 290-300) append `["--overlay", str(Path(ov).absolute())]` for each overlay path so the spawned `mock run` daemon loads the same overlay(s). (`mock stop`/`mock status` are unchanged — they load no config; the global flag is stored on ctx.obj and ignored.)

- [ ] **Step 1: Write the failing tests**

Two test groups:

A. **Forwarding (no live services needed)** — append to `tests/unit/test_overlay.py`. Use `monkeypatch` to capture the overlays a runtime command forwards. For one representative command per affected module, monkeypatch `load_config_or_raise` **at the module where it's used** (e.g. `agctl.commands.http_commands.load_config_or_raise`) with a stub that records its `overlay_paths` kwarg/arg and returns a minimal `Config` (build one via `Config(version="2", services={"orders": ServiceConfig(base_url="http://x")})`), then invoke `cli(["--overlay", str(ov), "http", "call", "some-template", "--config", str(base)])`. Assert the captured overlays equal `[str(ov)]`. Repeat the same shape for one command each in `db_commands` (`db query`), `kafka_commands` (`kafka produce` or `kafka consume`), `check_commands` (`check ready`), and `mock_commands` (`mock run`). Each asserts the overlay reached `load_config_or_raise`.

B. **`mock start` daemon argv** — append to `tests/unit/test_mock_commands.py`. Monkeypatch the daemon-spawn mechanism in `_mock_start_core` (the function that builds `daemon_argv` and spawns the process — locate it around lines 290-300) so it records `daemon_argv` instead of actually spawning; invoke `cli(["--overlay", str(ov), "mock", "start", "--config", str(base), ...])`; assert the recorded `daemon_argv` contains `--overlay` followed by the absolute path of `ov`. (If the spawn is hard to isolate, assert on the constructed argv list via the function that builds it, patched at that boundary.)

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_overlay.py tests/unit/test_mock_commands.py -v`
Expected: FAIL — overlays not forwarded (captured value is `None`/absent); daemon argv lacks `--overlay`.

- [ ] **Step 3: Write minimal implementation**

In each runtime callback (`http_commands`, `db_commands`, `kafka_commands`, `check_commands`, `mock_run`, `mock_start`), read `ovs` from ctx.obj and pass `overlay_paths=list(ovs) if ovs else None` to `load_config_or_raise`. In `_mock_start_core`, extend the daemon argv with `--overlay <abs>` for each overlay before spawning.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_overlay.py tests/unit/test_mock_commands.py -v`
Expected: PASS. Also run the broader unit suite to catch signature regressions: `pytest tests/unit -k "http or db or kafka or check or mock"` — Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/http_commands.py agctl/commands/db_commands.py agctl/commands/kafka_commands.py agctl/commands/check_commands.py agctl/commands/mock_commands.py tests/unit/test_overlay.py tests/unit/test_mock_commands.py && git commit -m "feat(config): thread --overlay through runtime + mock daemon argv"`

---

### Task 8: Skills — runbook pair (write authors sidecar, run discovers + injects)

**Files:**
- Modify: `skills/agctl-write-test-runbook/SKILL.md` (§3 Design ~lines 50-71; §4 Emit ~73-81)
- Modify: `skills/agctl-write-test-runbook/reference/runbook-template.md` (Preconditions ~lines 18-21)
- Modify: `skills/agctl-run-test-runbook/SKILL.md` (§1 Validate ~38-46; §2 Setup ~48-61; §3 Execute ~64-68)
- Test: manual verification via the CLI built in Tasks 3-7 (no code test)

**Interfaces / behavior to add (markdown procedure):**
- **write skill §3 Design:** when a needed template/mock/seed-template is **not** found via `agctl discover` against the main config, the skill places its definition in a **sidecar** `<runbook-base>.agctl.yaml` (sibling to the runbook) rather than instructing edits to the main `agctl.yaml`. State the rule: shared infra stays in main; runbook-specific fixtures (one-off seed `db write` templates, ad-hoc mocks, scratch HTTP templates) and per-runbook overrides go in the sidecar.
- **write skill §4 Emit:** when any sidecar entry was produced, also write `<runbook-base>.agctl.yaml` next to `runbook.md`, and add a `Preconditions` line to the runbook: `Requires overlay: <runbook-base>.agctl.yaml` (so a human copy-pasting a step knows to add `--overlay`). The runbook otherwise stays pure markdown — no front-matter, no embedded config block.
- **runbook-template.md Preconditions:** document the optional `Requires overlay: <file>` line as a precondition type.
- **run skill §1 Validate:** before executing, look for a sibling `<runbook-base>.agctl.yaml`; if present, run `agctl config validate --overlay <sidecar>` (surfacing any `overridden by overlay` warnings into the report) and treat the sidecar as active for the whole run.
- **run skill §2 Setup and §3 Execute:** when a sidecar is active, prefix every `agctl` invocation with the global `--overlay <sidecar>` form (`agctl --overlay <sidecar> <group> <cmd> …`) — covering `check ready`, `db execute` (seed), `mock run`, `http ping` (heartbeat), and every step's command. If no sidecar exists, run exactly as today.

- [ ] **Step 1: Write the failing "test" (verification procedure)**

These are markdown skills — the "test" is a trace against the real CLI. Document the expected outcome of the trace (no code to write):
1. Using `tests/fixtures/agctl.yaml` as the base, author (by hand, following the updated write skill) a tiny runbook `tmp_runbook.md` whose only template is one **not** in the base, plus the sidecar `tmp_runbook.agctl.yaml` defining it.
2. Run `agctl config validate --config tests/fixtures/agctl.yaml --overlay tmp_runbook.agctl.yaml` → Expected: `ok: true`, exit 0 (the sidecar is a valid overlay).
3. Run `agctl --overlay tmp_runbook.agctl.yaml discover --category http-templates --config tests/fixtures/agctl.yaml` → Expected: the sidecar-only template appears.
4. Confirm `tmp_runbook.md` contains the `Requires overlay: tmp_runbook.agctl.yaml` precondition line and **no** YAML front-matter/config block.

- [ ] **Step 2: Run the trace to confirm it currently can't pass**

Run the trace against the **unchanged** skills: the write skill still tells you to edit main (no sidecar produced), so steps 2-4 of the trace can't be satisfied as designed. Expected: the sidecar convention is absent.

- [ ] **Step 3: Apply the skill edits**

Edit `agctl-write-test-runbook/SKILL.md` (§3 and §4) and `reference/runbook-template.md` (Preconditions) to add the sidecar-authoring rule and the `Requires overlay:` precondition. Edit `agctl-run-test-runbook/SKILL.md` (§1, §2, §3) to add sidecar discovery + global `--overlay` injection. Keep all other procedure text intact; do not touch the report-format reference (`report-annotation.md`).

- [ ] **Step 4: Run the trace to verify it passes**

Re-run the trace from Step 1. Expected: `config validate --overlay` → `ok:true`; `discover --overlay` lists the sidecar template; the runbook carries the `Requires overlay:` line and stays pure markdown.

- [ ] **Step 5: Commit**

Run: `git add skills/agctl-write-test-runbook/SKILL.md skills/agctl-write-test-runbook/reference/runbook-template.md skills/agctl-run-test-runbook/SKILL.md && git commit -m "docs(skill): runbook sidecar authoring + run-time --overlay injection"`

---

### Task 9: Skill — `agctl-config` `--overlay` awareness

**Files:**
- Modify: `skills/agctl-config/SKILL.md` (intro ~6-16; Step 0 ~32-42; close-out ~91-112)
- Test: manual verification (no code test)

**Interfaces / behavior to add (markdown):**
- In the intro, note that runbook-specific fixtures live in sidecars layered via `--overlay`, not in the main `agctl.yaml` — the boundary this skill enforces when authoring.
- In the close-out, add: when editing a sidecar, the verify command becomes `agctl config validate --config <base> --overlay <sidecar>` (and `agctl discover --overlay <sidecar> --name <key>`). The base-only close-out stays the default.
- Reaffirm the unchanged drift-guard rule: never edit `agctl/data/sample-config.yaml`; the sidecar feature adds no sample.

- [ ] **Step 1: Write the failing "test" (verification)**

Expected: after editing a hypothetical sidecar, the skill's close-out instructs `config validate --overlay`. Before the edit, the close-out mentions only base `config validate`.

- [ ] **Step 2: Confirm current state fails the check**

Read the close-out section — it references only `agctl config validate` / `agctl discover` with no `--overlay`. Expected: the sidecar guidance is absent.

- [ ] **Step 3: Apply the skill edits**

Add the sidecar/`--overlay` notes to the intro and close-out as described. Do not change the authoring modes (`http`, `kafka`, `db`, `mock`, `init`) or the placeholder contract.

- [ ] **Step 4: Verify**

Re-read the edited sections; confirm the close-out now shows the `--overlay` verify form and the intro states the infra-vs-fixture boundary. Confirm `agctl/data/sample-config.yaml` is untouched.

- [ ] **Step 5: Commit**

Run: `git add skills/agctl-config/SKILL.md && git commit -m "docs(skill): agctl-config --overlay awareness"`

---

### Task 10: Docs — DESIGN.md + ARCHITECTURE.md, then docs-watcher

**Files:**
- Modify: `docs/DESIGN.md` (§2 config schema/discovery; §5 resolution pipeline)
- Modify: `docs/ARCHITECTURE.md` (§5 as-built load pipeline)
- Verification: invoke the `docs-watcher` subagent (per `CLAUDE.md` "Docs Sync")

**Behavior to document (WHAT/HOW altitude):**
- **DESIGN.md §2** (user-facing contract, WHAT/WHY): `--overlay <path>` (repeatable, later wins; precedence base < overlays < `AGCTL_*` env); the `<runbook>.agctl.yaml` sidecar convention (co-located fragment, sibling discovery, runbook stays markdown); that an overlay is a partial config (version optional, inherited); that overrides are surfaced as `config validate` warnings. This is **not** a schema version change.
- **DESIGN.md §5** (resolution pipeline): where overlay load + deep-merge sit in the pipeline (after per-file interpolation, before final `Config` validation), and the sidecar-wins merge contract (dict merge key-by-key; scalar/list replace; type clash → error).
- **ARCHITECTURE.md §5** (as-built, HOW): extend the load-pipeline diagram with the overlay loop (per-overlay interpolate → `PartialConfig` validate → version check → `deep_merge`) and the single final `Config.model_validate`; note `compose_config`/`ComposedConfig` and the override set feeding `validate`/`show`.

- [ ] **Step 1: Write the failing check**

Run the docs-watcher subagent against the current diff (Tasks 1-9 committed). Expected finding: `DESIGN.md` §2/§5 and `ARCHITECTURE.md` §5 are out of sync — they describe single-file loading with no `--overlay`, no overlay model, no sidecar.

- [ ] **Step 2: Confirm the gap**

Inspect the two docs' config sections; confirm neither mentions `--overlay`/overlay/sidecar. Expected: gap confirmed.

- [ ] **Step 3: Apply the doc edits**

Update `DESIGN.md` §2 and §5 (WHAT/WHY altitude — contract, not code) and `ARCHITECTURE.md` §5 (HOW altitude — as-built pipeline, module/function references: `compose_config`, `ComposedConfig`, `deep_merge`, `PartialConfig`). Preserve each doc's altitude (DESIGN = user contract; ARCHITECTURE = as-built).

- [ ] **Step 4: Verify**

Re-run the `docs-watcher` subagent. Expected: it confirms the docs are now in sync (or makes only altitude-appropriate touch-ups). Run `pytest tests/unit -q` — Expected: all green (docs changes don't affect tests; confirms no accidental code/sample drift).

- [ ] **Step 5: Commit**

Run: `git add docs/DESIGN.md docs/ARCHITECTURE.md && git commit -m "docs: config overlay --overlay + runbook sidecar (DESIGN §2/§5, ARCHITECTURE §5)"`

---

## Definition of Done

- `pytest tests/unit -q` is fully green, including the new `tests/unit/test_overlay.py` and the appended cases in `test_cli.py`, `test_config_commands.py`, `test_discover_command.py`, `test_mock_commands.py`.
- Drift guards `test_sample_matches_readme_block` and `test_config_init_writes_sample` still pass (sample-config + runbook-template byte-unchanged).
- `agctl --overlay <sidecar> config validate` warns on overrides; `config show --overlay` shows the composed config + overrides; `discover --overlay` lists composed entries; runtime commands and the `mock start` daemon inherit the overlay.
- The three skills teach sidecar authoring/consumption and `--overlay` awareness.
- DESIGN.md §2/§5 and ARCHITECTURE.md §5 are synced (docs-watcher confirmed).
- Every commit ends with `Co-Authored-By: Claude <noreply@anthropic.com>`.

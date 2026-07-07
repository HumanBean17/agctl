# Design: Config Overlay (`--overlay`) + Runbook Sidecar

**Status:** Approved (design) — ready for implementation plan
**Date:** 2026-07-07
**Author:** brainstorming session
**Affects:** `agctl/config/loader.py`, `agctl/config/models.py`, `agctl/config/validator.py`, `agctl/cli.py`, `agctl/commands/config_commands.py`, the `discover` command, `skills/agctl-write-test-runbook/`, `skills/agctl-run-test-runbook/`, `skills/agctl-config/`
**Relation to docs:** **Changes the `agctl` config contract.** `DESIGN.md` §2 (config schema & discovery) and §5 (resolution pipeline) and `ARCHITECTURE.md` §5 (load pipeline) require updates — this is **not** a docs no-op. `docs-watcher` runs after implementation per CLAUDE.md "Docs Sync".

---

## 1. Background & Problem

`agctl` is driven by a single config file, **`agctl.yaml`** (hard-coded name; `--config` / `AGCTL_CONFIG` / walk-up discovery). Every HTTP template, Kafka pattern, DB connection/template, and mock lives in that one file. There is **no include / overlay / multi-file** support anywhere today — confirmed across `agctl/` and `docs/`.

This is fine for daily reusable scenarios. It turns into a "messy swamp" once **test runbooks** enter the picture. A runbook (`runbook.md`, authored by `agctl-write-test-runbook`) is pure markdown — it embeds `agctl` command lines + `Expected` pairs + `Capture` vars + fixture lifecycle, and references templates/mocks/seed-templates **by name**, resolved from `agctl.yaml`. So every fixture a runbook needs — a one-off seed `db write` template, an ad-hoc mock, a scratch HTTP template — has nowhere to live **except the shared monolith**. Consequences:

- No boundary between *reusable infra* (core services, common templates) and *runbook-specific fixtures*.
- The main file grows without bound as runbooks accumulate.
- Deleting a runbook orphans its templates in the monolith.
- A runbook is not self-contained: its fixtures live in a different file it can't carry with it.

The test-runbook design (archived `2026-07-04-test-runbook-skills-design.md`) deliberately rejected a **YAML runbook format** (§9) and a **runbook registry / config backend** (§4). This spec solves the fixture-bloat problem **without crossing either line**: the runbook stays markdown; we add a *config* mechanism — a **runbook sidecar** — layered on the main file at execution time.

> **Scope honesty (load-bearing).** This is a real `agctl` core change, unlike the runbook-skills spec which added only markdown procedures. It introduces a repeatable global `--overlay` flag and a deep-merge step in the load pipeline. It is scoped to serve runbook sidecars; it is **deliberately not** a general "split your monolith" system (no directory auto-include, no `AGCTL_OVERLAY` env — see Non-Goals). The flag is general-purpose *in mechanism* but added for one consumer; if a second need appears, it is already there.

## 2. Goals

- Let a runbook carry its own fixtures (seed templates, mocks, scratch HTTP templates) and per-runbook overrides as a **co-located sidecar config**, so runbook + fixtures are a committable, portable unit.
- Keep the main `agctl.yaml` as the home for **shared infra**; stop forcing runbook-specific entries into it.
- Preserve the runbook as **pure markdown** (no front-matter, no embedded config block) — honor prior design decisions §4 (no runbook registry/backend) and §9 (no YAML runbook format).
- Reuse the existing Pydantic `Config` schema and validator — no parallel fragment schema to design or drift.
- Make overrides **never silent**: every base key a sidecar replaces is a `config validate` warning.

## 3. Scope & Design Constraints

- **Layered, not standalone.** A sidecar layers on a main `agctl.yaml`; it is not a complete config on its own and does not run without a base. Shared infra (services, common templates) stays in main; runbook fixtures + local overrides live in the sidecar. A runbook is portable across repos/branches that share the base infra.
- **Full overlay fragment.** The sidecar is a partial `Config` — any top-level section — deep-merged onto the base with sidecar-wins semantics. Not a restricted "fixtures-only" sub-schema (rejected, §10).
- **Core `--overlay` flag.** Composition is a first-class load-time step, not a temp-file `config compose` dance (rejected, §10). The flag mirrors `--config`: global, repeatable, walks into every command.
- **Runbook stays pristine.** Sidecar discovery is by sibling filename convention + an explicit `--overlay` escape hatch. No runbook front-matter.
- **Reuses existing pipeline.** Each file (base + each overlay) is loaded through today's pipeline (`yaml.safe_load` → `${VAR}` interpolation → `AGCTL_*` env overrides → version guard → Pydantic validate); deep-merge happens on the typed `Config` objects; the cross-reference validator runs on the composed result.

## 4. Non-Goals

- **A runbook registry / persistence backend.** Honored from the prior design (§4). The sidecar is a file found by naming convention — no store, no executor, no new subsystem.
- **A YAML runbook format.** Honored from the prior design (§9). The runbook is still markdown; the YAML lives in a separate sidecar file.
- **A general "split the monolith" system.** No `agctl.d/` directory auto-include, no `include:`/`imports:` directives, no `AGCTL_OVERLAY` env var. The `--overlay` flag exists to serve runbook sidecars (and is available for ad-hoc use); a directory/config-organization layer is YAGNI until a second concrete need appears.
- **Changing the config schema version.** The overlay is a **load-time** composition, not a schema change. Existing v2 configs are unaffected; no `config migrate` work.
- **Restricting which sections a sidecar may contain.** Any section is permitted; the infra-vs-fixture boundary is a **convention**, enforced by warnings on override, not by a sub-schema.

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Runbook sidecar = co-located config fragment, layered on main.** A file `<runbook-base>.agctl.yaml` next to the runbook, deep-merged onto the discovered base at load time. | Solves fixture-bloat directly. The runbook carries its fixtures as a committable unit; deleting the runbook deletes its fixtures (no orphans). Stays on the right side of §4 (no registry — it's a file by convention) and §9 (no YAML runbook — the runbook is still markdown; the YAML is a separate file). |
| D2 | **Full overlay fragment (any section), not fixtures-only.** The sidecar validates as a partial `Config` — `services`, `kafka`, `database`, `templates`, `defaults`, `mocks` all permitted. | "Fixture" is fuzzy: sometimes a mock, sometimes a seed `db write` template, sometimes a scratch HTTP template. A restricted sub-schema (rejected, §10) would fight that reality and require ongoing line-drawing. Reusing the full schema is zero new schema surface. |
| D3 | **Sidecar-wins deep-merge; every override is a `validate` WARNING.** Dict sections merge key-by-key (sidecar wins on collision = an *override*); scalar/list leaves are replaced; type mismatches are hard errors. | Flexibility (a runbook can redirect a service to a mock URL, override a base template) without silent clobbering. The warning set is the safety net for D2's permissiveness. |
| D4 | **Core `--overlay <path>` flag (global, repeatable), not a `config compose` temp-file command.** `load_config` deep-merges base + ordered overlays; `--overlay` mirrors `--config` and threads into every command. | First-class: `validate`/`show`/`discover` reason about the composed config natively; env interpolation stays fresh per invocation (a baked-once temp file goes stale if env changes mid-run, and writes merged secrets to a temp location). The run skill stays declarative — it passes a flag, not a merge procedure. Scoped to serve runbooks; general-directory config is a separate, deferred decision (Non-Goals). |
| D5 | **Partial-`Config` model for overlay loading.** An overlay parses against `Config` with every field optional (including `version`, which is inherited from base). | Reuses the existing Pydantic models; an overlay is definitionally a *fragment*. If an overlay declares `version`, its major must equal the base's (`2`) or load fails — guarding against accidental cross-version overlays. |
| D6 | **Discovery by sibling convention `<runbook-base>.agctl.yaml`, plus an explicit `--overlay` escape hatch; no runbook front-matter.** | Zero-config for the common case; the reserved `.agctl.yaml` extension is unambiguous and greppable. The escape hatch covers non-conventional locations. Keeping front-matter out of the runbook preserves §9 and keeps the markdown diff-able and pristine. |
| D7 | **The run skill injects `--overlay <sidecar>` into every `agctl` subprocess it issues; the runbook records the sidecar name in `Preconditions`.** | The sidecar must be in effect for the whole run — every `http`/`db`/`kafka` step and every `mock run/start/stop/status`. Preconditions disclosure keeps a human copy-pasting a single step honest (they see the overlay dependency). |
| D8 | **Cross-file cross-reference validation on the composed config; override and mock-shadow warnings extended across files.** | A sidecar HTTP template may legitimately reference a base `service`; a sidecar mock may shadow a base mock. The existing `validator.py` runs on the composed result, so dangling refs, write-on-readonly, and shadowing are all caught across the base/overlay boundary. |
| D9 | **No `AGCTL_OVERLAY` env var; no directory auto-include.** | The runbook sidecar is the only consumer. Both are trivial to add later; adding them now is YAGNI and blurs the "not a general splitting system" scope line. |

## 6. Core: Overlay Loading & Merge

**Load pipeline (as-built today, then extended):** `discover_config_path` → `yaml.safe_load` → `${VAR}` interpolation → `AGCTL_*` env overrides → `_check_version(major=="2")` → `Config.model_validate`. *(Sources: `loader.py` discover:152 / load:182 / version guard:196; `resolver.py` env deep-merge; `ARCHITECTURE.md` §5.)*

**Extension (design-level — no algorithm bodies here):**

1. **Base load** — unchanged. The discovered (or `--config`-given) file runs the full pipeline into a `Config`.
2. **Overlay load** — each `--overlay <path>` (in flag order) runs the **same** pipeline but validates against the **partial-`Config`** model (D5): all sections optional, `version` optional. Each overlay becomes its own typed partial-`Config`.
3. **Deep-merge** — overlays fold onto the base in order; the result is a `Config`. Merge contract:
   - **Dict sections** (`services`, `kafka.patterns`, `database.connections`, `database.templates`, `templates`, `mocks.http.stubs`, `mocks.kafka.reactors`): key-by-key; sidecar key wins on collision → recorded as an **override**.
   - **Scalar / list leaves** (`kafka.brokers`, `defaults.*`, a stub's `response`, etc.): sidecar value **replaces** base value (a scalar/list override is also recorded).
   - **`version`:** inherited from base; an overlay-declared `version` must match the base major or load fails.
   - **Type mismatch** on a merged key: composed result fails `Config.model_validate` → hard error (exit 2).
4. **Cross-reference validation** — `validator.validate_config` runs on the **composed** `Config` (D8), returning `(errors, warnings)`; override set from step 3 is appended to warnings.
5. **Precedence** — `AGCTL_*` env overrides remain highest (applied per-file in step 1/2, as today), so composed precedence matches current behavior with overlays inserted between base and env.

**Overlay fragment contract (example):**

```yaml
# runbooks/checkout.agctl.yaml — overlay for checkout.md
# All sections optional; version inherited from base agctl.yaml.
templates:
  create-order-test:            # scratch HTTP template, this runbook only
    method: POST
    service: orders
    path: /orders
    body: '{"customer_id":"${TEST_CUSTOMER}","marker":"checkout-runbook"}'
database:
  templates:
    seed-checkout-cart:         # seed data, this runbook only
      connection: writable
      mode: write
      sql: "INSERT INTO carts (...) VALUES (...) ON CONFLICT DO NOTHING;"
mocks:
  http:
    stubs:
      checkout-payment-svc:     # mock the payment dep for this runbook
        listen: { port: 18080 }
```

If the base also defines `templates.create-order` and the sidecar redefines it, `validate --overlay` emits one WARNING (`override: templates.create-order`) and the sidecar value wins.

## 7. CLI Surface

- **`--overlay <path>`** — new global option on the root group (`cli.py`, mirroring `--config` at `cli.py:111`); repeatable; stored on the context and threaded into `load_config_or_raise` (`command.py:70`) alongside `config_path`. Order = application order; later wins.
- **`config validate`** — accepts `--overlay`; validates the **composed** config (partial-`Config` parse + deep-merge + cross-ref validator + override/shadow warnings).
- **`config show`** — accepts `--overlay`; renders the composed config with overridden keys marked.
- **`discover`** — accepts `--overlay`; lists templates/patterns/mocks/seed-templates as the run would see them (base + sidecar), so authoring grounds in the composed surface.
- **Exit codes unchanged:** `0` success, `1` assertion fail (n/a here), `2` config/tool/env error (overlay parse failure, version mismatch, type mismatch, dangling ref).

## 8. Runbook Sidecar Convention & Skill Changes

**File model:** `runbooks/checkout.md` → `runbooks/checkout.agctl.yaml` (sibling, reserved extension). Absent sidecar ⇒ today's behavior (base only). Present ⇒ applied as overlay.

**Skill changes (design-level):**

- **`agctl-write-test-runbook`** — in the *Design* step, after `agctl discover`, any needed template/mock/seed-template **not in main** is created in `<runbook>.agctl.yaml` instead of instructing the user to edit main. The skill emits the sidecar next to `runbook.md` and records its name in the runbook's **Preconditions** (e.g. `Requires overlay: checkout.agctl.yaml`). Runbook markdown gains only that one Preconditions line — no front-matter, no config block.
- **`agctl-run-test-runbook`** — *Validate* phase auto-discovers `<runbook-base>.agctl.yaml`; if present, runs `agctl config validate --overlay <sidecar>` (surfacing override warnings) and injects `--overlay <sidecar>` into **every** `agctl` subprocess it issues, including `mock run/start/stop/status`. Execution semantics otherwise unchanged.
- **`agctl-config`** — documents that runbook fixtures live in sidecars (not main); `validate`/`show`/`init` guidance gains `--overlay` awareness. The packaged `sample-config.yaml` and the runbook template are **unchanged** — sidecar is opt-in, existing drift-guard tests stay green.

**Runbook purity preserved:** the markdown changes by one `Preconditions` line naming the sidecar. §4 (no registry) and §9 (no YAML runbook) remain intact.

## 9. Validation & Error Handling

- **Hard errors (exit 2):** overlay fails partial-`Config` parse; overlay-declared `version` major ≠ base; merged key type mismatch; dangling cross-reference (name resolves in neither base nor overlay).
- **Warnings:** every override (`override: <path>`); mock shadowing (existing warning, extended cross-file).
- **`config validate --overlay`** is the authoring-time and pre-run check; the run skill runs it in Setup and surfaces warnings in the report.

## 10. Rejected Alternatives (ADR-style)

- **Fixtures-only, extend-only sidecar (approach B).** Rejected (D2): "fixture" is fuzzy (HTTP template? mock? seed?), and restricting sections is ongoing design tax. A restricted sub-schema also can't redirect a service for one runbook without editing main. Full fragment + override warnings gives the same safety with less mechanism.
- **Full fragment, extend-only by default + opt-in override (approach C).** Rejected (D2/D3): A's scope with a policy flag and per-section semantics — more to spec and test for knobs rarely reached. The validate warning on every override already makes clobbering non-silent.
- **`config compose --overlay … -o merged.yaml` temp-file command.** Rejected (D4): a temp-file dance writes merged secrets to a temp location, bakes env interpolation once (stale if env changes mid-run), and forces `validate`/`discover` during a run to each re-derive the merged file. A core `--overlay` flag is cleaner and keeps the run skill declarative.
- **Runbook front-matter declaring the sidecar.** Rejected (D6): would change the runbook format (a §9-adjacent line) and harm diff-ability. Sibling convention + `--overlay` flag covers discovery without touching the markdown contract.
- **`AGCTL_OVERLAY` env var + directory auto-include.** Rejected (D9): YAGNI; only the runbook sidecar needs this now, and a directory/auto-include layer blurs the "not a general splitting system" scope. Trivial to add later.
- **Standalone-capable sidecar (no base needed).** Rejected (§3): service URLs are infra; forcing each runbook to re-declare services duplicates and drifts. Layered-on-main is the realistic portability model.

## 11. Validation Strategy

- **Unit — deep-merge:** dict key-by-key merge with sidecar-wins; scalar/list leaf replacement; `version` inheritance + mismatch error; override-detection set correctness; type-mismatch → error.
- **Unit — overlay load:** partial-`Config` parses an all-optional fragment; rejects an overlay whose declared `version` major ≠ 2.
- **Integration — composed resolution:** base defines `service: orders` + `template: create-order`; sidecar adds a `mock`, a `db write` seed template, and **overrides** `create-order`. `validate --overlay` warns on the one override and passes; `show --overlay` reflects the composed tree; real `agctl http call create-order` / `db execute` / `mock run` resolve names spanning both files.
- **Integration — runbook run:** the run skill executes a runbook with a sidecar end-to-end (`AGCTL_TEST_LIVE=1`); commands carry `--overlay`; override warning surfaces in setup; `$VAR` Capture still flows; Setup/Teardown still run.
- **Drift guards:** `sample-config.yaml` and `runbook-template.md` byte-unchanged; a runbook with **no** sidecar runs exactly as today.
- **Edge:** sidecar present but a command invoked as plain `agctl` (no runner, no `--overlay`) ⇒ base only, sidecar ignored — documented as expected (the sidecar is runbook-scoped, activated by the runner or an explicit flag).

After implementation, `docs-watcher` runs (CLAUDE.md "Docs Sync") — **expected real updates** to `DESIGN.md` §2/§5 and `ARCHITECTURE.md` §5 (new `--overlay` flag, partial-`Config`, sidecar convention), plus the `skills/` tree growth.

## 12. Docs & Skill Impact

- **`DESIGN.md`** §2 (config schema & discovery) and §5 (resolution pipeline): document `--overlay`, the partial-`Config` overlay, deep-merge contract, and override warnings.
- **`ARCHITECTURE.md`** §5 (load pipeline): add the overlay-load + deep-merge + composed-validation steps to the as-built pipeline diagram.
- **`skills/` tree:** `agctl-write-test-runbook` and `agctl-run-test-runbook` gain sidecar authoring/execution steps; `agctl-config` gains `--overlay` awareness. All three ship in `skills/` as portable consumer artifacts.
- **No config schema version bump; no `config migrate` work.** The overlay is load-time composition over the existing v2 schema.

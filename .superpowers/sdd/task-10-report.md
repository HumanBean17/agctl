# Task 10 Report: Config Overlay + Runbook Sidecar Documentation Sync

## Summary

Config overlay and runbook sidecar feature documentation synced across DESIGN.md, ARCHITECTURE.md, and skills/agctl/SKILL.md. All changes reflect the as-built implementation on branch `config-overlay-sidecar`.

## Files Changed

### `/Users/dmitry/Desktop/CursorProjects/agenttest/docs/DESIGN.md`

**§3 (CLI Command Design) — Global flags table:**
- Added `--overlay <path>` row: "Overlay config fragment (repeatable; later wins); layered on base config"

**§2.4 (Configuration Schema) — New section:**
- Added complete overlay documentation covering:
  - Overlay syntax (partial config with optional version)
  - Usage via `--overlay <path>` (repeatable)
  - Runbook sidecar convention (`<runbook-base>.agctl.yaml`)
  - Merge behavior (sidecar-wins: recursive dict merge, scalar/list replace, type clash → error)
  - Override tracking in `config validate --overlay`
  - Precedence: base < overlays < AGCTL_* env vars
  - Schema version note (not a version bump)

**§5 (Configuration Resolution Order):**
- Updated precedence chain from 5 to 6 steps
- Inserted overlays as step 5: `--overlay <path>` flags after interpolation, before env overrides
- Clarified that overlays are applied in flag order (later wins)

**§3.6 (`agctl config` — Config Introspection):**
- Updated `config validate` command: added `--overlay <path>` flag documentation
- Updated `config show` command: added `--overlay <path>` flag documentation
- Documented output shape changes: `config show --overlay` emits `{"config": ..., "overrides": [...]}`; back-compat form without overlays

**§3.7 (`agctl discover` — Lazy Scoped Discovery):**
- Added section intro note: "All `discover` commands accept `--overlay <path>`"
- Updated all four discover invocation forms:
  - Level 0 (summary): `agctl discover [--overlay <path>]`
  - Level 1 (category): `agctl discover --category <name> [--overlay <path>]`
  - Level 2 (item): `agctl discover --category <name> --name <item-name> [--overlay <path>]`
  - Search: `agctl discover --search <term> [--overlay <path>]`

**§3.5 (`agctl mock` — Mock Server):**
- Updated `mock run` command: added `--overlay <path>` flag
- Updated `mock start` command: added `--overlay <path>` flag with note "forwarded to the daemon"

**§4.2 (Result Shapes by Command Group):**
- Updated `config.validate` output: documented `warnings` array with override records
- Updated `config.show` output: documented branch result shape with `{"config": ..., "overrides": [...]}`

### `/Users/dmitry/Desktop/CursorProjects/agenttest/docs/ARCHITECTURE.md`

**§5 (Configuration Pipeline):**
- Extended as-built pipeline diagram with overlay loop:
  - After base interpolation and `_check_version`
  - For each overlay: discover (explicit only) → interpolate → `PartialConfig.model_validate` → version-major-match check → `deep_merge`
  - Then `apply_env_overrides` on MERGED dict (AGCTL_* wins over overlays)
  - Finally `Config.model_validate` (final)

- Added "Overlay types" subsection:
  - Documented `PartialConfig` (Config with optional version)
  - Documented `ComposedConfig` NamedTuple (config + overrides list)
  - Documented override record shape: `{"path": "<dotted>", "overlay": "<file>"}`

- Added "Deep merge" subsection:
  - Documented `deep_merge(base, overlay, overlay_name, overrides)` algorithm
  - Specified merge contract: sidecar-wins, recursive dict merge, scalar/list replace, override recording at leaves

### `/Users/dmitry/Desktop/CursorProjects/agenttest/skills/agctl/SKILL.md`

**Command forms section:**
- Updated global flags line from "Only `--config <path>` is global" to "`--config <path>` and `--overlay <path>` (repeatable) are global"

**Rationale:** The skill documents operational command surfaces. The global `--overlay` flag is part of that surface and agents need to know it exists when composing commands (especially when following runbook preconditions that require overlays).

## Altitude Preserved

All updates respect document altitude:
- **DESIGN.md** captures user-facing contract (WHAT/WHY): overlay syntax, precedence, CLI surface, output shapes
- **ARCHITECTURE.md** captures as-built implementation (HOW): pipeline stages, types, merge algorithm
- **skills/agctl/SKILL.md** captures operational surface: global flags an agent must know

No implementation details leaked into DESIGN.md, no user-facing contract leaked into ARCHITECTURE.md, and no design rationale leaked into skills/.

## Cross-References Verified

- DESIGN §2.4 overlays → DESIGN §3 global flags table → DESIGN §5 precedence order
- DESIGN §3.6 config commands → DESIGN §4.2 output shapes
- DESIGN §3.7 discover → references overlay support
- DESIGN §3.5 mock commands → overlay forwarding to daemon
- ARCHITECTURE §5 pipeline → references `compose_config`, `PartialConfig`, `ComposedConfig`, `deep_merge`
- skills/agctl → references global `--overlay` flag

## Code Review Verification

All documentation changes were verified against the as-built code:
- CLI flag registration in `agctl/cli.py`
- `PartialConfig` model in `agctl/config/models.py`
- `compose_config` pipeline in `agctl/config/loader.py`
- `deep_merge` algorithm in `agctl/config/loader.py`
- Command threading in `agctl/commands/*_commands.py`
- Output shape changes in `agctl/commands/config_commands.py`

## No Changes Intentionally Omitted

Assessed and found not requiring changes:
- `agctl/data/sample-config.yaml` — frozen by drift-guard test; out of scope for docs-watcher
- `.env.example` — not part of this feature
- Test files — out of scope for docs-watcher
- Archived specs — frozen history

## Conclusion

All three document families (DESIGN, ARCHITECTURE, skills/agctl) have been updated to reflect the config overlay and runbook sidecar feature at their respective altitudes. Documentation now matches the as-built implementation on branch `config-overlay-sidecar` (tip 4337f63).

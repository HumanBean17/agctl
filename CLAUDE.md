# agctl

Agentic CLI interface for system under test.

## Project Structure

[docs/](./docs) - documentation
  - [DESIGN.md](./docs/DESIGN.md) - project design doc (intent & spec)
  - [ARCHITECTURE.md](./docs/ARCHITECTURE.md) - as-built architecture (source of truth: module layout, runtime flow, extension points)
[skills/](./skills) - portable skill artifacts for agctl's *users*, not this project (consumers should copy this to `.claude/skills/`)

## Docs Sync

After completing any task that changes code or config, invoke the `docs-watcher` subagent to check whether DESIGN.md or ARCHITECTURE.md need syncing. The subagent preserves each doc's altitude: DESIGN.md captures user-facing contract (WHAT/WHY), ARCHITECTURE.md captures as-built implementation (HOW). A correct no-op is better than a speculative edit.

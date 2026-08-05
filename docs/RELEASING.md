# Releasing agctl

Releases are **tag-driven**. There is exactly one way a version reaches PyPI:
someone pushes a `v*` git tag, which triggers `.github/workflows/release.yml`.
That workflow builds the package, publishes to PyPI via **trusted publishing**
(OIDC — no stored tokens), regenerates `CHANGELOG.md` on `main`, and opens a
GitHub Release.

The package version is **derived from the tag** by `hatch-vcs` (see
`pyproject.toml` `[tool.hatch.version]`). Because the release fires *on* the tag
and the version is read *from* the tag, the two can never drift — there is no
version string in `pyproject.toml` to keep in sync.

## Versioning

[Semantic Versioning](https://semver.org/) + [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` → minor
- `fix:` / `perf:` / `refactor:` → patch
- `feat!:` or a `BREAKING CHANGE:` footer → **major**
- `docs:` / `chore:` / `ci:` / `test:` → no release / changelog entry

A PR-title lint (`.github/workflows/semantic-pr.yml`) enforces the format; the
PR title drives both the changelog entry and the bump.

From `3.0.0` the **stable surface** is: the CLI commands and flags, the
`agctl.yaml` runbook schema, and the plugin entry-point groups
(`agctl.db_drivers`, `agctl.plugins`, `agctl.assertions`, `agctl.logs_backends`).
A breaking change to any of these requires a major bump.

## One-time setup (do this before the first tag-driven release)

1. **PyPI trusted publishing.** On https://pypi.org → *Account settings →
   Publishing → Add a GitHub publisher* (and again on TestPyPI):
   - PyPI project name: `agctl`
   - Owner: `HumanBean17` · Repository: `agctl`
   - Workflow filename: `release.yml`
   - Environment name: `pypi`
   - The publisher is scoped to this single workflow + environment, so no other
     workflow or branch can publish.

2. **Branch protection on `main`.** Via `gh` or the repo settings:
   - Require the `tests` / `conventional-pr` status checks before merge.
   - Require linear history / PR merges (the release workflow assumes tags land
     on `main` commits).
   - Example:
     ```bash
     gh api -X PUT repos/HumanBean17/agctl/rulesets \
       -F name=main -F target=branch -F enforcement=active \
       -F 'conditions[ref_name][]=refs/heads/main' \
       ...
     ```

3. **Verify once on TestPyPI** (see *Dry-run* below) before the first real tag.

## Cutting a release

Only tag a commit that has **passed CI on `main`** (the workflow does not re-run
the suite — main's green status is the gate).

```bash
git checkout main && git pull
git tag v3.0.0          # version == tag, exactly
git push origin v3.0.0  # ← this fires release.yml
```

Then watch the **Actions** tab. The workflow logs the built version and asserts
it equals the tag before uploading. Expected order: build → `twine check` →
version-match → PyPI upload → `CHANGELOG.md` commit → GitHub Release.

A new **minor/patch** release is the same flow with `v3.1.0` / `v3.0.1`.

## Dry-run on TestPyPI (recommended before the first real release)

Validate the whole pipeline end-to-end without burning the real version:

```bash
# 1. locally: clean build + metadata check (no network)
rm -rf dist build && python3 -m build && python3 -m twine check dist/*

# 2. upload a THROWAWAY version to TestPyPI (never the real one — it's consumed)
TWINE_USERNAME=__token__ TWINE_PASSWORD=pypi-<testpypi-token> \
  python3 -m twine upload --repository testpypi dist/*

# 3. smoke-install in a clean venv
python3 -m venv /tmp/_smoke && /tmp/_smoke/bin/pip install --index-url https://test.pypi.org/simple/ agctl==<throwaway>
/tmp/_smoke/bin/agctl --version
```

For a full CI-side dry-run, temporarily point the `pypa/gh-action-pypi-publish`
step at TestPyPI (`with: repository-url: https://test.pypi.org/legacy/`) on a
throwaway tag, then revert.

## Fixing a bad release

PyPI file uploads are **irreversible** — a given `x.y.z` file can never be
re-uploaded (not after delete, not after yank). So:

- **Yank, don't delete or re-upload.** Yank via the PyPI project UI
  (*Manage → Releases → Yank*). A yanked file stays installable for exact pins
  (`agctl==x.y.z`) but is no longer served to unpinned `pip install agctl`.
  `twine` cannot yank or re-upload — those are not twine operations.
- **Never delete or move a tag** to retry a release. If `release.yml` failed
  before upload, nothing reached PyPI — fix the workflow and **re-run the
  workflow on the same tag**. If it failed *after* upload (CHANGELOG/Release
  steps), PyPI is already live; re-run just the failed steps.
- To ship a corrected release, **bump the version** (`v3.0.1`) and cut a new tag.

## Backfill note

`v2.1.0` / `v2.2.0` / `v2.3.0` were published to PyPI but never tagged. They are
backfilled at their bump commits purely for `git describe` / changelog history
consistency — the backfill touches nothing already on PyPI.

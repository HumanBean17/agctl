---
name: pypi-publish
description: Use when publishing a Python package to PyPI or TestPyPI with twine — build sdist+wheel, run twine check, upload, and verify the release. Also for completing pyproject.toml metadata before a first publish.
---

# Publish a Python package to PyPI via twine

PyPI publishes are **irreversible**: once version `x.y.z` is uploaded, that exact file can **never** be re-uploaded — not after delete, not after yank; only a new version number works. So verify everything you can *before* the upload.

For setup and edge cases — credentials, TestPyPI, metadata completion, the failures table, shell traps — read **`reference.md`** in this skill's directory.

## Flow

**1. Get name + version** from `pyproject.toml` `[project]`. `version` is the irreplaceable thing.

**2. Confirm the version is NOT already published** — the most common upload failure, and it reveals nothing fixable:
```bash
curl -s -o /dev/null -w "%{http_code}\n" "https://pypi.org/pypi/<pkg>/<ver>/json"
# 200 = ALREADY PUBLISHED → stop, ask user to bump version. 404 = free to publish.
```
(For a first publish, also check the project name isn't taken: drop `/<ver>` from the URL; 404 = name free.)

**3. Build** (clean first; builds from the **working tree**, not the last commit — uncommitted changes ship):
```bash
rm -rf dist build && python3 -m build
```

**4. `twine check dist/*`** — must pass cleanly. A `long_description missing` warning means a **blank PyPI page** for that version forever; fix `readme` in `pyproject.toml` first (see `reference.md`).

**5. Confirm with the user** before the irreversible upload — state the exact name + version, that it's not already on PyPI, the target repo, and that it can't be re-uploaded.

**6. Upload:**
```bash
twine upload dist/*                          # → PyPI
twine upload --repository testpypi dist/*    # → TestPyPI
```

**7. Verify it's actually live** (the JSON API lags a few seconds — don't trust the upload echo):
```bash
for i in 1 2 3 4 5; do
  c=$(curl -s -o /dev/null -w "%{http_code}" "https://pypi.org/pypi/<pkg>/<ver>/json")
  [ "$c" = 200 ] && { echo LIVE; break; }; sleep 4
done
```

Post-publish steps (git tag, GitHub Release) **only if the user asks** — don't commit or tag unprompted.

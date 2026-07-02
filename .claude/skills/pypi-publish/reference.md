# pypi-publish — reference

Detail loaded on demand. The core flow lives in `SKILL.md`.

## Prerequisites

```bash
python3 --version          # python3, not python (python is often absent on macOS)
python3 -m twine --version # twine 6+
python3 -m build --version # PEP 517 build frontend
```
Missing any → `python3 -m pip install --user --upgrade build twine`.

## Credentials

PyPI requires an **API token** (username `__token__`), not a password. Resolve in order:

1. **`~/.pypirc`** (recommended):
   ```ini
   [pypi]
   username = __token__
   password = pypi-<long-token>
   ```
   Add a second `[testpypi]` stanza for TestPyPI.
2. **Env vars** — `TWINE_USERNAME=__token__` + `TWINE_PASSWORD=pypi-<token>` (use `__token__` even for TestPyPI).
3. **Keyring** / interactive prompt as a last resort.

Never print the token; `twine upload` doesn't echo it.

## TestPyPI first?

For a package's **first-ever publish**, consider `--repository testpypi` to validate build, metadata, and install end-to-end. The real version number is still consumed, so use a throwaway version (`0.0.1` / `0.1.0.dev0`) on TestPyPI to avoid burning the real one. Skip for routine re-releases.

## Metadata (first publish especially)

For a non-blank, well-formed PyPI page, `[project]` in `pyproject.toml` should have:

```toml
readme = "README.md"        # WITHOUT this, the PyPI page renders BLANK
license = "MIT"             # SPDX expression (PEP 639)
authors = [{ name = "...", email = "..." }]
classifiers = [ ... ]       # Development Status, Python versions, Topic ...
[project.urls]
Homepage = "..."
Repository = "..."
```

**PEP 639 license gotcha:** use `license = "MIT"` (SPDX string) **and ship a `LICENSE` file**. Do **NOT** also add a `License :: OSI Approved :: MIT License` classifier — `License-Expression` + a license classifier are mutually exclusive and `twine check` fails. Pick one form (the SPDX string is the modern default).

If metadata is incomplete and the version will be locked in, surface it to the user *before* building — fixing it after publish needs a new version.

## Build notes

- **`python3`**, not `python` (macOS has no `python`).
- Don't `rm *.egg-info` with a glob — under `zsh` it errors `no matches found` if none exists. `rm -rf dist build` is enough; `python -m build` uses an isolated env and leaves no egg-info in the tree.
- Builds from the **working tree**, not the last commit. Tell the user what state is being packaged.

## twine check outcomes

- **PASSED** (green) → proceed.
- **PASSED with warnings** (`long_description missing`) → wire up `readme` before a first publish (blank page is permanent for that version).
- **FAILED** → fix and rebuild. Never upload a failed check.

## Verify (extended)

After the JSON API returns 200, expect **both** `*.whl` and `*.tar.gz`. Optional clean-venv smoke test:
```bash
python3 -m venv /tmp/_smoke && /tmp/_smoke/bin/pip install --quiet "<pkg>==<ver>"
/tmp/_smoke/bin/python -c "import <pkg>; print('import ok')"
```

## Post-publish (only if the user asks)

- `git tag v<ver> && git push origin v<ver>`
- GitHub Release from the tag.
- Commit metadata / `LICENSE` changes if not already committed.

## Failures → fixes

| Symptom | Cause | Fix |
|---|---|---|
| HTTP 400 `File already exists` | Version already on PyPI | Bump version; cannot re-upload |
| HTTP 403 / `Invalid or non-existent authentication` | No/wrong token | Fix `~/.pypirc` or `TWINE_*`; token must start `pypi-` |
| `twine check` FAILS, license conflict | `License-Expression` + license classifier both set | Drop the `License ::` classifier, keep `license = "SPDX"` |
| Blank PyPI page | no `readme = ` in `[project]` | Add it (needs a new version) |
| `command not found: python` | macOS has no `python` | Use `python3` |
| `no matches found: *.egg-info` | zsh glob over empty set | `rm -rf dist build` only |
| `long_description missing` warning | readme not wired | Add `readme = "README.md"` before publishing |

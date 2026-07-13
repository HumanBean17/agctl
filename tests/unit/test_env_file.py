"""Tests for `.env` auto-loading and explicit `--env-file` / `AGCTL_ENV_FILE` (DESIGN §2.2).

Covers:
- ``load_env_file`` parsing (quotes, comments, ``export`` prefix, bare keys, empty values).
- Real-environment-wins semantics (``.env`` provides defaults only).
- Auto-load of the ``.env`` sibling next to the resolved ``agctl.yaml``.
- Explicit sources: ``--env-file`` / ``AGCTL_ENV_FILE`` (missing -> ConfigError).
- Precedence: ``--env-file`` > ``AGCTL_ENV_FILE`` > sibling ``.env``.
- ``AGCTL_*`` overrides carried via ``.env``.
- Single interpolation engine: dotenv raw values + agctl's chained interpolator.
- CLI wiring (CliRunner): post-subcommand ``--env-file``, global flag, env var.
"""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from agctl.cli import cli
from agctl.config import ConfigError, load_config
from agctl.config.env_file import load_env_file


# --- minimal valid config with a single ${VAR} to resolve ------------------

_CFG = """version: "3"
services:
  s1:
    base_url: "${BASE}"
    health_path: /h
    timeout_seconds: 5
"""


def _write_cfg(d: Path, text: str = _CFG) -> Path:
    p = d / "agctl.yaml"
    p.write_text(text)
    return p


# --- load_env_file parsing --------------------------------------------------


def test_load_env_file_parses_common_forms(tmp_path):
    envf = tmp_path / ".env"
    envf.write_text(
        "# a comment\n"
        "\n"
        "PLAIN=value\n"
        "export EXPORTED=qux\n"  # `export` prefix stripped
        'QUOTED="hello world"\n'  # double quotes removed
        "EQUAL=empty\n"  # value with content
        "EMPTYVAL=\n"  # empty value -> ""
    )
    vals = load_env_file(envf, required=False)
    assert vals["PLAIN"] == "value"
    assert vals["EXPORTED"] == "qux"
    assert vals["QUOTED"] == "hello world"
    assert vals["EQUAL"] == "empty"
    assert vals["EMPTYVAL"] == ""


def test_load_env_file_drops_bare_keys(tmp_path):
    """A bare KEY (no '=') carries no value and is dropped (dotenv returns None)."""
    envf = tmp_path / ".env"
    envf.write_text("GOOD=1\nBARE\nALSO=2\n")
    vals = load_env_file(envf, required=False)
    assert vals == {"GOOD": "1", "ALSO": "2"}


def test_load_env_file_missing_required_raises(tmp_path):
    """An explicitly requested file that is missing -> ConfigError (mirrors --config)."""
    missing = tmp_path / "nope.env"
    with pytest.raises(ConfigError) as exc:
        load_env_file(missing, required=True)
    assert str(missing) in exc.value.message


def test_load_env_file_missing_optional_is_noop(tmp_path):
    """A missing sibling auto-load file is a silent no-op (normal)."""
    assert load_env_file(tmp_path / ".env", required=False) == {}


def test_dotenv_values_are_raw_not_expanded(tmp_path):
    """Headline invariant: dotenv_values is called with interpolate=False, so a
    value like ``FOO=a-${B}`` stays LITERAL in the env dict — agctl's own
    ``interpolate()`` owns all ``${...}`` resolution. This assertion FAILS if
    someone flips ``interpolate=False`` -> ``True`` (then vals["FOO"] == "a-b")."""
    envf = tmp_path / ".env"
    envf.write_text("B=b\nFOO=a-${B}\n")
    vals = load_env_file(envf, required=False)
    assert vals["FOO"] == "a-${B}"  # literal, NOT expanded


def test_malformed_utf8_env_file_raises_config_error(tmp_path):
    """A .env that isn't valid UTF-8 surfaces as ConfigError, not InternalError.
    UnicodeDecodeError is a ValueError (not an OSError), so it must be caught
    explicitly alongside OSError in load_env_file."""
    envf = tmp_path / "bad.env"
    envf.write_bytes(b"KEY=bad\xff\xfe\n")
    with pytest.raises(ConfigError):
        load_env_file(envf, required=True)


# --- pipeline semantics: real-env-wins, auto-load, explicit ----------------


def test_sibling_dotenv_provides_default(tmp_path):
    """A bare required ${VAR} resolves from the .env sitting next to agctl.yaml."""
    _write_cfg(tmp_path)
    (tmp_path / ".env").write_text("BASE=from-dotenv\n")
    cfg = load_config(str(tmp_path / "agctl.yaml"), env={})
    assert cfg.services["s1"].base_url == "from-dotenv"


def test_real_env_wins_over_dotenv(tmp_path):
    """Values already in the real env override .env (.env is defaults only)."""
    _write_cfg(tmp_path)
    (tmp_path / ".env").write_text("BASE=from-dotenv\n")
    cfg = load_config(str(tmp_path / "agctl.yaml"), env={"BASE": "from-real"})
    assert cfg.services["s1"].base_url == "from-real"


def test_missing_sibling_dotenv_is_silent(tmp_path):
    """No sibling .env -> no error; the bare ${VAR} simply stays unresolved."""
    _write_cfg(tmp_path)
    with pytest.raises(ConfigError) as exc:
        load_config(str(tmp_path / "agctl.yaml"), env={})
    assert "BASE" in exc.value.detail["variables"]


def test_bootstrap_var_in_dotenv_does_not_steer_resolution(tmp_path):
    """AGCTL_ENV_FILE set INSIDE the .env cannot redirect its own resolution
    (no cycle): resolution reads the real env BEFORE the dotenv merge, so only a
    real-env AGCTL_ENV_FILE is honored. Here the sibling .env tries to point at
    another file via AGCTL_ENV_FILE — it is ignored, and the sibling's own
    BASE wins."""
    _write_cfg(tmp_path)
    other = tmp_path / "other.env"
    other.write_text("BASE=from-other\n")
    (tmp_path / ".env").write_text("AGCTL_ENV_FILE=" + str(other) + "\nBASE=from-sibling\n")
    cfg = load_config(str(tmp_path / "agctl.yaml"), env={})
    assert cfg.services["s1"].base_url == "from-sibling"


def test_auto_load_is_sibling_only_no_parent_walk(tmp_path):
    """Auto-load looks ONLY at the .env next to the resolved config, not parent
    dirs (asymmetric with config discovery, which walks up). A .env in a parent
    dir is NOT loaded — BASE stays unresolved."""
    sub = tmp_path / "sub"
    sub.mkdir()
    _write_cfg(sub)
    (tmp_path / ".env").write_text("BASE=from-parent-dir\n")  # parent of config — ignored
    with pytest.raises(ConfigError) as exc:
        load_config(str(sub / "agctl.yaml"), env={})
    assert "BASE" in exc.value.detail["variables"]


def test_explicit_env_file_overrides_sibling(tmp_path):
    """--env-file (explicit) replaces the sibling auto-load, like --config replaces walk-up."""
    _write_cfg(tmp_path)
    (tmp_path / ".env").write_text("BASE=from-sibling\n")
    explicit = tmp_path / "explicit.env"
    explicit.write_text("BASE=from-explicit\n")
    cfg = load_config(str(tmp_path / "agctl.yaml"), env={}, env_file=str(explicit))
    assert cfg.services["s1"].base_url == "from-explicit"


def test_agctl_env_file_env_var(tmp_path):
    """AGCTL_ENV_FILE (real env) points at the .env to load."""
    _write_cfg(tmp_path)
    envf = tmp_path / "via-env.env"
    envf.write_text("BASE=via-env-var\n")
    cfg = load_config(str(tmp_path / "agctl.yaml"), env={"AGCTL_ENV_FILE": str(envf)})
    assert cfg.services["s1"].base_url == "via-env-var"


def test_precedence_explicit_beats_envvar_beats_sibling(tmp_path):
    """--env-file > AGCTL_ENV_FILE > sibling .env."""
    _write_cfg(tmp_path)
    (tmp_path / ".env").write_text("BASE=sibling\n")
    via_env = tmp_path / "via_env.env"
    via_env.write_text("BASE=env-var\n")
    explicit = tmp_path / "explicit.env"
    explicit.write_text("BASE=explicit\n")

    # explicit beats env var
    cfg = load_config(
        str(tmp_path / "agctl.yaml"),
        env={"AGCTL_ENV_FILE": str(via_env)},
        env_file=str(explicit),
    )
    assert cfg.services["s1"].base_url == "explicit"

    # env var beats sibling
    cfg = load_config(
        str(tmp_path / "agctl.yaml"), env={"AGCTL_ENV_FILE": str(via_env)}
    )
    assert cfg.services["s1"].base_url == "env-var"


def test_explicit_env_file_missing_raises(tmp_path):
    """A missing explicit --env-file is a user error -> ConfigError (not a no-op)."""
    _write_cfg(tmp_path)
    with pytest.raises(ConfigError):
        load_config(
            str(tmp_path / "agctl.yaml"), env={}, env_file=str(tmp_path / "nope.env")
        )


def test_missing_agctl_env_file_raises(tmp_path):
    """A missing AGCTL_ENV_FILE target is a user error -> ConfigError."""
    _write_cfg(tmp_path)
    with pytest.raises(ConfigError):
        load_config(
            str(tmp_path / "agctl.yaml"),
            env={"AGCTL_ENV_FILE": str(tmp_path / "nope.env")},
        )


# --- emergent: AGCTL_* overrides via .env, and single interpolation engine --


def test_agctl_override_via_dotenv(tmp_path):
    """An AGCTL_<SECTION>__<KEY> line in .env applies as an env override."""
    cfg_yaml = tmp_path / "agctl.yaml"
    cfg_yaml.write_text(
        'version: "3"\n'
        "services:\n"
        "  s1:\n"
        '    base_url: "http://x"\n'
        "    health_path: /h\n"
        "    timeout_seconds: 5\n"
        "defaults:\n"
        "  timeout_seconds: 10\n"
        "  database_connection: none\n"
    )
    (tmp_path / ".env").write_text("AGCTL_DEFAULTS__TIMEOUT_SECONDS=99\n")
    cfg = load_config(str(cfg_yaml), env={})
    assert cfg.defaults.timeout_seconds == 99


def test_single_interpolation_engine_chained(tmp_path):
    """dotenv's own ${VAR} expansion is OFF; agctl's chained interpolator owns it.

    .env: B=b, FOO=a-${B}  ->  raw 'a-${B}' enters env  ->  ${FOO} resolves to 'a-b'
    via agctl's multi-pass interpolate().
    """
    _write_cfg(tmp_path)
    (tmp_path / ".env").write_text("B=b\nFOO=a-${B}\n")
    # Switch the config to reference ${FOO} instead of ${BASE}.
    _write_cfg(tmp_path, _CFG.replace("${BASE}", "${FOO}"))
    cfg = load_config(str(tmp_path / "agctl.yaml"), env={"B": "b"})
    assert cfg.services["s1"].base_url == "a-b"


# --- CLI wiring ------------------------------------------------------------


def _valid(res) -> None:
    assert res.exit_code == 0, res.output


def test_cli_auto_load_sibling_dotenv(tmp_path):
    _write_cfg(tmp_path)
    (tmp_path / ".env").write_text("BASE=from-cli\n")
    res = CliRunner().invoke(
        cli, ["--config", str(tmp_path / "agctl.yaml"), "config", "validate"], env={}
    )
    _valid(res)


def test_cli_post_subcommand_env_file(tmp_path):
    """`agctl config validate --env-file X` works (per-command flag)."""
    _write_cfg(tmp_path)
    envf = tmp_path / "x.env"
    envf.write_text("BASE=from-flag\n")
    res = CliRunner().invoke(
        cli,
        [
            "--config",
            str(tmp_path / "agctl.yaml"),
            "config",
            "validate",
            "--env-file",
            str(envf),
        ],
        env={},
    )
    _valid(res)


def test_cli_global_env_file_flag(tmp_path):
    """`agctl --env-file X config validate` works (global flag before subcommand)."""
    _write_cfg(tmp_path)
    envf = tmp_path / "x.env"
    envf.write_text("BASE=from-global\n")
    res = CliRunner().invoke(
        cli,
        [
            "--env-file",
            str(envf),
            "--config",
            str(tmp_path / "agctl.yaml"),
            "config",
            "validate",
        ],
        env={},
    )
    _valid(res)


def test_cli_agctl_env_file_var(tmp_path):
    _write_cfg(tmp_path)
    envf = tmp_path / "x.env"
    envf.write_text("BASE=from-var\n")
    res = CliRunner().invoke(
        cli,
        ["--config", str(tmp_path / "agctl.yaml"), "config", "validate"],
        env={"AGCTL_ENV_FILE": str(envf)},
    )
    _valid(res)


def test_cli_real_env_wins(tmp_path):
    """Real env wins over .env at the CLI layer — verify the VALUE, not just
    exit-0 (exit-0 would also pass if .env clobbered the real env)."""
    _write_cfg(tmp_path)
    (tmp_path / ".env").write_text("BASE=from-file\n")
    res = CliRunner().invoke(
        cli,
        ["--config", str(tmp_path / "agctl.yaml"), "config", "show", "--unmask"],
        env={"BASE": "from-real-env"},
    )
    out = json.loads(res.output)
    assert out["result"]["services"]["s1"]["base_url"] == "from-real-env"


def test_cli_show_reflects_dotenv(tmp_path):
    _write_cfg(tmp_path)
    (tmp_path / ".env").write_text("BASE=shown\n")
    res = CliRunner().invoke(
        cli,
        ["--config", str(tmp_path / "agctl.yaml"), "config", "show", "--unmask"],
        env={},
    )
    out = json.loads(res.output)
    assert out["result"]["services"]["s1"]["base_url"] == "shown"

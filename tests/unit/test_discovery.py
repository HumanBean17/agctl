from pathlib import Path

import pytest

from agctl.config.loader import ConfigError, discover_config_path


def test_explicit_flag_wins(tmp_path):
    f = tmp_path / "agctl.yaml"
    f.write_text("version: '1'\n")
    assert discover_config_path(explicit=str(f)) == f


def test_explicit_missing_file_raises(tmp_path):
    with pytest.raises(ConfigError):
        discover_config_path(explicit=str(tmp_path / "nope.yaml"))


def test_agctl_config_env_used(tmp_path, monkeypatch):
    f = tmp_path / "cfg.yaml"
    f.write_text("version: '1'\n")
    monkeypatch.chdir(tmp_path)
    assert discover_config_path(env={"AGCTL_CONFIG": str(f)}) == f


def test_agctl_config_ignored_when_explicit(tmp_path, monkeypatch):
    explicit = tmp_path / "a.yaml"
    explicit.write_text("version: '1'\n")
    other = tmp_path / "b.yaml"
    other.write_text("version: '1'\n")
    assert discover_config_path(explicit=str(explicit), env={"AGCTL_CONFIG": str(other)}) == explicit


def test_walk_up_finds_agctl_yaml(tmp_path, monkeypatch):
    root = tmp_path / "proj"
    sub = root / "src" / "deep"
    sub.mkdir(parents=True)
    cfg = root / "agctl.yaml"
    cfg.write_text("version: '1'\n")
    monkeypatch.chdir(sub)
    assert discover_config_path(env={}) == cfg


def test_no_config_found_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ConfigError):
        discover_config_path(env={})

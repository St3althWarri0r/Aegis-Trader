"""``poseidon config validate`` reports (and warns about) the AI model.

AIConfig accepts any model string (a local/openai_compatible id is
legitimate), so a config pointed at a since-removed Anthropic id validates
fine at the pydantic layer and would otherwise 400 on every completion with
nothing here to say why. cmd_config now always prints the configured model
and warns (never fails) when it names a known-broken Anthropic id.
"""

from __future__ import annotations

import argparse

import yaml

from poseidon import cli


def _args(config_path) -> argparse.Namespace:
    return argparse.Namespace(config_action="validate", config=str(config_path))


def test_validate_prints_the_ai_model(tmp_path, capsys) -> None:
    config_path = tmp_path / "poseidon.yaml"
    config_path.write_text(yaml.safe_dump({"ai": {"model": "claude-sonnet-5"}}), encoding="utf-8")

    rc = cli.cmd_config(_args(config_path))

    assert rc == 0
    out = capsys.readouterr().out
    assert "ai_model=claude-sonnet-5" in out
    assert "WARNING" not in out


def test_validate_warns_on_a_known_broken_anthropic_model(tmp_path, capsys) -> None:
    config_path = tmp_path / "poseidon.yaml"
    config_path.write_text(
        yaml.safe_dump({"ai": {"utility_model": "claude-haiku-4-5-20251001"}}), encoding="utf-8"
    )

    rc = cli.cmd_config(_args(config_path))

    # Non-fatal: still reports valid.
    assert rc == 0
    err = capsys.readouterr().err
    assert "WARNING: ai.utility_model=claude-haiku-4-5-20251001" in err


def test_validate_does_not_warn_for_a_local_backend_model(tmp_path, capsys) -> None:
    # A local/openai_compatible model id is legitimate and must never be
    # gated against the Anthropic-only broken-model list.
    config_path = tmp_path / "poseidon.yaml"
    config_path.write_text(
        yaml.safe_dump({"ai": {
            "backend": "openai_compatible",
            "base_url": "http://localhost:1234/v1",
            "model": "claude-haiku-4-5-20251001",  # same string, different backend
        }}),
        encoding="utf-8",
    )

    rc = cli.cmd_config(_args(config_path))

    assert rc == 0
    err = capsys.readouterr().err
    assert "WARNING" not in err

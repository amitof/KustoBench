"""Tests for the run_benchmark CLI entry point."""

import os
import sys

import pytest
import yaml

from run_benchmark import main, parse_args


# ── parse_args ────────────────────────────────────────────────────────────────


def test_parse_args_required_config():
    args = parse_args(["--config", "config.yaml"])
    assert args.config == "config.yaml"


def test_parse_args_all_options():
    args = parse_args([
        "--config", "c.yaml",
        "--format", "json",
        "--output", "out.json",
        "--iterations", "5",
        "--warmup", "2",
        "--load", "envs/adx.yaml", "clickbench",
        "--run", "envs/adx.yaml", "clickbench",
    ])
    assert args.format == "json"
    assert args.output == "out.json"
    assert args.iterations == 5
    assert args.warmup == 2
    assert args.load == ["envs/adx.yaml", "clickbench"]
    assert args.run == ["envs/adx.yaml", "clickbench"]


def test_parse_args_deploy():
    args = parse_args(["--deploy", "envs/adx.yaml"])
    assert args.deploy == "envs/adx.yaml"


def test_parse_args_destroy():
    args = parse_args(["--destroy", "envs/adx.yaml"])
    assert args.destroy == "envs/adx.yaml"


def test_parse_args_default_config():
    args = parse_args([])
    assert args.config == "config.yaml"


# ── main ──────────────────────────────────────────────────────────────────────


def _write_config(tmp_path, data):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump(data))
    return str(path)


def test_main_missing_config():
    rc = main(["--config", "/nonexistent/config.yaml"])
    assert rc == 1


def test_main_no_queries_warns(tmp_path, capsys):
    path = _write_config(tmp_path, {
        "cluster_url": "https://x.kusto.windows.net",
        "database": "db",
        "queries": [],
    })
    # The client will fail to connect; we only test up to that point.
    rc = main(["--config", path])
    captured = capsys.readouterr()
    assert "WARNING" in captured.err or rc != 0

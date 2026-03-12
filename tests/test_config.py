"""Tests for benchmark.config."""

import os
import pytest
import yaml

from benchmark.config import load_config, _deep_merge, DEFAULT_CONFIG


# ── Helpers ──────────────────────────────────────────────────────────────────


def write_yaml(tmp_path, data: dict):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump(data))
    return str(path)


# ── _deep_merge ───────────────────────────────────────────────────────────────


def test_deep_merge_basic():
    base = {"a": 1, "b": {"c": 2, "d": 3}}
    override = {"b": {"c": 99}}
    result = _deep_merge(base, override)
    assert result == {"a": 1, "b": {"c": 99, "d": 3}}


def test_deep_merge_adds_keys():
    base = {"a": 1}
    override = {"b": 2}
    result = _deep_merge(base, override)
    assert result == {"a": 1, "b": 2}


def test_deep_merge_does_not_mutate_base():
    base = {"a": {"b": 1}}
    override = {"a": {"b": 2}}
    _deep_merge(base, override)
    assert base["a"]["b"] == 1


# ── load_config ───────────────────────────────────────────────────────────────


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config("/does/not/exist.yaml")


def test_load_config_merges_defaults(tmp_path):
    data = {"cluster_url": "https://example.kusto.windows.net", "database": "mydb"}
    path = write_yaml(tmp_path, data)
    config = load_config(path)
    assert config["cluster_url"] == "https://example.kusto.windows.net"
    assert config["database"] == "mydb"
    # Defaults should still be present
    assert config["benchmark"]["iterations"] == DEFAULT_CONFIG["benchmark"]["iterations"]


def test_load_config_overrides_benchmark(tmp_path):
    data = {"cluster_url": "https://x.kusto.windows.net", "database": "db",
            "benchmark": {"iterations": 10}}
    path = write_yaml(tmp_path, data)
    config = load_config(path)
    assert config["benchmark"]["iterations"] == 10
    # Other defaults still intact
    assert config["benchmark"]["warmup_iterations"] == DEFAULT_CONFIG["benchmark"]["warmup_iterations"]


def test_load_config_env_override_cluster(tmp_path, monkeypatch):
    data = {"cluster_url": "https://original.kusto.windows.net", "database": "db"}
    path = write_yaml(tmp_path, data)
    monkeypatch.setenv("KUSTO_CLUSTER_URL", "https://override.kusto.windows.net")
    config = load_config(path)
    assert config["cluster_url"] == "https://override.kusto.windows.net"


def test_load_config_env_override_database(tmp_path, monkeypatch):
    data = {"cluster_url": "https://x.kusto.windows.net", "database": "original"}
    path = write_yaml(tmp_path, data)
    monkeypatch.setenv("KUSTO_DATABASE", "override_db")
    config = load_config(path)
    assert config["database"] == "override_db"


def test_load_config_env_auth(tmp_path, monkeypatch):
    data = {"cluster_url": "https://x.kusto.windows.net", "database": "db"}
    path = write_yaml(tmp_path, data)
    monkeypatch.setenv("KUSTO_CLIENT_ID", "my-client-id")
    monkeypatch.setenv("KUSTO_CLIENT_SECRET", "my-secret")
    monkeypatch.setenv("KUSTO_TENANT_ID", "my-tenant")
    config = load_config(path)
    assert config["auth"]["client_id"] == "my-client-id"
    assert config["auth"]["client_secret"] == "my-secret"
    assert config["auth"]["tenant_id"] == "my-tenant"


def test_load_config_queries(tmp_path):
    data = {
        "cluster_url": "https://x.kusto.windows.net",
        "database": "db",
        "queries": [
            {"name": "q1", "query": "T | count"},
            {"name": "q2", "query": "T | take 10"},
        ],
    }
    path = write_yaml(tmp_path, data)
    config = load_config(path)
    assert len(config["queries"]) == 2
    assert config["queries"][0]["name"] == "q1"

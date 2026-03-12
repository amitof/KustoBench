"""Tests for benchmark.setup."""

import pytest
from unittest.mock import MagicMock, call

from benchmark.setup import run_setup, _drop_table, _create_table, _ingest_files, _resolve_parallelism


# ── Helpers ──────────────────────────────────────────────────────────────────


def _mock_client(total_cores=16):
    client = MagicMock()
    client.execute_control.return_value = {"row_count": 0, "column_count": 0}
    client.get_cluster_info.return_value = {
        "cluster_url": "https://test.kusto.windows.net",
        "nodes": 4,
        "cores_per_node": total_cores // 4 if total_cores else None,
        "total_cores": total_cores,
        "memory_gb_per_node": 64.0,
    }
    return client


def _make_config(table_name="hits", schema=".create table hits (Id: int)",
                 fmt="parquet", files=None):
    if files is None:
        files = [{"url": "https://example.com/data_0.parquet"}]
    return {
        "dataset": {
            "name": "test",
            "description": "test dataset",
            "schema": schema,
            "data": {
                "table_name": table_name,
                "format": fmt,
                "files": files,
            },
        },
        "queries": [],
    }


# ── run_setup ────────────────────────────────────────────────────────────────


def test_run_setup_full_flow():
    client = _mock_client()
    config = _make_config(files=[
        {"url": "https://example.com/data_0.parquet"},
        {"url": "https://example.com/data_1.parquet"},
    ])
    run_setup(client, config)
    # Should have 4 calls: drop + create + 2 ingests
    assert client.execute_control.call_count == 4


def test_run_setup_no_dataset():
    client = _mock_client()
    with pytest.raises(ValueError, match="No dataset"):
        run_setup(client, {"queries": []})


def test_run_setup_empty_schema():
    client = _mock_client()
    config = _make_config(schema="")
    with pytest.raises(ValueError, match="schema is empty"):
        run_setup(client, config)


def test_run_setup_no_table_name():
    client = _mock_client()
    config = _make_config(table_name="")
    with pytest.raises(ValueError, match="table_name"):
        run_setup(client, config)


# ── _drop_table ──────────────────────────────────────────────────────────────


def test_drop_table():
    client = _mock_client()
    _drop_table(client, "hits")
    client.execute_control.assert_called_once()
    cmd = client.execute_control.call_args[0][0]
    assert ".drop table" in cmd
    assert "hits" in cmd
    assert "ifexists" in cmd


# ── _create_table ────────────────────────────────────────────────────────────


def test_create_table():
    client = _mock_client()
    schema = "// Comment line\n.create table hits (Id: int)"
    _create_table(client, schema)
    client.execute_control.assert_called_once()
    cmd = client.execute_control.call_args[0][0]
    assert ".create table" in cmd
    assert "//" not in cmd  # comments stripped


def test_create_table_empty_after_strip():
    client = _mock_client()
    with pytest.raises(ValueError, match="no commands"):
        _create_table(client, "// only comments")


# ── _ingest_files ────────────────────────────────────────────────────────────


def test_ingest_files():
    client = _mock_client()
    files = [
        {"url": "https://example.com/hits_0.parquet"},
        {"url": "https://example.com/hits_1.parquet"},
    ]
    _ingest_files(client, "hits", "parquet", files)
    assert client.execute_control.call_count == 2
    for c in client.execute_control.call_args_list:
        cmd = c[0][0]
        assert ".ingest into table" in cmd
        assert "format='parquet'" in cmd


def test_ingest_files_parallel():
    client = _mock_client()
    files = [{"url": f"https://example.com/hits_{i}.parquet"} for i in range(6)]
    _ingest_files(client, "hits", "parquet", files, parallelism=3)
    assert client.execute_control.call_count == 6


def test_ingest_files_empty_list():
    client = _mock_client()
    _ingest_files(client, "hits", "parquet", [])
    assert client.execute_control.call_count == 0


def test_ingest_files_skips_empty_url():
    client = _mock_client()
    files = [{"url": ""}, {"url": "https://example.com/hits_0.parquet"}]
    _ingest_files(client, "hits", "parquet", files)
    assert client.execute_control.call_count == 1


# ── _resolve_parallelism ─────────────────────────────────────────────────────


def test_resolve_parallelism_uses_factor():
    client = _mock_client(total_cores=16)
    # 0.75 * 16 = 12
    assert _resolve_parallelism(client, {"ingestion_parallelism_factor": 0.75}) == 12


def test_resolve_parallelism_default_factor():
    client = _mock_client(total_cores=16)
    # default factor is 0.75 → 12
    assert _resolve_parallelism(client, {}) == 12


def test_resolve_parallelism_fallback_no_cores():
    client = _mock_client()
    client.get_cluster_info.return_value = {
        "cluster_url": "https://test.kusto.windows.net",
        "nodes": None, "cores_per_node": None,
        "total_cores": None, "memory_gb_per_node": None,
    }
    assert _resolve_parallelism(client, {}) == 1


def test_resolve_parallelism_min_one():
    client = _mock_client(total_cores=1)
    # 0.75 * 1 = 0.75 → int = 0 → clamped to 1
    assert _resolve_parallelism(client, {"ingestion_parallelism_factor": 0.75}) == 1

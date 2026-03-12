"""Tests for kustobench.config."""

import json

import pytest

from kustobench.config import BenchmarkConfig, QueryConfig

# ---------------------------------------------------------------------------
# QueryConfig tests
# ---------------------------------------------------------------------------

class TestQueryConfig:
    def test_valid_creation(self):
        q = QueryConfig(name="test", query="T | count", database="db")
        assert q.name == "test"
        assert q.iterations == 3  # default
        assert q.warmup_iterations == 1  # default

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="name"):
            QueryConfig(name="", query="T | count", database="db")

    def test_empty_query_raises(self):
        with pytest.raises(ValueError, match="[Qq]uery"):
            QueryConfig(name="q", query="", database="db")

    def test_empty_database_raises(self):
        with pytest.raises(ValueError, match="[Dd]atabase"):
            QueryConfig(name="q", query="T | count", database="")

    def test_zero_iterations_raises(self):
        with pytest.raises(ValueError, match="iterations"):
            QueryConfig(name="q", query="T | count", database="db", iterations=0)

    def test_negative_warmup_raises(self):
        with pytest.raises(ValueError, match="warmup"):
            QueryConfig(name="q", query="T | count", database="db", warmup_iterations=-1)


# ---------------------------------------------------------------------------
# BenchmarkConfig tests
# ---------------------------------------------------------------------------

QUERY = {"name": "q", "query": "T | count", "database": "db"}


class TestBenchmarkConfig:
    def _minimal(self, **kwargs) -> BenchmarkConfig:
        defaults = dict(cluster_uri="https://mycluster.kusto.windows.net", queries=[QUERY])
        defaults.update(kwargs)
        return BenchmarkConfig.from_dict(defaults)

    def test_valid_creation(self):
        cfg = self._minimal()
        assert cfg.cluster_uri == "https://mycluster.kusto.windows.net"
        assert len(cfg.queries) == 1

    def test_empty_cluster_raises(self):
        with pytest.raises(ValueError, match="cluster_uri"):
            self._minimal(cluster_uri="")

    def test_no_queries_raises(self):
        with pytest.raises(ValueError, match="query"):
            BenchmarkConfig.from_dict(
                {"cluster_uri": "https://c.kusto.windows.net", "queries": []}
            )

    def test_invalid_auth_raises(self):
        with pytest.raises(ValueError, match="auth_method"):
            self._minimal(auth_method="unknown")

    def test_invalid_output_format_raises(self):
        with pytest.raises(ValueError, match="output_format"):
            self._minimal(output_format="xml")

    def test_app_key_missing_fields_raises(self):
        with pytest.raises(ValueError, match="app_key"):
            self._minimal(auth_method="app_key")

    def test_app_key_with_all_fields(self):
        cfg = self._minimal(
            auth_method="app_key",
            client_id="cid",
            client_secret="secret",
            tenant_id="tid",
        )
        assert cfg.auth_method == "app_key"

    def test_from_file(self, tmp_path):
        data = {
            "cluster_uri": "https://mycluster.kusto.windows.net",
            "queries": [QUERY],
        }
        config_file = tmp_path / "bench.json"
        config_file.write_text(json.dumps(data), encoding="utf-8")
        cfg = BenchmarkConfig.from_file(config_file)
        assert cfg.cluster_uri == "https://mycluster.kusto.windows.net"
        assert len(cfg.queries) == 1

    def test_from_file_invalid_path(self):
        with pytest.raises(FileNotFoundError):
            BenchmarkConfig.from_file("/nonexistent/file.json")

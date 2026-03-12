"""Tests for kustobench.reporter."""

import json

import pytest

from kustobench.benchmark import BenchmarkResult, IterationResult, QueryResult
from kustobench.reporter import BenchmarkReporter


def _make_result() -> BenchmarkResult:
    result = BenchmarkResult(cluster_uri="https://mycluster.kusto.windows.net")
    qr = QueryResult(query_name="my_query", database="db")
    qr.iterations = [
        IterationResult(iteration=1, duration_ms=100.0, row_count=10),
        IterationResult(iteration=2, duration_ms=200.0, row_count=10),
        IterationResult(iteration=3, duration_ms=300.0, row_count=10),
    ]
    result.query_results.append(qr)
    result.finished_at = result.started_at + 5.0
    return result


class TestBenchmarkReporter:
    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            BenchmarkReporter(output_format="xml")

    def test_table_output_contains_query_name(self, capsys):
        reporter = BenchmarkReporter(output_format="table")
        reporter.report(_make_result())
        captured = capsys.readouterr()
        assert "my_query" in captured.out

    def test_table_output_contains_cluster_uri(self, capsys):
        reporter = BenchmarkReporter(output_format="table")
        reporter.report(_make_result())
        captured = capsys.readouterr()
        assert "mycluster" in captured.out

    def test_csv_output_has_header(self, capsys):
        reporter = BenchmarkReporter(output_format="csv")
        reporter.report(_make_result())
        captured = capsys.readouterr()
        assert "query" in captured.out
        assert "min_ms" in captured.out

    def test_csv_output_contains_query_name(self, capsys):
        reporter = BenchmarkReporter(output_format="csv")
        reporter.report(_make_result())
        captured = capsys.readouterr()
        assert "my_query" in captured.out

    def test_json_output_is_valid(self, capsys):
        reporter = BenchmarkReporter(output_format="json")
        reporter.report(_make_result())
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["cluster_uri"] == "https://mycluster.kusto.windows.net"
        assert len(data["queries"]) == 1
        assert data["queries"][0]["name"] == "my_query"
        assert data["queries"][0]["stats"]["count"] == 3

    def test_json_stats_values(self, capsys):
        reporter = BenchmarkReporter(output_format="json")
        reporter.report(_make_result())
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        stats = data["queries"][0]["stats"]
        assert stats["min_ms"] == 100.0
        assert stats["max_ms"] == 300.0
        assert abs(stats["avg_ms"] - 200.0) < 0.001

    def test_writes_to_file(self, tmp_path):
        out_file = str(tmp_path / "results.json")
        reporter = BenchmarkReporter(output_format="json", output_file=out_file)
        reporter.report(_make_result())
        with open(out_file, encoding="utf-8") as fh:
            data = json.load(fh)
        assert "queries" in data

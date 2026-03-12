"""Tests for kustobench.benchmark."""

from unittest.mock import MagicMock, patch

from kustobench.benchmark import BenchmarkResult, BenchmarkRunner, IterationResult, QueryResult
from kustobench.config import BenchmarkConfig

# ---------------------------------------------------------------------------
# IterationResult
# ---------------------------------------------------------------------------

class TestIterationResult:
    def test_succeeded_true_when_no_error(self):
        r = IterationResult(iteration=1, duration_ms=100.0, row_count=10)
        assert r.succeeded is True

    def test_succeeded_false_when_error(self):
        r = IterationResult(iteration=1, duration_ms=100.0, row_count=0, error="boom")
        assert r.succeeded is False


# ---------------------------------------------------------------------------
# QueryResult aggregation
# ---------------------------------------------------------------------------

class TestQueryResult:
    def _make_result(self, durations: list[float]) -> QueryResult:
        qr = QueryResult(query_name="q", database="db")
        for i, d in enumerate(durations, start=1):
            qr.iterations.append(IterationResult(iteration=i, duration_ms=d, row_count=1))
        return qr

    def test_empty_iterations(self):
        qr = QueryResult(query_name="q", database="db")
        assert qr.min_ms is None
        assert qr.max_ms is None
        assert qr.avg_ms is None
        assert qr.p50_ms is None

    def test_single_iteration(self):
        qr = self._make_result([200.0])
        assert qr.min_ms == 200.0
        assert qr.max_ms == 200.0
        assert qr.avg_ms == 200.0

    def test_multiple_iterations(self):
        qr = self._make_result([100.0, 200.0, 300.0])
        assert qr.min_ms == 100.0
        assert qr.max_ms == 300.0
        assert abs(qr.avg_ms - 200.0) < 0.001

    def test_failed_iteration_excluded_from_stats(self):
        qr = QueryResult(query_name="q", database="db")
        qr.iterations.append(IterationResult(iteration=1, duration_ms=100.0, row_count=1))
        qr.iterations.append(
            IterationResult(iteration=2, duration_ms=9999.0, row_count=0, error="err")
        )
        assert qr.min_ms == 100.0
        assert qr.max_ms == 100.0
        assert len(qr.failed_iterations) == 1

    def test_percentiles(self):
        qr = self._make_result([100.0, 200.0, 300.0, 400.0, 500.0])
        assert qr.p50_ms is not None
        assert qr.p90_ms is not None
        assert qr.p99_ms is not None
        assert qr.p50_ms <= qr.p90_ms <= qr.p99_ms


# ---------------------------------------------------------------------------
# BenchmarkRunner (mocked client)
# ---------------------------------------------------------------------------

def _make_config(iterations: int = 2, warmup: int = 0) -> BenchmarkConfig:
    return BenchmarkConfig.from_dict(
        {
            "cluster_uri": "https://mycluster.kusto.windows.net",
            "queries": [
                {
                    "name": "test_q",
                    "query": "T | count",
                    "database": "db",
                    "iterations": iterations,
                    "warmup_iterations": warmup,
                }
            ],
        }
    )


def _mock_response(row_count: int = 10):
    """Build a minimal fake Kusto response."""
    row = MagicMock()
    table = MagicMock()
    table.rows = [row] * row_count
    response = MagicMock()
    response.primary_results = [table]
    return response


class TestBenchmarkRunner:
    @patch("kustobench.benchmark.KustoBenchClient.from_default_auth")
    def test_run_returns_result(self, mock_factory):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.execute.return_value = _mock_response(5)
        mock_factory.return_value = mock_client

        runner = BenchmarkRunner(_make_config(iterations=2, warmup=1))
        result = runner.run()

        assert isinstance(result, BenchmarkResult)
        assert len(result.query_results) == 1
        qr = result.query_results[0]
        # warmup (1) + timed (2) = 3 execute calls total
        assert mock_client.execute.call_count == 3
        # Only 2 timed iterations stored
        assert len(qr.iterations) == 2
        assert all(r.succeeded for r in qr.iterations)
        assert result.finished_at is not None

    @patch("kustobench.benchmark.KustoBenchClient.from_default_auth")
    def test_failed_query_records_error(self, mock_factory):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.execute.side_effect = RuntimeError("query failed")
        mock_factory.return_value = mock_client

        runner = BenchmarkRunner(_make_config(iterations=1, warmup=0))
        result = runner.run()

        qr = result.query_results[0]
        assert len(qr.failed_iterations) == 1
        assert "query failed" in qr.failed_iterations[0].error

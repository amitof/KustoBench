"""Tests for benchmark.runner."""

import pytest
from unittest.mock import MagicMock, patch

from benchmark.runner import (
    BenchmarkResult,
    IterationResult,
    QueryResult,
    _execute_once,
    run_benchmark,
)


# ── QueryResult statistics ────────────────────────────────────────────────────


def _make_iterations(times, errors=None):
    errors = errors or []
    iters = []
    for i, t in enumerate(times):
        err = errors[i] if i < len(errors) else None
        iters.append(IterationResult(iteration=i + 1, elapsed_seconds=t,
                                     row_count=10, column_count=3, error=err))
    return iters


def test_query_result_statistics():
    qr = QueryResult(name="q", query="T | count")
    qr.iterations = _make_iterations([1.0, 2.0, 3.0])
    assert qr.min_seconds == pytest.approx(1.0)
    assert qr.max_seconds == pytest.approx(3.0)
    assert qr.mean_seconds == pytest.approx(2.0)
    assert qr.median_seconds == pytest.approx(2.0)
    assert qr.stdev_seconds is not None


def test_query_result_no_successful_iterations():
    qr = QueryResult(name="q", query="T | count")
    qr.iterations = _make_iterations([1.0, 2.0], errors=["err", "err"])
    assert qr.min_seconds is None
    assert qr.max_seconds is None
    assert qr.mean_seconds is None
    assert qr.stdev_seconds is None


def test_query_result_single_iteration():
    qr = QueryResult(name="q", query="T | count")
    qr.iterations = _make_iterations([1.5])
    assert qr.min_seconds == pytest.approx(1.5)
    assert qr.stdev_seconds is None  # need >= 2 for stdev


# ── _execute_once ─────────────────────────────────────────────────────────────


def test_execute_once_success():
    client = MagicMock()
    client.execute.return_value = {"row_count": 5, "column_count": 2}
    result = _execute_once(client, "T | count", iteration=1)
    assert result.success
    assert result.row_count == 5
    assert result.column_count == 2
    assert result.elapsed_seconds >= 0


def test_execute_once_failure():
    client = MagicMock()
    client.execute.side_effect = RuntimeError("connection refused")
    result = _execute_once(client, "T | count", iteration=1)
    assert not result.success
    assert "connection refused" in result.error


# ── run_benchmark ─────────────────────────────────────────────────────────────


def _mock_client(row_count=10, column_count=3):
    client = MagicMock()
    client.execute.return_value = {"row_count": row_count, "column_count": column_count}
    return client


def test_run_benchmark_basic():
    config = {
        "benchmark": {"iterations": 2, "warmup_iterations": 0},
        "queries": [
            {"name": "count", "query": "T | count"},
        ],
    }
    client = _mock_client()
    result = run_benchmark(client, config)
    assert len(result.query_results) == 1
    qr = result.query_results[0]
    assert qr.name == "count"
    assert len(qr.iterations) == 2
    # execute called twice (no warmup)
    assert client.execute.call_count == 2


def test_run_benchmark_warmup_discarded():
    config = {
        "benchmark": {"iterations": 3, "warmup_iterations": 2},
        "queries": [{"name": "q", "query": "T | count"}],
    }
    client = _mock_client()
    result = run_benchmark(client, config)
    # 2 warmup + 3 measured = 5 total calls
    assert client.execute.call_count == 5
    assert len(result.query_results[0].iterations) == 3


def test_run_benchmark_multiple_queries():
    config = {
        "benchmark": {"iterations": 1, "warmup_iterations": 0},
        "queries": [
            {"name": "q1", "query": "T | count"},
            {"name": "q2", "query": "T | take 10"},
        ],
    }
    client = _mock_client()
    result = run_benchmark(client, config)
    assert len(result.query_results) == 2
    assert result.query_results[0].name == "q1"
    assert result.query_results[1].name == "q2"


def test_run_benchmark_skips_empty_query():
    config = {
        "benchmark": {"iterations": 1, "warmup_iterations": 0},
        "queries": [{"name": "empty", "query": ""}],
    }
    client = _mock_client()
    result = run_benchmark(client, config)
    assert len(result.query_results) == 0
    assert client.execute.call_count == 0


def test_run_benchmark_records_total_time():
    config = {
        "benchmark": {"iterations": 1, "warmup_iterations": 0},
        "queries": [{"name": "q", "query": "T | count"}],
    }
    result = run_benchmark(_mock_client(), config)
    assert result.total_elapsed_seconds >= 0

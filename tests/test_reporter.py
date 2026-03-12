"""Tests for benchmark.reporter."""

import json

import pytest

from benchmark.reporter import report_csv, report_json, report_table, report
from benchmark.runner import BenchmarkResult, IterationResult, QueryResult


def _make_result() -> BenchmarkResult:
    br = BenchmarkResult(total_elapsed_seconds=10.0)
    qr = QueryResult(name="count", query="T | count")
    qr.iterations = [
        IterationResult(iteration=1, elapsed_seconds=1.0, row_count=5, column_count=2),
        IterationResult(iteration=2, elapsed_seconds=1.5, row_count=5, column_count=2),
        IterationResult(iteration=3, elapsed_seconds=2.0, row_count=5, column_count=2),
    ]
    br.query_results.append(qr)
    return br


def test_report_table_contains_query_name():
    result = _make_result()
    text = report_table(result)
    assert "count" in text


def test_report_table_contains_stats():
    result = _make_result()
    text = report_table(result)
    # tabulate may trim trailing zeros, so check for the values without trailing zeros
    assert "1.5" in text   # mean / median
    assert "count" in text  # query name is present


def test_report_table_total_time():
    result = _make_result()
    text = report_table(result)
    assert "10.000" in text


def test_report_csv_headers():
    result = _make_result()
    csv_text = report_csv(result)
    assert "Query" in csv_text
    assert "Mean (s)" in csv_text


def test_report_csv_row():
    result = _make_result()
    csv_text = report_csv(result)
    assert "count" in csv_text


def test_report_json_structure():
    result = _make_result()
    data = json.loads(report_json(result))
    assert "total_elapsed_seconds" in data
    assert "queries" in data
    assert len(data["queries"]) == 1
    q = data["queries"][0]
    assert q["name"] == "count"
    assert q["successful_iterations"] == 3
    assert q["failed_iterations"] == 0
    assert "iterations" in q
    assert len(q["iterations"]) == 3


def test_report_json_with_error():
    result = BenchmarkResult(total_elapsed_seconds=1.0)
    qr = QueryResult(name="failing", query="invalid")
    qr.iterations = [
        IterationResult(iteration=1, elapsed_seconds=0.1, row_count=0,
                        column_count=0, error="Timeout"),
    ]
    result.query_results.append(qr)
    data = json.loads(report_json(result))
    q = data["queries"][0]
    assert q["failed_iterations"] == 1
    assert q["successful_iterations"] == 0
    assert data["queries"][0]["iterations"][0]["error"] == "Timeout"


def test_report_writes_to_file(tmp_path):
    result = _make_result()
    out = str(tmp_path / "out.txt")
    report(result, fmt="table", output_file=out)
    content = open(out).read()
    assert "count" in content


def test_report_invalid_format():
    result = _make_result()
    with pytest.raises(ValueError, match="Unknown output format"):
        report(result, fmt="xml")

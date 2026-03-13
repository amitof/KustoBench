"""Results reporter for KustoBench."""

import csv
import io
import json
import sys
from typing import Optional

from tabulate import tabulate

from .runner import BenchmarkResult, QueryResult


def _fmt(value: Optional[float], decimals: int = 3) -> str:
    """Format an optional float value."""
    if value is None:
        return "N/A"
    return f"{value:.{decimals}f}"


def _is_single_iteration(result: BenchmarkResult) -> bool:
    """Return True when every query was run for exactly one measured iteration."""
    return all(
        len(qr.iterations) == 1 for qr in result.query_results
    ) if result.query_results else False


def _query_summary_rows(result: BenchmarkResult) -> list:
    single = _is_single_iteration(result)
    rows = []
    for qr in result.query_results:
        success = len(qr.successful_iterations)
        failure = len(qr.failed_iterations)
        if single:
            row = [
                qr.name,
                _fmt(qr.min_seconds),
            ]
        else:
            row = [
                qr.name,
                success,
                failure,
                _fmt(qr.min_seconds),
                _fmt(qr.mean_seconds),
                _fmt(qr.median_seconds),
                _fmt(qr.max_seconds),
                _fmt(qr.stdev_seconds),
            ]
        if failure > 0:
            row = [f"\033[91m{cell}\033[0m" for cell in row]
        rows.append(row)
    return rows


_HEADERS_SINGLE = [
    "Query",
    "Duration (s)",
]

_HEADERS_MULTI = [
    "Query",
    "OK",
    "Fail",
    "Min (s)",
    "Mean (s)",
    "Median (s)",
    "Max (s)",
    "Stdev (s)",
]


def report_table(result: BenchmarkResult, file=None) -> str:
    """Render benchmark results as a human-readable table.

    Args:
        result: The :class:`BenchmarkResult` to render.
        file: Optional file-like object to write to.  If ``None`` the string
              is returned but *not* printed.

    Returns:
        The formatted table as a string.
    """
    rows = _query_summary_rows(result)
    headers = _HEADERS_SINGLE if _is_single_iteration(result) else _HEADERS_MULTI
    table = tabulate(rows, headers=headers, tablefmt="github")
    footer = f"\nTotal wall-clock time: {result.total_elapsed_seconds:.3f}s\n"
    output = table + footer
    if file is not None:
        file.write(output)
    return output


def report_csv(result: BenchmarkResult, file=None) -> str:
    """Render benchmark results as CSV.

    Args:
        result: The :class:`BenchmarkResult` to render.
        file: Optional file-like object to write to.

    Returns:
        The CSV content as a string.
    """
    buf = io.StringIO()
    writer = csv.writer(buf)
    headers = _HEADERS_SINGLE if _is_single_iteration(result) else _HEADERS_MULTI
    writer.writerow(headers)
    writer.writerows(_query_summary_rows(result))
    output = buf.getvalue()
    if file is not None:
        file.write(output)
    return output


def report_json(result: BenchmarkResult, file=None) -> str:
    """Render benchmark results as JSON.

    Args:
        result: The :class:`BenchmarkResult` to render.
        file: Optional file-like object to write to.

    Returns:
        The JSON string.
    """
    data = {
        "total_elapsed_seconds": result.total_elapsed_seconds,
        "queries": [],
    }
    for qr in result.query_results:
        q = {
            "name": qr.name,
            "query": qr.query,
            "successful_iterations": len(qr.successful_iterations),
            "failed_iterations": len(qr.failed_iterations),
            "min_seconds": qr.min_seconds,
            "mean_seconds": qr.mean_seconds,
            "median_seconds": qr.median_seconds,
            "max_seconds": qr.max_seconds,
            "stdev_seconds": qr.stdev_seconds,
            "iterations": [
                {
                    "iteration": it.iteration,
                    "elapsed_seconds": it.elapsed_seconds,
                    "row_count": it.row_count,
                    "column_count": it.column_count,
                    "error": it.error,
                }
                for it in qr.iterations
            ],
        }
        data["queries"].append(q)

    output = json.dumps(data, indent=2)
    if file is not None:
        file.write(output)
    return output


def report(result: BenchmarkResult, fmt: str = "table", output_file: Optional[str] = None) -> None:
    """Print benchmark results in the requested format.

    Args:
        result: The :class:`BenchmarkResult` to render.
        fmt: Output format – one of ``"table"``, ``"csv"``, ``"json"``.
        output_file: Optional path to write the output to.  If ``None``
                     results are printed to stdout.

    Raises:
        ValueError: If *fmt* is not recognised.
    """
    fmt = fmt.lower()
    if fmt == "table":
        text = report_table(result)
    elif fmt == "csv":
        text = report_csv(result)
    elif fmt == "json":
        text = report_json(result)
    else:
        raise ValueError(f"Unknown output format: {fmt!r}. Choose from: table, csv, json")

    if output_file:
        with open(output_file, "w", encoding="utf-8") as fh:
            fh.write(text)
        print(f"Results written to {output_file}")
    else:
        print(text)

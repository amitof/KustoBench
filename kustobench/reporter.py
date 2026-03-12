"""Results reporting for KustoBench."""

from __future__ import annotations

import csv
import io
import json
import sys
from typing import Optional

from tabulate import tabulate

from .benchmark import BenchmarkResult


def _fmt(value: Optional[float], decimals: int = 1) -> str:
    """Format an optional float for display."""
    if value is None:
        return "N/A"
    return f"{value:.{decimals}f}"


class BenchmarkReporter:
    """Formats and writes benchmark results to a file or stdout."""

    def __init__(self, output_format: str = "table", output_file: Optional[str] = None) -> None:
        valid = {"table", "csv", "json"}
        if output_format not in valid:
            raise ValueError(f"output_format must be one of {valid}")
        self._format = output_format
        self._output_file = output_file

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def report(self, result: BenchmarkResult) -> None:
        """Format *result* and write it to the configured destination."""
        content = self._format_result(result)
        self._write(content)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _format_result(self, result: BenchmarkResult) -> str:
        if self._format == "json":
            return self._to_json(result)
        if self._format == "csv":
            return self._to_csv(result)
        return self._to_table(result)

    def _write(self, content: str) -> None:
        if self._output_file:
            with open(self._output_file, "w", encoding="utf-8") as fh:
                fh.write(content)
        else:
            sys.stdout.write(content)
            sys.stdout.write("\n")

    # ------------------------------------------------------------------
    # Formatters
    # ------------------------------------------------------------------

    @staticmethod
    def _rows(result: BenchmarkResult) -> list[dict]:
        rows = []
        for qr in result.query_results:
            rows.append(
                {
                    "query": qr.query_name,
                    "database": qr.database,
                    "iterations": len(qr.iterations),
                    "failed": len(qr.failed_iterations),
                    "min_ms": qr.min_ms,
                    "avg_ms": qr.avg_ms,
                    "p50_ms": qr.p50_ms,
                    "p90_ms": qr.p90_ms,
                    "p99_ms": qr.p99_ms,
                    "max_ms": qr.max_ms,
                }
            )
        return rows

    @classmethod
    def _to_table(cls, result: BenchmarkResult) -> str:
        rows = cls._rows(result)
        headers = [
            "Query", "Database", "Iters", "Failed",
            "Min(ms)", "Avg(ms)", "P50(ms)", "P90(ms)", "P99(ms)", "Max(ms)",
        ]
        table_rows = [
            [
                r["query"],
                r["database"],
                r["iterations"],
                r["failed"],
                _fmt(r["min_ms"]),
                _fmt(r["avg_ms"]),
                _fmt(r["p50_ms"]),
                _fmt(r["p90_ms"]),
                _fmt(r["p99_ms"]),
                _fmt(r["max_ms"]),
            ]
            for r in rows
        ]
        header = (
            f"Cluster : {result.cluster_uri}\n"
        )
        if result.total_duration_s is not None:
            header += f"Duration: {result.total_duration_s:.1f}s\n"
        header += "\n"
        return header + tabulate(table_rows, headers=headers, tablefmt="github")

    @classmethod
    def _to_csv(cls, result: BenchmarkResult) -> str:
        rows = cls._rows(result)
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=[
                "query", "database", "iterations", "failed",
                "min_ms", "avg_ms", "p50_ms", "p90_ms", "p99_ms", "max_ms",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        return output.getvalue()

    @classmethod
    def _to_json(cls, result: BenchmarkResult) -> str:
        data = {
            "cluster_uri": result.cluster_uri,
            "total_duration_s": result.total_duration_s,
            "queries": [],
        }
        for qr in result.query_results:
            entry = {
                "name": qr.query_name,
                "database": qr.database,
                "iterations": [
                    {
                        "iteration": it.iteration,
                        "duration_ms": it.duration_ms,
                        "row_count": it.row_count,
                        "error": it.error,
                    }
                    for it in qr.iterations
                ],
                "stats": {
                    "count": len(qr.successful_iterations),
                    "failed": len(qr.failed_iterations),
                    "min_ms": qr.min_ms,
                    "avg_ms": qr.avg_ms,
                    "p50_ms": qr.p50_ms,
                    "p90_ms": qr.p90_ms,
                    "p99_ms": qr.p99_ms,
                    "max_ms": qr.max_ms,
                },
            }
            data["queries"].append(entry)
        return json.dumps(data, indent=2)

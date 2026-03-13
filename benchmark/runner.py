"""Benchmark runner for KustoBench."""

import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class IterationResult:
    """Result of a single query iteration."""

    iteration: int
    elapsed_seconds: float
    row_count: int
    column_count: int
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.error is None


@dataclass
class QueryResult:
    """Aggregated results for a single benchmark query."""

    name: str
    query: str
    iterations: List[IterationResult] = field(default_factory=list)

    @property
    def successful_iterations(self) -> List[IterationResult]:
        return [r for r in self.iterations if r.success]

    @property
    def failed_iterations(self) -> List[IterationResult]:
        return [r for r in self.iterations if not r.success]

    @property
    def min_seconds(self) -> Optional[float]:
        times = [r.elapsed_seconds for r in self.successful_iterations]
        return min(times) if times else None

    @property
    def max_seconds(self) -> Optional[float]:
        times = [r.elapsed_seconds for r in self.successful_iterations]
        return max(times) if times else None

    @property
    def mean_seconds(self) -> Optional[float]:
        times = [r.elapsed_seconds for r in self.successful_iterations]
        return statistics.mean(times) if times else None

    @property
    def median_seconds(self) -> Optional[float]:
        times = [r.elapsed_seconds for r in self.successful_iterations]
        return statistics.median(times) if times else None

    @property
    def stdev_seconds(self) -> Optional[float]:
        times = [r.elapsed_seconds for r in self.successful_iterations]
        return statistics.stdev(times) if len(times) >= 2 else None


@dataclass
class BenchmarkResult:
    """Container for all query results from a benchmark run."""

    query_results: List[QueryResult] = field(default_factory=list)
    total_elapsed_seconds: float = 0.0


def run_benchmark(client, config: dict) -> BenchmarkResult:
    """Run the benchmark defined in *config* using *client*.

    The runner executes each query for ``warmup_iterations`` warm-up rounds
    (results discarded) followed by ``iterations`` measured rounds.

    Args:
        client: An initialised :class:`KustoBenchClient`.
        config: Benchmark configuration dictionary.

    Returns:
        A :class:`BenchmarkResult` with per-query statistics.
    """
    bench_cfg = config.get("benchmark", {})
    iterations = int(bench_cfg.get("iterations", 3))
    warmup_iterations = int(bench_cfg.get("warmup_iterations", 1))
    queries = config.get("queries", [])

    benchmark_result = BenchmarkResult()
    benchmark_start = time.perf_counter()

    for qi, query_def in enumerate(queries):
        name = query_def.get("name", "unnamed")
        query_text = query_def.get("query", "")
        if not query_text:
            continue

        query_result = QueryResult(name=name, query=query_text)

        # Warm-up rounds
        for _ in range(warmup_iterations):
            _execute_once(client, query_text)

        # Measured rounds
        for i in range(1, iterations + 1):
            iter_result = _execute_once(client, query_text, iteration=i)
            query_result.iterations.append(iter_result)

        # Print per-query summary
        display = query_text[:200].replace('\n', ' ')
        if query_result.successful_iterations:
            if iterations == 1:
                timing = f"duration={query_result.min_seconds:.3f}s"
            else:
                timing = (
                    f"min={query_result.min_seconds:.3f}s  "
                    f"max={query_result.max_seconds:.3f}s  "
                    f"avg={query_result.mean_seconds:.3f}s"
                )
            print(
                f"[{qi}/{len(queries)}] {name}: {timing}  | {display}",
                file=sys.stderr,
            )
        else:
            err = query_result.failed_iterations[0].error if query_result.failed_iterations else "unknown"
            print(f"\033[91m[{qi}/{len(queries)}] {name}: FAILED ({err}) | {display}\033[0m", file=sys.stderr)

        benchmark_result.query_results.append(query_result)

    benchmark_result.total_elapsed_seconds = time.perf_counter() - benchmark_start
    return benchmark_result


def _execute_once(
    client,
    query: str,
    iteration: int = 0,
) -> IterationResult:
    """Execute a single query iteration and time it.

    Args:
        client: The Kusto client.
        query: KQL query string.
        iteration: Iteration number (0 for warm-up).

    Returns:
        An :class:`IterationResult`.
    """
    start = time.perf_counter()
    try:
        meta = client.execute(query)
        elapsed = time.perf_counter() - start
        return IterationResult(
            iteration=iteration,
            elapsed_seconds=elapsed,
            row_count=meta["row_count"],
            column_count=meta["column_count"],
        )
    except Exception as exc:  # noqa: BLE001
        elapsed = time.perf_counter() - start
        return IterationResult(
            iteration=iteration,
            elapsed_seconds=elapsed,
            row_count=0,
            column_count=0,
            error=str(exc),
        )

"""Core benchmark runner for KustoBench."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from .client import KustoBenchClient
from .config import BenchmarkConfig, QueryConfig

logger = logging.getLogger(__name__)


@dataclass
class IterationResult:
    """Result of a single query iteration."""

    iteration: int
    duration_ms: float
    row_count: int
    error: Optional[str] = None

    @property
    def succeeded(self) -> bool:
        return self.error is None


@dataclass
class QueryResult:
    """Aggregated results for one :class:`QueryConfig`."""

    query_name: str
    database: str
    iterations: list[IterationResult] = field(default_factory=list)

    @property
    def successful_iterations(self) -> list[IterationResult]:
        return [r for r in self.iterations if r.succeeded]

    @property
    def failed_iterations(self) -> list[IterationResult]:
        return [r for r in self.iterations if not r.succeeded]

    @property
    def min_ms(self) -> Optional[float]:
        durations = [r.duration_ms for r in self.successful_iterations]
        return min(durations) if durations else None

    @property
    def max_ms(self) -> Optional[float]:
        durations = [r.duration_ms for r in self.successful_iterations]
        return max(durations) if durations else None

    @property
    def avg_ms(self) -> Optional[float]:
        durations = [r.duration_ms for r in self.successful_iterations]
        return sum(durations) / len(durations) if durations else None

    @property
    def p50_ms(self) -> Optional[float]:
        return self._percentile(50)

    @property
    def p90_ms(self) -> Optional[float]:
        return self._percentile(90)

    @property
    def p99_ms(self) -> Optional[float]:
        return self._percentile(99)

    def _percentile(self, pct: float) -> Optional[float]:
        durations = sorted(r.duration_ms for r in self.successful_iterations)
        if not durations:
            return None
        idx = max(0, int(len(durations) * pct / 100) - 1)
        return durations[idx]


@dataclass
class BenchmarkResult:
    """Collection of all query results for a benchmark run."""

    cluster_uri: str
    query_results: list[QueryResult] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None

    @property
    def total_duration_s(self) -> Optional[float]:
        if self.finished_at is None:
            return None
        return self.finished_at - self.started_at


class BenchmarkRunner:
    """Runs benchmark queries against a Kusto cluster."""

    def __init__(self, config: BenchmarkConfig) -> None:
        self._config = config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> BenchmarkResult:
        """Execute all configured benchmark queries and return the results."""
        client = self._build_client()
        result = BenchmarkResult(cluster_uri=self._config.cluster_uri)

        try:
            with client:
                for query_cfg in self._config.queries:
                    query_result = self._run_query(client, query_cfg)
                    result.query_results.append(query_result)
        finally:
            result.finished_at = time.time()

        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_client(self) -> KustoBenchClient:
        cfg = self._config
        if cfg.auth_method == "app_key":
            return KustoBenchClient.from_app_key(
                cfg.cluster_uri,
                cfg.client_id,
                cfg.client_secret,
                cfg.tenant_id,
                cfg.request_app_name,
            )
        if cfg.auth_method == "device":
            return KustoBenchClient.from_device_auth(
                cfg.cluster_uri,
                cfg.tenant_id,
                cfg.request_app_name,
            )
        return KustoBenchClient.from_default_auth(
            cfg.cluster_uri,
            cfg.request_app_name,
        )

    def _run_query(
        self, client: KustoBenchClient, query_cfg: QueryConfig
    ) -> QueryResult:
        query_result = QueryResult(
            query_name=query_cfg.name,
            database=query_cfg.database,
        )

        total = query_cfg.warmup_iterations + query_cfg.iterations
        for i in range(total):
            is_warmup = i < query_cfg.warmup_iterations
            label = (
                f"[warmup {i + 1}]"
                if is_warmup
                else f"[iter {i - query_cfg.warmup_iterations + 1}]"
            )
            logger.info("  %s %s", query_cfg.name, label)

            iteration_result = self._execute_once(
                client,
                query_cfg.database,
                query_cfg.query,
                query_cfg.timeout_seconds,
                iteration=i - query_cfg.warmup_iterations + 1,
            )

            if not is_warmup:
                query_result.iterations.append(iteration_result)

            if iteration_result.error:
                logger.warning(
                    "  %s %s FAILED: %s",
                    query_cfg.name,
                    label,
                    iteration_result.error,
                )
            else:
                logger.info(
                    "  %s %s completed in %.1f ms (%d rows)",
                    query_cfg.name,
                    label,
                    iteration_result.duration_ms,
                    iteration_result.row_count,
                )

        return query_result

    @staticmethod
    def _execute_once(
        client: KustoBenchClient,
        database: str,
        query: str,
        timeout_seconds: int,
        iteration: int,
    ) -> IterationResult:
        start = time.perf_counter()
        try:
            response = client.execute(database, query, timeout_seconds)
            duration_ms = (time.perf_counter() - start) * 1000
            row_count = sum(
                len(table.rows) for table in response.primary_results
            )
            return IterationResult(
                iteration=iteration,
                duration_ms=duration_ms,
                row_count=row_count,
            )
        except Exception as exc:  # noqa: BLE001
            duration_ms = (time.perf_counter() - start) * 1000
            return IterationResult(
                iteration=iteration,
                duration_ms=duration_ms,
                row_count=0,
                error=str(exc),
            )

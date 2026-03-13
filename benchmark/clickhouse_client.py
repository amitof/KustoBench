"""ClickHouse HTTP client for KustoBench.

Provides the same interface as :class:`KustoBenchClient` so that the
load / run pipeline can work with both ADX and ClickHouse transparently.
"""

from __future__ import annotations

import http.client
import urllib.parse
import json


class ClickHouseClient:
    """Thin HTTP wrapper for ClickHouse queries matching the KustoBenchClient API."""

    def __init__(self, config: dict) -> None:
        self._host = config.get("host", "localhost")
        self._port = int(config.get("port", 8123))
        self._database = config.get("database", "default")
        self._base_url = f"http://{self._host}:{self._port}/"
        # Verify connectivity
        self._query_raw("SELECT 1")

    # ── Public interface (mirrors KustoBenchClient) ──────────────────────

    def execute(self, query: str) -> dict:
        """Execute a SQL query and return row/column counts."""
        body = self._query_raw(query + " FORMAT JSONCompact")
        try:
            result = json.loads(body)
            row_count = result.get("rows", 0)
            column_count = len(result.get("meta", []))
        except (json.JSONDecodeError, KeyError):
            row_count = 0
            column_count = 0
        return {"row_count": row_count, "column_count": column_count}

    def execute_control(self, command: str) -> dict:
        """Execute a DDL / control statement (CREATE, DROP, INSERT, …)."""
        self._query_raw(command)
        return {"row_count": 0, "column_count": 0}

    def get_cluster_info(self) -> dict:
        info: dict = {
            "cluster_url": self._base_url,
            "nodes": None,
            "cores_per_node": None,
            "total_cores": None,
            "memory_gb_per_node": None,
        }
        try:
            body = self._query_raw(
                "SELECT hostName(), getSetting('max_threads') AS cores, "
                "getSetting('max_memory_usage') AS mem "
                "FORMAT JSONCompact"
            )
            result = json.loads(body)
            rows = result.get("data", [])
            if rows:
                info["nodes"] = len(rows)
                cores = int(rows[0][1])
                info["cores_per_node"] = cores
                info["total_cores"] = cores * len(rows)
        except Exception:
            pass

        # Try system.clusters for node count
        try:
            body = self._query_raw(
                "SELECT count() AS cnt FROM system.clusters "
                "WHERE cluster = 'kustobench_cluster' FORMAT JSONCompact"
            )
            result = json.loads(body)
            rows = result.get("data", [])
            if rows and int(rows[0][0]) > 0:
                node_count = int(rows[0][0])
                info["nodes"] = node_count
                if info["cores_per_node"]:
                    info["total_cores"] = info["cores_per_node"] * node_count
        except Exception:
            pass

        return info

    def drop_all_tables(self) -> list:
        """Drop all user tables in the current database. Returns list of dropped table names."""
        body = self._query_raw(
            "SELECT name FROM system.tables "
            f"WHERE database = '{self._database}' "
            "AND engine NOT IN ('SystemLog', 'Memory') FORMAT JSONCompact"
        )
        result = json.loads(body)
        tables = [row[0] for row in result.get("data", [])]
        for t in tables:
            self._query_raw(f"DROP TABLE IF EXISTS `{t}`")
        return tables

    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ── Internal ─────────────────────────────────────────────────────────

    def _query_raw(self, query: str, extra_params: dict | None = None) -> str:
        """Send a query via HTTP POST to ClickHouse, return the response body.

        Creates a fresh TCP connection per call so it is safe to use from
        multiple threads concurrently.  Retries up to 3 times on transient
        errors (syntax errors caused by body truncation, connection resets).
        """
        p = {"database": self._database}
        if extra_params:
            p.update(extra_params)
        path = f"/?{urllib.parse.urlencode(p)}"
        body = query.encode("utf-8")
        content_length = str(len(body))

        last_exc: Exception | None = None
        for attempt in range(3):
            conn = http.client.HTTPConnection(self._host, self._port, timeout=600)
            try:
                conn.request(
                    "POST", path, body=body,
                    headers={
                        "Content-Length": content_length,
                        "Content-Type": "application/octet-stream",
                    },
                )
                resp = conn.getresponse()
                response_body = resp.read().decode("utf-8")
                if resp.status == 200:
                    return response_body
                # Retry on truncation errors (position == end of query)
                if "failed at position" in response_body and "(end of query)" in response_body:
                    last_exc = RuntimeError(
                        f"ClickHouse HTTP {resp.status}: {response_body.strip()}"
                    )
                    continue
                raise RuntimeError(
                    f"ClickHouse HTTP {resp.status}: {response_body.strip()}"
                )
            except (ConnectionError, OSError) as exc:
                last_exc = exc
                continue
            finally:
                conn.close()
        raise last_exc  # type: ignore[misc]

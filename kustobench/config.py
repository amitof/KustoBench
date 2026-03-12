"""Configuration models for KustoBench."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class QueryConfig:
    """A single benchmark query definition."""

    name: str
    query: str
    database: str
    iterations: int = 3
    warmup_iterations: int = 1
    timeout_seconds: int = 300
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Query name must not be empty")
        if not self.query:
            raise ValueError("Query text must not be empty")
        if not self.database:
            raise ValueError("Database name must not be empty")
        if self.iterations < 1:
            raise ValueError("iterations must be >= 1")
        if self.warmup_iterations < 0:
            raise ValueError("warmup_iterations must be >= 0")
        if self.timeout_seconds < 1:
            raise ValueError("timeout_seconds must be >= 1")


@dataclass
class BenchmarkConfig:
    """Top-level benchmark configuration."""

    cluster_uri: str
    queries: list[QueryConfig]
    auth_method: str = "default"  # "default", "app_key", "device"
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    tenant_id: Optional[str] = None
    output_format: str = "table"  # "table", "csv", "json"
    output_file: Optional[str] = None
    request_app_name: str = "KustoBench"

    def __post_init__(self) -> None:
        if not self.cluster_uri:
            raise ValueError("cluster_uri must not be empty")
        if not self.queries:
            raise ValueError("At least one query must be specified")
        valid_auth = {"default", "app_key", "device"}
        if self.auth_method not in valid_auth:
            raise ValueError(f"auth_method must be one of {valid_auth}")
        valid_formats = {"table", "csv", "json"}
        if self.output_format not in valid_formats:
            raise ValueError(f"output_format must be one of {valid_formats}")
        if self.auth_method == "app_key":
            missing = [
                f for f in ("client_id", "client_secret", "tenant_id")
                if not getattr(self, f)
            ]
            if missing:
                raise ValueError(
                    f"auth_method 'app_key' requires: {', '.join(missing)}"
                )

    @classmethod
    def from_file(cls, path: str | Path) -> "BenchmarkConfig":
        """Load a BenchmarkConfig from a JSON file."""
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)

        raw_queries = data.pop("queries", [])
        queries = [QueryConfig(**q) for q in raw_queries]
        return cls(queries=queries, **data)

    @classmethod
    def from_dict(cls, data: dict) -> "BenchmarkConfig":
        """Build a BenchmarkConfig from a plain dictionary."""
        data = dict(data)
        raw_queries = data.pop("queries", [])
        queries = [QueryConfig(**q) for q in raw_queries]
        return cls(queries=queries, **data)

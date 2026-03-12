"""KustoBench – benchmark tool for Azure Data Explorer / Eventhouse."""

from .benchmark import BenchmarkRunner
from .config import BenchmarkConfig, QueryConfig
from .reporter import BenchmarkReporter

__all__ = ["BenchmarkRunner", "BenchmarkConfig", "QueryConfig", "BenchmarkReporter"]
__version__ = "0.1.0"

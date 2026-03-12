# KustoBench

A Python benchmark tool for [Azure Data Explorer (ADX)](https://azure.microsoft.com/en-us/products/data-explorer) and [Microsoft Fabric Eventhouse](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/eventhouse) Kusto clusters.

KustoBench measures query latency (min, average, P50, P90, P99, max) by running configurable KQL queries a specified number of times, with optional warm-up iterations, and reports results as a table, CSV, or JSON.

---

## Features

- Run one or many KQL benchmark queries against any Kusto cluster
- Configurable iterations and warm-up iterations per query
- Latency statistics: min, avg, P50, P90, P99, max
- Three authentication methods: DefaultAzureCredential, AAD app key (service principal), and device-code flow
- Output formats: rich table (default), CSV, JSON
- Optional file output
- CLI (`kustobench`) and importable Python API

---

## Project Structure

```
KustoBench/
├── kustobench/           # Python package
│   ├── __init__.py
│   ├── __main__.py       # CLI entry point
│   ├── benchmark.py      # Core benchmark runner and result models
│   ├── client.py         # Kusto client wrapper
│   ├── config.py         # Configuration models (BenchmarkConfig, QueryConfig)
│   └── reporter.py       # Results formatter (table, CSV, JSON)
├── queries/
│   └── sample_queries.json   # Example benchmark configuration
├── tests/
│   ├── test_benchmark.py
│   ├── test_config.py
│   └── test_reporter.py
├── pyproject.toml
├── requirements.txt
└── requirements-dev.txt
```

---

## Installation

### From source

```bash
git clone https://github.com/amitof/KustoBench.git
cd KustoBench
pip install -e .
```

### Development dependencies (linting + tests)

```bash
pip install -e ".[dev]"
# or
pip install -r requirements-dev.txt
```

---

## Quick Start

### Run a single inline query

```bash
kustobench \
  --cluster "https://help.kusto.windows.net" \
  --database Samples \
  --query "StormEvents | count" \
  --iterations 5 \
  --warmup 1
```

### Run from a configuration file

```bash
kustobench --config queries/sample_queries.json
```

Edit `queries/sample_queries.json` to point at your cluster and define your queries before running.

---

## Configuration File Format

```json
{
  "cluster_uri": "https://<your-cluster>.<region>.kusto.windows.net",
  "auth_method": "default",
  "output_format": "table",
  "queries": [
    {
      "name": "row_count",
      "query": "StormEvents | count",
      "database": "Samples",
      "iterations": 5,
      "warmup_iterations": 1
    }
  ]
}
```

### Top-level fields

| Field | Required | Default | Description |
|---|---|---|---|
| `cluster_uri` | ✅ | – | Full URI of the Kusto cluster |
| `auth_method` | | `"default"` | `"default"`, `"app_key"`, or `"device"` |
| `client_id` | app_key only | – | AAD application (client) ID |
| `client_secret` | app_key only | – | AAD application secret |
| `tenant_id` | app_key only | – | AAD tenant ID |
| `output_format` | | `"table"` | `"table"`, `"csv"`, or `"json"` |
| `output_file` | | – | Write results to this file instead of stdout |
| `request_app_name` | | `"KustoBench"` | Application name sent with each request for tracing |
| `queries` | ✅ | – | List of query definitions (see below) |

### Query fields

| Field | Required | Default | Description |
|---|---|---|---|
| `name` | ✅ | – | Friendly name for the query |
| `query` | ✅ | – | KQL query text |
| `database` | ✅ | – | Target database |
| `iterations` | | `3` | Number of timed executions |
| `warmup_iterations` | | `1` | Warm-up executions (results excluded from stats) |
| `timeout_seconds` | | `300` | Per-execution timeout |
| `tags` | | `[]` | Arbitrary string tags (informational) |

---

## Authentication

### DefaultAzureCredential (recommended)

Uses the [Azure DefaultAzureCredential](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential) chain — environment variables, managed identity, Azure CLI, etc.

```json
{ "auth_method": "default" }
```

### AAD Application Key (service principal)

```json
{
  "auth_method": "app_key",
  "client_id": "...",
  "client_secret": "...",
  "tenant_id": "..."
}
```

Alternatively set environment variables `KUSTO_CLIENT_ID`, `KUSTO_CLIENT_SECRET`, `KUSTO_TENANT_ID`.

### Device Code Flow

Interactive login via browser.

```json
{ "auth_method": "device" }
```

---

## CLI Reference

```
Usage: kustobench [OPTIONS]

  KustoBench – run benchmark queries against an Azure Data Explorer /
  Eventhouse cluster.

Options:
  -c, --config FILE        Path to a JSON benchmark configuration file.
  --cluster TEXT           Kusto cluster URI.  [env var: KUSTO_CLUSTER]
  -d, --database TEXT      Default database name.
  -q, --query TEXT         A single KQL query to benchmark.
  -n, --iterations INT     Number of timed iterations.  [default: 3]
  --warmup INT             Number of warm-up iterations (not timed).  [default: 1]
  --auth [default|app_key|device]  Authentication method.  [default: default]
  --client-id TEXT         AAD application (client) ID.  [env var: KUSTO_CLIENT_ID]
  --client-secret TEXT     AAD application secret.  [env var: KUSTO_CLIENT_SECRET]
  --tenant-id TEXT         AAD tenant ID.  [env var: KUSTO_TENANT_ID]
  -f, --output-format [table|csv|json]  Output format.  [default: table]
  -o, --output-file TEXT   Write results to this file instead of stdout.
  -v, --verbose            Enable verbose logging.
  --version                Show the version and exit.
  --help                   Show this message and exit.
```

---

## Python API

```python
from kustobench import BenchmarkConfig, BenchmarkRunner, BenchmarkReporter

config = BenchmarkConfig.from_file("queries/sample_queries.json")
runner = BenchmarkRunner(config)
result = runner.run()

reporter = BenchmarkReporter(output_format="table")
reporter.report(result)
```

---

## Running Tests

```bash
pytest
```

---

## Linting

```bash
ruff check kustobench/ tests/
```

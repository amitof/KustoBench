# KustoBench

A Python benchmarking tool for **Azure Data Explorer (Kusto / ADX)**.

KustoBench runs a configurable suite of KQL queries against any Kusto cluster,
measures latency across multiple iterations, and reports min / mean / median /
max / stdev timing statistics.

---

## Features

| Feature | Details |
|---|---|
| **Multiple auth methods** | Device-code, Service Principal, Username/Password, or pre-acquired token |
| **Warm-up iterations** | Discard initial "cold-cache" runs before measuring |
| **Output formats** | Human-readable table, CSV, JSON |
| **Environment overrides** | Override cluster, database, and auth via env vars |
| **CLI flags** | Override iterations, warmup, format, output file at runtime |

---

## Requirements

- Python ≥ 3.9
- `azure-kusto-data` ≥ 6.0
- `tabulate` ≥ 0.9
- `pyyaml` ≥ 6.0

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Quick Start

### 1 — Create a configuration file

Copy the example and fill in your cluster details:

```bash
cp config.yaml.example config.yaml
```

Edit `config.yaml`:

```yaml
cluster_url: "https://<your-cluster>.kusto.windows.net"
database: "<your-database>"

auth:
  method: aad_device_code   # prompts for browser login

benchmark:
  iterations: 3
  warmup_iterations: 1

output:
  format: table   # table | csv | json

queries:
  - name: "simple-count"
    query: |
      StormEvents
      | count

  - name: "top-states"
    query: |
      StormEvents
      | summarize EventCount = count() by State
      | top 10 by EventCount desc
```

### 2 — Run the benchmark

```bash
python run_benchmark.py --config config.yaml
```

Sample output:

```
| Query        |   OK |   Fail |   Min (s) |   Mean (s) |   Median (s) |   Max (s) |   Stdev (s) |
|--------------|------|--------|-----------|------------|--------------|-----------|-------------|
| simple-count |    3 |      0 |     0.231 |      0.248 |        0.245 |     0.268 |       0.019 |
| top-states   |    3 |      0 |     0.315 |      0.328 |        0.330 |     0.340 |       0.013 |

Total wall-clock time: 4.856s
```

---

## Authentication

### Interactive (device code)

```yaml
auth:
  method: aad_device_code
```

### Service Principal

```yaml
auth:
  method: aad_application
  client_id: "<app-id>"
  tenant_id: "<tenant-id>"
  # client_secret: set via KUSTO_CLIENT_SECRET env var
```

### Environment variables

| Variable | Description |
|---|---|
| `KUSTO_CLUSTER_URL` | Override `cluster_url` in config |
| `KUSTO_DATABASE` | Override `database` in config |
| `KUSTO_CLIENT_ID` | AAD application client ID |
| `KUSTO_CLIENT_SECRET` | AAD application client secret |
| `KUSTO_TENANT_ID` | AAD tenant ID |

---

## CLI Reference

```
usage: run_benchmark [-h] --config FILE [--format {table,csv,json}]
                     [--output FILE] [--iterations N] [--warmup N]

options:
  --config FILE          Path to the YAML configuration file (required)
  --format {table,csv,json}
                         Output format (overrides config)
  --output FILE          Write results to FILE instead of stdout
  --iterations N         Measured iterations per query (overrides config)
  --warmup N             Warm-up iterations per query (overrides config)
```

### Examples

```bash
# Run with default table output
python run_benchmark.py --config config.yaml

# Output JSON to a file
python run_benchmark.py --config config.yaml --format json --output results.json

# Quick single-iteration run
python run_benchmark.py --config config.yaml --iterations 1 --warmup 0
```

---

## Project Structure

```
KustoBench/
├── benchmark/
│   ├── __init__.py       # package metadata
│   ├── config.py         # YAML config loading + env overrides
│   ├── kusto_client.py   # KustoClient wrapper
│   ├── reporter.py       # table / CSV / JSON output
│   └── runner.py         # benchmark execution logic
├── queries/              # example KQL query files
│   ├── count.kql
│   ├── top_states.kql
│   └── events_over_time.kql
├── tests/                # pytest test suite
│   ├── test_cli.py
│   ├── test_config.py
│   ├── test_reporter.py
│   └── test_runner.py
├── config.yaml.example   # annotated configuration template
├── requirements.txt
└── run_benchmark.py      # CLI entry point
```

---

## Running the Tests

```bash
pip install -r requirements.txt pytest
python -m pytest tests/ -v
```

---

## License

MIT
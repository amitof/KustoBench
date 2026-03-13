# KustoBench

A benchmarking tool for **Azure Data Explorer (ADX)** and **ClickHouse** clusters.

## CLI

KustoBench has four commands: `--deploy`, `--destroy`, `--load`, and `--run`. Each takes an environment file that defines the target service.

### `--deploy <env_file>`

Deploy infrastructure from an environment YAML file.

```bash
# Deploy an ADX cluster
python run_benchmark.py --deploy envs/adx.yaml

# Deploy a 2-node ClickHouse cluster
python run_benchmark.py --deploy envs/clickhouse-2node.yaml
```

### `--destroy <env_file>`

Destroy infrastructure (deletes the resource group and all its resources).

```bash
python run_benchmark.py --destroy envs/adx.yaml
python run_benchmark.py --destroy envs/clickhouse-2node.yaml
```

### `--load <env_file> <dataset>`

Load a dataset (create tables + ingest data) into an environment.

```bash
# Load clickbench into ADX
python run_benchmark.py --load envs/adx.yaml clickbench

# Load clickbench into ClickHouse
python run_benchmark.py --load envs/clickhouse-2node.yaml clickbench
```

### `--run <env_file> <dataset>`

Run benchmark queries from a dataset against an environment.

```bash
# Run clickbench queries against ADX
python run_benchmark.py --run envs/adx.yaml clickbench

# Run with custom iterations and JSON output
python run_benchmark.py --run envs/adx.yaml clickbench --iterations 5 --format json
```

### Combined

Commands can be combined in a single invocation:

```bash
# Deploy + load + run
python run_benchmark.py \
    --deploy envs/adx.yaml \
    --load envs/adx.yaml clickbench \
    --run envs/adx.yaml clickbench
```
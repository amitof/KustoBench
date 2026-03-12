#!/usr/bin/env python3
"""KustoBench - CLI entry point for running benchmarks against Azure Data Explorer.

Usage::

    python run_benchmark.py --config config.yaml
    python run_benchmark.py --config config.yaml --format json --output results.json
"""

import argparse
import sys

from benchmark.config import apply_dataset, load_config
from benchmark.kusto_client import KustoBenchClient
from benchmark.reporter import report
from benchmark.runner import run_benchmark
from benchmark.setup import run_setup


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run_benchmark",
        description="Run a benchmark suite against an Azure Data Explorer (Kusto) cluster.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        metavar="FILE",
        help="Path to the YAML configuration file (default: config.yaml).",
    )
    parser.add_argument(
        "--dataset",
        default=None,
        metavar="NAME",
        help="Load a named dataset (e.g. 'clickbench'). Overrides queries in the config file.",
    )
    parser.add_argument(
        "--format",
        default=None,
        choices=["table", "csv", "json"],
        help="Output format (overrides config file setting).",
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="FILE",
        help="Write results to FILE instead of stdout.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        metavar="N",
        help="Number of measured iterations per query (overrides config).",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=None,
        metavar="N",
        help="Number of warm-up iterations per query (overrides config).",
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        default=False,
        help="Run the setup phase: drop/create table and ingest dataset files before benchmarking.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    try:
        config = load_config(args.config)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR loading configuration: {exc}", file=sys.stderr)
        return 1

    # Apply dataset if requested
    if args.dataset:
        try:
            apply_dataset(config, args.dataset)
        except FileNotFoundError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

    # Apply CLI overrides
    if args.iterations is not None:
        config["benchmark"]["iterations"] = args.iterations
    if args.warmup is not None:
        config["benchmark"]["warmup_iterations"] = args.warmup
    if args.format is not None:
        config["output"]["format"] = args.format
    if args.output is not None:
        config["output"]["file"] = args.output

    if not config.get("queries"):
        print("WARNING: No queries defined in the configuration.", file=sys.stderr)

    try:
        with KustoBenchClient(config) as client:
            _print_cluster_info(client)
            if args.setup:
                print("Running setup phase…", file=sys.stderr)
                run_setup(client, config)
            result = run_benchmark(client, config)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR during benchmark: {exc}", file=sys.stderr)
        return 1

    report(
        result,
        fmt=config["output"].get("format", "table"),
        output_file=config["output"].get("file"),
    )
    return 0


def _print_cluster_info(client: KustoBenchClient) -> None:
    """Query and display cluster hardware information."""
    info = client.get_cluster_info()
    lines = ["Cluster Info:"]
    lines.append(f"  URL:              {info['cluster_url'] or 'N/A'}")
    lines.append(f"  Nodes:            {info['nodes'] or 'N/A'}")
    cores_per = info["cores_per_node"]
    total_cores = info["total_cores"]
    if cores_per and total_cores:
        lines.append(f"  Cores per node:   {cores_per}")
        lines.append(f"  Total cores:      {total_cores}")
    else:
        lines.append(f"  Cores:            N/A")
    mem = info["memory_gb_per_node"]
    lines.append(f"  Memory per node:  {f'{mem} GB' if mem else 'N/A'}")
    print("\n".join(lines), file=sys.stderr)
    print(file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())

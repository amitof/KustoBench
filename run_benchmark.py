#!/usr/bin/env python3
"""KustoBench - CLI entry point for deploying, loading, and benchmarking.

Usage::

    # Deploy an environment
    python run_benchmark.py --deploy envs/adx-dev.yaml

    # Load a dataset into an environment
    python run_benchmark.py --load envs/adx-dev.yaml clickbench

    # Run benchmark queries against an environment
    python run_benchmark.py --run envs/adx-dev.yaml clickbench

    # All three in sequence
    python run_benchmark.py --deploy envs/adx-dev.yaml \\
        --load envs/adx-dev.yaml clickbench \\
        --run envs/adx-dev.yaml clickbench
"""

import argparse
import os
import sys
import time

from benchmark.config import apply_dataset, load_config
from benchmark.kusto_client import KustoBenchClient
from benchmark.reporter import report
from benchmark.runner import run_benchmark
from benchmark.load import run_load


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run_benchmark",
        description="Deploy, load data, and benchmark against ADX or ClickHouse clusters.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        metavar="FILE",
        help="Path to the YAML configuration file (default: config.yaml).",
    )
    parser.add_argument(
        "--deploy",
        default=None,
        metavar="ENV_FILE",
        help="Deploy infrastructure from an environment YAML file "
             "(e.g. 'envs/adx.yaml').",
    )
    parser.add_argument(
        "--destroy",
        default=None,
        metavar="ENV_FILE",
        help="Destroy infrastructure defined by an environment YAML file. "
             "Deletes the resource group and all its resources.",
    )
    parser.add_argument(
        "--load",
        default=None,
        nargs=2,
        metavar=("ENV_FILE", "DATASET"),
        help="Load a dataset into an environment. "
             "Example: --load envs/adx.yaml clickbench",
    )
    parser.add_argument(
        "--run",
        default=None,
        nargs=2,
        metavar=("ENV_FILE", "DATASET"),
        help="Run benchmark queries from a dataset against an environment. "
             "Example: --run envs/adx.yaml clickbench",
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
        "--query",
        type=int,
        nargs="+",
        default=None,
        metavar="INDEX",
        help="Run only specific queries by index (0-based). "
             "Example: --query 0 5 12",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    if not args.deploy and not args.destroy and not args.load and not args.run:
        print("ERROR: Specify at least one of --deploy, --destroy, --load, or --run.", file=sys.stderr)
        return 1

    try:
        config = load_config(args.config)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR loading configuration: {exc}", file=sys.stderr)
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

    # Cache of loaded env dicts, keyed by file path, so deploy outputs
    # carry through to subsequent --load / --run in the same invocation.
    _env_cache: dict[str, dict] = {}

    def _load_env_cached(path: str) -> dict:
        rp = os.path.normpath(path)
        if rp not in _env_cache:
            _env_cache[rp] = load_env(path)
        return _env_cache[rp]

    # ── Deploy ───────────────────────────────────────────────────────────
    if args.deploy:
        from infra.deploy import deploy_env, load_env

        try:
            env = _load_env_cached(args.deploy)
            outputs = deploy_env(env)
            # Update env with deployed connection info for subsequent commands.
            if outputs.get("clusterUri"):
                env["cluster_url"] = outputs["clusterUri"]
            if outputs.get("queryEndpoint"):
                env["host"] = outputs["queryEndpoint"]
            print(f"Infrastructure deployed ({env['type']}).", file=sys.stderr)
        except Exception as exc:
            print(f"ERROR deploying infrastructure: {exc}", file=sys.stderr)
            return 1

    # ── Destroy ──────────────────────────────────────────────────────────
    if args.destroy:
        from infra.deploy import destroy, load_env

        try:
            env = _load_env_cached(args.destroy)
            rg = env.get("deploy", {}).get("resource_group", "")
            if not rg:
                print("ERROR: No resource_group in env deploy settings.", file=sys.stderr)
                return 1
            destroy(rg)
            print(f"Resource group '{rg}' deletion initiated.", file=sys.stderr)
        except Exception as exc:
            print(f"ERROR destroying infrastructure: {exc}", file=sys.stderr)
            return 1

    # ── Load ─────────────────────────────────────────────────────────────
    if args.load:
        env_file, dataset_name = args.load
        from infra.deploy import load_env

        try:
            env = _load_env_cached(env_file)
        except (FileNotFoundError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

        _apply_env_to_config(config, env)

        try:
            apply_dataset(config, dataset_name)
        except FileNotFoundError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

        try:
            with KustoBenchClient(config) as client:
                _print_cluster_info(client)
                print(f"Loading dataset '{dataset_name}'…", file=sys.stderr)
                t0 = time.perf_counter()
                run_load(client, config)
                elapsed = time.perf_counter() - t0
                m, s = divmod(elapsed, 60)
                print(f"Load completed in {int(m)}m {s:.1f}s.", file=sys.stderr)
        except Exception as exc:
            print(f"ERROR loading data: {exc}", file=sys.stderr)
            return 1

    # ── Run ──────────────────────────────────────────────────────────────
    if args.run:
        env_file, dataset_name = args.run
        from infra.deploy import load_env

        try:
            env = _load_env_cached(env_file)
        except (FileNotFoundError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

        _apply_env_to_config(config, env)

        try:
            apply_dataset(config, dataset_name)
        except FileNotFoundError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

        if not config.get("queries"):
            print("WARNING: No queries defined.", file=sys.stderr)

        if args.query is not None:
            all_queries = config.get("queries", [])
            config["queries"] = [
                q for i, q in enumerate(all_queries) if i in args.query
            ]
            if not config["queries"]:
                print("ERROR: No queries matched the specified indices.", file=sys.stderr)
                return 1

        try:
            with KustoBenchClient(config) as client:
                _print_cluster_info(client)
                result = run_benchmark(client, config)
        except Exception as exc:
            print(f"ERROR during benchmark: {exc}", file=sys.stderr)
            return 1

        report(
            result,
            fmt=config["output"].get("format", "table"),
            output_file=config["output"].get("file"),
        )

    return 0


def _apply_env_to_config(config: dict, env: dict) -> None:
    """Merge environment connection settings into the benchmark config."""
    if env.get("type"):
        config["env_type"] = env["type"]
    if env.get("cluster_url"):
        config["cluster_url"] = env["cluster_url"]
    if env.get("database"):
        config["database"] = env["database"]
    if env.get("auth"):
        config["auth"] = env["auth"]


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

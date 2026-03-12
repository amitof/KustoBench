"""CLI entry point for KustoBench."""

from __future__ import annotations

import logging
import sys
from typing import Optional

import click

from .benchmark import BenchmarkRunner
from .config import BenchmarkConfig
from .reporter import BenchmarkReporter


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        stream=sys.stderr,
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


@click.command()
@click.option(
    "--config",
    "-c",
    "config_file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="Path to a JSON benchmark configuration file.",
)
@click.option("--cluster", envvar="KUSTO_CLUSTER", help="Kusto cluster URI.")
@click.option("--database", "-d", help="Default database name.")
@click.option(
    "--query",
    "-q",
    "inline_query",
    help="A single KQL query to benchmark (used together with --cluster and --database).",
)
@click.option(
    "--iterations", "-n", default=3, show_default=True, help="Number of timed iterations."
)
@click.option(
    "--warmup", default=1, show_default=True, help="Number of warm-up iterations (not timed)."
)
@click.option(
    "--auth",
    "auth_method",
    default="default",
    show_default=True,
    type=click.Choice(["default", "app_key", "device"]),
    help="Authentication method.",
)
@click.option("--client-id", envvar="KUSTO_CLIENT_ID", help="AAD application (client) ID.")
@click.option("--client-secret", envvar="KUSTO_CLIENT_SECRET", help="AAD application secret.")
@click.option("--tenant-id", envvar="KUSTO_TENANT_ID", help="AAD tenant ID.")
@click.option(
    "--output-format",
    "-f",
    default="table",
    show_default=True,
    type=click.Choice(["table", "csv", "json"]),
    help="Output format for the results.",
)
@click.option("--output-file", "-o", help="Write results to this file instead of stdout.")
@click.option("--verbose", "-v", is_flag=True, default=False, help="Enable verbose logging.")
@click.version_option(package_name="kustobench")
def main(
    config_file: Optional[str],
    cluster: Optional[str],
    database: Optional[str],
    inline_query: Optional[str],
    iterations: int,
    warmup: int,
    auth_method: str,
    client_id: Optional[str],
    client_secret: Optional[str],
    tenant_id: Optional[str],
    output_format: str,
    output_file: Optional[str],
    verbose: bool,
) -> None:
    """KustoBench – run benchmark queries against an Azure Data Explorer / Eventhouse cluster."""
    _setup_logging(verbose)

    if config_file:
        config = BenchmarkConfig.from_file(config_file)
    elif cluster and database and inline_query:
        config = BenchmarkConfig.from_dict(
            {
                "cluster_uri": cluster,
                "auth_method": auth_method,
                "client_id": client_id,
                "client_secret": client_secret,
                "tenant_id": tenant_id,
                "output_format": output_format,
                "output_file": output_file,
                "queries": [
                    {
                        "name": "inline_query",
                        "query": inline_query,
                        "database": database,
                        "iterations": iterations,
                        "warmup_iterations": warmup,
                    }
                ],
            }
        )
    else:
        click.echo(
            "Provide either --config or all of --cluster, --database, and --query.",
            err=True,
        )
        sys.exit(1)

    runner = BenchmarkRunner(config)
    result = runner.run()

    reporter = BenchmarkReporter(
        output_format=config.output_format,
        output_file=config.output_file,
    )
    reporter.report(result)

"""Setup phase for KustoBench – re-creates tables and ingests data."""

from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from .kusto_client import KustoBenchClient


def run_setup(client: KustoBenchClient, config: dict) -> None:
    """Execute the setup phase: drop/create table and ingest data files.

    The dataset section of *config* must contain a ``schema`` (the full
    ``.create table`` command text) and a ``data`` dict with ``table_name``
    and ``files`` (list of dicts each with a ``url`` key).

    Steps:
        1. Drop the target table (if it exists).
        2. Create the table using the schema command from the dataset.
        3. Ingest each data file via ``.ingest into table … ()``.

    Args:
        client: An initialised :class:`KustoBenchClient`.
        config: Benchmark configuration dictionary (must include ``dataset``).

    Raises:
        ValueError: If dataset metadata is missing or incomplete.
    """
    dataset = config.get("dataset")
    if not dataset:
        raise ValueError(
            "No dataset configured. Use --dataset to specify one for setup."
        )

    schema = dataset.get("schema", "")
    if not schema:
        raise ValueError("Dataset schema is empty; cannot create table.")

    data = dataset.get("data", {})
    table_name = data.get("table_name", "")
    if not table_name:
        raise ValueError("Dataset data.table_name is not set.")

    files: List[dict] = data.get("files", [])[:1]  # TODO: temporarily ingest only the first file
    parallelism = _resolve_parallelism(client, data)

    # 1. Drop the table if it already exists
    _drop_table(client, table_name)

    # 2. Create the table from the schema
    _create_table(client, schema)

    # 3. Ingest each data file
    fmt = data.get("format", "parquet")
    _ingest_files(client, table_name, fmt, files, parallelism=parallelism)


def _drop_table(client: KustoBenchClient, table_name: str) -> None:
    """Drop the table, suppressing errors if it does not exist."""
    command = f".drop table ['{table_name}'] ifexists"
    print(f"  Dropping table {table_name} (if exists)…", file=sys.stderr)
    client.execute_control(command)


def _create_table(client: KustoBenchClient, schema: str) -> None:
    """Execute the .create table command from the dataset schema."""
    # Strip comment lines from the schema text
    lines = schema.strip().splitlines()
    command = "\n".join(ln for ln in lines if not ln.lstrip().startswith("//")).strip()
    if not command:
        raise ValueError("Schema file contains no commands after stripping comments.")
    print(f"  Creating table…", file=sys.stderr)
    client.execute_control(command)


def _resolve_parallelism(client: KustoBenchClient, data: dict) -> int:
    """Determine ingestion parallelism from cluster core count.

    Uses ``data["ingestion_parallelism_factor"]`` (default 0.75) multiplied
    by the cluster's total core count.  Falls back to 1 if the cluster info
    is unavailable.
    """
    factor = float(data.get("ingestion_parallelism_factor", 0.75))
    try:
        info = client.get_cluster_info()
        total_cores = info.get("total_cores")
        if total_cores and total_cores > 0:
            parallelism = max(1, int(factor * total_cores))
            print(f"  Cluster has {total_cores} cores → ingestion parallelism = {parallelism}",
                  file=sys.stderr)
            return parallelism
    except Exception:  # noqa: BLE001
        pass
    print("  Could not determine cluster cores; using parallelism = 1",
          file=sys.stderr)
    return 1


def _ingest_files(
    client: KustoBenchClient,
    table_name: str,
    fmt: str,
    files: List[dict],
    *,
    parallelism: int = 1,
) -> None:
    """Ingest data files into the table."""
    valid_files = [f for f in files if f.get("url")]
    if not valid_files:
        print("  WARNING: No data files to ingest.", file=sys.stderr)
        return

    total = len(valid_files)
    parallelism = max(1, min(parallelism, total))
    print(f"  Ingesting {total} file(s) with parallelism={parallelism}…",
          file=sys.stderr)

    def _ingest_one(idx_and_entry):
        idx, file_entry = idx_and_entry
        url = file_entry["url"]
        command = (
            f".ingest into table ['{table_name}'] "
            f"('{url}') with (format='{fmt}')"
        )
        print(
            f"  Ingesting file {idx}/{total}: {url.rsplit('/', 1)[-1]}",
            file=sys.stderr,
        )
        client.execute_control(command)

    work = list(enumerate(valid_files, 1))

    if parallelism <= 1:
        for item in work:
            _ingest_one(item)
    else:
        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            futures = {pool.submit(_ingest_one, item): item for item in work}
            for future in as_completed(futures):
                future.result()  # propagate exceptions

    print(f"  Setup complete – ingested {total} file(s).", file=sys.stderr)

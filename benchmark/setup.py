"""Setup phase for KustoBench – re-creates tables and ingests data."""

from __future__ import annotations

import re
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

    files: List[dict] = data.get("files", [])
    parallelism = _resolve_parallelism(client, data)

    # 1. Drop the table if it already exists
    _drop_table(client, table_name)

    # 2. Create the table from the schema
    _create_table(client, schema)

    # 3. Build externaldata column list for type-coerced ingestion
    fmt = data.get("format", "parquet")
    ext_schema = _build_externaldata_schema(schema)

    # 4. Ingest each data file
    _ingest_files(client, table_name, fmt, files,
                  parallelism=parallelism, externaldata_schema=ext_schema)


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


def _build_externaldata_schema(schema: str) -> str:
    """Extract column definitions from the .create table schema and return
    an ``externaldata()`` column list like ``Col1:type1, Col2:type2, …``.

    This is used to build ``.set-or-append`` commands that read parquet
    files via ``externaldata()``.  The inline schema forces KQL to coerce
    Parquet BYTE_ARRAY to ``string``, INT64 timestamps to ``datetime``, etc.
    """
    clean_lines = [ln for ln in schema.strip().splitlines()
                   if not ln.lstrip().startswith("//")]
    clean_schema = "\n".join(clean_lines)
    pairs = re.findall(r'^\s+(\w+)\s*:\s*(\w+)', clean_schema, re.MULTILINE)
    if not pairs:
        raise ValueError(
            "Could not extract columns from schema for externaldata()."
        )
    return ", ".join(f"{name}:{typ}" for name, typ in pairs)


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
    externaldata_schema: str = "",
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
        # Use .set-or-append with externaldata() so KQL coerces types
        # (BYTE_ARRAY→string, INT64→datetime, etc.)
        command = (
            f".set-or-append ['{table_name}'] <| "
            f"externaldata({externaldata_schema}) "
            f"[@'{url}'] with (format='{fmt}')"
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

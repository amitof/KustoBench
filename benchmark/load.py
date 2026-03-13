"""Load phase for KustoBench – re-creates tables and ingests data."""

from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List


def _split_kql_commands(text: str) -> List[str]:
    """Split a block of KQL text into individual dot-commands.

    Each command starts with a line beginning with '.' (e.g. .create, .alter).
    Multi-line commands (like .create table with column definitions) are kept
    together until the next dot-command line.
    """
    commands: List[str] = []
    current_lines: List[str] = []
    for line in text.splitlines():
        if re.match(r'^\.\w', line) and current_lines:
            commands.append("\n".join(current_lines).strip())
            current_lines = []
        current_lines.append(line)
    if current_lines:
        commands.append("\n".join(current_lines).strip())
    return [c for c in commands if c]


def run_load(client, config: dict) -> None:
    """Execute the load phase: drop/create table and ingest data files.

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
    env_type = config.get("env_type", "adx")

    if env_type == "clickhouse":
        _run_load_clickhouse(client, config, table_name, schema, data, files)
    else:
        _run_load_adx(client, table_name, schema, data, files)

    _print_table_size(client, table_name, env_type)


def _run_load_adx(client, table_name: str, schema: str, data: dict, files: List[dict]) -> None:
    """ADX load path: .drop/.create table + .ingest."""
    parallelism = _resolve_parallelism(client, data)

    # 1. Drop
    command = f".drop table ['{table_name}'] ifexists"
    print(f"  Dropping table {table_name} (if exists)…", file=sys.stderr)
    client.execute_control(command)

    # 2. Create table and run post-create commands (e.g. .alter column)
    lines = schema.strip().splitlines()
    cleaned = "\n".join(ln for ln in lines if not ln.lstrip().startswith("//")).strip()
    if not cleaned:
        raise ValueError("Schema file contains no commands after stripping comments.")

    # Split on lines that start with a dot-command (e.g. .create, .alter)
    commands = _split_kql_commands(cleaned)
    print(f"  Creating table…", file=sys.stderr)
    client.execute_control(commands[0])

    # Run any additional commands (e.g. .alter column encoding policies)
    for cmd in commands[1:]:
        print(f"  Running: {cmd[:80]}…", file=sys.stderr)
        client.execute_control(cmd)

    # 3. Ingest
    fmt = data.get("format", "parquet")
    storage_key = data.get("storage_key", "")
    _ingest_files(client, table_name, fmt, files, storage_key=storage_key, parallelism=parallelism)


def _run_load_clickhouse(client, config: dict, table_name: str, schema: str, data: dict, files: List[dict]) -> None:
    """ClickHouse load path: DROP/CREATE TABLE + INSERT FROM url()."""
    parallelism = _resolve_parallelism(client, data)

    # 1. Drop
    print(f"  Dropping table {table_name} (if exists)…", file=sys.stderr)
    client.execute_control(f"DROP TABLE IF EXISTS {table_name}")

    # 2. Create using SQL schema
    sql_schema = config.get("dataset", {}).get("schema_sql", "")
    create_stmt = sql_schema or schema
    lines = create_stmt.strip().splitlines()
    command = "\n".join(ln for ln in lines if not ln.lstrip().startswith("--")).strip()
    if not command:
        raise ValueError("SQL schema is empty; cannot create table.")
    print(f"  Creating table…", file=sys.stderr)
    client.execute_control(command)

    # Validate table exists
    result = client._query_raw(f"EXISTS TABLE {table_name}").strip()
    if result != "1":
        raise RuntimeError(f"Table '{table_name}' was not created successfully.")
    print(f"  Table '{table_name}' verified.", file=sys.stderr)

    # 3. Ingest via INSERT INTO ... SELECT FROM url()
    fmt = data.get("format", "csv")
    ch_format = {"csv": "CSV", "tsv": "TabSeparated", "parquet": "Parquet"}.get(fmt, "CSV")
    storage_key = data.get("storage_key", "")
    valid_files = [f for f in files if f.get("url")]
    total = len(valid_files)
    parallelism = max(1, min(parallelism, total))
    print(f"  Ingesting {total} file(s) with parallelism={parallelism}…", file=sys.stderr)

    settings = "SETTINGS max_threads = 1, input_format_allow_errors_num = 100, input_format_allow_errors_ratio = 0.01"
    settings_params = {
        "max_threads": "1",
        "input_format_allow_errors_num": "100",
        "input_format_allow_errors_ratio": "0.01",
        "input_format_csv_skip_first_lines": "0",
    }
    failed: list = []

    def _ingest_one(idx_and_entry):
        idx, file_entry = idx_and_entry
        url = file_entry["url"]
        fname = url.rsplit('/', 1)[-1]
        print(f"  Ingesting file {idx}/{total}: {fname}", file=sys.stderr)

        if storage_key:
            insert_sql = (
                f"INSERT INTO {table_name} "
                f"SELECT * FROM azureBlobStorage("
                f"'{_blob_account_url(url)}', "
                f"'{_blob_container(url)}', "
                f"'{_blob_path(url)}', "
                f"'{_blob_account_name(url)}', "
                f"'{storage_key}', "
                f"'{ch_format}')"
            )
        else:
            insert_sql = (
                f"INSERT INTO {table_name} "
                f"SELECT * FROM url('{url}', '{ch_format}')"
            )
        try:
            client._query_raw(insert_sql, extra_params=settings_params)
        except Exception as exc:
            print(f"  ERROR ingesting {fname}: {exc}", file=sys.stderr)
            failed.append(fname)

    work = list(enumerate(valid_files, 1))

    if parallelism <= 1:
        for item in work:
            _ingest_one(item)
    else:
        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            futures = {pool.submit(_ingest_one, item): item for item in work}
            for future in as_completed(futures):
                future.result()

    if failed:
        print(f"  WARNING: {len(failed)}/{total} file(s) failed: {', '.join(failed)}", file=sys.stderr)
    print(f"  Load complete – ingested {total - len(failed)}/{total} file(s).", file=sys.stderr)


def _blob_account_url(url: str) -> str:
    """Extract 'https://account.blob.core.windows.net' from a blob URL."""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.hostname}"


def _blob_account_name(url: str) -> str:
    """Extract storage account name from a blob URL hostname."""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    return parsed.hostname.split(".")[0] if parsed.hostname else ""


def _blob_container(url: str) -> str:
    """Extract container name from a blob URL path."""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")
    return parts[0] if parts else ""


def _blob_path(url: str) -> str:
    """Extract blob path (after container) from a blob URL."""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/", 1)
    return parts[1] if len(parts) > 1 else ""


def _print_table_size(client, table_name: str, env_type: str) -> None:
    """Query and print row count and table size after ingestion."""
    try:
        if env_type == "clickhouse":
            raw_count = client._query_raw(f"SELECT count() FROM {table_name}").strip()
            row_count = int(raw_count)
            raw_size = client._query_raw(
                f"SELECT sum(bytes_on_disk) FROM system.parts "
                f"WHERE table = '{table_name}' AND active"
            ).strip()
            size_bytes = int(raw_size)
        else:
            resp_count = client._client.execute(client._database, f"{table_name} | count")
            primary = resp_count.primary_results[0] if resp_count.primary_results else None
            row_count = int(primary[0][0]) if primary and len(primary) > 0 else 0
            resp_size = client._client.execute_mgmt(
                client._database,
                f".show table ['{table_name}'] extents | summarize sum(ExtentSize)"
            )
            primary = resp_size.primary_results[0] if resp_size.primary_results else None
            size_bytes = int(primary[0][0]) if primary and len(primary) > 0 else 0

        gb = size_bytes / (1024 ** 3)
        print(f"  Table '{table_name}': {row_count:,} rows, {size_bytes:,} bytes ({gb:.2f} GB)",
              file=sys.stderr)
    except Exception as exc:
        print(f"  WARNING: Could not determine table stats: {exc}", file=sys.stderr)


def _resolve_parallelism(client, data: dict) -> int:
    """Determine ingestion parallelism as 75% of the cluster's total cores."""
    try:
        info = client.get_cluster_info()
        total_cores = info.get("total_cores")
        if total_cores and total_cores > 0:
            parallelism = max(1, int(0.75 * total_cores))
            print(f"  Cluster has {total_cores} cores → ingestion parallelism = {parallelism}",
                  file=sys.stderr)
            return parallelism
    except Exception:  # noqa: BLE001
        pass
    print("  Could not determine cluster cores; using parallelism = 1",
          file=sys.stderr)
    return 1


def _ingest_files(
    client,
    table_name: str,
    fmt: str,
    files: List[dict],
    *,
    storage_key: str = "",
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
        if storage_key:
            source = f"h'{url};{storage_key}'"
        else:
            source = f"'{url}'"
        command = (
            f".ingest into table ['{table_name}'] "
            f"({source}) with (format='{fmt}')"
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

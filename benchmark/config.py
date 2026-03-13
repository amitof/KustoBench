"""Configuration loading for KustoBench."""

import glob
import json
import os
import yaml


DATASETS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "datasets")


DEFAULT_CONFIG = {
    "cluster_url": "",
    "database": "",
    "auth": {
        "method": "aad_device_code",
    },
    "benchmark": {
        "iterations": 3,
        "warmup_iterations": 1,
        "timeout_seconds": 300,
        "concurrent": False,
        "concurrency": 1,
    },
    "output": {
        "format": "table",
        "file": None,
    },
    "queries": [],
}


def load_config(path: str) -> dict:
    """Load benchmark configuration from a YAML file.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        Configuration dictionary merged with defaults.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        yaml.YAMLError: If the configuration file is not valid YAML.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path, "r", encoding="utf-8") as fh:
        user_config = yaml.safe_load(fh) or {}

    config = _deep_merge(DEFAULT_CONFIG, user_config)
    _apply_env_overrides(config)
    return config


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into a copy of *base*."""
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _apply_env_overrides(config: dict) -> None:
    """Apply environment variable overrides to the configuration.

    Supported environment variables:
        KUSTO_CLUSTER_URL  - overrides config["cluster_url"]
        KUSTO_DATABASE     - overrides config["database"]
        KUSTO_CLIENT_ID    - AAD application (service principal) client ID
        KUSTO_CLIENT_SECRET - AAD application client secret
        KUSTO_TENANT_ID    - AAD tenant ID
    """
    if os.environ.get("KUSTO_CLUSTER_URL"):
        config["cluster_url"] = os.environ["KUSTO_CLUSTER_URL"]
    if os.environ.get("KUSTO_DATABASE"):
        config["database"] = os.environ["KUSTO_DATABASE"]
    if os.environ.get("KUSTO_CLIENT_ID"):
        config.setdefault("auth", {})["client_id"] = os.environ["KUSTO_CLIENT_ID"]
    if os.environ.get("KUSTO_CLIENT_SECRET"):
        config.setdefault("auth", {})["client_secret"] = os.environ["KUSTO_CLIENT_SECRET"]
    if os.environ.get("KUSTO_TENANT_ID"):
        config.setdefault("auth", {})["tenant_id"] = os.environ["KUSTO_TENANT_ID"]


def load_dataset(name: str) -> dict:
    """Load a named dataset definition from the datasets directory.

    A dataset bundles a table schema, data-file references, and benchmark
    queries so that ``--dataset clickbench`` is all a user needs to specify.

    Args:
        name: Dataset name (must match a subdirectory under ``datasets/``).

    Returns:
        A dictionary with keys ``name``, ``description``, ``data``,
        ``schema``, and ``queries`` (list of ``{name, query}`` dicts).

    Raises:
        FileNotFoundError: If the dataset directory or required files are
            missing.
    """
    dataset_dir = os.path.join(DATASETS_DIR, name)
    manifest_path = os.path.join(dataset_dir, "dataset.yaml")

    if not os.path.isfile(manifest_path):
        raise FileNotFoundError(
            f"Dataset '{name}' not found (expected {manifest_path})."
        )

    with open(manifest_path, "r", encoding="utf-8") as fh:
        manifest = yaml.safe_load(fh) or {}

    # Read schema file
    schema_file = manifest.get("schema_file", "schema.kql")
    schema_path = os.path.join(dataset_dir, schema_file)
    schema_text = ""
    if os.path.isfile(schema_path):
        with open(schema_path, "r", encoding="utf-8") as fh:
            schema_text = fh.read()

    # Read SQL schema file (for ClickHouse)
    sql_schema_file = manifest.get("schema_sql_file", "schema.sql")
    sql_schema_path = os.path.join(dataset_dir, sql_schema_file)
    sql_schema_text = ""
    if os.path.isfile(sql_schema_path):
        with open(sql_schema_path, "r", encoding="utf-8") as fh:
            sql_schema_text = fh.read()

    # Read query files from the queries directory
    queries_dir = manifest.get("queries_dir", "queries")
    queries_path = os.path.join(dataset_dir, queries_dir)
    queries = []
    if os.path.isdir(queries_path):
        for query_file in sorted(glob.glob(os.path.join(queries_path, "*.query"))):
            query_name = os.path.splitext(os.path.basename(query_file))[0]
            with open(query_file, "r", encoding="utf-8") as fh:
                parsed = json.load(fh)
            parsed["name"] = query_name
            queries.append(parsed)

    return {
        "name": manifest.get("name", name),
        "description": manifest.get("description", ""),
        "data": manifest.get("data", {}),
        "schema": schema_text,
        "schema_sql": sql_schema_text,
        "queries": queries,
    }


def apply_dataset(config: dict, dataset_name: str) -> dict:
    """Merge a named dataset's queries into the benchmark configuration.

    Loads the dataset and sets ``config["queries"]`` from the dataset's
    query files.  The dataset metadata is stored under ``config["dataset"]``.
    Selects the appropriate query dialect (kql or sql) based on
    ``config["env_type"]``.

    Args:
        config: The benchmark configuration dictionary (modified in place).
        dataset_name: Name of the dataset to load.

    Returns:
        The modified *config*.
    """
    ds = load_dataset(dataset_name)
    config["dataset"] = {
        "name": ds["name"],
        "description": ds["description"],
        "data": ds["data"],
        "schema": ds["schema"],
        "schema_sql": ds.get("schema_sql", ""),
    }

    env_type = config.get("env_type", "adx")
    dialect = "sql" if env_type == "clickhouse" else "kql"
    resolved = []
    for q in ds["queries"]:
        query_text = q.get(dialect, "") or q.get("kql", "")
        resolved.append({"name": q["name"], "query": query_text})
    config["queries"] = resolved
    return config

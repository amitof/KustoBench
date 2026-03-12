"""Configuration loading for KustoBench."""

import os
import yaml


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

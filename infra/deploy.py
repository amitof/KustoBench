"""Infrastructure deployment for KustoBench.

Drives Azure Bicep deployments for ADX clusters and ClickHouse VM sets.
Env config files (YAML) define environment type and deployment parameters.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.parse
from datetime import datetime
from typing import Optional

import yaml

INFRA_DIR = os.path.dirname(os.path.abspath(__file__))


def load_env(path: str) -> dict:
    """Load an environment config file.

    Args:
        path: Path to the YAML environment file.

    Returns:
        Parsed environment dictionary.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is missing a ``type`` field.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Environment file not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        env = yaml.safe_load(fh) or {}
    if "type" not in env:
        raise ValueError(f"Environment file {path} must have a 'type' field (adx or clickhouse).")
    return env


def deploy_env(env: dict) -> dict:
    """Deploy infrastructure defined by an environment config.

    Dispatches to :func:`deploy_adx` or :func:`deploy_clickhouse` based on
    ``env["type"]``.

    Args:
        env: Parsed environment dictionary (from :func:`load_env`).

    Returns:
        Deployment outputs dict.
    """
    env_type = env["type"]
    deploy_cfg = env.get("deploy", {})

    if env_type == "adx":
        return deploy_adx(
            resource_group=deploy_cfg.get("resource_group", "kustobench-rg"),
            cluster_name=deploy_cfg.get("cluster_name", "kustobench-adx"),
            location=deploy_cfg.get("location", "swedencentral"),
            sku=deploy_cfg.get("sku", "Dev(No SLA)_Standard_E2a_v4"),
            sku_tier=deploy_cfg.get("sku_tier", "Basic"),
            capacity=deploy_cfg.get("capacity", 1),
            database=deploy_cfg.get("database", "TestDB"),
        )

    if env_type == "clickhouse":
        return deploy_clickhouse(
            resource_group=deploy_cfg.get("resource_group", "kustobench-ch-rg"),
            location=deploy_cfg.get("location", "swedencentral"),
            vm_count=deploy_cfg.get("vm_count", 1),
            vm_size=deploy_cfg.get("vm_size", "Standard_E16s_v5"),
            ssh_public_key_path=deploy_cfg.get("ssh_public_key_path", "~/.ssh/id_rsa.pub"),
            admin_username=deploy_cfg.get("admin_username", "benchadmin"),
            base_name=deploy_cfg.get("base_name", "kustobench-ch"),
            storage_account_name=deploy_cfg.get("storage_account_name", "kustobenchch"),
            storage_container_name=deploy_cfg.get("storage_container_name", "clickhouse-data"),
            database=deploy_cfg.get("database", "TestDB"),
        )

    raise ValueError(f"Unknown environment type: {env_type}")


def deploy_adx(
    resource_group: str,
    cluster_name: str,
    location: str = "swedencentral",
    sku: str = "Dev(No SLA)_Standard_E2a_v4",
    sku_tier: str = "Basic",
    capacity: int = 1,
    database: str = "TestDB",
) -> dict:
    """Deploy an ADX cluster using the Bicep template.

    Returns:
        Deployment outputs dict with ``clusterUri``, ``clusterName``,
        ``databaseName``.
    """
    template = os.path.join(INFRA_DIR, "adx.bicep")
    _ensure_resource_group(resource_group, location)
    result = _deploy_bicep(
        resource_group,
        template,
        parameters={
            "clusterName": cluster_name,
            "location": location,
            "skuName": sku,
            "skuTier": sku_tier,
            "capacity": capacity,
            "databaseName": database,
        },
    )
    outputs = _extract_outputs(result)
    print(f"  ADX cluster deployed: {outputs.get('clusterUri')}", file=sys.stderr)
    return outputs


def deploy_clickhouse(
    resource_group: str,
    location: str = "swedencentral",
    vm_count: int = 1,
    vm_size: str = "Standard_E16s_v5",
    ssh_public_key_path: str = "~/.ssh/id_rsa.pub",
    admin_username: str = "benchadmin",
    base_name: str = "kustobench-ch",
    storage_account_name: str = "kustobenchch",
    storage_container_name: str = "clickhouse-data",
    database: str = "TestDB",
) -> dict:
    """Deploy ClickHouse OSS on N Linux VMs using the Bicep template.

    Returns:
        Deployment outputs dict with ``vmPublicIps``, ``vmPrivateIps``,
        ``queryEndpoint``.
    """
    template = os.path.join(INFRA_DIR, "clickhouse.bicep")
    ssh_key_path = os.path.expanduser(ssh_public_key_path)
    if not os.path.isfile(ssh_key_path):
        raise FileNotFoundError(f"SSH public key not found: {ssh_key_path}")
    with open(ssh_key_path, "r", encoding="utf-8") as fh:
        ssh_public_key = fh.read().strip()

    _ensure_resource_group(resource_group, location)
    result = _deploy_bicep(
        resource_group,
        template,
        parameters={
            "vmCount": vm_count,
            "vmSize": vm_size,
            "location": location,
            "adminUsername": admin_username,
            "sshPublicKey": ssh_public_key,
            "baseName": base_name,
            "storageAccountName": storage_account_name,
            "storageContainerName": storage_container_name,
        },
    )
    outputs = _extract_outputs(result)
    query_endpoint = outputs.get('queryEndpoint', '')
    print(
        f"  ClickHouse deployed: {vm_count} node(s), "
        f"query endpoint = {query_endpoint}",
        file=sys.stderr,
    )

    if database and query_endpoint:
        _create_clickhouse_database(query_endpoint, database)

    return outputs


def destroy(resource_group: str) -> None:
    """Delete the resource group and all its resources, waiting for completion."""
    print(f"  Deleting resource group {resource_group}…", file=sys.stderr)
    subprocess.run(
        ["az", "group", "delete", "--name", resource_group, "--yes", "--no-wait"],
        check=True,
        shell=True,
    )
    while True:
        result = subprocess.run(
            ["az", "group", "exists", "--name", resource_group],
            capture_output=True, text=True, shell=True,
        )
        if result.stdout.strip().lower() == "false":
            break
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"  [{ts}] Still deleting…", file=sys.stderr)
        time.sleep(30)
    print(f"  Resource group '{resource_group}' deleted.", file=sys.stderr)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _create_clickhouse_database(endpoint: str, database: str) -> None:
    """Create a database on the ClickHouse cluster via its HTTP API."""
    url = f"http://{endpoint}:8123/"
    query = f"CREATE DATABASE IF NOT EXISTS {database}"
    data = query.encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp.read()
        print(f"  ClickHouse database '{database}' created.", file=sys.stderr)
    except Exception as exc:
        print(
            f"  WARNING: Could not create database '{database}': {exc}",
            file=sys.stderr,
        )


def _ensure_resource_group(name: str, location: str) -> None:
    subprocess.run(
        [
            "az", "group", "create",
            "--name", name,
            "--location", location,
            "--output", "none",
        ],
        check=True,
        shell=True,
    )


def _deploy_bicep(
    resource_group: str,
    template: str,
    parameters: dict,
    deployment_name: Optional[str] = None,
) -> dict:
    if deployment_name is None:
        deployment_name = f"kustobench-{os.path.splitext(os.path.basename(template))[0]}"

    # Write parameters to a temp JSON file to avoid shell escaping issues
    # (e.g. SSH keys with special characters).
    params_json = {k: {"value": v} for k, v in parameters.items()}
    params_file = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8",
    )
    try:
        json.dump({"$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#", "contentVersion": "1.0.0.0", "parameters": params_json}, params_file)
        params_file.close()

        cmd = [
            "az", "deployment", "group", "create",
            "--resource-group", resource_group,
            "--template-file", template,
            "--name", deployment_name,
            "--parameters", f"@{params_file.name}",
            "--output", "json",
            "--no-wait",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        if proc.returncode != 0:
            error_msg = proc.stderr.strip() or proc.stdout.strip()
            raise RuntimeError(f"Deployment submission failed: {error_msg}")
    finally:
        os.unlink(params_file.name)

    print(f"  Deployment '{deployment_name}' submitted, waiting for completion…", file=sys.stderr)

    return _wait_for_deployment(resource_group, deployment_name)


def _wait_for_deployment(
    resource_group: str,
    deployment_name: str,
    poll_interval: int = 30,
) -> dict:
    """Poll deployment status every *poll_interval* seconds until terminal."""
    terminal_states = {"Succeeded", "Failed", "Canceled"}
    while True:
        proc = subprocess.run(
            [
                "az", "deployment", "group", "show",
                "--resource-group", resource_group,
                "--name", deployment_name,
                "--output", "json",
            ],
            capture_output=True, text=True, shell=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"Failed to query deployment status: {proc.stderr.strip()}"
            )
        result = json.loads(proc.stdout)
        state = result.get("properties", {}).get("provisioningState", "Unknown")
        now = datetime.now().strftime("%H:%M:%S")
        print(f"  [{now}] Deployment state: {state}", file=sys.stderr)

        if state in terminal_states:
            if state != "Succeeded":
                error = result.get("properties", {}).get("error", {})
                raise RuntimeError(
                    f"Deployment ended with state '{state}': "
                    f"{json.dumps(error, indent=2)}"
                )
            return result

        time.sleep(poll_interval)


def _extract_outputs(result: dict) -> dict:
    raw = result.get("properties", {}).get("outputs", {})
    return {k: v["value"] for k, v in raw.items()}

"""Infrastructure deployment for KustoBench.

Drives Azure Bicep deployments for ADX clusters and ClickHouse VM sets.
Env config files (YAML) define environment type and deployment parameters.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
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
        )

    raise ValueError(f"Unknown environment type: {env_type}")


def deploy_adx(
    resource_group: str,
    cluster_name: str,
    location: str = "swedencentral",
    sku: str = "Dev(No SLA)_Standard_E2a_v4",
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
        },
    )
    outputs = _extract_outputs(result)
    print(
        f"  ClickHouse deployed: {vm_count} node(s), "
        f"query endpoint = {outputs.get('queryEndpoint')}",
        file=sys.stderr,
    )
    return outputs


def destroy(resource_group: str) -> None:
    """Delete the resource group and all its resources."""
    print(f"  Deleting resource group {resource_group}…", file=sys.stderr)
    subprocess.run(
        ["az", "group", "delete", "--name", resource_group, "--yes", "--no-wait"],
        check=True,
    )


# ── Helpers ──────────────────────────────────────────────────────────────────


def _ensure_resource_group(name: str, location: str) -> None:
    subprocess.run(
        [
            "az", "group", "create",
            "--name", name,
            "--location", location,
            "--output", "none",
        ],
        check=True,
    )


def _deploy_bicep(
    resource_group: str,
    template: str,
    parameters: dict,
    deployment_name: Optional[str] = None,
) -> dict:
    if deployment_name is None:
        deployment_name = f"kustobench-{os.path.splitext(os.path.basename(template))[0]}"

    param_args = []
    for k, v in parameters.items():
        param_args.append(f"{k}={json.dumps(v)}")

    cmd = [
        "az", "deployment", "group", "create",
        "--resource-group", resource_group,
        "--template-file", template,
        "--name", deployment_name,
        "--parameters", *param_args,
        "--output", "json",
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return json.loads(proc.stdout)


def _extract_outputs(result: dict) -> dict:
    raw = result.get("properties", {}).get("outputs", {})
    return {k: v["value"] for k, v in raw.items()}

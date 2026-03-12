"""Kusto client wrapper for KustoBench."""

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError


def build_connection_string(config: dict) -> KustoConnectionStringBuilder:
    """Build a KustoConnectionStringBuilder from the configuration.

    Supported auth methods (config["auth"]["method"]):
        - ``aad_device_code``  : interactive device code flow (default)
        - ``aad_application``  : service principal (client_id + client_secret + tenant_id)
        - ``aad_user_password``: username / password
        - ``token``            : pre-acquired bearer token (config["auth"]["token"])

    Args:
        config: Benchmark configuration dictionary.

    Returns:
        A configured :class:`KustoConnectionStringBuilder`.

    Raises:
        ValueError: If required auth parameters are missing.
    """
    cluster_url = config.get("cluster_url", "")
    if not cluster_url:
        raise ValueError("cluster_url must be set in the configuration.")

    auth = config.get("auth", {})
    method = auth.get("method", "aad_device_code")

    if method == "aad_application":
        client_id = auth.get("client_id") or ""
        client_secret = auth.get("client_secret") or ""
        tenant_id = auth.get("tenant_id") or ""
        if not all([client_id, client_secret, tenant_id]):
            raise ValueError(
                "auth.method='aad_application' requires client_id, client_secret and tenant_id."
            )
        return KustoConnectionStringBuilder.with_aad_application_key_authentication(
            cluster_url, client_id, client_secret, tenant_id
        )

    if method == "aad_user_password":
        username = auth.get("username") or ""
        password = auth.get("password") or ""
        tenant_id = auth.get("tenant_id") or ""
        if not all([username, password]):
            raise ValueError(
                "auth.method='aad_user_password' requires username and password."
            )
        return KustoConnectionStringBuilder.with_aad_user_password_authentication(
            cluster_url, username, password, tenant_id
        )

    if method == "token":
        token = auth.get("token") or ""
        if not token:
            raise ValueError("auth.method='token' requires auth.token to be set.")
        return KustoConnectionStringBuilder.with_token_provider(
            cluster_url, lambda: token
        )

    # Default: interactive device code
    return KustoConnectionStringBuilder.with_aad_device_authentication(cluster_url)


class KustoBenchClient:
    """Thin wrapper around :class:`KustoClient` for benchmarking purposes."""

    def __init__(self, config: dict) -> None:
        kcsb = build_connection_string(config)
        self._client = KustoClient(kcsb)
        self._database = config.get("database", "")
        if not self._database:
            raise ValueError("database must be set in the configuration.")

    def execute(self, query: str) -> dict:
        """Execute a KQL query and return a summary of the results.

        Args:
            query: The KQL query string to execute.

        Returns:
            A dict with keys:
                - ``row_count``: number of rows returned.
                - ``column_count``: number of columns in the primary result.

        Raises:
            KustoServiceError: On server-side errors.
        """
        response = self._client.execute(self._database, query)
        primary = response.primary_results[0] if response.primary_results else None
        row_count = len(primary) if primary is not None else 0
        column_count = len(primary.columns) if primary is not None else 0
        return {"row_count": row_count, "column_count": column_count}

    def close(self) -> None:
        """Close the underlying Kusto client."""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

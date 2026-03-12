"""Kusto client wrapper for KustoBench."""

from __future__ import annotations

from azure.identity import DefaultAzureCredential
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError


class KustoBenchClient:
    """Thin wrapper around :class:`azure.kusto.data.KustoClient`."""

    def __init__(self, cluster_uri: str, kcsb: KustoConnectionStringBuilder) -> None:
        self._cluster_uri = cluster_uri
        self._client = KustoClient(kcsb)

    # ------------------------------------------------------------------
    # Factory helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_default_auth(
        cls,
        cluster_uri: str,
        request_app_name: str = "KustoBench",
    ) -> "KustoBenchClient":
        """Create a client using ``DefaultAzureCredential`` (env vars, managed identity, etc.)."""
        credential = DefaultAzureCredential()
        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
            cluster_uri, credential
        )
        kcsb.application_name_for_tracing = request_app_name
        return cls(cluster_uri, kcsb)

    @classmethod
    def from_app_key(
        cls,
        cluster_uri: str,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        request_app_name: str = "KustoBench",
    ) -> "KustoBenchClient":
        """Create a client using an AAD application key (service principal)."""
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            cluster_uri, client_id, client_secret, tenant_id
        )
        kcsb.application_name_for_tracing = request_app_name
        return cls(cluster_uri, kcsb)

    @classmethod
    def from_device_auth(
        cls,
        cluster_uri: str,
        tenant_id: str | None = None,
        request_app_name: str = "KustoBench",
    ) -> "KustoBenchClient":
        """Create a client using the AAD device-code flow."""
        if tenant_id:
            kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(
                cluster_uri, tenant_id
            )
        else:
            kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(
                cluster_uri
            )
        kcsb.application_name_for_tracing = request_app_name
        return cls(cluster_uri, kcsb)

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute(
        self,
        database: str,
        query: str,
        timeout_seconds: int = 300,
    ):
        """Execute a KQL query and return the raw response.

        Raises :class:`azure.kusto.data.exceptions.KustoServiceError` on
        server-side failures.
        """
        from azure.kusto.data.request import ClientRequestProperties

        props = ClientRequestProperties()
        props.set_option(
            ClientRequestProperties.request_timeout_option_name,
            timeout_seconds,
        )
        try:
            return self._client.execute(database, query, props)
        except KustoServiceError:
            raise

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._client.close()

    def __enter__(self) -> "KustoBenchClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()

#!/usr/bin/env bash
# install-clickhouse.sh — Bootstrap ClickHouse OSS on an Ubuntu VM.
# Usage: install-clickhouse.sh <node_index> <node_count> <ip1,ip2,...> <storage_account> <storage_key> <container>
set -euo pipefail

NODE_INDEX="${1:?node index required}"
NODE_COUNT="${2:?node count required}"
NODE_IPS="${3:?comma-separated node IPs required}"
STORAGE_ACCOUNT="${4:?storage account name required}"
STORAGE_KEY="${5:?storage account key required}"
STORAGE_CONTAINER="${6:?storage container name required}"

# ── Install ClickHouse ──────────────────────────────────────────────────────
for i in 1 2 3; do
  apt-get update -qq && break
  echo "apt-get update failed (attempt $i), retrying in 10s…"
  sleep 10
done
apt-get install -y -qq apt-transport-https ca-certificates curl gnupg

curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' \
  | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] \
https://packages.clickhouse.com/deb stable main" \
  > /etc/apt/sources.list.d/clickhouse.list

apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq clickhouse-server clickhouse-client

# ── Configure cluster topology ──────────────────────────────────────────────
IFS=',' read -ra IPS <<< "$NODE_IPS"

# Build <remote_servers> XML fragment
SHARDS=""
for ip in "${IPS[@]}"; do
  SHARDS+="
            <shard>
                <replica>
                    <host>${ip}</host>
                    <port>9000</port>
                </replica>
            </shard>"
done

cat > /etc/clickhouse-server/config.d/cluster.xml << EOF
<clickhouse>
    <remote_servers>
        <kustobench_cluster>
            ${SHARDS}
        </kustobench_cluster>
    </remote_servers>
    <listen_host>0.0.0.0</listen_host>
    <macros>
        <shard>${NODE_INDEX}</shard>
        <replica>${NODE_INDEX}</replica>
    </macros>
</clickhouse>
EOF

# ── Configure storage: Azure Blob + local SSD cache ─────────────────────────
mkdir -p /var/lib/clickhouse/disks/azure_blob_cache

cat > /etc/clickhouse-server/config.d/storage.xml << EOF
<clickhouse>
    <storage_configuration>
        <disks>
            <azure_blob>
                <type>azure_blob_storage</type>
                <storage_account_url>https://${STORAGE_ACCOUNT}.blob.core.windows.net</storage_account_url>
                <container_name>${STORAGE_CONTAINER}</container_name>
                <account_name>${STORAGE_ACCOUNT}</account_name>
                <account_key>${STORAGE_KEY}</account_key>
            </azure_blob>
            <azure_blob_cache>
                <type>cache</type>
                <disk>azure_blob</disk>
                <path>/var/lib/clickhouse/disks/azure_blob_cache/</path>
                <max_size>200Gi</max_size>
                <do_not_evict_index_and_mark_files>true</do_not_evict_index_and_mark_files>
            </azure_blob_cache>
        </disks>
        <policies>
            <blob_cached>
                <volumes>
                    <main>
                        <disk>azure_blob_cache</disk>
                    </main>
                </volumes>
            </blob_cached>
        </policies>
    </storage_configuration>
</clickhouse>
EOF

chown -R clickhouse:clickhouse /var/lib/clickhouse/disks

# ── Start ClickHouse ────────────────────────────────────────────────────────
systemctl enable clickhouse-server
systemctl restart clickhouse-server

echo "ClickHouse node ${NODE_INDEX}/${NODE_COUNT} ready."

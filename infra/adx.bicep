// infra/adx.bicep
// Deploys an Azure Data Explorer (Kusto) cluster + database.

@description('Cluster name (globally unique).')
param clusterName string

@description('Azure region.')
param location string = resourceGroup().location

@description('VM SKU for cluster nodes.')
param skuName string = 'Dev(No SLA)_Standard_E2a_v4'

@description('Number of cluster nodes.')
param capacity int = 1

@description('SKU tier.')
@allowed(['Basic', 'Standard'])
param skuTier string = 'Basic'

@description('Database name.')
param databaseName string = 'TestDB'

@description('Database soft-delete period (ISO 8601).')
param softDeletePeriod string = 'P365D'

@description('Database hot-cache period (ISO 8601).')
param hotCachePeriod string = 'P31D'

resource cluster 'Microsoft.Kusto/clusters@2023-08-15' = {
  name: clusterName
  location: location
  sku: {
    name: skuName
    tier: skuTier
    capacity: capacity
  }
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    enableStreamingIngest: false
    enablePurge: false
  }
}

resource database 'Microsoft.Kusto/clusters/databases@2023-08-15' = {
  parent: cluster
  name: databaseName
  location: location
  kind: 'ReadWrite'
  properties: {
    softDeletePeriod: softDeletePeriod
    hotCachePeriod: hotCachePeriod
  }
}

output clusterUri string = cluster.properties.uri
output clusterName string = cluster.name
output databaseName string = database.name

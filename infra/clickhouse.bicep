// infra/clickhouse.bicep
// Deploys N Linux VMs with ClickHouse OSS installed.

@description('Number of ClickHouse nodes.')
@minValue(1)
param vmCount int = 1

@description('VM SKU.')
param vmSize string = 'Standard_E16s_v5'

@description('Azure region.')
param location string = resourceGroup().location

@description('Admin username for VMs.')
param adminUsername string = 'benchadmin'

@description('SSH public key for admin user.')
@secure()
param sshPublicKey string

@description('Base name for resources.')
param baseName string = 'kustobench-ch'

@description('Storage account name for durable blob storage backend.')
param storageAccountName string = '${baseName}sa'

@description('Blob container name for ClickHouse data.')
param storageContainerName string = 'clickhouse-data'

// ── Storage Account ─────────────────────────────────────────────────────────

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    allowBlobPublicAccess: false
    minimumTlsVersion: 'TLS1_2'
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: storageContainerName
}

// ── Network ─────────────────────────────────────────────────────────────────

resource vnet 'Microsoft.Network/virtualNetworks@2023-09-01' = {
  name: '${baseName}-vnet'
  location: location
  properties: {
    addressSpace: { addressPrefixes: ['10.0.0.0/16'] }
    subnets: [
      {
        name: 'default'
        properties: {
          addressPrefix: '10.0.0.0/24'
          networkSecurityGroup: { id: nsg.id }
        }
      }
    ]
  }
}

resource nsg 'Microsoft.Network/networkSecurityGroups@2023-09-01' = {
  name: '${baseName}-nsg'
  location: location
  properties: {
    securityRules: [
      {
        name: 'AllowSSH'
        properties: {
          priority: 1000
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '22'
        }
      }
      {
        name: 'AllowClickHouseHTTP'
        properties: {
          priority: 1010
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '8123'
        }
      }
      {
        name: 'AllowClickHouseNative'
        properties: {
          priority: 1020
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '9000'
        }
      }
      {
        name: 'AllowInterServer'
        properties: {
          priority: 1030
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourceAddressPrefix: '10.0.0.0/24'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '9009'
        }
      }
    ]
  }
}

// ── Per-node resources ──────────────────────────────────────────────────────

resource publicIps 'Microsoft.Network/publicIPAddresses@2023-09-01' = [
  for i in range(0, vmCount): {
    name: '${baseName}-pip-${i}'
    location: location
    sku: { name: 'Standard' }
    properties: {
      publicIPAllocationMethod: 'Static'
    }
  }
]

resource nics 'Microsoft.Network/networkInterfaces@2023-09-01' = [
  for i in range(0, vmCount): {
    name: '${baseName}-nic-${i}'
    location: location
    properties: {
      ipConfigurations: [
        {
          name: 'ipconfig1'
          properties: {
            privateIPAllocationMethod: 'Static'
            privateIPAddress: '10.0.0.${10 + i}'
            subnet: { id: vnet.properties.subnets[0].id }
            publicIPAddress: { id: publicIps[i].id }
          }
        }
      ]
    }
  }
]

// Build the list of private IPs for the cluster config.
var nodePrivateIps = [for i in range(0, vmCount): '10.0.0.${10 + i}']

resource vms 'Microsoft.Compute/virtualMachines@2023-09-01' = [
  for i in range(0, vmCount): {
    name: '${baseName}-vm-${i}'
    location: location
    properties: {
      hardwareProfile: { vmSize: vmSize }
      osProfile: {
        computerName: '${baseName}-${i}'
        adminUsername: adminUsername
        linuxConfiguration: {
          disablePasswordAuthentication: true
          ssh: {
            publicKeys: [
              {
                path: '/home/${adminUsername}/.ssh/authorized_keys'
                keyData: sshPublicKey
              }
            ]
          }
        }
      }
      storageProfile: {
        imageReference: {
          publisher: 'Canonical'
          offer: '0001-com-ubuntu-server-jammy'
          sku: '22_04-lts-gen2'
          version: 'latest'
        }
        osDisk: {
          createOption: 'FromImage'
          managedDisk: { storageAccountType: 'Premium_LRS' }
          diskSizeGB: 256
        }
      }
      networkProfile: {
        networkInterfaces: [{ id: nics[i].id }]
      }
    }
  }
]

resource extensions 'Microsoft.Compute/virtualMachines/extensions@2023-09-01' = [
  for i in range(0, vmCount): {
    parent: vms[i]
    name: 'install-clickhouse'
    location: location
    properties: {
      publisher: 'Microsoft.Azure.Extensions'
      type: 'CustomScript'
      typeHandlerVersion: '2.1'
      autoUpgradeMinorVersion: true
      protectedSettings: {
        commandToExecute: 'echo "${loadFileAsBase64('../infra/scripts/install-clickhouse.sh')}" | base64 -d > /tmp/install-clickhouse.sh && bash /tmp/install-clickhouse.sh ${i} ${vmCount} ${join(nodePrivateIps, ',')} ${storageAccount.name} ${storageAccount.listKeys().keys[0].value} ${storageContainerName}'
      }
    }
  }
]

output vmPublicIps array = [for i in range(0, vmCount): publicIps[i].properties.ipAddress]
output vmPrivateIps array = nodePrivateIps
output queryEndpoint string = publicIps[0].properties.ipAddress
output storageAccountName string = storageAccount.name
output storageAccountKey string = storageAccount.listKeys().keys[0].value
output storageContainerName string = storageContainerName

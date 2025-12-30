import yaml
import os
from pathlib import Path

# Create directory structure
base_dirs = ['ops/pulsar/tenants', 'ops/pulsar/namespaces', 'ops/pulsar/dlq', 'ops/pulsar/retry']
for dir_path in base_dirs:
    Path(dir_path).mkdir(parents=True, exist_ok=True)

# Define tenant configurations
tenants_config = [
    {
        'tenant_id': 'shipping-co-alpha',
        'admin_roles': ['admin-alpha', 'ops-alpha'],
        'allowed_clusters': ['pulsar-prod', 'pulsar-dr'],
        'description': 'Shipping Company Alpha - Container vessel operations'
    },
    {
        'tenant_id': 'logistics-beta',
        'admin_roles': ['admin-beta', 'ops-beta'],
        'allowed_clusters': ['pulsar-prod', 'pulsar-dr'],
        'description': 'Logistics Provider Beta - Multi-modal transport'
    },
    {
        'tenant_id': 'maritime-gamma',
        'admin_roles': ['admin-gamma', 'ops-gamma'],
        'allowed_clusters': ['pulsar-prod'],
        'description': 'Maritime Services Gamma - Port operations'
    }
]

# Define namespace configurations per tenant with isolation and retention policies
namespace_configs = []
for tenant in tenants_config:
    tenant_id = tenant['tenant_id']
    
    # Primary namespace for vessel tracking
    namespace_configs.append({
        'tenant': tenant_id,
        'namespace': f'{tenant_id}/vessel-tracking',
        'policies': {
            'retention_policy': {
                'retention_minutes': 10080,  # 7 days
                'retention_size_mb': 10240   # 10GB
            },
            'backlog_quota': {
                'limit_size': 5368709120,    # 5GB
                'limit_time': 604800,        # 7 days
                'policy': 'producer_exception'
            },
            'message_ttl': 1209600,          # 14 days
            'deduplication_enabled': True,
            'persistence_policy': {
                'bookkeeper_ensemble': 3,
                'bookkeeper_write_quorum': 3,
                'bookkeeper_ack_quorum': 2
            },
            'max_producers_per_topic': 5,
            'max_consumers_per_topic': 20,
            'max_consumers_per_subscription': 10,
            'is_allow_auto_update_schema': True
        },
        'authorization': {
            'produce': [f'producer-{tenant_id}', f'service-{tenant_id}'],
            'consume': [f'consumer-{tenant_id}', f'analytics-{tenant_id}'],
            'admin': tenant['admin_roles']
        }
    })
    
    # Analytics namespace with longer retention
    namespace_configs.append({
        'tenant': tenant_id,
        'namespace': f'{tenant_id}/analytics',
        'policies': {
            'retention_policy': {
                'retention_minutes': 43200,  # 30 days
                'retention_size_mb': 51200   # 50GB
            },
            'backlog_quota': {
                'limit_size': 10737418240,   # 10GB
                'limit_time': 2592000,       # 30 days
                'policy': 'producer_exception'
            },
            'message_ttl': 2592000,          # 30 days
            'deduplication_enabled': False,
            'persistence_policy': {
                'bookkeeper_ensemble': 3,
                'bookkeeper_write_quorum': 3,
                'bookkeeper_ack_quorum': 2
            },
            'max_producers_per_topic': 10,
            'max_consumers_per_topic': 50,
            'max_consumers_per_subscription': 20,
            'is_allow_auto_update_schema': True
        },
        'authorization': {
            'produce': [f'producer-{tenant_id}', f'service-{tenant_id}'],
            'consume': [f'analytics-{tenant_id}', f'reporting-{tenant_id}'],
            'admin': tenant['admin_roles']
        }
    })

# Generate tenant manifest files
tenant_manifests = []
for tenant in tenants_config:
    tenant_manifest = {
        'apiVersion': 'pulsar.apache.org/v1',
        'kind': 'Tenant',
        'metadata': {
            'name': tenant['tenant_id'],
            'description': tenant['description']
        },
        'spec': {
            'adminRoles': tenant['admin_roles'],
            'allowedClusters': tenant['allowed_clusters']
        }
    }
    
    file_path = f"ops/pulsar/tenants/{tenant['tenant_id']}.yaml"
    with open(file_path, 'w') as f:
        yaml.dump(tenant_manifest, f, default_flow_style=False, sort_keys=False)
    tenant_manifests.append(file_path)

print("✓ Generated Tenant Manifests:")
for path in tenant_manifests:
    print(f"  - {path}")
print()

# Generate namespace manifest files
namespace_manifests = []
for ns_config in namespace_configs:
    ns_name = ns_config['namespace'].split('/')[-1]
    tenant_id = ns_config['tenant']
    
    namespace_manifest = {
        'apiVersion': 'pulsar.apache.org/v1',
        'kind': 'Namespace',
        'metadata': {
            'name': ns_config['namespace'],
            'tenant': tenant_id,
            'namespace': ns_name
        },
        'spec': {
            'policies': ns_config['policies'],
            'authorization': ns_config['authorization']
        }
    }
    
    file_path = f"ops/pulsar/namespaces/{tenant_id}_{ns_name}.yaml"
    with open(file_path, 'w') as f:
        yaml.dump(namespace_manifest, f, default_flow_style=False, sort_keys=False)
    namespace_manifests.append(file_path)

print("✓ Generated Namespace Manifests:")
for path in namespace_manifests:
    print(f"  - {path}")
print()

# Store configurations for downstream blocks
pulsar_tenants = tenants_config
pulsar_namespaces = namespace_configs

print(f"✓ Created {len(tenant_manifests)} tenant configurations")
print(f"✓ Created {len(namespace_manifests)} namespace configurations")
print(f"✓ Configured isolation policies with resource quotas")
print(f"✓ Configured authentication/authorization per namespace")
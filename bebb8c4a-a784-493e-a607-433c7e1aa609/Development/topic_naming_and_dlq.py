import yaml
from pathlib import Path

# Define topic naming convention: tenant.<tenant_id>.<domain>.<stream>
# Example domains: vessel-tracking, cargo, port-operations, analytics
# Example streams: position-updates, cargo-manifest, berth-assignments

topic_examples = []
dlq_configs = []

# Define topic categories and DLQ configurations per tenant
for tenant in pulsar_tenants:
    tenant_id = tenant['tenant_id']
    
    # Define example topics following naming convention
    topic_examples.extend([
        f"persistent://{tenant_id}/vessel-tracking/tenant.{tenant_id}.vessel-tracking.position-updates",
        f"persistent://{tenant_id}/vessel-tracking/tenant.{tenant_id}.vessel-tracking.speed-changes",
        f"persistent://{tenant_id}/vessel-tracking/tenant.{tenant_id}.vessel-tracking.route-updates",
        f"persistent://{tenant_id}/vessel-tracking/tenant.{tenant_id}.cargo.manifest-updates",
        f"persistent://{tenant_id}/vessel-tracking/tenant.{tenant_id}.port-operations.berth-assignments",
        f"persistent://{tenant_id}/analytics/tenant.{tenant_id}.analytics.voyage-statistics",
        f"persistent://{tenant_id}/analytics/tenant.{tenant_id}.analytics.fuel-consumption"
    ])
    
    # DLQ configuration per tenant for each domain
    domains = ['vessel-tracking', 'cargo', 'port-operations', 'analytics']
    
    for domain in domains:
        dlq_config = {
            'apiVersion': 'pulsar.apache.org/v1',
            'kind': 'DeadLetterQueue',
            'metadata': {
                'tenant': tenant_id,
                'domain': domain,
                'name': f"{tenant_id}-{domain}-dlq"
            },
            'spec': {
                'dlq_topic': f"persistent://{tenant_id}/vessel-tracking/tenant.{tenant_id}.{domain}.dlq",
                'max_redeliver_count': 3,
                'dead_letter_topic_retention': {
                    'retention_minutes': 20160,  # 14 days
                    'retention_size_mb': 5120    # 5GB
                },
                'initial_subscription_name': f"{domain}-dlq-subscription",
                'subscription_type': 'Shared',
                'ack_timeout_millis': 30000,
                'enable_batch_index_acknowledgment': True
            }
        }
        
        # Ensure directory exists before writing
        Path('ops/pulsar/dlq').mkdir(parents=True, exist_ok=True)
        
        file_path = f"ops/pulsar/dlq/{tenant_id}_{domain}_dlq.yaml"
        with open(file_path, 'w') as f:
            yaml.dump(dlq_config, f, default_flow_style=False, sort_keys=False)
        
        dlq_configs.append(file_path)

print("✓ Topic Naming Convention: tenant.<tenant_id>.<domain>.<stream>")
print()
print("Example Topics:")
for topic in topic_examples[:10]:  # Show first 10 examples
    print(f"  - {topic}")
print(f"  ... and {len(topic_examples) - 10} more")
print()

print("✓ Generated DLQ Configurations:")
for path in dlq_configs:
    print(f"  - {path}")
print()

dlq_topics = dlq_configs
print(f"✓ Created {len(dlq_configs)} DLQ topic configurations")
print(f"✓ DLQ max redelivery count: 3 attempts")
print(f"✓ DLQ retention: 14 days / 5GB per domain")
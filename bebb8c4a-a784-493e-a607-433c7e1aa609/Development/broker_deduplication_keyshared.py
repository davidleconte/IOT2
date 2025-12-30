import yaml
from pathlib import Path

# Broker-side deduplication configuration
broker_dedup_config = {
    'apiVersion': 'pulsar.apache.org/v1',
    'kind': 'BrokerConfiguration',
    'metadata': {
        'name': 'broker-deduplication-config',
        'description': 'Global broker configuration for message deduplication'
    },
    'spec': {
        'deduplication': {
            'enabled': True,
            'snapshot_interval_seconds': 120,
            'max_num_producers': 1000,
            'deduplication_scope': 'namespace'
        },
        'broker_deduplication_entries_interval': 1000,
        'broker_deduplication_max_number_of_producers': 1000,
        'broker_deduplication_snapshot_interval_seconds': 120
    }
}

# Ensure directory exists
Path('ops/pulsar').mkdir(parents=True, exist_ok=True)

broker_config_path = 'ops/pulsar/broker_deduplication_config.yaml'
with open(broker_config_path, 'w') as f:
    yaml.dump(broker_dedup_config, f, default_flow_style=False, sort_keys=False)

print("✓ Generated Broker Deduplication Configuration:")
print(f"  - {broker_config_path}")
print(f"  - Deduplication enabled at namespace scope")
print(f"  - Snapshot interval: 120 seconds")
print()

# Key_Shared subscription configurations for per-vessel ordering
keyshared_configs = []

for tenant in pulsar_tenants:
    tenant_id = tenant['tenant_id']
    domains = ['vessel-tracking', 'cargo', 'port-operations']
    
    for domain in domains:
        keyshared_config = {
            'apiVersion': 'pulsar.apache.org/v1',
            'kind': 'SubscriptionConfiguration',
            'metadata': {
                'tenant': tenant_id,
                'domain': domain,
                'name': f"{tenant_id}-{domain}-keyshared"
            },
            'spec': {
                'topic_pattern': f"persistent://{tenant_id}/vessel-tracking/tenant.{tenant_id}.{domain}.*",
                'subscription_name': f"{domain}-keyshared-sub",
                'subscription_type': 'Key_Shared',
                'subscription_properties': {
                    'key_shared_mode': 'AUTO_SPLIT',
                    'allow_out_of_order_delivery': False,
                    'subscription_initial_position': 'Latest'
                },
                'consumer_configuration': {
                    'receiver_queue_size': 1000,
                    'max_total_receiver_queue_size_across_partitions': 50000,
                    'priority_level': 0,
                    'crypto_failure_action': 'FAIL'
                },
                'ordering_guarantee': {
                    'key_based': True,
                    'description': 'Messages with same vessel_id key are delivered in order to same consumer',
                    'partition_routing': 'RoundRobinPartition'
                },
                'message_routing': {
                    'routing_mode': 'CustomPartition',
                    'hash_ranges': 'AUTO',
                    'sticky_key_consumer_name': True
                }
            }
        }
        
        file_path = f"ops/pulsar/namespaces/{tenant_id}_{domain}_keyshared_subscription.yaml"
        with open(file_path, 'w') as f:
            yaml.dump(keyshared_config, f, default_flow_style=False, sort_keys=False)
        
        keyshared_configs.append(file_path)

print("✓ Generated Key_Shared Subscription Configurations:")
for path in keyshared_configs:
    print(f"  - {path}")
print()

keyshared_subscriptions = keyshared_configs

print(f"✓ Created {len(keyshared_configs)} Key_Shared subscription configurations")
print(f"✓ Subscription type: Key_Shared with AUTO_SPLIT mode")
print(f"✓ Ordering guarantee: Per-vessel ordering using vessel_id as message key")
print(f"✓ Out-of-order delivery: Disabled for strict ordering")
print()
print("Key_Shared ensures:")
print("  - Messages with same vessel_id are delivered to same consumer in order")
print("  - Different vessels can be processed in parallel by different consumers")
print("  - Scalable parallel processing while maintaining per-vessel ordering")
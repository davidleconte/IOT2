import yaml
from pathlib import Path

# Retry topic configurations with 5s, 1m, 10m delays
retry_configs = []
quarantine_configs = []

# Define retry topics with exponential backoff per tenant
retry_delays = [
    {'name': '5s', 'delay_seconds': 5},
    {'name': '1m', 'delay_seconds': 60},
    {'name': '10m', 'delay_seconds': 600}
]

for tenant in pulsar_tenants:
    tenant_id = tenant['tenant_id']
    domains = ['vessel-tracking', 'cargo', 'port-operations', 'analytics']
    
    for domain in domains:
        for retry_delay in retry_delays:
            retry_config = {
                'apiVersion': 'pulsar.apache.org/v1',
                'kind': 'RetryTopic',
                'metadata': {
                    'tenant': tenant_id,
                    'domain': domain,
                    'name': f"{tenant_id}-{domain}-retry-{retry_delay['name']}"
                },
                'spec': {
                    'retry_topic': f"persistent://{tenant_id}/vessel-tracking/tenant.{tenant_id}.{domain}.retry-{retry_delay['name']}",
                    'delay_seconds': retry_delay['delay_seconds'],
                    'max_retry_attempts': 3,
                    'subscription_type': 'Shared',
                    'ack_timeout_millis': retry_delay['delay_seconds'] * 1000 + 5000,
                    'retention_policy': {
                        'retention_minutes': 1440,  # 1 day
                        'retention_size_mb': 1024   # 1GB
                    },
                    'negative_ack_redelivery_delay_millis': retry_delay['delay_seconds'] * 1000,
                    'enable_retry_letter_topic': True,
                    'backoff_policy': 'exponential'
                }
            }
            
            # Ensure directory exists
            Path('ops/pulsar/retry').mkdir(parents=True, exist_ok=True)
            
            file_path = f"ops/pulsar/retry/{tenant_id}_{domain}_retry_{retry_delay['name']}.yaml"
            with open(file_path, 'w') as f:
                yaml.dump(retry_config, f, default_flow_style=False, sort_keys=False)
            
            retry_configs.append(file_path)
        
        # Quarantine topic configuration per domain
        quarantine_config = {
            'apiVersion': 'pulsar.apache.org/v1',
            'kind': 'QuarantineTopic',
            'metadata': {
                'tenant': tenant_id,
                'domain': domain,
                'name': f"{tenant_id}-{domain}-quarantine"
            },
            'spec': {
                'quarantine_topic': f"persistent://{tenant_id}/vessel-tracking/tenant.{tenant_id}.{domain}.quarantine",
                'description': 'Messages that failed all retry attempts and require manual intervention',
                'subscription_type': 'Exclusive',
                'retention_policy': {
                    'retention_minutes': 43200,  # 30 days
                    'retention_size_mb': 10240   # 10GB
                },
                'alert_on_message': True,
                'alert_threshold': 10,
                'enable_message_replay': True,
                'replay_subscription_name': f"{domain}-quarantine-replay"
            }
        }
        
        # Store in retry directory
        file_path = f"ops/pulsar/retry/{tenant_id}_{domain}_quarantine.yaml"
        with open(file_path, 'w') as f:
            yaml.dump(quarantine_config, f, default_flow_style=False, sort_keys=False)
        
        quarantine_configs.append(file_path)

print("✓ Generated Retry Topic Configurations:")
print(f"  - Retry delays: 5s, 1m, 10m with exponential backoff")
print(f"  - Total retry configs: {len(retry_configs)}")
for path in retry_configs[:6]:  # Show first 6
    print(f"  - {path}")
print(f"  ... and {len(retry_configs) - 6} more")
print()

print("✓ Generated Quarantine Topic Configurations:")
for path in quarantine_configs:
    print(f"  - {path}")
print()

retry_topics = retry_configs
quarantine_topics = quarantine_configs

print(f"✓ Created {len(retry_configs)} retry topic configurations (3 delays × 4 domains × 3 tenants)")
print(f"✓ Created {len(quarantine_configs)} quarantine topic configurations")
print(f"✓ Retry strategy: 5s → 1m → 10m with max 3 attempts per level")
print(f"✓ Quarantine retention: 30 days for manual review and replay")
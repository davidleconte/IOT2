import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# MinIO lifecycle policy configuration for 3-tier intelligent data tiering
# Hot tier (7 days on NVMe), Warm tier (30 days on HDD), Cold tier (90+ days on erasure-coded storage)

# Define tiering strategy
tiering_strategy = {
    'Hot Tier (NVMe)': {
        'duration_days': 7,
        'storage_class': 'STANDARD',
        'description': 'High-performance NVMe storage for recent data requiring <10ms access',
        'cost_per_gb_month': 0.25,
        'iops': 50000,
        'latency_ms': 1
    },
    'Warm Tier (HDD)': {
        'duration_days': 30,
        'storage_class': 'STANDARD_IA',
        'transition_after_days': 7,
        'description': 'Standard HDD storage for frequently accessed recent data',
        'cost_per_gb_month': 0.10,
        'iops': 5000,
        'latency_ms': 5
    },
    'Cold Tier (Erasure Coded)': {
        'duration_days': 90,
        'storage_class': 'GLACIER',
        'transition_after_days': 30,
        'description': 'Erasure-coded long-term storage for compliance and archive',
        'cost_per_gb_month': 0.03,
        'iops': 500,
        'latency_ms': 8
    }
}

# MinIO lifecycle policy JSON configuration
minio_lifecycle_policy = {
    "Rules": [
        {
            "ID": "hot-to-warm-transition",
            "Status": "Enabled",
            "Filter": {
                "Prefix": "telemetry/"
            },
            "Transition": {
                "Days": 7,
                "StorageClass": "STANDARD_IA"
            }
        },
        {
            "ID": "warm-to-cold-transition",
            "Status": "Enabled",
            "Filter": {
                "Prefix": "telemetry/"
            },
            "Transition": {
                "Days": 30,
                "StorageClass": "GLACIER"
            }
        },
        {
            "ID": "cold-tier-retention",
            "Status": "Enabled",
            "Filter": {
                "Prefix": "telemetry/"
            },
            "Expiration": {
                "Days": 365
            }
        }
    ]
}

# Create tiering strategy DataFrame
tiering_df = pd.DataFrame([
    ['Hot Tier', '0-7 days', 'NVMe SSD', 'STANDARD', '$0.25/GB', '50K IOPS', '1ms', 'Real-time analytics, live dashboards'],
    ['Warm Tier', '7-30 days', 'HDD', 'STANDARD_IA', '$0.10/GB', '5K IOPS', '5ms', 'Recent historical queries, weekly reports'],
    ['Cold Tier', '30-365 days', 'Erasure Coded', 'GLACIER', '$0.03/GB', '500 IOPS', '8ms', 'Compliance, audit trails, long-term trends']
], columns=['Tier', 'Data Age', 'Storage Type', 'MinIO Class', 'Cost', 'Performance', 'Latency', 'Use Cases'])

print("=" * 100)
print("MinIO 3-Tier Intelligent Data Tiering Strategy")
print("=" * 100)
print(tiering_df.to_string(index=False))
print()

# Calculate cost savings vs flat storage
data_volume_gb = 10000  # 10TB total data
flat_storage_cost = 0.15  # Average flat storage cost per GB/month

# Distribution of data across tiers (%)
hot_pct = 0.05  # 5% of data in hot tier
warm_pct = 0.25  # 25% of data in warm tier
cold_pct = 0.70  # 70% of data in cold tier

tiered_cost = (
    data_volume_gb * hot_pct * 0.25 +
    data_volume_gb * warm_pct * 0.10 +
    data_volume_gb * cold_pct * 0.03
)
flat_cost = data_volume_gb * flat_storage_cost
cost_savings_pct = ((flat_cost - tiered_cost) / flat_cost) * 100

cost_analysis = pd.DataFrame([
    ['Flat Storage', data_volume_gb, flat_storage_cost, flat_cost, 0],
    ['Hot Tier (5%)', data_volume_gb * hot_pct, 0.25, data_volume_gb * hot_pct * 0.25, 0],
    ['Warm Tier (25%)', data_volume_gb * warm_pct, 0.10, data_volume_gb * warm_pct * 0.10, 0],
    ['Cold Tier (70%)', data_volume_gb * cold_pct, 0.03, data_volume_gb * cold_pct * 0.03, 0],
    ['Tiered Total', data_volume_gb, tiered_cost/data_volume_gb, tiered_cost, cost_savings_pct]
], columns=['Storage Model', 'Data Volume (GB)', 'Cost per GB/Month', 'Total Monthly Cost ($)', 'Savings (%)'])

print("\n" + "=" * 100)
print(f"Cost Analysis: 3-Tier vs Flat Storage (10TB Dataset)")
print("=" * 100)
print(cost_analysis.to_string(index=False))
print(f"\n✓ Monthly Cost Savings: ${flat_cost - tiered_cost:.2f} ({cost_savings_pct:.1f}%)")
print(f"✓ Annual Cost Savings: ${(flat_cost - tiered_cost) * 12:.2f}")
print()

# MinIO CLI commands for lifecycle policy implementation
minio_commands = """
# Configure MinIO lifecycle policy using mc (MinIO Client)

# 1. Set up MinIO client alias
mc alias set minio http://minio-server:9000 ACCESS_KEY SECRET_KEY

# 2. Create lifecycle policy file
cat > lifecycle-policy.json <<EOF
{
  "Rules": [
    {
      "ID": "hot-to-warm-transition",
      "Status": "Enabled",
      "Filter": {"Prefix": "telemetry/"},
      "Transition": {"Days": 7, "StorageClass": "STANDARD_IA"}
    },
    {
      "ID": "warm-to-cold-transition",
      "Status": "Enabled",
      "Filter": {"Prefix": "telemetry/"},
      "Transition": {"Days": 30, "StorageClass": "GLACIER"}
    },
    {
      "ID": "cold-tier-retention",
      "Status": "Enabled",
      "Filter": {"Prefix": "telemetry/"},
      "Expiration": {"Days": 365}
    }
  ]
}
EOF

# 3. Apply lifecycle policy to bucket
mc ilm import minio/vessel-telemetry < lifecycle-policy.json

# 4. Verify lifecycle rules
mc ilm ls minio/vessel-telemetry

# 5. Monitor tier transitions
mc admin tier info minio

# 6. Configure storage tiers
mc admin tier add s3 minio STANDARD_IA --endpoint http://warm-storage:9000 --access-key KEY --secret-key SECRET
mc admin tier add s3 minio GLACIER --endpoint http://cold-storage:9000 --access-key KEY --secret-key SECRET
"""

print("=" * 100)
print("MinIO Lifecycle Policy Implementation Commands")
print("=" * 100)
print(minio_commands)

# Store policy configuration
minio_lifecycle_config = minio_lifecycle_policy
tier_strategy = tiering_df
cost_comparison = cost_analysis
savings_percentage = cost_savings_pct

print(f"\n✓ Configuration variables created:")
print(f"  - minio_lifecycle_config: Lifecycle policy JSON")
print(f"  - tier_strategy: Tiering strategy DataFrame")
print(f"  - cost_comparison: Cost analysis DataFrame")
print(f"  - savings_percentage: {savings_percentage:.1f}% cost reduction")

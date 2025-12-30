import yaml
import json
import os
from pathlib import Path

# Create OpenSearch configuration directory structure
opensearch_dirs = [
    'ops/opensearch/index-templates',
    'ops/opensearch/ilm-policies',
    'ops/opensearch/roles',
    'ops/opensearch/anomaly-detectors',
    'ops/opensearch/vector-indices'
]
for dir_path in opensearch_dirs:
    Path(dir_path).mkdir(parents=True, exist_ok=True)

# Load tenant configurations from upstream
tenant_configs = pulsar_tenants

# Define data domains for index organization
data_domains = [
    'vessel-telemetry',
    'engine-metrics',
    'cargo-operations',
    'port-operations',
    'maintenance-events',
    'incidents',
    'anomalies'
]

# Generate index lifecycle management policies
ilm_policies = {}

# Hot-warm-cold ILP for telemetry data
ilm_policies['telemetry_ilm'] = {
    "policy": {
        "description": "ILM policy for vessel telemetry data - hot/warm/cold tiering",
        "default_state": "hot",
        "states": [
            {
                "name": "hot",
                "actions": [
                    {
                        "rollover": {
                            "min_index_age": "1d",
                            "min_primary_shard_size": "50gb"
                        }
                    }
                ],
                "transitions": [
                    {
                        "state_name": "warm",
                        "conditions": {
                            "min_index_age": "7d"
                        }
                    }
                ]
            },
            {
                "name": "warm",
                "actions": [
                    {
                        "replica_count": {
                            "number_of_replicas": 1
                        }
                    },
                    {
                        "force_merge": {
                            "max_num_segments": 1
                        }
                    }
                ],
                "transitions": [
                    {
                        "state_name": "cold",
                        "conditions": {
                            "min_index_age": "30d"
                        }
                    }
                ]
            },
            {
                "name": "cold",
                "actions": [
                    {
                        "replica_count": {
                            "number_of_replicas": 0
                        }
                    }
                ],
                "transitions": [
                    {
                        "state_name": "delete",
                        "conditions": {
                            "min_index_age": "90d"
                        }
                    }
                ]
            },
            {
                "name": "delete",
                "actions": [
                    {
                        "delete": {}
                    }
                ]
            }
        ]
    }
}

# ILM for long-term analytics and maintenance data
ilm_policies['analytics_ilm'] = {
    "policy": {
        "description": "ILM policy for analytics and maintenance data - extended retention",
        "default_state": "hot",
        "states": [
            {
                "name": "hot",
                "actions": [
                    {
                        "rollover": {
                            "min_index_age": "7d",
                            "min_primary_shard_size": "50gb"
                        }
                    }
                ],
                "transitions": [
                    {
                        "state_name": "warm",
                        "conditions": {
                            "min_index_age": "30d"
                        }
                    }
                ]
            },
            {
                "name": "warm",
                "actions": [
                    {
                        "replica_count": {
                            "number_of_replicas": 1
                        }
                    }
                ],
                "transitions": [
                    {
                        "state_name": "cold",
                        "conditions": {
                            "min_index_age": "180d"
                        }
                    }
                ]
            },
            {
                "name": "cold",
                "actions": [
                    {
                        "replica_count": {
                            "number_of_replicas": 0
                        }
                    }
                ],
                "transitions": [
                    {
                        "state_name": "delete",
                        "conditions": {
                            "min_index_age": "730d"
                        }
                    }
                ]
            },
            {
                "name": "delete",
                "actions": [
                    {
                        "delete": {}
                    }
                ]
            }
        ]
    }
}

# Save ILM policies
for policy_name, policy_config in ilm_policies.items():
    file_path = f"ops/opensearch/ilm-policies/{policy_name}.json"
    with open(file_path, 'w') as f:
        json.dump(policy_config, f, indent=2)

print("✓ Generated ILM Policies:")
print(f"  - telemetry_ilm: hot(1d)→warm(7d)→cold(30d)→delete(90d)")
print(f"  - analytics_ilm: hot(7d)→warm(30d)→cold(180d)→delete(730d)")
print()

# Store for downstream blocks
opensearch_ilm_policies = ilm_policies
opensearch_tenants = tenant_configs
opensearch_data_domains = data_domains

print(f"✓ Created {len(ilm_policies)} ILM policies")
print(f"✓ Prepared configuration for {len(tenant_configs)} tenants")
print(f"✓ Defined {len(data_domains)} data domains for indexing")

import yaml
import json
from pathlib import Path

# Create directory structure for Presto configuration
presto_dirs = ['ops/watsonx-data/presto', 'ops/watsonx-data/presto/catalog']
for dir_path in presto_dirs:
    Path(dir_path).mkdir(parents=True, exist_ok=True)

# Generate Presto C++ Engine Configuration
presto_config = {
    'coordinator': True,
    'node-scheduler.include-coordinator': False,
    'http-server.http.port': 8080,
    'query.max-memory': '50GB',
    'query.max-memory-per-node': '10GB',
    'query.max-total-memory-per-node': '12GB',
    'memory.heap-headroom-per-node': '2GB',
    'discovery-server.enabled': True,
    'discovery.uri': 'http://presto-coordinator:8080'
}

presto_config_path = 'ops/watsonx-data/presto/config.properties'
with open(presto_config_path, 'w') as f:
    for key, value in presto_config.items():
        f.write(f"{key}={value}\n")

# Generate Presto JVM Configuration
jvm_config = [
    '-server',
    '-Xmx16G',
    '-XX:+UseG1GC',
    '-XX:G1HeapRegionSize=32M',
    '-XX:+UseGCOverheadLimit',
    '-XX:+ExplicitGCInvokesConcurrent',
    '-XX:+HeapDumpOnOutOfMemoryError',
    '-XX:+ExitOnOutOfMemoryError',
    '-Djdk.attach.allowAttachSelf=true',
    '-Djdk.nio.maxCachedBufferSize=262144'
]

jvm_config_path = 'ops/watsonx-data/presto/jvm.config'
with open(jvm_config_path, 'w') as f:
    f.write('\n'.join(jvm_config))

# Generate Presto Resource Groups Configuration for Multi-Tenant Query Management
resource_groups_config = {
    'rootGroups': [
        {
            'name': 'global',
            'softMemoryLimit': '80%',
            'hardConcurrencyLimit': 100,
            'maxQueued': 1000,
            'schedulingPolicy': 'weighted_fair',
            'jmxExport': True,
            'subGroups': [
                {
                    'name': 'shipping_co_alpha',
                    'softMemoryLimit': '30%',
                    'hardConcurrencyLimit': 30,
                    'maxQueued': 300,
                    'schedulingWeight': 3,
                    'schedulingPolicy': 'query_priority',
                    'jmxExport': True,
                    'subGroups': [
                        {
                            'name': 'analytics',
                            'softMemoryLimit': '60%',
                            'hardConcurrencyLimit': 15,
                            'maxQueued': 100,
                            'schedulingWeight': 2,
                            'softCpuLimit': '12h',
                            'hardCpuLimit': '24h'
                        },
                        {
                            'name': 'adhoc',
                            'softMemoryLimit': '30%',
                            'hardConcurrencyLimit': 10,
                            'maxQueued': 50,
                            'schedulingWeight': 1,
                            'softCpuLimit': '4h',
                            'hardCpuLimit': '8h'
                        },
                        {
                            'name': 'etl',
                            'softMemoryLimit': '10%',
                            'hardConcurrencyLimit': 5,
                            'maxQueued': 150,
                            'schedulingWeight': 1,
                            'softCpuLimit': '24h',
                            'hardCpuLimit': '48h'
                        }
                    ]
                },
                {
                    'name': 'logistics_beta',
                    'softMemoryLimit': '30%',
                    'hardConcurrencyLimit': 30,
                    'maxQueued': 300,
                    'schedulingWeight': 3,
                    'schedulingPolicy': 'query_priority',
                    'jmxExport': True,
                    'subGroups': [
                        {
                            'name': 'analytics',
                            'softMemoryLimit': '60%',
                            'hardConcurrencyLimit': 15,
                            'maxQueued': 100,
                            'schedulingWeight': 2,
                            'softCpuLimit': '12h',
                            'hardCpuLimit': '24h'
                        },
                        {
                            'name': 'adhoc',
                            'softMemoryLimit': '30%',
                            'hardConcurrencyLimit': 10,
                            'maxQueued': 50,
                            'schedulingWeight': 1,
                            'softCpuLimit': '4h',
                            'hardCpuLimit': '8h'
                        },
                        {
                            'name': 'etl',
                            'softMemoryLimit': '10%',
                            'hardConcurrencyLimit': 5,
                            'maxQueued': 150,
                            'schedulingWeight': 1,
                            'softCpuLimit': '24h',
                            'hardCpuLimit': '48h'
                        }
                    ]
                },
                {
                    'name': 'maritime_gamma',
                    'softMemoryLimit': '30%',
                    'hardConcurrencyLimit': 30,
                    'maxQueued': 300,
                    'schedulingWeight': 3,
                    'schedulingPolicy': 'query_priority',
                    'jmxExport': True,
                    'subGroups': [
                        {
                            'name': 'analytics',
                            'softMemoryLimit': '60%',
                            'hardConcurrencyLimit': 15,
                            'maxQueued': 100,
                            'schedulingWeight': 2,
                            'softCpuLimit': '12h',
                            'hardCpuLimit': '24h'
                        },
                        {
                            'name': 'adhoc',
                            'softMemoryLimit': '30%',
                            'hardConcurrencyLimit': 10,
                            'maxQueued': 50,
                            'schedulingWeight': 1,
                            'softCpuLimit': '4h',
                            'hardCpuLimit': '8h'
                        },
                        {
                            'name': 'etl',
                            'softMemoryLimit': '10%',
                            'hardConcurrencyLimit': 5,
                            'maxQueued': 150,
                            'schedulingWeight': 1,
                            'softCpuLimit': '24h',
                            'hardCpuLimit': '48h'
                        }
                    ]
                },
                {
                    'name': 'admin',
                    'softMemoryLimit': '10%',
                    'hardConcurrencyLimit': 10,
                    'maxQueued': 100,
                    'schedulingWeight': 1,
                    'schedulingPolicy': 'query_priority'
                }
            ]
        }
    ],
    'selectors': [
        {
            'user': 'shipping_co_alpha_.*',
            'group': 'global.shipping_co_alpha.adhoc'
        },
        {
            'user': 'shipping_co_alpha_analytics',
            'group': 'global.shipping_co_alpha.analytics'
        },
        {
            'user': 'shipping_co_alpha_etl',
            'group': 'global.shipping_co_alpha.etl'
        },
        {
            'user': 'logistics_beta_.*',
            'group': 'global.logistics_beta.adhoc'
        },
        {
            'user': 'logistics_beta_analytics',
            'group': 'global.logistics_beta.analytics'
        },
        {
            'user': 'logistics_beta_etl',
            'group': 'global.logistics_beta.etl'
        },
        {
            'user': 'maritime_gamma_.*',
            'group': 'global.maritime_gamma.adhoc'
        },
        {
            'user': 'maritime_gamma_analytics',
            'group': 'global.maritime_gamma.analytics'
        },
        {
            'user': 'maritime_gamma_etl',
            'group': 'global.maritime_gamma.etl'
        },
        {
            'user': 'admin',
            'group': 'global.admin'
        }
    ],
    'cpuQuotaPeriod': '1h'
}

resource_groups_path = 'ops/watsonx-data/presto/resource-groups.json'
with open(resource_groups_path, 'w') as f:
    json.dump(resource_groups_config, f, indent=2)

# Generate Presto Access Control Rules (File-based)
access_control_rules = {
    'catalogs': [
        {
            'catalog': 'maritime_iceberg',
            'allow': 'all',
            'user': '(shipping_co_alpha_.*|logistics_beta_.*|maritime_gamma_.*|admin)'
        }
    ],
    'schemas': [
        {
            'catalog': 'maritime_iceberg',
            'schema': 'shipping_co_alpha',
            'owner': True,
            'user': 'shipping_co_alpha_.*'
        },
        {
            'catalog': 'maritime_iceberg',
            'schema': 'logistics_beta',
            'owner': True,
            'user': 'logistics_beta_.*'
        },
        {
            'catalog': 'maritime_iceberg',
            'schema': 'maritime_gamma',
            'owner': True,
            'user': 'maritime_gamma_.*'
        },
        {
            'catalog': 'maritime_iceberg',
            'schema': '.*',
            'owner': True,
            'user': 'admin'
        }
    ],
    'tables': [
        {
            'catalog': 'maritime_iceberg',
            'schema': 'shipping_co_alpha',
            'table': '.*',
            'privileges': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'user': 'shipping_co_alpha_.*'
        },
        {
            'catalog': 'maritime_iceberg',
            'schema': 'logistics_beta',
            'table': '.*',
            'privileges': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'user': 'logistics_beta_.*'
        },
        {
            'catalog': 'maritime_iceberg',
            'schema': 'maritime_gamma',
            'table': '.*',
            'privileges': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'user': 'maritime_gamma_.*'
        },
        {
            'catalog': 'maritime_iceberg',
            'schema': '.*',
            'table': '.*',
            'privileges': ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'OWNERSHIP'],
            'user': 'admin'
        }
    ],
    'session_properties': [
        {
            'catalog': 'maritime_iceberg',
            'property': 'query_max_memory',
            'allowed': True,
            'user': '.*'
        },
        {
            'catalog': 'maritime_iceberg',
            'property': 'query_max_execution_time',
            'allowed': True,
            'user': '.*'
        }
    ]
}

access_control_path = 'ops/watsonx-data/presto/access-control.json'
with open(access_control_path, 'w') as f:
    json.dump(access_control_rules, f, indent=2)

# Generate Presto Session Property Configuration
session_property_config = {
    'query.max-memory': '10GB',
    'query.max-execution-time': '4h',
    'query.max-run-time': '6h',
    'query.max-stage-count': 100,
    'distributed-sort': True,
    'redistribute-writes': True,
    'scale-writers': True,
    'writer-min-size': '32MB',
    'task.concurrency': 16,
    'task.max-worker-threads': 64,
    'task.writer-count': 4,
    'exchange.compression-enabled': True
}

session_property_path = 'ops/watsonx-data/presto/session-properties.properties'
with open(session_property_path, 'w') as f:
    for key, value in session_property_config.items():
        f.write(f"{key}={value}\n")

# Generate Presto Connector Configuration for Iceberg
iceberg_connector_config = {
    'connector.name': 'iceberg',
    'hive.metastore.uri': 'thrift://hive-metastore:9083',
    'iceberg.catalog.type': 'hive',
    'iceberg.file-format': 'PARQUET',
    'iceberg.compression-codec': 'ZSTD',
    'hive.s3.aws-access-key': '${ENV:AWS_ACCESS_KEY_ID}',
    'hive.s3.aws-secret-key': '${ENV:AWS_SECRET_ACCESS_KEY}',
    'hive.s3.endpoint': 'https://s3.us-east-1.amazonaws.com',
    'hive.s3.path-style-access': True,
    'hive.s3.max-connections': 500,
    'hive.metastore-cache-ttl': '2h',
    'hive.metastore-refresh-interval': '10m',
    'iceberg.max-partitions-per-writer': 100,
    'iceberg.minimum-assigned-split-weight': 0.05
}

iceberg_connector_path = 'ops/watsonx-data/presto/catalog/maritime_iceberg.properties'
with open(iceberg_connector_path, 'w') as f:
    for key, value in iceberg_connector_config.items():
        f.write(f"{key}={value}\n")

print("✓ Generated Presto C++ Configuration Files:")
print(f"  - {presto_config_path}")
print(f"  - {jvm_config_path}")
print(f"  - {resource_groups_path}")
print(f"  - {access_control_path}")
print(f"  - {session_property_path}")
print(f"  - {iceberg_connector_path}")
print()

print("✓ Presto Resource Management:")
print("  - Multi-tenant resource groups with weighted fair scheduling")
print("  - Per-tenant memory limits: 30% each (shipping_co_alpha, logistics_beta, maritime_gamma)")
print("  - Query concurrency limits: 30 per tenant, sub-divided by workload type")
print("  - CPU quotas: analytics (24h), adhoc (8h), etl (48h)")
print()

print("✓ Presto Access Controls:")
print("  - Schema-level isolation: Each tenant owns their schema")
print("  - Table-level privileges: SELECT, INSERT, UPDATE, DELETE per tenant")
print("  - Admin user has full ownership across all schemas")
print("  - Session property controls for query memory and execution time")
print()

# Store configurations for downstream blocks
presto_resource_groups = resource_groups_config
presto_access_control = access_control_rules
presto_session_properties = session_property_config

print("✓ Presto Query Optimization Settings:")
print("  - Max memory per query: 10GB")
print("  - Max execution time: 4h")
print("  - Distributed sorting enabled for large queries")
print("  - Writer scaling enabled for optimal parallelism")
print("  - Exchange compression enabled for network efficiency")

"""
Feast Integration Configuration for DataStax HCD (Cassandra)
Configures Feast feature store to use Cassandra as online store
"""
import os
import yaml

# ========================================
# Feast Feature Store Configuration
# ========================================

# Feast feature_store.yaml configuration for Cassandra online store
feast_config = {
    "project": "maritime_vessel_ml",
    "registry": {
        "registry_type": "sql",
        "path": "postgresql://feast:password@postgres:5432/feast_registry",
        "cache_ttl_seconds": 60
    },
    "provider": "local",
    "online_store": {
        "type": "cassandra",
        "hosts": ["cassandra-node-1", "cassandra-node-2", "cassandra-node-3"],
        "keyspace": "feast_online_store",
        "port": 9042,
        "username": "feast_user",
        "password": "${CASSANDRA_PASSWORD}",
        "protocol_version": 4,
        "load_balancing": {
            "local_dc": "datacenter1",
            "load_balancing_policy": "DCAwareRoundRobinPolicy"
        },
        "read_consistency": "LOCAL_ONE",
        "write_consistency": "LOCAL_QUORUM"
    },
    "offline_store": {
        "type": "spark",
        "spark_conf": {
            "spark.master": "local[*]",
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.spark_catalog.type": "hadoop"
        }
    },
    "entity_key_serialization_version": 2
}

# Write Feast configuration
feast_dir = "ops/cassandra/feast"
os.makedirs(feast_dir, exist_ok=True)

feast_config_file = f"{feast_dir}/feature_store.yaml"
with open(feast_config_file, 'w') as f:
    yaml.dump(feast_config, f, default_flow_style=False, sort_keys=False)

# ========================================
# Feast Entity Definitions
# ========================================

vessel_entity = """
from feast import Entity
from feast.value_type import ValueType

# Vessel entity for feature store
vessel = Entity(
    name="vessel",
    value_type=ValueType.STRING,
    description="Maritime vessel identifier (vessel_id)",
    join_keys=["vessel_id"]
)
"""

# ========================================
# Feast Feature View Definitions
# ========================================

operational_features = """
from datetime import timedelta
from feast import FeatureView, Field
from feast.types import Float64, String
from feast.data_source import PushSource

# Define push source for streaming features
vessel_push_source = PushSource(
    name="vessel_telemetry_push",
    batch_source=None  # Real-time only
)

# Vessel operational features (real-time)
vessel_operational_features = FeatureView(
    name="vessel_operational_features",
    entities=["vessel"],
    ttl=timedelta(days=30),
    schema=[
        Field(name="fuel_efficiency_7d", dtype=Float64),
        Field(name="avg_speed_7d", dtype=Float64),
        Field(name="route_deviation_score", dtype=Float64),
        Field(name="engine_health_score", dtype=Float64),
        Field(name="maintenance_prediction", dtype=Float64),
        Field(name="eta_accuracy_score", dtype=Float64),
    ],
    online=True,
    source=vessel_push_source,
    tags={"team": "ml-platform", "use_case": "vessel_monitoring"}
)
"""

maintenance_features = """
from datetime import timedelta
from feast import FeatureView, Field
from feast.types import Float64, Int64
from feast.data_source import PushSource

# Predictive maintenance features
vessel_maintenance_push = PushSource(
    name="vessel_maintenance_push",
    batch_source=None
)

vessel_maintenance_features = FeatureView(
    name="vessel_maintenance_features",
    entities=["vessel"],
    ttl=timedelta(days=30),
    schema=[
        Field(name="engine_failure_prob", dtype=Float64),
        Field(name="next_maintenance_days", dtype=Int64),
        Field(name="parts_replacement_score", dtype=Float64),
        Field(name="component_health_engine", dtype=Float64),
        Field(name="component_health_hull", dtype=Float64),
        Field(name="component_health_propeller", dtype=Float64),
    ],
    online=True,
    source=vessel_maintenance_push,
    tags={"team": "ml-platform", "use_case": "predictive_maintenance"}
)
"""

# Write feature definitions
entities_file = f"{feast_dir}/entities.py"
with open(entities_file, 'w') as f:
    f.write(vessel_entity)

features_dir = f"{feast_dir}/features"
os.makedirs(features_dir, exist_ok=True)

with open(f"{features_dir}/operational_features.py", 'w') as f:
    f.write(operational_features)

with open(f"{features_dir}/maintenance_features.py", 'w') as f:
    f.write(maintenance_features)

# ========================================
# Cassandra Online Store Schema
# ========================================

cassandra_feast_schema = """-- ========================================
-- Feast Online Store Schema for Cassandra
-- ========================================

CREATE KEYSPACE IF NOT EXISTS feast_online_store
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
}
AND durable_writes = true;

USE feast_online_store;

-- Feast online store table (auto-created by Feast, shown for reference)
-- This follows Feast's schema requirements
CREATE TABLE IF NOT EXISTS online_store (
    feature_view text,
    entity_key blob,
    feature_name text,
    value blob,
    event_ts timestamp,
    created_ts timestamp,
    PRIMARY KEY ((feature_view, entity_key), feature_name)
)
WITH CLUSTERING ORDER BY (feature_name ASC)
AND default_time_to_live = 2592000  -- 30 days
AND compaction = {'class': 'LeveledCompactionStrategy'}
AND bloom_filter_fp_chance = 0.001  -- Low latency reads
AND compression = {'class': 'LZ4Compressor'};

-- Index for timestamp-based queries (optional)
CREATE INDEX IF NOT EXISTS idx_online_store_event_ts 
ON online_store (event_ts);

-- Per-tenant online store tables (multi-tenant approach)
"""

for tenant_id in ["shipping-co-alpha", "logistics-beta", "maritime-gamma"]:
    keyspace = f"maritime_{tenant_id.replace('-', '_')}"
    cassandra_feast_schema += f"""
-- Tenant-specific Feast online store: {tenant_id}
CREATE TABLE IF NOT EXISTS {keyspace}.feast_online_features (
    feature_view_name text,
    entity_id text,
    feature_name text,
    value_blob blob,
    event_timestamp timestamp,
    created_timestamp timestamp,
    PRIMARY KEY ((feature_view_name, entity_id), feature_name, event_timestamp)
)
WITH CLUSTERING ORDER BY (feature_name ASC, event_timestamp DESC)
AND default_time_to_live = 2592000
AND compaction = {{'class': 'LeveledCompactionStrategy'}}
AND bloom_filter_fp_chance = 0.001
AND compression = {{'class': 'LZ4Compressor'}};
"""

feast_schema_file = f"{feast_dir}/cassandra_feast_schema.cql"
with open(feast_schema_file, 'w') as f:
    f.write(cassandra_feast_schema)

# ========================================
# Feature Serving Client Example
# ========================================

serving_example = """#!/usr/bin/env python3
\"\"\"
Example: Serving features from Cassandra online store via Feast
\"\"\"
from feast import FeatureStore
from datetime import datetime

# Initialize Feast feature store
store = FeatureStore(repo_path="ops/cassandra/feast")

# Define entity for lookup
entity_dict = {
    "vessel_id": "vessel-12345"
}

# Get online features (low-latency read from Cassandra)
features = store.get_online_features(
    features=[
        "vessel_operational_features:fuel_efficiency_7d",
        "vessel_operational_features:avg_speed_7d",
        "vessel_operational_features:engine_health_score",
        "vessel_maintenance_features:engine_failure_prob",
        "vessel_maintenance_features:next_maintenance_days"
    ],
    entity_rows=[entity_dict]
).to_dict()

print("Real-time Features Retrieved:")
print(f"  Vessel ID: {entity_dict['vessel_id']}")
print(f"  Fuel Efficiency (7d): {features['fuel_efficiency_7d'][0]}")
print(f"  Avg Speed (7d): {features['avg_speed_7d'][0]}")
print(f"  Engine Health Score: {features['engine_health_score'][0]}")
print(f"  Engine Failure Probability: {features['engine_failure_prob'][0]}")
print(f"  Next Maintenance (days): {features['next_maintenance_days'][0]}")

# ========================================
# Push features to online store (write path)
# ========================================
from feast import PushSource

# Push real-time feature updates
feature_data = {
    "vessel_id": ["vessel-12345"],
    "fuel_efficiency_7d": [2.5],
    "avg_speed_7d": [18.3],
    "route_deviation_score": [0.92],
    "engine_health_score": [0.87],
    "maintenance_prediction": [0.15],
    "eta_accuracy_score": [0.94],
    "event_timestamp": [datetime.now()]
}

# Push to Cassandra online store
store.push("vessel_telemetry_push", feature_data)
print("\\nFeatures pushed to Cassandra online store")
"""

serving_example_file = f"{feast_dir}/feature_serving_example.py"
with open(serving_example_file, 'w') as f:
    f.write(serving_example)

os.chmod(serving_example_file, 0o755)

# ========================================
# Docker Compose for Feast + Cassandra
# ========================================

docker_compose = """version: '3.8'

services:
  cassandra:
    image: datastax/hcd:latest
    container_name: hcd-cassandra
    ports:
      - "9042:9042"
    environment:
      - CASSANDRA_CLUSTER_NAME=maritime-cluster
      - CASSANDRA_DC=datacenter1
      - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
    volumes:
      - cassandra_data:/var/lib/cassandra
    healthcheck:
      test: ["CMD-SHELL", "cqlsh -e 'describe cluster'"]
      interval: 30s
      timeout: 10s
      retries: 5

  feast-registry:
    image: postgres:14
    container_name: feast-registry
    environment:
      - POSTGRES_DB=feast_registry
      - POSTGRES_USER=feast
      - POSTGRES_PASSWORD=feast_password
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  feast-server:
    image: feastdev/feature-server:latest
    container_name: feast-server
    ports:
      - "6566:6566"
    environment:
      - FEAST_FEATURE_STORE_PATH=/feast/feature_store.yaml
    volumes:
      - ./feast:/feast
    depends_on:
      - cassandra
      - feast-registry
    command: feast serve -h 0.0.0.0

volumes:
  cassandra_data:
  postgres_data:
"""

docker_compose_file = f"{feast_dir}/docker-compose.yml"
with open(docker_compose_file, 'w') as f:
    f.write(docker_compose)

# ========================================
# README for Feast Integration
# ========================================

readme_content = """# Feast + DataStax HCD (Cassandra) Integration

## Overview
This integration configures Feast feature store to use DataStax HCD Cassandra as the online store for low-latency feature serving in ML applications.

## Architecture
- **Online Store**: Cassandra (per-tenant keyspaces)
- **Registry**: PostgreSQL
- **Offline Store**: Delta Lake / Spark
- **Serving**: Feast Feature Server

## Setup

### 1. Deploy Cassandra Schema
```bash
cd ops/cassandra/schemas
cqlsh -f cassandra_feast_schema.cql
```

### 2. Initialize Feast Feature Store
```bash
cd ops/cassandra/feast
feast apply
```

### 3. Start Services (Docker)
```bash
docker-compose up -d
```

## Feature Definitions

### Entities
- **vessel**: Maritime vessel identifier

### Feature Views
- **vessel_operational_features**: Real-time operational metrics (fuel efficiency, speed, engine health)
- **vessel_maintenance_features**: Predictive maintenance scores

## Usage

### Online Feature Retrieval (ML Serving)
```python
from feast import FeatureStore

store = FeatureStore(repo_path=".")
features = store.get_online_features(
    features=["vessel_operational_features:engine_health_score"],
    entity_rows=[{"vessel_id": "vessel-12345"}]
)
```

### Push Real-Time Features
```python
store.push("vessel_telemetry_push", feature_data)
```

## Performance Characteristics
- **Read Latency**: < 5ms (LOCAL_ONE consistency)
- **Write Throughput**: High (LOCAL_QUORUM consistency)
- **TTL**: 30 days (automatic expiration)
- **Tenant Isolation**: Per-keyspace separation

## Files
- `feature_store.yaml`: Feast configuration
- `entities.py`: Entity definitions
- `features/*.py`: Feature view definitions
- `cassandra_feast_schema.cql`: Cassandra schema DDL
- `feature_serving_example.py`: Usage example
- `docker-compose.yml`: Local development environment
"""

readme_file = f"{feast_dir}/README.md"
with open(readme_file, 'w') as f:
    f.write(readme_content)

# ========================================
# Summary
# ========================================

feast_summary = {
    "config_file": feast_config_file,
    "entities": 1,
    "feature_views": 2,
    "schema_file": feast_schema_file,
    "example_file": serving_example_file,
    "docker_compose": docker_compose_file,
    "output_dir": feast_dir
}

print("=" * 80)
print("Feast Integration Configuration Complete")
print("=" * 80)

print(f"\n📦 Feast Configuration:")
print(f"  • Project: {feast_config['project']}")
print(f"  • Online Store: Cassandra (DataStax HCD)")
print(f"  • Read Consistency: {feast_config['online_store']['read_consistency']}")
print(f"  • Write Consistency: {feast_config['online_store']['write_consistency']}")

print(f"\n🎯 Feature Store Components:")
print(f"  • Entities: 1 (vessel)")
print(f"  • Feature Views: 2")
print(f"    - vessel_operational_features (6 features)")
print(f"    - vessel_maintenance_features (6 features)")

print(f"\n📁 Generated Files:")
print(f"  • {feast_config_file}")
print(f"  • {entities_file}")
print(f"  • {features_dir}/operational_features.py")
print(f"  • {features_dir}/maintenance_features.py")
print(f"  • {feast_schema_file}")
print(f"  • {serving_example_file}")
print(f"  • {docker_compose_file}")
print(f"  • {readme_file}")

print(f"\n💡 Quick Start:")
print(f"   cd {feast_dir}")
print("   docker-compose up -d")
print("   feast apply")
print("   python feature_serving_example.py")

print(f"\n✅ Feast online store configured for low-latency feature serving")
print(f"✅ Per-tenant isolation with Cassandra keyspaces")

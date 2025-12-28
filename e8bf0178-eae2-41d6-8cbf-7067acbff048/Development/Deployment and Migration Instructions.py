import pandas as pd

# Deployment instructions
deployment_instructions = """
# FLEET GUARDIAN HYBRID ARCHITECTURE DEPLOYMENT GUIDE

## Prerequisites

1. **System Requirements**
   - Linux host with Podman installed (or Docker)
   - Minimum 16GB RAM, 100GB disk space
   - Network connectivity for container registry access

2. **Existing OpenSearch Stack**
   - Ensure OpenSearch cluster is healthy
   - Backup existing data before migration
   - Document current index patterns and retention policies

## Phase 1: Deploy watsonx.data Services (Day 1)

### Step 1.1: Create Directory Structure
```bash
mkdir -p fleet-guardian-hybrid
cd fleet-guardian-hybrid

# Create config directories
mkdir -p presto-config/coordinator presto-config/worker
mkdir -p telemetry-consumer
mkdir -p scripts
```

### Step 1.2: Add Configuration Files
```bash
# Copy Presto configurations (from Service Configuration Files block)
# presto-config/coordinator/config.properties
# presto-config/coordinator/catalog/iceberg.properties
# etc.

# Copy Hive, Spark configs to appropriate locations
```

### Step 1.3: Deploy Infrastructure Services
```bash
# Start PostgreSQL, MinIO, Hive Metastore first
podman-compose up -d postgres-hms minio hive-metastore

# Wait for health checks
podman-compose ps

# Initialize MinIO bucket
podman exec -it minio mc alias set local http://localhost:9000 minioadmin minioadmin
podman exec -it minio mc mb local/telemetry-data
```

### Step 1.4: Deploy Query Engines
```bash
# Start Presto
podman-compose up -d presto-coordinator presto-worker-1

# Start Spark
podman-compose up -d spark-master spark-worker-1

# Verify connectivity
podman exec -it presto-coordinator presto-cli --server localhost:8080 --execute "SHOW CATALOGS;"
```

### Step 1.5: Initialize Iceberg Schema
```bash
# Create namespaces and tables
podman exec -it presto-coordinator presto-cli --server localhost:8080 <<EOF
CREATE SCHEMA IF NOT EXISTS iceberg.telemetry;
CREATE SCHEMA IF NOT EXISTS iceberg.operational;
CREATE SCHEMA IF NOT EXISTS iceberg.analytics;

CREATE TABLE IF NOT EXISTS iceberg.telemetry.raw_events (
    message_id VARCHAR,
    timestamp VARCHAR,
    vehicle_id VARCHAR,
    telemetry_type VARCHAR,
    value VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    publish_time BIGINT
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(timestamp)']
);
EOF
```

## Phase 2: Implement Dual-Sink Consumer (Day 2-3)

### Step 2.1: Build Consumer Container
```bash
cd telemetry-consumer

# Create Dockerfile and consumer.py (from Dual-Sink Consumer block)
# Create requirements.txt

# Build image
podman build -t telemetry-consumer:latest .
```

### Step 2.2: Deploy Consumer (Shadow Mode)
```bash
# Initially deploy with ENABLE_DUAL_SINK=false to test
# This writes only to OpenSearch (no watsonx writes yet)

podman-compose up -d telemetry-consumer

# Monitor logs
podman logs -f telemetry-consumer
```

### Step 2.3: Enable Dual-Sink
```bash
# Update environment variable
podman-compose stop telemetry-consumer

# Edit podman-compose.yml: ENABLE_DUAL_SINK=true

podman-compose up -d telemetry-consumer

# Verify both sinks receiving data
podman logs telemetry-consumer | grep "Wrote .* messages"
```

## Phase 3: Configure Query Layer (Day 3-4)

### Step 3.1: Validate Data in watsonx.data
```bash
# Query via Presto
podman exec -it presto-coordinator presto-cli --server localhost:8080

# Run test query
SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
FROM iceberg.telemetry.raw_events;

# Check partitions
SELECT "$path", COUNT(*)
FROM iceberg.telemetry.raw_events
GROUP BY "$path";
```

### Step 3.2: Set Up Spark Access
```bash
# Test Spark SQL
podman exec -it spark-master spark-sql \\
    --master spark://spark-master:7077 \\
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog

# Run test query
SELECT vehicle_id, COUNT(*) as event_count
FROM iceberg.telemetry.raw_events
GROUP BY vehicle_id
LIMIT 10;
```

### Step 3.3: Configure OpenSearch Dashboards
- Access http://localhost:5601
- Create index pattern: telemetry-*
- Set up operational dashboards
- Configure alerts

## Phase 4: Integrate Cassandra (Day 5-7)

### Step 4.1: Deploy Debezium Connector
```bash
# Start Debezium
podman-compose up -d debezium-connect

# Register CDC connector (from Cassandra CDC Integration block)
curl -X POST http://localhost:8084/connectors \\
    -H "Content-Type: application/json" \\
    -d @debezium-cassandra-config.json
```

### Step 4.2: Deploy CDC Consumer
```bash
# Build and deploy CDC consumer
cd cdc-consumer
podman build -t cdc-consumer:latest .
podman run -d --name cdc-consumer --network fleet-guardian cdc-consumer:latest
```

### Step 4.3: Validate Operational Data Sync
```bash
# Check Cassandra data in Iceberg
podman exec -it presto-coordinator presto-cli --server localhost:8080

SELECT * FROM iceberg.operational.vehicles LIMIT 10;
SELECT COUNT(*) FROM iceberg.operational.maintenance;
```

## Phase 5: Migration & Validation (Day 8-10)

### Step 5.1: Historical Data Backfill
```bash
# Export historical data from OpenSearch to watsonx.data
# Use Spark job for large-scale migration

spark-submit --master spark://spark-master:7077 \\
    backfill_historical.py \\
    --opensearch-url http://opensearch-node1:9200 \\
    --index-pattern 'telemetry-*' \\
    --iceberg-table iceberg.telemetry.raw_events \\
    --start-date 2024-01-01 \\
    --end-date 2024-12-31
```

### Step 5.2: Validation Queries
```bash
# Compare counts between OpenSearch and watsonx.data
# OpenSearch (last 7 days)
curl -X GET "localhost:9200/telemetry-*/_count?q=timestamp:[now-7d TO now]"

# watsonx.data (last 7 days)
podman exec -it presto-coordinator presto-cli --server localhost:8080 \\
    --execute "SELECT COUNT(*) FROM iceberg.telemetry.raw_events WHERE timestamp >= CURRENT_DATE - INTERVAL '7' DAY;"
```

### Step 5.3: Performance Testing
```bash
# Run load tests
# Monitor resource usage
podman stats

# Check query performance
# Compare OpenSearch vs Presto query times for common patterns
```

## Phase 6: Production Cutover (Day 11+)

### Step 6.1: Update Client Applications
- Update BI tools to use Presto JDBC connection
- Migrate long-running analytics from OpenSearch to Presto/Spark
- Keep operational dashboards on OpenSearch

### Step 6.2: Implement Retention Policies
```bash
# OpenSearch: Reduce retention to 7 days
curl -X PUT "localhost:9200/_ilm/policy/telemetry-policy" \\
    -H 'Content-Type: application/json' \\
    -d '{ "policy": { "phases": { "hot": { "actions": {} }, "delete": { "min_age": "7d", "actions": { "delete": {} } } } } }'

# watsonx.data: Keep all data, compress older partitions
# Use Iceberg table maintenance
```

### Step 6.3: Monitoring & Alerting
```bash
# Set up monitoring for:
# - Consumer lag (Pulsar)
# - Dual-sink write latency
# - Storage growth (MinIO)
# - Query performance (Presto/Spark)
# - Service health checks
```

## Rollback Plan

If issues arise:

1. **Stop Dual-Sink**: Set ENABLE_DUAL_SINK=false, restart consumer
2. **OpenSearch Only**: All data still flowing to OpenSearch
3. **Remove watsonx Services**: `podman-compose down presto-coordinator presto-worker-1 spark-master spark-worker-1`
4. **Data Preserved**: Parquet files remain in MinIO for later recovery

## Success Criteria

✅ All services healthy and passing health checks
✅ Dual-sink consumer writing to both OpenSearch and watsonx.data
✅ Query validation shows data consistency
✅ Performance meets SLAs (see Performance Characteristics)
✅ Operational dashboards functioning on OpenSearch
✅ Analytics queries working on Presto/Spark
✅ Cassandra data syncing to watsonx.data (if applicable)
"""

print("=" * 100)
print("DEPLOYMENT & MIGRATION INSTRUCTIONS")
print("=" * 100)
print(deployment_instructions)

# Phase timeline
timeline = pd.DataFrame({
    'Phase': ['Phase 1', 'Phase 2', 'Phase 3', 'Phase 4', 'Phase 5', 'Phase 6'],
    'Description': [
        'Deploy watsonx.data Services',
        'Implement Dual-Sink Consumer',
        'Configure Query Layer',
        'Integrate Cassandra',
        'Migration & Validation',
        'Production Cutover'
    ],
    'Duration': ['1 day', '2 days', '2 days', '3 days', '3 days', 'Ongoing'],
    'Key_Deliverables': [
        'PostgreSQL, MinIO, HMS, Presto, Spark running',
        'Dual-sink consumer writing to both systems',
        'Presto, Spark, OpenSearch Dashboards configured',
        'CDC pipeline operational',
        'Historical data migrated, queries validated',
        'All systems in production, monitoring active'
    ],
    'Risk_Level': ['Low', 'Medium', 'Low', 'Medium', 'High', 'Medium'],
    'Rollback_Easy': ['Yes', 'Yes', 'Yes', 'Yes', 'No', 'Partial']
})

print("\n" + "=" * 100)
print("DEPLOYMENT TIMELINE")
print("=" * 100)
print(timeline.to_string(index=False))

# Pre-deployment checklist
checklist = pd.DataFrame({
    'Category': [
        'Infrastructure', 'Infrastructure', 'Infrastructure',
        'Configuration', 'Configuration', 'Configuration',
        'Data', 'Data',
        'Testing', 'Testing',
        'Documentation', 'Documentation'
    ],
    'Task': [
        'Verify Podman/Docker installed and configured',
        'Ensure sufficient disk space (100GB+)',
        'Network connectivity to container registries',
        'All configuration files created and reviewed',
        'Credentials and secrets configured',
        'Port mappings validated (no conflicts)',
        'OpenSearch data backed up',
        'Cassandra schema documented',
        'Test Pulsar message publishing',
        'Validate OpenSearch cluster health',
        'Architecture diagrams updated',
        'Runbook created for common issues'
    ],
    'Status': ['☐'] * 12,
    'Owner': ['DevOps'] * 3 + ['Platform Team'] * 3 + ['Data Team'] * 2 + ['QA Team'] * 2 + ['Tech Lead'] * 2
})

print("\n" + "=" * 100)
print("PRE-DEPLOYMENT CHECKLIST")
print("=" * 100)
print(checklist.to_string(index=False))

# Post-deployment validation
validation = pd.DataFrame({
    'Validation_Test': [
        'All containers running',
        'Health checks passing',
        'Presto catalog query succeeds',
        'Spark SQL query succeeds',
        'OpenSearch Dashboards accessible',
        'Dual-sink writing to both systems',
        'Data consistency check',
        'Query performance within SLA',
        'Monitoring dashboards active',
        'Backup procedures tested'
    ],
    'Expected_Result': [
        'All services STATUS=running',
        'Health endpoints return 200 OK',
        'SHOW CATALOGS returns iceberg',
        'SELECT 1 returns result',
        'http://localhost:5601 loads',
        'Logs show writes to both OpenSearch and watsonx',
        'Row counts match between systems',
        'OpenSearch <100ms, Presto <5s for common queries',
        'Grafana/Prometheus showing metrics',
        'Backup script executes successfully'
    ],
    'Command': [
        'podman-compose ps',
        'curl http://localhost:9200/_cluster/health',
        'presto-cli --execute "SHOW CATALOGS;"',
        'spark-sql --execute "SELECT 1;"',
        'curl http://localhost:5601',
        'podman logs telemetry-consumer | grep "Wrote"',
        'Compare counts script',
        'Run performance test suite',
        'Check Grafana dashboards',
        './backup.sh'
    ]
})

print("\n" + "=" * 100)
print("POST-DEPLOYMENT VALIDATION TESTS")
print("=" * 100)
print(validation.to_string(index=False))

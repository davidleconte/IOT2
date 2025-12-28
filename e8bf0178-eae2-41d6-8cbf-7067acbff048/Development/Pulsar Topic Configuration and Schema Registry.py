import pandas as pd

# Pulsar topic mappings following data-{keyspace}.{table} pattern
pulsar_topics = pd.DataFrame({
    'Cassandra_Source': [
        'fleet_operational.vehicles',
        'fleet_operational.maintenance_records',
        'fleet_operational.fleet_assignments',
        'fleet_operational.drivers',
        'fleet_operational.routes'
    ],
    'Pulsar_Topic': [
        'data-fleet_operational.vehicles',
        'data-fleet_operational.maintenance_records',
        'data-fleet_operational.fleet_assignments',
        'data-fleet_operational.drivers',
        'data-fleet_operational.routes'
    ],
    'Partitions': [3, 3, 2, 1, 2],
    'Retention_Policy': [
        '7 days (delete)',
        '30 days (delete)',
        '90 days (delete)',
        '365 days (delete)',
        '30 days (delete)'
    ],
    'Serialization': ['Avro', 'Avro', 'Avro', 'Avro', 'Avro'],
    'Schema_Evolution': [
        'Backward compatible',
        'Backward compatible',
        'Full compatible',
        'Full compatible',
        'Backward compatible'
    ]
})

print("=" * 100)
print("PULSAR TOPIC MAPPINGS (data-{keyspace}.{table} PATTERN)")
print("=" * 100)
print(pulsar_topics.to_string(index=False))

# Pulsar configuration for CDC topics
pulsar_broker_config = """
# Pulsar broker.conf additions for CDC pipeline
# /opt/pulsar/conf/broker.conf

## Topic-level settings for CDC
# Allow automatic topic creation
allowAutoTopicCreation=true
allowAutoTopicCreationType=partitioned
defaultNumPartitions=3

## Retention and backlog
# Default message retention (7 days)
backlogQuotaDefaultRetentionPolicy=producer_exception
backlogQuotaDefaultLimitGB=100
messageExpiryCheckIntervalInMinutes=5

## Schema Registry configuration
schemaRegistryStorageClassName=org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorageFactory
isSchemaValidationEnforced=true

## Performance tuning for CDC workload
# Batch settings
maxMessagePublishBufferSizeInMB=256
maxPublishRatePerTopicInMessages=10000
maxPublishRatePerTopicInBytes=100000000

# Backpressure and flow control
maxConcurrentLookupRequest=50000
maxConcurrentTopicLoadRequest=5000

## Deduplication (important for exactly-once)
brokerDeduplicationEnabled=true
brokerDeduplicationMaxNumberOfProducers=1000

## Monitoring
statsUpdateFrequencyInSecs=60
exposeTopicLevelMetricsInPrometheus=true
exposeManagedLedgerMetricsInPrometheus=true
"""

print("\n" + "=" * 100)
print("PULSAR BROKER CONFIGURATION FOR CDC")
print("=" * 100)
print(pulsar_broker_config)

# Create Pulsar topics script
create_topics_script = """#!/bin/bash
# Create Pulsar topics for Cassandra CDC pipeline
# Run this script after Pulsar cluster is up

PULSAR_ADMIN="pulsar-admin"
TENANT="public"
NAMESPACE="default"

echo "Creating CDC topics with data-{keyspace}.{table} pattern..."

# Create topics with specific configurations
$PULSAR_ADMIN topics create-partitioned-topic \\
    persistent://$TENANT/$NAMESPACE/data-fleet_operational.vehicles \\
    --partitions 3

$PULSAR_ADMIN topics create-partitioned-topic \\
    persistent://$TENANT/$NAMESPACE/data-fleet_operational.maintenance_records \\
    --partitions 3

$PULSAR_ADMIN topics create-partitioned-topic \\
    persistent://$TENANT/$NAMESPACE/data-fleet_operational.fleet_assignments \\
    --partitions 2

$PULSAR_ADMIN topics create-partitioned-topic \\
    persistent://$TENANT/$NAMESPACE/data-fleet_operational.drivers \\
    --partitions 1

$PULSAR_ADMIN topics create-partitioned-topic \\
    persistent://$TENANT/$NAMESPACE/data-fleet_operational.routes \\
    --partitions 2

# Set retention policies
echo "Setting retention policies..."

$PULSAR_ADMIN topics set-retention \\
    persistent://$TENANT/$NAMESPACE/data-fleet_operational.vehicles \\
    --size 10G --time 7d

$PULSAR_ADMIN topics set-retention \\
    persistent://$TENANT/$NAMESPACE/data-fleet_operational.maintenance_records \\
    --size 50G --time 30d

$PULSAR_ADMIN topics set-retention \\
    persistent://$TENANT/$NAMESPACE/data-fleet_operational.fleet_assignments \\
    --size 5G --time 90d

$PULSAR_ADMIN topics set-retention \\
    persistent://$TENANT/$NAMESPACE/data-fleet_operational.drivers \\
    --size 1G --time 365d

$PULSAR_ADMIN topics set-retention \\
    persistent://$TENANT/$NAMESPACE/data-fleet_operational.routes \\
    --size 10G --time 30d

# Set deduplication (exactly-once semantics)
echo "Enabling deduplication..."
for TOPIC in vehicles maintenance_records fleet_assignments drivers routes; do
    $PULSAR_ADMIN topics set-deduplication \\
        persistent://$TENANT/$NAMESPACE/data-fleet_operational.$TOPIC \\
        --enable
done

# Verify topics created
echo "Verifying topics..."
$PULSAR_ADMIN topics list $TENANT/$NAMESPACE | grep "data-fleet_operational"

echo "CDC topics created successfully!"
"""

print("\n" + "=" * 100)
print("PULSAR TOPIC CREATION SCRIPT")
print("=" * 100)
print(create_topics_script)

# Avro schema definitions for CDC events
avro_schemas = {
    'vehicles': """
{
  "type": "record",
  "name": "Vehicle",
  "namespace": "com.fleetguardian.cdc",
  "doc": "CDC schema for vehicles table",
  "fields": [
    {"name": "vehicle_id", "type": "string"},
    {"name": "make", "type": ["null", "string"], "default": null},
    {"name": "model", "type": ["null", "string"], "default": null},
    {"name": "year", "type": ["null", "int"], "default": null},
    {"name": "vin", "type": ["null", "string"], "default": null},
    {"name": "registration_date", "type": ["null", "long"], "default": null},
    {"name": "status", "type": ["null", "string"], "default": null},
    {"name": "last_updated", "type": "long"},
    {"name": "_op", "type": "string", "doc": "Operation: INSERT, UPDATE, DELETE"},
    {"name": "_ts", "type": "long", "doc": "Timestamp of CDC event"}
  ]
}
""",
    'maintenance_records': """
{
  "type": "record",
  "name": "MaintenanceRecord",
  "namespace": "com.fleetguardian.cdc",
  "doc": "CDC schema for maintenance_records table",
  "fields": [
    {"name": "record_id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "maintenance_date", "type": "long"},
    {"name": "maintenance_type", "type": ["null", "string"], "default": null},
    {"name": "description", "type": ["null", "string"], "default": null},
    {"name": "cost", "type": ["null", "double"], "default": null},
    {"name": "technician", "type": ["null", "string"], "default": null},
    {"name": "last_updated", "type": "long"},
    {"name": "_op", "type": "string"},
    {"name": "_ts", "type": "long"}
  ]
}
"""
}

print("\n" + "=" * 100)
print("AVRO SCHEMA DEFINITIONS FOR CDC EVENTS")
print("=" * 100)
print("Schema for vehicles table:")
print(avro_schemas['vehicles'])
print("\nSchema for maintenance_records table:")
print(avro_schemas['maintenance_records'])

# Schema Registry integration
schema_registry_config = """
# Pulsar Schema Registry Configuration
# Integrated with Pulsar broker

## Schema Storage
# Schemas stored in BookKeeper (same as Pulsar metadata)
schemaRegistryStorageClassName=org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorageFactory

## Schema Compatibility Checks
# Enforce schema compatibility on publish
isSchemaValidationEnforced=true

# Default compatibility strategy
schemaCompatibilityStrategy=BACKWARD

## Schema Evolution Policies
# - BACKWARD: New schema can read old data
# - FORWARD: Old schema can read new data  
# - FULL: Both backward and forward compatible
# - NONE: No compatibility checks

## Schema Upload/Management via Pulsar Admin

# Upload Avro schema for vehicles topic
pulsar-admin schemas upload \\
    persistent://public/default/data-fleet_operational.vehicles \\
    --filename vehicles.avsc

# Upload Avro schema for maintenance_records topic
pulsar-admin schemas upload \\
    persistent://public/default/data-fleet_operational.maintenance_records \\
    --filename maintenance_records.avsc

# Get schema for a topic
pulsar-admin schemas get \\
    persistent://public/default/data-fleet_operational.vehicles

# List all schemas
pulsar-admin schemas list public/default

## Schema Compatibility Testing
# Test schema compatibility before deployment
pulsar-admin schemas test-compatibility \\
    persistent://public/default/data-fleet_operational.vehicles \\
    --filename vehicles_v2.avsc

## Producer Configuration for Schema
# Producers automatically register schemas on first publish
# Schema is included in message metadata
# Consumer validates schema on message consumption
"""

print("\n" + "=" * 100)
print("SCHEMA REGISTRY CONFIGURATION & MANAGEMENT")
print("=" * 100)
print(schema_registry_config)

# Schema evolution examples
schema_evolution = pd.DataFrame({
    'Scenario': [
        'Add optional field',
        'Add required field with default',
        'Remove field',
        'Rename field',
        'Change field type',
        'Change field from optional to required'
    ],
    'Compatibility': [
        'BACKWARD compatible',
        'FORWARD compatible',
        'FORWARD compatible',
        'NOT compatible (requires new topic)',
        'NOT compatible (major version)',
        'NOT compatible (breaking change)'
    ],
    'Action_Required': [
        'None - old consumers work',
        'Update consumers before producers',
        'Update producers before consumers',
        'Create new topic with migration',
        'Create new schema version',
        'Requires data migration'
    ],
    'Example': [
        'Add "color" field with default null',
        'Add "priority" with default "normal"',
        'Remove deprecated "legacy_id" field',
        'Rename "desc" to "description"',
        'Change timestamp from long to string',
        'Make "owner_id" required from optional'
    ]
})

print("\n" + "=" * 100)
print("SCHEMA EVOLUTION COMPATIBILITY MATRIX")
print("=" * 100)
print(schema_evolution.to_string(index=False))

# Pulsar topic monitoring metrics
monitoring_metrics = pd.DataFrame({
    'Metric_Name': [
        'pulsar_producers_count',
        'pulsar_consumers_count',
        'pulsar_rate_in',
        'pulsar_rate_out',
        'pulsar_throughput_in',
        'pulsar_throughput_out',
        'pulsar_storage_size',
        'pulsar_subscription_back_log',
        'pulsar_consumer_msg_ack_rate',
        'pulsar_in_bytes_total',
        'pulsar_schema_version'
    ],
    'Description': [
        'Number of active producers per topic',
        'Number of active consumers per topic',
        'Messages per second published',
        'Messages per second consumed',
        'Bytes per second published',
        'Bytes per second consumed',
        'Total storage used by topic',
        'Number of unacknowledged messages',
        'Message acknowledgment rate',
        'Total bytes received since startup',
        'Current schema version for topic'
    ],
    'Alert_Threshold': [
        '> 5 (CDC agent should be 1 per node)',
        '< 1 (no consumers)',
        'Sudden drop > 50%',
        'Lag > rate_in for > 1 min',
        'N/A',
        'N/A',
        '> retention limit',
        '> 100k messages',
        '< rate_out for > 1 min',
        'N/A',
        'Version mismatch between topics'
    ],
    'Used_For': [
        'CDC agent health',
        'Sink connector health',
        'CDC capture rate',
        'Sink consumption rate',
        'Network bandwidth',
        'Network bandwidth',
        'Storage capacity planning',
        'Consumer lag detection',
        'Consumer processing issues',
        'Capacity planning',
        'Schema compatibility tracking'
    ]
})

print("\n" + "=" * 100)
print("PULSAR MONITORING METRICS FOR CDC PIPELINE")
print("=" * 100)
print(monitoring_metrics.to_string(index=False))

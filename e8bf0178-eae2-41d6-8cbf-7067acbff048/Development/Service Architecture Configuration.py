import pandas as pd

# Define the complete service architecture
service_architecture = {
    'Service': [
        'OpenSearch Cluster',
        'OpenSearch Dashboards',
        'Pulsar/Kafka Broker',
        'Telemetry Consumer (Dual-Sink)',
        'Presto Coordinator',
        'Presto Workers',
        'Spark Master',
        'Spark Workers',
        'Hive Metastore (HMS)',
        'MinIO/S3 Storage',
        'PostgreSQL (HMS Backend)',
        'HCD/Cassandra',
        'CDC Connector (Debezium)'
    ],
    'Purpose': [
        'Real-time indexing and search',
        'Operational dashboards and monitoring',
        'Message queue for telemetry data',
        'Consumes messages, writes to both OpenSearch and watsonx.data',
        'SQL query coordinator for Presto',
        'Distributed query execution',
        'Spark job orchestration',
        'Distributed Spark processing',
        'Iceberg catalog and metadata management',
        'Object storage for Parquet files',
        'Metadata persistence for HMS',
        'Operational data store',
        'Change data capture from Cassandra'
    ],
    'Technology': [
        'OpenSearch 2.x',
        'OpenSearch Dashboards 2.x',
        'Apache Pulsar 3.x',
        'Python/Java',
        'Presto 0.28x',
        'Presto 0.28x',
        'Apache Spark 3.4',
        'Apache Spark 3.4',
        'Apache Hive 3.x',
        'MinIO (S3-compatible)',
        'PostgreSQL 14',
        'Apache Cassandra 4.x',
        'Debezium 2.x'
    ],
    'Port': [
        '9200, 9300',
        '5601',
        '6650, 8080',
        'N/A',
        '8080',
        '8080',
        '7077, 8080',
        '8081',
        '9083',
        '9000, 9001',
        '5432',
        '9042',
        '8083'
    ],
    'Container': [
        'opensearch-node1',
        'opensearch-dashboards',
        'pulsar-broker',
        'telemetry-consumer',
        'presto-coordinator',
        'presto-worker-1',
        'spark-master',
        'spark-worker-1',
        'hive-metastore',
        'minio',
        'postgres-hms',
        'cassandra',
        'debezium-connect'
    ],
    'New_Service': [
        False,
        False,
        False,
        True,  # Modified
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        False,  # Existing
        True
    ]
}

service_df = pd.DataFrame(service_architecture)

print("=" * 100)
print("FLEET GUARDIAN SERVICE ARCHITECTURE")
print("=" * 100)
print(f"\nTotal Services: {len(service_df)}")
print(f"New Services to Deploy: {service_df['New_Service'].sum()}")
print(f"Existing Services: {(~service_df['New_Service']).sum()}")

print("\n" + "=" * 100)
print("NEW SERVICES TO DEPLOY")
print("=" * 100)
print(service_df[service_df['New_Service']][['Service', 'Technology', 'Purpose']].to_string(index=False))

print("\n" + "=" * 100)
print("EXISTING SERVICES (MAINTAINED)")
print("=" * 100)
print(service_df[~service_df['New_Service']][['Service', 'Technology', 'Purpose']].to_string(index=False))

print("\n" + "=" * 100)
print("COMPLETE SERVICE INVENTORY")
print("=" * 100)
print(service_df.to_string(index=False))

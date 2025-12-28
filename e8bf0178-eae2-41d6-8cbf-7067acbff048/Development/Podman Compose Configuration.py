import pandas as pd

# Generate the complete podman-compose.yml configuration
compose_config = """version: '3.8'

networks:
  fleet-guardian:
    driver: bridge

volumes:
  opensearch-data1:
  cassandra-data:
  postgres-hms-data:
  minio-data:
  spark-events:

services:
  # ============================================
  # EXISTING SERVICES (OpenSearch Stack)
  # ============================================
  
  opensearch-node1:
    image: opensearchproject/opensearch:2.11.0
    container_name: opensearch-node1
    environment:
      - cluster.name=fleet-guardian-cluster
      - node.name=opensearch-node1
      - discovery.type=single-node
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms2g -Xmx2g"
      - DISABLE_SECURITY_PLUGIN=true
    ulimits:
      memlock:
        soft: -1
        hard: -1
      nofile:
        soft: 65536
        hard: 65536
    volumes:
      - opensearch-data1:/usr/share/opensearch/data
    ports:
      - 9200:9200
      - 9300:9300
    networks:
      - fleet-guardian
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:9200/_cluster/health || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5

  opensearch-dashboards:
    image: opensearchproject/opensearch-dashboards:2.11.0
    container_name: opensearch-dashboards
    ports:
      - 5601:5601
    environment:
      - OPENSEARCH_HOSTS=http://opensearch-node1:9200
      - DISABLE_SECURITY_DASHBOARDS_PLUGIN=true
    networks:
      - fleet-guardian
    depends_on:
      - opensearch-node1

  # ============================================
  # MESSAGE BROKER (Pulsar)
  # ============================================
  
  pulsar-broker:
    image: apachepulsar/pulsar:3.1.1
    container_name: pulsar-broker
    command: bin/pulsar standalone
    ports:
      - 6650:6650
      - 8080:8080
    environment:
      - PULSAR_MEM=-Xms1g -Xmx1g
    networks:
      - fleet-guardian
    healthcheck:
      test: ["CMD-SHELL", "bin/pulsar-admin brokers healthcheck"]
      interval: 30s
      timeout: 10s
      retries: 5

  # ============================================
  # OPERATIONAL STORE (Cassandra)
  # ============================================
  
  cassandra:
    image: cassandra:4.1
    container_name: cassandra
    ports:
      - 9042:9042
    environment:
      - CASSANDRA_CLUSTER_NAME=fleet-guardian
      - CASSANDRA_DC=datacenter1
      - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
      - MAX_HEAP_SIZE=1G
      - HEAP_NEWSIZE=256M
    volumes:
      - cassandra-data:/var/lib/cassandra
    networks:
      - fleet-guardian
    healthcheck:
      test: ["CMD-SHELL", "cqlsh -e 'describe cluster'"]
      interval: 30s
      timeout: 10s
      retries: 10

  # ============================================
  # WATSONX.DATA SERVICES
  # ============================================
  
  # PostgreSQL for Hive Metastore
  postgres-hms:
    image: postgres:14-alpine
    container_name: postgres-hms
    environment:
      - POSTGRES_USER=hive
      - POSTGRES_PASSWORD=hivepassword
      - POSTGRES_DB=metastore
    volumes:
      - postgres-hms-data:/var/lib/postgresql/data
    ports:
      - 5432:5432
    networks:
      - fleet-guardian
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U hive"]
      interval: 10s
      timeout: 5s
      retries: 5

  # MinIO Object Storage (S3-compatible)
  minio:
    image: minio/minio:latest
    container_name: minio
    command: server /data --console-address ":9001"
    environment:
      - MINIO_ROOT_USER=minioadmin
      - MINIO_ROOT_PASSWORD=minioadmin
    volumes:
      - minio-data:/data
    ports:
      - 9000:9000
      - 9001:9001
    networks:
      - fleet-guardian
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 20s
      retries: 3

  # Hive Metastore (Iceberg Catalog)
  hive-metastore:
    image: apache/hive:3.1.3
    container_name: hive-metastore
    environment:
      - SERVICE_NAME=metastore
      - DB_DRIVER=postgres
      - SERVICE_OPTS=-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver 
        -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres-hms:5432/metastore
        -Djavax.jdo.option.ConnectionUserName=hive
        -Djavax.jdo.option.ConnectionPassword=hivepassword
    ports:
      - 9083:9083
    networks:
      - fleet-guardian
    depends_on:
      - postgres-hms
      - minio

  # Presto Coordinator
  presto-coordinator:
    image: prestodb/presto:0.282
    container_name: presto-coordinator
    ports:
      - 8081:8080
    environment:
      - PRESTO_ENVIRONMENT=production
    volumes:
      - ./presto-config/coordinator:/opt/presto/etc
    networks:
      - fleet-guardian
    depends_on:
      - hive-metastore
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:8080/v1/info || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5

  # Presto Worker
  presto-worker-1:
    image: prestodb/presto:0.282
    container_name: presto-worker-1
    environment:
      - PRESTO_ENVIRONMENT=production
    volumes:
      - ./presto-config/worker:/opt/presto/etc
    networks:
      - fleet-guardian
    depends_on:
      - presto-coordinator

  # Spark Master
  spark-master:
    image: bitnami/spark:3.4
    container_name: spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - 7077:7077
      - 8082:8080
    volumes:
      - spark-events:/opt/spark/spark-events
    networks:
      - fleet-guardian

  # Spark Worker
  spark-worker-1:
    image: bitnami/spark:3.4
    container_name: spark-worker-1
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_WORKER_CORES=2
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - 8083:8081
    volumes:
      - spark-events:/opt/spark/spark-events
    networks:
      - fleet-guardian
    depends_on:
      - spark-master

  # ============================================
  # DATA PIPELINE
  # ============================================
  
  # Dual-Sink Telemetry Consumer
  telemetry-consumer:
    build:
      context: ./telemetry-consumer
      dockerfile: Dockerfile
    container_name: telemetry-consumer
    environment:
      - PULSAR_SERVICE_URL=pulsar://pulsar-broker:6650
      - OPENSEARCH_URL=http://opensearch-node1:9200
      - S3_ENDPOINT=http://minio:9000
      - S3_ACCESS_KEY=minioadmin
      - S3_SECRET_KEY=minioadmin
      - S3_BUCKET=telemetry-data
      - HIVE_METASTORE_URI=thrift://hive-metastore:9083
      - ENABLE_DUAL_SINK=true
    networks:
      - fleet-guardian
    depends_on:
      - pulsar-broker
      - opensearch-node1
      - minio
      - hive-metastore
    restart: unless-stopped

  # Debezium CDC Connector for Cassandra
  debezium-connect:
    image: debezium/connect:2.4
    container_name: debezium-connect
    ports:
      - 8084:8083
    environment:
      - BOOTSTRAP_SERVERS=pulsar-broker:6650
      - GROUP_ID=debezium-cassandra
      - CONFIG_STORAGE_TOPIC=debezium_configs
      - OFFSET_STORAGE_TOPIC=debezium_offsets
      - STATUS_STORAGE_TOPIC=debezium_status
    networks:
      - fleet-guardian
    depends_on:
      - pulsar-broker
      - cassandra
"""

print("=" * 100)
print("PODMAN-COMPOSE.YML CONFIGURATION")
print("=" * 100)
print(compose_config)

# Configuration summary
config_summary = pd.DataFrame({
    'Component': ['Networks', 'Volumes', 'Services', 'Health Checks', 'Port Mappings'],
    'Count': [1, 5, 13, 8, 15],
    'Details': [
        'fleet-guardian bridge network',
        'Data persistence for OpenSearch, Cassandra, PostgreSQL, MinIO, Spark',
        '3 existing + 10 new services',
        'Health monitoring for critical services',
        'External access to all service endpoints'
    ]
})

print("\n" + "=" * 100)
print("CONFIGURATION SUMMARY")
print("=" * 100)
print(config_summary.to_string(index=False))

# Service dependencies
dependencies = pd.DataFrame({
    'Service': ['hive-metastore', 'presto-coordinator', 'presto-worker-1', 
                'spark-worker-1', 'telemetry-consumer', 'debezium-connect'],
    'Dependencies': [
        'postgres-hms, minio',
        'hive-metastore',
        'presto-coordinator',
        'spark-master',
        'pulsar-broker, opensearch-node1, minio, hive-metastore',
        'pulsar-broker, cassandra'
    ],
    'Startup_Order': [3, 4, 5, 5, 5, 4]
})

print("\n" + "=" * 100)
print("SERVICE DEPENDENCIES & STARTUP ORDER")
print("=" * 100)
print(dependencies.to_string(index=False))

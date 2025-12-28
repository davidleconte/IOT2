import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle
import pandas as pd

# Zerve design system
bg_color = '#1D1D20'
text_primary = '#fbfbff'
text_secondary = '#909094'
light_blue = '#A1C9F4'
orange = '#FFB482'
green = '#8DE5A1'
coral = '#FF9F9B'
lavender = '#D0BBFF'
highlight = '#ffd400'
success = '#17b26a'
warning = '#f04438'

print("=" * 100)
print("FLEET GUARDIAN: DETAILED DUAL-PATH ARCHITECTURE SPECIFICATION")
print("=" * 100)

# Create comprehensive specification dataframes
print("\n📋 1. DATA FLOW ARCHITECTURE WITH LATENCY REQUIREMENTS\n")

flow_specs = {
    'Pipeline Stage': [
        '1. Data Ingestion',
        '2a. OpenSearch Real-time',
        '2a. OpenSearch Indexing',
        '2a. OpenSearch Query',
        '2b. watsonx Batch Collection',
        '2b. watsonx Parquet Write',
        '2b. watsonx Iceberg Commit',
        '2b. watsonx Query'
    ],
    'Component': [
        'Pulsar Topics',
        'OpenSearch Connector',
        'OpenSearch Index',
        'OpenSearch DSL/REST API',
        'Kafka Connect / Pulsar IO',
        'Object Storage (S3/COS)',
        'Iceberg Table Metadata',
        'Presto/Spark Engine'
    ],
    'Latency Target': [
        '<100ms (publish)',
        '<500ms (streaming)',
        '<2 seconds (index refresh)',
        '<100ms (search)',
        '1-5 minutes (micro-batch)',
        '5-15 minutes (batch write)',
        '<1 minute (metadata update)',
        '10s - 5 min (query complexity)'
    ],
    'Throughput': [
        '100K+ msg/sec',
        '50K+ msg/sec',
        '50K+ docs/sec',
        '1K+ queries/sec',
        '10K+ msg/sec',
        '1-5 GB/min',
        'Metadata only',
        '100+ MB/sec scan'
    ]
}

flow_spec_df = pd.DataFrame(flow_specs)
print(flow_spec_df.to_string(index=False))

print("\n\n💾 2. STORAGE FORMAT SPECIFICATIONS\n")

storage_specs = {
    'System': ['OpenSearch', 'OpenSearch', 'OpenSearch', 
               'watsonx.data', 'watsonx.data', 'watsonx.data', 'watsonx.data'],
    'Layer': ['Document Store', 'Inverted Index', 'Columnar (TSDB)',
              'Object Storage', 'File Format', 'Table Format', 'Compression'],
    'Format/Technology': [
        'JSON documents with dynamic mapping',
        'Lucene inverted indices + doc values',
        'Time-series optimized columnar storage',
        'S3/COS parquet files (partitioned)',
        'Apache Parquet (columnar, compressed)',
        'Apache Iceberg (ACID, schema evolution)',
        'Snappy/Zstd compression'
    ],
    'Purpose': [
        'Full document retrieval, flexible schema',
        'Fast text search, filtering, aggregations',
        'Efficient time-range queries',
        'Cost-effective long-term storage',
        'Optimized for analytical queries',
        'ACID transactions, time travel',
        'Reduce storage cost by 5-10x'
    ],
    'Data Retention': [
        'Hot: 7 days, Warm: 30 days, Cold: 90 days',
        'Same as document lifecycle',
        'Same as document lifecycle',
        'Unlimited (cost ~$0.023/GB/month)',
        'Years of historical data',
        'Multiple table versions/snapshots',
        'All historical data'
    ]
}

storage_spec_df = pd.DataFrame(storage_specs)
print(storage_spec_df.to_string(index=False))

print("\n\n🔍 3. QUERY ACCESS PATTERNS & USE CASES\n")

query_patterns = {
    'Query Pattern': [
        'Fleet Health Dashboard',
        'Critical Alert Search',
        'Anomaly Detection',
        'Real-time Aggregations',
        'Historical Trend Analysis',
        'Predictive Maintenance ML',
        'Multi-source Data Joins',
        'Compliance Reporting'
    ],
    'Primary System': [
        'OpenSearch',
        'OpenSearch',
        'OpenSearch (RCF)',
        'OpenSearch',
        'watsonx.data',
        'watsonx.data',
        'watsonx.data',
        'watsonx.data'
    ],
    'Data Window': [
        'Last 24-48 hours',
        'Last 15 minutes',
        'Real-time stream',
        'Last 1-7 days',
        'Last 6-24 months',
        'Last 2+ years',
        'Historical (all time)',
        'Configurable periods'
    ],
    'Expected Latency': [
        '<1 second',
        '<500ms',
        '<2 seconds',
        '<3 seconds',
        '30-120 seconds',
        '2-10 minutes',
        '1-5 minutes',
        '5-30 minutes'
    ],
    'Data Volume': [
        '10-100 GB (hot)',
        '1-10 GB (recent)',
        'Stream processing',
        '50-500 GB',
        '1-10 TB',
        '5-50 TB',
        '10-100 TB',
        '1-50 TB'
    ],
    'Example Query': [
        'GET /fleet/_search {last 24h metrics}',
        'GET /alerts/_search {severity:critical}',
        'POST /_plugins/_anomaly_detection',
        'GET /_search {aggs by vehicle_id}',
        'SELECT AVG(temp) FROM telemetry',
        'SELECT * FROM maintenance_history',
        'JOIN telemetry WITH service_records',
        'SELECT * WHERE timestamp BETWEEN...'
    ]
}

query_df = pd.DataFrame(query_patterns)
print(query_df.to_string(index=False))

print("\n\n⚙️ 4. HCD/CASSANDRA TIME-SERIES INTEGRATION\n")

cassandra_specs = {
    'Component': [
        'Primary Use Case',
        'Write Pattern',
        'Data Model',
        'CDC to watsonx.data',
        'Retention Policy',
        'Query Pattern'
    ],
    'Specification': [
        'Operational time-series store (high-velocity writes)',
        'Time-bucketed wide rows (partition by vehicle_id + time)',
        'CREATE TABLE fleet_ts (vehicle_id, ts, metrics MAP<TEXT,DOUBLE>)',
        'Cassandra CDC → Kafka → watsonx.data (async replication)',
        'Hot: 30 days (SSD), Archive via CDC to watsonx.data',
        'Read latest metrics by vehicle (operational queries)'
    ],
    'Latency': [
        'Sub-millisecond writes, <10ms reads',
        '<1ms per write',
        'Optimized for time-series',
        '5-15 minute delay to watsonx.data',
        'Automatic TTL expiration',
        '<10ms for recent data'
    ],
    'Integration': [
        'Complements OpenSearch & watsonx.data',
        'Handles 100K+ writes/sec per node',
        'Wide row = one vehicle hour of metrics',
        'Uses Kafka Connect S3 sink',
        'Reduces storage costs (archive to S3)',
        'OpenSearch for search, Cassandra for operational'
    ]
}

cassandra_df = pd.DataFrame(cassandra_specs)
print(cassandra_df.to_string(index=False))

print("\n\n🔄 5. DUAL-SINK PATTERN IMPLEMENTATION\n")

dual_sink = {
    'Aspect': [
        'Source Topics',
        'Sink 1: OpenSearch',
        'Sink 2: watsonx.data',
        'Message Format',
        'Partitioning Strategy',
        'Failure Handling',
        'Ordering Guarantees'
    ],
    'Implementation': [
        'navbox.raw → fleet.telemetry.enriched (Pulsar topics)',
        'Pulsar OpenSearch Connector (direct streaming)',
        'Pulsar → Kafka Connect → S3 → Iceberg',
        'Avro/JSON in Pulsar, JSON in OpenSearch, Parquet in watsonx',
        'Partition by vehicle_id in Pulsar (maintains ordering)',
        'Independent failures: one sink failure doesn\'t affect other',
        'Pulsar guarantees ordering per partition (per vehicle)'
    ],
    'Configuration': [
        'Pulsar namespaces: fleet/telemetry',
        'opensearch.sink.connector (auto-commit)',
        'kafka-connect-s3 + iceberg-sink-connector',
        'Schema registry for Avro serialization',
        'Hash(vehicle_id) → partition',
        'Dead letter queues for failed messages',
        'Effective-once semantics in Pulsar'
    ]
}

dual_sink_df = pd.DataFrame(dual_sink)
print(dual_sink_df.to_string(index=False))

print("\n\n📊 6. PROCESSING PIPELINE DETAILS\n")

processing_specs = {
    'Processing Stage': [
        'OpenSearch: Ingest Pipeline',
        'OpenSearch: RCF Anomaly Detection',
        'OpenSearch: Alerting',
        'OpenSearch: Orchestrator Actions',
        'watsonx: Spark Batch Jobs',
        'watsonx: Presto Interactive',
        'watsonx: ML Training (Spark MLlib)'
    ],
    'Technology': [
        'Ingest processors (parse, enrich, transform)',
        'Random Cut Forest algorithm (streaming ML)',
        'Alerting plugin (monitors, triggers, notifications)',
        'Workflow automation (response orchestration)',
        'Spark 3.x on compute clusters',
        'Presto/Trino SQL engine',
        'Spark MLlib distributed training'
    ],
    'Input': [
        'Raw JSON docs from Pulsar',
        'Indexed time-series data',
        'Anomaly detection results',
        'Alert triggers',
        'Parquet files from Iceberg tables',
        'Iceberg tables via Presto connector',
        'Historical telemetry + maintenance data'
    ],
    'Output': [
        'Enriched, parsed JSON docs → index',
        'Anomaly scores (0-1) per metric',
        'Alert notifications (Slack, PagerDuty, etc)',
        'Automated remediation actions',
        'Aggregated analytics tables (daily/monthly)',
        'Query results for dashboards/reports',
        'Trained models (RandomForest, GBT) → model registry'
    ],
    'Latency': [
        '<500ms (inline)',
        '1-5 seconds per detection',
        '<30 seconds (trigger to notification)',
        '<2 minutes (action execution)',
        '10-60 minutes (batch job)',
        '10 seconds - 5 minutes (query)',
        '2-8 hours (training job)'
    ]
}

processing_df = pd.DataFrame(processing_specs)
print(processing_df.to_string(index=False))

print("\n" + "=" * 100)
print("SPECIFICATION SUMMARY")
print("=" * 100)
print("✓ Dual-sink architecture from Pulsar topics")
print("✓ OpenSearch: JSON storage, inverted indices, <2 second latency")
print("✓ watsonx.data: Parquet files, Iceberg tables, 1-5 minute latency")
print("✓ HCD/Cassandra: Time-series operational store with CDC to watsonx")
print("✓ Query patterns optimized for real-time ops (OpenSearch) vs analytics (watsonx)")
print("=" * 100)

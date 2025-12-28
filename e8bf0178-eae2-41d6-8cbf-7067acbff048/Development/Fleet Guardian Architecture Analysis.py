import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import pandas as pd

# Zerve design system colors
bg_color = '#1D1D20'
text_primary = '#fbfbff'
text_secondary = '#909094'
light_blue = '#A1C9F4'
orange = '#FFB482'
green = '#8DE5A1'
coral = '#FF9F9B'
lavender = '#D0BBFF'
highlight = '#ffd400'

print("=" * 80)
print("FLEET GUARDIAN ARCHITECTURE ANALYSIS")
print("OpenSearch vs watsonx.data Integration Patterns")
print("=" * 80)

# 1. Data Flow Differences Analysis
print("\n📊 1. DATA FLOW DIFFERENCES\n")
print("Dual-Path Ingestion Architecture:")
print("-" * 80)

data_flow_comparison = {
    'Component': ['Data Source', 'Message Broker', 'Real-time Path', 'Analytics Path', 
                  'Storage Layer', 'Query Interface'],
    'OpenSearch Integration': [
        'Fleet telemetry, logs, metrics',
        'Apache Pulsar topics',
        'Pulsar → OpenSearch (streaming)',
        'N/A (direct ingestion)',
        'OpenSearch indices (JSON docs)',
        'OpenSearch DSL, REST API'
    ],
    'watsonx.data Integration': [
        'Fleet telemetry, logs, metrics',
        'Apache Pulsar topics',
        'N/A (batch-oriented)',
        'Pulsar → Kafka Connect → watsonx.data',
        'Iceberg tables, Parquet files',
        'Presto SQL, Spark SQL'
    ]
}

flow_df = pd.DataFrame(data_flow_comparison)
print(flow_df.to_string(index=False))

print("\n🔄 Pipeline Patterns:")
print("  • OpenSearch: Pulsar → OpenSearch Connector → Index → Query")
print("  • watsonx.data: Pulsar → Kafka Connect → Object Storage → Iceberg → Query Engine")
print("  • Latency: OpenSearch ~seconds | watsonx.data ~minutes to hours")
print("  • Use Case: OpenSearch for real-time ops | watsonx.data for analytical insights")

# 2. Storage Layer Synergies
print("\n\n💾 2. STORAGE LAYER SYNERGIES\n")
print("-" * 80)

storage_synergies = {
    'Aspect': ['Primary Format', 'Optimization', 'Schema', 'Time-based Data', 
               'Search Capability', 'Analytics Capability', 'Retention'],
    'OpenSearch': [
        'Inverted indices, columnar',
        'Real-time indexing, compression',
        'Dynamic mapping, schema-on-write',
        'Time-series indices with ILM',
        'Full-text, fuzzy, semantic search',
        'Basic aggregations, limited joins',
        'Hot-warm-cold architecture'
    ],
    'watsonx.data': [
        'Parquet (columnar), Iceberg tables',
        'Partition pruning, predicate pushdown',
        'Schema evolution, versioning',
        'Partitioned by time (year/month/day)',
        'None (requires external indexing)',
        'Complex SQL, joins, windowing',
        'Unlimited in object storage'
    ]
}

storage_df = pd.DataFrame(storage_synergies)
print(storage_df.to_string(index=False))

print("\n🤝 Complementary Strengths:")
print("  • OpenSearch: Millisecond search, real-time anomaly detection, operational queries")
print("  • watsonx.data: Cost-effective long-term storage, complex analytics, historical trends")
print("  • Together: Real-time monitoring + deep historical analysis on same data")

# 3. Processing Complementarity
print("\n\n⚙️ 3. PROCESSING COMPLEMENTARITY\n")
print("-" * 80)

processing_comparison = {
    'Processing Type': ['Anomaly Detection', 'Real-time Processing', 'Batch Analytics', 
                        'Machine Learning', 'Data Transformation', 'Join Operations'],
    'OpenSearch': [
        'RCF (Random Cut Forest) algorithm',
        'Stream processing, index-time',
        'Limited (aggregation pipelines)',
        'Built-in ML (AD, forecasting)',
        'Ingest pipelines, processors',
        'Parent-child, nested docs only'
    ],
    'watsonx.data (Spark/Presto)': [
        'Custom ML models, MLlib',
        'Micro-batch (Spark Streaming)',
        'Massive parallel processing',
        'Full Spark ML ecosystem',
        'Spark transformations, UDFs',
        'Full SQL joins, complex queries'
    ]
}

processing_df = pd.DataFrame(processing_comparison)
print(processing_df.to_string(index=False))

print("\n🔬 Processing Patterns:")
print("  • OpenSearch RCF: Streaming anomaly detection on live data (fleet health alerts)")
print("  • Spark MLlib: Batch training on historical data (predictive maintenance models)")
print("  • Presto: Interactive ad-hoc queries across multi-TB datasets")
print("  • Combined: Real-time alerting (OS) + root cause analysis (watsonx.data)")

# 4. Query Patterns
print("\n\n🔍 4. QUERY PATTERNS\n")
print("-" * 80)

query_patterns = {
    'Query Type': ['Operational Queries', 'Analytical Queries', 'Time Range', 
                   'Typical User', 'Response Time', 'Data Volume'],
    'OpenSearch DSL': [
        '✓ Optimized for ops',
        '○ Basic aggregations',
        'Last 24-48 hours (hot data)',
        'DevOps, SRE, security analysts',
        'Milliseconds to seconds',
        'GBs to TBs (recent data)'
    ],
    'Presto SQL': [
        '○ Possible but slower',
        '✓ Complex analytics',
        'Days to years (all history)',
        'Data analysts, data scientists',
        'Seconds to minutes',
        'TBs to PBs (full history)'
    ]
}

query_df = pd.DataFrame(query_patterns)
print(query_df.to_string(index=False))

print("\n📝 Example Query Scenarios:")
print("  OpenSearch DSL:")
print("    - 'Show all critical fleet alerts in last 15 minutes'")
print("    - 'Find vehicles with CPU usage >90% currently'")
print("    - 'Real-time dashboard of fleet health metrics'")
print("\n  Presto SQL:")
print("    - 'Analyze failure patterns across 2 years of maintenance logs'")
print("    - 'Join fleet telemetry with service records for predictive models'")
print("    - 'Monthly aggregations of fleet performance by region'")

print("\n" + "=" * 80)
print("Analysis complete. Generating visualizations...")
print("=" * 80)
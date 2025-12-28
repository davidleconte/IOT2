import pandas as pd
import matplotlib.pyplot as plt

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

print("=" * 100)
print("TECHNOLOGY MAPPING TABLE: OpenSearch vs watsonx.data")
print("=" * 100)

# Comprehensive technology mapping
tech_mapping_data = {
    'Technology Layer': [
        'Message Ingestion',
        'Data Format (Storage)',
        'Data Format (Processing)',
        'Index Structure',
        'Query Language',
        'Real-time Processing',
        'Batch Processing',
        'Anomaly Detection',
        'Machine Learning',
        'Data Transformation',
        'Join Capabilities',
        'Search Capabilities',
        'Aggregation Engine',
        'Time-series Optimization',
        'Data Retention',
        'Schema Management',
        'Scaling Model',
        'Query Latency',
        'Typical Data Volume',
        'Primary User Persona'
    ],
    'OpenSearch': [
        'Pulsar Connector (native)',
        'JSON documents',
        'Columnar (doc values)',
        'Inverted index + BKD trees',
        'OpenSearch DSL (JSON)',
        '✓ Stream-native',
        '○ Limited',
        'RCF (Random Cut Forest)',
        'Built-in ML features',
        'Ingest pipelines',
        'Parent-child, nested only',
        '✓✓ Full-text, fuzzy, semantic',
        'Bucket aggregations',
        'Time-series indices + ILM',
        'Hot-warm-cold tiers',
        'Dynamic mapping',
        'Horizontal (shards)',
        'Milliseconds to seconds',
        'GBs to TBs (hot data)',
        'DevOps, SRE, Security Ops'
    ],
    'watsonx.data': [
        'Kafka Connect / Pulsar IO',
        'Parquet (columnar)',
        'Apache Iceberg tables',
        'No indexing (full scans)',
        'Presto SQL / Spark SQL',
        '○ Micro-batch only',
        '✓ Massive parallelism',
        'Custom models (bring your own)',
        'Spark MLlib ecosystem',
        'Spark transformations, UDFs',
        '✓✓ Full SQL joins',
        '○ None (needs integration)',
        'SQL aggregations + windowing',
        'Time-based partitioning',
        'Unlimited (object storage)',
        'Schema evolution (Iceberg)',
        'Horizontal (compute nodes)',
        'Seconds to minutes',
        'TBs to PBs (historical)',
        'Data Analysts, Data Scientists'
    ],
    'Best Use Case': [
        'Real-time streaming',
        'Operational queries',
        'Analytics queries',
        'Search operations',
        'Interactive queries',
        'Alert generation',
        'Historical analysis',
        'Fleet health monitoring',
        'Predictive maintenance',
        'Log enrichment',
        'Multi-table analytics',
        'Log search & discovery',
        'Real-time metrics',
        'Compliance & audit logs',
        'Long-term analytics',
        'Evolving data models',
        'Growing fleet size',
        'Live dashboards',
        'Data lake analytics',
        'Operational vs analytical'
    ]
}

tech_mapping_df = pd.DataFrame(tech_mapping_data)

# Print full table
print("\n")
print(tech_mapping_df.to_string(index=False, max_colwidth=40))

# Create detailed comparison categories
print("\n\n" + "=" * 100)
print("KEY INTEGRATION PATTERNS")
print("=" * 100)

patterns = {
    'Pattern': ['Lambda Architecture', 'Data Tiering', 'Query Federation', 
                'Dual-Write Strategy', 'Event-Driven Sync'],
    'Description': [
        'Real-time (OpenSearch) + Batch (watsonx.data) layers',
        'Hot data in OpenSearch, cold in watsonx.data',
        'Query both systems from unified interface',
        'Write to both systems simultaneously from Pulsar',
        'OpenSearch triggers watsonx.data updates on events'
    ],
    'Complexity': ['High', 'Medium', 'Medium', 'High', 'Very High'],
    'Use When': [
        'Need both real-time and deep analytics',
        'Cost optimization + performance required',
        'Single query interface needed',
        'Data consistency critical',
        'Complex workflows with dependencies'
    ]
}

patterns_df = pd.DataFrame(patterns)
print("\n")
print(patterns_df.to_string(index=False))

# Technology synergy matrix
print("\n\n" + "=" * 100)
print("TECHNOLOGY SYNERGY MATRIX")
print("=" * 100)

synergy_data = {
    'Integration Point': [
        'Data Ingestion',
        'Storage',
        'Processing',
        'Query Access',
        'ML/Analytics',
        'Monitoring'
    ],
    'OpenSearch Strength': [
        'Low-latency streaming',
        'Fast random access',
        'Real-time aggregations',
        'Full-text search',
        'Anomaly detection',
        'Operational dashboards'
    ],
    'watsonx.data Strength': [
        'Batch efficiency',
        'Cost-effective scale',
        'Complex transformations',
        'SQL analytics',
        'Predictive models',
        'Historical trends'
    ],
    'Synergy Value': [
        'Dual ingestion paths',
        'Tiered storage strategy',
        'Complementary engines',
        'Multi-modal access',
        'Complete ML pipeline',
        'Full observability'
    ]
}

synergy_df = pd.DataFrame(synergy_data)
print("\n")
print(synergy_df.to_string(index=False))

# Create visual table
table_fig, ax = plt.subplots(figsize=(18, 12), facecolor=bg_color)
ax.axis('tight')
ax.axis('off')

# Create subset for visualization (top 12 rows)
visual_data = tech_mapping_df.head(12).values.tolist()
visual_cols = tech_mapping_df.columns.tolist()

table = ax.table(cellText=visual_data, colLabels=visual_cols,
                cellLoc='left', loc='center',
                colWidths=[0.25, 0.3, 0.3, 0.15])

table.auto_set_font_size(False)
table.set_fontsize(9)
table.scale(1, 2.5)

# Style header
for i in range(len(visual_cols)):
    cell = table[(0, i)]
    cell.set_facecolor(bg_color)
    cell.set_edgecolor(highlight)
    cell.set_text_props(weight='bold', color=text_primary, fontsize=10)
    cell.set_linewidth(2)

# Style cells with alternating colors and column-specific colors
for i in range(1, len(visual_data) + 1):
    # Technology Layer column
    cell = table[(i, 0)]
    cell.set_facecolor(bg_color)
    cell.set_edgecolor(text_secondary)
    cell.set_text_props(color=text_secondary, fontsize=9)
    
    # OpenSearch column
    cell = table[(i, 1)]
    cell.set_facecolor(bg_color)
    cell.set_edgecolor(light_blue)
    cell.set_text_props(color=light_blue, fontsize=9)
    
    # watsonx.data column
    cell = table[(i, 2)]
    cell.set_facecolor(bg_color)
    cell.set_edgecolor(coral)
    cell.set_text_props(color=coral, fontsize=9)
    
    # Best Use Case column
    cell = table[(i, 3)]
    cell.set_facecolor(bg_color)
    cell.set_edgecolor(green)
    cell.set_text_props(color=green, fontsize=9)

ax.set_title('Fleet Guardian Technology Comparison: OpenSearch vs watsonx.data', 
            fontsize=16, fontweight='bold', color=text_primary, pad=20)

plt.tight_layout()

print("\n\n" + "=" * 100)
print("✓ Technology mapping completed")
print("  - 20 technology layers compared")
print("  - Integration patterns identified")
print("  - Synergy matrix created")
print("=" * 100)
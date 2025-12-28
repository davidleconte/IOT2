import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np

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
success = '#17b26a'
warning = '#f04438'

# Define comprehensive use case matrix with latency requirements and query patterns
use_case_matrix = {
    'Use Case': [
        'Real-Time Anomaly Detection',
        'Live Vessel Tracking',
        'Geo-Spatial Proximity Queries',
        'Operational Dashboards',
        'Real-Time Alerting',
        'Historical Trend Analysis',
        'Predictive Modeling',
        'Fleet-Wide Analytics',
        'Compliance Reporting',
        'Cross-Fleet Comparisons',
        'Hybrid: Real-Time Alert with Context',
        'Hybrid: Dashboard with Historical Baseline',
        'Hybrid: Geo-Query with Historical Routes',
        'ML: Feature Engineering',
        'ML: Model Training'
    ],
    'System': [
        'OpenSearch',
        'OpenSearch',
        'OpenSearch',
        'OpenSearch',
        'OpenSearch',
        'watsonx.data',
        'watsonx.data',
        'watsonx.data',
        'watsonx.data',
        'watsonx.data',
        'OpenSearch + watsonx.data',
        'OpenSearch + watsonx.data',
        'OpenSearch + watsonx.data',
        'watsonx.data (Spark)',
        'watsonx.data (Spark)'
    ],
    'Latency Requirement': [
        '< 100ms',
        '< 50ms',
        '< 200ms',
        '< 500ms',
        '< 100ms',
        'Minutes to Hours',
        'Hours to Days',
        'Minutes to Hours',
        'Hours to Days',
        'Minutes to Hours',
        '< 200ms (query), enriched async',
        '< 500ms (recent), batch (historical)',
        '< 200ms (current), async (history)',
        'Hours (batch)',
        'Hours to Days (batch)'
    ],
    'Data Scope': [
        'Last 7 days',
        'Last 24 hours',
        'Last 24 hours',
        'Last 7 days',
        'Last 7 days',
        '1+ years',
        '2+ years',
        'All historical data',
        'All historical data',
        'All historical data',
        '7 days + historical context',
        '24 hours + monthly averages',
        '24 hours + 1 year routes',
        '6 months+ for features',
        '1+ years training data'
    ],
    'Query Pattern': [
        'Time-series aggregations, threshold checks',
        'Point queries by vessel ID, geo-location',
        'Geo-spatial radius/polygon queries',
        'Multi-metric aggregations, filters',
        'Real-time stream queries, rule evaluation',
        'Complex aggregations, window functions',
        'JOIN multiple tables, feature engineering',
        'GROUP BY fleet/region, statistical analysis',
        'Complex JOINs, regulatory calculations',
        'Multi-table JOINs, comparative analytics',
        'Fast point query + async enrichment',
        'Real-time metrics + pre-computed baselines',
        'Geo-query + federated historical JOIN',
        'Spark transformations, feature extraction',
        'Distributed model training on Parquet/Iceberg'
    ]
}

use_case_df = pd.DataFrame(use_case_matrix)

print("=" * 80)
print("INTEGRATION PATTERNS & USE CASE MATRIX")
print("=" * 80)
print("\nComprehensive use case matrix showing:")
print("  • Hot path (OpenSearch): Real-time operational use cases")
print("  • Cold path (watsonx.data): Historical analytics & ML")
print("  • Hybrid patterns: Real-time + historical context")
print("  • ML workflows: Feature engineering & training")
print("\n")
print(use_case_df.to_string(index=False))
print("\n")

# Example queries for each pattern category
example_queries = {
    'Pattern Category': [
        'Hot Path: Real-Time Anomaly Detection',
        'Hot Path: Live Vessel Tracking',
        'Hot Path: Geo-Spatial Proximity',
        'Cold Path: Historical Trend Analysis',
        'Cold Path: Predictive Modeling Prep',
        'Hybrid: Real-Time Alert + Context',
        'Hybrid: Dashboard + Baseline',
        'ML: Feature Engineering',
        'ML: Model Inference to Index'
    ],
    'System': [
        'OpenSearch',
        'OpenSearch',
        'OpenSearch',
        'watsonx.data (Presto)',
        'watsonx.data (Spark)',
        'OpenSearch → Presto Federation',
        'OpenSearch + Presto',
        'watsonx.data (Spark)',
        'Spark → OpenSearch'
    ],
    'Example Query': [
        '''GET /vessel-telemetry/_search
{
  "query": {"range": {"timestamp": {"gte": "now-5m"}}},
  "aggs": {
    "by_vessel": {
      "terms": {"field": "vessel_id"},
      "aggs": {"avg_speed": {"avg": {"field": "speed"}}}
    }
  }
}''',
        '''GET /vessel-telemetry/_search
{
  "query": {
    "bool": {
      "must": [
        {"term": {"vessel_id": "V12345"}},
        {"range": {"timestamp": {"gte": "now-1h"}}}
      ]
    }
  },
  "sort": [{"timestamp": "desc"}],
  "size": 1
}''',
        '''GET /vessel-telemetry/_search
{
  "query": {
    "geo_distance": {
      "distance": "10km",
      "location": {"lat": 37.7749, "lon": -122.4194}
    }
  }
}''',
        '''SELECT 
  DATE_TRUNC('month', timestamp) AS month,
  AVG(fuel_consumption) AS avg_fuel,
  STDDEV(fuel_consumption) AS fuel_variance
FROM iceberg.vessel_telemetry
WHERE timestamp >= CURRENT_DATE - INTERVAL '2' YEAR
GROUP BY DATE_TRUNC('month', timestamp)
ORDER BY month''',
        '''# PySpark on watsonx.data
df = spark.read.format("iceberg").load("vessel_telemetry")
features = df.groupBy("vessel_id", "route_id") \\
  .agg(
    avg("speed").alias("avg_speed"),
    stddev("fuel_consumption").alias("fuel_variance"),
    count("*").alias("trip_count")
  )
features.write.format("parquet").save("s3://features/")''',
        '''# Step 1: Real-time alert (OpenSearch)
GET /vessel-telemetry/_search
{"query": {"term": {"vessel_id": "V12345"}}, "size": 1}

# Step 2: Enrich with historical context (Presto)
SELECT AVG(speed), STDDEV(speed)
FROM iceberg.vessel_telemetry
WHERE vessel_id = 'V12345'
  AND timestamp >= CURRENT_DATE - INTERVAL '6' MONTH''',
        '''# Dashboard: Recent data (OpenSearch)
GET /vessel-telemetry/_search
{"query": {"range": {"timestamp": {"gte": "now-24h"}}}}

# Baseline: Historical averages (Presto)
SELECT vessel_id, AVG(speed) AS baseline_speed
FROM iceberg.vessel_telemetry
WHERE timestamp >= CURRENT_DATE - INTERVAL '3' MONTH
GROUP BY vessel_id''',
        '''# PySpark feature engineering
from pyspark.sql import functions as F

telemetry = spark.read.format("iceberg").load("vessel_telemetry")
weather = spark.read.format("iceberg").load("weather_data")

features = telemetry.join(weather, ["timestamp", "location"]) \\
  .withColumn("speed_to_wind_ratio", F.col("speed") / F.col("wind_speed")) \\
  .groupBy("vessel_id", F.window("timestamp", "1 hour")) \\
  .agg(
    F.avg("speed").alias("avg_speed"),
    F.avg("speed_to_wind_ratio").alias("avg_speed_wind_ratio")
  )''',
        '''# After model inference, index results to OpenSearch
predictions_df = model.transform(features)

# Write to OpenSearch for real-time querying
predictions_df.write \\
  .format("opensearch") \\
  .option("es.nodes", "opensearch-cluster") \\
  .option("es.resource", "vessel-predictions") \\
  .save()'''
    ]
}

query_examples_df = pd.DataFrame(example_queries)

print("=" * 80)
print("EXAMPLE QUERIES BY PATTERN")
print("=" * 80)
print("\nQuery examples demonstrating each integration pattern:")
for idx, row in query_examples_df.iterrows():
    print(f"\n{idx+1}. {row['Pattern Category']}")
    print(f"   System: {row['System']}")
    print(f"   Query:\n{row['Example Query'][:200]}...")
    print()

# Data lifecycle specification
data_lifecycle = {
    'Time Range': [
        'Real-time (< 1 hour)',
        'Recent (1-24 hours)',
        'Active (1-7 days)',
        'Warm (7-30 days)',
        'Cold (30+ days)',
        'Archive (1+ years)'
    ],
    'Primary Storage': [
        'OpenSearch hot indices',
        'OpenSearch hot indices',
        'OpenSearch warm indices',
        'OpenSearch warm/Cassandra',
        'watsonx.data (Iceberg)',
        'watsonx.data (Iceberg)'
    ],
    'Access Pattern': [
        'High-frequency queries (1000s/sec)',
        'Frequent queries (100s/sec)',
        'Regular queries (10s/sec)',
        'Occasional queries',
        'Analytical queries',
        'Batch/ML workloads'
    ],
    'Retention Policy': [
        'In-memory + SSD',
        'SSD storage',
        'SSD storage',
        'Transition to object storage',
        'Object storage (S3)',
        'Object storage (S3)'
    ],
    'Cost (Relative)': [
        'Highest (10x)',
        'High (8x)',
        'Medium-High (5x)',
        'Medium (3x)',
        'Low (1.5x)',
        'Lowest (1x)'
    ]
}

lifecycle_df = pd.DataFrame(data_lifecycle)

print("=" * 80)
print("DATA LIFECYCLE MANAGEMENT")
print("=" * 80)
print("\nData aging and archival strategy:")
print(lifecycle_df.to_string(index=False))
print("\n")

# Create comprehensive visualization
fig = plt.figure(figsize=(18, 14), facecolor=bg_color)
fig.suptitle('Fleet Guardian: Integration Patterns & Use Cases', 
             fontsize=20, color=text_primary, fontweight='bold', y=0.98)

# Use GridSpec for complex layout
gs = fig.add_gridspec(4, 2, hspace=0.4, wspace=0.3, 
                      left=0.08, right=0.95, top=0.94, bottom=0.05)

# 1. Use Case Distribution by System
ax1 = fig.add_subplot(gs[0, :])
ax1.set_facecolor(bg_color)
systems_count = use_case_df['System'].value_counts()
colors_map = {
    'OpenSearch': light_blue,
    'watsonx.data': orange,
    'OpenSearch + watsonx.data': green,
    'watsonx.data (Spark)': lavender
}
bar_colors = [colors_map.get(sys, coral) for sys in systems_count.index]
bars = ax1.bar(range(len(systems_count)), systems_count.values, color=bar_colors, alpha=0.8)
ax1.set_xticks(range(len(systems_count)))
ax1.set_xticklabels(systems_count.index, rotation=15, ha='right', color=text_primary, fontsize=10)
ax1.set_ylabel('Number of Use Cases', color=text_primary, fontsize=11)
ax1.set_title('Use Case Distribution by System', color=text_primary, fontsize=13, pad=10)
ax1.tick_params(colors=text_primary)
ax1.spines['bottom'].set_color(text_secondary)
ax1.spines['left'].set_color(text_secondary)
ax1.spines['top'].set_visible(False)
ax1.spines['right'].set_visible(False)
ax1.grid(axis='y', alpha=0.2, color=text_secondary)

for i, (bar, count) in enumerate(zip(bars, systems_count.values)):
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
             str(count), ha='center', va='bottom', color=text_primary, fontsize=10, fontweight='bold')

# 2. Latency Requirements Heatmap
ax2 = fig.add_subplot(gs[1, 0])
ax2.set_facecolor(bg_color)

latency_categories = ['< 100ms', '< 500ms', 'Minutes', 'Hours+']
latency_mapping = {
    '< 100ms': 0, '< 50ms': 0, '< 200ms': 0, '< 500ms': 1,
    'Minutes to Hours': 2, 'Hours to Days': 3, 'Hours (batch)': 3,
    '< 200ms (query), enriched async': 1,
    '< 500ms (recent), batch (historical)': 1,
    '< 200ms (current), async (history)': 1
}

systems_list = ['OpenSearch', 'watsonx.data', 'Hybrid', 'Spark']
latency_matrix = np.zeros((len(systems_list), len(latency_categories)))

for _, row in use_case_df.iterrows():
    system = row['System']
    latency = row['Latency Requirement']
    
    if 'Spark' in system:
        sys_idx = 3
    elif '+' in system:
        sys_idx = 2
    elif 'watsonx' in system:
        sys_idx = 1
    else:
        sys_idx = 0
    
    lat_idx = latency_mapping.get(latency, 2)
    latency_matrix[sys_idx, lat_idx] += 1

im = ax2.imshow(latency_matrix, cmap='YlOrRd', aspect='auto', alpha=0.8)
ax2.set_xticks(range(len(latency_categories)))
ax2.set_xticklabels(latency_categories, rotation=45, ha='right', color=text_primary, fontsize=9)
ax2.set_yticks(range(len(systems_list)))
ax2.set_yticklabels(systems_list, color=text_primary, fontsize=10)
ax2.set_title('Use Cases by Latency Requirement', color=text_primary, fontsize=12, pad=10)

for i in range(len(systems_list)):
    for j in range(len(latency_categories)):
        if latency_matrix[i, j] > 0:
            ax2.text(j, i, int(latency_matrix[i, j]), ha='center', va='center',
                    color=text_primary if latency_matrix[i, j] < 3 else bg_color, 
                    fontsize=11, fontweight='bold')

# 3. Data Lifecycle Timeline
ax3 = fig.add_subplot(gs[1, 1])
ax3.set_facecolor(bg_color)
ax3.set_xlim(0, 10)
ax3.set_ylim(0, 7)
ax3.axis('off')
ax3.set_title('Data Lifecycle & Archival Strategy', color=text_primary, fontsize=12, pad=10)

timeline_stages = [
    ('Real-time\n< 1hr', 1, light_blue, 'OpenSearch\nHot'),
    ('Recent\n1-24hr', 2.5, light_blue, 'OpenSearch\nHot'),
    ('Active\n1-7d', 4, orange, 'OpenSearch\nWarm'),
    ('Warm\n7-30d', 5.5, orange, 'OS/Cassandra'),
    ('Cold\n30d+', 7, green, 'watsonx.data\nIceberg'),
    ('Archive\n1yr+', 8.5, lavender, 'watsonx.data\nIceberg')
]

for label, x, color, storage in timeline_stages:
    box = FancyBboxPatch((x-0.4, 5.5), 0.8, 1, boxstyle='round,pad=0.05',
                         facecolor=color, edgecolor=text_primary, linewidth=1.5, alpha=0.7)
    ax3.add_patch(box)
    ax3.text(x, 6, label, ha='center', va='center', color=bg_color, 
            fontsize=8, fontweight='bold')
    ax3.text(x, 4.8, storage, ha='center', va='top', color=text_primary,
            fontsize=7, style='italic')
    
    if x < 8.5:
        arrow = FancyArrowPatch((x+0.4, 6), (x+0.6, 6), 
                               arrowstyle='->', mutation_scale=15,
                               color=text_secondary, linewidth=1.5)
        ax3.add_patch(arrow)

# Add cost gradient
cost_y = 3.5
ax3.text(1, cost_y+0.5, 'Cost:', ha='left', va='center', 
        color=text_primary, fontsize=9, fontweight='bold')
for i, cost in enumerate(['10x', '8x', '5x', '3x', '1.5x', '1x']):
    x_pos = 1 + i * 1.5
    ax3.text(x_pos, cost_y, cost, ha='center', va='center',
            color=warning if i < 2 else (highlight if i < 4 else success),
            fontsize=8, fontweight='bold')

# 4. ML Workflow Diagram
ax4 = fig.add_subplot(gs[2, :])
ax4.set_facecolor(bg_color)
ax4.set_xlim(0, 12)
ax4.set_ylim(0, 5)
ax4.axis('off')
ax4.set_title('ML Workflow: Feature Engineering to Real-Time Inference', 
             color=text_primary, fontsize=13, pad=10)

ml_stages = [
    ('Historical Data\n(Iceberg)', 1, 2.5, orange, 1.2, 1.5),
    ('Feature Eng\n(Spark)', 3, 2.5, lavender, 1.2, 1.5),
    ('Model Training\n(Spark ML)', 5.2, 2.5, green, 1.4, 1.5),
    ('Trained Model', 7.8, 2.5, coral, 1.2, 1.5),
    ('Inference Results\n→ OpenSearch', 10, 2.5, light_blue, 1.6, 1.5)
]

for label, x, y, color, w, h in ml_stages:
    box = FancyBboxPatch((x-w/2, y-h/2), w, h, boxstyle='round,pad=0.1',
                         facecolor=color, edgecolor=text_primary, linewidth=2, alpha=0.8)
    ax4.add_patch(box)
    ax4.text(x, y, label, ha='center', va='center', color=bg_color,
            fontsize=9, fontweight='bold')
    
    if x < 10:
        arrow = FancyArrowPatch((x+w/2+0.1, y), (x+w/2+0.5, y),
                               arrowstyle='->', mutation_scale=20,
                               color=text_primary, linewidth=2.5)
        ax4.add_patch(arrow)

# Add workflow annotations
ax4.text(6, 4.2, 'Batch Processing (Hours-Days)', ha='center', va='center',
        color=text_secondary, fontsize=9, style='italic')
ax4.text(6, 0.8, 'watsonx.data Compute', ha='center', va='center',
        color=orange, fontsize=8, fontweight='bold')
ax4.text(10, 0.8, 'Real-Time Serving', ha='center', va='center',
        color=light_blue, fontsize=8, fontweight='bold')

# 5. Hybrid Query Pattern Example
ax5 = fig.add_subplot(gs[3, 0])
ax5.set_facecolor(bg_color)
ax5.set_xlim(0, 10)
ax5.set_ylim(0, 6)
ax5.axis('off')
ax5.set_title('Hybrid Pattern: Real-Time Alert + Historical Context',
             color=text_primary, fontsize=12, pad=10)

# Real-time path
rt_alert = FancyBboxPatch((0.5, 4), 2, 1, boxstyle='round,pad=0.08',
                          facecolor=warning, edgecolor=text_primary, linewidth=1.5, alpha=0.8)
ax5.add_patch(rt_alert)
ax5.text(1.5, 4.5, 'Anomaly Alert\n(OpenSearch)', ha='center', va='center',
        color=bg_color, fontsize=9, fontweight='bold')

os_query = FancyBboxPatch((3.5, 4), 2, 1, boxstyle='round,pad=0.08',
                          facecolor=light_blue, edgecolor=text_primary, linewidth=1.5, alpha=0.8)
ax5.add_patch(os_query)
ax5.text(4.5, 4.5, 'Real-Time Data\n< 100ms', ha='center', va='center',
        color=bg_color, fontsize=9, fontweight='bold')

# Historical context path
hist_query = FancyBboxPatch((3.5, 2), 2, 1, boxstyle='round,pad=0.08',
                            facecolor=orange, edgecolor=text_primary, linewidth=1.5, alpha=0.8)
ax5.add_patch(hist_query)
ax5.text(4.5, 2.5, 'Historical Context\n(Presto)', ha='center', va='center',
        color=bg_color, fontsize=9, fontweight='bold')

# Enrichment
enriched = FancyBboxPatch((6.5, 3), 2.5, 2, boxstyle='round,pad=0.1',
                          facecolor=green, edgecolor=text_primary, linewidth=2, alpha=0.8)
ax5.add_patch(enriched)
ax5.text(7.75, 4, 'Enriched Alert', ha='center', va='center',
        color=bg_color, fontsize=10, fontweight='bold')
ax5.text(7.75, 3.5, 'Real-time + Context', ha='center', va='center',
        color=bg_color, fontsize=8)

# Arrows
arrow1 = FancyArrowPatch((2.5, 4.5), (3.5, 4.5), arrowstyle='->', mutation_scale=15,
                        color=text_primary, linewidth=2)
ax5.add_patch(arrow1)
arrow2 = FancyArrowPatch((5.5, 4.5), (6.5, 4.2), arrowstyle='->', mutation_scale=15,
                        color=text_primary, linewidth=2)
ax5.add_patch(arrow2)
arrow3 = FancyArrowPatch((5.5, 2.5), (6.5, 3.8), arrowstyle='->', mutation_scale=15,
                        color=text_primary, linewidth=2, linestyle='--')
ax5.add_patch(arrow3)

ax5.text(4.5, 1.2, 'Async enrichment (< 2s)', ha='center', va='center',
        color=text_secondary, fontsize=8, style='italic')

# 6. Query Pattern Summary
ax6 = fig.add_subplot(gs[3, 1])
ax6.set_facecolor(bg_color)
ax6.axis('off')
ax6.set_title('Query Patterns Summary', color=text_primary, fontsize=12, pad=10)

patterns_summary = [
    ('OpenSearch Patterns:', light_blue, [
        '• Time-series aggregations',
        '• Geo-spatial queries',
        '• Real-time filtering',
        '• Full-text search'
    ]),
    ('watsonx.data Patterns:', orange, [
        '• Complex JOINs',
        '• Window functions',
        '• Statistical analysis',
        '• Batch aggregations'
    ]),
    ('Hybrid Patterns:', green, [
        '• Fast lookup + enrichment',
        '• Real-time + baseline',
        '• Federated queries'
    ])
]

y_pos = 5.5
for title, color, items in patterns_summary:
    ax6.text(0.5, y_pos, title, ha='left', va='top', color=color,
            fontsize=10, fontweight='bold')
    y_pos -= 0.4
    for item in items:
        ax6.text(0.7, y_pos, item, ha='left', va='top', color=text_primary,
                fontsize=8)
        y_pos -= 0.3
    y_pos -= 0.2

ax6.set_xlim(0, 10)
ax6.set_ylim(0, 6)

plt.tight_layout()
print("\n✓ Comprehensive integration patterns visualization created")
print("=" * 80)

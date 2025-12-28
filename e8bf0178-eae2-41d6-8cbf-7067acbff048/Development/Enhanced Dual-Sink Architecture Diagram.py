import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Circle
import matplotlib.patches as mpatches

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

# Create comprehensive architecture diagram
arch_fig, ax = plt.subplots(1, 1, figsize=(20, 14), facecolor=bg_color)
ax.set_xlim(0, 20)
ax.set_ylim(0, 14)
ax.axis('off')
ax.set_facecolor(bg_color)

# Title
ax.text(10, 13.3, 'Fleet Guardian: Comprehensive Dual-Sink Architecture', 
        fontsize=22, fontweight='bold', ha='center', color=text_primary)
ax.text(10, 12.8, 'navbox.raw → fleet.telemetry.enriched with parallel OpenSearch + watsonx.data paths', 
        fontsize=13, ha='center', color=text_secondary, style='italic')

# === LAYER 1: DATA SOURCES (top) ===
ax.text(10, 12, 'DATA SOURCES', fontsize=11, ha='center', color=highlight, fontweight='bold')

sources = [
    ('Fleet Vehicles', 2, 11),
    ('IoT Sensors', 6, 11),
    ('Logs/Metrics', 10, 11),
    ('External APIs', 14, 11),
    ('CDC Streams', 18, 11)
]

for label, x, y in sources:
    source_box = FancyBboxPatch((x-0.8, y-0.3), 1.6, 0.5, boxstyle="round,pad=0.08", 
                               edgecolor=highlight, facecolor=bg_color, linewidth=1.5)
    ax.add_patch(source_box)
    ax.text(x, y, label, fontsize=8, ha='center', color=text_primary, fontweight='bold')
    # Arrow down to Pulsar
    arrow = FancyArrowPatch((x, y-0.3), (x, 9.8), arrowstyle='->', 
                           mutation_scale=15, linewidth=1.5, color=highlight, alpha=0.7)
    ax.add_patch(arrow)

# === LAYER 2: APACHE PULSAR (message broker) ===
ax.text(10, 10.5, 'MESSAGE BROKER', fontsize=11, ha='center', color=orange, fontweight='bold')

pulsar_box = FancyBboxPatch((4, 9.2), 12, 1.1, boxstyle="round,pad=0.15", 
                           edgecolor=orange, facecolor=bg_color, linewidth=3.5)
ax.add_patch(pulsar_box)
ax.text(10, 9.95, 'Apache Pulsar', fontsize=14, ha='center', color=text_primary, fontweight='bold')
ax.text(5.5, 9.55, 'Topic: navbox.raw', fontsize=9, ha='left', color=orange)
ax.text(5.5, 9.35, 'Topic: fleet.telemetry.enriched', fontsize=9, ha='left', color=orange)
ax.text(14.5, 9.65, 'Partitioned by vehicle_id', fontsize=8, ha='right', color=text_secondary, style='italic')
ax.text(14.5, 9.45, 'Throughput: 100K+ msg/sec', fontsize=8, ha='right', color=text_secondary, style='italic')
ax.text(14.5, 9.25, 'Latency: <100ms', fontsize=8, ha='right', color=success, style='italic')

# === DUAL-SINK PATTERN (splitting) ===
ax.text(10, 8.5, 'DUAL-SINK PATTERN', fontsize=11, ha='center', color=lavender, fontweight='bold')

# Left arrow to OpenSearch path
left_arrow = FancyArrowPatch((7, 9.2), (5, 7.8), arrowstyle='->', 
                            mutation_scale=25, linewidth=3, color=light_blue)
ax.add_patch(left_arrow)
ax.text(5.5, 8.5, 'Real-time', fontsize=9, ha='center', color=light_blue, fontweight='bold')
ax.text(5.5, 8.2, 'Stream', fontsize=8, ha='center', color=text_secondary)

# Right arrow to watsonx path
right_arrow = FancyArrowPatch((13, 9.2), (15, 7.8), arrowstyle='->', 
                             mutation_scale=25, linewidth=3, color=coral)
ax.add_patch(right_arrow)
ax.text(14.5, 8.5, 'Analytics', fontsize=9, ha='center', color=coral, fontweight='bold')
ax.text(14.5, 8.2, 'Batch', fontsize=8, ha='center', color=text_secondary)

# Center arrow to Cassandra (optional path)
center_arrow = FancyArrowPatch((10, 9.2), (10, 7.8), arrowstyle='->', 
                              mutation_scale=20, linewidth=2, color=lavender, linestyle='--')
ax.add_patch(center_arrow)
ax.text(10.8, 8.5, 'Operational', fontsize=8, ha='center', color=lavender)

# === LAYER 3: CONNECTORS ===
ax.text(2, 7.3, 'REAL-TIME PATH', fontsize=10, ha='center', color=light_blue, fontweight='bold',
        bbox=dict(boxstyle='round,pad=0.3', facecolor=bg_color, edgecolor=light_blue, linewidth=2))

ax.text(18, 7.3, 'ANALYTICS PATH', fontsize=10, ha='center', color=coral, fontweight='bold',
        bbox=dict(boxstyle='round,pad=0.3', facecolor=bg_color, edgecolor=coral, linewidth=2))

# OpenSearch Connector
os_conn = FancyBboxPatch((3.5, 6.5), 3, 0.9, boxstyle="round,pad=0.1", 
                        edgecolor=light_blue, facecolor=bg_color, linewidth=2.5)
ax.add_patch(os_conn)
ax.text(5, 7.1, 'OpenSearch Sink Connector', fontsize=10, ha='center', color=text_primary, fontweight='bold')
ax.text(5, 6.8, 'Latency: <500ms | Format: JSON', fontsize=7, ha='center', color=text_secondary)

# Kafka Connect
kafka_conn = FancyBboxPatch((13.5, 6.5), 3, 0.9, boxstyle="round,pad=0.1", 
                           edgecolor=coral, facecolor=bg_color, linewidth=2.5)
ax.add_patch(kafka_conn)
ax.text(15, 7.1, 'Kafka Connect / Pulsar IO', fontsize=10, ha='center', color=text_primary, fontweight='bold')
ax.text(15, 6.8, 'Batch: 1-5 min | Format: Avro/JSON', fontsize=7, ha='center', color=text_secondary)

# Cassandra (middle path)
cassandra_conn = FancyBboxPatch((8.5, 6.5), 3, 0.9, boxstyle="round,pad=0.1", 
                               edgecolor=lavender, facecolor=bg_color, linewidth=2)
ax.add_patch(cassandra_conn)
ax.text(10, 7.1, 'HCD/Cassandra Direct Write', fontsize=9, ha='center', color=text_primary, fontweight='bold')
ax.text(10, 6.8, 'Latency: <1ms | Time-series', fontsize=7, ha='center', color=text_secondary)

# Arrows down from connectors
FancyArrowPatch((5, 6.5), (5, 5.7), arrowstyle='->', mutation_scale=20, linewidth=2.5, color=light_blue).set_clip_on(False)
ax.add_patch(FancyArrowPatch((5, 6.5), (5, 5.7), arrowstyle='->', mutation_scale=20, linewidth=2.5, color=light_blue))
ax.add_patch(FancyArrowPatch((15, 6.5), (15, 5.7), arrowstyle='->', mutation_scale=20, linewidth=2.5, color=coral))
ax.add_patch(FancyArrowPatch((10, 6.5), (10, 5.7), arrowstyle='->', mutation_scale=20, linewidth=1.8, color=lavender))

# === LAYER 4: STORAGE ===
ax.text(10, 5.2, 'STORAGE LAYER', fontsize=11, ha='center', color=green, fontweight='bold')

# OpenSearch Storage
os_storage = FancyBboxPatch((2.5, 3.5), 5, 1.5, boxstyle="round,pad=0.12", 
                           edgecolor=light_blue, facecolor=bg_color, linewidth=3)
ax.add_patch(os_storage)
ax.text(5, 4.7, 'OpenSearch Cluster', fontsize=11, ha='center', color=text_primary, fontweight='bold')
ax.text(5, 4.4, '📄 JSON Documents', fontsize=8, ha='center', color=light_blue)
ax.text(5, 4.15, '🔍 Inverted Indices', fontsize=8, ha='center', color=light_blue)
ax.text(5, 3.9, '⏱️ Time-series Indices (ILM)', fontsize=8, ha='center', color=light_blue)
ax.text(5, 3.65, 'Retention: 7d hot, 30d warm, 90d cold', fontsize=7, ha='center', color=text_secondary, style='italic')

# watsonx.data Storage
wx_storage = FancyBboxPatch((12.5, 3.5), 5, 1.5, boxstyle="round,pad=0.12", 
                           edgecolor=coral, facecolor=bg_color, linewidth=3)
ax.add_patch(wx_storage)
ax.text(15, 4.7, 'watsonx.data Lakehouse', fontsize=11, ha='center', color=text_primary, fontweight='bold')
ax.text(15, 4.4, '📦 Parquet Files (S3/COS)', fontsize=8, ha='center', color=coral)
ax.text(15, 4.15, '🧊 Apache Iceberg Tables', fontsize=8, ha='center', color=coral)
ax.text(15, 3.9, '🗜️ Snappy/Zstd Compression', fontsize=8, ha='center', color=coral)
ax.text(15, 3.65, 'Retention: Unlimited (~$0.023/GB/mo)', fontsize=7, ha='center', color=text_secondary, style='italic')

# Cassandra Storage
cass_storage = FancyBboxPatch((7.5, 3.5), 5, 1.5, boxstyle="round,pad=0.12", 
                             edgecolor=lavender, facecolor=bg_color, linewidth=2.5)
ax.add_patch(cass_storage)
ax.text(10, 4.7, 'HCD/Cassandra Cluster', fontsize=11, ha='center', color=text_primary, fontweight='bold')
ax.text(10, 4.4, '⚡ Wide-row Time-series', fontsize=8, ha='center', color=lavender)
ax.text(10, 4.15, '📊 Operational Metrics Store', fontsize=8, ha='center', color=lavender)
ax.text(10, 3.9, '🔄 CDC to watsonx.data', fontsize=8, ha='center', color=lavender)
ax.text(10, 3.65, 'Retention: 30d hot, then CDC archive', fontsize=7, ha='center', color=text_secondary, style='italic')

# CDC arrow from Cassandra to watsonx
cdc_arrow = FancyArrowPatch((12.5, 4.2), (12.5, 4.2), arrowstyle='->', 
                           mutation_scale=15, linewidth=2, color=lavender, linestyle='--')
ax.annotate('', xy=(12.5, 4.2), xytext=(12.5, 4.2),
            arrowprops=dict(arrowstyle='->', lw=2, color=lavender, linestyle='--'))
ax.text(11.5, 4.5, 'CDC', fontsize=7, ha='center', color=lavender, fontweight='bold')

# === LAYER 5: PROCESSING ===
ax.text(10, 2.9, 'PROCESSING LAYER', fontsize=11, ha='center', color=orange, fontweight='bold')

# OpenSearch Processing
os_proc = FancyBboxPatch((2.5, 1.5), 5, 1.2, boxstyle="round,pad=0.1", 
                        edgecolor=light_blue, facecolor=bg_color, linewidth=2.5)
ax.add_patch(os_proc)
ax.text(5, 2.4, 'OpenSearch Processing', fontsize=10, ha='center', color=text_primary, fontweight='bold')
ax.text(5, 2.15, '🤖 RCF Anomaly Detection', fontsize=8, ha='center', color=light_blue)
ax.text(5, 1.95, '⚠️ Alerting Plugin', fontsize=8, ha='center', color=light_blue)
ax.text(5, 1.75, '🔧 Orchestrator Actions', fontsize=8, ha='center', color=light_blue)
ax.text(5, 1.55, 'Latency: 1-5 sec', fontsize=7, ha='center', color=success, style='italic')

# watsonx Processing
wx_proc = FancyBboxPatch((12.5, 1.5), 5, 1.2, boxstyle="round,pad=0.1", 
                        edgecolor=coral, facecolor=bg_color, linewidth=2.5)
ax.add_patch(wx_proc)
ax.text(15, 2.4, 'watsonx.data Processing', fontsize=10, ha='center', color=text_primary, fontweight='bold')
ax.text(15, 2.15, '⚙️ Spark Batch Jobs', fontsize=8, ha='center', color=coral)
ax.text(15, 1.95, '🔍 Presto Interactive SQL', fontsize=8, ha='center', color=coral)
ax.text(15, 1.75, '🧠 Spark MLlib Training', fontsize=8, ha='center', color=coral)
ax.text(15, 1.55, 'Latency: 10s - 60 min', fontsize=7, ha='center', color=warning, style='italic')

# === LAYER 6: QUERY & APPLICATIONS ===
ax.text(10, 0.9, 'QUERY & APPLICATIONS', fontsize=11, ha='center', color=highlight, fontweight='bold')

# Query interfaces
queries = [
    ('Dashboards\n(Real-time)', 3.5, 0.3, light_blue),
    ('Alerting\nSystems', 6.5, 0.3, light_blue),
    ('Analytics\nReports', 13.5, 0.3, coral),
    ('ML Models\n& Training', 16.5, 0.3, coral)
]

for label, x, y, color in queries:
    query_box = FancyBboxPatch((x-0.9, y-0.15), 1.8, 0.5, boxstyle="round,pad=0.08", 
                              edgecolor=color, facecolor=bg_color, linewidth=2)
    ax.add_patch(query_box)
    ax.text(x, y+0.1, label, fontsize=7, ha='center', color=text_primary, fontweight='bold')

# Connection arrows
ax.add_patch(FancyArrowPatch((4.5, 1.5), (3.5, 0.5), arrowstyle='->', mutation_scale=15, linewidth=1.5, color=light_blue))
ax.add_patch(FancyArrowPatch((5.5, 1.5), (6.5, 0.5), arrowstyle='->', mutation_scale=15, linewidth=1.5, color=light_blue))
ax.add_patch(FancyArrowPatch((14.5, 1.5), (13.5, 0.5), arrowstyle='->', mutation_scale=15, linewidth=1.5, color=coral))
ax.add_patch(FancyArrowPatch((15.5, 1.5), (16.5, 0.5), arrowstyle='->', mutation_scale=15, linewidth=1.5, color=coral))

plt.tight_layout()
print("✓ Enhanced dual-sink architecture diagram created")
print("  ✓ Complete data flow from sources → Pulsar → dual sinks")
print("  ✓ Storage formats: JSON (OpenSearch) vs Parquet (watsonx.data)")
print("  ✓ Latency requirements annotated at each layer")
print("  ✓ HCD/Cassandra operational store with CDC integration")
print("  ✓ Processing pipelines and query access patterns")

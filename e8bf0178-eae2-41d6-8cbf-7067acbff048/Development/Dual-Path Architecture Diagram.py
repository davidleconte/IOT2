import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle

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

arch_fig, ax = plt.subplots(1, 1, figsize=(16, 10), facecolor=bg_color)
ax.set_xlim(0, 10)
ax.set_ylim(0, 10)
ax.axis('off')
ax.set_facecolor(bg_color)

# Title
ax.text(5, 9.5, 'Fleet Guardian: Dual-Path Ingestion Architecture', 
        fontsize=20, fontweight='bold', ha='center', color=text_primary)
ax.text(5, 9.1, 'OpenSearch (Real-time) + watsonx.data (Analytics)', 
        fontsize=14, ha='center', color=text_secondary, style='italic')

# Data Source
source_box = FancyBboxPatch((0.5, 7.5), 1.5, 0.8, boxstyle="round,pad=0.1", 
                           edgecolor=highlight, facecolor=bg_color, linewidth=2.5)
ax.add_patch(source_box)
ax.text(1.25, 8.1, 'Fleet Data', fontsize=11, ha='center', color=text_primary, fontweight='bold')
ax.text(1.25, 7.85, 'Sources', fontsize=9, ha='center', color=text_secondary)

# Apache Pulsar (central broker)
pulsar_box = FancyBboxPatch((2.5, 7.3), 1.5, 1.2, boxstyle="round,pad=0.1", 
                           edgecolor=orange, facecolor=bg_color, linewidth=3)
ax.add_patch(pulsar_box)
ax.text(3.25, 8.2, 'Apache Pulsar', fontsize=12, ha='center', color=text_primary, fontweight='bold')
ax.text(3.25, 7.95, 'Message Broker', fontsize=9, ha='center', color=text_secondary)
ax.text(3.25, 7.7, 'Topics:', fontsize=8, ha='center', color=text_secondary)
ax.text(3.25, 7.5, 'telemetry | logs | metrics', fontsize=8, ha='center', color=light_blue)

# Arrow from source to Pulsar
arrow1 = FancyArrowPatch((2, 7.9), (2.5, 7.9), arrowstyle='->', 
                        mutation_scale=25, linewidth=2.5, color=highlight)
ax.add_patch(arrow1)

# UPPER PATH: OpenSearch (Real-time)
# Connector
os_connector = FancyBboxPatch((4.5, 7.8), 1.2, 0.6, boxstyle="round,pad=0.08", 
                             edgecolor=light_blue, facecolor=bg_color, linewidth=2)
ax.add_patch(os_connector)
ax.text(5.1, 8.3, 'OpenSearch', fontsize=9, ha='center', color=text_primary, fontweight='bold')
ax.text(5.1, 8.05, 'Connector', fontsize=8, ha='center', color=text_secondary)

# OpenSearch storage
os_storage = FancyBboxPatch((6.2, 7.5), 1.4, 1.2, boxstyle="round,pad=0.1", 
                           edgecolor=light_blue, facecolor=bg_color, linewidth=3)
ax.add_patch(os_storage)
ax.text(6.9, 8.4, 'OpenSearch', fontsize=11, ha='center', color=text_primary, fontweight='bold')
ax.text(6.9, 8.15, 'Indices', fontsize=9, ha='center', color=text_secondary)
ax.text(6.9, 7.9, '• JSON documents', fontsize=7, ha='center', color=light_blue)
ax.text(6.9, 7.7, '• Inverted indices', fontsize=7, ha='center', color=light_blue)

# Query interface - OpenSearch
os_query = FancyBboxPatch((8.1, 7.8), 1.3, 0.6, boxstyle="round,pad=0.08", 
                         edgecolor=green, facecolor=bg_color, linewidth=2)
ax.add_patch(os_query)
ax.text(8.75, 8.25, 'Real-time', fontsize=9, ha='center', color=text_primary, fontweight='bold')
ax.text(8.75, 8, 'Queries', fontsize=8, ha='center', color=text_secondary)

# Arrows for upper path
arrow2 = FancyArrowPatch((4, 8.1), (4.5, 8.1), arrowstyle='->', 
                        mutation_scale=20, linewidth=2, color=light_blue)
ax.add_patch(arrow2)
arrow3 = FancyArrowPatch((5.7, 8.1), (6.2, 8.1), arrowstyle='->', 
                        mutation_scale=20, linewidth=2, color=light_blue)
ax.add_patch(arrow3)
arrow4 = FancyArrowPatch((7.6, 8.1), (8.1, 8.1), arrowstyle='->', 
                        mutation_scale=20, linewidth=2, color=green)
ax.add_patch(arrow4)

# Label for upper path
ax.text(6.9, 8.9, 'REAL-TIME PATH (seconds latency)', fontsize=10, ha='center', 
        color=light_blue, fontweight='bold', bbox=dict(boxstyle='round,pad=0.3', 
        facecolor=bg_color, edgecolor=light_blue, linewidth=1.5))

# LOWER PATH: watsonx.data (Analytics)
# Kafka Connect
kafka_connector = FancyBboxPatch((4.5, 6.4), 1.2, 0.6, boxstyle="round,pad=0.08", 
                               edgecolor=coral, facecolor=bg_color, linewidth=2)
ax.add_patch(kafka_connector)
ax.text(5.1, 6.9, 'Kafka Connect', fontsize=9, ha='center', color=text_primary, fontweight='bold')
ax.text(5.1, 6.65, '/ Pulsar IO', fontsize=8, ha='center', color=text_secondary)

# Object Storage
obj_storage = FancyBboxPatch((6.2, 6.1), 1.4, 1.2, boxstyle="round,pad=0.1", 
                            edgecolor=coral, facecolor=bg_color, linewidth=3)
ax.add_patch(obj_storage)
ax.text(6.9, 7, 'watsonx.data', fontsize=11, ha='center', color=text_primary, fontweight='bold')
ax.text(6.9, 6.75, 'Storage', fontsize=9, ha='center', color=text_secondary)
ax.text(6.9, 6.5, '• Iceberg tables', fontsize=7, ha='center', color=coral)
ax.text(6.9, 6.3, '• Parquet files', fontsize=7, ha='center', color=coral)

# Query engine - watsonx
wx_query = FancyBboxPatch((8.1, 6.4), 1.3, 0.6, boxstyle="round,pad=0.08", 
                         edgecolor=lavender, facecolor=bg_color, linewidth=2)
ax.add_patch(wx_query)
ax.text(8.75, 6.85, 'Analytics', fontsize=9, ha='center', color=text_primary, fontweight='bold')
ax.text(8.75, 6.6, 'Queries', fontsize=8, ha='center', color=text_secondary)

# Arrows for lower path
arrow5 = FancyArrowPatch((4, 7.7), (4.5, 6.7), arrowstyle='->', 
                        mutation_scale=20, linewidth=2, color=coral)
ax.add_patch(arrow5)
arrow6 = FancyArrowPatch((5.7, 6.7), (6.2, 6.7), arrowstyle='->', 
                        mutation_scale=20, linewidth=2, color=coral)
ax.add_patch(arrow6)
arrow7 = FancyArrowPatch((7.6, 6.7), (8.1, 6.7), arrowstyle='->', 
                        mutation_scale=20, linewidth=2, color=lavender)
ax.add_patch(arrow7)

# Label for lower path
ax.text(6.9, 5.7, 'ANALYTICS PATH (minutes-hours latency)', fontsize=10, ha='center', 
        color=coral, fontweight='bold', bbox=dict(boxstyle='round,pad=0.3', 
        facecolor=bg_color, edgecolor=coral, linewidth=1.5))

# Processing capabilities boxes
proc_y = 4.5
ax.text(5, proc_y + 0.8, 'PROCESSING CAPABILITIES', fontsize=12, ha='center', 
        color=text_primary, fontweight='bold')

# OpenSearch processing
os_proc = FancyBboxPatch((2.5, proc_y - 0.8), 2.2, 1.2, boxstyle="round,pad=0.1", 
                        edgecolor=light_blue, facecolor=bg_color, linewidth=2)
ax.add_patch(os_proc)
ax.text(3.6, proc_y + 0.2, 'OpenSearch', fontsize=10, ha='center', color=text_primary, fontweight='bold')
ax.text(3.6, proc_y - 0.05, '• RCF Anomaly Detection', fontsize=8, ha='center', color=light_blue)
ax.text(3.6, proc_y - 0.25, '• Stream Processing', fontsize=8, ha='center', color=light_blue)
ax.text(3.6, proc_y - 0.45, '• Real-time Aggregations', fontsize=8, ha='center', color=light_blue)

# watsonx processing
wx_proc = FancyBboxPatch((5.3, proc_y - 0.8), 2.2, 1.2, boxstyle="round,pad=0.1", 
                        edgecolor=coral, facecolor=bg_color, linewidth=2)
ax.add_patch(wx_proc)
ax.text(6.4, proc_y + 0.2, 'watsonx.data', fontsize=10, ha='center', color=text_primary, fontweight='bold')
ax.text(6.4, proc_y - 0.05, '• Spark MLlib Models', fontsize=8, ha='center', color=coral)
ax.text(6.4, proc_y - 0.25, '• Presto SQL Analytics', fontsize=8, ha='center', color=coral)
ax.text(6.4, proc_y - 0.45, '• Batch Processing', fontsize=8, ha='center', color=coral)

# Use cases
use_y = 2.2
ax.text(5, use_y + 0.8, 'USE CASES', fontsize=12, ha='center', 
        color=text_primary, fontweight='bold')

# Real-time use cases
rt_use = FancyBboxPatch((1.5, use_y - 0.8), 2.8, 1.2, boxstyle="round,pad=0.1", 
                       edgecolor=green, facecolor=bg_color, linewidth=2)
ax.add_patch(rt_use)
ax.text(2.9, use_y + 0.2, 'Real-time Operations', fontsize=10, ha='center', color=text_primary, fontweight='bold')
ax.text(2.9, use_y - 0.05, '✓ Fleet health dashboards', fontsize=8, ha='center', color=green)
ax.text(2.9, use_y - 0.25, '✓ Critical alerts (<1 min)', fontsize=8, ha='center', color=green)
ax.text(2.9, use_y - 0.45, '✓ Anomaly detection alerts', fontsize=8, ha='center', color=green)

# Analytics use cases
an_use = FancyBboxPatch((5.7, use_y - 0.8), 2.8, 1.2, boxstyle="round,pad=0.1", 
                       edgecolor=lavender, facecolor=bg_color, linewidth=2)
ax.add_patch(an_use)
ax.text(7.1, use_y + 0.2, 'Deep Analytics', fontsize=10, ha='center', color=text_primary, fontweight='bold')
ax.text(7.1, use_y - 0.05, '✓ Historical trend analysis', fontsize=8, ha='center', color=lavender)
ax.text(7.1, use_y - 0.25, '✓ Predictive maintenance', fontsize=8, ha='center', color=lavender)
ax.text(7.1, use_y - 0.45, '✓ Multi-table joins & reports', fontsize=8, ha='center', color=lavender)

# Legend
legend_y = 0.5
ax.text(1, legend_y, '▶', fontsize=14, color=highlight, fontweight='bold')
ax.text(1.3, legend_y, 'Data Ingestion', fontsize=8, color=text_secondary)
ax.text(3, legend_y, '▶', fontsize=14, color=light_blue, fontweight='bold')
ax.text(3.3, legend_y, 'Real-time Flow', fontsize=8, color=text_secondary)
ax.text(5, legend_y, '▶', fontsize=14, color=coral, fontweight='bold')
ax.text(5.3, legend_y, 'Analytics Flow', fontsize=8, color=text_secondary)
ax.text(7.2, legend_y, '▶', fontsize=14, color=green, fontweight='bold')
ax.text(7.5, legend_y, 'Query/Access', fontsize=8, color=text_secondary)

plt.tight_layout()
print("✓ Dual-path architecture diagram created")
print("  - Shows parallel ingestion through Pulsar")
print("  - Real-time path via OpenSearch")
print("  - Analytics path via watsonx.data")
print("  - Processing and use case distinctions")
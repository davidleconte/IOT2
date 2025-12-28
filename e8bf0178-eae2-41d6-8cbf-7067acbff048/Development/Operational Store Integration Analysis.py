import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle
import matplotlib.patches as mpatches

# Analysis of Operational Store (IBM HCD/Cassandra) integration
print("=" * 100)
print("OPERATIONAL STORE INTEGRATION ANALYSIS")
print("IBM HCD 1.2.4 / Cassandra 4.1+ Integration Patterns")
print("=" * 100)
print()

# Operational store comparison
ops_store_data = {
    'Aspect': [
        'Primary Use Case',
        'Write Performance',
        'Read Performance',
        'Data Model',
        'Consistency Model',
        'Export Mechanism',
        'Integration Pattern',
        'watsonx.data Role'
    ],
    'IBM HCD 1.2.4': [
        'Operational transactions, vehicle state',
        'Very High (in-memory + disk)',
        'Ultra Low Latency (<1ms)',
        'Key-value, columnar',
        'Tunable (eventual to strong)',
        'CDC via Debezium/custom',
        'HCD → CDC → Pulsar → watsonx.data',
        'Analytics on operational history'
    ],
    'Cassandra 4.1+': [
        'Distributed operational data',
        'Very High (optimized writes)',
        'Fast (typically <10ms)',
        'Wide-column store',
        'Tunable (eventual to strong)',
        'CDC native in 4.0+',
        'Cassandra → CDC → Kafka → watsonx.data',
        'Analytics on operational history'
    ]
}

ops_df = pd.DataFrame(ops_store_data)
print(ops_df.to_string(index=False))
print()

# Integration architecture with operational store
print("INTEGRATION ARCHITECTURE OPTIONS:")
print("-" * 100)
print()
print("Option 1: Direct Export (Batch)")
print("  IBM HCD/Cassandra → Scheduled Export → Object Storage → watsonx.data Iceberg")
print("  • Pros: Simple, no streaming infrastructure")
print("  • Cons: Higher latency (minutes to hours), batch-oriented")
print("  • Use: Historical analysis, compliance, data lake population")
print()
print("Option 2: CDC Streaming (Real-time)")
print("  IBM HCD/Cassandra → CDC → Pulsar/Kafka → watsonx.data Iceberg")
print("  • Pros: Real-time sync, event-driven, captures all changes")
print("  • Cons: Complex setup, requires CDC tooling (Debezium)")
print("  • Use: Real-time analytics, event sourcing, audit trails")
print()
print("Option 3: Dual-Write Pattern")
print("  Application → [IBM HCD/Cassandra + Pulsar] → watsonx.data")
print("  • Pros: Independent systems, no CDC dependency")
print("  • Cons: Application complexity, consistency challenges")
print("  • Use: New systems, when CDC not available")
print()

# Create visualization of operational store integration
ops_fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))
ops_fig.patch.set_facecolor(bg_color)

# Left side: Integration patterns
ax1.set_xlim(0, 10)
ax1.set_ylim(0, 12)
ax1.axis('off')
ax1.set_facecolor(bg_color)
ax1.set_title('Operational Store Integration Patterns', 
              fontsize=14, color=text_primary, fontweight='bold', pad=20)

# Pattern 1: Direct Export
y_start = 10
pattern1_title = ax1.text(5, y_start, 'Pattern 1: Batch Export', 
                          ha='center', fontsize=11, color=highlight, fontweight='bold')
ops_store1 = FancyBboxPatch((0.5, y_start-1.5), 2, 0.8, boxstyle="round,pad=0.1",
                            edgecolor=light_blue, facecolor=light_blue, alpha=0.3, linewidth=2)
ax1.add_patch(ops_store1)
ax1.text(1.5, y_start-1.1, 'Ops Store\n(HCD/Cass)', ha='center', va='center',
         fontsize=9, color=text_primary, fontweight='bold')

export1 = FancyBboxPatch((3.5, y_start-1.5), 2, 0.8, boxstyle="round,pad=0.1",
                         edgecolor=orange, facecolor=orange, alpha=0.3, linewidth=2)
ax1.add_patch(export1)
ax1.text(4.5, y_start-1.1, 'Scheduled\nExport Job', ha='center', va='center',
         fontsize=9, color=text_primary, fontweight='bold')

wxdata1 = FancyBboxPatch((6.5, y_start-1.5), 2.5, 0.8, boxstyle="round,pad=0.1",
                         edgecolor=green, facecolor=green, alpha=0.3, linewidth=2)
ax1.add_patch(wxdata1)
ax1.text(7.75, y_start-1.1, 'watsonx.data\nIceberg', ha='center', va='center',
         fontsize=9, color=text_primary, fontweight='bold')

arrow1_1 = FancyArrowPatch((2.5, y_start-1.1), (3.5, y_start-1.1),
                          arrowstyle='->', mutation_scale=20, linewidth=2, color=text_secondary)
ax1.add_patch(arrow1_1)
arrow1_2 = FancyArrowPatch((5.5, y_start-1.1), (6.5, y_start-1.1),
                          arrowstyle='->', mutation_scale=20, linewidth=2, color=text_secondary)
ax1.add_patch(arrow1_2)
ax1.text(5, y_start-2.2, 'Latency: Minutes to Hours | Complexity: Low', 
         ha='center', fontsize=8, color=text_secondary, style='italic')

# Pattern 2: CDC Streaming
y_start = 6.5
pattern2_title = ax1.text(5, y_start, 'Pattern 2: CDC Streaming', 
                          ha='center', fontsize=11, color=highlight, fontweight='bold')
ops_store2 = FancyBboxPatch((0.5, y_start-1.5), 2, 0.8, boxstyle="round,pad=0.1",
                            edgecolor=light_blue, facecolor=light_blue, alpha=0.3, linewidth=2)
ax1.add_patch(ops_store2)
ax1.text(1.5, y_start-1.1, 'Ops Store\n(HCD/Cass)', ha='center', va='center',
         fontsize=9, color=text_primary, fontweight='bold')

cdc2 = FancyBboxPatch((3, y_start-1.5), 1.5, 0.8, boxstyle="round,pad=0.1",
                      edgecolor=coral, facecolor=coral, alpha=0.3, linewidth=2)
ax1.add_patch(cdc2)
ax1.text(3.75, y_start-1.1, 'CDC\n(Debezium)', ha='center', va='center',
         fontsize=9, color=text_primary, fontweight='bold')

stream2 = FancyBboxPatch((5, y_start-1.5), 1.5, 0.8, boxstyle="round,pad=0.1",
                         edgecolor=lavender, facecolor=lavender, alpha=0.3, linewidth=2)
ax1.add_patch(stream2)
ax1.text(5.75, y_start-1.1, 'Pulsar/\nKafka', ha='center', va='center',
         fontsize=9, color=text_primary, fontweight='bold')

wxdata2 = FancyBboxPatch((7, y_start-1.5), 2.5, 0.8, boxstyle="round,pad=0.1",
                         edgecolor=green, facecolor=green, alpha=0.3, linewidth=2)
ax1.add_patch(wxdata2)
ax1.text(8.25, y_start-1.1, 'watsonx.data\nIceberg', ha='center', va='center',
         fontsize=9, color=text_primary, fontweight='bold')

arrow2_1 = FancyArrowPatch((2.5, y_start-1.1), (3, y_start-1.1),
                          arrowstyle='->', mutation_scale=20, linewidth=2, color=text_secondary)
ax1.add_patch(arrow2_1)
arrow2_2 = FancyArrowPatch((4.5, y_start-1.1), (5, y_start-1.1),
                          arrowstyle='->', mutation_scale=20, linewidth=2, color=text_secondary)
ax1.add_patch(arrow2_2)
arrow2_3 = FancyArrowPatch((6.5, y_start-1.1), (7, y_start-1.1),
                          arrowstyle='->', mutation_scale=20, linewidth=2, color=text_secondary)
ax1.add_patch(arrow2_3)
ax1.text(5, y_start-2.2, 'Latency: Seconds | Complexity: Medium-High', 
         ha='center', fontsize=8, color=text_secondary, style='italic')

# Pattern 3: Dual-Write
y_start = 3
pattern3_title = ax1.text(5, y_start, 'Pattern 3: Dual-Write', 
                          ha='center', fontsize=11, color=highlight, fontweight='bold')
app3 = FancyBboxPatch((1, y_start-1.5), 2, 0.8, boxstyle="round,pad=0.1",
                      edgecolor=light_blue, facecolor=light_blue, alpha=0.3, linewidth=2)
ax1.add_patch(app3)
ax1.text(2, y_start-1.1, 'Application', ha='center', va='center',
         fontsize=9, color=text_primary, fontweight='bold')

ops_store3 = FancyBboxPatch((4.5, y_start-2.2), 1.8, 0.6, boxstyle="round,pad=0.1",
                            edgecolor=orange, facecolor=orange, alpha=0.3, linewidth=2)
ax1.add_patch(ops_store3)
ax1.text(5.4, y_start-1.9, 'Ops Store', ha='center', va='center',
         fontsize=8, color=text_primary, fontweight='bold')

stream3 = FancyBboxPatch((4.5, y_start-0.8), 1.8, 0.6, boxstyle="round,pad=0.1",
                         edgecolor=lavender, facecolor=lavender, alpha=0.3, linewidth=2)
ax1.add_patch(stream3)
ax1.text(5.4, y_start-0.5, 'Pulsar', ha='center', va='center',
         fontsize=8, color=text_primary, fontweight='bold')

wxdata3 = FancyBboxPatch((7, y_start-1.5), 2, 0.8, boxstyle="round,pad=0.1",
                         edgecolor=green, facecolor=green, alpha=0.3, linewidth=2)
ax1.add_patch(wxdata3)
ax1.text(8, y_start-1.1, 'watsonx.data', ha='center', va='center',
         fontsize=9, color=text_primary, fontweight='bold')

arrow3_1 = FancyArrowPatch((3, y_start-1.5), (4.5, y_start-1.9),
                          arrowstyle='->', mutation_scale=20, linewidth=2, color=text_secondary)
ax1.add_patch(arrow3_1)
arrow3_2 = FancyArrowPatch((3, y_start-0.7), (4.5, y_start-0.5),
                          arrowstyle='->', mutation_scale=20, linewidth=2, color=text_secondary)
ax1.add_patch(arrow3_2)
arrow3_3 = FancyArrowPatch((6.3, y_start-0.5), (7, y_start-0.8),
                          arrowstyle='->', mutation_scale=20, linewidth=2, color=text_secondary)
ax1.add_patch(arrow3_3)
ax1.text(5, y_start-2.9, 'Latency: Seconds | Complexity: Medium (App-level)', 
         ha='center', fontsize=8, color=text_secondary, style='italic')

# Right side: Data flow scenarios
ax2.set_xlim(0, 10)
ax2.set_ylim(0, 12)
ax2.axis('off')
ax2.set_facecolor(bg_color)
ax2.set_title('Fleet Guardian Complete Data Flow', 
              fontsize=14, color=text_primary, fontweight='bold', pad=20)

# Complete architecture
y_pos = 10.5
navbox = FancyBboxPatch((3.5, y_pos), 3, 0.7, boxstyle="round,pad=0.1",
                        edgecolor=highlight, facecolor=highlight, alpha=0.4, linewidth=2)
ax2.add_patch(navbox)
ax2.text(5, y_pos+0.35, 'NavBox Simulator', ha='center', va='center',
         fontsize=10, color=text_primary, fontweight='bold')

# Pulsar
y_pos = 9
pulsar_box = FancyBboxPatch((3.5, y_pos), 3, 0.7, boxstyle="round,pad=0.1",
                            edgecolor=lavender, facecolor=lavender, alpha=0.4, linewidth=2)
ax2.add_patch(pulsar_box)
ax2.text(5, y_pos+0.35, 'Apache Pulsar', ha='center', va='center',
         fontsize=10, color=text_primary, fontweight='bold')

arrow_sim_pulsar = FancyArrowPatch((5, 10.5), (5, 9.7),
                                   arrowstyle='->', mutation_scale=20, linewidth=2.5, color=text_primary)
ax2.add_patch(arrow_sim_pulsar)

# Three paths
# Path 1: OpenSearch (left)
os_box = FancyBboxPatch((0.2, 6.5), 2.5, 1.5, boxstyle="round,pad=0.1",
                        edgecolor=light_blue, facecolor=light_blue, alpha=0.3, linewidth=2)
ax2.add_patch(os_box)
ax2.text(1.45, 7.6, 'OpenSearch', ha='center', va='top',
         fontsize=9, color=text_primary, fontweight='bold')
ax2.text(1.45, 7.2, '• Real-time search', ha='center', va='top',
         fontsize=7, color=text_primary)
ax2.text(1.45, 6.9, '• Anomaly detection', ha='center', va='top',
         fontsize=7, color=text_primary)
ax2.text(1.45, 6.6, '• Ops dashboards', ha='center', va='top',
         fontsize=7, color=text_primary)

arrow_p_os = FancyArrowPatch((3.7, 9), (1.8, 8),
                             arrowstyle='->', mutation_scale=18, linewidth=2, color=light_blue)
ax2.add_patch(arrow_p_os)
ax2.text(2.5, 8.7, 'Stream', ha='center', fontsize=7, color=light_blue, fontweight='bold')

# Path 2: Operational Store (middle)
ops_box = FancyBboxPatch((3.5, 6.5), 3, 1.5, boxstyle="round,pad=0.1",
                         edgecolor=orange, facecolor=orange, alpha=0.3, linewidth=2)
ax2.add_patch(ops_box)
ax2.text(5, 7.6, 'Operational Store', ha='center', va='top',
         fontsize=9, color=text_primary, fontweight='bold')
ax2.text(5, 7.2, 'IBM HCD / Cassandra', ha='center', va='top',
         fontsize=8, color=text_primary, style='italic')
ax2.text(5, 6.9, '• Transactional data', ha='center', va='top',
         fontsize=7, color=text_primary)
ax2.text(5, 6.6, '• Vehicle state', ha='center', va='top',
         fontsize=7, color=text_primary)

arrow_p_ops = FancyArrowPatch((5, 9), (5, 8),
                              arrowstyle='->', mutation_scale=18, linewidth=2, color=orange)
ax2.add_patch(arrow_p_ops)
ax2.text(5.8, 8.5, 'Dual\nWrite', ha='center', fontsize=7, color=orange, fontweight='bold')

# Path 3: watsonx.data (right)
wx_box = FancyBboxPatch((7.3, 6.5), 2.5, 1.5, boxstyle="round,pad=0.1",
                        edgecolor=green, facecolor=green, alpha=0.3, linewidth=2)
ax2.add_patch(wx_box)
ax2.text(8.55, 7.6, 'watsonx.data', ha='center', va='top',
         fontsize=9, color=text_primary, fontweight='bold')
ax2.text(8.55, 7.2, '• Iceberg tables', ha='center', va='top',
         fontsize=7, color=text_primary)
ax2.text(8.55, 6.9, '• SQL analytics', ha='center', va='top',
         fontsize=7, color=text_primary)
ax2.text(8.55, 6.6, '• ML training', ha='center', va='top',
         fontsize=7, color=text_primary)

arrow_p_wx = FancyArrowPatch((6.3, 9), (8.2, 8),
                             arrowstyle='->', mutation_scale=18, linewidth=2, color=green)
ax2.add_patch(arrow_p_wx)
ax2.text(7.5, 8.7, 'Stream', ha='center', fontsize=7, color=green, fontweight='bold')

# CDC from ops store to watsonx.data
arrow_ops_wx = FancyArrowPatch((6.5, 7.2), (7.3, 7.2),
                               arrowstyle='->', mutation_scale=18, linewidth=2, 
                               color=coral, linestyle='dashed')
ax2.add_patch(arrow_ops_wx)
ax2.text(6.9, 7.5, 'CDC', ha='center', fontsize=7, color=coral, fontweight='bold')

# Use cases at bottom
use_y = 4.5
ax2.text(5, use_y, 'PRIMARY USE CASES', ha='center', fontsize=10, 
         color=highlight, fontweight='bold')

use1_box = FancyBboxPatch((0.5, use_y-1.5), 2.8, 1, boxstyle="round,pad=0.1",
                          edgecolor=text_secondary, facecolor=bg_color, alpha=0.5, linewidth=1.5)
ax2.add_patch(use1_box)
ax2.text(1.9, use_y-0.7, 'Real-Time Ops', ha='center', fontsize=8, 
         color=text_primary, fontweight='bold')
ax2.text(1.9, use_y-1.1, '• Fleet monitoring', ha='left', fontsize=7, color=text_primary)
ax2.text(1.9, use_y-1.3, '• Alert generation', ha='left', fontsize=7, color=text_primary)

use2_box = FancyBboxPatch((3.6, use_y-1.5), 2.8, 1, boxstyle="round,pad=0.1",
                          edgecolor=text_secondary, facecolor=bg_color, alpha=0.5, linewidth=1.5)
ax2.add_patch(use2_box)
ax2.text(5, use_y-0.7, 'Transactions', ha='center', fontsize=8, 
         color=text_primary, fontweight='bold')
ax2.text(5, use_y-1.1, '• Service records', ha='left', fontsize=7, color=text_primary)
ax2.text(5, use_y-1.3, '• State management', ha='left', fontsize=7, color=text_primary)

use3_box = FancyBboxPatch((6.7, use_y-1.5), 2.8, 1, boxstyle="round,pad=0.1",
                          edgecolor=text_secondary, facecolor=bg_color, alpha=0.5, linewidth=1.5)
ax2.add_patch(use3_box)
ax2.text(8.1, use_y-0.7, 'Analytics & ML', ha='center', fontsize=8, 
         color=text_primary, fontweight='bold')
ax2.text(8.1, use_y-1.1, '• Predictive models', ha='left', fontsize=7, color=text_primary)
ax2.text(8.1, use_y-1.3, '• Historical trends', ha='left', fontsize=7, color=text_primary)

# Legend
legend_y = 1.5
ax2.text(5, legend_y, 'DATA FLOW LEGEND', ha='center', fontsize=9, 
         color=text_secondary, fontweight='bold')
ax2.plot([2, 2.8], [legend_y-0.5, legend_y-0.5], linewidth=2.5, color=text_primary)
ax2.text(3.2, legend_y-0.5, 'Real-time Stream', va='center', fontsize=7, color=text_primary)
ax2.plot([5.5, 6.3], [legend_y-0.5, legend_y-0.5], linewidth=2.5, color=coral, linestyle='dashed')
ax2.text(6.7, legend_y-0.5, 'CDC Export', va='center', fontsize=7, color=text_primary)

plt.tight_layout()
print("✓ Operational store integration visualizations created")
print()
print("RECOMMENDATION:")
print("-" * 100)
print("• Use CDC Streaming (Pattern 2) for production Fleet Guardian deployment")
print("• Provides real-time operational analytics while maintaining system independence")
print("• Cassandra 4.1+ native CDC preferred over IBM HCD custom integration")
print("• Enables complete data lineage: Operations → Analytics → ML")

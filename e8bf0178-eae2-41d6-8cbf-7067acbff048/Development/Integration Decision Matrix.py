import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

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
print("INTEGRATION DECISION MATRIX")
print("Choosing Between OpenSearch, watsonx.data, or Both")
print("=" * 100)

# Decision matrix based on use case requirements
decision_matrix = {
    'Use Case Requirement': [
        'Real-time alerting (<1 min)',
        'Historical trend analysis',
        'Full-text search needed',
        'Complex multi-table joins',
        'Anomaly detection on streams',
        'Predictive ML models',
        'Operational dashboards',
        'Ad-hoc analytical queries',
        'Log search & discovery',
        'Cost-effective long-term storage',
        'Sub-second query response',
        'Petabyte-scale analytics',
        'Event correlation',
        'Schema evolution required',
        'Time-series optimization'
    ],
    'OpenSearch': [
        '✓✓ Excellent',
        '○ Limited (recent only)',
        '✓✓ Excellent',
        '○ Not supported',
        '✓✓ Built-in RCF',
        '○ Limited ML',
        '✓✓ Optimized',
        '○ Basic only',
        '✓✓ Core strength',
        '○ Expensive at scale',
        '✓✓ Milliseconds',
        '○ Not suitable',
        '✓ Good',
        '○ Dynamic only',
        '✓✓ Native support'
    ],
    'watsonx.data': [
        '○ Not suitable',
        '✓✓ Excellent',
        '○ Not available',
        '✓✓ Full SQL support',
        '○ Batch only',
        '✓✓ Full MLlib',
        '○ Too slow',
        '✓✓ Optimized',
        '○ Not available',
        '✓✓ Very cost-effective',
        '○ Seconds range',
        '✓✓ Designed for it',
        '○ Requires joins',
        '✓✓ Iceberg support',
        '✓ Via partitioning'
    ],
    'Recommended': [
        'OpenSearch',
        'watsonx.data',
        'OpenSearch',
        'watsonx.data',
        'OpenSearch',
        'watsonx.data',
        'OpenSearch',
        'watsonx.data',
        'OpenSearch',
        'watsonx.data',
        'OpenSearch',
        'watsonx.data',
        'OpenSearch',
        'watsonx.data',
        'Both (tiered)'
    ]
}

decision_df = pd.DataFrame(decision_matrix)
print("\n")
print(decision_df.to_string(index=False))

# Architecture patterns and when to use each
print("\n\n" + "=" * 100)
print("ARCHITECTURE PATTERNS & DECISION GUIDE")
print("=" * 100)

arch_patterns = {
    'Architecture': [
        'OpenSearch Only',
        'watsonx.data Only',
        'Both (Lambda)',
        'Both (Tiered Storage)',
        'Both (Query Federation)'
    ],
    'When to Use': [
        'Real-time ops, no historical analytics needed',
        'Pure analytics workload, no real-time requirements',
        'Need both real-time alerts AND deep analytics',
        'Cost optimization: hot/cold data separation',
        'Unified query interface across both systems'
    ],
    'Data Volume': [
        '< 10 TB hot data',
        '> 100 TB historical',
        'Any (optimal for all)',
        '10TB hot + 100TB+ cold',
        'Any'
    ],
    'Latency SLA': [
        '< 1 second',
        '> 30 seconds OK',
        'Mixed: <1s ops, >30s analytics',
        'Mixed requirements',
        'Mixed requirements'
    ],
    'Complexity': [
        'Low',
        'Low',
        'High',
        'Medium',
        'Very High'
    ],
    'Cost (Relative)': [
        'Medium',
        'Low',
        'High (dual systems)',
        'Medium (optimized)',
        'High'
    ]
}

arch_df = pd.DataFrame(arch_patterns)
print("\n")
print(arch_df.to_string(index=False))

# Fleet Guardian specific recommendations
print("\n\n" + "=" * 100)
print("FLEET GUARDIAN SPECIFIC RECOMMENDATIONS")
print("=" * 100)

fleet_scenarios = {
    'Fleet Scenario': [
        'Fleet Monitoring Dashboard',
        'Incident Response',
        'Predictive Maintenance',
        'Compliance Reporting',
        'Root Cause Analysis',
        'Capacity Planning',
        'Anomaly Investigation',
        'Performance Benchmarking'
    ],
    'Primary System': [
        'OpenSearch',
        'OpenSearch',
        'watsonx.data',
        'watsonx.data',
        'Both',
        'watsonx.data',
        'Both',
        'watsonx.data'
    ],
    'Secondary System': [
        'N/A',
        'watsonx.data (context)',
        'N/A',
        'N/A',
        'N/A',
        'N/A',
        'N/A',
        'OpenSearch (recent)'
    ],
    'Reasoning': [
        'Need real-time health status',
        'Fast detection + historical context',
        'Requires ML on historical patterns',
        'Long-term data aggregation required',
        'Real-time detection + historical drill-down',
        'Analyze trends over months/years',
        'Detect in real-time, analyze in batch',
        'Compare current vs historical performance'
    ]
}

fleet_df = pd.DataFrame(fleet_scenarios)
print("\n")
print(fleet_df.to_string(index=False))

# Create visual decision tree
decision_fig = plt.figure(figsize=(16, 10), facecolor=bg_color)
ax = plt.subplot(111)
ax.set_xlim(0, 10)
ax.set_ylim(0, 10)
ax.axis('off')
ax.set_facecolor(bg_color)

# Title
ax.text(5, 9.5, 'Fleet Guardian Integration Decision Tree', 
        fontsize=18, ha='center', fontweight='bold', color=text_primary)

# Root question
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

root = FancyBboxPatch((3.5, 8.2), 3, 0.8, boxstyle="round,pad=0.15", 
                      edgecolor=highlight, facecolor=bg_color, linewidth=3)
ax.add_patch(root)
ax.text(5, 8.7, 'Do you need', fontsize=11, ha='center', color=text_primary, fontweight='bold')
ax.text(5, 8.4, 'real-time alerting?', fontsize=11, ha='center', color=text_primary, fontweight='bold')

# Left branch - No
no_box = FancyBboxPatch((0.5, 6.5), 2.5, 0.8, boxstyle="round,pad=0.1", 
                        edgecolor=coral, facecolor=bg_color, linewidth=2.5)
ax.add_patch(no_box)
ax.text(1.75, 7, 'NO', fontsize=10, ha='center', color=coral, fontweight='bold')
ax.text(1.75, 6.7, '→ watsonx.data only', fontsize=9, ha='center', color=text_primary)

arrow_no = FancyArrowPatch((4.2, 8.2), (2.5, 7.3), arrowstyle='->', 
                          mutation_scale=20, linewidth=2.5, color=coral)
ax.add_patch(arrow_no)

# Right branch - Yes -> Further split
yes_box = FancyBboxPatch((5.5, 6.5), 2.5, 0.8, boxstyle="round,pad=0.1", 
                         edgecolor=light_blue, facecolor=bg_color, linewidth=2.5)
ax.add_patch(yes_box)
ax.text(6.75, 7, 'YES', fontsize=10, ha='center', color=light_blue, fontweight='bold')
ax.text(6.75, 6.7, 'Need historical analytics?', fontsize=9, ha='center', color=text_primary)

arrow_yes = FancyArrowPatch((5.8, 8.2), (7.5, 7.3), arrowstyle='->', 
                           mutation_scale=20, linewidth=2.5, color=light_blue)
ax.add_patch(arrow_yes)

# Left sub-branch (Yes + No)
opensearch_only = FancyBboxPatch((4.5, 4.8), 2.2, 0.8, boxstyle="round,pad=0.1", 
                                edgecolor=green, facecolor=bg_color, linewidth=2.5)
ax.add_patch(opensearch_only)
ax.text(5.6, 5.35, 'NO', fontsize=9, ha='center', color=green, fontweight='bold')
ax.text(5.6, 5.05, '✓ OpenSearch only', fontsize=10, ha='center', color=text_primary, fontweight='bold')

arrow_os = FancyArrowPatch((6, 6.5), (5.6, 5.6), arrowstyle='->', 
                          mutation_scale=20, linewidth=2.5, color=green)
ax.add_patch(arrow_os)

# Right sub-branch (Yes + Yes)
both_systems = FancyBboxPatch((7.3, 4.8), 2.2, 0.8, boxstyle="round,pad=0.1", 
                             edgecolor=lavender, facecolor=bg_color, linewidth=2.5)
ax.add_patch(both_systems)
ax.text(8.4, 5.35, 'YES', fontsize=9, ha='center', color=lavender, fontweight='bold')
ax.text(8.4, 5.05, '✓ Both (Lambda)', fontsize=10, ha='center', color=text_primary, fontweight='bold')

arrow_both = FancyArrowPatch((7.5, 6.5), (8.4, 5.6), arrowstyle='->', 
                            mutation_scale=20, linewidth=2.5, color=lavender)
ax.add_patch(arrow_both)

# Additional consideration boxes
storage_box = FancyBboxPatch((0.3, 3.2), 3, 1.2, boxstyle="round,pad=0.1", 
                            edgecolor=coral, facecolor=bg_color, linewidth=2)
ax.add_patch(storage_box)
ax.text(1.8, 4.1, 'watsonx.data Only', fontsize=11, ha='center', color=text_primary, fontweight='bold')
ax.text(1.8, 3.85, 'Best for:', fontsize=9, ha='center', color=text_secondary)
ax.text(1.8, 3.62, '• Pure analytics workloads', fontsize=8, ha='center', color=coral)
ax.text(1.8, 3.42, '• Historical trend analysis', fontsize=8, ha='center', color=coral)
ax.text(1.8, 3.22, '• Cost-optimized storage', fontsize=8, ha='center', color=coral)

ops_box = FancyBboxPatch((3.8, 3.2), 3, 1.2, boxstyle="round,pad=0.1", 
                        edgecolor=green, facecolor=bg_color, linewidth=2)
ax.add_patch(ops_box)
ax.text(5.3, 4.1, 'OpenSearch Only', fontsize=11, ha='center', color=text_primary, fontweight='bold')
ax.text(5.3, 3.85, 'Best for:', fontsize=9, ha='center', color=text_secondary)
ax.text(5.3, 3.62, '• Real-time operations', fontsize=8, ha='center', color=green)
ax.text(5.3, 3.42, '• Fleet health monitoring', fontsize=8, ha='center', color=green)
ax.text(5.3, 3.22, '• Log search & discovery', fontsize=8, ha='center', color=green)

lambda_box = FancyBboxPatch((7.3, 3.2), 2.5, 1.2, boxstyle="round,pad=0.1", 
                           edgecolor=lavender, facecolor=bg_color, linewidth=2)
ax.add_patch(lambda_box)
ax.text(8.55, 4.1, 'Both Systems', fontsize=11, ha='center', color=text_primary, fontweight='bold')
ax.text(8.55, 3.85, 'Best for:', fontsize=9, ha='center', color=text_secondary)
ax.text(8.55, 3.62, '• Complete observability', fontsize=8, ha='center', color=lavender)
ax.text(8.55, 3.42, '• Real-time + analytics', fontsize=8, ha='center', color=lavender)
ax.text(8.55, 3.22, '• Production fleets', fontsize=8, ha='center', color=lavender)

# Bottom summary
summary_box = FancyBboxPatch((1.5, 0.5), 7, 2, boxstyle="round,pad=0.15", 
                            edgecolor=highlight, facecolor=bg_color, linewidth=2)
ax.add_patch(summary_box)
ax.text(5, 2.2, 'Fleet Guardian Recommendation: Dual-Path Architecture', 
        fontsize=13, ha='center', color=highlight, fontweight='bold')
ax.text(5, 1.85, 'Use BOTH systems for comprehensive fleet management:', 
        fontsize=10, ha='center', color=text_primary)
ax.text(5, 1.58, '• OpenSearch: Real-time alerts, anomaly detection, operational dashboards (<1 sec latency)', 
        fontsize=9, ha='center', color=light_blue)
ax.text(5, 1.35, '• watsonx.data: Predictive maintenance, historical analysis, compliance reports (batch)', 
        fontsize=9, ha='center', color=coral)
ax.text(5, 1.12, '• Pulsar: Unified data ingestion feeding both paths simultaneously', 
        fontsize=9, ha='center', color=orange)
ax.text(5, 0.89, '✓ Complete visibility: Real-time operations + deep analytics on same fleet data', 
        fontsize=9, ha='center', color=green, style='italic')

plt.tight_layout()

print("\n\n" + "=" * 100)
print("KEY DECISION FACTORS:")
print("=" * 100)
print("1. LATENCY REQUIREMENTS:")
print("   - Sub-second → OpenSearch")
print("   - Minutes acceptable → watsonx.data")
print("   - Both needed → Dual-path\n")
print("2. QUERY PATTERNS:")
print("   - Search & filter → OpenSearch")
print("   - Complex SQL & joins → watsonx.data")
print("   - Both → Dual-path\n")
print("3. DATA VOLUME & COST:")
print("   - < 10 TB hot data → OpenSearch")
print("   - > 100 TB cold data → watsonx.data")
print("   - Mixed → Tiered storage\n")
print("4. PROCESSING NEEDS:")
print("   - Streaming anomaly detection → OpenSearch RCF")
print("   - ML model training → watsonx.data Spark")
print("   - Both → Dual-path\n")
print("=" * 100)
print("✓ Integration decision matrix completed")
print("=" * 100)
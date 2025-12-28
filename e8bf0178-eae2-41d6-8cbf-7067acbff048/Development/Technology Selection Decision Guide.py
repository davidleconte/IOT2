import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
import numpy as np

# Comprehensive technology selection guide
print("=" * 100)
print("TECHNOLOGY SELECTION DECISION GUIDE")
print("When to Use Each System - Fleet Guardian Architecture")
print("=" * 100)
print()

# Decision criteria matrix
decision_criteria = {
    'Requirement': [
        'Sub-second query response',
        'Full-text search capability',
        'Real-time anomaly detection',
        'Geo-spatial visualization',
        'Historical trend analysis (>1 year)',
        'Complex multi-table joins',
        'Machine learning model training',
        'Cost-effective long-term storage',
        'High concurrent read workload',
        'High write throughput (>500K/sec)',
        'ACID transactions',
        'Schema evolution & versioning',
        'Real-time dashboards',
        'Regulatory compliance & audit',
        'Predictive analytics'
    ],
    'OpenSearch': [
        '✓✓✓ Optimal',
        '✓✓✓ Native capability',
        '✓✓✓ Built-in RCF',
        '✓✓✓ Tile maps, geo queries',
        '○ Limited (cost)',
        '○ Very limited',
        '○ Basic ML only',
        '○ Expensive at scale',
        '✓✓ Good',
        '✓ Moderate',
        '✗ Not supported',
        '✓ Dynamic mapping',
        '✓✓✓ Excellent',
        '✓✓ Good (searchable)',
        '○ Limited'
    ],
    'watsonx.data': [
        '✓ Good for cold data',
        '✗ Requires integration',
        '○ Custom implementation',
        '○ Via tools (Superset)',
        '✓✓✓ Optimal',
        '✓✓✓ Full SQL',
        '✓✓✓ Spark ML',
        '✓✓✓ Very cost-effective',
        '✓✓ Good (Presto)',
        '✓✓✓ Excellent',
        '✓✓ Iceberg ACID',
        '✓✓✓ Native support',
        '○ Higher latency',
        '✓✓✓ Excellent (Iceberg)',
        '✓✓✓ Full capabilities'
    ],
    'Operational Store': [
        '✓✓✓ Ultra-fast',
        '✗ Not designed for',
        '✗ Not designed for',
        '✗ Not designed for',
        '○ Not primary use',
        '○ Limited',
        '✗ Not designed for',
        '○ Moderate',
        '✓✓✓ Excellent',
        '✓✓✓ Excellent',
        '✓✓✓ Native',
        '✓ Supported',
        '○ Via APIs',
        '✓✓ Good',
        '✗ Not designed for'
    ]
}

decision_df = pd.DataFrame(decision_criteria)
print(decision_df.to_string(index=False))
print()
print("Legend: ✓✓✓=Excellent, ✓✓=Good, ✓=Adequate, ○=Limited, ✗=Not suitable")
print()

# Use case recommendations
print("\nUSE CASE RECOMMENDATIONS:")
print("-" * 100)
print()

use_cases = {
    'Use Case': [
        'Fleet Health Monitoring',
        'Real-time Alert Generation',
        'Operational Dashboards',
        'Log Analysis & Troubleshooting',
        'Anomaly Detection (Real-time)',
        'Predictive Maintenance Models',
        'Historical Performance Analysis',
        'Cost Optimization Analysis',
        'Regulatory Reporting',
        'Root Cause Analysis',
        'Fleet-wide Trend Analysis',
        'Service Record Management',
        'Vehicle State Tracking',
        'Multi-year Trend Analysis',
        'Ad-hoc Data Exploration'
    ],
    'Primary System': [
        'OpenSearch',
        'OpenSearch',
        'OpenSearch',
        'OpenSearch',
        'OpenSearch',
        'watsonx.data',
        'watsonx.data',
        'watsonx.data',
        'watsonx.data',
        'Both (OS→find, wx→analyze)',
        'watsonx.data',
        'Operational Store',
        'Operational Store',
        'watsonx.data',
        'watsonx.data'
    ],
    'Secondary System': [
        'Operational Store',
        'Operational Store',
        'Operational Store',
        'watsonx.data',
        'watsonx.data',
        'Operational Store',
        'OpenSearch',
        'Operational Store',
        'Operational Store',
        '-',
        'OpenSearch',
        'watsonx.data',
        'watsonx.data',
        'OpenSearch',
        'OpenSearch'
    ],
    'Time Horizon': [
        'Last 24-48 hours',
        'Real-time (seconds)',
        'Last 7 days',
        'Last 30 days',
        'Real-time (seconds)',
        '1-3 years historical',
        '3-12 months',
        '1-5 years',
        'Multi-year',
        'Any (contextual)',
        '1+ years',
        'Current state',
        'Current state',
        '1-5 years',
        'Any'
    ]
}

use_case_df = pd.DataFrame(use_cases)
print(use_case_df.to_string(index=False))
print()

# Create comprehensive decision flowchart visualization
guide_fig = plt.figure(figsize=(18, 12))
guide_fig.patch.set_facecolor(bg_color)
gs = guide_fig.add_gridspec(2, 2, hspace=0.3, wspace=0.3)

# Top left: Time-based decision
ax1 = guide_fig.add_subplot(gs[0, 0])
ax1.set_xlim(0, 10)
ax1.set_ylim(0, 10)
ax1.axis('off')
ax1.set_facecolor(bg_color)
ax1.set_title('Decision Factor 1: Time Horizon', 
              fontsize=13, color=text_primary, fontweight='bold', pad=15)

time_boxes = [
    (1, 8, 2.5, 1, 'Real-time\n(< 1 min)', light_blue, 'OpenSearch'),
    (4, 8, 2.5, 1, 'Recent\n(1-7 days)', orange, 'OpenSearch'),
    (7, 8, 2.5, 1, 'Medium\n(1-3 months)', lavender, 'Both'),
    (1, 5.5, 2.5, 1, 'Historical\n(3-12 months)', green, 'watsonx.data'),
    (4, 5.5, 2.5, 1, 'Long-term\n(1+ years)', coral, 'watsonx.data'),
]

for x, y, w, h, label, color, system in time_boxes:
    box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.15",
                         edgecolor=color, facecolor=color, alpha=0.3, linewidth=2.5)
    ax1.add_patch(box)
    lines = label.split('\n')
    ax1.text(x + w/2, y + h/2 + 0.2, lines[0], ha='center', va='center',
             fontsize=10, color=text_primary, fontweight='bold')
    ax1.text(x + w/2, y + h/2 - 0.2, lines[1], ha='center', va='center',
             fontsize=8, color=text_secondary)
    ax1.text(x + w/2, y - 0.5, f'→ {system}', ha='center', va='top',
             fontsize=8, color=color, fontweight='bold')

ax1.text(5, 3.5, 'Choose based on query time range', ha='center',
         fontsize=9, color=text_secondary, style='italic')

# Top right: Query type decision
ax2 = guide_fig.add_subplot(gs[0, 1])
ax2.set_xlim(0, 10)
ax2.set_ylim(0, 10)
ax2.axis('off')
ax2.set_facecolor(bg_color)
ax2.set_title('Decision Factor 2: Query Type', 
              fontsize=13, color=text_primary, fontweight='bold', pad=15)

query_boxes = [
    (0.5, 7.5, 4, 1.2, 'Full-text Search', light_blue, 'OpenSearch'),
    (5, 7.5, 4, 1.2, 'Complex SQL Joins', green, 'watsonx.data'),
    (0.5, 6, 4, 1.2, 'Geo-spatial Queries', light_blue, 'OpenSearch'),
    (5, 6, 4, 1.2, 'Aggregations (TBs)', green, 'watsonx.data'),
    (0.5, 4.5, 4, 1.2, 'Real-time Anomaly Det.', light_blue, 'OpenSearch'),
    (5, 4.5, 4, 1.2, 'ML Model Training', green, 'watsonx.data'),
]

for x, y, w, h, label, color, system in query_boxes:
    box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.1",
                         edgecolor=color, facecolor=color, alpha=0.3, linewidth=2)
    ax2.add_patch(box)
    ax2.text(x + w/2, y + h/2 + 0.25, label, ha='center', va='center',
             fontsize=9, color=text_primary, fontweight='bold')
    ax2.text(x + w/2, y + h/2 - 0.25, f'→ {system}', ha='center', va='center',
             fontsize=8, color=color, fontweight='bold')

ax2.text(5, 3, 'Choose based on query pattern and complexity', ha='center',
         fontsize=9, color=text_secondary, style='italic')

# Bottom left: Cost vs Performance tradeoff
ax3 = guide_fig.add_subplot(gs[1, 0])
ax3.set_facecolor(bg_color)

retention_periods = ['1 day', '1 week', '1 month', '3 months', '1 year', '3 years']
cost_opensearch = [1, 3, 8, 20, 50, 120]
cost_watsonx = [1, 1.5, 2, 3, 5, 8]
performance_opensearch = [10, 10, 9, 7, 5, 3]
performance_watsonx = [6, 6, 7, 8, 9, 10]

x_axis = np.arange(len(retention_periods))
width = 0.35

bars1 = ax3.bar(x_axis - width/2, cost_opensearch, width, 
                label='OpenSearch Cost', color=light_blue, alpha=0.7)
bars2 = ax3.bar(x_axis + width/2, cost_watsonx, width,
                label='watsonx.data Cost', color=green, alpha=0.7)

ax3.set_ylabel('Relative Cost (Storage + Compute)', fontsize=10, color=text_primary)
ax3.set_xlabel('Data Retention Period', fontsize=10, color=text_primary)
ax3.set_title('Cost vs Retention Period', fontsize=13, color=text_primary, fontweight='bold', pad=15)
ax3.set_xticks(x_axis)
ax3.set_xticklabels(retention_periods, color=text_primary, rotation=0)
ax3.tick_params(colors=text_primary)
ax3.legend(facecolor=bg_color, edgecolor=text_secondary, labelcolor=text_primary, loc='upper left')
ax3.spines['bottom'].set_color(text_secondary)
ax3.spines['top'].set_color(text_secondary)
ax3.spines['left'].set_color(text_secondary)
ax3.spines['right'].set_color(text_secondary)
ax3.grid(True, alpha=0.2, color=text_secondary, axis='y')
ax3.set_yscale('log')

# Crossover annotation
ax3.annotate('Cost crossover\npoint', xy=(1.5, 5), xytext=(3, 20),
            arrowprops=dict(arrowstyle='->', color=highlight, lw=2),
            fontsize=9, color=highlight, fontweight='bold')

# Bottom right: Integration pattern recommendation
ax4 = guide_fig.add_subplot(gs[1, 1])
ax4.set_xlim(0, 10)
ax4.set_ylim(0, 10)
ax4.axis('off')
ax4.set_facecolor(bg_color)
ax4.set_title('Recommended Integration Pattern', 
              fontsize=13, color=text_primary, fontweight='bold', pad=15)

# Tri-system integration
systems_y = 7
opensearch_rect = FancyBboxPatch((0.5, systems_y), 2.5, 1.5, boxstyle="round,pad=0.1",
                                 edgecolor=light_blue, facecolor=light_blue, alpha=0.3, linewidth=2.5)
ax4.add_patch(opensearch_rect)
ax4.text(1.75, systems_y + 1.2, 'OpenSearch', ha='center', fontsize=10, 
         color=text_primary, fontweight='bold')
ax4.text(1.75, systems_y + 0.8, 'Real-time', ha='center', fontsize=8, color=text_primary)
ax4.text(1.75, systems_y + 0.5, 'Ops & Search', ha='center', fontsize=8, color=text_primary)

ops_rect = FancyBboxPatch((3.75, systems_y), 2.5, 1.5, boxstyle="round,pad=0.1",
                          edgecolor=orange, facecolor=orange, alpha=0.3, linewidth=2.5)
ax4.add_patch(ops_rect)
ax4.text(5, systems_y + 1.2, 'Ops Store', ha='center', fontsize=10, 
         color=text_primary, fontweight='bold')
ax4.text(5, systems_y + 0.8, 'HCD/Cassandra', ha='center', fontsize=8, color=text_primary)
ax4.text(5, systems_y + 0.5, 'Transactions', ha='center', fontsize=8, color=text_primary)

watsonx_rect = FancyBboxPatch((7, systems_y), 2.5, 1.5, boxstyle="round,pad=0.1",
                              edgecolor=green, facecolor=green, alpha=0.3, linewidth=2.5)
ax4.add_patch(watsonx_rect)
ax4.text(8.25, systems_y + 1.2, 'watsonx.data', ha='center', fontsize=10, 
         color=text_primary, fontweight='bold')
ax4.text(8.25, systems_y + 0.8, 'Analytics', ha='center', fontsize=8, color=text_primary)
ax4.text(8.25, systems_y + 0.5, '& ML', ha='center', fontsize=8, color=text_primary)

# Integration benefits
benefits_y = 4.5
ax4.text(5, benefits_y + 0.8, 'COMBINED BENEFITS', ha='center', fontsize=11, 
         color=highlight, fontweight='bold')

benefits = [
    '✓ Real-time monitoring + Historical analysis',
    '✓ Transactional integrity + Analytical insights',
    '✓ Cost-effective at scale',
    '✓ Each system optimized for its workload'
]

for idx, benefit in enumerate(benefits):
    ax4.text(5, benefits_y - (idx * 0.5), benefit, ha='center', fontsize=9,
             color=text_primary)

# Key recommendation
ax4.text(5, 1.5, 'Deploy all three systems for comprehensive fleet management', 
         ha='center', fontsize=10, color=green, fontweight='bold',
         bbox=dict(boxstyle='round,pad=0.5', facecolor=green, alpha=0.2, edgecolor=green, linewidth=2))

plt.tight_layout()

print("\nKEY DECISION RULES:")
print("-" * 100)
print("1. Time Horizon Rule:")
print("   • Last 48 hours → OpenSearch (optimal)")
print("   • Last 1-30 days → OpenSearch (cost increases)")
print("   • 1-3 months → Both systems (transition zone)")
print("   • 3+ months → watsonx.data (cost-effective)")
print()
print("2. Query Type Rule:")
print("   • Full-text search, geo queries, real-time AD → OpenSearch")
print("   • Complex SQL, joins, aggregations on TBs → watsonx.data")
print("   • Transactional queries, state lookups → Operational Store")
print()
print("3. Cost Optimization Rule:")
print("   • Storage costs cross over at ~1 week retention")
print("   • Use OpenSearch for hot data (fast access required)")
print("   • Use watsonx.data for warm/cold data (cost > speed)")
print()
print("4. Integration Rule:")
print("   • Deploy all three systems for comprehensive coverage")
print("   • Use Pulsar for real-time data distribution")
print("   • Use CDC for operational store → watsonx.data sync")
print("   • Query federation for cross-system analytics when needed")

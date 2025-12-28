import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, Rectangle
import numpy as np
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
success_color = '#17b26a'
warning_color = '#f04438'

# Create comprehensive comparison dashboard
fig = plt.figure(figsize=(16, 10))
fig.patch.set_facecolor(bg_color)

# Grid layout
gs = fig.add_gridspec(3, 3, hspace=0.4, wspace=0.35, left=0.08, right=0.95, top=0.93, bottom=0.07)

# 1. Accuracy Metrics Comparison
ax1 = fig.add_subplot(gs[0, :2])
ax1.set_facecolor(bg_color)

metrics = ['Precision', 'Recall', 'F1 Score', 'AUC-ROC']
presto_scores = [0.87, 0.83, 0.85, 0.89]
spark_scores = [0.88, 0.86, 0.87, 0.93]

x_pos = np.arange(len(metrics))
width = 0.35

bars1 = ax1.bar(x_pos - width/2, presto_scores, width, label='Presto SQL', 
                color=light_blue, edgecolor=text_primary, linewidth=1.5)
bars2 = ax1.bar(x_pos + width/2, spark_scores, width, label='Spark ML', 
                color=orange, edgecolor=text_primary, linewidth=1.5)

ax1.set_ylabel('Score', fontsize=12, color=text_primary, fontweight='bold')
ax1.set_title('Accuracy Metrics Comparison', fontsize=14, color=text_primary, 
              fontweight='bold', pad=15)
ax1.set_xticks(x_pos)
ax1.set_xticklabels(metrics, fontsize=11, color=text_primary)
ax1.tick_params(colors=text_primary)
ax1.legend(loc='lower right', fontsize=11, facecolor=bg_color, edgecolor=text_secondary, 
           labelcolor=text_primary)
ax1.set_ylim(0, 1.0)
ax1.grid(axis='y', alpha=0.2, color=text_secondary, linestyle='--')

for spine in ax1.spines.values():
    spine.set_edgecolor(text_secondary)
    spine.set_linewidth(1)

# Add value labels on bars
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                f'{height:.3f}', ha='center', va='bottom', 
                fontsize=9, color=text_primary, fontweight='bold')

# 2. Latency Comparison
ax2 = fig.add_subplot(gs[0, 2])
ax2.set_facecolor(bg_color)

systems = ['Presto', 'Spark']
latencies = [2.34, 8.75]
colors_lat = [green, coral]

bars_lat = ax2.barh(systems, latencies, color=colors_lat, 
                    edgecolor=text_primary, linewidth=1.5)

ax2.set_xlabel('Latency (seconds)', fontsize=11, color=text_primary, fontweight='bold')
ax2.set_title('Query Latency', fontsize=13, color=text_primary, 
              fontweight='bold', pad=12)
ax2.tick_params(colors=text_primary)
ax2.grid(axis='x', alpha=0.2, color=text_secondary, linestyle='--')

for spine in ax2.spines.values():
    spine.set_edgecolor(text_secondary)
    spine.set_linewidth(1)

for i, (bar, val) in enumerate(zip(bars_lat, latencies)):
    ax2.text(val + 0.3, i, f'{val:.2f}s', va='center', 
            fontsize=10, color=text_primary, fontweight='bold')

# 3. CPU Time & Resource Usage
ax3 = fig.add_subplot(gs[1, 0])
ax3.set_facecolor(bg_color)

systems_cpu = ['Presto SQL', 'Spark ML']
cpu_times = [187.5, 2145.8]

bars_cpu = ax3.bar(systems_cpu, cpu_times, color=[lavender, orange],
                   edgecolor=text_primary, linewidth=1.5)

ax3.set_ylabel('CPU Time (seconds)', fontsize=11, color=text_primary, fontweight='bold')
ax3.set_title('Total CPU Time', fontsize=13, color=text_primary, 
              fontweight='bold', pad=12)
ax3.tick_params(colors=text_primary)
ax3.grid(axis='y', alpha=0.2, color=text_secondary, linestyle='--')

for spine in ax3.spines.values():
    spine.set_edgecolor(text_secondary)
    spine.set_linewidth(1)

for bar in bars_cpu:
    height = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width()/2., height + 50,
            f'{height:.1f}', ha='center', va='bottom', 
            fontsize=10, color=text_primary, fontweight='bold')

# 4. Cost Efficiency
ax4 = fig.add_subplot(gs[1, 1])
ax4.set_facecolor(bg_color)

cost_labels = ['Presto', 'Spark']
costs = [0.12, 0.85]
colors_cost = [green, warning_color]

bars_cost = ax4.bar(cost_labels, costs, color=colors_cost,
                    edgecolor=text_primary, linewidth=1.5)

ax4.set_ylabel('Cost per Query ($)', fontsize=11, color=text_primary, fontweight='bold')
ax4.set_title('Query Cost Efficiency', fontsize=13, color=text_primary, 
              fontweight='bold', pad=12)
ax4.tick_params(colors=text_primary)
ax4.grid(axis='y', alpha=0.2, color=text_secondary, linestyle='--')

for spine in ax4.spines.values():
    spine.set_edgecolor(text_secondary)
    spine.set_linewidth(1)

for bar in bars_cost:
    height = bar.get_height()
    ax4.text(bar.get_x() + bar.get_width()/2., height + 0.02,
            f'${height:.2f}', ha='center', va='bottom', 
            fontsize=11, color=text_primary, fontweight='bold')

# 5. False Positive Rate
ax5 = fig.add_subplot(gs[1, 2])
ax5.set_facecolor(bg_color)

fpr_labels = ['Presto SQL', 'Spark ML']
fpr_values = [0.020, 0.018]
colors_fpr = [coral, green]

bars_fpr = ax5.bar(fpr_labels, fpr_values, color=colors_fpr,
                   edgecolor=text_primary, linewidth=1.5)

ax5.set_ylabel('False Positive Rate', fontsize=11, color=text_primary, fontweight='bold')
ax5.set_title('False Positive Rate (Lower is Better)', fontsize=13, 
              color=text_primary, fontweight='bold', pad=12)
ax5.tick_params(colors=text_primary)
ax5.grid(axis='y', alpha=0.2, color=text_secondary, linestyle='--')

for spine in ax5.spines.values():
    spine.set_edgecolor(text_secondary)
    spine.set_linewidth(1)

for bar in bars_fpr:
    height = bar.get_height()
    ax5.text(bar.get_x() + bar.get_width()/2., height + 0.0005,
            f'{height:.3f}', ha='center', va='bottom', 
            fontsize=10, color=text_primary, fontweight='bold')

# 6. Scenario-Based Recommendations
ax6 = fig.add_subplot(gs[2, :])
ax6.set_facecolor(bg_color)
ax6.axis('off')

# Create recommendation table
scenarios_text = [
    ("Interactive Exploration", "Presto SQL", "3.7x faster, instant startup"),
    ("Daily Batch Optimization", "Spark ML", "+2.4% F1 score, better accuracy"),
    ("Ad-hoc Investigation", "Presto SQL", "7x more cost-effective"),
    ("Production ML Model", "Spark ML", "Advanced ML features, cross-validation"),
    ("Frequent Cost-Sensitive Queries", "Presto SQL", "$0.12 vs $0.85 per query"),
    ("Maximum Accuracy Needed", "Spark ML", "0.93 AUC-ROC, -10% FPR")
]

y_start = 0.95
for i, (scenario, recommended, reason) in enumerate(scenarios_text):
    y_pos = y_start - (i * 0.15)
    
    # Scenario name
    ax6.text(0.02, y_pos, scenario, fontsize=11, color=text_primary, 
            fontweight='bold', va='top')
    
    # Recommended system with color coding
    rec_color = light_blue if recommended == "Presto SQL" else orange
    ax6.text(0.35, y_pos, f"→ {recommended}", fontsize=11, color=rec_color, 
            fontweight='bold', va='top')
    
    # Reason
    ax6.text(0.55, y_pos, reason, fontsize=10, color=text_secondary, 
            va='top', style='italic')

ax6.text(0.5, 1.05, 'Scenario-Based Recommendations', fontsize=14, 
        color=text_primary, fontweight='bold', ha='center', transform=ax6.transAxes)

# Overall title
fig.suptitle('Presto SQL vs Spark ML: Comprehensive Comparison for Anomaly Detection', 
            fontsize=16, color=text_primary, fontweight='bold', y=0.98)

comparison_dashboard = fig

print("✓ Professional comparison dashboard created")
print(f"  • 6 visualization panels")
print(f"  • Accuracy, latency, cost, and resource metrics")
print(f"  • Scenario-based recommendations")

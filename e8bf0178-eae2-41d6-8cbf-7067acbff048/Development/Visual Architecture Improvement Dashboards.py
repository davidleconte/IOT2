import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
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
success_color = '#17b26a'
warning_color = '#f04438'

# Create comprehensive visualization dashboard
dashboard_fig = plt.figure(figsize=(20, 12), facecolor=bg_color)
gs = dashboard_fig.add_gridspec(3, 3, hspace=0.35, wspace=0.3, left=0.05, right=0.95, top=0.95, bottom=0.05)

# === 1. COST SAVINGS BY CATEGORY ===
ax1 = dashboard_fig.add_subplot(gs[0, 0])
ax1.set_facecolor(bg_color)

categories_cost = arch_df.groupby('category')['cost_impact_monthly_usd'].sum() * -12 / 1000  # in thousands, annual
categories_cost = categories_cost.sort_values(ascending=True)

y_pos_cost = np.arange(len(categories_cost))
bars_cost = ax1.barh(y_pos_cost, categories_cost.values, color=[light_blue, orange, green, coral, lavender, highlight])

ax1.set_yticks(y_pos_cost)
ax1.set_yticklabels([c.replace(' - ', '\n') for c in categories_cost.index], fontsize=9, color=text_primary)
ax1.set_xlabel('Annual Savings ($K)', fontsize=10, color=text_primary, weight='bold')
ax1.set_title('Cost Savings by Category', fontsize=12, color=text_primary, weight='bold', pad=15)
ax1.tick_params(axis='x', colors=text_secondary)
ax1.spines['bottom'].set_color(text_secondary)
ax1.spines['left'].set_color(text_secondary)
ax1.spines['top'].set_visible(False)
ax1.spines['right'].set_visible(False)

for i_bar, (bar_patch, val) in enumerate(zip(bars_cost, categories_cost.values)):
    ax1.text(val + 2, bar_patch.get_y() + bar_patch.get_height()/2, 
             f'${val:.0f}K', va='center', fontsize=9, color=text_primary, weight='bold')

# === 2. LATENCY REDUCTION METRICS ===
ax2 = dashboard_fig.add_subplot(gs[0, 1])
ax2.set_facecolor(bg_color)

latency_improvements = []
latency_labels = []

# CDC latency
cdc_row = arch_df[arch_df['id'] == 'ARCH-01'].iloc[0]
latency_improvements.append(cdc_row['latency_reduction_pct'])
latency_labels.append('CDC\nThroughput')

# Spark job
spark_row = arch_df[arch_df['id'] == 'ARCH-02'].iloc[0]
latency_improvements.append(spark_row['job_speedup_pct'])
latency_labels.append('Spark\nJob Speed')

# Presto query
presto_row = arch_df[arch_df['id'] == 'ARCH-03'].iloc[0]
latency_improvements.append(presto_row['query_speedup_pct'])
latency_labels.append('Presto\nQuery')

# End-to-end
e2e_row = arch_df[arch_df['id'] == 'ARCH-11'].iloc[0]
latency_improvements.append(e2e_row['latency_reduction_pct'])
latency_labels.append('End-to-End\nLatency')

x_pos_lat = np.arange(len(latency_improvements))
bars_lat = ax2.bar(x_pos_lat, latency_improvements, color=[coral, orange, light_blue, green], width=0.6)

ax2.set_xticks(x_pos_lat)
ax2.set_xticklabels(latency_labels, fontsize=9, color=text_primary)
ax2.set_ylabel('Reduction %', fontsize=10, color=text_primary, weight='bold')
ax2.set_title('Latency & Performance Improvements', fontsize=12, color=text_primary, weight='bold', pad=15)
ax2.tick_params(axis='y', colors=text_secondary)
ax2.spines['bottom'].set_color(text_secondary)
ax2.spines['left'].set_color(text_secondary)
ax2.spines['top'].set_visible(False)
ax2.spines['right'].set_visible(False)
ax2.set_ylim(0, 100)

for bar_patch in bars_lat:
    height = bar_patch.get_height()
    ax2.text(bar_patch.get_x() + bar_patch.get_width()/2, height + 2,
             f'{height:.0f}%', ha='center', va='bottom', fontsize=10, color=text_primary, weight='bold')

# === 3. IMPLEMENTATION COMPLEXITY DISTRIBUTION ===
ax3 = dashboard_fig.add_subplot(gs[0, 2])
ax3.set_facecolor(bg_color)

complexity_order = ['Very Low', 'Low', 'Medium', 'High']
complexity_counts_list = [len(arch_df[arch_df['complexity'] == c]) for c in complexity_order]
complexity_colors = [success_color, light_blue, highlight, warning_color]

bars_complex = ax3.bar(range(len(complexity_order)), complexity_counts_list, color=complexity_colors, width=0.6)

ax3.set_xticks(range(len(complexity_order)))
ax3.set_xticklabels(complexity_order, fontsize=9, color=text_primary)
ax3.set_ylabel('Number of Optimizations', fontsize=10, color=text_primary, weight='bold')
ax3.set_title('Implementation Complexity', fontsize=12, color=text_primary, weight='bold', pad=15)
ax3.tick_params(axis='y', colors=text_secondary)
ax3.spines['bottom'].set_color(text_secondary)
ax3.spines['left'].set_color(text_secondary)
ax3.spines['top'].set_visible(False)
ax3.spines['right'].set_visible(False)

for bar_patch in bars_complex:
    height = bar_patch.get_height()
    if height > 0:
        ax3.text(bar_patch.get_x() + bar_patch.get_width()/2, height + 0.1,
                 f'{int(height)}', ha='center', va='bottom', fontsize=10, color=text_primary, weight='bold')

# === 4. PHASED ROADMAP TIMELINE ===
ax4 = dashboard_fig.add_subplot(gs[1, :])
ax4.set_facecolor(bg_color)

phases_data = [
    {'name': 'Phase 1:\nQuick Wins', 'weeks': 4, 'count': len(phase_1_quick_wins), 'color': success_color},
    {'name': 'Phase 2:\nHigh-Value', 'weeks': 8, 'count': len(phase_2_high_value), 'color': light_blue},
    {'name': 'Phase 3:\nStrategic', 'weeks': 12, 'count': len(phase_3_strategic), 'color': highlight},
    {'name': 'Phase 4:\nContinuous', 'weeks': 4, 'count': len(phase_4_continuous), 'color': lavender},
]

current_week = 0
for phase in phases_data:
    rect = FancyBboxPatch((current_week, 0), phase['weeks'], 1, 
                          boxstyle="round,pad=0.05", 
                          facecolor=phase['color'], edgecolor=text_primary, linewidth=2)
    ax4.add_patch(rect)
    
    ax4.text(current_week + phase['weeks']/2, 0.5, 
             f"{phase['name']}\n{phase['weeks']} weeks\n{phase['count']} optimizations",
             ha='center', va='center', fontsize=10, color=bg_color, weight='bold')
    
    current_week += phase['weeks']

ax4.set_xlim(0, 28)
ax4.set_ylim(-0.2, 1.3)
ax4.set_xlabel('Timeline (Weeks)', fontsize=10, color=text_primary, weight='bold')
ax4.set_title('Implementation Roadmap - 6 Month Plan', fontsize=13, color=text_primary, weight='bold', pad=15)
ax4.tick_params(axis='x', colors=text_secondary)
ax4.spines['bottom'].set_color(text_secondary)
ax4.spines['left'].set_visible(False)
ax4.spines['top'].set_visible(False)
ax4.spines['right'].set_visible(False)
ax4.set_yticks([])

# === 5. BEFORE/AFTER COMPARISON ===
ax5 = dashboard_fig.add_subplot(gs[2, 0])
ax5.set_facecolor(bg_color)

comparison_metrics_names = ['CDC\nThroughput', 'Spark Job\nDuration', 'Storage\nCosts']
before_vals = [2000, 45, 11300]
after_vals = [15000, 8, 4400]

x_compare = np.arange(len(comparison_metrics_names))
width_compare = 0.35

bars_before = ax5.bar(x_compare - width_compare/2, before_vals, width_compare, 
                      label='Before', color=warning_color, alpha=0.8)
bars_after = ax5.bar(x_compare + width_compare/2, after_vals, width_compare,
                     label='After', color=success_color, alpha=0.8)

ax5.set_xticks(x_compare)
ax5.set_xticklabels(comparison_metrics_names, fontsize=9, color=text_primary)
ax5.set_ylabel('Value (normalized scale)', fontsize=10, color=text_primary, weight='bold')
ax5.set_title('Before vs After - Key Metrics', fontsize=12, color=text_primary, weight='bold', pad=15)
ax5.legend(fontsize=9, facecolor=bg_color, edgecolor=text_secondary, labelcolor=text_primary)
ax5.tick_params(axis='y', colors=text_secondary)
ax5.spines['bottom'].set_color(text_secondary)
ax5.spines['left'].set_color(text_secondary)
ax5.spines['top'].set_visible(False)
ax5.spines['right'].set_visible(False)
ax5.set_yscale('log')

# === 6. ROI PROJECTION ===
ax6 = dashboard_fig.add_subplot(gs[2, 1])
ax6.set_facecolor(bg_color)

months = np.arange(1, 13)
cumulative_savings = (total_cost_savings * months) / 1000  # in thousands

ax6.plot(months, cumulative_savings, color=success_color, linewidth=3, marker='o', markersize=6)
ax6.fill_between(months, 0, cumulative_savings, color=success_color, alpha=0.2)

ax6.set_xlabel('Months After Implementation', fontsize=10, color=text_primary, weight='bold')
ax6.set_ylabel('Cumulative Savings ($K)', fontsize=10, color=text_primary, weight='bold')
ax6.set_title('First-Year ROI Projection', fontsize=12, color=text_primary, weight='bold', pad=15)
ax6.tick_params(axis='both', colors=text_secondary)
ax6.spines['bottom'].set_color(text_secondary)
ax6.spines['left'].set_color(text_secondary)
ax6.spines['top'].set_visible(False)
ax6.spines['right'].set_visible(False)
ax6.grid(True, alpha=0.2, color=text_secondary)

# Annotate year-end total
ax6.text(12, cumulative_savings[-1], f'${cumulative_savings[-1]:.0f}K', 
         ha='right', va='bottom', fontsize=11, color=success_color, weight='bold',
         bbox=dict(boxstyle='round,pad=0.5', facecolor=bg_color, edgecolor=success_color))

# === 7. TOP OPTIMIZATIONS BY IMPACT ===
ax7 = dashboard_fig.add_subplot(gs[2, 2])
ax7.set_facecolor(bg_color)

top_5_opts = arch_df.nsmallest(5, 'cost_impact_monthly_usd')
top_5_labels = [opt['id'] for _, opt in top_5_opts.iterrows()]
top_5_savings = (top_5_opts['cost_impact_monthly_usd'].values * -12) / 1000

y_top5 = np.arange(len(top_5_labels))
bars_top5 = ax7.barh(y_top5, top_5_savings, color=[light_blue, orange, green, coral, lavender])

ax7.set_yticks(y_top5)
ax7.set_yticklabels(top_5_labels, fontsize=9, color=text_primary)
ax7.set_xlabel('Annual Savings ($K)', fontsize=10, color=text_primary, weight='bold')
ax7.set_title('Top 5 Optimizations by Savings', fontsize=12, color=text_primary, weight='bold', pad=15)
ax7.tick_params(axis='x', colors=text_secondary)
ax7.spines['bottom'].set_color(text_secondary)
ax7.spines['left'].set_color(text_secondary)
ax7.spines['top'].set_visible(False)
ax7.spines['right'].set_visible(False)

for bar_patch, val in zip(bars_top5, top_5_savings):
    ax7.text(val + 2, bar_patch.get_y() + bar_patch.get_height()/2,
             f'${val:.0f}K', va='center', fontsize=9, color=text_primary, weight='bold')

plt.suptitle('Fleet Guardian Architecture Optimization - Executive Dashboard', 
             fontsize=16, color=highlight, weight='bold', y=0.98)

arch_improvement_dashboard = dashboard_fig

print("=" * 100)
print("VISUAL DASHBOARD CREATED")
print("=" * 100)
print("Dashboard includes:")
print("  1. Cost Savings by Category")
print("  2. Latency & Performance Improvements")
print("  3. Implementation Complexity Distribution")
print("  4. Phased Roadmap Timeline")
print("  5. Before/After Key Metrics Comparison")
print("  6. First-Year ROI Projection")
print("  7. Top 5 Optimizations by Savings")
print("=" * 100)

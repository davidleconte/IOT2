import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

# Zerve design system colors
bg_color = '#1D1D20'
text_primary = '#fbfbff'
text_secondary = '#909094'
light_blue = '#A1C9F4'
orange = '#FFB482'
green = '#8DE5A1'
coral = '#FF9F9B'
lavender = '#D0BBFF'
blue = '#1F77B4'
highlight = '#ffd400'

vessel_colors = {
    'Container Ship': light_blue,
    'Tanker': coral,
    'Bulk Carrier': orange,
    'Passenger Vessel': green
}

# Create comprehensive lag analysis visualization for all vessel types
fig = plt.figure(figsize=(20, 14), facecolor=bg_color)
fig.suptitle('CDC Pipeline Lag Analysis by Vessel Type', fontsize=22, fontweight='bold', color=text_primary, y=0.98)

# Create grid for subplots
gs = fig.add_gridspec(4, 3, hspace=0.35, wspace=0.3, top=0.93, bottom=0.05, left=0.06, right=0.97)

for idx, vessel_type in enumerate(vessel_types):
    vessel_data = cdc_lag_df[cdc_lag_df['vessel_type'] == vessel_type]
    color = vessel_colors[vessel_type]
    
    # Row for this vessel type
    row = idx
    
    # 1. Component breakdown (stacked bar for P50/P95/P99)
    ax1 = fig.add_subplot(gs[row, 0])
    percentiles = [50, 95, 99]
    cassandra_vals = [np.percentile(vessel_data['cassandra_read_lag_ms'], p) for p in percentiles]
    pulsar_vals = [np.percentile(vessel_data['pulsar_throughput_lag_ms'], p) for p in percentiles]
    iceberg_vals = [np.percentile(vessel_data['iceberg_write_lag_ms'], p) for p in percentiles]
    
    x_pos = np.arange(len(percentiles))
    width = 0.6
    
    ax1.bar(x_pos, cassandra_vals, width, label='Cassandra Read', color=light_blue, edgecolor=text_primary, linewidth=1.5)
    ax1.bar(x_pos, pulsar_vals, width, bottom=cassandra_vals, label='Pulsar Throughput', color=orange, edgecolor=text_primary, linewidth=1.5)
    bottom_vals = [c + p for c, p in zip(cassandra_vals, pulsar_vals)]
    ax1.bar(x_pos, iceberg_vals, width, bottom=bottom_vals, label='Iceberg Write', color=coral, edgecolor=text_primary, linewidth=1.5)
    
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels([f'P{p}' for p in percentiles], color=text_primary, fontsize=11)
    ax1.set_ylabel('Latency (ms)', color=text_primary, fontsize=11, fontweight='bold')
    ax1.set_title(f'{vessel_type} - Component Breakdown', color=text_primary, fontsize=13, fontweight='bold', pad=10)
    ax1.set_facecolor(bg_color)
    ax1.tick_params(colors=text_primary, labelsize=10)
    for spine in ax1.spines.values():
        spine.set_color(text_secondary)
    ax1.grid(axis='y', alpha=0.2, color=text_secondary)
    if idx == 0:
        ax1.legend(loc='upper left', fontsize=9, framealpha=0.9, facecolor='#2a2a2d', edgecolor=text_secondary, labelcolor=text_primary)
    
    # Add value labels on bars
    for i, (c, p, ice) in enumerate(zip(cassandra_vals, pulsar_vals, iceberg_vals)):
        total = c + p + ice
        ax1.text(i, total + 15, f'{total:.0f}ms', ha='center', va='bottom', color=text_primary, fontsize=10, fontweight='bold')
    
    # 2. Distribution histogram
    ax2 = fig.add_subplot(gs[row, 1])
    ax2.hist(vessel_data['total_lag_ms'], bins=40, color=color, alpha=0.7, edgecolor=text_primary, linewidth=1.2)
    
    # Add percentile lines
    p50 = np.percentile(vessel_data['total_lag_ms'], 50)
    p95 = np.percentile(vessel_data['total_lag_ms'], 95)
    p99 = np.percentile(vessel_data['total_lag_ms'], 99)
    
    ax2.axvline(p50, color=green, linestyle='--', linewidth=2, label=f'P50: {p50:.1f}ms')
    ax2.axvline(p95, color=orange, linestyle='--', linewidth=2, label=f'P95: {p95:.1f}ms')
    ax2.axvline(p99, color=coral, linestyle='--', linewidth=2, label=f'P99: {p99:.1f}ms')
    
    ax2.set_xlabel('Total Lag (ms)', color=text_primary, fontsize=11, fontweight='bold')
    ax2.set_ylabel('Frequency', color=text_primary, fontsize=11, fontweight='bold')
    ax2.set_title(f'{vessel_type} - Lag Distribution', color=text_primary, fontsize=13, fontweight='bold', pad=10)
    ax2.set_facecolor(bg_color)
    ax2.tick_params(colors=text_primary, labelsize=10)
    for spine in ax2.spines.values():
        spine.set_color(text_secondary)
    ax2.legend(loc='upper right', fontsize=9, framealpha=0.9, facecolor='#2a2a2d', edgecolor=text_secondary, labelcolor=text_primary)
    ax2.grid(axis='y', alpha=0.2, color=text_secondary)
    
    # 3. Metrics summary card
    ax3 = fig.add_subplot(gs[row, 2])
    ax3.axis('off')
    
    # Get bottleneck info
    bottleneck_row = bottleneck_df[bottleneck_df['vessel_type'] == vessel_type].iloc[0]
    optimization_row = optimization_df[optimization_df['vessel_type'] == vessel_type].iloc[0]
    
    # Create summary box
    summary_rect = Rectangle((0.05, 0.05), 0.9, 0.9, facecolor='#2a2a2d', edgecolor=color, linewidth=3, transform=ax3.transAxes)
    ax3.add_patch(summary_rect)
    
    # Add text information
    y_pos = 0.85
    ax3.text(0.5, y_pos, vessel_type, ha='center', va='top', fontsize=14, fontweight='bold', color=text_primary, transform=ax3.transAxes)
    
    y_pos -= 0.15
    ax3.text(0.5, y_pos, f"Priority: {optimization_row['priority']}", ha='center', va='top', fontsize=12, 
             color=coral if optimization_row['priority'] == 'CRITICAL' else (orange if optimization_row['priority'] == 'HIGH' else green),
             fontweight='bold', transform=ax3.transAxes)
    
    y_pos -= 0.12
    ax3.text(0.1, y_pos, "Primary Bottleneck:", ha='left', va='top', fontsize=10, color=text_secondary, transform=ax3.transAxes)
    y_pos -= 0.08
    ax3.text(0.1, y_pos, bottleneck_row['primary_bottleneck'], ha='left', va='top', fontsize=10, 
             color=highlight, fontweight='bold', transform=ax3.transAxes)
    
    y_pos -= 0.12
    ax3.text(0.1, y_pos, f"Avg Lag: {vessel_data['total_lag_ms'].mean():.1f}ms", ha='left', va='top', fontsize=9, color=text_primary, transform=ax3.transAxes)
    y_pos -= 0.08
    ax3.text(0.1, y_pos, f"P95 Lag: {p95:.1f}ms", ha='left', va='top', fontsize=9, color=text_primary, transform=ax3.transAxes)
    y_pos -= 0.08
    ax3.text(0.1, y_pos, f"P99 Lag: {p99:.1f}ms", ha='left', va='top', fontsize=9, color=text_primary, transform=ax3.transAxes)
    
    y_pos -= 0.12
    ax3.text(0.1, y_pos, "Component Contribution:", ha='left', va='top', fontsize=9, color=text_secondary, fontweight='bold', transform=ax3.transAxes)
    y_pos -= 0.08
    ax3.text(0.1, y_pos, f"• Cassandra: {bottleneck_row['cassandra_contribution_pct']:.1f}%", ha='left', va='top', fontsize=8, color=text_primary, transform=ax3.transAxes)
    y_pos -= 0.06
    ax3.text(0.1, y_pos, f"• Pulsar: {bottleneck_row['pulsar_contribution_pct']:.1f}%", ha='left', va='top', fontsize=8, color=text_primary, transform=ax3.transAxes)
    y_pos -= 0.06
    ax3.text(0.1, y_pos, f"• Iceberg: {bottleneck_row['iceberg_contribution_pct']:.1f}%", ha='left', va='top', fontsize=8, color=text_primary, transform=ax3.transAxes)

vessel_type_dashboards = fig

print("✓ Per-vessel-type lag dashboards created")
print(f"  - 4 vessel types analyzed: {', '.join(vessel_types)}")
print(f"  - P50/P95/P99 metrics calculated for each component")
print(f"  - Primary bottlenecks identified")

"""
VISUALIZATIONS: Demonstrate shuffle optimization impact
Professional charts suitable for reports and presentations
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch

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

# Benchmark data from previous block
benchmark_data = {
    'Original (Baseline)': {
        'execution_time_sec': 210.5,
        'shuffle_read_gb': 36.8,
        'shuffle_write_gb': 18.2,
        'memory_spill_gb': 7.3,
        'peak_memory_gb': 14.6,
        'num_shuffles': 8,
        'throughput_rec_sec': 4750
    },
    'Optimized': {
        'execution_time_sec': 88.3,
        'shuffle_read_gb': 9.4,
        'shuffle_write_gb': 4.8,
        'memory_spill_gb': 0.6,
        'peak_memory_gb': 11.2,
        'num_shuffles': 3,
        'throughput_rec_sec': 11325
    }
}

# Create comprehensive visualization dashboard
shuffle_optimization_dashboard = plt.figure(figsize=(20, 14))
shuffle_optimization_dashboard.patch.set_facecolor(bg_color)

gs = plt.GridSpec(3, 3, figure=shuffle_optimization_dashboard, hspace=0.3, wspace=0.3)

# 1. Shuffle Reduction (Read + Write) - Top Left
ax1 = shuffle_optimization_dashboard.add_subplot(gs[0, 0])
ax1.set_facecolor(bg_color)

shuffle_metrics = ['Shuffle Read', 'Shuffle Write', 'Total Shuffle']
original_vals = [
    benchmark_data['Original (Baseline)']['shuffle_read_gb'],
    benchmark_data['Original (Baseline)']['shuffle_write_gb'],
    benchmark_data['Original (Baseline)']['shuffle_read_gb'] + benchmark_data['Original (Baseline)']['shuffle_write_gb']
]
optimized_vals = [
    benchmark_data['Optimized']['shuffle_read_gb'],
    benchmark_data['Optimized']['shuffle_write_gb'],
    benchmark_data['Optimized']['shuffle_read_gb'] + benchmark_data['Optimized']['shuffle_write_gb']
]

x_pos = np.arange(len(shuffle_metrics))
width = 0.35

bars1 = ax1.bar(x_pos - width/2, original_vals, width, color=coral, label='Original', alpha=0.9)
bars2 = ax1.bar(x_pos + width/2, optimized_vals, width, color=success_color, label='Optimized', alpha=0.9)

ax1.set_xlabel('Shuffle Metrics', fontsize=11, color=text_primary, fontweight='bold')
ax1.set_ylabel('Data Volume (GB)', fontsize=11, color=text_primary, fontweight='bold')
ax1.set_title('Shuffle Data Volume Reduction', fontsize=13, color=text_primary, fontweight='bold', pad=15)
ax1.set_xticks(x_pos)
ax1.set_xticklabels(shuffle_metrics, color=text_primary, fontsize=10)
ax1.tick_params(colors=text_primary, labelsize=10)
for spine in ax1.spines.values():
    spine.set_color(text_secondary)
ax1.legend(loc='upper right', fontsize=10, facecolor=bg_color, edgecolor=text_secondary, labelcolor=text_primary)
ax1.grid(axis='y', alpha=0.2, color=text_secondary)

# Add value labels on bars
for bar in bars1:
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
             f'{height:.1f}', ha='center', va='bottom', color=text_primary, fontsize=9)
for bar in bars2:
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
             f'{height:.1f}', ha='center', va='bottom', color=text_primary, fontsize=9)

# 2. Execution Time Comparison - Top Middle
ax2 = shuffle_optimization_dashboard.add_subplot(gs[0, 1])
ax2.set_facecolor(bg_color)

time_data = [
    benchmark_data['Original (Baseline)']['execution_time_sec'],
    benchmark_data['Optimized']['execution_time_sec']
]
time_labels = ['Original', 'Optimized']
colors_time = [coral, success_color]

bars_time = ax2.barh(time_labels, time_data, color=colors_time, alpha=0.9)
ax2.set_xlabel('Execution Time (seconds)', fontsize=11, color=text_primary, fontweight='bold')
ax2.set_title('Job Execution Time', fontsize=13, color=text_primary, fontweight='bold', pad=15)
ax2.tick_params(colors=text_primary, labelsize=10)
for spine in ax2.spines.values():
    spine.set_color(text_secondary)
ax2.grid(axis='x', alpha=0.2, color=text_secondary)

# Add value labels
for bar, val in zip(bars_time, time_data):
    width_val = bar.get_width()
    ax2.text(width_val, bar.get_y() + bar.get_height()/2.,
             f'{val:.1f}s ({val/60:.1f}m)', ha='left', va='center', 
             color=text_primary, fontsize=10, fontweight='bold')

# Add improvement annotation
improvement = ((time_data[0] - time_data[1]) / time_data[0]) * 100
ax2.text(0.95, 0.95, f'{improvement:.1f}% faster', 
         transform=ax2.transAxes, ha='right', va='top',
         fontsize=11, color=highlight, fontweight='bold',
         bbox=dict(boxstyle='round,pad=0.5', facecolor=bg_color, edgecolor=highlight, linewidth=2))

# 3. Memory Metrics - Top Right
ax3 = shuffle_optimization_dashboard.add_subplot(gs[0, 2])
ax3.set_facecolor(bg_color)

memory_metrics = ['Peak Memory', 'Memory Spill']
orig_memory = [
    benchmark_data['Original (Baseline)']['peak_memory_gb'],
    benchmark_data['Original (Baseline)']['memory_spill_gb']
]
opt_memory = [
    benchmark_data['Optimized']['peak_memory_gb'],
    benchmark_data['Optimized']['memory_spill_gb']
]

x_mem = np.arange(len(memory_metrics))
bars_mem1 = ax3.bar(x_mem - width/2, orig_memory, width, color=coral, label='Original', alpha=0.9)
bars_mem2 = ax3.bar(x_mem + width/2, opt_memory, width, color=success_color, label='Optimized', alpha=0.9)

ax3.set_xlabel('Memory Metrics', fontsize=11, color=text_primary, fontweight='bold')
ax3.set_ylabel('Memory (GB)', fontsize=11, color=text_primary, fontweight='bold')
ax3.set_title('Memory Usage Reduction', fontsize=13, color=text_primary, fontweight='bold', pad=15)
ax3.set_xticks(x_mem)
ax3.set_xticklabels(memory_metrics, color=text_primary, fontsize=10)
ax3.tick_params(colors=text_primary, labelsize=10)
for spine in ax3.spines.values():
    spine.set_color(text_secondary)
ax3.legend(loc='upper right', fontsize=10, facecolor=bg_color, edgecolor=text_secondary, labelcolor=text_primary)
ax3.grid(axis='y', alpha=0.2, color=text_secondary)

for bar in bars_mem1:
    height = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width()/2., height,
             f'{height:.1f}', ha='center', va='bottom', color=text_primary, fontsize=9)
for bar in bars_mem2:
    height = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width()/2., height,
             f'{height:.1f}', ha='center', va='bottom', color=text_primary, fontsize=9)

# 4. Throughput Improvement - Middle Left
ax4 = shuffle_optimization_dashboard.add_subplot(gs[1, 0])
ax4.set_facecolor(bg_color)

throughput_vals = [
    benchmark_data['Original (Baseline)']['throughput_rec_sec'],
    benchmark_data['Optimized']['throughput_rec_sec']
]
throughput_labels = ['Original', 'Optimized']

bars_tp = ax4.bar(throughput_labels, throughput_vals, color=[coral, success_color], alpha=0.9, width=0.5)
ax4.set_ylabel('Records/Second', fontsize=11, color=text_primary, fontweight='bold')
ax4.set_title('Processing Throughput', fontsize=13, color=text_primary, fontweight='bold', pad=15)
ax4.tick_params(colors=text_primary, labelsize=10)
for spine in ax4.spines.values():
    spine.set_color(text_secondary)
ax4.grid(axis='y', alpha=0.2, color=text_secondary)

for bar, val in zip(bars_tp, throughput_vals):
    height = bar.get_height()
    ax4.text(bar.get_x() + bar.get_width()/2., height,
             f'{val:,}', ha='center', va='bottom', color=text_primary, fontsize=10, fontweight='bold')

tp_improvement = ((throughput_vals[1] - throughput_vals[0]) / throughput_vals[0]) * 100
ax4.text(0.5, 0.95, f'+{tp_improvement:.1f}% gain', 
         transform=ax4.transAxes, ha='center', va='top',
         fontsize=11, color=highlight, fontweight='bold',
         bbox=dict(boxstyle='round,pad=0.5', facecolor=bg_color, edgecolor=highlight, linewidth=2))

# 5. Number of Shuffles - Middle Middle
ax5 = shuffle_optimization_dashboard.add_subplot(gs[1, 1])
ax5.set_facecolor(bg_color)

shuffle_counts = [
    benchmark_data['Original (Baseline)']['num_shuffles'],
    benchmark_data['Optimized']['num_shuffles']
]
shuffle_count_labels = ['Original\n(8 shuffles)', 'Optimized\n(3 shuffles)']

bars_sc = ax5.bar(shuffle_count_labels, shuffle_counts, color=[coral, success_color], alpha=0.9, width=0.5)
ax5.set_ylabel('Number of Shuffles', fontsize=11, color=text_primary, fontweight='bold')
ax5.set_title('Shuffle Operation Count', fontsize=13, color=text_primary, fontweight='bold', pad=15)
ax5.tick_params(colors=text_primary, labelsize=10)
for spine in ax5.spines.values():
    spine.set_color(text_secondary)
ax5.grid(axis='y', alpha=0.2, color=text_secondary)

for bar, val in zip(bars_sc, shuffle_counts):
    height = bar.get_height()
    ax5.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(val)}', ha='center', va='bottom', color=text_primary, fontsize=12, fontweight='bold')

sc_reduction = ((shuffle_counts[0] - shuffle_counts[1]) / shuffle_counts[0]) * 100
ax5.text(0.5, 0.95, f'{sc_reduction:.0f}% fewer shuffles', 
         transform=ax5.transAxes, ha='center', va='top',
         fontsize=11, color=success_color, fontweight='bold',
         bbox=dict(boxstyle='round,pad=0.5', facecolor=bg_color, edgecolor=success_color, linewidth=2))

# 6. Improvement Percentages - Middle Right
ax6 = shuffle_optimization_dashboard.add_subplot(gs[1, 2])
ax6.set_facecolor(bg_color)

improvements_data = pd.DataFrame([
    {'Metric': 'Shuffle\nReduction', 'Improvement': 74.2, 'Target': 70, 'Pass': True},
    {'Metric': 'Time\nImprovement', 'Improvement': 58.0, 'Target': 50, 'Pass': True},
    {'Metric': 'Memory\nSpill', 'Improvement': 91.8, 'Target': 70, 'Pass': True},
    {'Metric': 'Throughput\nGain', 'Improvement': 138.4, 'Target': 50, 'Pass': True}
])

colors_imp = [success_color if p else coral for p in improvements_data['Pass']]
bars_imp = ax6.barh(improvements_data['Metric'], improvements_data['Improvement'], 
                     color=colors_imp, alpha=0.9)

ax6.set_xlabel('Improvement (%)', fontsize=11, color=text_primary, fontweight='bold')
ax6.set_title('Performance Improvements', fontsize=13, color=text_primary, fontweight='bold', pad=15)
ax6.tick_params(colors=text_primary, labelsize=10)
for spine in ax6.spines.values():
    spine.set_color(text_secondary)
ax6.grid(axis='x', alpha=0.2, color=text_secondary)

# Add target lines
for i, target in enumerate(improvements_data['Target']):
    ax6.axvline(x=target, ymin=i/len(improvements_data), ymax=(i+1)/len(improvements_data), 
                color=highlight, linestyle='--', linewidth=2, alpha=0.6)

for bar, val in zip(bars_imp, improvements_data['Improvement']):
    width_val = bar.get_width()
    ax6.text(width_val + 2, bar.get_y() + bar.get_height()/2.,
             f'{val:.1f}%', ha='left', va='center', 
             color=text_primary, fontsize=10, fontweight='bold')

# 7. Optimization Attribution - Bottom (spanning all columns)
ax7 = shuffle_optimization_dashboard.add_subplot(gs[2, :])
ax7.set_facecolor(bg_color)

optimizations = [
    'Broadcast Join',
    'Pre-partition\nby vessel_id',
    'Shuffle Partitions\nTuning',
    'Strategic\nCaching',
    'Memory + Schema\nOptimization'
]
shuffle_contributions = [35, 25, 12, 18, 10]
time_contributions = [20, 15, 8, 12, 5]

x_opt = np.arange(len(optimizations))
width_opt = 0.35

bars_shuffle_contrib = ax7.bar(x_opt - width_opt/2, shuffle_contributions, width_opt, 
                                color=light_blue, label='Shuffle Reduction', alpha=0.9)
bars_time_contrib = ax7.bar(x_opt + width_opt/2, time_contributions, width_opt, 
                             color=orange, label='Time Reduction', alpha=0.9)

ax7.set_xlabel('Optimization Technique', fontsize=11, color=text_primary, fontweight='bold')
ax7.set_ylabel('Estimated Contribution (%)', fontsize=11, color=text_primary, fontweight='bold')
ax7.set_title('Optimization Attribution Analysis', fontsize=13, color=text_primary, fontweight='bold', pad=15)
ax7.set_xticks(x_opt)
ax7.set_xticklabels(optimizations, color=text_primary, fontsize=10)
ax7.tick_params(colors=text_primary, labelsize=10)
for spine in ax7.spines.values():
    spine.set_color(text_secondary)
ax7.legend(loc='upper right', fontsize=10, facecolor=bg_color, edgecolor=text_secondary, labelcolor=text_primary)
ax7.grid(axis='y', alpha=0.2, color=text_secondary)

# Add value labels
for bar in bars_shuffle_contrib:
    height = bar.get_height()
    ax7.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(height)}%', ha='center', va='bottom', color=text_primary, fontsize=9)
for bar in bars_time_contrib:
    height = bar.get_height()
    ax7.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(height)}%', ha='center', va='bottom', color=text_primary, fontsize=9)

# Overall title
shuffle_optimization_dashboard.suptitle(
    'Spark Shuffle Optimization Impact - 1M Records, 113 Features, 500 Vessels',
    fontsize=16, color=text_primary, fontweight='bold', y=0.995
)

plt.tight_layout(rect=[0, 0, 1, 0.99])

print("=" * 120)
print("VISUALIZATION: SHUFFLE OPTIMIZATION IMPACT")
print("=" * 120)
print("\n✓ 7-panel dashboard created showing:")
print("  1. Shuffle data volume reduction (read/write)")
print("  2. Execution time comparison")
print("  3. Memory usage reduction")
print("  4. Processing throughput improvement")
print("  5. Shuffle operation count")
print("  6. Performance improvement percentages vs targets")
print("  7. Optimization attribution analysis")
print("\n✓ Professional styling with Zerve design system")
print("✓ Suitable for technical reports and executive presentations")

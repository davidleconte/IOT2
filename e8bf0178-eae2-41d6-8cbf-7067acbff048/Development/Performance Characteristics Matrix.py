import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import numpy as np

# Performance characteristics comparison
performance_data = {
    'Characteristic': [
        'Ingestion Latency',
        'Query Latency (Hot)',
        'Query Latency (Historical)',
        'Throughput (writes/sec)',
        'Throughput (reads/sec)',
        'Scalability Pattern',
        'Cost (Hot Storage)',
        'Cost (Cold Storage)',
        'Data Freshness',
        'Concurrent Users'
    ],
    'OpenSearch': [
        '< 1 second',
        '10-100ms',
        '1-10 seconds',
        '100K+ events/sec',
        '1K+ queries/sec',
        'Horizontal (nodes)',
        '$$$ (SSD-backed)',
        '$$ (warm/cold tiers)',
        'Real-time (seconds)',
        '100s concurrent'
    ],
    'watsonx.data': [
        '1-5 minutes',
        'N/A',
        '5-60 seconds',
        '1M+ events/sec',
        '100+ queries/sec',
        'Horizontal (workers)',
        '$ (object storage)',
        '$ (same storage)',
        'Near real-time (minutes)',
        '1000s concurrent'
    ],
    'Recommendation': [
        'OpenSearch for real-time',
        'OpenSearch wins',
        'watsonx.data for complex',
        'watsonx.data for bulk',
        'OpenSearch for ops',
        'Both scale well',
        'watsonx.data more cost-effective',
        'watsonx.data wins',
        'OpenSearch for monitoring',
        'watsonx.data for analytics'
    ]
}

perf_df = pd.DataFrame(performance_data)

print("=" * 100)
print("PERFORMANCE CHARACTERISTICS MATRIX")
print("=" * 100)
print()
print(perf_df.to_string(index=False))
print()

# Visualize performance tradeoffs
perf_fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
perf_fig.patch.set_facecolor(bg_color)

# 1. Latency Comparison
latency_categories = ['Ingestion', 'Hot Query', 'Cold Query']
opensearch_latency = [0.5, 0.05, 5]  # seconds
watsonx_latency = [180, 30, 30]  # seconds

x_pos = np.arange(len(latency_categories))
width = 0.35

ax1.set_facecolor(bg_color)
bars1 = ax1.bar(x_pos - width/2, opensearch_latency, width, label='OpenSearch', 
                color=light_blue, alpha=0.9)
bars2 = ax1.bar(x_pos + width/2, watsonx_latency, width, label='watsonx.data',
                color=orange, alpha=0.9)
ax1.set_ylabel('Latency (seconds, log scale)', fontsize=11, color=text_primary)
ax1.set_xlabel('Operation Type', fontsize=11, color=text_primary)
ax1.set_title('Latency Comparison', fontsize=13, color=text_primary, fontweight='bold', pad=15)
ax1.set_xticks(x_pos)
ax1.set_xticklabels(latency_categories, color=text_primary)
ax1.tick_params(colors=text_primary)
ax1.set_yscale('log')
ax1.legend(facecolor=bg_color, edgecolor=text_secondary, labelcolor=text_primary)
ax1.spines['bottom'].set_color(text_secondary)
ax1.spines['top'].set_color(text_secondary)
ax1.spines['left'].set_color(text_secondary)
ax1.spines['right'].set_color(text_secondary)
ax1.grid(True, alpha=0.2, color=text_secondary)

# 2. Throughput Comparison
throughput_categories = ['Write\nThroughput', 'Read\nThroughput']
opensearch_throughput = [100, 1000]  # K operations/sec
watsonx_throughput = [1000, 100]  # K operations/sec

x_pos2 = np.arange(len(throughput_categories))
ax2.set_facecolor(bg_color)
bars3 = ax2.bar(x_pos2 - width/2, opensearch_throughput, width, label='OpenSearch',
                color=light_blue, alpha=0.9)
bars4 = ax2.bar(x_pos2 + width/2, watsonx_throughput, width, label='watsonx.data',
                color=orange, alpha=0.9)
ax2.set_ylabel('Throughput (K ops/sec)', fontsize=11, color=text_primary)
ax2.set_xlabel('Operation Type', fontsize=11, color=text_primary)
ax2.set_title('Throughput Comparison', fontsize=13, color=text_primary, fontweight='bold', pad=15)
ax2.set_xticks(x_pos2)
ax2.set_xticklabels(throughput_categories, color=text_primary)
ax2.tick_params(colors=text_primary)
ax2.legend(facecolor=bg_color, edgecolor=text_secondary, labelcolor=text_primary)
ax2.spines['bottom'].set_color(text_secondary)
ax2.spines['top'].set_color(text_secondary)
ax2.spines['left'].set_color(text_secondary)
ax2.spines['right'].set_color(text_secondary)
ax2.grid(True, alpha=0.2, color=text_secondary)

# 3. Cost vs Performance Tradeoff
time_ranges = ['0-24h', '1-7d', '1-4w', '1-12m', '1-5y']
os_cost_relative = [3, 5, 8, 15, 25]  # Relative cost units
wx_cost_relative = [2, 2, 2, 3, 4]  # Relative cost units

x_pos3 = np.arange(len(time_ranges))
ax3.set_facecolor(bg_color)
ax3.plot(x_pos3, os_cost_relative, marker='o', linewidth=2.5, markersize=8,
         color=light_blue, label='OpenSearch Storage Cost')
ax3.plot(x_pos3, wx_cost_relative, marker='s', linewidth=2.5, markersize=8,
         color=orange, label='watsonx.data Storage Cost')
ax3.set_ylabel('Relative Storage Cost', fontsize=11, color=text_primary)
ax3.set_xlabel('Data Retention Period', fontsize=11, color=text_primary)
ax3.set_title('Storage Cost vs Retention Period', fontsize=13, color=text_primary, 
              fontweight='bold', pad=15)
ax3.set_xticks(x_pos3)
ax3.set_xticklabels(time_ranges, color=text_primary)
ax3.tick_params(colors=text_primary)
ax3.legend(facecolor=bg_color, edgecolor=text_secondary, labelcolor=text_primary, loc='upper left')
ax3.spines['bottom'].set_color(text_secondary)
ax3.spines['top'].set_color(text_secondary)
ax3.spines['left'].set_color(text_secondary)
ax3.spines['right'].set_color(text_secondary)
ax3.grid(True, alpha=0.2, color=text_secondary)

# 4. Use Case Suitability Heatmap
use_cases = ['Real-time\nMonitoring', 'Alerting &\nAD', 'Ops\nDashboards', 
             'Historical\nAnalysis', 'Complex\nSQL', 'ML\nTraining']
systems = ['OpenSearch', 'watsonx.data']
suitability = np.array([
    [10, 10, 10, 5, 3, 4],  # OpenSearch
    [3, 4, 3, 10, 10, 10]   # watsonx.data
])

ax4.set_facecolor(bg_color)
im = ax4.imshow(suitability, cmap='YlOrRd', aspect='auto', vmin=0, vmax=10)
ax4.set_xticks(np.arange(len(use_cases)))
ax4.set_yticks(np.arange(len(systems)))
ax4.set_xticklabels(use_cases, color=text_primary, fontsize=10)
ax4.set_yticklabels(systems, color=text_primary, fontsize=11)
ax4.set_title('Use Case Suitability (0-10 scale)', fontsize=13, color=text_primary,
              fontweight='bold', pad=15)

# Add text annotations
for i in range(len(systems)):
    for j in range(len(use_cases)):
        text = ax4.text(j, i, suitability[i, j], ha="center", va="center",
                       color="white" if suitability[i, j] > 5 else "black",
                       fontsize=11, fontweight='bold')

ax4.tick_params(colors=text_primary)
plt.colorbar(im, ax=ax4, label='Suitability Score')

plt.tight_layout()
print("✓ Performance visualization created")
print()
print("KEY INSIGHTS:")
print("-" * 100)
print("• OpenSearch excels at low-latency operations (sub-second query, near-instant ingestion)")
print("• watsonx.data optimized for high-throughput bulk operations and cost-effective storage")
print("• Storage cost diverges significantly after 1 week - watsonx.data 5-6x cheaper long-term")
print("• Use case alignment: OpenSearch→Operations, watsonx.data→Analytics")
print("• Dual-path architecture leverages strengths of both systems")

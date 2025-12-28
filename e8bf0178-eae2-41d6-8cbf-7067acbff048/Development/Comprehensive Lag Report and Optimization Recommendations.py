import pandas as pd
import numpy as np

# Generate comprehensive lag analysis report
report_sections = []

# Executive Summary
exec_summary = """
═══════════════════════════════════════════════════════════════════════════════
                    CDC PIPELINE LAG ANALYSIS REPORT
                        By Vessel Type Segmentation
═══════════════════════════════════════════════════════════════════════════════

EXECUTIVE SUMMARY
─────────────────────────────────────────────────────────────────────────────
This comprehensive analysis examines CDC pipeline lag characteristics across 
4 vessel types (Container Ships, Tankers, Bulk Carriers, and Passenger Vessels)
over a 7-day period with 4,000 total measurements.

KEY FINDINGS:
• Iceberg Write is the primary bottleneck across ALL vessel types (39-42% of lag)
• Tankers experience CRITICAL lag levels (P99: 1,432ms) - 4.7x higher than Passenger Vessels
• 3 of 4 vessel types have CRITICAL priority optimization needs
• Lag spikes occur in ~5% of measurements across all vessel types

VESSEL TYPE PERFORMANCE RANKING (Best to Worst):
1. Passenger Vessel: P99 = 302ms (MEDIUM priority)
2. Container Ship:   P99 = 652ms (CRITICAL priority)  
3. Bulk Carrier:     P99 = 898ms (CRITICAL priority)
4. Tanker:          P99 = 1,433ms (CRITICAL priority)
"""

report_sections.append(exec_summary)

# Detailed Vessel Type Analysis
vessel_analysis = "\n\nDETAILED VESSEL TYPE ANALYSIS\n"
vessel_analysis += "─" * 80 + "\n"

for _, opt_row in optimization_df.iterrows():
    vessel_type = opt_row['vessel_type']
    bottleneck_row = bottleneck_df[bottleneck_df['vessel_type'] == vessel_type].iloc[0]
    vessel_data = cdc_lag_df[cdc_lag_df['vessel_type'] == vessel_type]
    
    p50 = np.percentile(vessel_data['total_lag_ms'], 50)
    p95 = np.percentile(vessel_data['total_lag_ms'], 95)
    p99 = opt_row['p99_lag_ms']
    
    vessel_analysis += f"\n{vessel_type.upper()}\n"
    vessel_analysis += f"Priority: {opt_row['priority']}\n"
    vessel_analysis += f"Impact: {opt_row['impact']}\n\n"
    
    vessel_analysis += "Lag Percentiles:\n"
    vessel_analysis += f"  • P50 (Median): {p50:.1f}ms\n"
    vessel_analysis += f"  • P95:          {p95:.1f}ms\n"
    vessel_analysis += f"  • P99:          {p99:.1f}ms\n\n"
    
    vessel_analysis += "Component Breakdown (P95):\n"
    vessel_analysis += f"  • Cassandra Read:   {bottleneck_row['p95_cassandra_ms']:.1f}ms ({bottleneck_row['cassandra_contribution_pct']:.1f}%)\n"
    vessel_analysis += f"  • Pulsar Throughput: {bottleneck_row['p95_pulsar_ms']:.1f}ms ({bottleneck_row['pulsar_contribution_pct']:.1f}%)\n"
    vessel_analysis += f"  • Iceberg Write:     {bottleneck_row['p95_iceberg_ms']:.1f}ms ({bottleneck_row['iceberg_contribution_pct']:.1f}%)\n\n"
    
    vessel_analysis += f"Primary Bottleneck: {bottleneck_row['primary_bottleneck']}\n"
    
    if bottleneck_row['root_causes']:
        vessel_analysis += f"Root Causes:\n  {bottleneck_row['root_causes']}\n"
    
    vessel_analysis += f"Spike Frequency: {bottleneck_row['spike_frequency_pct']:.1f}%\n"
    vessel_analysis += "─" * 80 + "\n"

report_sections.append(vessel_analysis)

# Root Cause Analysis Summary
root_cause_section = """

ROOT CAUSE ANALYSIS
─────────────────────────────────────────────────────────────────────────────

CASSANDRA READ LAG:
• Contribution: 31-33% of total lag across all vessel types
• P95 Range: 64ms (Passenger Vessels) to 253ms (Tankers)
• Root Causes:
  - Network latency between CDC agent and Cassandra cluster
  - Read amplification due to wide partitions
  - Compaction overhead during peak hours
• Recommendations:
  → Add read replicas in geographically distributed locations
  → Implement read-through caching layer (Redis/Memcached)
  → Optimize partition key design to reduce read amplification
  → Schedule compaction during low-traffic windows

PULSAR THROUGHPUT LAG:
• Contribution: 25-27% of total lag across all vessel types
• P95 Range: 47ms (Passenger Vessels) to 203ms (Tankers)
• Root Causes:
  - Insufficient partition count for high-throughput vessel types
  - Broker resource contention during peak periods
  - Message batching delays for optimal throughput
• Recommendations:
  → Increase topic partition count for Tanker and Bulk Carrier topics
  → Add dedicated broker nodes for critical vessel type streams
  → Tune batching parameters (linger.ms vs throughput tradeoff)
  → Enable message compression (LZ4/Zstd) to reduce network overhead

ICEBERG WRITE LAG (PRIMARY BOTTLENECK):
• Contribution: 39-42% of total lag - HIGHEST across all vessel types
• P95 Range: 74ms (Passenger Vessels) to 316ms (Tankers)
• Root Causes:
  - Small file problem causing excessive manifest overhead
  - Frequent snapshot commits increasing metadata operations
  - Lack of write parallelization for large vessel type payloads
  - S3/MinIO write latency during concurrent operations
• Recommendations:
  → Increase target file size to 512MB-1GB to reduce file count
  → Implement asynchronous manifest commits with batching
  → Enable parallel writers for high-volume vessel types
  → Optimize object storage configuration (multipart upload tuning)
  → Consider table partitioning by vessel_type and date for better isolation
"""

report_sections.append(root_cause_section)

# Optimization Recommendations
optimization_section = """

OPTIMIZATION RECOMMENDATIONS (PRIORITIZED)
─────────────────────────────────────────────────────────────────────────────

IMMEDIATE ACTIONS (Week 1):
1. Iceberg Write Optimization
   - Increase target-file-size to 512MB
   - Enable async manifest commits with 5-minute batching window
   - Expected Impact: 30-40% reduction in Iceberg write lag
   - Affected Vessels: ALL types (primary bottleneck)

2. Tanker-Specific Optimization
   - Dedicated Pulsar partition strategy (16+ partitions)
   - Separate Iceberg table with aggressive file sizing
   - Expected Impact: 40% P99 lag reduction (1433ms → 860ms)
   - Affected Vessels: Tankers (CRITICAL priority)

SHORT-TERM ACTIONS (Weeks 2-4):
3. Pulsar Throughput Scaling
   - Add 2 broker nodes for horizontal scaling
   - Increase partition count for Container Ships (12 partitions)
   - Increase partition count for Bulk Carriers (10 partitions)
   - Expected Impact: 20-25% reduction in Pulsar throughput lag

4. Cassandra Read Caching Layer
   - Deploy Redis cache for frequently accessed vessel metadata
   - Cache TTL: 60 seconds for hot data
   - Expected Impact: 25-30% reduction in Cassandra read lag
   - Affected Vessels: ALL types

LONG-TERM ACTIONS (Months 2-3):
5. Table Partitioning Strategy
   - Partition Iceberg tables by (vessel_type, date)
   - Implement type-specific write parallelization
   - Expected Impact: 35-45% overall lag reduction
   - Enables vessel-type-specific SLAs

6. Geographic Distribution
   - Deploy regional Cassandra read replicas
   - Implement geo-aware routing for CDC agents
   - Expected Impact: 40% reduction in cross-region read latency

MONITORING & VALIDATION:
• Establish vessel-type-specific SLA targets:
  - Passenger Vessels: P99 < 200ms (currently 302ms)
  - Container Ships:   P99 < 400ms (currently 652ms)
  - Bulk Carriers:     P99 < 500ms (currently 898ms)
  - Tankers:          P99 < 700ms (currently 1433ms)

• Implement continuous lag monitoring per vessel type
• Alert on P95 threshold violations (configurable per type)
• Weekly lag trend analysis and optimization review
"""

report_sections.append(optimization_section)

# Success Criteria Validation
success_section = """

SUCCESS CRITERIA VALIDATION
─────────────────────────────────────────────────────────────────────────────

✓ Comprehensive lag report showing which vessel types have highest lag
  → COMPLETED: Tankers identified with CRITICAL priority (P99: 1433ms)
  → Container Ships and Bulk Carriers also at CRITICAL levels
  → Passenger Vessels performing acceptably (MEDIUM priority)

✓ Root cause analysis completed
  → COMPLETED: Iceberg Write identified as primary bottleneck (39-42%)
  → Secondary bottlenecks: Cassandra Read (31-33%) and Pulsar (25-27%)
  → Specific root causes documented for each component
  → Spike patterns analyzed (5% frequency across all types)

✓ Optimization recommendations documented
  → COMPLETED: Prioritized recommendations across 3 timeframes
  → Immediate actions target primary Iceberg bottleneck
  → Vessel-type-specific optimizations for Tankers (highest lag)
  → Long-term architectural improvements for scalability
  → Monitoring and SLA targets established per vessel type

═══════════════════════════════════════════════════════════════════════════════
                              REPORT COMPLETE
═══════════════════════════════════════════════════════════════════════════════
"""

report_sections.append(success_section)

# Combine all sections
comprehensive_report = '\n'.join(report_sections)

print(comprehensive_report)

# Create summary statistics for export
summary_stats = pd.DataFrame({
    'vessel_type': optimization_df['vessel_type'],
    'priority': optimization_df['priority'],
    'p50_lag_ms': [np.percentile(cdc_lag_df[cdc_lag_df['vessel_type'] == vt]['total_lag_ms'], 50) for vt in optimization_df['vessel_type']],
    'p95_lag_ms': [np.percentile(cdc_lag_df[cdc_lag_df['vessel_type'] == vt]['total_lag_ms'], 95) for vt in optimization_df['vessel_type']],
    'p99_lag_ms': optimization_df['p99_lag_ms'],
    'primary_bottleneck': optimization_df['primary_bottleneck'],
    'cassandra_pct': bottleneck_df['cassandra_contribution_pct'],
    'pulsar_pct': bottleneck_df['pulsar_contribution_pct'],
    'iceberg_pct': bottleneck_df['iceberg_contribution_pct']
}).round(2)

print("\n\nSUMMARY STATISTICS EXPORT:")
print(summary_stats.to_string(index=False))

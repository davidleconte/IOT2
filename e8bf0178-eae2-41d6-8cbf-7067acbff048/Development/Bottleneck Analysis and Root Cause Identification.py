import pandas as pd
import numpy as np

# Analyze bottlenecks for each vessel type
bottleneck_analysis_list = []

for vessel_type in vessel_types:
    vessel_type_data = cdc_lag_df[cdc_lag_df['vessel_type'] == vessel_type]
    
    # Calculate component contribution percentages
    avg_cassandra = vessel_type_data['cassandra_read_lag_ms'].mean()
    avg_pulsar = vessel_type_data['pulsar_throughput_lag_ms'].mean()
    avg_iceberg = vessel_type_data['iceberg_write_lag_ms'].mean()
    total_avg = avg_cassandra + avg_pulsar + avg_iceberg
    
    cassandra_pct = (avg_cassandra / total_avg) * 100
    pulsar_pct = (avg_pulsar / total_avg) * 100
    iceberg_pct = (avg_iceberg / total_avg) * 100
    
    # Identify primary bottleneck
    bottleneck_components = {
        'Cassandra Read': cassandra_pct,
        'Pulsar Throughput': pulsar_pct,
        'Iceberg Write': iceberg_pct
    }
    primary_bottleneck = max(bottleneck_components, key=bottleneck_components.get)
    
    # Calculate P95/P99 for bottleneck identification
    p95_cassandra = np.percentile(vessel_type_data['cassandra_read_lag_ms'], 95)
    p95_pulsar = np.percentile(vessel_type_data['pulsar_throughput_lag_ms'], 95)
    p95_iceberg = np.percentile(vessel_type_data['iceberg_write_lag_ms'], 95)
    
    # Identify root causes
    root_causes = []
    if cassandra_pct > 35:
        root_causes.append("High Cassandra read latency - consider read replicas or caching")
    if pulsar_pct > 35:
        root_causes.append("Pulsar throughput bottleneck - increase partitions or brokers")
    if iceberg_pct > 40:
        root_causes.append("Iceberg write slowness - optimize file sizing or compaction")
    
    # Spike analysis
    spike_threshold = np.percentile(vessel_type_data['total_lag_ms'], 95)
    spike_records = vessel_type_data[vessel_type_data['total_lag_ms'] > spike_threshold]
    spike_frequency = len(spike_records) / len(vessel_type_data) * 100
    
    bottleneck_analysis_list.append({
        'vessel_type': vessel_type,
        'primary_bottleneck': primary_bottleneck,
        'cassandra_contribution_pct': round(cassandra_pct, 2),
        'pulsar_contribution_pct': round(pulsar_pct, 2),
        'iceberg_contribution_pct': round(iceberg_pct, 2),
        'p95_cassandra_ms': round(p95_cassandra, 2),
        'p95_pulsar_ms': round(p95_pulsar, 2),
        'p95_iceberg_ms': round(p95_iceberg, 2),
        'spike_frequency_pct': round(spike_frequency, 2),
        'root_causes': ' | '.join(root_causes)
    })

bottleneck_df = pd.DataFrame(bottleneck_analysis_list)

print("\nBOTTLENECK ANALYSIS BY VESSEL TYPE")
print("=" * 80)
print(bottleneck_df.to_string(index=False))

# Calculate optimization priorities
optimization_priorities = []

for _, row in bottleneck_df.iterrows():
    vessel_type = row['vessel_type']
    vessel_data = cdc_lag_df[cdc_lag_df['vessel_type'] == vessel_type]
    p99_total = np.percentile(vessel_data['total_lag_ms'], 99)
    
    if p99_total > 600:
        priority = "CRITICAL"
        impact = "High lag affecting real-time operations"
    elif p99_total > 400:
        priority = "HIGH"
        impact = "Moderate lag degrading user experience"
    else:
        priority = "MEDIUM"
        impact = "Acceptable lag with room for optimization"
    
    optimization_priorities.append({
        'vessel_type': vessel_type,
        'priority': priority,
        'p99_lag_ms': round(p99_total, 2),
        'impact': impact,
        'primary_bottleneck': row['primary_bottleneck']
    })

optimization_df = pd.DataFrame(optimization_priorities)

print("\n\nOPTIMIZATION PRIORITIES")
print("=" * 80)
print(optimization_df.to_string(index=False))

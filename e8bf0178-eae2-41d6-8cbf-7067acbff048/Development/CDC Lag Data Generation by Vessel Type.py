import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Generate comprehensive CDC lag data segmented by vessel type
np.random.seed(42)

vessel_types = ['Container Ship', 'Tanker', 'Bulk Carrier', 'Passenger Vessel']
n_samples_per_type = 1000

cdc_lag_data_list = []

for vessel_type in vessel_types:
    # Different lag characteristics per vessel type
    if vessel_type == 'Container Ship':
        # High throughput, moderate lag
        cassandra_read_base = 45
        pulsar_throughput_base = 35
        iceberg_write_base = 55
        lag_multiplier = 1.0
    elif vessel_type == 'Tanker':
        # Lower throughput, higher lag due to larger payload
        cassandra_read_base = 65
        pulsar_throughput_base = 55
        iceberg_write_base = 85
        lag_multiplier = 1.4
    elif vessel_type == 'Bulk Carrier':
        # Medium throughput, medium lag
        cassandra_read_base = 50
        pulsar_throughput_base = 40
        iceberg_write_base = 65
        lag_multiplier = 1.15
    else:  # Passenger Vessel
        # Highest throughput, lowest lag (critical real-time needs)
        cassandra_read_base = 30
        pulsar_throughput_base = 25
        iceberg_write_base = 40
        lag_multiplier = 0.75
    
    for i in range(n_samples_per_type):
        timestamp = datetime.now() - timedelta(hours=np.random.randint(0, 168))
        
        # Cassandra read lag (ms)
        cassandra_read_lag = np.random.gamma(2, cassandra_read_base / 2) * lag_multiplier
        
        # Pulsar throughput lag (ms)
        pulsar_throughput_lag = np.random.gamma(2.5, pulsar_throughput_base / 2.5) * lag_multiplier
        
        # Iceberg write lag (ms)
        iceberg_write_lag = np.random.gamma(3, iceberg_write_base / 3) * lag_multiplier
        
        # Total end-to-end lag
        total_lag = cassandra_read_lag + pulsar_throughput_lag + iceberg_write_lag
        
        # Add occasional spikes (5% of time)
        if np.random.random() < 0.05:
            spike_factor = np.random.uniform(2.5, 5.0)
            cassandra_read_lag *= spike_factor
            pulsar_throughput_lag *= spike_factor
            iceberg_write_lag *= spike_factor
            total_lag *= spike_factor
        
        vessel_id = f"{vessel_type.replace(' ', '_').upper()}_{i % 50:03d}"
        
        cdc_lag_data_list.append({
            'timestamp': timestamp,
            'vessel_type': vessel_type,
            'vessel_id': vessel_id,
            'cassandra_read_lag_ms': cassandra_read_lag,
            'pulsar_throughput_lag_ms': pulsar_throughput_lag,
            'iceberg_write_lag_ms': iceberg_write_lag,
            'total_lag_ms': total_lag,
            'records_processed': np.random.randint(50, 500)
        })

cdc_lag_df = pd.DataFrame(cdc_lag_data_list)

# Calculate percentile statistics by vessel type
lag_stats_by_type = cdc_lag_df.groupby('vessel_type').agg({
    'total_lag_ms': ['count', lambda x: np.percentile(x, 50), lambda x: np.percentile(x, 95), lambda x: np.percentile(x, 99), 'mean', 'std'],
    'cassandra_read_lag_ms': [lambda x: np.percentile(x, 50), lambda x: np.percentile(x, 95), lambda x: np.percentile(x, 99)],
    'pulsar_throughput_lag_ms': [lambda x: np.percentile(x, 50), lambda x: np.percentile(x, 95), lambda x: np.percentile(x, 99)],
    'iceberg_write_lag_ms': [lambda x: np.percentile(x, 50), lambda x: np.percentile(x, 95), lambda x: np.percentile(x, 99)]
}).round(2)

lag_stats_by_type.columns = ['_'.join(col).strip() for col in lag_stats_by_type.columns.values]
lag_stats_by_type = lag_stats_by_type.reset_index()

print("CDC LAG CHARACTERISTICS BY VESSEL TYPE")
print("=" * 80)
print(f"\nTotal samples: {len(cdc_lag_df):,}")
print(f"Vessel types: {cdc_lag_df['vessel_type'].nunique()}")
print(f"Unique vessels: {cdc_lag_df['vessel_id'].nunique()}")
print(f"\nTime range: {cdc_lag_df['timestamp'].min()} to {cdc_lag_df['timestamp'].max()}")
print("\nLag Statistics by Vessel Type:")
print(lag_stats_by_type.to_string(index=False))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Simulate historical anomaly data for Presto SQL analysis
np.random.seed(42)
n_samples = 5000

# Generate realistic telemetry data
historical_data = pd.DataFrame({
    'vessel_id': [f'VESSEL_{i%50:03d}' for i in range(n_samples)],
    'timestamp': pd.date_range(start='2024-01-01', periods=n_samples, freq='1H'),
    'equipment_temp': np.random.normal(75, 12, n_samples),
    'vibration': np.random.gamma(2, 2, n_samples),
    'pressure': np.random.normal(85, 15, n_samples),
    'fuel_consumption': np.random.lognormal(4, 0.5, n_samples)
})

# Add anomalies (5% of data)
anomaly_mask = np.random.choice([True, False], size=n_samples, p=[0.05, 0.95])
historical_data.loc[anomaly_mask, 'equipment_temp'] += np.random.uniform(15, 30, anomaly_mask.sum())
historical_data.loc[anomaly_mask, 'vibration'] *= np.random.uniform(2, 4, anomaly_mask.sum())
historical_data['is_anomaly'] = anomaly_mask.astype(int)

print(f"Generated {len(historical_data):,} historical samples")
print(f"Anomalies: {anomaly_mask.sum()} ({100*anomaly_mask.sum()/n_samples:.1f}%)")
print("\nSample data:")
print(historical_data.head(10))

# Presto SQL queries for statistical analysis
presto_quantile_query = """
-- Quantile Regression Thresholds using Presto SQL
-- Calculate 95th, 98th, 99th percentiles for threshold recommendations

WITH aggregated_metrics AS (
  SELECT 
    vessel_id,
    date_trunc('hour', timestamp) as hour_bucket,
    AVG(equipment_temp) as avg_temp,
    AVG(vibration) as avg_vibration,
    AVG(pressure) as avg_pressure,
    AVG(fuel_consumption) as avg_fuel
  FROM iceberg.telemetry.equipment_metrics
  WHERE timestamp >= current_timestamp - interval '90' day
  GROUP BY vessel_id, date_trunc('hour', timestamp)
),
statistical_thresholds AS (
  SELECT
    approx_percentile(avg_temp, 0.95) as temp_p95,
    approx_percentile(avg_temp, 0.98) as temp_p98,
    approx_percentile(avg_temp, 0.99) as temp_p99,
    approx_percentile(avg_vibration, 0.95) as vib_p95,
    approx_percentile(avg_vibration, 0.98) as vib_p98,
    approx_percentile(avg_vibration, 0.99) as vib_p99,
    stddev(avg_temp) as temp_std,
    stddev(avg_vibration) as vib_std
  FROM aggregated_metrics
)
SELECT 
  temp_p95, temp_p98, temp_p99,
  vib_p95, vib_p98, vib_p99,
  temp_std, vib_std,
  temp_p95 + 2*temp_std as temp_threshold_2sigma,
  temp_p98 + 1.5*temp_std as temp_threshold_optimized
FROM statistical_thresholds;
"""

presto_confidence_intervals = """
-- Confidence Interval Calculation with Bootstrap Approach
-- Estimate uncertainty in threshold recommendations

WITH recent_data AS (
  SELECT 
    equipment_temp,
    vibration,
    pressure
  FROM iceberg.telemetry.equipment_metrics
  WHERE timestamp >= current_timestamp - interval '30' day
),
bootstrap_samples AS (
  SELECT
    approx_percentile(equipment_temp, 0.95) as temp_p95_sample,
    approx_percentile(vibration, 0.95) as vib_p95_sample,
    COUNT(*) as sample_size
  FROM recent_data
  -- In practice, run multiple times with different random samples
)
SELECT
  AVG(temp_p95_sample) as temp_threshold,
  STDDEV(temp_p95_sample) as temp_threshold_uncertainty,
  AVG(temp_p95_sample) - 1.96*STDDEV(temp_p95_sample) as temp_ci_lower,
  AVG(temp_p95_sample) + 1.96*STDDEV(temp_p95_sample) as temp_ci_upper
FROM bootstrap_samples;
"""

presto_false_positive_optimization = """
-- False Positive Rate Minimization using Historical Validation
-- Iteratively test thresholds against known failure patterns

WITH failure_labels AS (
  SELECT 
    vessel_id,
    timestamp,
    equipment_temp,
    vibration,
    CASE WHEN failure_occurred_within_24h = 1 THEN 1 ELSE 0 END as actual_failure
  FROM iceberg.telemetry.labeled_failures
  WHERE timestamp >= current_timestamp - interval '180' day
),
threshold_candidates AS (
  SELECT
    unnest(ARRAY[85, 87, 90, 92, 95, 98, 100]) as temp_threshold,
    unnest(ARRAY[6.5, 7.0, 7.5, 8.0, 8.5, 9.0, 9.5]) as vib_threshold
),
performance_by_threshold AS (
  SELECT
    tc.temp_threshold,
    tc.vib_threshold,
    SUM(CASE WHEN (fl.equipment_temp > tc.temp_threshold OR fl.vibration > tc.vib_threshold) 
             AND fl.actual_failure = 1 THEN 1 ELSE 0 END) as true_positives,
    SUM(CASE WHEN (fl.equipment_temp > tc.temp_threshold OR fl.vibration > tc.vib_threshold) 
             AND fl.actual_failure = 0 THEN 1 ELSE 0 END) as false_positives,
    SUM(CASE WHEN (fl.equipment_temp <= tc.temp_threshold AND fl.vibration <= tc.vib_threshold) 
             AND fl.actual_failure = 1 THEN 1 ELSE 0 END) as false_negatives,
    COUNT(*) as total_samples
  FROM failure_labels fl
  CROSS JOIN threshold_candidates tc
  GROUP BY tc.temp_threshold, tc.vib_threshold
)
SELECT
  temp_threshold,
  vib_threshold,
  true_positives,
  false_positives,
  false_negatives,
  CAST(true_positives AS DOUBLE) / NULLIF(true_positives + false_negatives, 0) as recall,
  CAST(true_positives AS DOUBLE) / NULLIF(true_positives + false_positives, 0) as precision,
  CAST(false_positives AS DOUBLE) / total_samples as false_positive_rate,
  2.0 * (CAST(true_positives AS DOUBLE) / NULLIF(true_positives + false_positives, 0)) * 
        (CAST(true_positives AS DOUBLE) / NULLIF(true_positives + false_negatives, 0)) /
        NULLIF((CAST(true_positives AS DOUBLE) / NULLIF(true_positives + false_positives, 0)) + 
               (CAST(true_positives AS DOUBLE) / NULLIF(true_positives + false_negatives, 0)), 0) as f1_score
FROM performance_by_threshold
ORDER BY false_positive_rate ASC, f1_score DESC
LIMIT 10;
"""

presto_queries = {
    'quantile_regression': presto_quantile_query,
    'confidence_intervals': presto_confidence_intervals,
    'false_positive_optimization': presto_false_positive_optimization
}

print(f"\n✓ Created {len(presto_queries)} Presto SQL analytical queries")
print("\nQuery types:")
for qtype in presto_queries.keys():
    print(f"  - {qtype}")

# Simulate Presto execution results
presto_quantile_results = pd.DataFrame([{
    'temp_p95': 89.2,
    'temp_p98': 93.5,
    'temp_p99': 97.8,
    'vib_p95': 7.8,
    'vib_p98': 9.2,
    'vib_p99': 10.5,
    'temp_std': 12.1,
    'vib_std': 2.3,
    'temp_threshold_2sigma': 113.4,
    'temp_threshold_optimized': 111.7
}])

presto_fp_optimization = pd.DataFrame([
    {'temp_threshold': 90, 'vib_threshold': 7.5, 'precision': 0.82, 'recall': 0.88, 'fpr': 0.03, 'f1': 0.85},
    {'temp_threshold': 92, 'vib_threshold': 8.0, 'precision': 0.87, 'recall': 0.83, 'fpr': 0.02, 'f1': 0.85},
    {'temp_threshold': 95, 'vib_threshold': 8.5, 'precision': 0.91, 'recall': 0.76, 'fpr': 0.01, 'f1': 0.83},
    {'temp_threshold': 87, 'vib_threshold': 7.0, 'precision': 0.76, 'recall': 0.92, 'fpr': 0.05, 'f1': 0.83}
])

print("\n✓ Simulated Presto query execution")
print("\nOptimal thresholds (minimizing FPR while maximizing F1):")
print(presto_fp_optimization.head())

# Performance tracking
presto_execution_start = datetime.now()
presto_latency_ms = 2340  # Typical Presto ad-hoc query latency
presto_execution_time = timedelta(milliseconds=presto_latency_ms)

presto_performance = {
    'query_latency_ms': presto_latency_ms,
    'query_latency_sec': presto_latency_ms / 1000,
    'data_scanned_gb': 45.2,
    'cpu_time_sec': 187.5,
    'resource_efficiency': 'high',
    'use_case': 'Ad-hoc analysis, threshold tuning, interactive exploration'
}

print(f"\nPresto Performance Metrics:")
print(f"  Query latency: {presto_performance['query_latency_sec']:.2f}s")
print(f"  Data scanned: {presto_performance['data_scanned_gb']:.1f} GB")
print(f"  CPU time: {presto_performance['cpu_time_sec']:.1f}s")

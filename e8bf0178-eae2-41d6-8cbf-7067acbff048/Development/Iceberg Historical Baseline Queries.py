import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Simulated historical anomaly data from Iceberg
# In production: queries Iceberg tables via Presto/Spark

np.random.seed(100)

# Generate historical baseline data (30d, 90d, 1y windows)
def generate_historical_baseline(window_days, base_rate):
    """Generate historical anomaly frequency and severity"""
    n_records = window_days * 24  # hourly aggregates
    dates = pd.date_range(end=datetime.now() - timedelta(days=1), periods=n_records, freq='1H')
    
    return pd.DataFrame({
        'hour': dates,
        'anomaly_count': np.random.poisson(base_rate, n_records),
        'avg_severity': np.random.uniform(0.5, 0.9, n_records),
        'max_severity': np.random.uniform(0.7, 0.98, n_records),
        'vessels_affected': np.random.randint(1, 11, n_records)
    })

hist_30d = generate_historical_baseline(30, 2.5)
hist_90d = generate_historical_baseline(90, 2.3)
hist_365d = generate_historical_baseline(365, 2.0)

# Presto queries for Iceberg historical data
presto_30d_query = """
-- Historical Anomaly Baseline: 30-day window
SELECT 
    date_trunc('hour', timestamp) as hour,
    COUNT(*) as anomaly_count,
    AVG(anomaly_score) as avg_severity,
    MAX(anomaly_score) as max_severity,
    COUNT(DISTINCT vessel_id) as vessels_affected
FROM iceberg.fleet_analytics.anomaly_history
WHERE timestamp >= CURRENT_DATE - INTERVAL '30' DAY
  AND confidence > 0.7
GROUP BY date_trunc('hour', timestamp)
ORDER BY hour DESC;
"""

presto_90d_query = """
-- Historical Anomaly Baseline: 90-day window
SELECT 
    date_trunc('day', timestamp) as day,
    COUNT(*) as anomaly_count,
    AVG(anomaly_score) as avg_severity,
    MAX(anomaly_score) as max_severity,
    COUNT(DISTINCT vessel_id) as vessels_affected,
    COUNT(DISTINCT feature_name) as features_affected
FROM iceberg.fleet_analytics.anomaly_history
WHERE timestamp >= CURRENT_DATE - INTERVAL '90' DAY
  AND confidence > 0.7
GROUP BY date_trunc('day', timestamp)
ORDER BY day DESC;
"""

presto_1y_query = """
-- Historical Anomaly Baseline: 1-year window  
SELECT 
    date_trunc('week', timestamp) as week,
    COUNT(*) as anomaly_count,
    AVG(anomaly_score) as avg_severity,
    MAX(anomaly_score) as max_severity,
    COUNT(DISTINCT vessel_id) as vessels_affected,
    feature_name,
    COUNT(*) as feature_count
FROM iceberg.fleet_analytics.anomaly_history
WHERE timestamp >= CURRENT_DATE - INTERVAL '365' DAY
  AND confidence > 0.7
GROUP BY date_trunc('week', timestamp), feature_name
ORDER BY week DESC, feature_count DESC;
"""

print("=" * 80)
print("Iceberg Historical Baseline Analysis")
print("=" * 80)

print("\n30-Day Baseline Summary:")
print(f"  Total hourly periods: {len(hist_30d)}")
print(f"  Avg anomalies/hour: {hist_30d['anomaly_count'].mean():.2f}")
print(f"  Avg severity: {hist_30d['avg_severity'].mean():.3f}")
print(f"  Avg vessels affected/hour: {hist_30d['vessels_affected'].mean():.1f}")

print("\n90-Day Baseline Summary:")
print(f"  Total hourly periods: {len(hist_90d)}")
print(f"  Avg anomalies/hour: {hist_90d['anomaly_count'].mean():.2f}")
print(f"  Avg severity: {hist_90d['avg_severity'].mean():.3f}")
print(f"  Avg vessels affected/hour: {hist_90d['vessels_affected'].mean():.1f}")

print("\n1-Year Baseline Summary:")
print(f"  Total hourly periods: {len(hist_365d)}")
print(f"  Avg anomalies/hour: {hist_365d['anomaly_count'].mean():.2f}")
print(f"  Avg severity: {hist_365d['avg_severity'].mean():.3f}")
print(f"  Avg vessels affected/hour: {hist_365d['vessels_affected'].mean():.1f}")

# Aggregated baseline metrics
baseline_metrics = pd.DataFrame({
    'Window': ['30 days', '90 days', '1 year'],
    'Avg_Anomalies_Per_Hour': [
        hist_30d['anomaly_count'].mean(),
        hist_90d['anomaly_count'].mean(),
        hist_365d['anomaly_count'].mean()
    ],
    'Avg_Severity': [
        hist_30d['avg_severity'].mean(),
        hist_90d['avg_severity'].mean(),
        hist_365d['avg_severity'].mean()
    ],
    'P95_Severity': [
        hist_30d['max_severity'].quantile(0.95),
        hist_90d['max_severity'].quantile(0.95),
        hist_365d['max_severity'].quantile(0.95)
    ]
})

print("\n\nAggregated Baseline Metrics:")
print(baseline_metrics.to_string(index=False))

print("\n\nExample Presto Query (30-day):")
print(presto_30d_query)
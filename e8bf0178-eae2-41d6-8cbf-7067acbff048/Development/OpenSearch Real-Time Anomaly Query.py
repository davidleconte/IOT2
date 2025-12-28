import pandas as pd
from datetime import datetime, timedelta
import numpy as np

# Simulated OpenSearch real-time anomaly data (last 24h)
# In production, this would query OpenSearch AD results indices
# Query: GET opensearch_ad_results/_search with date range filter

np.random.seed(42)
n_anomalies = 48

realtime_anomalies = pd.DataFrame({
    'timestamp': pd.date_range(end=datetime.now(), periods=n_anomalies, freq='30min'),
    'vessel_id': ['V' + str(i % 10 + 1) for i in range(n_anomalies)],
    'anomaly_score': np.random.uniform(0.71, 0.95, n_anomalies),
    'feature_name': [['temperature', 'vibration', 'pressure', 'rpm', 'fuel_consumption'][i % 5] for i in range(n_anomalies)],
    'anomaly_grade': np.random.uniform(0.70, 0.98, n_anomalies),
    'confidence': np.random.uniform(0.75, 0.98, n_anomalies)
})

# Filter by confidence > 0.7
realtime_anomalies_filtered = realtime_anomalies[realtime_anomalies['confidence'] > 0.7].copy()

# OpenSearch query example (Presto/SQL syntax)
opensearch_query = """
-- OpenSearch Real-Time Anomaly Detection Query (Last 24h)
SELECT 
    timestamp,
    vessel_id,
    anomaly_score,
    feature_name,
    anomaly_grade,
    confidence
FROM opensearch.ad_results.detector_results
WHERE timestamp >= NOW() - INTERVAL '24' HOUR
  AND confidence > 0.7
ORDER BY timestamp DESC, anomaly_score DESC
LIMIT 1000;
"""

print("=" * 80)
print("OpenSearch Real-Time Anomaly Query (Last 24h, Confidence > 0.7)")
print("=" * 80)
print(f"\nTotal anomalies detected: {len(realtime_anomalies_filtered)}")
print(f"Time range: {realtime_anomalies_filtered['timestamp'].min()} to {realtime_anomalies_filtered['timestamp'].max()}")
print(f"\nTop 10 anomalies by score:")
print(realtime_anomalies_filtered.nlargest(10, 'anomaly_score')[['timestamp', 'vessel_id', 'feature_name', 'anomaly_score', 'confidence']])

print("\n\nSummary by Feature:")
feature_summary = realtime_anomalies_filtered.groupby('feature_name').agg({
    'anomaly_score': ['count', 'mean', 'max'],
    'confidence': 'mean'
}).round(3)
print(feature_summary)

print("\n\nOpenSearch Presto Query:")
print(opensearch_query)
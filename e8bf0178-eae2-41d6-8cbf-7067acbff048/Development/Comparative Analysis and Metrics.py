import pandas as pd
import numpy as np

# Comparative analysis between real-time and historical patterns
# Simulates actual failure data to calculate false positive rate

np.random.seed(200)

# Simulate actual failure events (ground truth) for last 24h
# In reality, this would come from maintenance logs, failure reports
actual_failures = pd.DataFrame({
    'timestamp': pd.date_range(end=pd.Timestamp.now(), periods=12, freq='2H'),
    'vessel_id': ['V3', 'V7', 'V2', 'V5', 'V9', 'V3', 'V1', 'V8', 'V4', 'V6', 'V10', 'V2'],
    'failure_type': ['engine_overheating', 'bearing_failure', 'fuel_contamination', 
                     'sensor_malfunction', 'hydraulic_leak', 'engine_overheating',
                     'electrical_fault', 'bearing_failure', 'cooling_failure',
                     'fuel_pump_failure', 'transmission_fault', 'fuel_contamination'],
    'severity': ['high', 'critical', 'medium', 'low', 'high', 'critical', 
                 'medium', 'high', 'critical', 'medium', 'high', 'medium']
})

# Match anomalies to actual failures (simplified)
# True Positives: anomalies that preceded actual failures within 4 hours
# False Positives: anomalies with no corresponding failure
# False Negatives: failures with no preceding anomaly alert

# Simplified matching logic
rt_anomaly_vessels = set(realtime_anomalies_filtered['vessel_id'].unique())
failure_vessels = set(actual_failures['vessel_id'].unique())

true_positive_vessels = rt_anomaly_vessels.intersection(failure_vessels)
false_positive_vessels = rt_anomaly_vessels - failure_vessels
false_negative_vessels = failure_vessels - rt_anomaly_vessels

tp_count = len(realtime_anomalies_filtered[realtime_anomalies_filtered['vessel_id'].isin(true_positive_vessels)])
fp_count = len(realtime_anomalies_filtered[realtime_anomalies_filtered['vessel_id'].isin(false_positive_vessels)])
fn_count = len(actual_failures[actual_failures['vessel_id'].isin(false_negative_vessels)])
tn_count = 10  # vessels with no anomalies and no failures (fleet baseline)

# Calculate metrics
precision = tp_count / (tp_count + fp_count) if (tp_count + fp_count) > 0 else 0
recall = tp_count / (tp_count + fn_count) if (tp_count + fn_count) > 0 else 0
f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
false_positive_rate = fp_count / (fp_count + tn_count) if (fp_count + tn_count) > 0 else 0

print("=" * 80)
print("Comparative Analysis: Real-Time vs Historical Anomaly Patterns")
print("=" * 80)

print("\n1. ANOMALY DETECTION PERFORMANCE METRICS")
print("-" * 80)
print(f"True Positives (TP):  {tp_count:3d}  - Anomalies correctly predicting failures")
print(f"False Positives (FP): {fp_count:3d}  - Anomalies with no actual failures")
print(f"False Negatives (FN): {fn_count:3d}  - Failures not predicted by anomalies")
print(f"True Negatives (TN):  {tn_count:3d}  - Normal operation correctly identified")
print()
print(f"Precision:             {precision:.3f} - Of all alerts, what % were real failures")
print(f"Recall (Sensitivity):  {recall:.3f} - Of all failures, what % were detected")
print(f"F1 Score:              {f1_score:.3f} - Harmonic mean of precision & recall")
print(f"False Positive Rate:   {false_positive_rate:.3f} - Of normal ops, what % flagged incorrectly")

# Compare real-time severity vs historical baseline
rt_avg_severity = realtime_anomalies_filtered['anomaly_score'].mean()
hist_30d_avg = hist_30d['avg_severity'].mean()

severity_deviation = ((rt_avg_severity - hist_30d_avg) / hist_30d_avg) * 100

print("\n\n2. SEVERITY COMPARISON: REAL-TIME vs HISTORICAL")
print("-" * 80)
print(f"Real-time avg severity (24h):    {rt_avg_severity:.3f}")
print(f"Historical avg severity (30d):   {hist_30d_avg:.3f}")
print(f"Deviation from baseline:         {severity_deviation:+.1f}%")

if severity_deviation > 15:
    print("⚠️  ALERT: Significantly elevated anomaly severity detected")
elif severity_deviation > 5:
    print("⚡ WARNING: Moderate increase in anomaly severity")
else:
    print("✓  NORMAL: Severity within historical norms")

# Frequency comparison
rt_freq = len(realtime_anomalies_filtered) / 24  # anomalies per hour (24h window)
hist_freq = hist_30d['anomaly_count'].mean()  # historical avg per hour

freq_deviation = ((rt_freq - hist_freq) / hist_freq) * 100

print("\n\n3. FREQUENCY COMPARISON: REAL-TIME vs HISTORICAL")
print("-" * 80)
print(f"Real-time frequency (24h):       {rt_freq:.2f} anomalies/hour")
print(f"Historical frequency (30d avg):  {hist_freq:.2f} anomalies/hour")
print(f"Deviation from baseline:         {freq_deviation:+.1f}%")

# Seasonality detection (simplified - would use time series decomposition in production)
# Check if current hour-of-day pattern matches historical
current_hour = pd.Timestamp.now().hour
hist_same_hour = hist_30d[hist_30d['hour'].dt.hour == current_hour]
seasonality_factor = hist_same_hour['anomaly_count'].mean()

print("\n\n4. SEASONALITY ANALYSIS")
print("-" * 80)
print(f"Current hour of day:             {current_hour}:00")
print(f"Historical avg at this hour:     {seasonality_factor:.2f} anomalies/hour")
print(f"Expected seasonal variation:     ±15% from daily average")

# Summary metrics table
comparison_summary = pd.DataFrame({
    'Metric': ['Precision', 'Recall', 'F1 Score', 'False Positive Rate', 
               'Severity Deviation', 'Frequency Deviation'],
    'Value': [f'{precision:.3f}', f'{recall:.3f}', f'{f1_score:.3f}', 
              f'{false_positive_rate:.3f}', f'{severity_deviation:+.1f}%', 
              f'{freq_deviation:+.1f}%'],
    'Target': ['> 0.70', '> 0.80', '> 0.75', '< 0.20', '±10%', '±15%'],
    'Status': [
        '✓' if precision > 0.70 else '✗',
        '✓' if recall > 0.80 else '✗',
        '✓' if f1_score > 0.75 else '✗',
        '✓' if false_positive_rate < 0.20 else '✗',
        '✓' if abs(severity_deviation) <= 10 else '✗',
        '✓' if abs(freq_deviation) <= 15 else '✗'
    ]
})

print("\n\n5. PERFORMANCE SUMMARY")
print("-" * 80)
print(comparison_summary.to_string(index=False))

# Presto federated query example
federated_query = """
-- Presto Federated Query: Join Real-Time + Historical
WITH realtime AS (
    SELECT vessel_id, feature_name, anomaly_score, timestamp
    FROM opensearch.ad_results.detector_results
    WHERE timestamp >= NOW() - INTERVAL '24' HOUR
      AND confidence > 0.7
),
historical AS (
    SELECT vessel_id, feature_name,
           AVG(anomaly_score) as hist_avg_score,
           STDDEV(anomaly_score) as hist_stddev,
           COUNT(*) as hist_occurrence_count
    FROM iceberg.fleet_analytics.anomaly_history
    WHERE timestamp >= CURRENT_DATE - INTERVAL '30' DAY
    GROUP BY vessel_id, feature_name
)
SELECT 
    rt.vessel_id,
    rt.feature_name,
    rt.anomaly_score as current_score,
    h.hist_avg_score,
    (rt.anomaly_score - h.hist_avg_score) / h.hist_stddev as z_score,
    h.hist_occurrence_count,
    CASE 
        WHEN (rt.anomaly_score - h.hist_avg_score) / h.hist_stddev > 2 
        THEN 'persistent'
        ELSE 'transient'
    END as anomaly_classification
FROM realtime rt
LEFT JOIN historical h 
    ON rt.vessel_id = h.vessel_id 
    AND rt.feature_name = h.feature_name
ORDER BY z_score DESC;
"""

print("\n\n6. FEDERATED QUERY EXAMPLE")
print("-" * 80)
print(federated_query)
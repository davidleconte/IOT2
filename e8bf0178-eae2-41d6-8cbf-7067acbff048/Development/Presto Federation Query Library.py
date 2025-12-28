import pandas as pd
from datetime import datetime, timedelta

# Presto Federation Query Library for Real-time OpenSearch vs Historical Iceberg Analysis
# Target: <10s execution, optimized with predicate pushdown and partition pruning

# ============================================================================
# QUERY CATEGORY 1: REAL-TIME ANOMALY RETRIEVAL (<100ms latency)
# ============================================================================

q1_realtime_anomalies = """
-- Q1: Real-time Anomaly Detection from OpenSearch (<100ms target)
-- Retrieves anomalies detected in the last hour with severity filtering
SELECT 
    timestamp,
    vessel_id,
    anomaly_score,
    confidence_level,
    feature_name,
    feature_value,
    alert_type
FROM opensearch.anomaly_detection.alerts
WHERE timestamp >= current_timestamp - INTERVAL '1' HOUR
  AND confidence_level >= 0.7
  AND anomaly_score >= 0.7
ORDER BY anomaly_score DESC, timestamp DESC
LIMIT 1000;

-- Optimization: Index on (timestamp, confidence_level, anomaly_score)
-- Predicate pushdown to OpenSearch reduces data transfer
"""

q2_vessel_specific_alerts = """
-- Q2: Vessel-Specific Alert History (Real-time)
-- Get all recent alerts for a specific vessel with feature details
SELECT 
    timestamp,
    vessel_id,
    anomaly_score,
    confidence_level,
    feature_name,
    feature_value,
    alert_type,
    equipment_id,
    sensor_reading
FROM opensearch.anomaly_detection.alerts
WHERE vessel_id = 'VESSEL_001'
  AND timestamp >= current_timestamp - INTERVAL '24' HOUR
ORDER BY timestamp DESC;

-- Optimization: Partition by vessel_id in OpenSearch
-- Use vessel_id filter for partition pruning
"""

q3_critical_equipment_alerts = """
-- Q3: Critical Equipment Alerts Across Fleet
-- Monitor high-severity equipment failures in real-time
SELECT 
    vessel_id,
    equipment_id,
    equipment_type,
    MAX(anomaly_score) as max_severity,
    COUNT(*) as alert_count,
    MAX(timestamp) as latest_alert,
    ARRAY_AGG(DISTINCT feature_name) as affected_features
FROM opensearch.anomaly_detection.alerts
WHERE timestamp >= current_timestamp - INTERVAL '15' MINUTE
  AND equipment_type IN ('engine', 'propulsion', 'navigation')
  AND anomaly_score >= 0.8
GROUP BY vessel_id, equipment_id, equipment_type
HAVING COUNT(*) >= 3
ORDER BY max_severity DESC;

-- Optimization: Equipment type index, temporal partition
-- Filter early to minimize aggregation overhead
"""

# ============================================================================
# QUERY CATEGORY 2: HISTORICAL BASELINE AGGREGATIONS (30d/90d/365d)
# ============================================================================

q4_30d_baseline = """
-- Q4: 30-Day Historical Baseline Aggregation
-- Calculate statistical baselines for comparison with real-time data
SELECT 
    vessel_id,
    feature_name,
    COUNT(*) as event_count,
    AVG(anomaly_score) as avg_score,
    STDDEV(anomaly_score) as stddev_score,
    APPROX_PERCENTILE(anomaly_score, 0.5) as median_score,
    APPROX_PERCENTILE(anomaly_score, 0.95) as p95_score,
    APPROX_PERCENTILE(anomaly_score, 0.99) as p99_score
FROM iceberg.fleet_telemetry.anomaly_history
WHERE date_partition >= current_date - INTERVAL '30' DAY
  AND date_partition < current_date
GROUP BY vessel_id, feature_name;

-- Optimization: Partition pruning on date_partition (30 partitions)
-- Pre-aggregated daily summaries reduce scan volume
"""

q5_90d_trend_analysis = """
-- Q5: 90-Day Trend Analysis with Weekly Aggregation
-- Identify long-term trends and seasonal patterns
SELECT 
    vessel_id,
    DATE_TRUNC('week', timestamp) as week_start,
    feature_name,
    COUNT(*) as weekly_events,
    AVG(anomaly_score) as avg_weekly_score,
    MAX(anomaly_score) as max_weekly_score,
    COUNT(DISTINCT DATE_TRUNC('day', timestamp)) as active_days
FROM iceberg.fleet_telemetry.anomaly_history
WHERE date_partition >= current_date - INTERVAL '90' DAY
  AND date_partition < current_date
GROUP BY vessel_id, DATE_TRUNC('week', timestamp), feature_name
ORDER BY vessel_id, week_start;

-- Optimization: 90 partition scan with date_partition predicate
-- Weekly aggregation reduces output by ~13x
"""

q6_365d_annual_baseline = """
-- Q6: 365-Day Annual Baseline for Long-Term Context
-- Full year statistics for rare event detection
SELECT 
    vessel_id,
    equipment_type,
    feature_name,
    COUNT(*) as annual_event_count,
    AVG(anomaly_score) as annual_avg_score,
    STDDEV(anomaly_score) as annual_stddev,
    APPROX_PERCENTILE(anomaly_score, 0.99) as annual_p99,
    MIN(timestamp) as first_event,
    MAX(timestamp) as last_event,
    COUNT(DISTINCT date_partition) as active_days
FROM iceberg.fleet_telemetry.anomaly_history
WHERE date_partition >= current_date - INTERVAL '365' DAY
  AND date_partition < current_date
GROUP BY vessel_id, equipment_type, feature_name
HAVING COUNT(*) >= 10;

-- Optimization: Full year scan (365 partitions)
-- Filter low-frequency events with HAVING clause
-- Use columnar format for efficient aggregation
"""

# ============================================================================
# QUERY CATEGORY 3: FEDERATION JOIN QUERIES (Real-time + Historical)
# ============================================================================

q7_federated_context_enrichment = """
-- Q7: Real-time Alerts Enriched with Historical Context
-- Join live alerts with 30-day baselines for anomaly severity assessment
WITH realtime_alerts AS (
    SELECT 
        timestamp,
        vessel_id,
        feature_name,
        anomaly_score,
        confidence_level
    FROM opensearch.anomaly_detection.alerts
    WHERE timestamp >= current_timestamp - INTERVAL '1' HOUR
      AND confidence_level >= 0.7
),
historical_baseline AS (
    SELECT 
        vessel_id,
        feature_name,
        AVG(anomaly_score) as hist_avg_score,
        STDDEV(anomaly_score) as hist_stddev,
        APPROX_PERCENTILE(anomaly_score, 0.95) as hist_p95
    FROM iceberg.fleet_telemetry.anomaly_history
    WHERE date_partition >= current_date - INTERVAL '30' DAY
      AND date_partition < current_date
    GROUP BY vessel_id, feature_name
)
SELECT 
    rt.timestamp,
    rt.vessel_id,
    rt.feature_name,
    rt.anomaly_score,
    rt.confidence_level,
    hb.hist_avg_score,
    hb.hist_stddev,
    hb.hist_p95,
    (rt.anomaly_score - hb.hist_avg_score) / NULLIF(hb.hist_stddev, 0) as z_score,
    CASE 
        WHEN rt.anomaly_score > hb.hist_p95 THEN 'SEVERE'
        WHEN rt.anomaly_score > hb.hist_avg_score + hb.hist_stddev THEN 'ELEVATED'
        ELSE 'NORMAL'
    END as severity_category
FROM realtime_alerts rt
LEFT JOIN historical_baseline hb
    ON rt.vessel_id = hb.vessel_id
    AND rt.feature_name = hb.feature_name
ORDER BY z_score DESC NULLS LAST;

-- Optimization: Small real-time result set (broadcast join)
-- Historical CTE uses partition pruning (30 partitions)
-- Target execution: <5s
"""

q8_temporal_pattern_comparison = """
-- Q8: Hour-of-Day Pattern Comparison (Real-time vs Historical)
-- Compare current hour alerts to same hour historically
WITH current_hour_alerts AS (
    SELECT 
        vessel_id,
        feature_name,
        COUNT(*) as current_count,
        AVG(anomaly_score) as current_avg_score
    FROM opensearch.anomaly_detection.alerts
    WHERE timestamp >= DATE_TRUNC('hour', current_timestamp)
      AND timestamp < DATE_TRUNC('hour', current_timestamp) + INTERVAL '1' HOUR
    GROUP BY vessel_id, feature_name
),
historical_same_hour AS (
    SELECT 
        vessel_id,
        feature_name,
        AVG(hourly_count) as hist_avg_count,
        STDDEV(hourly_count) as hist_stddev_count,
        AVG(hourly_avg_score) as hist_avg_score
    FROM (
        SELECT 
            vessel_id,
            feature_name,
            DATE_TRUNC('hour', timestamp) as hour_bucket,
            COUNT(*) as hourly_count,
            AVG(anomaly_score) as hourly_avg_score
        FROM iceberg.fleet_telemetry.anomaly_history
        WHERE date_partition >= current_date - INTERVAL '30' DAY
          AND date_partition < current_date
          AND EXTRACT(HOUR FROM timestamp) = EXTRACT(HOUR FROM current_timestamp)
        GROUP BY vessel_id, feature_name, DATE_TRUNC('hour', timestamp)
    ) hourly_stats
    GROUP BY vessel_id, feature_name
)
SELECT 
    ch.vessel_id,
    ch.feature_name,
    ch.current_count,
    ch.current_avg_score,
    hsh.hist_avg_count,
    hsh.hist_stddev_count,
    hsh.hist_avg_score,
    (ch.current_count - hsh.hist_avg_count) / NULLIF(hsh.hist_stddev_count, 0) as count_z_score,
    ch.current_avg_score - hsh.hist_avg_score as score_deviation
FROM current_hour_alerts ch
LEFT JOIN historical_same_hour hsh
    ON ch.vessel_id = hsh.vessel_id
    AND ch.feature_name = hsh.feature_name
WHERE ABS((ch.current_count - hsh.hist_avg_count) / NULLIF(hsh.hist_stddev_count, 0)) > 2.0
ORDER BY ABS(count_z_score) DESC;

-- Optimization: Filter by hour in historical query
-- Nested aggregation for hourly statistics
-- Only return statistically significant deviations
"""

# Store queries in structured format
presto_query_library = {
    "Q1_realtime_anomalies": {
        "query": q1_realtime_anomalies,
        "category": "Real-time Anomaly Retrieval",
        "target_latency_ms": 100,
        "data_source": "OpenSearch",
        "use_case": "Get all recent high-confidence anomalies"
    },
    "Q2_vessel_specific_alerts": {
        "query": q2_vessel_specific_alerts,
        "category": "Real-time Anomaly Retrieval", 
        "target_latency_ms": 100,
        "data_source": "OpenSearch",
        "use_case": "Track alerts for specific vessel"
    },
    "Q3_critical_equipment_alerts": {
        "query": q3_critical_equipment_alerts,
        "category": "Real-time Anomaly Retrieval",
        "target_latency_ms": 100,
        "data_source": "OpenSearch",
        "use_case": "Monitor critical equipment failures fleet-wide"
    },
    "Q4_30d_baseline": {
        "query": q4_30d_baseline,
        "category": "Historical Baseline Aggregation",
        "target_latency_ms": 3000,
        "data_source": "Iceberg",
        "use_case": "30-day statistical baseline for anomaly comparison"
    },
    "Q5_90d_trend_analysis": {
        "query": q5_90d_trend_analysis,
        "category": "Historical Baseline Aggregation",
        "target_latency_ms": 5000,
        "data_source": "Iceberg",
        "use_case": "90-day trend analysis with weekly aggregation"
    },
    "Q6_365d_annual_baseline": {
        "query": q6_365d_annual_baseline,
        "category": "Historical Baseline Aggregation",
        "target_latency_ms": 8000,
        "data_source": "Iceberg",
        "use_case": "365-day annual baseline for rare event detection"
    },
    "Q7_federated_context_enrichment": {
        "query": q7_federated_context_enrichment,
        "category": "Federation JOIN",
        "target_latency_ms": 5000,
        "data_source": "OpenSearch + Iceberg",
        "use_case": "Enrich real-time alerts with historical context"
    },
    "Q8_temporal_pattern_comparison": {
        "query": q8_temporal_pattern_comparison,
        "category": "Federation JOIN",
        "target_latency_ms": 6000,
        "data_source": "OpenSearch + Iceberg",
        "use_case": "Compare current hour to historical same-hour patterns"
    }
}

# Create summary DataFrame
query_summary = pd.DataFrame([
    {
        "Query ID": qid,
        "Category": details["category"],
        "Data Source": details["data_source"],
        "Target Latency (ms)": details["target_latency_ms"],
        "Use Case": details["use_case"]
    }
    for qid, details in presto_query_library.items()
])

print("=" * 80)
print("PRESTO FEDERATION QUERY LIBRARY - PART 1")
print("=" * 80)
print(f"\nTotal Queries Defined: {len(presto_query_library)}")
print(f"\nQuery Summary:\n")
print(query_summary.to_string(index=False))
print("\n" + "=" * 80)

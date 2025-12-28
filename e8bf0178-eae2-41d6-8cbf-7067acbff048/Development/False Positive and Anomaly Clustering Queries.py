import pandas as pd

# ============================================================================
# QUERY CATEGORY 4: FALSE POSITIVE ANALYSIS
# ============================================================================

q9_false_positive_correlation = """
-- Q9: False Positive Analysis - Alert vs Actual Failure Correlation
-- Analyze alert accuracy by comparing alerts to actual equipment failures
WITH alert_history AS (
    SELECT 
        vessel_id,
        equipment_id,
        DATE_TRUNC('day', timestamp) as alert_date,
        COUNT(*) as alert_count,
        AVG(anomaly_score) as avg_alert_score
    FROM iceberg.fleet_telemetry.anomaly_history
    WHERE date_partition >= current_date - INTERVAL '90' DAY
      AND date_partition < current_date
    GROUP BY vessel_id, equipment_id, DATE_TRUNC('day', timestamp)
),
failure_events AS (
    SELECT 
        vessel_id,
        equipment_id,
        DATE_TRUNC('day', failure_timestamp) as failure_date,
        failure_type,
        downtime_hours,
        repair_cost
    FROM iceberg.fleet_telemetry.equipment_failures
    WHERE date_partition >= current_date - INTERVAL '90' DAY
      AND date_partition < current_date
)
SELECT 
    ah.vessel_id,
    ah.equipment_id,
    ah.alert_date,
    ah.alert_count,
    ah.avg_alert_score,
    fe.failure_type,
    fe.downtime_hours,
    fe.repair_cost,
    CASE 
        WHEN fe.failure_date IS NOT NULL THEN 'TRUE_POSITIVE'
        ELSE 'FALSE_POSITIVE'
    END as outcome,
    DATE_DIFF('day', ah.alert_date, fe.failure_date) as days_to_failure
FROM alert_history ah
LEFT JOIN failure_events fe
    ON ah.vessel_id = fe.vessel_id
    AND ah.equipment_id = fe.equipment_id
    AND ah.alert_date <= fe.failure_date
    AND ah.alert_date >= fe.failure_date - INTERVAL '7' DAY
ORDER BY ah.vessel_id, ah.alert_date;

-- Optimization: Date partition pruning on both tables
-- 7-day window for alert-to-failure matching
-- Target execution: <7s
"""

q10_false_positive_rate_by_feature = """
-- Q10: False Positive Rate by Feature and Equipment Type
-- Calculate precision metrics for different anomaly features
WITH alert_outcomes AS (
    SELECT 
        ah.feature_name,
        ah.equipment_type,
        ah.vessel_id,
        ah.equipment_id,
        ah.timestamp as alert_time,
        CASE 
            WHEN fe.failure_timestamp IS NOT NULL 
                AND fe.failure_timestamp >= ah.timestamp
                AND fe.failure_timestamp <= ah.timestamp + INTERVAL '7' DAY
            THEN 1 ELSE 0 
        END as is_true_positive
    FROM iceberg.fleet_telemetry.anomaly_history ah
    LEFT JOIN iceberg.fleet_telemetry.equipment_failures fe
        ON ah.vessel_id = fe.vessel_id
        AND ah.equipment_id = fe.equipment_id
        AND fe.failure_timestamp >= ah.timestamp
        AND fe.failure_timestamp <= ah.timestamp + INTERVAL '7' DAY
    WHERE ah.date_partition >= current_date - INTERVAL '90' DAY
      AND ah.date_partition < current_date
      AND ah.anomaly_score >= 0.7
)
SELECT 
    feature_name,
    equipment_type,
    COUNT(*) as total_alerts,
    SUM(is_true_positive) as true_positives,
    COUNT(*) - SUM(is_true_positive) as false_positives,
    CAST(SUM(is_true_positive) AS DOUBLE) / COUNT(*) as precision,
    CAST(COUNT(*) - SUM(is_true_positive) AS DOUBLE) / COUNT(*) as false_positive_rate,
    COUNT(DISTINCT vessel_id) as vessels_affected
FROM alert_outcomes
GROUP BY feature_name, equipment_type
HAVING COUNT(*) >= 10
ORDER BY false_positive_rate DESC, total_alerts DESC;

-- Optimization: Single pass over alert data with LEFT JOIN
-- Filter for significant sample size (>=10 alerts)
-- Materialized join for efficiency
"""

q11_alert_frequency_analysis = """
-- Q11: Alert Frequency vs Failure Correlation
-- Identify equipment with high alert frequency but low failure rate
WITH equipment_metrics AS (
    SELECT 
        vessel_id,
        equipment_id,
        equipment_type,
        COUNT(*) as total_alerts,
        AVG(anomaly_score) as avg_score,
        STDDEV(anomaly_score) as stddev_score,
        COUNT(DISTINCT date_partition) as active_days
    FROM iceberg.fleet_telemetry.anomaly_history
    WHERE date_partition >= current_date - INTERVAL '90' DAY
      AND date_partition < current_date
    GROUP BY vessel_id, equipment_id, equipment_type
),
failure_metrics AS (
    SELECT 
        vessel_id,
        equipment_id,
        COUNT(*) as failure_count,
        SUM(downtime_hours) as total_downtime,
        SUM(repair_cost) as total_cost
    FROM iceberg.fleet_telemetry.equipment_failures
    WHERE date_partition >= current_date - INTERVAL '90' DAY
      AND date_partition < current_date
    GROUP BY vessel_id, equipment_id
)
SELECT 
    em.vessel_id,
    em.equipment_id,
    em.equipment_type,
    em.total_alerts,
    em.avg_score,
    COALESCE(fm.failure_count, 0) as actual_failures,
    COALESCE(fm.total_downtime, 0) as downtime_hours,
    COALESCE(fm.total_cost, 0) as repair_cost,
    CAST(em.total_alerts AS DOUBLE) / NULLIF(COALESCE(fm.failure_count, 0), 0) as alerts_per_failure,
    CASE 
        WHEN em.total_alerts > 100 AND COALESCE(fm.failure_count, 0) = 0 THEN 'HIGH_FALSE_POSITIVE'
        WHEN em.total_alerts > 50 AND COALESCE(fm.failure_count, 0) <= 2 THEN 'MODERATE_FALSE_POSITIVE'
        ELSE 'ACCEPTABLE'
    END as alert_quality
FROM equipment_metrics em
LEFT JOIN failure_metrics fm
    ON em.vessel_id = fm.vessel_id
    AND em.equipment_id = fm.equipment_id
ORDER BY em.total_alerts DESC;

-- Optimization: Two independent aggregations then JOIN
-- Identify equipment needing threshold tuning
"""

# ============================================================================
# QUERY CATEGORY 5: ANOMALY PATTERN CLUSTERING
# ============================================================================

q12_kmeans_feature_clustering = """
-- Q12: Anomaly Pattern Clustering with Feature Vectors
-- Group similar anomaly patterns using statistical features
WITH anomaly_features AS (
    SELECT 
        vessel_id,
        equipment_type,
        feature_name,
        AVG(anomaly_score) as avg_score,
        STDDEV(anomaly_score) as stddev_score,
        MAX(anomaly_score) as max_score,
        COUNT(*) as event_count,
        COUNT(DISTINCT date_partition) as active_days,
        APPROX_PERCENTILE(anomaly_score, 0.95) as p95_score
    FROM iceberg.fleet_telemetry.anomaly_history
    WHERE date_partition >= current_date - INTERVAL '30' DAY
      AND date_partition < current_date
    GROUP BY vessel_id, equipment_type, feature_name
    HAVING COUNT(*) >= 5
)
SELECT 
    vessel_id,
    equipment_type,
    feature_name,
    avg_score,
    stddev_score,
    max_score,
    event_count,
    active_days,
    p95_score,
    -- Z-score normalization for clustering
    (avg_score - AVG(avg_score) OVER ()) / NULLIF(STDDEV(avg_score) OVER (), 0) as avg_score_z,
    (event_count - AVG(event_count) OVER ()) / NULLIF(STDDEV(event_count) OVER (), 0) as count_z,
    -- Pattern classification
    CASE 
        WHEN avg_score > 0.85 AND event_count > 50 THEN 'HIGH_PERSISTENT'
        WHEN avg_score > 0.85 AND event_count <= 50 THEN 'HIGH_INTERMITTENT'
        WHEN avg_score BETWEEN 0.7 AND 0.85 AND stddev_score < 0.1 THEN 'MODERATE_STABLE'
        WHEN avg_score BETWEEN 0.7 AND 0.85 AND stddev_score >= 0.1 THEN 'MODERATE_VARIABLE'
        ELSE 'LOW_SEVERITY'
    END as pattern_cluster
FROM anomaly_features
ORDER BY avg_score DESC, event_count DESC;

-- Optimization: Window functions for z-score calculation
-- Statistical clustering without external ML model
-- Export for K-means clustering in Python
"""

q13_temporal_clustering = """
-- Q13: Temporal Pattern Clustering - Time-of-Day and Day-of-Week
-- Identify temporal patterns in anomaly occurrence
SELECT 
    vessel_id,
    equipment_type,
    feature_name,
    EXTRACT(HOUR FROM timestamp) as hour_of_day,
    EXTRACT(DOW FROM timestamp) as day_of_week,
    COUNT(*) as occurrence_count,
    AVG(anomaly_score) as avg_score,
    MAX(anomaly_score) as max_score,
    COUNT(DISTINCT date_partition) as distinct_days
FROM iceberg.fleet_telemetry.anomaly_history
WHERE date_partition >= current_date - INTERVAL '90' DAY
  AND date_partition < current_date
GROUP BY vessel_id, equipment_type, feature_name, 
         EXTRACT(HOUR FROM timestamp), EXTRACT(DOW FROM timestamp)
HAVING COUNT(*) >= 3
ORDER BY vessel_id, equipment_type, occurrence_count DESC;

-- Optimization: Group by temporal dimensions
-- Identify time-based patterns for predictive modeling
-- Useful for operational scheduling
"""

q14_cross_vessel_similarity = """
-- Q14: Cross-Vessel Anomaly Pattern Similarity
-- Find vessels with similar anomaly profiles for fleet-wide insights
WITH vessel_profiles AS (
    SELECT 
        vessel_id,
        ARRAY_AGG(feature_name ORDER BY feature_name) as feature_set,
        AVG(anomaly_score) as fleet_avg_score,
        STDDEV(anomaly_score) as fleet_stddev,
        COUNT(*) as total_events
    FROM iceberg.fleet_telemetry.anomaly_history
    WHERE date_partition >= current_date - INTERVAL '30' DAY
      AND date_partition < current_date
    GROUP BY vessel_id
),
pairwise_comparison AS (
    SELECT 
        v1.vessel_id as vessel_1,
        v2.vessel_id as vessel_2,
        ABS(v1.fleet_avg_score - v2.fleet_avg_score) as score_diff,
        ABS(v1.total_events - v2.total_events) as event_count_diff,
        CARDINALITY(ARRAY_INTERSECT(v1.feature_set, v2.feature_set)) as common_features,
        CARDINALITY(v1.feature_set) as vessel1_features,
        CARDINALITY(v2.feature_set) as vessel2_features
    FROM vessel_profiles v1
    CROSS JOIN vessel_profiles v2
    WHERE v1.vessel_id < v2.vessel_id
)
SELECT 
    vessel_1,
    vessel_2,
    score_diff,
    event_count_diff,
    common_features,
    CAST(common_features AS DOUBLE) / GREATEST(vessel1_features, vessel2_features) as similarity_score
FROM pairwise_comparison
WHERE CAST(common_features AS DOUBLE) / GREATEST(vessel1_features, vessel2_features) >= 0.7
ORDER BY similarity_score DESC, score_diff ASC
LIMIT 50;

-- Optimization: Array operations for feature comparison
-- Cross join with self-filter to avoid duplicates
-- Limit to top 50 most similar pairs
"""

# Add queries to library
additional_queries = {
    "Q9_false_positive_correlation": {
        "query": q9_false_positive_correlation,
        "category": "False Positive Analysis",
        "target_latency_ms": 7000,
        "data_source": "Iceberg",
        "use_case": "Analyze alert accuracy vs actual failures"
    },
    "Q10_false_positive_rate_by_feature": {
        "query": q10_false_positive_rate_by_feature,
        "category": "False Positive Analysis",
        "target_latency_ms": 8000,
        "data_source": "Iceberg",
        "use_case": "Calculate precision metrics by feature"
    },
    "Q11_alert_frequency_analysis": {
        "query": q11_alert_frequency_analysis,
        "category": "False Positive Analysis",
        "target_latency_ms": 6000,
        "data_source": "Iceberg",
        "use_case": "Identify high-alert low-failure equipment"
    },
    "Q12_kmeans_feature_clustering": {
        "query": q12_kmeans_feature_clustering,
        "category": "Anomaly Pattern Clustering",
        "target_latency_ms": 5000,
        "data_source": "Iceberg",
        "use_case": "Statistical feature clustering for pattern recognition"
    },
    "Q13_temporal_clustering": {
        "query": q13_temporal_clustering,
        "category": "Anomaly Pattern Clustering",
        "target_latency_ms": 6000,
        "data_source": "Iceberg",
        "use_case": "Time-of-day and day-of-week pattern analysis"
    },
    "Q14_cross_vessel_similarity": {
        "query": q14_cross_vessel_similarity,
        "category": "Anomaly Pattern Clustering",
        "target_latency_ms": 9000,
        "data_source": "Iceberg",
        "use_case": "Find vessels with similar anomaly patterns"
    }
}

# Create summary
summary_additional = pd.DataFrame([
    {
        "Query ID": qid,
        "Category": details["category"],
        "Data Source": details["data_source"],
        "Target Latency (ms)": details["target_latency_ms"],
        "Use Case": details["use_case"]
    }
    for qid, details in additional_queries.items()
])

print("=" * 80)
print("PRESTO FEDERATION QUERY LIBRARY - PART 2")
print("False Positive Analysis & Anomaly Pattern Clustering")
print("=" * 80)
print(f"\nQueries in This Section: {len(additional_queries)}")
print(f"\nQuery Summary:\n")
print(summary_additional.to_string(index=False))
print("\n" + "=" * 80)

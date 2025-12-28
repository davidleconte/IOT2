import pandas as pd

# ============================================================================
# QUERY CATEGORY 6: THRESHOLD TUNING RECOMMENDATIONS
# ============================================================================

q15_optimal_threshold_analysis = """
-- Q15: Optimal Threshold Analysis for Precision/Recall Balance
-- Statistical analysis to recommend optimal confidence thresholds
WITH threshold_simulation AS (
    SELECT 
        vessel_id,
        equipment_type,
        feature_name,
        threshold,
        COUNT(*) as alerts_at_threshold,
        SUM(CASE WHEN actual_failure = 1 THEN 1 ELSE 0 END) as true_positives,
        SUM(CASE WHEN actual_failure = 0 THEN 1 ELSE 0 END) as false_positives
    FROM (
        SELECT 
            ah.vessel_id,
            ah.equipment_type,
            ah.feature_name,
            ah.anomaly_score,
            threshold_values.threshold,
            CASE 
                WHEN fe.failure_timestamp IS NOT NULL 
                    AND fe.failure_timestamp >= ah.timestamp
                    AND fe.failure_timestamp <= ah.timestamp + INTERVAL '7' DAY
                THEN 1 ELSE 0 
            END as actual_failure
        FROM iceberg.fleet_telemetry.anomaly_history ah
        CROSS JOIN UNNEST(SEQUENCE(0.5, 0.95, 0.05)) AS threshold_values(threshold)
        LEFT JOIN iceberg.fleet_telemetry.equipment_failures fe
            ON ah.vessel_id = fe.vessel_id
            AND ah.equipment_id = fe.equipment_id
            AND fe.failure_timestamp >= ah.timestamp
            AND fe.failure_timestamp <= ah.timestamp + INTERVAL '7' DAY
        WHERE ah.date_partition >= current_date - INTERVAL '90' DAY
          AND ah.date_partition < current_date
          AND ah.anomaly_score >= threshold_values.threshold
    ) threshold_data
    GROUP BY vessel_id, equipment_type, feature_name, threshold
),
metrics_by_threshold AS (
    SELECT 
        vessel_id,
        equipment_type,
        feature_name,
        threshold,
        alerts_at_threshold,
        true_positives,
        false_positives,
        CAST(true_positives AS DOUBLE) / NULLIF(alerts_at_threshold, 0) as precision,
        CAST(true_positives AS DOUBLE) / NULLIF(
            true_positives + (SELECT COUNT(*) FROM iceberg.fleet_telemetry.equipment_failures 
                             WHERE date_partition >= current_date - INTERVAL '90' DAY), 0
        ) as recall,
        -- F1 Score
        2.0 * (CAST(true_positives AS DOUBLE) / NULLIF(alerts_at_threshold, 0)) * 
        (CAST(true_positives AS DOUBLE) / NULLIF(true_positives + false_positives, 0)) /
        NULLIF((CAST(true_positives AS DOUBLE) / NULLIF(alerts_at_threshold, 0)) + 
               (CAST(true_positives AS DOUBLE) / NULLIF(true_positives + false_positives, 0)), 0) as f1_score
    FROM threshold_simulation
)
SELECT 
    vessel_id,
    equipment_type,
    feature_name,
    threshold,
    alerts_at_threshold,
    precision,
    recall,
    f1_score,
    -- Rank by F1 score to find optimal threshold
    ROW_NUMBER() OVER (PARTITION BY vessel_id, equipment_type, feature_name 
                       ORDER BY f1_score DESC) as threshold_rank
FROM metrics_by_threshold
WHERE alerts_at_threshold >= 10
ORDER BY vessel_id, equipment_type, feature_name, threshold_rank;

-- Optimization: Threshold simulation with UNNEST
-- Cross join simulates different thresholds
-- ROW_NUMBER identifies optimal threshold per feature
"""

q16_adaptive_threshold_recommendations = """
-- Q16: Adaptive Threshold Recommendations Based on Historical Performance
-- Recommend threshold adjustments for underperforming features
WITH current_performance AS (
    SELECT 
        feature_name,
        equipment_type,
        0.7 as current_threshold,
        COUNT(*) as total_alerts,
        SUM(CASE WHEN is_true_positive = 1 THEN 1 ELSE 0 END) as true_positives,
        CAST(SUM(CASE WHEN is_true_positive = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as current_precision
    FROM (
        SELECT 
            ah.feature_name,
            ah.equipment_type,
            CASE 
                WHEN fe.failure_timestamp IS NOT NULL THEN 1 ELSE 0 
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
    ) performance_data
    GROUP BY feature_name, equipment_type
),
score_distribution AS (
    SELECT 
        feature_name,
        equipment_type,
        APPROX_PERCENTILE(anomaly_score, 0.50) as median_score,
        APPROX_PERCENTILE(anomaly_score, 0.75) as p75_score,
        APPROX_PERCENTILE(anomaly_score, 0.90) as p90_score,
        AVG(anomaly_score) as avg_score,
        STDDEV(anomaly_score) as stddev_score
    FROM iceberg.fleet_telemetry.anomaly_history
    WHERE date_partition >= current_date - INTERVAL '90' DAY
      AND date_partition < current_date
      AND anomaly_score >= 0.7
    GROUP BY feature_name, equipment_type
)
SELECT 
    cp.feature_name,
    cp.equipment_type,
    cp.current_threshold,
    cp.total_alerts,
    cp.current_precision,
    sd.median_score,
    sd.p75_score,
    sd.p90_score,
    -- Recommendation logic
    CASE 
        WHEN cp.current_precision < 0.5 THEN sd.p75_score
        WHEN cp.current_precision < 0.7 THEN sd.median_score + sd.stddev_score
        ELSE cp.current_threshold
    END as recommended_threshold,
    CASE 
        WHEN cp.current_precision < 0.5 THEN 'CRITICAL - Increase threshold significantly'
        WHEN cp.current_precision < 0.7 THEN 'MODERATE - Slight threshold increase needed'
        ELSE 'OPTIMAL - No change needed'
    END as recommendation,
    ROUND(cp.total_alerts * (1 - (CASE 
        WHEN cp.current_precision < 0.5 THEN sd.p75_score
        WHEN cp.current_precision < 0.7 THEN sd.median_score + sd.stddev_score
        ELSE cp.current_threshold
    END - cp.current_threshold) / 0.3), 0) as estimated_new_alert_count
FROM current_performance cp
JOIN score_distribution sd
    ON cp.feature_name = sd.feature_name
    AND cp.equipment_type = sd.equipment_type
WHERE cp.total_alerts >= 20
ORDER BY cp.current_precision ASC, cp.total_alerts DESC;

-- Optimization: Two-stage aggregation with JOIN
-- Percentile analysis for threshold recommendations
-- Business logic for threshold adjustment
"""

q17_threshold_impact_projection = """
-- Q17: Threshold Impact Projection - What-If Analysis
-- Project the impact of threshold changes on alert volume and precision
WITH baseline_metrics AS (
    SELECT 
        0.7 as baseline_threshold,
        COUNT(*) as baseline_alert_count,
        SUM(CASE WHEN is_failure = 1 THEN 1 ELSE 0 END) as baseline_true_positives,
        CAST(SUM(CASE WHEN is_failure = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as baseline_precision
    FROM (
        SELECT 
            ah.anomaly_score,
            CASE WHEN fe.failure_timestamp IS NOT NULL THEN 1 ELSE 0 END as is_failure
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
),
projected_thresholds AS (
    SELECT 
        new_threshold.threshold as projected_threshold,
        COUNT(*) as projected_alert_count,
        SUM(CASE WHEN is_failure = 1 THEN 1 ELSE 0 END) as projected_true_positives,
        CAST(SUM(CASE WHEN is_failure = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as projected_precision
    FROM (
        SELECT 
            ah.anomaly_score,
            CASE WHEN fe.failure_timestamp IS NOT NULL THEN 1 ELSE 0 END as is_failure
        FROM iceberg.fleet_telemetry.anomaly_history ah
        LEFT JOIN iceberg.fleet_telemetry.equipment_failures fe
            ON ah.vessel_id = fe.vessel_id
            AND ah.equipment_id = fe.equipment_id
            AND fe.failure_timestamp >= ah.timestamp
            AND fe.failure_timestamp <= ah.timestamp + INTERVAL '7' DAY
        WHERE ah.date_partition >= current_date - INTERVAL '90' DAY
          AND ah.date_partition < current_date
    ) alert_data
    CROSS JOIN UNNEST(ARRAY[0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9]) AS new_threshold(threshold)
    WHERE alert_data.anomaly_score >= new_threshold.threshold
    GROUP BY new_threshold.threshold
)
SELECT 
    pt.projected_threshold,
    pt.projected_alert_count,
    pt.projected_precision,
    bm.baseline_alert_count,
    bm.baseline_precision,
    pt.projected_alert_count - bm.baseline_alert_count as alert_count_delta,
    ROUND((pt.projected_precision - bm.baseline_precision) * 100, 2) as precision_improvement_pct,
    ROUND(CAST(pt.projected_alert_count AS DOUBLE) / bm.baseline_alert_count * 100, 2) as alert_volume_pct
FROM projected_thresholds pt
CROSS JOIN baseline_metrics bm
ORDER BY pt.projected_threshold;

-- Optimization: What-if analysis with array of thresholds
-- Compare projected vs baseline metrics
-- Business decision support for threshold changes
"""

# ============================================================================
# QUERY CATEGORY 7: TEMPORAL ANALYSIS
# ============================================================================

q18_hour_of_day_patterns = """
-- Q18: Hour-of-Day Anomaly Pattern Analysis
-- Identify peak hours for anomalies and operational scheduling
SELECT 
    EXTRACT(HOUR FROM timestamp) as hour_of_day,
    COUNT(*) as total_anomalies,
    COUNT(DISTINCT vessel_id) as vessels_affected,
    AVG(anomaly_score) as avg_severity,
    MAX(anomaly_score) as max_severity,
    APPROX_PERCENTILE(anomaly_score, 0.95) as p95_severity,
    COUNT(DISTINCT feature_name) as distinct_features,
    -- Top 3 features by hour
    ARRAY_AGG(DISTINCT feature_name ORDER BY COUNT(*) DESC)[1] as top_feature_1,
    ARRAY_AGG(DISTINCT feature_name ORDER BY COUNT(*) DESC)[2] as top_feature_2,
    ARRAY_AGG(DISTINCT feature_name ORDER BY COUNT(*) DESC)[3] as top_feature_3
FROM iceberg.fleet_telemetry.anomaly_history
WHERE date_partition >= current_date - INTERVAL '90' DAY
  AND date_partition < current_date
GROUP BY EXTRACT(HOUR FROM timestamp)
ORDER BY hour_of_day;

-- Optimization: Single group by hour
-- Identifies operational patterns for maintenance scheduling
-- Array aggregation for top features
"""

q19_day_of_week_seasonality = """
-- Q19: Day-of-Week Seasonality Analysis
-- Understand weekly patterns in anomaly occurrence
SELECT 
    EXTRACT(DOW FROM timestamp) as day_of_week,
    CASE EXTRACT(DOW FROM timestamp)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_name,
    COUNT(*) as total_anomalies,
    AVG(anomaly_score) as avg_severity,
    COUNT(DISTINCT vessel_id) as vessels_affected,
    COUNT(DISTINCT equipment_id) as equipment_affected,
    -- Compare to weekly average
    COUNT(*) - AVG(COUNT(*)) OVER () as deviation_from_weekly_avg,
    ROUND(CAST(COUNT(*) AS DOUBLE) / AVG(COUNT(*)) OVER () * 100, 2) as pct_of_weekly_avg
FROM iceberg.fleet_telemetry.anomaly_history
WHERE date_partition >= current_date - INTERVAL '90' DAY
  AND date_partition < current_date
GROUP BY EXTRACT(DOW FROM timestamp)
ORDER BY day_of_week;

-- Optimization: Window function for weekly average comparison
-- Identifies weekend vs weekday patterns
-- Useful for operational planning
"""

q20_monthly_trend_forecasting = """
-- Q20: Monthly Trend Analysis for Forecasting
-- Analyze month-over-month trends for capacity planning
WITH monthly_stats AS (
    SELECT 
        DATE_TRUNC('month', timestamp) as month_start,
        COUNT(*) as monthly_anomalies,
        AVG(anomaly_score) as avg_monthly_score,
        COUNT(DISTINCT vessel_id) as active_vessels,
        COUNT(DISTINCT equipment_id) as affected_equipment,
        SUM(CASE WHEN anomaly_score >= 0.9 THEN 1 ELSE 0 END) as critical_anomalies
    FROM iceberg.fleet_telemetry.anomaly_history
    WHERE date_partition >= current_date - INTERVAL '365' DAY
      AND date_partition < current_date
    GROUP BY DATE_TRUNC('month', timestamp)
)
SELECT 
    month_start,
    monthly_anomalies,
    avg_monthly_score,
    active_vessels,
    critical_anomalies,
    -- Month-over-month change
    LAG(monthly_anomalies, 1) OVER (ORDER BY month_start) as prev_month_anomalies,
    monthly_anomalies - LAG(monthly_anomalies, 1) OVER (ORDER BY month_start) as mom_change,
    ROUND((CAST(monthly_anomalies - LAG(monthly_anomalies, 1) OVER (ORDER BY month_start) AS DOUBLE) / 
           NULLIF(LAG(monthly_anomalies, 1) OVER (ORDER BY month_start), 0)) * 100, 2) as mom_pct_change,
    -- 3-month moving average
    AVG(monthly_anomalies) OVER (ORDER BY month_start ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma_3month,
    -- Linear trend indicator
    ROW_NUMBER() OVER (ORDER BY month_start) as month_sequence
FROM monthly_stats
ORDER BY month_start DESC;

-- Optimization: Window functions for trend analysis
-- LAG for month-over-month comparison
-- Moving averages for smoothing
"""

# Add queries to library
threshold_temporal_queries = {
    "Q15_optimal_threshold_analysis": {
        "query": q15_optimal_threshold_analysis,
        "category": "Threshold Tuning",
        "target_latency_ms": 9000,
        "data_source": "Iceberg",
        "use_case": "Statistical analysis for optimal confidence thresholds"
    },
    "Q16_adaptive_threshold_recommendations": {
        "query": q16_adaptive_threshold_recommendations,
        "category": "Threshold Tuning",
        "target_latency_ms": 7000,
        "data_source": "Iceberg",
        "use_case": "Adaptive threshold recommendations based on performance"
    },
    "Q17_threshold_impact_projection": {
        "query": q17_threshold_impact_projection,
        "category": "Threshold Tuning",
        "target_latency_ms": 8000,
        "data_source": "Iceberg",
        "use_case": "What-if analysis for threshold changes"
    },
    "Q18_hour_of_day_patterns": {
        "query": q18_hour_of_day_patterns,
        "category": "Temporal Analysis",
        "target_latency_ms": 5000,
        "data_source": "Iceberg",
        "use_case": "Hour-of-day anomaly patterns for scheduling"
    },
    "Q19_day_of_week_seasonality": {
        "query": q19_day_of_week_seasonality,
        "category": "Temporal Analysis",
        "target_latency_ms": 4000,
        "data_source": "Iceberg",
        "use_case": "Weekly seasonality analysis"
    },
    "Q20_monthly_trend_forecasting": {
        "query": q20_monthly_trend_forecasting,
        "category": "Temporal Analysis",
        "target_latency_ms": 6000,
        "data_source": "Iceberg",
        "use_case": "Monthly trends for capacity planning"
    }
}

# Create summary
summary_threshold_temporal = pd.DataFrame([
    {
        "Query ID": qid,
        "Category": details["category"],
        "Data Source": details["data_source"],
        "Target Latency (ms)": details["target_latency_ms"],
        "Use Case": details["use_case"]
    }
    for qid, details in threshold_temporal_queries.items()
])

print("=" * 80)
print("PRESTO FEDERATION QUERY LIBRARY - PART 3")
print("Threshold Tuning & Temporal Analysis")
print("=" * 80)
print(f"\nQueries in This Section: {len(threshold_temporal_queries)}")
print(f"\nQuery Summary:\n")
print(summary_threshold_temporal.to_string(index=False))
print("\n" + "=" * 80)

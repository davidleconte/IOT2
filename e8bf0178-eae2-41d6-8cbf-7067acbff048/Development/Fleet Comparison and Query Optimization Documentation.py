import pandas as pd

# ============================================================================
# QUERY CATEGORY 8: FLEET-WIDE COMPARISON QUERIES
# ============================================================================

q21_vessel_performance_rankings = """
-- Q21: Fleet-Wide Vessel Performance Rankings
-- Rank vessels by anomaly rate, severity, and operational health
WITH vessel_metrics AS (
    SELECT 
        vessel_id,
        COUNT(*) as total_anomalies,
        AVG(anomaly_score) as avg_anomaly_score,
        MAX(anomaly_score) as max_anomaly_score,
        COUNT(DISTINCT equipment_id) as affected_equipment_count,
        COUNT(DISTINCT feature_name) as distinct_anomaly_types,
        COUNT(DISTINCT date_partition) as active_days,
        SUM(CASE WHEN anomaly_score >= 0.9 THEN 1 ELSE 0 END) as critical_anomalies
    FROM iceberg.fleet_telemetry.anomaly_history
    WHERE date_partition >= current_date - INTERVAL '30' DAY
      AND date_partition < current_date
    GROUP BY vessel_id
),
failure_metrics AS (
    SELECT 
        vessel_id,
        COUNT(*) as failure_count,
        SUM(downtime_hours) as total_downtime,
        SUM(repair_cost) as total_repair_cost
    FROM iceberg.fleet_telemetry.equipment_failures
    WHERE date_partition >= current_date - INTERVAL '30' DAY
      AND date_partition < current_date
    GROUP BY vessel_id
)
SELECT 
    vm.vessel_id,
    vm.total_anomalies,
    vm.avg_anomaly_score,
    vm.critical_anomalies,
    vm.affected_equipment_count,
    COALESCE(fm.failure_count, 0) as actual_failures,
    COALESCE(fm.total_downtime, 0) as downtime_hours,
    COALESCE(fm.total_repair_cost, 0) as repair_cost_usd,
    -- Performance metrics
    CAST(vm.total_anomalies AS DOUBLE) / vm.active_days as anomalies_per_day,
    CAST(COALESCE(fm.failure_count, 0) AS DOUBLE) / NULLIF(vm.total_anomalies, 0) as failure_rate,
    -- Rankings
    ROW_NUMBER() OVER (ORDER BY vm.avg_anomaly_score DESC) as severity_rank,
    ROW_NUMBER() OVER (ORDER BY vm.total_anomalies DESC) as volume_rank,
    ROW_NUMBER() OVER (ORDER BY COALESCE(fm.total_downtime, 0) DESC) as downtime_rank,
    -- Overall health score (lower is better)
    (ROW_NUMBER() OVER (ORDER BY vm.avg_anomaly_score DESC) +
     ROW_NUMBER() OVER (ORDER BY vm.total_anomalies DESC) +
     ROW_NUMBER() OVER (ORDER BY COALESCE(fm.total_downtime, 0) DESC)) / 3.0 as composite_health_rank
FROM vessel_metrics vm
LEFT JOIN failure_metrics fm
    ON vm.vessel_id = fm.vessel_id
ORDER BY composite_health_rank ASC;

-- Optimization: Two CTEs with aggregations, then JOIN
-- Multiple ranking metrics for comprehensive comparison
-- Composite health score for overall fleet management
"""

q22_equipment_reliability_comparison = """
-- Q22: Cross-Vessel Equipment Reliability Comparison
-- Compare same equipment types across vessels for maintenance insights
SELECT 
    equipment_type,
    vessel_id,
    COUNT(*) as anomaly_count,
    AVG(anomaly_score) as avg_score,
    COUNT(DISTINCT equipment_id) as equipment_units,
    COUNT(DISTINCT feature_name) as distinct_features,
    -- Compare to fleet average for this equipment type
    AVG(COUNT(*)) OVER (PARTITION BY equipment_type) as fleet_avg_anomalies,
    COUNT(*) - AVG(COUNT(*)) OVER (PARTITION BY equipment_type) as deviation_from_fleet_avg,
    -- Percentile ranking within equipment type
    PERCENT_RANK() OVER (PARTITION BY equipment_type ORDER BY COUNT(*)) as percentile_rank,
    CASE 
        WHEN PERCENT_RANK() OVER (PARTITION BY equipment_type ORDER BY COUNT(*)) > 0.75 THEN 'TOP_25_PCT_ISSUES'
        WHEN PERCENT_RANK() OVER (PARTITION BY equipment_type ORDER BY COUNT(*)) < 0.25 THEN 'BOTTOM_25_PCT_BEST'
        ELSE 'AVERAGE'
    END as performance_category
FROM iceberg.fleet_telemetry.anomaly_history
WHERE date_partition >= current_date - INTERVAL '90' DAY
  AND date_partition < current_date
GROUP BY equipment_type, vessel_id
ORDER BY equipment_type, anomaly_count DESC;

-- Optimization: Window functions for fleet comparison
-- Percentile ranking for outlier identification
-- Equipment-specific benchmarking
"""

q23_feature_sensitivity_comparison = """
-- Q23: Feature Sensitivity Analysis Across Fleet
-- Identify which features are most predictive across vessels
WITH feature_outcomes AS (
    SELECT 
        feature_name,
        vessel_id,
        COUNT(*) as alert_count,
        SUM(CASE WHEN is_true_positive = 1 THEN 1 ELSE 0 END) as tp_count,
        CAST(SUM(CASE WHEN is_true_positive = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as precision
    FROM (
        SELECT 
            ah.feature_name,
            ah.vessel_id,
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
    )
    GROUP BY feature_name, vessel_id
)
SELECT 
    feature_name,
    COUNT(DISTINCT vessel_id) as vessels_using_feature,
    SUM(alert_count) as total_fleet_alerts,
    AVG(precision) as avg_fleet_precision,
    STDDEV(precision) as precision_stddev,
    MIN(precision) as min_vessel_precision,
    MAX(precision) as max_vessel_precision,
    -- Feature consistency score (lower stddev = more consistent)
    CASE 
        WHEN STDDEV(precision) < 0.1 THEN 'HIGHLY_CONSISTENT'
        WHEN STDDEV(precision) < 0.2 THEN 'MODERATELY_CONSISTENT'
        ELSE 'VARIABLE_PERFORMANCE'
    END as consistency_rating,
    -- Recommendation
    CASE 
        WHEN AVG(precision) > 0.7 AND STDDEV(precision) < 0.15 THEN 'RELIABLE_ACROSS_FLEET'
        WHEN AVG(precision) > 0.7 AND STDDEV(precision) >= 0.15 THEN 'GOOD_BUT_NEEDS_TUNING'
        WHEN AVG(precision) <= 0.7 THEN 'NEEDS_IMPROVEMENT'
    END as fleet_recommendation
FROM feature_outcomes
WHERE alert_count >= 5
GROUP BY feature_name
HAVING COUNT(DISTINCT vessel_id) >= 3
ORDER BY avg_fleet_precision DESC, precision_stddev ASC;

-- Optimization: Nested aggregation for feature-level analysis
-- Statistical measures for fleet-wide consistency
-- Actionable recommendations for feature management
"""

# ============================================================================
# QUERY OPTIMIZATION TECHNIQUES & PERFORMANCE DOCUMENTATION
# ============================================================================

optimization_techniques = pd.DataFrame([
    {
        "Technique": "Predicate Pushdown",
        "Description": "Push WHERE filters to data source before data transfer",
        "Example": "WHERE date_partition >= current_date - INTERVAL '30' DAY",
        "Impact": "Reduces data scanned by 90%+ for time-bounded queries",
        "Applicable Queries": "Q1-Q23 (all queries with date filters)"
    },
    {
        "Technique": "Partition Pruning",
        "Description": "Use partition columns in WHERE clause to skip partitions",
        "Example": "date_partition column filters eliminate reading unnecessary dates",
        "Impact": "Iceberg queries scan only relevant partitions (30/365 for 30d query)",
        "Applicable Queries": "Q4, Q5, Q6, Q9-Q23 (Iceberg queries)"
    },
    {
        "Technique": "Broadcast Join",
        "Description": "Small table broadcast to all workers for efficient JOIN",
        "Example": "Real-time alerts (small) JOIN historical baseline (large)",
        "Impact": "Eliminates shuffle for asymmetric joins, 3-5x faster",
        "Applicable Queries": "Q7, Q8 (federation joins)"
    },
    {
        "Technique": "CTE Materialization",
        "Description": "WITH clauses materialize intermediate results",
        "Example": "WITH realtime_alerts AS (...) prevents repeated scans",
        "Impact": "Reduces repeated subquery execution, 2-3x faster",
        "Applicable Queries": "Q7-Q11, Q15-Q17, Q21-Q23"
    },
    {
        "Technique": "Columnar Projection",
        "Description": "SELECT only needed columns from Iceberg columnar format",
        "Example": "SELECT vessel_id, anomaly_score (not SELECT *)",
        "Impact": "Iceberg parquet reads only required columns, 5-10x less I/O",
        "Applicable Queries": "All Iceberg queries (Q4-Q23)"
    },
    {
        "Technique": "Approximate Aggregations",
        "Description": "Use APPROX_PERCENTILE instead of exact percentile",
        "Example": "APPROX_PERCENTILE(anomaly_score, 0.95)",
        "Impact": "10-20x faster for large datasets with <1% error",
        "Applicable Queries": "Q4, Q6, Q12, Q16, Q18"
    },
    {
        "Technique": "LIMIT Early",
        "Description": "Apply LIMIT to reduce result set size",
        "Example": "ORDER BY z_score DESC LIMIT 1000",
        "Impact": "Stops processing after reaching limit, saves resources",
        "Applicable Queries": "Q1, Q2, Q14"
    },
    {
        "Technique": "Index Hints",
        "Description": "Leverage OpenSearch indices on key fields",
        "Example": "Index on (timestamp, confidence_level, anomaly_score)",
        "Impact": "Sub-100ms queries on OpenSearch with proper indices",
        "Applicable Queries": "Q1, Q2, Q3 (OpenSearch queries)"
    },
    {
        "Technique": "Array Operations",
        "Description": "Use native array functions for efficient processing",
        "Example": "ARRAY_AGG, ARRAY_INTERSECT, CARDINALITY",
        "Impact": "Vectorized operations 10x faster than loops",
        "Applicable Queries": "Q3, Q14, Q18"
    },
    {
        "Technique": "Window Functions",
        "Description": "Single-pass aggregations with OVER clause",
        "Example": "ROW_NUMBER() OVER (PARTITION BY vessel_id ORDER BY score)",
        "Impact": "Replaces self-joins, 2-4x faster than subqueries",
        "Applicable Queries": "Q12, Q15, Q19-Q23"
    }
])

# Performance benchmarks
performance_benchmarks = pd.DataFrame([
    {
        "Query ID": "Q1-Q3",
        "Category": "Real-time OpenSearch",
        "Data Volume": "1K-10K rows",
        "Target Latency": "<100ms",
        "Optimization Keys": "Index usage, predicate pushdown",
        "Cache Strategy": "5-minute TTL on OpenSearch query cache"
    },
    {
        "Query ID": "Q4",
        "Category": "30-day Iceberg",
        "Data Volume": "1M-10M rows",
        "Target Latency": "<3s",
        "Optimization Keys": "Partition pruning (30 partitions), columnar projection",
        "Cache Strategy": "1-hour TTL, refresh hourly"
    },
    {
        "Query ID": "Q5",
        "Category": "90-day Iceberg",
        "Data Volume": "5M-30M rows",
        "Target Latency": "<5s",
        "Optimization Keys": "Weekly aggregation, partition pruning (90 partitions)",
        "Cache Strategy": "4-hour TTL, refresh 4x daily"
    },
    {
        "Query ID": "Q6",
        "Category": "365-day Iceberg",
        "Data Volume": "20M-100M rows",
        "Target Latency": "<8s",
        "Optimization Keys": "Columnar format, HAVING filter, approx percentiles",
        "Cache Strategy": "24-hour TTL, daily refresh"
    },
    {
        "Query ID": "Q7-Q8",
        "Category": "Federation JOIN",
        "Data Volume": "100K-1M rows",
        "Target Latency": "<6s",
        "Optimization Keys": "Broadcast join, CTE materialization, small result sets",
        "Cache Strategy": "15-minute TTL on historical baselines"
    },
    {
        "Query ID": "Q9-Q11",
        "Category": "False Positive Analysis",
        "Data Volume": "5M-20M rows",
        "Target Latency": "<8s",
        "Optimization Keys": "Date partition pruning, LEFT JOIN optimization",
        "Cache Strategy": "6-hour TTL, business hours refresh"
    },
    {
        "Query ID": "Q12-Q14",
        "Category": "Pattern Clustering",
        "Data Volume": "1M-10M rows",
        "Target Latency": "<9s",
        "Optimization Keys": "Window functions, array operations, LIMIT top-K",
        "Cache Strategy": "12-hour TTL, twice-daily refresh"
    },
    {
        "Query ID": "Q15-Q17",
        "Category": "Threshold Tuning",
        "Data Volume": "5M-30M rows",
        "Target Latency": "<9s",
        "Optimization Keys": "UNNEST for threshold simulation, CTE reuse",
        "Cache Strategy": "Daily refresh, used for weekly tuning"
    },
    {
        "Query ID": "Q18-Q20",
        "Category": "Temporal Analysis",
        "Data Volume": "5M-50M rows",
        "Target Latency": "<6s",
        "Optimization Keys": "EXTRACT for temporal dimensions, window functions",
        "Cache Strategy": "Daily refresh, operational dashboard use"
    },
    {
        "Query ID": "Q21-Q23",
        "Category": "Fleet Comparison",
        "Data Volume": "10M-50M rows",
        "Target Latency": "<9s",
        "Optimization Keys": "Multi-level aggregation, percentile ranking",
        "Cache Strategy": "Daily refresh, management reporting"
    }
])

# Result caching strategies
caching_strategies = pd.DataFrame([
    {
        "Strategy": "Query Result Cache",
        "TTL": "Varies by query type",
        "Implementation": "Presto coordinator result caching",
        "Benefits": "Eliminates re-execution for identical queries",
        "Use Cases": "Dashboard refresh, repeated analysis queries"
    },
    {
        "Strategy": "Materialized Views",
        "TTL": "Daily/Hourly refresh",
        "Implementation": "Pre-computed aggregations in Iceberg",
        "Benefits": "Sub-second response for common aggregations",
        "Use Cases": "Daily baselines, vessel metrics, fleet summaries"
    },
    {
        "Strategy": "Hot Partition Cache",
        "TTL": "Memory-resident",
        "Implementation": "Recent partitions cached in Presto workers",
        "Benefits": "10x faster for recent data queries",
        "Use Cases": "Last 7 days queries, real-time comparison"
    },
    {
        "Strategy": "OpenSearch Cache",
        "TTL": "5-15 minutes",
        "Implementation": "OpenSearch query cache + field data cache",
        "Benefits": "Sub-100ms response for recent anomaly queries",
        "Use Cases": "Real-time monitoring, alert dashboards"
    },
    {
        "Strategy": "Application-Level Cache",
        "TTL": "Query-specific",
        "Implementation": "Redis/Memcached with query hash keys",
        "Benefits": "API-level caching, reduces Presto load",
        "Use Cases": "Web dashboards, API endpoints, reports"
    }
])

# Complete query library
complete_library = {
    "Q21_vessel_performance_rankings": {
        "query": q21_vessel_performance_rankings,
        "category": "Fleet-Wide Comparison",
        "target_latency_ms": 8000,
        "data_source": "Iceberg",
        "use_case": "Rank vessels by health and performance metrics"
    },
    "Q22_equipment_reliability_comparison": {
        "query": q22_equipment_reliability_comparison,
        "category": "Fleet-Wide Comparison",
        "target_latency_ms": 7000,
        "data_source": "Iceberg",
        "use_case": "Compare equipment reliability across vessels"
    },
    "Q23_feature_sensitivity_comparison": {
        "query": q23_feature_sensitivity_comparison,
        "category": "Fleet-Wide Comparison",
        "target_latency_ms": 9000,
        "data_source": "Iceberg",
        "use_case": "Analyze feature consistency and reliability fleet-wide"
    }
}

summary_fleet = pd.DataFrame([
    {
        "Query ID": qid,
        "Category": details["category"],
        "Data Source": details["data_source"],
        "Target Latency (ms)": details["target_latency_ms"],
        "Use Case": details["use_case"]
    }
    for qid, details in complete_library.items()
])

print("=" * 80)
print("PRESTO FEDERATION QUERY LIBRARY - PART 4")
print("Fleet-Wide Comparison & Optimization Documentation")
print("=" * 80)
print(f"\nFinal Queries (Fleet Comparison): {len(complete_library)}")
print(f"\nQuery Summary:\n")
print(summary_fleet.to_string(index=False))

print("\n" + "=" * 80)
print("QUERY OPTIMIZATION TECHNIQUES")
print("=" * 80)
print(optimization_techniques.to_string(index=False))

print("\n" + "=" * 80)
print("PERFORMANCE BENCHMARKS & TARGETS")
print("=" * 80)
print(performance_benchmarks.to_string(index=False))

print("\n" + "=" * 80)
print("RESULT CACHING STRATEGIES")
print("=" * 80)
print(caching_strategies.to_string(index=False))
print("\n" + "=" * 80)

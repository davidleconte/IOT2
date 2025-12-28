import pandas as pd

# Presto/Trino SQL queries for aggregating accuracy results from both systems
# These queries federate data from OpenSearch and watsonx.data (Iceberg)

presto_queries = {}

# Query 1: Daily accuracy metrics comparison
presto_queries['daily_accuracy'] = """
-- Daily Accuracy Metrics Comparison
-- Federates OpenSearch real-time detections with Iceberg ground truth

WITH opensearch_detections AS (
    SELECT 
        DATE_TRUNC('day', detection_timestamp) AS detection_date,
        vessel_id,
        anomaly_score,
        confidence_level,
        feature_name
    FROM opensearch.fleet_telemetry.anomaly_detections
    WHERE detection_timestamp >= CURRENT_DATE - INTERVAL '30' DAY
),

watsonx_predictions AS (
    SELECT
        DATE_TRUNC('day', prediction_timestamp) AS prediction_date,
        vessel_id,
        failure_probability,
        model_name,
        prediction_category
    FROM iceberg.fleet_analytics.ml_predictions
    WHERE prediction_timestamp >= CURRENT_DATE - INTERVAL '30' DAY
),

ground_truth AS (
    SELECT
        DATE_TRUNC('day', incident_timestamp) AS incident_date,
        vessel_id,
        actual_failure,
        failure_type,
        failure_cost
    FROM iceberg.fleet_operations.historical_incidents
    WHERE incident_timestamp >= CURRENT_DATE - INTERVAL '30' DAY
)

SELECT 
    COALESCE(gt.incident_date, os.detection_date, wx.prediction_date) AS metric_date,
    
    -- OpenSearch metrics
    COUNT(DISTINCT os.vessel_id) AS os_detections,
    COUNT(DISTINCT CASE WHEN gt.actual_failure THEN os.vessel_id END) AS os_true_positives,
    COUNT(DISTINCT CASE WHEN NOT gt.actual_failure THEN os.vessel_id END) AS os_false_positives,
    
    -- watsonx.data metrics
    COUNT(DISTINCT wx.vessel_id) AS wx_detections,
    COUNT(DISTINCT CASE WHEN gt.actual_failure THEN wx.vessel_id END) AS wx_true_positives,
    COUNT(DISTINCT CASE WHEN NOT gt.actual_failure THEN wx.vessel_id END) AS wx_false_positives,
    
    -- Ground truth
    COUNT(DISTINCT gt.vessel_id) AS actual_failures,
    SUM(gt.failure_cost) AS total_failure_cost,
    
    -- Accuracy calculations
    CAST(COUNT(DISTINCT CASE WHEN gt.actual_failure THEN os.vessel_id END) AS DOUBLE) / 
        NULLIF(COUNT(DISTINCT os.vessel_id), 0) AS os_precision,
    CAST(COUNT(DISTINCT CASE WHEN gt.actual_failure THEN os.vessel_id END) AS DOUBLE) / 
        NULLIF(COUNT(DISTINCT gt.vessel_id), 0) AS os_recall,
    
    CAST(COUNT(DISTINCT CASE WHEN gt.actual_failure THEN wx.vessel_id END) AS DOUBLE) / 
        NULLIF(COUNT(DISTINCT wx.vessel_id), 0) AS wx_precision,
    CAST(COUNT(DISTINCT CASE WHEN gt.actual_failure THEN wx.vessel_id END) AS DOUBLE) / 
        NULLIF(COUNT(DISTINCT gt.vessel_id), 0) AS wx_recall

FROM ground_truth gt
FULL OUTER JOIN opensearch_detections os 
    ON gt.incident_date = os.detection_date AND gt.vessel_id = os.vessel_id
FULL OUTER JOIN watsonx_predictions wx 
    ON gt.incident_date = wx.prediction_date AND gt.vessel_id = wx.vessel_id
GROUP BY 1
ORDER BY 1 DESC;
"""

# Query 2: Vessel-level accuracy ranking
presto_queries['vessel_accuracy'] = """
-- Vessel-Level Detection Accuracy
-- Identifies which vessels have best/worst detection rates

WITH vessel_stats AS (
    SELECT
        v.vessel_id,
        v.vessel_name,
        v.vessel_type,
        COUNT(DISTINCT i.incident_id) AS total_incidents,
        
        -- OpenSearch stats
        COUNT(DISTINCT CASE WHEN od.detection_id IS NOT NULL THEN i.incident_id END) AS os_detected,
        COUNT(DISTINCT CASE WHEN i.actual_failure AND od.detection_id IS NOT NULL 
                           THEN i.incident_id END) AS os_tp,
        
        -- watsonx.data stats  
        COUNT(DISTINCT CASE WHEN mp.prediction_id IS NOT NULL THEN i.incident_id END) AS wx_detected,
        COUNT(DISTINCT CASE WHEN i.actual_failure AND mp.prediction_id IS NOT NULL 
                           THEN i.incident_id END) AS wx_tp,
        
        -- Hybrid stats
        COUNT(DISTINCT CASE WHEN (od.detection_id IS NOT NULL OR mp.prediction_id IS NOT NULL) 
                           THEN i.incident_id END) AS hybrid_detected,
        COUNT(DISTINCT CASE WHEN i.actual_failure AND 
                           (od.detection_id IS NOT NULL OR mp.prediction_id IS NOT NULL)
                           THEN i.incident_id END) AS hybrid_tp

    FROM iceberg.fleet_metadata.vessels v
    JOIN iceberg.fleet_operations.historical_incidents i ON v.vessel_id = i.vessel_id
    LEFT JOIN opensearch.fleet_telemetry.anomaly_detections od 
        ON i.vessel_id = od.vessel_id 
        AND ABS(TIMESTAMP_DIFF('second', i.incident_timestamp, od.detection_timestamp)) < 3600
    LEFT JOIN iceberg.fleet_analytics.ml_predictions mp
        ON i.vessel_id = mp.vessel_id
        AND ABS(TIMESTAMP_DIFF('second', i.incident_timestamp, mp.prediction_timestamp)) < 3600
    
    WHERE i.incident_timestamp >= CURRENT_DATE - INTERVAL '90' DAY
    GROUP BY 1, 2, 3
)

SELECT
    vessel_id,
    vessel_name,
    vessel_type,
    total_incidents,
    
    -- Accuracy rates
    ROUND(CAST(os_tp AS DOUBLE) / NULLIF(total_incidents, 0) * 100, 2) AS os_recall_pct,
    ROUND(CAST(wx_tp AS DOUBLE) / NULLIF(total_incidents, 0) * 100, 2) AS wx_recall_pct,
    ROUND(CAST(hybrid_tp AS DOUBLE) / NULLIF(total_incidents, 0) * 100, 2) AS hybrid_recall_pct,
    
    -- Detection counts
    os_detected,
    wx_detected,
    hybrid_detected,
    
    -- F1 calculations
    2.0 * os_tp / NULLIF(os_detected + total_incidents, 0) AS os_f1_score,
    2.0 * wx_tp / NULLIF(wx_detected + total_incidents, 0) AS wx_f1_score,
    2.0 * hybrid_tp / NULLIF(hybrid_detected + total_incidents, 0) AS hybrid_f1_score

FROM vessel_stats
WHERE total_incidents >= 5  -- Filter for statistical significance
ORDER BY hybrid_f1_score DESC;
"""

# Query 3: Feature importance analysis
presto_queries['feature_importance'] = """
-- Feature Importance by Detection Accuracy
-- Analyzes which anomaly features best predict actual failures

SELECT
    od.feature_name,
    COUNT(*) AS detection_count,
    COUNT(DISTINCT CASE WHEN i.actual_failure THEN od.detection_id END) AS true_positives,
    COUNT(DISTINCT CASE WHEN NOT i.actual_failure THEN od.detection_id END) AS false_positives,
    
    -- Accuracy metrics
    ROUND(CAST(COUNT(CASE WHEN i.actual_failure THEN 1 END) AS DOUBLE) / 
          NULLIF(COUNT(*), 0) * 100, 2) AS precision_pct,
    
    AVG(od.anomaly_score) AS avg_anomaly_score,
    AVG(od.confidence_level) AS avg_confidence,
    
    -- Latency analysis
    AVG(TIMESTAMP_DIFF('millisecond', i.incident_timestamp, od.detection_timestamp)) AS avg_detection_latency_ms,
    
    -- Business impact
    SUM(CASE WHEN i.actual_failure THEN i.failure_cost ELSE 8000 END) AS total_cost_impact

FROM opensearch.fleet_telemetry.anomaly_detections od
LEFT JOIN iceberg.fleet_operations.historical_incidents i
    ON od.vessel_id = i.vessel_id
    AND ABS(TIMESTAMP_DIFF('second', od.detection_timestamp, i.incident_timestamp)) < 3600
WHERE od.detection_timestamp >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY 1
ORDER BY precision_pct DESC, true_positives DESC;
"""

# Query 4: A/B testing statistical significance
presto_queries['ab_test_significance'] = """
-- A/B Testing Statistical Significance
-- Chi-square test for comparing system performance

WITH system_performance AS (
    SELECT
        'OpenSearch' AS system,
        COUNT(CASE WHEN od.detection_id IS NOT NULL AND i.actual_failure THEN 1 END) AS tp,
        COUNT(CASE WHEN od.detection_id IS NOT NULL AND NOT i.actual_failure THEN 1 END) AS fp,
        COUNT(CASE WHEN od.detection_id IS NULL AND i.actual_failure THEN 1 END) AS fn,
        COUNT(CASE WHEN od.detection_id IS NULL AND NOT i.actual_failure THEN 1 END) AS tn
    FROM iceberg.fleet_operations.historical_incidents i
    LEFT JOIN opensearch.fleet_telemetry.anomaly_detections od
        ON i.vessel_id = od.vessel_id
        AND ABS(TIMESTAMP_DIFF('second', i.incident_timestamp, od.detection_timestamp)) < 3600
    WHERE i.incident_timestamp >= CURRENT_DATE - INTERVAL '30' DAY
    
    UNION ALL
    
    SELECT
        'watsonx.data' AS system,
        COUNT(CASE WHEN mp.prediction_id IS NOT NULL AND i.actual_failure THEN 1 END) AS tp,
        COUNT(CASE WHEN mp.prediction_id IS NOT NULL AND NOT i.actual_failure THEN 1 END) AS fp,
        COUNT(CASE WHEN mp.prediction_id IS NULL AND i.actual_failure THEN 1 END) AS fn,
        COUNT(CASE WHEN mp.prediction_id IS NULL AND NOT i.actual_failure THEN 1 END) AS tn
    FROM iceberg.fleet_operations.historical_incidents i
    LEFT JOIN iceberg.fleet_analytics.ml_predictions mp
        ON i.vessel_id = mp.vessel_id
        AND ABS(TIMESTAMP_DIFF('second', i.incident_timestamp, mp.prediction_timestamp)) < 3600
    WHERE i.incident_timestamp >= CURRENT_DATE - INTERVAL '30' DAY
)

SELECT
    system,
    tp AS true_positives,
    fp AS false_positives,
    fn AS false_negatives,
    tn AS true_negatives,
    
    -- Metrics
    ROUND(CAST(tp AS DOUBLE) / NULLIF(tp + fp, 0), 4) AS precision,
    ROUND(CAST(tp AS DOUBLE) / NULLIF(tp + fn, 0), 4) AS recall,
    ROUND(2.0 * tp / NULLIF(2*tp + fp + fn, 0), 4) AS f1_score,
    
    -- Sample size
    tp + fp + fn + tn AS total_samples,
    
    -- Z-score calculation for statistical significance
    ROUND((CAST(tp AS DOUBLE) / NULLIF(tp + fn, 0) - 0.85) / 
          SQRT(0.85 * 0.15 / NULLIF(tp + fn, 0)), 3) AS z_score

FROM system_performance
ORDER BY f1_score DESC;
"""

print("=" * 100)
print("PRESTO FEDERATED QUERY TEMPLATES FOR ACCURACY AGGREGATION")
print("=" * 100)

query_descriptions = {
    'daily_accuracy': 'Daily accuracy metrics comparing OS and watsonx with ground truth',
    'vessel_accuracy': 'Vessel-level accuracy ranking showing best/worst detection rates',
    'feature_importance': 'Feature importance analysis for anomaly detection accuracy',
    'ab_test_significance': 'Statistical significance testing for A/B comparison'
}

for query_name, description in query_descriptions.items():
    print(f"\n{'=' * 100}")
    print(f"Query: {query_name.upper()}")
    print(f"Purpose: {description}")
    print(f"{'=' * 100}")
    print(presto_queries[query_name][:800] + "..." if len(presto_queries[query_name]) > 800 else presto_queries[query_name])

print("\n" + "=" * 100)
print("QUERY EXECUTION INSTRUCTIONS")
print("=" * 100)
print("""
These queries are designed to run on Presto/Trino with:
- OpenSearch connector: opensearch.fleet_telemetry schema
- Iceberg connector: iceberg.fleet_operations and iceberg.fleet_analytics schemas

To execute:
1. Ensure both connectors are configured in Presto catalog
2. Run queries through Presto CLI or JDBC connection
3. Schedule daily execution via Airflow or cron
4. Store results in reporting dashboard (Grafana/Superset)
""")

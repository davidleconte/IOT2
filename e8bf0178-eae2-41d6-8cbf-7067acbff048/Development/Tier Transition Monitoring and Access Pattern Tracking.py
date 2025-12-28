import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# Tier transition monitoring and access pattern tracking system

# Monitoring metrics for tier transitions
monitoring_metrics = {
    'transition_events': [
        'hot_to_warm_transition',
        'warm_to_cold_transition',
        'cold_retrieval_request',
        'tier_promotion'
    ],
    'performance_metrics': [
        'transition_duration_seconds',
        'data_integrity_check',
        'access_latency_ms',
        'cost_per_transition'
    ],
    'alerts': [
        'failed_transition',
        'latency_threshold_exceeded',
        'cost_anomaly',
        'data_corruption'
    ]
}

# Create monitoring dashboard specification
monitoring_dashboard_spec = pd.DataFrame([
    ['Tier Distribution', 'Gauge', 'Current % of data in each tier', 'Real-time', 'minio_tier_stats'],
    ['Transition Rate', 'Time Series', 'Objects transitioned per hour', '5 min', 'minio_transition_events'],
    ['Access Pattern Heatmap', 'Heatmap', 'Access frequency by tier and time', '15 min', 'minio_access_logs'],
    ['Query Latency by Tier', 'Histogram', 'Query response time distribution', '1 min', 'presto_query_logs'],
    ['Cost Efficiency', 'Line Chart', 'Cost per GB by tier over time', '1 hour', 'minio_billing_metrics'],
    ['Failed Transitions', 'Alert Panel', 'Failed transition count and details', 'Real-time', 'minio_error_logs'],
    ['Data Age Distribution', 'Stacked Bar', 'Data volume by age and tier', '1 hour', 'minio_object_metadata'],
    ['Hot Data Predictions', 'Forecast', 'Predicted hot tier usage', '6 hours', 'ml_access_predictor']
], columns=['Panel', 'Type', 'Description', 'Refresh', 'Data Source'])

print("=" * 100)
print("MinIO Tier Transition Monitoring Dashboard")
print("=" * 100)
print(monitoring_dashboard_spec.to_string(index=False))
print()

# Access pattern analysis queries
access_pattern_queries = """
-- Query 1: Access frequency by data age (Presto federated query)
SELECT 
    CASE 
        WHEN age_days <= 7 THEN 'Hot (0-7 days)'
        WHEN age_days <= 30 THEN 'Warm (7-30 days)'
        ELSE 'Cold (30+ days)'
    END AS tier,
    COUNT(*) AS access_count,
    AVG(query_duration_ms) AS avg_latency_ms,
    SUM(bytes_scanned) / 1e9 AS gb_scanned
FROM (
    SELECT 
        object_key,
        DATE_DIFF('day', object_created_at, CURRENT_TIMESTAMP) AS age_days,
        query_duration_ms,
        bytes_scanned
    FROM iceberg.telemetry.access_logs
    WHERE access_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
) subquery
GROUP BY 1
ORDER BY 1;

-- Query 2: Identify hot data candidates for tier promotion
SELECT 
    object_key,
    current_tier,
    access_count_7d,
    avg_query_latency_ms,
    last_access_timestamp,
    CASE 
        WHEN current_tier = 'GLACIER' AND access_count_7d > 10 THEN 'Promote to Warm'
        WHEN current_tier = 'STANDARD_IA' AND access_count_7d > 50 THEN 'Promote to Hot'
        ELSE 'No action'
    END AS recommendation
FROM (
    SELECT 
        object_key,
        current_tier,
        COUNT(*) AS access_count_7d,
        AVG(query_duration_ms) AS avg_query_latency_ms,
        MAX(access_timestamp) AS last_access_timestamp
    FROM iceberg.telemetry.access_logs
    WHERE access_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
    GROUP BY object_key, current_tier
) access_summary
WHERE access_count_7d > 5
ORDER BY access_count_7d DESC
LIMIT 100;

-- Query 3: Tier transition success rate monitoring
SELECT 
    DATE_TRUNC('hour', transition_timestamp) AS hour,
    source_tier,
    target_tier,
    COUNT(*) AS total_transitions,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
    AVG(duration_seconds) AS avg_duration_sec,
    CAST(SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100 AS success_rate_pct
FROM minio.system.tier_transitions
WHERE transition_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC;

-- Query 4: Cost efficiency analysis by tier
SELECT 
    tier,
    SUM(storage_gb) AS total_storage_gb,
    AVG(cost_per_gb_month) AS cost_per_gb,
    SUM(storage_gb * cost_per_gb_month) AS total_monthly_cost,
    SUM(access_count) AS total_accesses,
    SUM(storage_gb * cost_per_gb_month) / NULLIF(SUM(access_count), 0) AS cost_per_access
FROM (
    SELECT 
        storage_class AS tier,
        SUM(object_size_bytes) / 1e9 AS storage_gb,
        CASE storage_class
            WHEN 'STANDARD' THEN 0.25
            WHEN 'STANDARD_IA' THEN 0.10
            WHEN 'GLACIER' THEN 0.03
        END AS cost_per_gb_month,
        COUNT(DISTINCT a.object_key) AS access_count
    FROM minio.system.objects o
    LEFT JOIN iceberg.telemetry.access_logs a 
        ON o.object_key = a.object_key 
        AND a.access_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
    GROUP BY storage_class
) tier_metrics
GROUP BY tier
ORDER BY total_monthly_cost DESC;
"""

print("=" * 100)
print("Access Pattern Analysis Queries (Presto)")
print("=" * 100)
print(access_pattern_queries)
print()

# MinIO tier monitoring configuration (Prometheus metrics)
prometheus_metrics = pd.DataFrame([
    ['minio_tier_transition_total', 'Counter', 'Total tier transitions', 'source_tier, target_tier, status'],
    ['minio_tier_transition_duration_seconds', 'Histogram', 'Transition duration', 'source_tier, target_tier'],
    ['minio_tier_objects_total', 'Gauge', 'Objects per tier', 'tier'],
    ['minio_tier_storage_bytes', 'Gauge', 'Storage used per tier', 'tier'],
    ['minio_tier_access_latency_seconds', 'Histogram', 'Access latency by tier', 'tier, operation'],
    ['minio_tier_cost_total', 'Gauge', 'Total cost by tier', 'tier'],
    ['minio_tier_promotion_candidates', 'Gauge', 'Objects eligible for promotion', 'current_tier, target_tier'],
    ['minio_tier_policy_violations', 'Counter', 'Lifecycle policy violations', 'policy_id, violation_type']
], columns=['Metric', 'Type', 'Description', 'Labels'])

print("=" * 100)
print("Prometheus Metrics for MinIO Tier Monitoring")
print("=" * 100)
print(prometheus_metrics.to_string(index=False))
print()

# Grafana dashboard JSON for tier monitoring
grafana_dashboard_config = {
    "dashboard": {
        "title": "MinIO Intelligent Tiering Monitor",
        "panels": [
            {
                "title": "Tier Distribution",
                "type": "gauge",
                "targets": [{"expr": "minio_tier_storage_bytes / sum(minio_tier_storage_bytes) * 100"}]
            },
            {
                "title": "Transition Success Rate",
                "type": "stat",
                "targets": [{"expr": "rate(minio_tier_transition_total{status='success'}[5m]) / rate(minio_tier_transition_total[5m]) * 100"}]
            },
            {
                "title": "Query Latency by Tier (<10s target)",
                "type": "graph",
                "targets": [{"expr": "histogram_quantile(0.95, rate(minio_tier_access_latency_seconds_bucket[5m]))"}]
            },
            {
                "title": "Cost Savings vs Flat Storage",
                "type": "stat",
                "targets": [{"expr": "(1 - sum(minio_tier_cost_total) / (sum(minio_tier_storage_bytes) * 0.15)) * 100"}]
            }
        ]
    }
}

print("=" * 100)
print("Grafana Dashboard Configuration")
print("=" * 100)
print(f"Dashboard: {grafana_dashboard_config['dashboard']['title']}")
print(f"Panels: {len(grafana_dashboard_config['dashboard']['panels'])}")
for panel in grafana_dashboard_config['dashboard']['panels']:
    print(f"  - {panel['title']} ({panel['type']})")
print()

# Alert rules for tier monitoring
alert_rules = pd.DataFrame([
    ['Tier Transition Failure Rate', 'rate(minio_tier_transition_total{status="failed"}[5m]) > 0.05', 'critical', '5m', 'Tier transitions failing at >5%'],
    ['Query Latency Threshold', 'histogram_quantile(0.95, minio_tier_access_latency_seconds_bucket) > 10', 'warning', '2m', 'P95 latency exceeds 10s target'],
    ['Cost Savings Below Target', '(1 - sum(minio_tier_cost_total) / (sum(minio_tier_storage_bytes) * 0.15)) < 0.40', 'warning', '1h', 'Cost savings below 40% target'],
    ['Hot Tier Capacity Alert', 'minio_tier_storage_bytes{tier="STANDARD"} > 0.10 * sum(minio_tier_storage_bytes)', 'warning', '15m', 'Hot tier exceeds 10% capacity'],
    ['Stuck Transitions', 'minio_tier_transition_duration_seconds > 300', 'critical', '5m', 'Transition taking >5 minutes'],
    ['Data Access Anomaly', 'rate(minio_tier_access_latency_seconds_count{tier="GLACIER"}[1h]) > 100', 'info', '1h', 'Unusual cold tier access pattern']
], columns=['Alert Name', 'PromQL Expression', 'Severity', 'Duration', 'Description'])

print("=" * 100)
print("Alert Rules for Tier Monitoring")
print("=" * 100)
print(alert_rules.to_string(index=False))
print()

# Store monitoring configuration
monitoring_spec = monitoring_dashboard_spec
access_queries = access_pattern_queries
prometheus_config = prometheus_metrics
grafana_config = grafana_dashboard_config
alert_config = alert_rules

print("✓ Monitoring configuration created:")
print(f"  - monitoring_spec: {len(monitoring_spec)} dashboard panels")
print(f"  - prometheus_config: {len(prometheus_config)} metrics")
print(f"  - alert_config: {len(alert_config)} alert rules")
print(f"  - access_queries: Presto queries for access pattern analysis")

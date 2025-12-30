import os

# Explicitly create the subdirectory before writing to ensure it exists
os.makedirs(f'{presto_queries_dir}/multi-source-joins', exist_ok=True)

# Query 1: Multi-Source Join - Iceberg + HCD for Vessel Performance Analysis
multi_source_join_q1 = """-- Query: Vessel Performance with Maintenance History (Iceberg + HCD Join)
-- Complexity: Complex
-- Federation: Joins Iceberg (telemetry) with Cassandra HCD (maintenance features)
-- Tenant-Aware: Uses tenant_id filtering
-- Description: Correlates vessel telemetry from lakehouse with operational features from HCD

SELECT 
    t.tenant_id,
    t.vessel_imo,
    t.timestamp,
    t.fuel_consumption_mt,
    t.speed_knots,
    t.engine_rpm,
    t.coordinates.lat AS latitude,
    t.coordinates.lon AS longitude,
    mf.maintenance_score,
    mf.engine_health_index,
    mf.days_since_last_service,
    mf.predicted_maintenance_date,
    -- Calculated efficiency metric
    (t.speed_knots / NULLIF(t.fuel_consumption_mt, 0)) AS fuel_efficiency,
    -- Performance degradation indicator
    CASE 
        WHEN mf.engine_health_index < 0.7 AND t.fuel_consumption_mt > 2.5 
        THEN 'CRITICAL'
        WHEN mf.engine_health_index < 0.8 
        THEN 'WARNING'
        ELSE 'NORMAL'
    END AS performance_status
FROM maritime_iceberg.maritime.vessel_telemetry t
INNER JOIN cassandra.feast_feature_store.maintenance_features mf
    ON t.vessel_imo = mf.vessel_imo 
    AND t.timestamp BETWEEN mf.feature_timestamp - INTERVAL '1' HOUR 
                        AND mf.feature_timestamp + INTERVAL '1' HOUR
WHERE t.tenant_id = '${tenant_id}'
    AND t.event_date >= CURRENT_DATE - INTERVAL '7' DAYS
    AND t.fuel_consumption_mt IS NOT NULL
    AND mf.engine_health_index IS NOT NULL
ORDER BY t.timestamp DESC
LIMIT 10000;
"""

# Query 2: Enriched Voyage Analysis with Operational Features
multi_source_join_q2 = """-- Query: Enriched Voyage Analysis with Real-Time Features
-- Complexity: Highly Complex
-- Federation: Three-way join across Iceberg tables and HCD operational features
-- Window Functions: Includes ranking and aggregation
-- Description: Comprehensive voyage analysis with operational context from feature store

WITH voyage_telemetry_agg AS (
    SELECT 
        v.voyage_id,
        v.tenant_id,
        v.vessel_imo,
        v.departure_port,
        v.arrival_port,
        v.departure_time,
        v.estimated_arrival_time,
        COUNT(DISTINCT t.timestamp) AS telemetry_point_count,
        AVG(t.fuel_consumption_mt) AS avg_fuel_consumption,
        AVG(t.speed_knots) AS avg_speed,
        MAX(t.speed_knots) AS max_speed,
        AVG(t.engine_rpm) AS avg_engine_rpm,
        SUM(t.fuel_consumption_mt) AS total_fuel_consumed
    FROM maritime_iceberg.maritime.voyages v
    INNER JOIN maritime_iceberg.maritime.vessel_telemetry t
        ON v.voyage_id = t.voyage_id 
        AND v.tenant_id = t.tenant_id
    WHERE v.tenant_id = '${tenant_id}'
        AND v.departure_time >= CURRENT_DATE - INTERVAL '30' DAYS
    GROUP BY v.voyage_id, v.tenant_id, v.vessel_imo, 
             v.departure_port, v.arrival_port, 
             v.departure_time, v.estimated_arrival_time
),
operational_features AS (
    SELECT 
        vessel_imo,
        avg_speed_7d,
        avg_fuel_consumption_7d,
        port_call_frequency,
        idle_time_ratio,
        feature_timestamp
    FROM cassandra.feast_feature_store.operational_features
    WHERE feature_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30' DAYS
)
SELECT 
    vt.voyage_id,
    vt.tenant_id,
    vt.vessel_imo,
    vt.departure_port,
    vt.arrival_port,
    vt.departure_time,
    vt.estimated_arrival_time,
    vt.telemetry_point_count,
    vt.avg_fuel_consumption,
    vt.avg_speed,
    vt.max_speed,
    vt.total_fuel_consumed,
    of.avg_speed_7d AS baseline_avg_speed,
    of.avg_fuel_consumption_7d AS baseline_fuel_consumption,
    of.port_call_frequency,
    of.idle_time_ratio,
    -- Deviation from baseline
    (vt.avg_speed - of.avg_speed_7d) AS speed_deviation,
    (vt.avg_fuel_consumption - of.avg_fuel_consumption_7d) AS fuel_deviation,
    -- Performance ranking within tenant
    RANK() OVER (
        PARTITION BY vt.tenant_id 
        ORDER BY (vt.avg_speed / NULLIF(vt.avg_fuel_consumption, 0)) DESC
    ) AS efficiency_rank
FROM voyage_telemetry_agg vt
LEFT JOIN operational_features of
    ON vt.vessel_imo = of.vessel_imo
    AND vt.departure_time BETWEEN of.feature_timestamp - INTERVAL '12' HOURS
                              AND of.feature_timestamp + INTERVAL '12' HOURS
ORDER BY vt.departure_time DESC;
"""

# Save queries
q1_path = f'{presto_queries_dir}/multi-source-joins/01_vessel_performance_maintenance.sql'
with open(q1_path, 'w') as f:
    f.write(multi_source_join_q1)

q2_path = f'{presto_queries_dir}/multi-source-joins/02_enriched_voyage_analysis.sql'
with open(q2_path, 'w') as f:
    f.write(multi_source_join_q2)

print("Multi-Source Federated Join Queries Created:")
print(f"1. Vessel Performance with Maintenance History: {q1_path}")
print(f"2. Enriched Voyage Analysis with Operational Features: {q2_path}")
print("\nCapabilities Demonstrated:")
print("- Iceberg + Cassandra HCD federation")
print("- Cross-catalog joins with temporal alignment")
print("- Tenant-aware filtering")
print("- Window functions (RANK) with tenant partitioning")
print("- Complex CTEs and multi-way joins")

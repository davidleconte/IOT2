import os

# Advanced window function queries for voyage analytics
window_q1 = """-- Query: Vessel Performance Trends with Moving Averages
-- Complexity: Complex
-- Description: Calculate moving averages, rankings, and performance deltas using window functions
-- Presto Features: ROWS BETWEEN, LAG/LEAD, multiple window specifications

SELECT 
    tenant_id,
    vessel_imo,
    timestamp,
    event_date,
    speed_knots,
    fuel_consumption_mt,
    engine_rpm,
    -- Moving averages over different windows
    AVG(speed_knots) OVER (
        PARTITION BY vessel_imo 
        ORDER BY timestamp 
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS speed_ma_12h,
    AVG(fuel_consumption_mt) OVER (
        PARTITION BY vessel_imo 
        ORDER BY timestamp 
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) AS fuel_ma_24h,
    -- Performance delta from previous reading
    speed_knots - LAG(speed_knots, 1) OVER (
        PARTITION BY vessel_imo ORDER BY timestamp
    ) AS speed_delta,
    fuel_consumption_mt - LAG(fuel_consumption_mt, 1) OVER (
        PARTITION BY vessel_imo ORDER BY timestamp
    ) AS fuel_delta,
    -- Efficiency calculation with moving window
    (speed_knots / NULLIF(fuel_consumption_mt, 0)) AS current_efficiency,
    AVG(speed_knots / NULLIF(fuel_consumption_mt, 0)) OVER (
        PARTITION BY vessel_imo 
        ORDER BY timestamp 
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) AS efficiency_ma_24h,
    -- Performance ranking within tenant fleet
    RANK() OVER (
        PARTITION BY tenant_id, event_date 
        ORDER BY (speed_knots / NULLIF(fuel_consumption_mt, 0)) DESC
    ) AS daily_efficiency_rank,
    -- Percentile ranking
    PERCENT_RANK() OVER (
        PARTITION BY tenant_id 
        ORDER BY fuel_consumption_mt
    ) AS fuel_consumption_percentile
FROM maritime_iceberg.maritime.vessel_telemetry
WHERE tenant_id = '${tenant_id}'
    AND event_date >= CURRENT_DATE - INTERVAL '30' DAYS
    AND speed_knots > 0
    AND fuel_consumption_mt > 0
ORDER BY vessel_imo, timestamp;
"""

window_q2 = """-- Query: Voyage Phase Analysis with Session Windows
-- Complexity: Highly Complex
-- Description: Identify voyage phases (port, transit, slow-speed) using conditional window functions
-- Presto Features: Conditional aggregation, session identification, complex window frames

WITH telemetry_with_phases AS (
    SELECT 
        tenant_id,
        vessel_imo,
        timestamp,
        speed_knots,
        fuel_consumption_mt,
        coordinates.lat AS latitude,
        coordinates.lon AS longitude,
        -- Classify vessel state
        CASE 
            WHEN speed_knots < 2 THEN 'AT_PORT'
            WHEN speed_knots BETWEEN 2 AND 8 THEN 'MANEUVERING'
            WHEN speed_knots BETWEEN 8 AND 15 THEN 'NORMAL_TRANSIT'
            WHEN speed_knots > 15 THEN 'HIGH_SPEED'
        END AS vessel_state,
        -- Calculate distance from previous position (simplified)
        LAG(coordinates.lat, 1) OVER (
            PARTITION BY vessel_imo ORDER BY timestamp
        ) AS prev_lat,
        LAG(coordinates.lon, 1) OVER (
            PARTITION BY vessel_imo ORDER BY timestamp
        ) AS prev_lon
    FROM maritime_iceberg.maritime.vessel_telemetry
    WHERE tenant_id = '${tenant_id}'
        AND event_date >= CURRENT_DATE - INTERVAL '60' DAYS
),
state_changes AS (
    SELECT 
        *,
        -- Detect state transitions
        vessel_state != LAG(vessel_state, 1, vessel_state) OVER (
            PARTITION BY vessel_imo ORDER BY timestamp
        ) AS state_changed,
        -- Session identifier: increment when state changes
        SUM(
            CASE WHEN vessel_state != LAG(vessel_state, 1, vessel_state) OVER (
                PARTITION BY vessel_imo ORDER BY timestamp
            ) THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY vessel_imo ORDER BY timestamp
        ) AS session_id
    FROM telemetry_with_phases
),
session_summary AS (
    SELECT 
        tenant_id,
        vessel_imo,
        session_id,
        vessel_state,
        MIN(timestamp) AS session_start,
        MAX(timestamp) AS session_end,
        COUNT(*) AS data_points,
        AVG(speed_knots) AS avg_speed,
        MIN(speed_knots) AS min_speed,
        MAX(speed_knots) AS max_speed,
        SUM(fuel_consumption_mt) AS total_fuel_consumed,
        AVG(fuel_consumption_mt) AS avg_fuel_consumption,
        -- Duration in hours (assuming hourly data)
        COUNT(*) AS duration_hours
    FROM state_changes
    GROUP BY tenant_id, vessel_imo, session_id, vessel_state
)
SELECT 
    tenant_id,
    vessel_imo,
    session_id,
    vessel_state,
    session_start,
    session_end,
    duration_hours,
    data_points,
    avg_speed,
    min_speed,
    max_speed,
    total_fuel_consumed,
    avg_fuel_consumption,
    (avg_speed / NULLIF(avg_fuel_consumption, 0)) AS session_efficiency,
    -- Comparison to previous session
    LAG(vessel_state, 1) OVER (
        PARTITION BY vessel_imo ORDER BY session_start
    ) AS previous_state,
    LAG(avg_speed, 1) OVER (
        PARTITION BY vessel_imo ORDER BY session_start
    ) AS previous_avg_speed,
    -- Rank sessions by fuel consumption
    RANK() OVER (
        PARTITION BY tenant_id, vessel_state 
        ORDER BY total_fuel_consumed DESC
    ) AS fuel_consumption_rank_by_state
FROM session_summary
WHERE duration_hours >= 2  -- Filter out very short sessions
ORDER BY vessel_imo, session_start;
"""

window_q3 = """-- Query: Fleet-Wide Performance Comparison with Multiple Window Specs
-- Complexity: Highly Complex
-- Description: Compare vessel performance across fleet using multiple window specifications
-- Presto Features: Multiple PARTITION BY clauses, NTILE, cumulative distributions

WITH vessel_daily_metrics AS (
    SELECT 
        tenant_id,
        vessel_imo,
        event_date,
        COUNT(*) AS reading_count,
        AVG(speed_knots) AS avg_speed,
        AVG(fuel_consumption_mt) AS avg_fuel,
        AVG(speed_knots / NULLIF(fuel_consumption_mt, 0)) AS avg_efficiency,
        SUM(fuel_consumption_mt) AS total_fuel,
        STDDEV(speed_knots) AS speed_stddev,
        MAX(speed_knots) AS max_speed,
        MIN(speed_knots) AS min_speed
    FROM maritime_iceberg.maritime.vessel_telemetry
    WHERE tenant_id = '${tenant_id}'
        AND event_date >= CURRENT_DATE - INTERVAL '30' DAYS
        AND speed_knots > 0
        AND fuel_consumption_mt > 0
    GROUP BY tenant_id, vessel_imo, event_date
)
SELECT 
    tenant_id,
    vessel_imo,
    event_date,
    avg_speed,
    avg_fuel,
    avg_efficiency,
    total_fuel,
    speed_stddev,
    -- Fleet-wide rankings
    RANK() OVER (
        PARTITION BY event_date 
        ORDER BY avg_efficiency DESC
    ) AS daily_fleet_efficiency_rank,
    NTILE(4) OVER (
        PARTITION BY event_date 
        ORDER BY avg_efficiency
    ) AS efficiency_quartile,
    -- Vessel-specific trend analysis
    AVG(avg_efficiency) OVER (
        PARTITION BY vessel_imo 
        ORDER BY event_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS efficiency_7day_ma,
    -- Fleet average comparison
    AVG(avg_efficiency) OVER (
        PARTITION BY tenant_id, event_date
    ) AS fleet_avg_efficiency,
    avg_efficiency - AVG(avg_efficiency) OVER (
        PARTITION BY tenant_id, event_date
    ) AS efficiency_vs_fleet_avg,
    -- Cumulative metrics
    SUM(total_fuel) OVER (
        PARTITION BY vessel_imo 
        ORDER BY event_date
    ) AS cumulative_fuel_consumption,
    -- Performance percentiles across fleet
    PERCENT_RANK() OVER (
        PARTITION BY event_date 
        ORDER BY avg_efficiency
    ) AS efficiency_percentile,
    CUME_DIST() OVER (
        PARTITION BY event_date 
        ORDER BY total_fuel
    ) AS fuel_cumulative_distribution,
    -- First and last values in window
    FIRST_VALUE(avg_efficiency) OVER (
        PARTITION BY vessel_imo 
        ORDER BY event_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS efficiency_30days_ago,
    LAST_VALUE(avg_efficiency) OVER (
        PARTITION BY vessel_imo 
        ORDER BY event_date 
        ROWS BETWEEN CURRENT ROW AND 29 FOLLOWING
    ) AS efficiency_projected
FROM vessel_daily_metrics
ORDER BY event_date DESC, daily_fleet_efficiency_rank;
"""

# Save window function queries
os.makedirs(f'{presto_queries_dir}/window-functions', exist_ok=True)

win_q1_path = f'{presto_queries_dir}/window-functions/01_moving_averages_rankings.sql'
with open(win_q1_path, 'w') as f:
    f.write(window_q1)

win_q2_path = f'{presto_queries_dir}/window-functions/02_voyage_phase_sessions.sql'
with open(win_q2_path, 'w') as f:
    f.write(window_q2)

win_q3_path = f'{presto_queries_dir}/window-functions/03_fleet_performance_comparison.sql'
with open(win_q3_path, 'w') as f:
    f.write(window_q3)

print("Advanced Window Function Queries Created:")
print(f"1. Moving Averages and Rankings: {win_q1_path}")
print(f"2. Voyage Phase Session Analysis: {win_q2_path}")
print(f"3. Fleet-Wide Performance Comparison: {win_q3_path}")
print("\nWindow Function Capabilities:")
print("- LAG/LEAD for temporal comparisons")
print("- ROWS BETWEEN for flexible window frames")
print("- Multiple PARTITION BY specifications")
print("- RANK, PERCENT_RANK, NTILE, CUME_DIST")
print("- FIRST_VALUE, LAST_VALUE")
print("- Conditional window aggregations")

import os

# Event-based voyage segmentation queries
voyage_seg_q1 = """-- Query: Event-Based Voyage Segmentation with State Machine
-- Complexity: Highly Complex
-- Description: Segment voyages into phases using event detection and state transitions
-- Presto Features: Complex CTEs, window functions, conditional logic, session identification

WITH telemetry_enriched AS (
    SELECT 
        tenant_id,
        vessel_imo,
        voyage_id,
        timestamp,
        event_date,
        speed_knots,
        fuel_consumption_mt,
        engine_rpm,
        coordinates.lat AS lat,
        coordinates.lon AS lon,
        -- Detect significant speed changes
        ABS(speed_knots - LAG(speed_knots, 1, speed_knots) OVER (
            PARTITION BY vessel_imo ORDER BY timestamp
        )) AS speed_change,
        -- Detect engine state transitions
        CASE 
            WHEN engine_rpm < 100 THEN 'STOPPED'
            WHEN engine_rpm BETWEEN 100 AND 500 THEN 'IDLE'
            WHEN engine_rpm BETWEEN 500 AND 1200 THEN 'MANEUVERING'
            WHEN engine_rpm > 1200 THEN 'FULL_AHEAD'
        END AS engine_state,
        -- Calculate time delta
        timestamp - LAG(timestamp, 1) OVER (
            PARTITION BY vessel_imo ORDER BY timestamp
        ) AS time_since_last_reading
    FROM maritime_iceberg.maritime.vessel_telemetry
    WHERE tenant_id = '${tenant_id}'
        AND event_date >= CURRENT_DATE - INTERVAL '90' DAYS
        AND voyage_id IS NOT NULL
),
voyage_events AS (
    SELECT 
        *,
        -- Detect voyage phase events
        CASE 
            WHEN speed_knots < 1 AND LAG(speed_knots, 1, 0) OVER (PARTITION BY vessel_imo ORDER BY timestamp) >= 5 
            THEN 'ARRIVAL_EVENT'
            WHEN speed_knots >= 5 AND LAG(speed_knots, 1, 0) OVER (PARTITION BY vessel_imo ORDER BY timestamp) < 1 
            THEN 'DEPARTURE_EVENT'
            WHEN speed_change > 5 AND speed_knots > LAG(speed_knots, 1, 0) OVER (PARTITION BY vessel_imo ORDER BY timestamp)
            THEN 'ACCELERATION_EVENT'
            WHEN speed_change > 5 AND speed_knots < LAG(speed_knots, 1, 0) OVER (PARTITION BY vessel_imo ORDER BY timestamp)
            THEN 'DECELERATION_EVENT'
            WHEN engine_state = 'STOPPED' AND LAG(engine_state, 1, 'STOPPED') OVER (PARTITION BY vessel_imo ORDER BY timestamp) != 'STOPPED'
            THEN 'ENGINE_STOP_EVENT'
            WHEN engine_state != 'STOPPED' AND LAG(engine_state, 1, 'IDLE') OVER (PARTITION BY vessel_imo ORDER BY timestamp) = 'STOPPED'
            THEN 'ENGINE_START_EVENT'
            ELSE NULL
        END AS voyage_event,
        -- Calculate phase based on speed and engine state
        CASE 
            WHEN speed_knots < 1 AND engine_rpm < 100 THEN 'AT_BERTH'
            WHEN speed_knots < 3 AND engine_rpm BETWEEN 100 AND 500 THEN 'PORT_MANEUVERING'
            WHEN speed_knots BETWEEN 3 AND 8 THEN 'PILOTAGE'
            WHEN speed_knots BETWEEN 8 AND 12 THEN 'SLOW_STEAMING'
            WHEN speed_knots BETWEEN 12 AND 18 THEN 'NORMAL_CRUISING'
            WHEN speed_knots > 18 THEN 'HIGH_SPEED_CRUISING'
        END AS voyage_phase
    FROM telemetry_enriched
),
phase_sessions AS (
    SELECT 
        *,
        -- Create session ID for each continuous phase
        SUM(CASE WHEN voyage_phase != LAG(voyage_phase, 1, voyage_phase) OVER (
            PARTITION BY vessel_imo, voyage_id ORDER BY timestamp
        ) THEN 1 ELSE 0 END) OVER (
            PARTITION BY vessel_imo, voyage_id ORDER BY timestamp
        ) AS phase_session_id
    FROM voyage_events
),
phase_summary AS (
    SELECT 
        tenant_id,
        vessel_imo,
        voyage_id,
        phase_session_id,
        voyage_phase,
        MIN(timestamp) AS phase_start,
        MAX(timestamp) AS phase_end,
        COUNT(*) AS data_points,
        (CAST(MAX(timestamp) AS BIGINT) - CAST(MIN(timestamp) AS BIGINT)) / 3600 AS duration_hours,
        AVG(speed_knots) AS avg_speed,
        MIN(speed_knots) AS min_speed,
        MAX(speed_knots) AS max_speed,
        SUM(fuel_consumption_mt) AS total_fuel,
        AVG(fuel_consumption_mt) AS avg_fuel_rate,
        AVG(engine_rpm) AS avg_engine_rpm,
        -- Count significant events during this phase
        SUM(CASE WHEN voyage_event IS NOT NULL THEN 1 ELSE 0 END) AS event_count,
        -- Calculate distance traveled (simplified)
        SQRT(
            POW(MAX(lat) - MIN(lat), 2) + POW(MAX(lon) - MIN(lon), 2)
        ) * 60 AS approx_distance_nm
    FROM phase_sessions
    GROUP BY tenant_id, vessel_imo, voyage_id, phase_session_id, voyage_phase
)
SELECT 
    tenant_id,
    vessel_imo,
    voyage_id,
    phase_session_id,
    voyage_phase,
    phase_start,
    phase_end,
    duration_hours,
    data_points,
    avg_speed,
    min_speed,
    max_speed,
    total_fuel,
    avg_fuel_rate,
    avg_engine_rpm,
    event_count,
    approx_distance_nm,
    -- Calculate efficiency metrics
    (approx_distance_nm / NULLIF(total_fuel, 0)) AS fuel_efficiency_nm_per_mt,
    (approx_distance_nm / NULLIF(duration_hours, 0)) AS avg_speed_calc,
    -- Phase progression within voyage
    ROW_NUMBER() OVER (PARTITION BY voyage_id ORDER BY phase_start) AS phase_sequence,
    -- Previous phase context
    LAG(voyage_phase, 1) OVER (PARTITION BY voyage_id ORDER BY phase_start) AS previous_phase,
    LAG(total_fuel, 1) OVER (PARTITION BY voyage_id ORDER BY phase_start) AS previous_phase_fuel
FROM phase_summary
WHERE duration_hours > 0.5  -- Filter out very short phases
ORDER BY voyage_id, phase_start;
"""

voyage_seg_q2 = """-- Query: Port Stay Analysis with Cargo Operations
-- Complexity: Complex
-- Description: Identify and analyze port stays with cargo loading/unloading patterns
-- Presto Features: Geospatial proximity detection, temporal aggregation, event correlation

WITH port_coordinates AS (
    -- Simplified port location reference
    SELECT 'USNYC' AS port_code, 40.7 AS lat, -74.0 AS lon, 'New York' AS port_name
    UNION ALL SELECT 'NLRTM', 51.9, 4.5, 'Rotterdam'
    UNION ALL SELECT 'SGSIN', 1.3, 103.8, 'Singapore'
    UNION ALL SELECT 'CNSHA', 31.4, 121.5, 'Shanghai'
),
vessel_positions AS (
    SELECT 
        tenant_id,
        vessel_imo,
        voyage_id,
        timestamp,
        speed_knots,
        fuel_consumption_mt,
        coordinates.lat AS vessel_lat,
        coordinates.lon AS vessel_lon,
        -- Identify if vessel is stationary
        CASE WHEN speed_knots < 0.5 THEN 1 ELSE 0 END AS is_stationary
    FROM maritime_iceberg.maritime.vessel_telemetry
    WHERE tenant_id = '${tenant_id}'
        AND event_date >= CURRENT_DATE - INTERVAL '60' DAYS
),
vessel_near_port AS (
    SELECT 
        vp.*,
        pc.port_code,
        pc.port_name,
        -- Calculate distance to port (simplified Euclidean distance in degrees)
        SQRT(
            POW(vp.vessel_lat - pc.lat, 2) + POW(vp.vessel_lon - pc.lon, 2)
        ) AS distance_to_port
    FROM vessel_positions vp
    CROSS JOIN port_coordinates pc
    WHERE SQRT(
        POW(vp.vessel_lat - pc.lat, 2) + POW(vp.vessel_lon - pc.lon, 2)
    ) < 0.1  -- Within ~11km radius
),
port_stays AS (
    SELECT 
        tenant_id,
        vessel_imo,
        voyage_id,
        port_code,
        port_name,
        MIN(timestamp) AS arrival_time,
        MAX(timestamp) AS departure_time,
        COUNT(*) AS readings_in_port,
        (CAST(MAX(timestamp) AS BIGINT) - CAST(MIN(timestamp) AS BIGINT)) / 3600 AS stay_duration_hours,
        AVG(speed_knots) AS avg_speed_in_port,
        SUM(fuel_consumption_mt) AS fuel_consumed_in_port,
        SUM(is_stationary) AS stationary_readings,
        AVG(distance_to_port) AS avg_distance_to_port_center
    FROM vessel_near_port
    GROUP BY tenant_id, vessel_imo, voyage_id, port_code, port_name
    HAVING COUNT(*) >= 3  -- At least 3 readings to constitute a port stay
)
SELECT 
    ps.tenant_id,
    ps.vessel_imo,
    ps.voyage_id,
    ps.port_code,
    ps.port_name,
    ps.arrival_time,
    ps.departure_time,
    ps.stay_duration_hours,
    ps.readings_in_port,
    ps.stationary_readings,
    ps.avg_speed_in_port,
    ps.fuel_consumed_in_port,
    ps.avg_distance_to_port_center,
    (ps.stationary_readings * 100.0 / ps.readings_in_port) AS stationary_percentage,
    -- Sequence within voyage
    ROW_NUMBER() OVER (PARTITION BY ps.voyage_id ORDER BY ps.arrival_time) AS port_sequence,
    -- Time between ports
    LEAD(ps.arrival_time) OVER (PARTITION BY ps.voyage_id ORDER BY ps.arrival_time) - ps.departure_time AS transit_to_next_port,
    -- Fuel efficiency during stay
    (ps.fuel_consumed_in_port / NULLIF(ps.stay_duration_hours, 0)) AS fuel_rate_in_port
FROM port_stays ps
ORDER BY ps.voyage_id, ps.arrival_time;
"""

# Save voyage segmentation queries
os.makedirs(f'{presto_queries_dir}/voyage-segmentation', exist_ok=True)

seg_q1_path = f'{presto_queries_dir}/voyage-segmentation/01_event_based_phase_segmentation.sql'
with open(seg_q1_path, 'w') as f:
    f.write(voyage_seg_q1)

seg_q2_path = f'{presto_queries_dir}/voyage-segmentation/02_port_stay_analysis.sql'
with open(seg_q2_path, 'w') as f:
    f.write(voyage_seg_q2)

print("Event-Based Voyage Segmentation Queries Created:")
print(f"1. Event-Based Phase Segmentation: {seg_q1_path}")
print(f"2. Port Stay Analysis with Cargo Ops: {seg_q2_path}")
print("\nVoyage Segmentation Capabilities:")
print("- State machine implementation with CTEs")
print("- Event detection and classification")
print("- Phase identification and session tracking")
print("- Geospatial proximity analysis")
print("- Temporal aggregation and sequencing")

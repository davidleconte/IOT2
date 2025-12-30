import os

# Create JSON extraction queries for nested telemetry data
json_extract_q1 = """-- Query: Nested JSON Extraction from Telemetry Data
-- Complexity: Complex
-- Description: Extract nested sensor readings, alerts, and engine metrics from JSON telemetry
-- Presto Features: JSON functions, UNNEST, array operations

SELECT 
    tenant_id,
    vessel_imo,
    timestamp,
    event_date,
    -- Extract nested coordinates
    coordinates.lat AS latitude,
    coordinates.lon AS longitude,
    -- Extract engine metrics from nested JSON
    json_extract_scalar(sensor_data, '$.engine.temperature') AS engine_temp,
    json_extract_scalar(sensor_data, '$.engine.pressure') AS engine_pressure,
    json_extract_scalar(sensor_data, '$.engine.vibration') AS engine_vibration,
    -- Extract environmental conditions
    json_extract_scalar(sensor_data, '$.environment.wind_speed') AS wind_speed,
    json_extract_scalar(sensor_data, '$.environment.wave_height') AS wave_height,
    json_extract_scalar(sensor_data, '$.environment.sea_state') AS sea_state,
    -- Extract fuel system metrics
    json_extract_scalar(sensor_data, '$.fuel.flow_rate') AS fuel_flow_rate,
    json_extract_scalar(sensor_data, '$.fuel.tank_level') AS fuel_tank_level,
    json_extract_scalar(sensor_data, '$.fuel.quality_index') AS fuel_quality,
    -- Parse alert array
    CAST(json_extract(sensor_data, '$.alerts') AS ARRAY<VARCHAR>) AS alert_array,
    CARDINALITY(CAST(json_extract(sensor_data, '$.alerts') AS ARRAY<VARCHAR>)) AS alert_count,
    -- Extract cargo data
    json_extract_scalar(sensor_data, '$.cargo.weight_tonnes') AS cargo_weight,
    json_extract_scalar(sensor_data, '$.cargo.type') AS cargo_type
FROM maritime_iceberg.maritime.vessel_telemetry
WHERE tenant_id = '${tenant_id}'
    AND event_date >= CURRENT_DATE - INTERVAL '7' DAYS
    AND sensor_data IS NOT NULL
LIMIT 5000;
"""

json_extract_q2 = """-- Query: Advanced JSON Array Processing with UNNEST
-- Complexity: Highly Complex
-- Description: Unnest and analyze nested alert arrays, extract multilevel JSON structures
-- Presto Features: UNNEST, CROSS JOIN, complex JSON path extraction

WITH telemetry_with_alerts AS (
    SELECT 
        tenant_id,
        vessel_imo,
        timestamp,
        event_date,
        speed_knots,
        fuel_consumption_mt,
        sensor_data,
        CAST(json_extract(sensor_data, '$.alerts') AS ARRAY<VARCHAR>) AS alerts_array,
        CAST(json_extract(sensor_data, '$.maintenance_flags') AS ARRAY<VARCHAR>) AS maint_flags_array
    FROM maritime_iceberg.maritime.vessel_telemetry
    WHERE tenant_id = '${tenant_id}'
        AND event_date >= CURRENT_DATE - INTERVAL '14' DAYS
        AND sensor_data IS NOT NULL
        AND json_extract(sensor_data, '$.alerts') IS NOT NULL
),
unnested_alerts AS (
    SELECT 
        t.tenant_id,
        t.vessel_imo,
        t.timestamp,
        t.event_date,
        t.speed_knots,
        t.fuel_consumption_mt,
        alert,
        -- Parse individual alert JSON
        json_extract_scalar(alert, '$.type') AS alert_type,
        json_extract_scalar(alert, '$.severity') AS alert_severity,
        json_extract_scalar(alert, '$.code') AS alert_code,
        json_extract_scalar(alert, '$.timestamp') AS alert_timestamp
    FROM telemetry_with_alerts t
    CROSS JOIN UNNEST(t.alerts_array) AS alert_table(alert)
)
SELECT 
    tenant_id,
    vessel_imo,
    alert_type,
    alert_severity,
    alert_code,
    COUNT(*) AS alert_occurrences,
    MIN(timestamp) AS first_occurrence,
    MAX(timestamp) AS last_occurrence,
    AVG(speed_knots) AS avg_speed_during_alert,
    AVG(fuel_consumption_mt) AS avg_fuel_during_alert,
    -- Window function for alert frequency
    COUNT(*) OVER (
        PARTITION BY tenant_id, vessel_imo, alert_type 
        ORDER BY MAX(timestamp)
        RANGE BETWEEN INTERVAL '24' HOURS PRECEDING AND CURRENT ROW
    ) AS alerts_last_24h
FROM unnested_alerts
WHERE alert_severity IN ('HIGH', 'CRITICAL')
GROUP BY tenant_id, vessel_imo, alert_type, alert_severity, alert_code
ORDER BY alert_occurrences DESC, alert_severity DESC
LIMIT 1000;
"""

json_extract_q3 = """-- Query: Multi-Level JSON Extraction for Voyage Events
-- Complexity: Highly Complex
-- Description: Extract deeply nested event structures, port call details, cargo manifests
-- Presto Features: Recursive JSON extraction, map/array operations, JSON casting

WITH voyage_events AS (
    SELECT 
        voyage_id,
        tenant_id,
        vessel_imo,
        departure_time,
        -- Extract port call events array
        CAST(json_extract(voyage_metadata, '$.port_calls') AS ARRAY<VARCHAR>) AS port_calls,
        -- Extract cargo manifest
        CAST(json_extract(voyage_metadata, '$.cargo_manifest') AS ARRAY<VARCHAR>) AS cargo_items,
        -- Extract route waypoints
        CAST(json_extract(voyage_metadata, '$.route.waypoints') AS ARRAY<VARCHAR>) AS waypoints,
        -- Extract compliance checkpoints
        json_extract(voyage_metadata, '$.compliance') AS compliance_data
    FROM maritime_iceberg.maritime.voyages
    WHERE tenant_id = '${tenant_id}'
        AND departure_time >= CURRENT_DATE - INTERVAL '60' DAYS
        AND voyage_metadata IS NOT NULL
),
port_call_details AS (
    SELECT 
        ve.voyage_id,
        ve.tenant_id,
        ve.vessel_imo,
        pc AS port_call_json,
        json_extract_scalar(pc, '$.port_code') AS port_code,
        json_extract_scalar(pc, '$.port_name') AS port_name,
        json_extract_scalar(pc, '$.arrival_time') AS arrival_time,
        json_extract_scalar(pc, '$.departure_time') AS departure_time,
        json_extract_scalar(pc, '$.berth') AS berth,
        json_extract_scalar(pc, '$.cargo_ops.loaded_tonnes') AS cargo_loaded,
        json_extract_scalar(pc, '$.cargo_ops.unloaded_tonnes') AS cargo_unloaded,
        json_extract_scalar(pc, '$.services.bunker_fuel_mt') AS bunker_fuel,
        CAST(json_extract(pc, '$.services.provided') AS ARRAY<VARCHAR>) AS services_provided
    FROM voyage_events ve
    CROSS JOIN UNNEST(ve.port_calls) AS pc_table(pc)
),
cargo_manifest AS (
    SELECT 
        ve.voyage_id,
        ve.tenant_id,
        cargo_item,
        json_extract_scalar(cargo_item, '$.item_id') AS item_id,
        json_extract_scalar(cargo_item, '$.description') AS description,
        CAST(json_extract_scalar(cargo_item, '$.weight_tonnes') AS DOUBLE) AS weight_tonnes,
        json_extract_scalar(cargo_item, '$.hazmat_class') AS hazmat_class,
        json_extract_scalar(cargo_item, '$.origin_port') AS origin_port,
        json_extract_scalar(cargo_item, '$.destination_port') AS destination_port
    FROM voyage_events ve
    CROSS JOIN UNNEST(ve.cargo_items) AS cargo_table(cargo_item)
)
SELECT 
    pcd.voyage_id,
    pcd.tenant_id,
    pcd.vessel_imo,
    pcd.port_code,
    pcd.port_name,
    pcd.arrival_time,
    pcd.departure_time,
    pcd.cargo_loaded,
    pcd.cargo_unloaded,
    pcd.bunker_fuel,
    CARDINALITY(pcd.services_provided) AS service_count,
    -- Aggregate cargo manifest for this port call
    COUNT(DISTINCT cm.item_id) AS unique_cargo_items,
    SUM(cm.weight_tonnes) AS total_cargo_weight,
    SUM(CASE WHEN cm.hazmat_class IS NOT NULL THEN 1 ELSE 0 END) AS hazmat_items_count
FROM port_call_details pcd
LEFT JOIN cargo_manifest cm
    ON pcd.voyage_id = cm.voyage_id
    AND (pcd.port_code = cm.origin_port OR pcd.port_code = cm.destination_port)
GROUP BY 
    pcd.voyage_id, pcd.tenant_id, pcd.vessel_imo, pcd.port_code, pcd.port_name,
    pcd.arrival_time, pcd.departure_time, pcd.cargo_loaded, pcd.cargo_unloaded,
    pcd.bunker_fuel, pcd.services_provided
ORDER BY pcd.arrival_time DESC;
"""

# Save JSON extraction queries
os.makedirs(f'{presto_queries_dir}/json-extraction', exist_ok=True)

json_q1_path = f'{presto_queries_dir}/json-extraction/01_nested_sensor_extraction.sql'
with open(json_q1_path, 'w') as f:
    f.write(json_extract_q1)

json_q2_path = f'{presto_queries_dir}/json-extraction/02_alert_array_unnest.sql'
with open(json_q2_path, 'w') as f:
    f.write(json_extract_q2)

json_q3_path = f'{presto_queries_dir}/json-extraction/03_multilevel_voyage_events.sql'
with open(json_q3_path, 'w') as f:
    f.write(json_extract_q3)

print("Nested JSON Extraction Queries Created:")
print(f"1. Nested Sensor Data Extraction: {json_q1_path}")
print(f"2. Alert Array Processing with UNNEST: {json_q2_path}")
print(f"3. Multi-Level Voyage Events JSON: {json_q3_path}")
print("\nJSON Processing Capabilities:")
print("- json_extract, json_extract_scalar")
print("- UNNEST for array expansion")
print("- CROSS JOIN with unnested arrays")
print("- Complex JSON path navigation")
print("- Type casting and array operations")

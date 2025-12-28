import pandas as pd
import numpy as np

# Presto/Trino query examples for route optimization
route_optimization_queries = {
    'nearest_available_port': """
-- Find vessels within 200nm of ports with available berths
SELECT 
    v.vessel_id,
    v.nearest_port_name,
    v.distance_to_port_nm,
    v.weather_zone,
    v.berths_available,
    v.avg_wait_hours,
    v.storm_risk,
    -- Calculate ETA at current speed (assuming 15 knots average)
    ROUND(v.distance_to_port_nm / 15.0, 2) AS eta_hours
FROM iceberg.vessel_positions_enriched v
WHERE v.distance_to_port_nm < 200
  AND v.berths_available > 5
  AND v.position_timestamp > CURRENT_TIMESTAMP - INTERVAL '1' HOUR
ORDER BY v.distance_to_port_nm ASC;
""",
    
    'alternate_port_recommendation': """
-- Recommend alternate ports if primary port is congested
WITH vessel_current AS (
    SELECT vessel_id, nearest_port_name, distance_to_port_nm, berths_available
    FROM iceberg.vessel_positions_enriched
    WHERE position_timestamp > CURRENT_TIMESTAMP - INTERVAL '15' MINUTE
),
port_alternatives AS (
    SELECT 
        p.port_name,
        p.latitude,
        p.longitude,
        p.berths_available,
        p.avg_wait_hours,
        p.weather_zone
    FROM cassandra.operational_store.port_reference_data p
    WHERE p.berths_available > 10
)
SELECT 
    vc.vessel_id,
    vc.nearest_port_name AS current_target,
    vc.berths_available AS current_berths,
    pa.port_name AS alternate_port,
    pa.berths_available AS alternate_berths,
    pa.avg_wait_hours,
    -- Simplified distance calc - in production use actual vessel position
    ROUND(SQRT(POWER(pa.latitude - 1.26, 2) + POWER(pa.longitude - 103.85, 2)) * 60, 2) AS distance_to_alternate_nm
FROM vessel_current vc
CROSS JOIN port_alternatives pa
WHERE vc.berths_available < 5
  AND pa.port_name != vc.nearest_port_name
ORDER BY vc.vessel_id, distance_to_alternate_nm ASC;
""",

    'weather_zone_routing': """
-- Identify vessels in high storm risk zones needing route adjustment
SELECT 
    v.vessel_id,
    v.nearest_port_name,
    v.weather_zone,
    v.storm_risk,
    v.position_timestamp,
    CASE 
        WHEN v.storm_risk > 0.25 THEN 'HIGH_RISK'
        WHEN v.storm_risk > 0.15 THEN 'MODERATE_RISK'
        ELSE 'LOW_RISK'
    END AS risk_level,
    -- Recommendation
    CASE 
        WHEN v.storm_risk > 0.25 AND v.distance_to_port_nm > 100 
            THEN 'Consider alternate route or await weather improvement'
        WHEN v.storm_risk > 0.15 
            THEN 'Monitor weather closely, prepare contingency'
        ELSE 'Continue current route'
    END AS routing_recommendation
FROM iceberg.vessel_positions_enriched v
WHERE v.position_timestamp > CURRENT_TIMESTAMP - INTERVAL '30' MINUTE
  AND v.storm_risk > 0.15
ORDER BY v.storm_risk DESC, v.distance_to_port_nm DESC;
""",

    'fleet_congestion_analysis': """
-- Analyze port congestion across entire fleet
SELECT 
    nearest_port_name,
    COUNT(DISTINCT vessel_id) AS vessels_approaching,
    AVG(distance_to_port_nm) AS avg_distance_nm,
    MIN(distance_to_port_nm) AS closest_vessel_nm,
    MAX(berths_available) AS berths_available,
    MAX(avg_wait_hours) AS expected_wait_hours,
    -- Calculate total vessels that could arrive in next 12 hours
    COUNT(CASE WHEN distance_to_port_nm / 15.0 <= 12 THEN 1 END) AS vessels_arriving_12h
FROM iceberg.vessel_positions_enriched
WHERE position_timestamp > CURRENT_TIMESTAMP - INTERVAL '1' HOUR
  AND distance_to_port_nm < 500
GROUP BY nearest_port_name
HAVING vessels_approaching > 3
ORDER BY vessels_arriving_12h DESC, berths_available ASC;
""",

    'fuel_optimal_routing': """
-- Route optimization considering distance and weather conditions
WITH vessel_routes AS (
    SELECT 
        vessel_id,
        nearest_port_name,
        distance_to_port_nm,
        storm_risk,
        -- Fuel consumption penalty for adverse weather (simplified model)
        ROUND(distance_to_port_nm * (1 + storm_risk * 0.3), 2) AS fuel_adjusted_distance,
        berths_available
    FROM iceberg.vessel_positions_enriched
    WHERE position_timestamp > CURRENT_TIMESTAMP - INTERVAL '30' MINUTE
)
SELECT 
    vessel_id,
    nearest_port_name,
    distance_to_port_nm AS actual_distance_nm,
    fuel_adjusted_distance AS fuel_equivalent_distance_nm,
    ROUND((fuel_adjusted_distance - distance_to_port_nm) / distance_to_port_nm * 100, 1) AS fuel_penalty_pct,
    berths_available,
    CASE 
        WHEN berths_available < 5 AND fuel_adjusted_distance > distance_to_port_nm * 1.2
            THEN 'Consider alternate port - high fuel cost and congestion'
        WHEN fuel_adjusted_distance > distance_to_port_nm * 1.2
            THEN 'High fuel consumption expected - weather impact'
        ELSE 'Optimal route'
    END AS routing_advice
FROM vessel_routes
ORDER BY fuel_adjusted_distance DESC;
"""
}

# Validation test cases with expected enriched fields
validation_test_cases = pd.DataFrame({
    'Test Case': [
        'Basic Enrichment',
        'Distance Calculation Accuracy',
        'Weather Zone Assignment',
        'Port Selection Logic',
        'Real-time Query Performance'
    ],
    'Input': [
        'Vessel at (1.3, 103.9)',
        'Vessel at (51.5, 4.3) should be ~30nm from Rotterdam',
        'Vessel near Singapore should get SEA_TROPICAL zone',
        'Vessel should select nearest port by great-circle distance',
        'Query enriched data within 100ms'
    ],
    'Expected Output': [
        'All fields populated: nearest_port_name, distance_to_port_nm, weather_zone',
        'Distance within 5% of actual great-circle distance',
        'weather_zone = SEA_TROPICAL, storm_risk = 0.15',
        'Nearest port matches minimum haversine distance',
        'P99 latency < 100ms for Cassandra, < 500ms for Iceberg'
    ],
    'Success Criteria': [
        'No null values in enriched fields',
        'Distance validation passed',
        'Correct weather zone lookup',
        'Port selection algorithm validated',
        'Query performance SLA met'
    ]
})

# Example enriched data for route optimization use
sample_enriched_fleet = pd.DataFrame({
    'vessel_id': ['IMO-9876543', 'IMO-8765432', 'IMO-7654321', 'IMO-6543210', 'IMO-5432109'],
    'latitude': [1.28, 51.82, 31.40, 22.35, 40.68],
    'longitude': [103.88, 4.52, 121.35, 114.12, -74.02],
    'nearest_port_name': ['Singapore', 'Rotterdam', 'Shanghai', 'Hong Kong', 'New York'],
    'distance_to_port_nm': [5.2, 12.8, 35.6, 8.4, 15.2],
    'weather_zone': ['SEA_TROPICAL', 'EUR_TEMPERATE', 'CHN_SUBTROPICAL', 'CHN_SUBTROPICAL', 'USA_ATLANTIC'],
    'berths_available': [45, 32, 55, 35, 25],
    'avg_wait_hours': [12, 18, 15, 14, 22],
    'storm_risk': [0.15, 0.25, 0.20, 0.20, 0.18]
})

# Calculate route optimization metrics
sample_enriched_fleet['eta_hours'] = np.round(sample_enriched_fleet['distance_to_port_nm'] / 15.0, 2)
sample_enriched_fleet['fuel_penalty_pct'] = np.round(sample_enriched_fleet['storm_risk'] * 30, 1)
sample_enriched_fleet['congestion_risk'] = sample_enriched_fleet['berths_available'].apply(
    lambda x: 'HIGH' if x < 30 else ('MODERATE' if x < 40 else 'LOW')
)

print("=" * 80)
print("ROUTE OPTIMIZATION QUERY EXAMPLES & VALIDATION")
print("=" * 80)

print("\n📊 Sample Enriched Fleet Data:")
print(sample_enriched_fleet.to_string(index=False))

print("\n🔍 Validation Test Cases:")
print(validation_test_cases.to_string(index=False))

print("\n🎯 Key Route Optimization Queries:")
for query_name, query_sql in route_optimization_queries.items():
    print(f"\n  • {query_name.replace('_', ' ').title()}")
    
print("\n✅ Success Criteria Validation:")
print(f"  ✓ All vessels have nearest_port_name: {sample_enriched_fleet['nearest_port_name'].notna().all()}")
print(f"  ✓ All vessels have distance_to_port_nm: {sample_enriched_fleet['distance_to_port_nm'].notna().all()}")
print(f"  ✓ All vessels have weather_zone: {sample_enriched_fleet['weather_zone'].notna().all()}")
print(f"  ✓ All vessels have berth availability data: {sample_enriched_fleet['berths_available'].notna().all()}")
print(f"  ✓ ETA calculations completed: {sample_enriched_fleet['eta_hours'].notna().all()}")

print("\n📈 Route Optimization Insights:")
print(f"  • Vessels within 1 hour of arrival: {(sample_enriched_fleet['eta_hours'] < 1).sum()}")
print(f"  • Vessels in high storm risk zones: {(sample_enriched_fleet['storm_risk'] > 0.2).sum()}")
print(f"  • Ports with high congestion risk: {(sample_enriched_fleet['congestion_risk'] == 'HIGH').sum()}")

print("\n📝 Query examples saved as 'route_optimization_queries' dict")

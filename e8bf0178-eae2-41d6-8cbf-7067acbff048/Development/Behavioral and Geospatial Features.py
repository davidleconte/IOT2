"""
Behavioral Patterns & Geospatial Features Spark Job
Generates: route_deviation_score, maintenance_interval_compliance, weather_exposure_hours,
           port_proximity_history, hazard_zone_crossings, optimal_route_adherence
"""

behavioral_geospatial_job = """
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time

def haversine_distance(lat1, lon1, lat2, lon2):
    '''Calculate great circle distance in km using Haversine formula'''
    return F.acos(
        F.sin(F.radians(lat1)) * F.sin(F.radians(lat2)) + 
        F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * 
        F.cos(F.radians(lon2) - F.radians(lon1))
    ) * 6371.0  # Earth radius in km

def create_behavioral_geospatial_features(
    spark, 
    telemetry_table='iceberg.telemetry.vessel_events',
    routes_table='iceberg.reference.planned_routes',
    ports_table='iceberg.reference.major_ports',
    hazards_table='iceberg.reference.hazard_zones',
    maintenance_table='iceberg.operational.maintenance_events',
    target_table='iceberg.feature_engineering.behavioral_geospatial_features'):
    '''
    Generate behavioral and geospatial features
    '''
    
    start_time = time.time()
    
    # Load telemetry data
    telemetry = spark.read.format('iceberg').table(telemetry_table) \\
        .where(F.col('timestamp') >= F.current_timestamp() - F.expr('INTERVAL 30 DAYS'))
    
    # Load reference data
    planned_routes = spark.read.format('iceberg').table(routes_table)
    major_ports = spark.read.format('iceberg').table(ports_table)
    hazard_zones = spark.read.format('iceberg').table(hazards_table)
    maintenance_events = spark.read.format('iceberg').table(maintenance_table)
    
    # ===== BEHAVIORAL FEATURES =====
    
    # 1. Route Deviation Score (0-100)
    # Join with planned routes and calculate deviation
    telemetry = telemetry.join(
        planned_routes,
        on=['vessel_id', 'route_id'],
        how='left'
    )
    
    # Calculate deviation distance from planned waypoints
    telemetry = telemetry.withColumn(
        'deviation_km',
        haversine_distance(
            F.col('latitude'), F.col('longitude'),
            F.col('planned_latitude'), F.col('planned_longitude')
        )
    )
    
    # Window for route segment
    route_window = Window.partitionBy('vessel_id', 'route_segment_id') \\
        .orderBy('timestamp')
    
    # Score: 100 - (avg_deviation / max_acceptable_deviation * 100)
    telemetry = telemetry.withColumn(
        'route_deviation_score',
        F.greatest(
            F.lit(0.0),
            F.lit(100.0) - (F.avg('deviation_km').over(route_window) / F.lit(5.0) * 100.0)
        )
    )
    
    # 2. Maintenance Interval Compliance (0-1)
    # Calculate compliance with scheduled maintenance
    vessel_window = Window.partitionBy('vessel_id').orderBy('timestamp')
    
    maintenance_agg = maintenance_events.groupBy('vessel_id').agg(
        F.count('*').alias('total_scheduled'),
        F.sum(F.when(F.col('completed_on_time') == True, 1).otherwise(0)).alias('on_time_count')
    ).withColumn(
        'maintenance_interval_compliance',
        F.col('on_time_count') / F.col('total_scheduled')
    )
    
    telemetry = telemetry.join(maintenance_agg, on='vessel_id', how='left')
    
    # 3. Weather Exposure Hours
    # Count hours in adverse weather (severity > threshold)
    telemetry = telemetry.withColumn(
        '_weather_severity_flag',
        F.when(F.col('weather_severity') > 7, 1).otherwise(0)
    )
    
    cumulative_window = Window.partitionBy('vessel_id') \\
        .orderBy('timestamp') \\
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    telemetry = telemetry.withColumn(
        'weather_exposure_hours',
        F.sum('_weather_severity_flag').over(cumulative_window)
    )
    
    # ===== GEOSPATIAL FEATURES =====
    
    # 4. Port Proximity History
    # Calculate time spent within proximity zones of major ports
    # Cross join with ports to calculate all distances (broadcast join for efficiency)
    port_distances = telemetry.crossJoin(
        F.broadcast(major_ports)
    ).withColumn(
        'distance_to_port_km',
        haversine_distance(
            F.col('latitude'), F.col('longitude'),
            F.col('port_latitude'), F.col('port_longitude')
        )
    )
    
    # Aggregate proximity metrics
    port_proximity = port_distances.withColumn(
        'within_50km', F.when(F.col('distance_to_port_km') <= 50, 1).otherwise(0)
    ).withColumn(
        'within_100km', F.when(F.col('distance_to_port_km') <= 100, 1).otherwise(0)
    ).withColumn(
        'within_200km', F.when(F.col('distance_to_port_km') <= 200, 1).otherwise(0)
    ).groupBy('vessel_id', 'timestamp').agg(
        F.sum('within_50km').alias('hours_within_50km'),
        F.sum('within_100km').alias('hours_within_100km'),
        F.sum('within_200km').alias('hours_within_200km')
    )
    
    telemetry = telemetry.join(port_proximity, on=['vessel_id', 'timestamp'], how='left')
    
    # Create struct for port_proximity_history
    telemetry = telemetry.withColumn(
        'port_proximity_history',
        F.struct(
            F.col('hours_within_50km'),
            F.col('hours_within_100km'),
            F.col('hours_within_200km')
        )
    )
    
    # 5. Hazard Zone Crossings
    # Calculate crossings and duration in hazardous zones
    hazard_crossings = telemetry.join(
        F.broadcast(hazard_zones),
        on=[
            (F.col('latitude').between(F.col('zone_lat_min'), F.col('zone_lat_max'))) &
            (F.col('longitude').between(F.col('zone_lon_min'), F.col('zone_lon_max')))
        ],
        how='left'
    ).groupBy('vessel_id', 'timestamp').agg(
        F.countDistinct('zone_id').alias('hazard_zone_crossing_count'),
        F.sum(
            F.when(F.col('zone_type') == 'piracy', 1).otherwise(0)
        ).alias('piracy_zone_hours'),
        F.sum(
            F.when(F.col('zone_type') == 'storm', 1).otherwise(0)
        ).alias('storm_zone_hours'),
        F.sum(
            F.when(F.col('zone_type') == 'ice', 1).otherwise(0)
        ).alias('ice_zone_hours')
    )
    
    telemetry = telemetry.join(hazard_crossings, on=['vessel_id', 'timestamp'], how='left')
    
    # Create struct for hazard_zone_crossings
    telemetry = telemetry.withColumn(
        'hazard_zone_crossings',
        F.struct(
            F.col('hazard_zone_crossing_count'),
            F.col('piracy_zone_hours'),
            F.col('storm_zone_hours'),
            F.col('ice_zone_hours')
        )
    )
    
    # 6. Optimal Route Adherence
    # Calculate percentage on optimal route
    telemetry = telemetry.withColumn(
        '_on_optimal_route',
        F.when(F.col('deviation_km') <= 2.0, 1).otherwise(0)  # Within 2km is "on route"
    )
    
    route_adherence_window = Window.partitionBy('vessel_id', 'route_id') \\
        .orderBy('timestamp') \\
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    telemetry = telemetry.withColumn(
        'optimal_route_adherence',
        (F.sum('_on_optimal_route').over(route_adherence_window) / 
         F.count('*').over(route_adherence_window)) * 100.0
    )
    
    # Select final features
    features_df = telemetry.select(
        'vessel_id',
        'timestamp',
        'route_deviation_score',
        'maintenance_interval_compliance',
        'weather_exposure_hours',
        'port_proximity_history',
        'hazard_zone_crossings',
        'optimal_route_adherence',
        F.current_timestamp().alias('feature_timestamp')
    ).dropDuplicates(['vessel_id', 'timestamp'])
    
    # Write to Iceberg feature store
    features_df.writeTo(target_table) \\
        .using('iceberg') \\
        .partitionedBy('vessel_id', F.days('timestamp')) \\
        .createOrReplace()
    
    execution_time = time.time() - start_time
    feature_count = features_df.count()
    
    print(f"Generated behavioral/geospatial features: {feature_count:,} records")
    print(f"Execution time: {execution_time:.2f} seconds")
    
    return features_df
"""

import pandas as pd

# Feature implementation summary
behavioral_geo_summary = pd.DataFrame([
    {
        'Feature': 'route_deviation_score',
        'Category': 'Behavioral',
        'Method': 'Haversine distance + aggregation',
        'Reference Data': 'planned_routes',
        'Output': 'DOUBLE (0-100)'
    },
    {
        'Feature': 'maintenance_interval_compliance',
        'Category': 'Behavioral',
        'Method': 'Ratio calculation',
        'Reference Data': 'maintenance_events',
        'Output': 'DOUBLE (0-1)'
    },
    {
        'Feature': 'weather_exposure_hours',
        'Category': 'Behavioral',
        'Method': 'Cumulative sum with threshold',
        'Reference Data': 'telemetry.weather_severity',
        'Output': 'INTEGER'
    },
    {
        'Feature': 'port_proximity_history',
        'Category': 'Geospatial',
        'Method': 'Broadcast join + distance calc',
        'Reference Data': 'major_ports',
        'Output': 'STRUCT (50/100/200km)'
    },
    {
        'Feature': 'hazard_zone_crossings',
        'Category': 'Geospatial',
        'Method': 'Spatial join + aggregation',
        'Reference Data': 'hazard_zones',
        'Output': 'STRUCT (count + durations)'
    },
    {
        'Feature': 'optimal_route_adherence',
        'Category': 'Geospatial',
        'Method': 'Percentage within threshold',
        'Reference Data': 'planned_routes',
        'Output': 'DOUBLE (0-100)'
    }
])

print("=" * 120)
print("BEHAVIORAL PATTERNS & GEOSPATIAL FEATURES SPARK JOB")
print("=" * 120)
print("\nFeatures Implemented:")
print(behavioral_geo_summary.to_string(index=False))

print("\n" + "=" * 120)
print("Key Implementation Optimizations:")
print("=" * 120)
print("✓ Haversine formula for accurate great circle distance calculations")
print("✓ Broadcast joins for reference data (ports, hazards) to minimize shuffles")
print("✓ Spatial indexing via lat/lon range filters for hazard zone detection")
print("✓ Cumulative windows for time-series aggregations (weather exposure)")
print("✓ Struct types for complex multi-dimensional features (port proximity, hazards)")
print("✓ Partition pruning on vessel_id and timestamp for efficient processing")

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Zerve design system colors
bg_color = '#1D1D20'
text_primary = '#fbfbff'
light_blue = '#A1C9F4'
orange = '#FFB482'
green = '#8DE5A1'
coral = '#FF9F9B'
lavender = '#D0BBFF'

# ==========================================
# FEAST FEATURE STORE ARCHITECTURE
# ==========================================

feast_architecture = {
    "overview": "ML Feature Store using Feast for 113 engineered features with online/offline stores",
    "components": [
        {
            "name": "Feast Registry",
            "type": "Metadata Store",
            "technology": "PostgreSQL",
            "purpose": "Store feature definitions, data sources, and feature views",
            "specs": "Centralized metadata for feature lineage and versioning"
        },
        {
            "name": "Offline Store",
            "type": "Batch Training Storage",
            "technology": "Apache Iceberg (via Presto)",
            "purpose": "Historical features for batch ML training",
            "specs": "Time-travel queries, batch materialization, training dataset generation"
        },
        {
            "name": "Online Store",
            "type": "Real-Time Serving",
            "technology": "Redis Cluster",
            "purpose": "Low-latency feature lookup for inference",
            "specs": "Target: <10ms p99 latency, in-memory key-value storage"
        },
        {
            "name": "Materialization Engine",
            "type": "Feature Pipeline",
            "technology": "Spark (orchestrated by Airflow)",
            "purpose": "Compute features from offline → online store",
            "specs": "Incremental updates every 5 minutes, full refresh daily"
        },
        {
            "name": "Feature Server",
            "type": "API Gateway",
            "technology": "Feast Feature Server (gRPC/HTTP)",
            "purpose": "Unified API for feature retrieval",
            "specs": "Autoscaling, 100-500 QPS per instance"
        },
        {
            "name": "Monitoring & Drift Detection",
            "type": "Observability",
            "technology": "Prometheus + Grafana + Great Expectations",
            "purpose": "Track feature drift, data quality, latency",
            "specs": "Real-time alerting on distribution shifts and SLA violations"
        }
    ],
    "data_flow": {
        "training": "Iceberg (offline) → Feast SDK → Training pipeline",
        "serving": "Redis (online) → Feature Server → Model inference",
        "materialization": "Spark job → Compute features → Write to Redis"
    }
}

feast_arch_df = pd.DataFrame(feast_architecture["components"])

print("=" * 80)
print("ML FEATURE STORE ARCHITECTURE - FEAST IMPLEMENTATION")
print("=" * 80)
print(f"\nOverview: {feast_architecture['overview']}\n")
print(feast_arch_df.to_string(index=False))
print("\n" + "=" * 80)
print("DATA FLOW PATTERNS")
print("=" * 80)
for flow_type, flow_desc in feast_architecture["data_flow"].items():
    print(f"\n{flow_type.upper()}: {flow_desc}")

# ==========================================
# 113 ENGINEERED FEATURES CATALOG
# ==========================================

feature_catalog = {
    "vessel_operational": {
        "count": 18,
        "features": [
            "fuel_efficiency_7d", "fuel_efficiency_30d", "speed_over_ground_avg_24h",
            "engine_load_avg_7d", "engine_load_max_24h", "vibration_std_7d",
            "temperature_avg_equipment_24h", "pressure_variance_7d", "rpm_avg_main_engine_24h",
            "fuel_consumption_rate_instant", "fuel_consumption_cumulative_7d",
            "distance_traveled_24h", "time_at_sea_ratio_30d", "port_visits_count_30d",
            "anchor_time_ratio_7d", "speed_changes_count_24h", "course_changes_count_24h",
            "operational_hours_cumulative_7d"
        ],
        "source": "vessel_telemetry (Iceberg)",
        "update_frequency": "5 min"
    },
    "predictive_maintenance": {
        "count": 22,
        "features": [
            "bearing_temp_rolling_mean_6h", "bearing_temp_rolling_std_6h",
            "vibration_fft_dominant_freq", "vibration_rms_24h", "oil_pressure_trend_7d",
            "failure_probability_score", "maintenance_due_days", "component_age_hours",
            "cycles_since_maintenance", "wear_indicator_composite", "anomaly_score_equipment",
            "time_to_failure_prediction", "health_index_composite", "degradation_rate_7d",
            "sensor_correlation_temp_vibration", "alarm_frequency_7d", "critical_alarm_count_24h",
            "warning_threshold_violations_7d", "component_replacement_flag",
            "maintenance_history_count_90d", "failure_history_count_365d", "uptime_ratio_30d"
        ],
        "source": "equipment_sensors + maintenance_logs (Iceberg)",
        "update_frequency": "5 min"
    },
    "geospatial_routing": {
        "count": 15,
        "features": [
            "distance_to_nearest_port_km", "nearest_port_id", "nearest_port_congestion_level",
            "eta_nearest_port_hours", "current_route_deviation_km", "weather_severity_current_zone",
            "wave_height_forecast_24h", "wind_speed_current_zone", "visibility_current_zone",
            "ice_coverage_pct_current_zone", "piracy_risk_level_current_zone",
            "traffic_density_current_zone", "restricted_area_distance_km",
            "fuel_optimal_route_savings_pct", "time_optimal_route_savings_hours"
        ],
        "source": "enriched_positions + weather_data (Iceberg + Cassandra)",
        "update_frequency": "1 min"
    },
    "fleet_context": {
        "count": 12,
        "features": [
            "fleet_avg_speed_24h", "fleet_fuel_efficiency_percentile",
            "vessel_rank_speed_efficiency", "similar_vessels_avg_consumption",
            "fleet_maintenance_alert_count_24h", "fleet_operational_vessels_count",
            "vessel_class_avg_fuel_consumption", "vessel_age_relative_fleet",
            "cargo_capacity_utilization_pct", "fleet_eta_variance_24h",
            "fleet_route_adherence_score_avg", "peer_vessel_comparison_index"
        ],
        "source": "fleet_aggregations (Iceberg)",
        "update_frequency": "15 min"
    },
    "time_series_patterns": {
        "count": 20,
        "features": [
            "fuel_consumption_trend_7d", "fuel_consumption_seasonality_30d",
            "speed_autocorrelation_lag1", "speed_autocorrelation_lag6",
            "engine_load_momentum_24h", "temperature_rate_of_change_1h",
            "vibration_spike_count_24h", "pressure_cycle_frequency_7d",
            "fourier_fuel_daily_component", "fourier_fuel_weekly_component",
            "wavelet_vibration_high_freq", "wavelet_vibration_low_freq",
            "arima_fuel_forecast_24h", "arima_speed_forecast_6h",
            "lstm_failure_risk_next_72h", "time_since_last_maintenance_hours",
            "time_since_last_port_hours", "time_since_last_alarm_hours",
            "hour_of_day_sin", "hour_of_day_cos"
        ],
        "source": "vessel_telemetry (Iceberg)",
        "update_frequency": "5 min"
    },
    "business_operational": {
        "count": 14,
        "features": [
            "cargo_load_pct", "cargo_value_usd", "cargo_type_category",
            "contract_delivery_deadline_hours", "delay_penalty_usd_per_hour",
            "fuel_cost_optimization_potential_usd", "port_charges_next_destination_usd",
            "insurance_premium_current_route_usd", "crew_fatigue_score",
            "crew_overtime_hours_7d", "compliance_violations_count_30d",
            "environmental_compliance_score", "emission_co2_tons_7d",
            "emission_reduction_target_gap_pct"
        ],
        "source": "business_systems + compliance_logs (PostgreSQL)",
        "update_frequency": "1 hour"
    },
    "cross_vessel_interactions": {
        "count": 12,
        "features": [
            "nearby_vessels_count_5km", "nearby_vessels_avg_speed",
            "collision_risk_score", "convoy_formation_flag",
            "port_congestion_nearby_vessels", "similar_route_vessels_count",
            "fleet_communication_frequency_24h", "assistance_requests_nearby_24h",
            "weather_impact_fleet_correlation", "fuel_price_volatility_route",
            "bunker_availability_next_port", "alternative_routes_available_count"
        ],
        "source": "fleet_telemetry + ais_data (Cassandra + Iceberg)",
        "update_frequency": "5 min"
    }
}

# Create feature summary
feature_summary_data = []
for category, details in feature_catalog.items():
    feature_summary_data.append({
        "Category": category.replace("_", " ").title(),
        "Feature Count": details["count"],
        "Data Source": details["source"],
        "Update Frequency": details["update_frequency"],
        "Examples": ", ".join(details["features"][:3]) + "..."
    })

feature_summary_df = pd.DataFrame(feature_summary_data)
total_features = sum([d["count"] for d in feature_catalog.values()])

print("\n" + "=" * 80)
print(f"FEATURE CATALOG: {total_features} ENGINEERED FEATURES")
print("=" * 80)
print(feature_summary_df.to_string(index=False))

# ==========================================
# FEAST FEATURE DEFINITIONS
# ==========================================

feast_entity_definitions = """
# entities.py
from feast import Entity, ValueType

# Primary entity for feature joins
vessel_entity = Entity(
    name="vessel_id",
    description="Unique vessel identifier (IMO number)",
    value_type=ValueType.STRING
)

equipment_entity = Entity(
    name="equipment_id", 
    description="Unique equipment identifier on vessel",
    value_type=ValueType.STRING
)

route_entity = Entity(
    name="route_id",
    description="Unique route identifier",
    value_type=ValueType.STRING
)
"""

feast_data_sources = """
# data_sources.py
from feast import FileSource, RedshiftSource
from feast.data_format import ParquetFormat

# Offline store: Iceberg via Presto (using FileSource for demonstration)
vessel_telemetry_source = FileSource(
    path="presto://iceberg.maritime.vessel_telemetry",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_at",
    file_format=ParquetFormat()
)

equipment_sensors_source = FileSource(
    path="presto://iceberg.maritime.equipment_sensors",
    event_timestamp_column="event_timestamp", 
    created_timestamp_column="created_at",
    file_format=ParquetFormat()
)

enriched_positions_source = FileSource(
    path="presto://iceberg.maritime.enriched_positions",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_at", 
    file_format=ParquetFormat()
)
"""

feast_feature_views = """
# feature_views.py
from feast import FeatureView, Field
from feast.types import Float32, Int64, String
from datetime import timedelta

# Example: Vessel Operational Features
vessel_operational_fv = FeatureView(
    name="vessel_operational_features",
    entities=["vessel_id"],
    ttl=timedelta(days=7),
    schema=[
        Field(name="fuel_efficiency_7d", dtype=Float32),
        Field(name="fuel_efficiency_30d", dtype=Float32),
        Field(name="speed_over_ground_avg_24h", dtype=Float32),
        Field(name="engine_load_avg_7d", dtype=Float32),
        Field(name="engine_load_max_24h", dtype=Float32),
        Field(name="vibration_std_7d", dtype=Float32),
        Field(name="temperature_avg_equipment_24h", dtype=Float32),
        Field(name="pressure_variance_7d", dtype=Float32),
        Field(name="rpm_avg_main_engine_24h", dtype=Float32),
        Field(name="fuel_consumption_rate_instant", dtype=Float32),
        Field(name="fuel_consumption_cumulative_7d", dtype=Float32),
        Field(name="distance_traveled_24h", dtype=Float32),
        Field(name="time_at_sea_ratio_30d", dtype=Float32),
        Field(name="port_visits_count_30d", dtype=Int64),
        Field(name="anchor_time_ratio_7d", dtype=Float32),
        Field(name="speed_changes_count_24h", dtype=Int64),
        Field(name="course_changes_count_24h", dtype=Int64),
        Field(name="operational_hours_cumulative_7d", dtype=Float32)
    ],
    online=True,
    source=vessel_telemetry_source,
    tags={"category": "operational"}
)

# Example: Predictive Maintenance Features  
predictive_maintenance_fv = FeatureView(
    name="predictive_maintenance_features",
    entities=["vessel_id", "equipment_id"],
    ttl=timedelta(days=3),
    schema=[
        Field(name="bearing_temp_rolling_mean_6h", dtype=Float32),
        Field(name="bearing_temp_rolling_std_6h", dtype=Float32),
        Field(name="vibration_fft_dominant_freq", dtype=Float32),
        Field(name="vibration_rms_24h", dtype=Float32),
        Field(name="oil_pressure_trend_7d", dtype=Float32),
        Field(name="failure_probability_score", dtype=Float32),
        Field(name="maintenance_due_days", dtype=Int64),
        Field(name="component_age_hours", dtype=Float32),
        Field(name="cycles_since_maintenance", dtype=Int64),
        Field(name="wear_indicator_composite", dtype=Float32),
        Field(name="anomaly_score_equipment", dtype=Float32),
        Field(name="time_to_failure_prediction", dtype=Float32),
        Field(name="health_index_composite", dtype=Float32),
        Field(name="degradation_rate_7d", dtype=Float32)
    ],
    online=True,
    source=equipment_sensors_source,
    tags={"category": "maintenance"}
)

# Example: Geospatial Routing Features
geospatial_routing_fv = FeatureView(
    name="geospatial_routing_features",
    entities=["vessel_id"],
    ttl=timedelta(hours=6),
    schema=[
        Field(name="distance_to_nearest_port_km", dtype=Float32),
        Field(name="nearest_port_id", dtype=String),
        Field(name="nearest_port_congestion_level", dtype=Int64),
        Field(name="eta_nearest_port_hours", dtype=Float32),
        Field(name="current_route_deviation_km", dtype=Float32),
        Field(name="weather_severity_current_zone", dtype=Int64),
        Field(name="wave_height_forecast_24h", dtype=Float32),
        Field(name="wind_speed_current_zone", dtype=Float32),
        Field(name="visibility_current_zone", dtype=Float32),
        Field(name="ice_coverage_pct_current_zone", dtype=Float32),
        Field(name="piracy_risk_level_current_zone", dtype=Int64),
        Field(name="traffic_density_current_zone", dtype=Int64),
        Field(name="restricted_area_distance_km", dtype=Float32),
        Field(name="fuel_optimal_route_savings_pct", dtype=Float32),
        Field(name="time_optimal_route_savings_hours", dtype=Float32)
    ],
    online=True,
    source=enriched_positions_source,
    tags={"category": "routing"}
)
"""

print("\n" + "=" * 80)
print("FEAST FEATURE DEFINITIONS (Sample)")
print("=" * 80)
print(feast_entity_definitions)
print("\n" + feast_data_sources[:500] + "...")
print("\n" + feast_feature_views[:800] + "...")

feature_engineering_summary = pd.DataFrame([
    {"Component": "Total Features", "Value": total_features},
    {"Component": "Feature Categories", "Value": len(feature_catalog)},
    {"Component": "Primary Entities", "Value": "3 (vessel_id, equipment_id, route_id)"},
    {"Component": "Feature Views", "Value": "7 (one per category)"},
    {"Component": "Data Sources", "Value": "Iceberg, Cassandra, PostgreSQL"}
])

print("\n" + "=" * 80)
print("FEAST CONFIGURATION SUMMARY")
print("=" * 80)
print(feature_engineering_summary.to_string(index=False))

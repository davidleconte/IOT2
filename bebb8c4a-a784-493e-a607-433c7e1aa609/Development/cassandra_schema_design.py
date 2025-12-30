"""
DataStax HCD (Cassandra) Multi-Tenant Schema Design
Implements per-tenant keyspace isolation with efficient partition keys,
TTL policies, and compaction strategies for maritime vessel tracking.
"""
import os
import yaml

# ========================================
# Schema Design Strategy
# ========================================

# Strategy: Per-tenant keyspace isolation for complete data separation
# Each tenant gets their own keyspace with tenant_id prefix
# Partition keys optimized for: tenant_id, vessel_id, time bucketing

schema_strategy = {
    "isolation_model": "per_tenant_keyspace",
    "partition_strategy": "compound_keys_with_time_bucketing",
    "consistency_levels": {
        "telemetry_writes": "LOCAL_ONE",  # High throughput
        "metadata_reads": "LOCAL_QUORUM",  # Strong consistency
        "feature_store_reads": "LOCAL_ONE",  # Low latency for online features
        "audit_writes": "LOCAL_QUORUM"  # Durability
    },
    "ttl_policies": {
        "raw_telemetry": "90 days",
        "aggregated_metrics": "2 years",
        "alerts": "1 year",
        "audit_logs": "7 years",
        "feature_store": "30 days"  # Feast online store
    }
}

# ========================================
# Table Schemas for Each Tenant
# ========================================

# 1. VESSEL TELEMETRY - Time-series optimized
telemetry_schema = {
    "table_name": "vessel_telemetry",
    "description": "High-throughput time-series data from vessel sensors",
    "partition_key": ["vessel_id", "day_bucket"],  # Time bucketing by day
    "clustering_key": ["timestamp", "sensor_type"],
    "columns": {
        "vessel_id": "uuid",
        "day_bucket": "date",  # YYYY-MM-DD for partition distribution
        "timestamp": "timestamp",
        "sensor_type": "text",
        "latitude": "decimal",
        "longitude": "decimal",
        "speed_knots": "decimal",
        "heading": "decimal",
        "fuel_consumption": "decimal",
        "engine_rpm": "int",
        "engine_temp": "decimal",
        "cargo_weight": "decimal",
        "weather_conditions": "text",
        "raw_payload": "text"  # JSON for flexible schema
    },
    "clustering_order": "timestamp DESC",
    "ttl": 7776000,  # 90 days in seconds
    "compaction": {
        "class": "TimeWindowCompactionStrategy",
        "compaction_window_unit": "DAYS",
        "compaction_window_size": 1
    },
    "bloom_filter_fp_chance": 0.01,
    "access_pattern": "Recent data queries by vessel and time range"
}

# 2. VESSEL METADATA - Reference data
metadata_schema = {
    "table_name": "vessel_metadata",
    "description": "Static and semi-static vessel information",
    "partition_key": ["vessel_id"],
    "clustering_key": [],
    "columns": {
        "vessel_id": "uuid",
        "vessel_name": "text",
        "imo_number": "text",
        "vessel_type": "text",
        "flag": "text",
        "gross_tonnage": "int",
        "deadweight_tonnage": "int",
        "length_meters": "decimal",
        "beam_meters": "decimal",
        "year_built": "int",
        "owner": "text",
        "operator": "text",
        "home_port": "text",
        "sensor_config": "text",  # JSON
        "last_updated": "timestamp",
        "status": "text"
    },
    "ttl": None,  # No expiration for metadata
    "compaction": {
        "class": "LeveledCompactionStrategy"
    },
    "access_pattern": "Point reads by vessel_id"
}

# 3. ALERTS - Event tracking
alerts_schema = {
    "table_name": "vessel_alerts",
    "description": "Generated alerts and anomalies",
    "partition_key": ["vessel_id", "month_bucket"],
    "clustering_key": ["timestamp", "alert_id"],
    "columns": {
        "vessel_id": "uuid",
        "month_bucket": "text",  # YYYY-MM
        "timestamp": "timestamp",
        "alert_id": "uuid",
        "alert_type": "text",  # route_deviation, fuel_anomaly, engine_warning, etc.
        "severity": "text",  # critical, high, medium, low
        "title": "text",
        "description": "text",
        "sensor_readings": "text",  # JSON
        "ml_model_version": "text",
        "confidence_score": "decimal",
        "acknowledged": "boolean",
        "acknowledged_by": "text",
        "acknowledged_at": "timestamp",
        "resolved": "boolean",
        "resolved_at": "timestamp"
    },
    "clustering_order": "timestamp DESC",
    "ttl": 31536000,  # 1 year
    "compaction": {
        "class": "TimeWindowCompactionStrategy",
        "compaction_window_unit": "DAYS",
        "compaction_window_size": 7
    },
    "access_pattern": "Recent alerts by vessel, filtered by severity"
}

# 4. FEATURE STORE ONLINE TABLES - Feast integration
feature_store_schema = {
    "table_name": "feast_online_features",
    "description": "Online feature store for ML model serving (Feast integration)",
    "partition_key": ["feature_view_name", "entity_id"],
    "clustering_key": ["event_timestamp"],
    "columns": {
        "feature_view_name": "text",  # e.g., vessel_operational_features
        "entity_id": "text",  # vessel_id as string
        "event_timestamp": "timestamp",
        "created_timestamp": "timestamp",
        "feature_data": "text",  # JSON blob with all features
        "fuel_efficiency_7d": "decimal",
        "avg_speed_7d": "decimal",
        "route_deviation_score": "decimal",
        "engine_health_score": "decimal",
        "maintenance_prediction": "decimal",
        "eta_accuracy_score": "decimal"
    },
    "clustering_order": "event_timestamp DESC",
    "ttl": 2592000,  # 30 days
    "compaction": {
        "class": "LeveledCompactionStrategy"  # Frequent updates
    },
    "bloom_filter_fp_chance": 0.001,  # Low latency requirement
    "access_pattern": "Point reads by feature_view and entity for ML inference"
}

# 5. AUDIT LOGS - Compliance tracking
audit_schema = {
    "table_name": "audit_logs",
    "description": "Audit trail for compliance and security",
    "partition_key": ["year_month"],  # YYYY-MM
    "clustering_key": ["timestamp", "log_id"],
    "columns": {
        "year_month": "text",
        "timestamp": "timestamp",
        "log_id": "uuid",
        "user_id": "text",
        "action": "text",  # read, write, delete, admin_action
        "resource_type": "text",  # vessel, alert, feature, etc.
        "resource_id": "text",
        "ip_address": "text",
        "user_agent": "text",
        "request_details": "text",  # JSON
        "response_status": "int",
        "error_message": "text"
    },
    "clustering_order": "timestamp DESC",
    "ttl": 220752000,  # 7 years (compliance requirement)
    "compaction": {
        "class": "TimeWindowCompactionStrategy",
        "compaction_window_unit": "DAYS",
        "compaction_window_size": 30
    },
    "access_pattern": "Time-range queries for audit reports"
}

# ========================================
# Supporting Tables (Shared or Per-Tenant)
# ========================================

# Secondary index table for vessel lookup by IMO
vessel_by_imo_schema = {
    "table_name": "vessel_by_imo",
    "description": "Secondary index for vessel lookup by IMO number",
    "partition_key": ["imo_number"],
    "clustering_key": [],
    "columns": {
        "imo_number": "text",
        "vessel_id": "uuid",
        "vessel_name": "text"
    },
    "ttl": None,
    "compaction": {
        "class": "LeveledCompactionStrategy"
    }
}

# Materialized view alternative for alert counts
alert_counts_schema = {
    "table_name": "alert_counts_by_type",
    "description": "Aggregated alert counts for dashboard queries",
    "partition_key": ["vessel_id", "date_bucket"],
    "clustering_key": ["alert_type"],
    "columns": {
        "vessel_id": "uuid",
        "date_bucket": "date",
        "alert_type": "text",
        "count": "counter"
    },
    "ttl": 63072000,  # 2 years
    "compaction": {
        "class": "LeveledCompactionStrategy"
    }
}

all_schemas = {
    "vessel_telemetry": telemetry_schema,
    "vessel_metadata": metadata_schema,
    "vessel_alerts": alerts_schema,
    "feast_online_features": feature_store_schema,
    "audit_logs": audit_schema,
    "vessel_by_imo": vessel_by_imo_schema,
    "alert_counts_by_type": alert_counts_schema
}

# Summary output
print("=" * 80)
print("DataStax HCD (Cassandra) Multi-Tenant Schema Design")
print("=" * 80)
print(f"\n✓ Isolation Model: {schema_strategy['isolation_model']}")
print(f"✓ Partition Strategy: {schema_strategy['partition_strategy']}")
print(f"\n📊 Schema Definitions: {len(all_schemas)} tables")
for table_name, schema in all_schemas.items():
    print(f"  • {table_name}: {schema['description']}")

print(f"\n🔒 Consistency Levels:")
for operation, level in schema_strategy['consistency_levels'].items():
    print(f"  • {operation}: {level}")

print(f"\n⏰ TTL Policies:")
for data_type, ttl in schema_strategy['ttl_policies'].items():
    print(f"  • {data_type}: {ttl}")

print("\n✅ Schema design complete with tenant isolation, optimized partition keys,")
print("   and production-ready consistency/compaction strategies")

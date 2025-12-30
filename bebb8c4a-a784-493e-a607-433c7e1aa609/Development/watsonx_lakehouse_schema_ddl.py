import yaml
from pathlib import Path

# Create directory structure for watsonx.data lakehouse configuration
lakehouse_dirs = [
    'ops/watsonx-data/catalog',
    'ops/watsonx-data/schemas',
    'ops/watsonx-data/tables',
    'ops/watsonx-data/presto',
    'ops/watsonx-data/spark'
]
for dir_path in lakehouse_dirs:
    Path(dir_path).mkdir(parents=True, exist_ok=True)

# Tenant configurations for multi-tenant Iceberg lakehouse
lakehouse_tenants = [
    {'tenant_id': 'shipping_co_alpha', 'display_name': 'Shipping Company Alpha'},
    {'tenant_id': 'logistics_beta', 'display_name': 'Logistics Provider Beta'},
    {'tenant_id': 'maritime_gamma', 'display_name': 'Maritime Services Gamma'}
]

# Generate Iceberg Catalog Configuration
catalog_config = {
    'catalog-name': 'maritime_iceberg',
    'catalog-type': 'iceberg',
    'connector.name': 'iceberg',
    'iceberg.catalog.type': 'hive',
    'hive.metastore.uri': 'thrift://hive-metastore:9083',
    'warehouse': 's3://maritime-lakehouse/warehouse',
    's3.endpoint': 'https://s3.us-east-1.amazonaws.com',
    's3.path-style-access': 'true',
    'iceberg.file-format': 'PARQUET',
    'iceberg.compression-codec': 'ZSTD',
    'iceberg.target-file-size-bytes': '536870912',  # 512MB
    'iceberg.parquet.row-group-size-bytes': '134217728',  # 128MB
    'iceberg.parquet.page-size-bytes': '1048576',  # 1MB
    'iceberg.parquet.dict-size-bytes': '2097152',  # 2MB
    'iceberg.min-snapshots-to-keep': '20',
    'iceberg.max-snapshot-age-ms': '2592000000',  # 30 days
    'iceberg.delete-orphan-files-older-than-ms': '259200000',  # 3 days
    'iceberg.split-size': '134217728',  # 128MB for query planning
    'iceberg.allow-incompatible-schema-changes': 'false'
}

catalog_path = 'ops/watsonx-data/catalog/maritime_iceberg.properties'
with open(catalog_path, 'w') as f:
    for key, value in catalog_config.items():
        f.write(f"{key}={value}\n")

print("✓ Generated Iceberg Catalog Configuration:")
print(f"  - {catalog_path}")
print()

# Generate SQL DDL for multi-tenant schema structure
ddl_statements = []

# Create tenant-specific schemas (namespaces)
ddl_statements.append("-- ============================================")
ddl_statements.append("-- TENANT SCHEMA CREATION")
ddl_statements.append("-- ============================================\n")

tenant_schemas = []
for tenant in lakehouse_tenants:
    tenant_id = tenant['tenant_id']
    tenant_schemas.append(tenant_id)
    
    ddl_statements.append(f"-- Tenant: {tenant['display_name']}")
    ddl_statements.append(f"CREATE SCHEMA IF NOT EXISTS maritime_iceberg.{tenant_id}")
    ddl_statements.append(f"WITH (location = 's3://maritime-lakehouse/warehouse/{tenant_id}');\n")

# Create historical telemetry tables per tenant
ddl_statements.append("\n-- ============================================")
ddl_statements.append("-- HISTORICAL TELEMETRY TABLES")
ddl_statements.append("-- Multi-level partitioning: tenant_id, vessel_id, date")
ddl_statements.append("-- ============================================\n")

telemetry_ddl = """CREATE TABLE IF NOT EXISTS maritime_iceberg.{tenant_id}.historical_telemetry (
    message_id VARCHAR,
    tenant_id VARCHAR NOT NULL,
    vessel_id VARCHAR NOT NULL,
    vessel_name VARCHAR,
    imo_number VARCHAR,
    timestamp_utc TIMESTAMP(6) NOT NULL,
    event_date DATE NOT NULL,
    latitude DOUBLE,
    longitude DOUBLE,
    speed_knots DOUBLE,
    heading_degrees DOUBLE,
    draft_meters DOUBLE,
    navigation_status VARCHAR,
    destination_port VARCHAR,
    eta_utc TIMESTAMP(6),
    cargo_weight_tons DOUBLE,
    fuel_consumption_rate DOUBLE,
    engine_rpm INTEGER,
    weather_conditions ROW(
        temperature_celsius DOUBLE,
        wind_speed_knots DOUBLE,
        wind_direction_degrees DOUBLE,
        wave_height_meters DOUBLE,
        visibility_km DOUBLE
    ),
    sensor_data ROW(
        pressure_bar DOUBLE,
        vibration_level DOUBLE,
        temperature_engine DOUBLE
    ),
    compliance_flags ARRAY(VARCHAR),
    data_quality_score DOUBLE,
    ingestion_timestamp TIMESTAMP(6),
    source_system VARCHAR,
    record_version INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tenant_id', 'vessel_id', 'event_date'],
    sorted_by = ARRAY['timestamp_utc'],
    location = 's3://maritime-lakehouse/warehouse/{tenant_id}/historical_telemetry',
    compression_codec = 'ZSTD'
);
"""

for tenant in lakehouse_tenants:
    tenant_id = tenant['tenant_id']
    ddl_statements.append(f"-- Tenant: {tenant['display_name']}")
    ddl_statements.append(telemetry_ddl.format(tenant_id=tenant_id))

# Create processed features tables
ddl_statements.append("\n-- ============================================")
ddl_statements.append("-- PROCESSED FEATURES TABLES")
ddl_statements.append("-- Engineered features for ML model training")
ddl_statements.append("-- ============================================\n")

features_ddl = """CREATE TABLE IF NOT EXISTS maritime_iceberg.{tenant_id}.processed_features (
    feature_id VARCHAR,
    tenant_id VARCHAR NOT NULL,
    vessel_id VARCHAR NOT NULL,
    event_date DATE NOT NULL,
    timestamp_utc TIMESTAMP(6) NOT NULL,
    
    -- Temporal features
    hour_of_day INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN,
    
    -- Vessel motion features
    speed_mean_1h DOUBLE,
    speed_std_1h DOUBLE,
    speed_max_1h DOUBLE,
    heading_change_rate DOUBLE,
    acceleration DOUBLE,
    
    -- Port proximity features
    distance_to_nearest_port_km DOUBLE,
    eta_deviation_hours DOUBLE,
    port_approach_speed DOUBLE,
    
    -- Operational features
    fuel_efficiency DOUBLE,
    engine_load_percentage DOUBLE,
    cargo_utilization_ratio DOUBLE,
    draft_change_rate DOUBLE,
    
    -- Weather impact features
    weather_severity_score DOUBLE,
    wind_resistance_factor DOUBLE,
    wave_impact_score DOUBLE,
    
    -- Anomaly detection features
    speed_anomaly_score DOUBLE,
    route_deviation_score DOUBLE,
    sensor_anomaly_flags ARRAY(VARCHAR),
    
    -- Rolling aggregates
    rolling_24h_distance_nm DOUBLE,
    rolling_24h_fuel_consumption DOUBLE,
    rolling_7d_avg_speed DOUBLE,
    
    -- Compliance features
    eca_zone_violations INTEGER,
    speed_limit_violations INTEGER,
    route_restriction_violations INTEGER,
    
    feature_version VARCHAR,
    created_timestamp TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tenant_id', 'vessel_id', 'event_date'],
    sorted_by = ARRAY['timestamp_utc'],
    location = 's3://maritime-lakehouse/warehouse/{tenant_id}/processed_features',
    compression_codec = 'ZSTD'
);
"""

for tenant in lakehouse_tenants:
    tenant_id = tenant['tenant_id']
    ddl_statements.append(f"-- Tenant: {tenant['display_name']}")
    ddl_statements.append(features_ddl.format(tenant_id=tenant_id))

# Create model training datasets table
ddl_statements.append("\n-- ============================================")
ddl_statements.append("-- MODEL TRAINING DATASETS")
ddl_statements.append("-- Curated datasets for ML model training")
ddl_statements.append("-- ============================================\n")

training_ddl = """CREATE TABLE IF NOT EXISTS maritime_iceberg.{tenant_id}.model_training_datasets (
    dataset_id VARCHAR,
    tenant_id VARCHAR NOT NULL,
    dataset_name VARCHAR NOT NULL,
    dataset_version VARCHAR NOT NULL,
    model_type VARCHAR,
    created_date DATE NOT NULL,
    
    -- Training configuration
    training_start_date DATE,
    training_end_date DATE,
    vessel_ids ARRAY(VARCHAR),
    feature_columns ARRAY(VARCHAR),
    target_variable VARCHAR,
    
    -- Dataset statistics
    total_records BIGINT,
    train_records BIGINT,
    validation_records BIGINT,
    test_records BIGINT,
    
    -- Data quality metrics
    completeness_score DOUBLE,
    consistency_score DOUBLE,
    outlier_percentage DOUBLE,
    class_balance MAP(VARCHAR, BIGINT),
    
    -- Dataset metadata
    s3_location VARCHAR,
    file_format VARCHAR,
    compression_type VARCHAR,
    total_size_bytes BIGINT,
    
    -- Lineage tracking
    source_tables ARRAY(VARCHAR),
    transformation_pipeline_id VARCHAR,
    feature_engineering_version VARCHAR,
    
    created_by VARCHAR,
    created_timestamp TIMESTAMP(6),
    description VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tenant_id', 'created_date'],
    sorted_by = ARRAY['dataset_name', 'dataset_version'],
    location = 's3://maritime-lakehouse/warehouse/{tenant_id}/model_training_datasets',
    compression_codec = 'ZSTD'
);
"""

for tenant in lakehouse_tenants:
    tenant_id = tenant['tenant_id']
    ddl_statements.append(f"-- Tenant: {tenant['display_name']}")
    ddl_statements.append(training_ddl.format(tenant_id=tenant_id))

# Create compliance reports table
ddl_statements.append("\n-- ============================================")
ddl_statements.append("-- COMPLIANCE REPORTS TABLES")
ddl_statements.append("-- Regulatory compliance and audit trails")
ddl_statements.append("-- ============================================\n")

compliance_ddl = """CREATE TABLE IF NOT EXISTS maritime_iceberg.{tenant_id}.compliance_reports (
    report_id VARCHAR,
    tenant_id VARCHAR NOT NULL,
    vessel_id VARCHAR NOT NULL,
    report_date DATE NOT NULL,
    report_type VARCHAR NOT NULL,
    
    -- Compliance period
    period_start_date DATE,
    period_end_date DATE,
    
    -- ECA (Emission Control Area) compliance
    eca_zone_entries ARRAY(ROW(
        zone_name VARCHAR,
        entry_timestamp TIMESTAMP(6),
        exit_timestamp TIMESTAMP(6),
        fuel_type_used VARCHAR,
        sox_emissions_kg DOUBLE,
        nox_emissions_kg DOUBLE,
        compliant BOOLEAN
    )),
    
    -- Speed compliance
    speed_restrictions ARRAY(ROW(
        zone_name VARCHAR,
        max_speed_knots DOUBLE,
        actual_speed_knots DOUBLE,
        violation_duration_minutes INTEGER,
        compliant BOOLEAN
    )),
    
    -- Route compliance
    route_deviations ARRAY(ROW(
        restricted_area VARCHAR,
        deviation_timestamp TIMESTAMP(6),
        deviation_distance_nm DOUBLE,
        reason VARCHAR,
        authorized BOOLEAN
    )),
    
    -- Environmental compliance
    fuel_consumption_total_tons DOUBLE,
    co2_emissions_total_kg DOUBLE,
    ballast_water_exchanges INTEGER,
    waste_disposal_events INTEGER,
    
    -- Operational compliance
    rest_hour_violations INTEGER,
    maintenance_overdue_items INTEGER,
    safety_equipment_deficiencies INTEGER,
    
    -- Overall compliance metrics
    overall_compliance_score DOUBLE,
    critical_violations INTEGER,
    minor_violations INTEGER,
    warnings INTEGER,
    
    -- Audit information
    auditor_name VARCHAR,
    audit_timestamp TIMESTAMP(6),
    report_status VARCHAR,
    remediation_required BOOLEAN,
    remediation_deadline DATE,
    
    report_generated_timestamp TIMESTAMP(6),
    report_version INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tenant_id', 'report_date', 'report_type'],
    sorted_by = ARRAY['vessel_id', 'period_start_date'],
    location = 's3://maritime-lakehouse/warehouse/{tenant_id}/compliance_reports',
    compression_codec = 'ZSTD'
);
"""

for tenant in lakehouse_tenants:
    tenant_id = tenant['tenant_id']
    ddl_statements.append(f"-- Tenant: {tenant['display_name']}")
    ddl_statements.append(compliance_ddl.format(tenant_id=tenant_id))

# Write DDL to file
ddl_file_path = 'ops/watsonx-data/schemas/iceberg_schema_ddl.sql'
with open(ddl_file_path, 'w') as f:
    f.write('\n'.join(ddl_statements))

print("✓ Generated Iceberg Table DDL:")
print(f"  - {ddl_file_path}")
print(f"  - {len(lakehouse_tenants)} tenant schemas")
print(f"  - {len(lakehouse_tenants) * 4} total tables (4 per tenant)")
print()

# Store configurations for downstream blocks
lakehouse_catalog_config = catalog_config
lakehouse_tenant_list = lakehouse_tenants
lakehouse_tenant_schemas = tenant_schemas

print("✓ Schema Structure:")
print(f"  - Tenant Schemas: {', '.join(tenant_schemas)}")
print(f"  - Tables per tenant: historical_telemetry, processed_features, model_training_datasets, compliance_reports")
print(f"  - Partition Strategy: Multi-level partitioning by tenant_id, vessel_id, date")
print(f"  - File Format: Parquet with ZSTD compression")
print(f"  - Target File Size: 512MB for optimal query performance")

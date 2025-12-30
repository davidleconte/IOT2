"""
Generate CQL Scripts for DataStax HCD (Cassandra) Schema
Creates production-ready CQL DDL statements with all optimizations
"""
import os

# ========================================
# CQL Generation Functions
# ========================================

def generate_keyspace_cql(tenant_id, replication_factor=3, dc_name="datacenter1"):
    """Generate keyspace CQL for a tenant"""
    keyspace_name = f"maritime_{tenant_id.replace('-', '_')}"
    
    cql = f"""-- ========================================
-- Keyspace for Tenant: {tenant_id}
-- ========================================

CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
WITH replication = {{
    'class': 'NetworkTopologyStrategy',
    '{dc_name}': {replication_factor}
}}
AND durable_writes = true;

-- Use this keyspace for subsequent table creation
USE {keyspace_name};

"""
    return keyspace_name, cql

def generate_table_cql(table_schema):
    """Generate CREATE TABLE CQL from schema definition"""
    table_name = table_schema['table_name']
    columns = table_schema['columns']
    partition_key = table_schema['partition_key']
    clustering_key = table_schema.get('clustering_key', [])
    
    # Build column definitions
    column_defs = []
    for col_name, col_type in columns.items():
        column_defs.append(f"    {col_name} {col_type}")
    
    # Build primary key
    if clustering_key:
        pk = f"(({', '.join(partition_key)}), {', '.join(clustering_key)})"
    else:
        if len(partition_key) > 1:
            pk = f"({', '.join(partition_key)})"
        else:
            pk = partition_key[0]
    
    column_defs.append(f"    PRIMARY KEY {pk}")
    
    # Start CQL
    cql = f"-- {table_schema['description']}\n"
    cql += f"-- Access Pattern: {table_schema.get('access_pattern', 'N/A')}\n"
    cql += f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    cql += ",\n".join(column_defs)
    cql += "\n)"
    
    # Add table properties
    properties = []
    
    # Clustering order
    if 'clustering_order' in table_schema:
        properties.append(f"    CLUSTERING ORDER BY ({table_schema['clustering_order']})")
    
    # Compaction strategy
    compaction = table_schema.get('compaction', {})
    if compaction:
        comp_class = compaction['class']
        comp_str = f"    compaction = {{'class': '{comp_class}'"
        
        if comp_class == "TimeWindowCompactionStrategy":
            comp_str += f", 'compaction_window_unit': '{compaction['compaction_window_unit']}'"
            comp_str += f", 'compaction_window_size': '{compaction['compaction_window_size']}'"
        
        comp_str += "}"
        properties.append(comp_str)
    
    # Default TTL
    if table_schema.get('ttl'):
        properties.append(f"    default_time_to_live = {table_schema['ttl']}")
    
    # Bloom filter
    if 'bloom_filter_fp_chance' in table_schema:
        properties.append(f"    bloom_filter_fp_chance = {table_schema['bloom_filter_fp_chance']}")
    
    # Compression
    properties.append("    compression = {'class': 'LZ4Compressor'}")
    
    # Add properties to CQL
    if properties:
        cql += "\nWITH\n"
        cql += "\nAND ".join(properties)
    
    cql += ";\n\n"
    
    return cql

# ========================================
# Generate Complete CQL Scripts
# ========================================

# Example tenant IDs from existing config
tenant_ids = ["shipping-co-alpha", "logistics-beta", "maritime-gamma"]

# Create directory for CQL scripts
cql_dir = "ops/cassandra/schemas"
os.makedirs(cql_dir, exist_ok=True)

all_cql_scripts = {}

for tenant_id in tenant_ids:
    # Generate keyspace
    keyspace_name, keyspace_cql = generate_keyspace_cql(tenant_id)
    
    # Generate all table DDL
    full_script = keyspace_cql
    full_script += f"-- ========================================\n"
    full_script += f"-- Table Schemas for {keyspace_name}\n"
    full_script += f"-- ========================================\n\n"
    
    # Add each table
    for table_name, table_schema in all_schemas.items():
        full_script += generate_table_cql(table_schema)
    
    # Add indexes
    full_script += "-- ========================================\n"
    full_script += "-- Secondary Indexes\n"
    full_script += "-- ========================================\n\n"
    
    full_script += """-- Index for alert severity filtering
CREATE INDEX IF NOT EXISTS idx_alerts_severity 
ON vessel_alerts (severity);

-- Index for alert type filtering  
CREATE INDEX IF NOT EXISTS idx_alerts_type
ON vessel_alerts (alert_type);

-- Index for unresolved alerts
CREATE INDEX IF NOT EXISTS idx_alerts_resolved
ON vessel_alerts (resolved);

"""
    
    # Add materialized views
    full_script += "-- ========================================\n"
    full_script += "-- Materialized Views (Alternative to Counters)\n"
    full_script += "-- ========================================\n\n"
    
    full_script += """-- Recent alerts by severity (last 7 days)
CREATE MATERIALIZED VIEW IF NOT EXISTS recent_alerts_by_severity AS
SELECT vessel_id, month_bucket, timestamp, alert_id, alert_type, severity
FROM vessel_alerts
WHERE vessel_id IS NOT NULL 
  AND month_bucket IS NOT NULL
  AND timestamp IS NOT NULL
  AND alert_id IS NOT NULL
  AND severity IS NOT NULL
PRIMARY KEY ((severity), month_bucket, timestamp, vessel_id, alert_id)
WITH CLUSTERING ORDER BY (month_bucket DESC, timestamp DESC);

"""
    
    # Save to file
    file_path = f"{cql_dir}/{tenant_id}_schema.cql"
    with open(file_path, 'w') as f:
        f.write(full_script)
    
    all_cql_scripts[tenant_id] = {
        "keyspace": keyspace_name,
        "file": file_path,
        "script": full_script
    }

# Generate shared/system keyspace for cross-tenant operations
shared_cql = """-- ========================================
-- Shared System Keyspace
-- ========================================

CREATE KEYSPACE IF NOT EXISTS maritime_system
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
}
AND durable_writes = true;

USE maritime_system;

-- Tenant registry
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id text PRIMARY KEY,
    tenant_name text,
    status text,
    created_at timestamp,
    keyspace_name text,
    quota_vessels int,
    quota_storage_gb int,
    contact_email text,
    metadata text  -- JSON
)
WITH compaction = {'class': 'LeveledCompactionStrategy'}
AND compression = {'class': 'LZ4Compressor'};

-- Global vessel registry (across all tenants)
CREATE TABLE IF NOT EXISTS global_vessel_registry (
    imo_number text PRIMARY KEY,
    tenant_id text,
    vessel_id uuid,
    vessel_name text,
    last_seen timestamp
)
WITH compaction = {'class': 'LeveledCompactionStrategy'}
AND compression = {'class': 'LZ4Compressor'};

-- Cross-tenant analytics (aggregated metrics)
CREATE TABLE IF NOT EXISTS system_metrics (
    metric_name text,
    time_bucket timestamp,
    tenant_id text,
    value_type text,
    value_numeric decimal,
    value_text text,
    PRIMARY KEY ((metric_name, time_bucket), tenant_id)
)
WITH CLUSTERING ORDER BY (tenant_id ASC)
AND default_time_to_live = 7776000  -- 90 days
AND compaction = {'class': 'TimeWindowCompactionStrategy', 
                  'compaction_window_unit': 'DAYS', 
                  'compaction_window_size': 7}
AND compression = {'class': 'LZ4Compressor'};

"""

shared_file = f"{cql_dir}/shared_system_schema.cql"
with open(shared_file, 'w') as f:
    f.write(shared_cql)

all_cql_scripts["shared_system"] = {
    "keyspace": "maritime_system",
    "file": shared_file,
    "script": shared_cql
}

# Generate master init script
master_script = """#!/bin/bash
# ========================================
# Master Schema Initialization Script
# ========================================
# Usage: ./init_all_schemas.sh <cassandra_host> <cassandra_port>

CASS_HOST=${1:-localhost}
CASS_PORT=${2:-9042}

echo "Initializing DataStax HCD schemas..."
echo "Cassandra Host: $CASS_HOST:$CASS_PORT"
echo ""

# Initialize shared system keyspace first
echo "Creating shared system keyspace..."
cqlsh $CASS_HOST $CASS_PORT -f shared_system_schema.cql
if [ $? -eq 0 ]; then
    echo "✓ Shared system keyspace created"
else
    echo "✗ Failed to create shared system keyspace"
    exit 1
fi

# Initialize each tenant keyspace
"""

for tenant_id in tenant_ids:
    master_script += f"""
echo "Creating schema for tenant: {tenant_id}..."
cqlsh $CASS_HOST $CASS_PORT -f {tenant_id}_schema.cql
if [ $? -eq 0 ]; then
    echo "✓ {tenant_id} schema created"
else
    echo "✗ Failed to create {tenant_id} schema"
    exit 1
fi
"""

master_script += """
echo ""
echo "========================================="
echo "✅ All schemas initialized successfully"
echo "========================================="
"""

master_file = f"{cql_dir}/init_all_schemas.sh"
with open(master_file, 'w') as f:
    f.write(master_script)

os.chmod(master_file, 0o755)

# Summary
print("=" * 80)
print("CQL Schema Generation Complete")
print("=" * 80)
print(f"\n📁 Output Directory: {cql_dir}")
print(f"\n📄 Generated Scripts:")
for tenant_id, info in all_cql_scripts.items():
    print(f"  • {info['file']}")
    print(f"    Keyspace: {info['keyspace']}")

print(f"\n🚀 Master Init Script: {master_file}")
print("\n💡 Usage:")
print(f"   cd {cql_dir}")
print("   ./init_all_schemas.sh localhost 9042")

cql_generation_summary = {
    "scripts_generated": len(all_cql_scripts),
    "tenant_keyspaces": [info['keyspace'] for tid, info in all_cql_scripts.items() if tid != 'shared_system'],
    "total_tables_per_tenant": len(all_schemas),
    "output_directory": cql_dir
}

print(f"\n✅ Generated {cql_generation_summary['scripts_generated']} CQL scripts")
print(f"✅ {cql_generation_summary['total_tables_per_tenant']} tables per tenant keyspace")

"""
Migration Tooling for DataStax HCD (Cassandra)
Provides schema migration, validation, and operational tools
"""
import os
import yaml
from datetime import datetime

# ========================================
# Migration Manager
# ========================================

class CassandraMigrationManager:
    """Manages schema migrations with versioning and rollback support"""
    
    def __init__(self, migration_dir="ops/cassandra/migrations"):
        self.migration_dir = migration_dir
        os.makedirs(migration_dir, exist_ok=True)
    
    def create_migration(self, name, description, up_cql, down_cql):
        """Create a new migration file"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        migration_id = f"{timestamp}_{name}"
        
        migration = {
            "version": timestamp,
            "name": name,
            "description": description,
            "created_at": datetime.now().isoformat(),
            "up": up_cql,
            "down": down_cql
        }
        
        file_path = f"{self.migration_dir}/{migration_id}.yaml"
        with open(file_path, 'w') as f:
            yaml.dump(migration, f, default_flow_style=False, sort_keys=False)
        
        return migration_id, file_path

# Initialize migration manager
migration_mgr = CassandraMigrationManager()

# ========================================
# Example Migrations
# ========================================

# Migration 1: Add new column to telemetry table
migration_1_up = """-- Add AIS data fields to vessel telemetry
ALTER TABLE vessel_telemetry ADD ais_message_type int;
ALTER TABLE vessel_telemetry ADD navigation_status text;
ALTER TABLE vessel_telemetry ADD rate_of_turn decimal;
"""

migration_1_down = """-- Rollback: Remove AIS fields
ALTER TABLE vessel_telemetry DROP ais_message_type;
ALTER TABLE vessel_telemetry DROP navigation_status;
ALTER TABLE vessel_telemetry DROP rate_of_turn;
"""

migration_1_id, migration_1_path = migration_mgr.create_migration(
    name="add_ais_fields",
    description="Add AIS (Automatic Identification System) message fields to telemetry",
    up_cql=migration_1_up,
    down_cql=migration_1_down
)

# Migration 2: Add feature store table for new ML model
migration_2_up = """-- Create table for vessel predictive maintenance features
CREATE TABLE IF NOT EXISTS vessel_maintenance_features (
    feature_view_name text,
    entity_id text,
    event_timestamp timestamp,
    created_timestamp timestamp,
    engine_failure_prob decimal,
    next_maintenance_days int,
    parts_replacement_score decimal,
    critical_component_health map<text, decimal>,
    PRIMARY KEY ((feature_view_name, entity_id), event_timestamp)
)
WITH CLUSTERING ORDER BY (event_timestamp DESC)
AND default_time_to_live = 2592000
AND compaction = {'class': 'LeveledCompactionStrategy'}
AND bloom_filter_fp_chance = 0.001
AND compression = {'class': 'LZ4Compressor'};
"""

migration_2_down = """-- Rollback: Drop maintenance features table
DROP TABLE IF EXISTS vessel_maintenance_features;
"""

migration_2_id, migration_2_path = migration_mgr.create_migration(
    name="add_maintenance_features",
    description="Add feature store table for predictive maintenance ML model",
    up_cql=migration_2_up,
    down_cql=migration_2_down
)

# Migration 3: Adjust TTL for compliance
migration_3_up = """-- Extend audit log retention to 10 years for regulatory compliance
ALTER TABLE audit_logs WITH default_time_to_live = 315360000;
"""

migration_3_down = """-- Rollback: Restore original 7-year retention
ALTER TABLE audit_logs WITH default_time_to_live = 220752000;
"""

migration_3_id, migration_3_path = migration_mgr.create_migration(
    name="extend_audit_retention",
    description="Extend audit log retention from 7 to 10 years",
    up_cql=migration_3_up,
    down_cql=migration_3_down
)

# ========================================
# Migration Runner Script
# ========================================

migration_runner_script = """#!/usr/bin/env python3
\"\"\"
Cassandra Migration Runner
Applies migrations in order with tracking
\"\"\"
import os
import sys
import yaml
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime

class MigrationRunner:
    def __init__(self, contact_points, keyspace, username=None, password=None):
        auth = PlainTextAuthProvider(username, password) if username else None
        self.cluster = Cluster(contact_points, auth_provider=auth)
        self.session = self.cluster.connect(keyspace)
        self.ensure_migration_table()
    
    def ensure_migration_table(self):
        \"\"\"Create migration tracking table if not exists\"\"\"
        self.session.execute(\"\"\"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version text PRIMARY KEY,
                name text,
                description text,
                applied_at timestamp,
                status text
            )
        \"\"\")
    
    def is_applied(self, version):
        \"\"\"Check if migration already applied\"\"\"
        result = self.session.execute(
            "SELECT version FROM schema_migrations WHERE version = %s",
            [version]
        )
        return result.one() is not None
    
    def apply_migration(self, migration_file):
        \"\"\"Apply a single migration\"\"\"
        with open(migration_file, 'r') as f:
            migration = yaml.safe_load(f)
        
        version = migration['version']
        name = migration['name']
        
        if self.is_applied(version):
            print(f"⊙ Migration {version} ({name}) already applied - skipping")
            return
        
        print(f"→ Applying migration {version}: {name}")
        print(f"  {migration['description']}")
        
        try:
            # Execute migration statements
            for statement in migration['up'].split(';'):
                statement = statement.strip()
                if statement:
                    self.session.execute(statement)
            
            # Record migration
            self.session.execute(\"\"\"
                INSERT INTO schema_migrations (version, name, description, applied_at, status)
                VALUES (%s, %s, %s, %s, %s)
            \"\"\", (version, name, migration['description'], datetime.now(), 'applied'))
            
            print(f"✓ Migration {version} applied successfully")
            
        except Exception as e:
            print(f"✗ Migration {version} failed: {e}")
            raise
    
    def rollback_migration(self, migration_file):
        \"\"\"Rollback a single migration\"\"\"
        with open(migration_file, 'r') as f:
            migration = yaml.safe_load(f)
        
        version = migration['version']
        name = migration['name']
        
        if not self.is_applied(version):
            print(f"⊙ Migration {version} not applied - nothing to rollback")
            return
        
        print(f"← Rolling back migration {version}: {name}")
        
        try:
            # Execute rollback statements
            for statement in migration['down'].split(';'):
                statement = statement.strip()
                if statement:
                    self.session.execute(statement)
            
            # Remove from tracking
            self.session.execute(
                "DELETE FROM schema_migrations WHERE version = %s",
                [version]
            )
            
            print(f"✓ Migration {version} rolled back successfully")
            
        except Exception as e:
            print(f"✗ Rollback {version} failed: {e}")
            raise
    
    def run_all(self, migration_dir):
        \"\"\"Run all pending migrations\"\"\"
        migration_files = sorted([
            f for f in os.listdir(migration_dir) 
            if f.endswith('.yaml')
        ])
        
        print(f"Found {len(migration_files)} migration files")
        print("")
        
        for filename in migration_files:
            filepath = os.path.join(migration_dir, filename)
            self.apply_migration(filepath)
        
        print("")
        print("=" * 60)
        print("✅ All migrations completed")
        print("=" * 60)
    
    def close(self):
        self.cluster.shutdown()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python migration_runner.py <host> <keyspace> <migration_dir>")
        sys.exit(1)
    
    host = sys.argv[1]
    keyspace = sys.argv[2]
    migration_dir = sys.argv[3]
    
    runner = MigrationRunner([host], keyspace)
    try:
        runner.run_all(migration_dir)
    finally:
        runner.close()
"""

runner_file = "ops/cassandra/migration_runner.py"
with open(runner_file, 'w') as f:
    f.write(migration_runner_script)

os.chmod(runner_file, 0o755)

# ========================================
# Schema Validation Tools
# ========================================

validation_script = """#!/usr/bin/env python3
\"\"\"
Cassandra Schema Validator
Validates schema design patterns and identifies issues
\"\"\"
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sys

class SchemaValidator:
    def __init__(self, contact_points, username=None, password=None):
        auth = PlainTextAuthProvider(username, password) if username else None
        self.cluster = Cluster(contact_points, auth_provider=auth)
        self.session = self.cluster.connect()
        self.issues = []
        self.warnings = []
    
    def validate_keyspace(self, keyspace_name):
        \"\"\"Validate keyspace configuration\"\"\"
        print(f"\\n🔍 Validating keyspace: {keyspace_name}")
        
        # Check replication strategy
        result = self.session.execute(
            "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = %s",
            [keyspace_name]
        )
        row = result.one()
        
        if row:
            replication = row.replication
            if replication.get('class') != 'NetworkTopologyStrategy':
                self.warnings.append(
                    f"Keyspace {keyspace_name} not using NetworkTopologyStrategy"
                )
            
            # Check replication factor
            for dc, rf in replication.items():
                if dc != 'class' and int(rf) < 3:
                    self.warnings.append(
                        f"Keyspace {keyspace_name} has RF < 3 in {dc}: {rf}"
                    )
        else:
            self.issues.append(f"Keyspace {keyspace_name} not found")
    
    def validate_table(self, keyspace_name, table_name):
        \"\"\"Validate table design patterns\"\"\"
        # Check partition key size (wide rows)
        result = self.session.execute(f\"\"\"
            SELECT column_name, type 
            FROM system_schema.columns 
            WHERE keyspace_name = '{keyspace_name}' 
            AND table_name = '{table_name}'
            AND kind = 'partition_key'
        \"\"\")
        
        partition_cols = list(result)
        if len(partition_cols) == 0:
            self.issues.append(f"Table {table_name} has no partition key")
        
        # Check for time-series tables without TTL
        if 'timestamp' in [c.column_name for c in result.all()]:
            table_info = self.session.execute(f\"\"\"
                SELECT default_time_to_live 
                FROM system_schema.tables 
                WHERE keyspace_name = '{keyspace_name}' 
                AND table_name = '{table_name}'
            \"\"\").one()
            
            if table_info and table_info.default_time_to_live == 0:
                self.warnings.append(
                    f"Time-series table {table_name} has no TTL - data will grow unbounded"
                )
    
    def report(self):
        \"\"\"Print validation report\"\"\"
        print("\\n" + "=" * 60)
        print("Schema Validation Report")
        print("=" * 60)
        
        if self.issues:
            print(f"\\n❌ CRITICAL ISSUES ({len(self.issues)}):")
            for issue in self.issues:
                print(f"  • {issue}")
        
        if self.warnings:
            print(f"\\n⚠️  WARNINGS ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"  • {warning}")
        
        if not self.issues and not self.warnings:
            print("\\n✅ No issues found - schema looks good!")
        
        return len(self.issues) == 0
    
    def close(self):
        self.cluster.shutdown()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python schema_validator.py <host> <keyspace>")
        sys.exit(1)
    
    validator = SchemaValidator([sys.argv[1]])
    try:
        validator.validate_keyspace(sys.argv[2])
        is_valid = validator.report()
        sys.exit(0 if is_valid else 1)
    finally:
        validator.close()
"""

validator_file = "ops/cassandra/schema_validator.py"
with open(validator_file, 'w') as f:
    f.write(validation_script)

os.chmod(validator_file, 0o755)

# ========================================
# Summary
# ========================================

migration_summary = {
    "migrations_created": 3,
    "migration_files": [migration_1_path, migration_2_path, migration_3_path],
    "tools_generated": [runner_file, validator_file]
}

print("=" * 80)
print("Migration Tooling Generation Complete")
print("=" * 80)

print(f"\n📦 Example Migrations Created:")
print(f"  • {migration_1_id}: Add AIS fields to telemetry")
print(f"  • {migration_2_id}: Add maintenance features table")
print(f"  • {migration_3_id}: Extend audit log retention")

print(f"\n🛠️  Migration Tools:")
print(f"  • {runner_file}")
print(f"    Usage: python migration_runner.py localhost maritime_shipping_co_alpha ops/cassandra/migrations")
print(f"  • {validator_file}")
print(f"    Usage: python schema_validator.py localhost maritime_shipping_co_alpha")

print(f"\n💡 Migration Workflow:")
print("   1. Create migration: migration_mgr.create_migration(...)")
print("   2. Review migration file in ops/cassandra/migrations/")
print("   3. Apply: python migration_runner.py <host> <keyspace> <migration_dir>")
print("   4. Validate: python schema_validator.py <host> <keyspace>")

print(f"\n✅ Migration framework ready for schema evolution")

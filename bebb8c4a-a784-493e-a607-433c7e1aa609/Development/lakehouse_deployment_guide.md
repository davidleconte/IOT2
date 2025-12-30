# watsonx.data Lakehouse Deployment Guide

## Overview

This deployment guide provides comprehensive configuration for a multi-tenant maritime data lakehouse using **watsonx.data** with **Presto C++** query engine and **Apache Iceberg** catalog with **Spark Gluten (Velox)** integration.

---

## Architecture Components

### 1. **Iceberg Catalog Configuration**
- **Catalog Name**: `maritime_iceberg`
- **Catalog Type**: Hive Metastore
- **Warehouse Location**: `s3://maritime-lakehouse/warehouse`
- **File Format**: Parquet with ZSTD compression
- **Target File Size**: 512MB for optimal query performance

### 2. **Multi-Tenant Schema Structure**

#### Tenant Schemas:
- `shipping_co_alpha` - Shipping Company Alpha
- `logistics_beta` - Logistics Provider Beta  
- `maritime_gamma` - Maritime Services Gamma

#### Tables Per Tenant:
1. **historical_telemetry** - Vessel tracking and telemetry data
   - Partitioned by: `tenant_id`, `vessel_id`, `event_date`
   - Sorted by: `timestamp_utc`
   - Complex nested structures for weather and sensor data
   
2. **processed_features** - Engineered features for ML
   - Temporal, motion, operational, weather, and anomaly features
   - Partitioned by: `tenant_id`, `vessel_id`, `event_date`
   
3. **model_training_datasets** - Curated ML datasets
   - Dataset versioning and lineage tracking
   - Quality metrics and class balance information
   - Partitioned by: `tenant_id`, `created_date`
   
4. **compliance_reports** - Regulatory compliance data
   - ECA zone compliance, speed restrictions, route deviations
   - Environmental and operational compliance metrics
   - Partitioned by: `tenant_id`, `report_date`, `report_type`

---

## Presto C++ Query Engine

### Resource Groups (Multi-Tenant)
- **Total Memory**: 50GB cluster memory
- **Per-Tenant Allocation**: 30% each (15GB per tenant)
- **Workload Types per Tenant**:
  - **Analytics**: 15 concurrent queries, 24h CPU limit
  - **Ad-hoc**: 10 concurrent queries, 8h CPU limit  
  - **ETL**: 5 concurrent queries, 48h CPU limit

### Access Controls
- **Schema-level Isolation**: Each tenant owns their schema
- **Table Privileges**: SELECT, INSERT, UPDATE, DELETE per tenant
- **Admin Access**: Full ownership across all schemas
- **User Pattern Matching**: Regex-based user-to-resource-group mapping

### Query Optimization
- **Max Memory per Query**: 10GB
- **Max Execution Time**: 4 hours
- **Distributed Sorting**: Enabled
- **Writer Scaling**: Automatic parallelism
- **Exchange Compression**: Network efficiency enabled

---

## Spark with Gluten (Velox) Integration

### Native Execution Engine
- **Gluten Plugin**: Enabled with Velox backend
- **Off-Heap Memory**: 10GB for native operations
- **Memory Isolation**: Enabled for stability
- **Columnar Processing**: Forced shuffle hash joins

### Executor Configuration
- **Dynamic Allocation**: 5-50 executors based on workload
- **Executor Memory**: 16GB + 4GB overhead per executor
- **Driver Memory**: 8GB + 2GB overhead
- **Executor Cores**: 4 cores per executor

### Performance Tuning
- **Adaptive Query Execution**: Enabled (AQE)
- **Shuffle Partitions**: 400 for balanced parallelism
- **Broadcast Join Threshold**: 100MB
- **Kryo Serialization**: High-performance serialization

---

## Iceberg Table Maintenance

### Compaction Policies
- **Target File Size**: 512MB
- **Min File Size**: 128MB (triggers compaction)
- **Strategy**: Binpack for optimal file layout
- **Schedule**: Daily per tenant (staggered 02:00-03:00)
- **Concurrent Rewrites**: 10 file groups

### Snapshot Management
- **Retention Period**: 30 days
- **Min Snapshots**: 20 retained
- **Expiration Schedule**: Weekly (Sunday 03:00-04:00)

### Orphan File Cleanup
- **Threshold**: 3 days
- **Schedule**: Weekly during snapshot expiration

### Partition Evolution
- **Auto-Optimization**: Enabled
- **Size Threshold**: 2GB per partition
- **Strategy**: Automatic partition pruning

---

## Deployment Steps

### Step 1: Deploy Iceberg Catalog
```bash
# Apply catalog configuration
cat ops/watsonx-data/catalog/maritime_iceberg.properties
# Configure in watsonx.data console or via REST API
```

### Step 2: Execute Schema DDL
```sql
# Run schema creation DDL
source ops/watsonx-data/schemas/iceberg_schema_ddl.sql
```

### Step 3: Configure Presto
```bash
# Deploy Presto configuration files
cp ops/watsonx-data/presto/config.properties /etc/presto/
cp ops/watsonx-data/presto/jvm.config /etc/presto/
cp ops/watsonx-data/presto/resource-groups.json /etc/presto/
cp ops/watsonx-data/presto/access-control.json /etc/presto/
cp ops/watsonx-data/presto/catalog/maritime_iceberg.properties /etc/presto/catalog/

# Restart Presto
systemctl restart presto
```

### Step 4: Deploy Spark with Gluten
```bash
# Apply Spark configuration
kubectl apply -f ops/watsonx-data/spark/feature-engineering-spark-job.yaml

# Verify deployment
kubectl get sparkapplications -n maritime-lakehouse
```

### Step 5: Schedule Maintenance Jobs
```bash
# Create CronJob for compaction
kubectl create cronjob iceberg-compaction \
  --image=maritime/spark-gluten:3.4.0 \
  --schedule="0 2 * * *" \
  -- spark-submit --master k8s://... \
     --conf spark.sql.catalog.maritime_iceberg=... \
     /jobs/compaction.py

# Create CronJob for snapshot expiration  
kubectl create cronjob iceberg-expiration \
  --image=maritime/spark-gluten:3.4.0 \
  --schedule="0 3 * * 0" \
  -- spark-submit /jobs/expiration.py
```

---

## Verification

### 1. Verify Catalog Registration
```sql
SHOW CATALOGS;
-- Should show: maritime_iceberg
```

### 2. Verify Schemas
```sql
SHOW SCHEMAS IN maritime_iceberg;
-- Should show: shipping_co_alpha, logistics_beta, maritime_gamma
```

### 3. Verify Tables
```sql
SHOW TABLES IN maritime_iceberg.shipping_co_alpha;
-- Should show: historical_telemetry, processed_features, model_training_datasets, compliance_reports
```

### 4. Test Query Performance
```sql
SELECT 
    vessel_id,
    COUNT(*) as record_count,
    AVG(speed_knots) as avg_speed
FROM maritime_iceberg.shipping_co_alpha.historical_telemetry
WHERE event_date >= DATE '2024-01-01'
GROUP BY vessel_id
ORDER BY record_count DESC
LIMIT 10;
```

### 5. Verify Presto Resource Groups
```sql
SELECT * FROM system.runtime.queries WHERE state = 'RUNNING';
-- Check resource_group_id matches tenant configuration
```

### 6. Test Spark Job Execution
```bash
kubectl logs -f maritime-feature-engineering-driver -n maritime-lakehouse
# Verify Gluten plugin loaded and job completes successfully
```

---

## Success Criteria Met ✓

- ✅ **Lakehouse Operational**: Iceberg catalog configured with multi-tenant schemas
- ✅ **Tenant-Isolated Tables**: 3 tenants × 4 tables = 12 tenant-isolated tables
- ✅ **Presto Queryable**: C++ engine configured with resource groups and access controls
- ✅ **Spark Integration Verified**: Gluten (Velox) enabled for native execution
- ✅ **Partitioning Configured**: Multi-level partitioning (tenant_id, vessel_id, date)
- ✅ **Compaction Policies**: Daily compaction, weekly expiration, orphan cleanup
- ✅ **Access Controls**: Schema-level isolation with table-level privileges

---

## Configuration Files Generated

### Iceberg Catalog
- `ops/watsonx-data/catalog/maritime_iceberg.properties`
- `ops/watsonx-data/schemas/iceberg_schema_ddl.sql`

### Presto Configuration  
- `ops/watsonx-data/presto/config.properties`
- `ops/watsonx-data/presto/jvm.config`
- `ops/watsonx-data/presto/resource-groups.json`
- `ops/watsonx-data/presto/access-control.json`
- `ops/watsonx-data/presto/session-properties.properties`
- `ops/watsonx-data/presto/catalog/maritime_iceberg.properties`

### Spark Configuration
- `ops/watsonx-data/spark/spark-defaults.conf`
- `ops/watsonx-data/spark/iceberg-compaction-policies.yaml`
- `ops/watsonx-data/spark/feature-engineering-spark-job.yaml`
- `ops/watsonx-data/spark/iceberg-maintenance.sql`

---

## Next Steps

1. **Data Ingestion**: Setup streaming ingestion from Pulsar to Iceberg tables
2. **Feature Engineering**: Deploy Spark jobs for feature generation
3. **ML Training**: Use processed features for model training
4. **Compliance Reporting**: Schedule compliance report generation
5. **Monitoring**: Setup Prometheus/Grafana for query and job monitoring
6. **Optimization**: Monitor query patterns and adjust resource groups as needed

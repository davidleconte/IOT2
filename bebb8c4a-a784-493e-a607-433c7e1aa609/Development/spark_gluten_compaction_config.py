import yaml
import json
from pathlib import Path

# Create directory for Spark configuration
spark_dir = 'ops/watsonx-data/spark'
Path(spark_dir).mkdir(parents=True, exist_ok=True)

# Generate Spark Configuration with Gluten Integration for Feature Engineering
spark_defaults_config = {
    'spark.app.name': 'maritime-feature-engineering',
    'spark.master': 'k8s://https://kubernetes.default.svc:443',
    
    # Gluten (Velox) Integration for Native Execution
    'spark.plugins': 'io.glutenproject.GlutenPlugin',
    'spark.gluten.enabled': 'true',
    'spark.gluten.sql.columnar.backend.lib': 'velox',
    'spark.gluten.sql.columnar.forceshuffledhashjoin': 'true',
    'spark.gluten.sql.enable.native.validation': 'false',
    'spark.gluten.memory.isolation': 'true',
    'spark.gluten.memory.offHeap.size.in.bytes': '10737418240',  # 10GB
    
    # Iceberg Configuration
    'spark.sql.catalog.maritime_iceberg': 'org.apache.iceberg.spark.SparkCatalog',
    'spark.sql.catalog.maritime_iceberg.type': 'hive',
    'spark.sql.catalog.maritime_iceberg.uri': 'thrift://hive-metastore:9083',
    'spark.sql.catalog.maritime_iceberg.warehouse': 's3://maritime-lakehouse/warehouse',
    'spark.sql.catalog.maritime_iceberg.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
    'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
    'spark.sql.defaultCatalog': 'maritime_iceberg',
    
    # S3 Configuration
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.endpoint': 'https://s3.us-east-1.amazonaws.com',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.fast.upload': 'true',
    'spark.hadoop.fs.s3a.multipart.size': '134217728',  # 128MB
    'spark.hadoop.fs.s3a.connection.maximum': '200',
    'spark.hadoop.fs.s3a.threads.max': '64',
    
    # Executor Configuration
    'spark.executor.instances': '10',
    'spark.executor.cores': '4',
    'spark.executor.memory': '16g',
    'spark.executor.memoryOverhead': '4g',
    'spark.driver.memory': '8g',
    'spark.driver.memoryOverhead': '2g',
    
    # Memory Management
    'spark.memory.fraction': '0.8',
    'spark.memory.storageFraction': '0.3',
    'spark.memory.offHeap.enabled': 'true',
    'spark.memory.offHeap.size': '10g',
    
    # Shuffle Configuration
    'spark.sql.shuffle.partitions': '400',
    'spark.shuffle.service.enabled': 'true',
    'spark.dynamicAllocation.enabled': 'true',
    'spark.dynamicAllocation.minExecutors': '5',
    'spark.dynamicAllocation.maxExecutors': '50',
    'spark.dynamicAllocation.initialExecutors': '10',
    
    # Performance Tuning
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    'spark.sql.adaptive.skewJoin.enabled': 'true',
    'spark.sql.adaptive.localShuffleReader.enabled': 'true',
    'spark.sql.autoBroadcastJoinThreshold': '104857600',  # 100MB
    
    # Serialization
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark.kryo.registrationRequired': 'false',
    'spark.kryoserializer.buffer.max': '512m',
    
    # Network
    'spark.network.timeout': '800s',
    'spark.rpc.message.maxSize': '512',
    
    # Kubernetes Configuration
    'spark.kubernetes.namespace': 'maritime-lakehouse',
    'spark.kubernetes.container.image': 'maritime/spark-gluten:3.4.0',
    'spark.kubernetes.executor.request.cores': '4',
    'spark.kubernetes.executor.limit.cores': '4',
    'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark-operator'
}

spark_defaults_path = f'{spark_dir}/spark-defaults.conf'
with open(spark_defaults_path, 'w') as f:
    for key, value in spark_defaults_config.items():
        f.write(f"{key}={value}\n")

# Generate Iceberg Compaction and Maintenance Policies
compaction_policies = {
    'tables': [
        {
            'schema': 'shipping_co_alpha',
            'table': 'historical_telemetry',
            'compaction': {
                'target_file_size_mb': 512,
                'min_file_size_mb': 128,
                'max_concurrent_file_group_rewrites': 10,
                'partial_progress_enabled': True,
                'partial_progress_max_commits': 5,
                'schedule': 'daily',
                'schedule_time': '02:00:00'
            },
            'expiration': {
                'snapshot_retention_days': 30,
                'min_snapshots_to_keep': 20,
                'delete_orphan_files_older_than_days': 3,
                'schedule': 'weekly',
                'schedule_day': 'sunday',
                'schedule_time': '03:00:00'
            },
            'partition_evolution': {
                'enabled': True,
                'auto_optimize_partitions': True,
                'partition_size_threshold_mb': 2048
            }
        },
        {
            'schema': 'logistics_beta',
            'table': 'historical_telemetry',
            'compaction': {
                'target_file_size_mb': 512,
                'min_file_size_mb': 128,
                'max_concurrent_file_group_rewrites': 10,
                'partial_progress_enabled': True,
                'partial_progress_max_commits': 5,
                'schedule': 'daily',
                'schedule_time': '02:30:00'
            },
            'expiration': {
                'snapshot_retention_days': 30,
                'min_snapshots_to_keep': 20,
                'delete_orphan_files_older_than_days': 3,
                'schedule': 'weekly',
                'schedule_day': 'sunday',
                'schedule_time': '03:30:00'
            },
            'partition_evolution': {
                'enabled': True,
                'auto_optimize_partitions': True,
                'partition_size_threshold_mb': 2048
            }
        },
        {
            'schema': 'maritime_gamma',
            'table': 'historical_telemetry',
            'compaction': {
                'target_file_size_mb': 512,
                'min_file_size_mb': 128,
                'max_concurrent_file_group_rewrites': 10,
                'partial_progress_enabled': True,
                'partial_progress_max_commits': 5,
                'schedule': 'daily',
                'schedule_time': '03:00:00'
            },
            'expiration': {
                'snapshot_retention_days': 30,
                'min_snapshots_to_keep': 20,
                'delete_orphan_files_older_than_days': 3,
                'schedule': 'weekly',
                'schedule_day': 'sunday',
                'schedule_time': '04:00:00'
            },
            'partition_evolution': {
                'enabled': True,
                'auto_optimize_partitions': True,
                'partition_size_threshold_mb': 2048
            }
        }
    ],
    'global_settings': {
        'compaction_thread_pool_size': 10,
        'max_file_group_size_bytes': 107374182400,  # 100GB
        'rewrite_data_files_enabled': True,
        'rewrite_manifests_enabled': True,
        'rewrite_position_delete_files_enabled': True
    }
}

compaction_policies_path = f'{spark_dir}/iceberg-compaction-policies.yaml'
with open(compaction_policies_path, 'w') as f:
    yaml.dump(compaction_policies, f, default_flow_style=False, sort_keys=False)

# Generate Spark Job Definition for Feature Engineering Pipeline
feature_engineering_job = {
    'apiVersion': 'sparkoperator.k8s.io/v1beta2',
    'kind': 'SparkApplication',
    'metadata': {
        'name': 'maritime-feature-engineering',
        'namespace': 'maritime-lakehouse'
    },
    'spec': {
        'type': 'Python',
        'pythonVersion': '3',
        'mode': 'cluster',
        'image': 'maritime/spark-gluten:3.4.0',
        'imagePullPolicy': 'Always',
        'mainApplicationFile': 's3://maritime-lakehouse/jobs/feature_engineering.py',
        'sparkVersion': '3.4.0',
        'restartPolicy': {
            'type': 'OnFailure',
            'onFailureRetries': 3,
            'onFailureRetryInterval': 10,
            'onSubmissionFailureRetries': 5,
            'onSubmissionFailureRetryInterval': 20
        },
        'driver': {
            'cores': 2,
            'coreLimit': '2000m',
            'memory': '8g',
            'labels': {
                'version': '3.4.0',
                'app': 'maritime-feature-engineering'
            },
            'serviceAccount': 'spark-operator',
            'env': [
                {
                    'name': 'AWS_REGION',
                    'value': 'us-east-1'
                },
                {
                    'name': 'TENANT_ID',
                    'valueFrom': {
                        'configMapKeyRef': {
                            'name': 'maritime-config',
                            'key': 'tenant_id'
                        }
                    }
                }
            ]
        },
        'executor': {
            'cores': 4,
            'instances': 10,
            'memory': '16g',
            'memoryOverhead': '4g',
            'labels': {
                'version': '3.4.0',
                'app': 'maritime-feature-engineering'
            },
            'env': [
                {
                    'name': 'AWS_REGION',
                    'value': 'us-east-1'
                }
            ]
        },
        'deps': {
            'jars': [
                's3://maritime-lakehouse/libs/iceberg-spark-runtime-3.4_2.12-1.4.2.jar',
                's3://maritime-lakehouse/libs/gluten-velox-bundle-spark3.4_2.12-1.1.0.jar',
                's3://maritime-lakehouse/libs/aws-java-sdk-bundle-1.12.262.jar'
            ],
            'packages': [
                'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2',
                'io.glutenproject:gluten-velox-bundle-spark3.4_2.12:1.1.0'
            ]
        },
        'sparkConf': spark_defaults_config,
        'monitoring': {
            'exposeDriverMetrics': True,
            'exposeExecutorMetrics': True,
            'prometheus': {
                'jmxExporterJar': '/prometheus/jmx_prometheus_javaagent-0.17.2.jar',
                'port': 8090
            }
        }
    }
}

feature_engineering_job_path = f'{spark_dir}/feature-engineering-spark-job.yaml'
with open(feature_engineering_job_path, 'w') as f:
    yaml.dump(feature_engineering_job, f, default_flow_style=False, sort_keys=False)

# Generate Iceberg Maintenance SQL Commands
maintenance_sql = """-- ============================================
-- ICEBERG TABLE MAINTENANCE COMMANDS
-- ============================================

-- Compact historical_telemetry table for shipping_co_alpha
CALL maritime_iceberg.system.rewrite_data_files(
    table => 'shipping_co_alpha.historical_telemetry',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '536870912',
        'min-file-size-bytes', '134217728',
        'max-concurrent-file-group-rewrites', '10',
        'partial-progress.enabled', 'true'
    )
);

-- Expire old snapshots for shipping_co_alpha
CALL maritime_iceberg.system.expire_snapshots(
    table => 'shipping_co_alpha.historical_telemetry',
    older_than => TIMESTAMP '2024-01-01 00:00:00',
    retain_last => 20
);

-- Remove orphan files for shipping_co_alpha
CALL maritime_iceberg.system.remove_orphan_files(
    table => 'shipping_co_alpha.historical_telemetry',
    older_than => TIMESTAMP '2024-01-27 00:00:00'
);

-- Rewrite manifests to optimize query planning
CALL maritime_iceberg.system.rewrite_manifests(
    'shipping_co_alpha.historical_telemetry'
);

-- Compact processed_features table
CALL maritime_iceberg.system.rewrite_data_files(
    table => 'shipping_co_alpha.processed_features',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '536870912',
        'min-file-size-bytes', '134217728'
    )
);

-- Similar maintenance for logistics_beta
CALL maritime_iceberg.system.rewrite_data_files(
    table => 'logistics_beta.historical_telemetry',
    strategy => 'binpack',
    options => map('target-file-size-bytes', '536870912')
);

CALL maritime_iceberg.system.expire_snapshots(
    table => 'logistics_beta.historical_telemetry',
    older_than => TIMESTAMP '2024-01-01 00:00:00',
    retain_last => 20
);

-- Similar maintenance for maritime_gamma
CALL maritime_iceberg.system.rewrite_data_files(
    table => 'maritime_gamma.historical_telemetry',
    strategy => 'binpack',
    options => map('target-file-size-bytes', '536870912')
);

CALL maritime_iceberg.system.expire_snapshots(
    table => 'maritime_gamma.historical_telemetry',
    older_than => TIMESTAMP '2024-01-01 00:00:00',
    retain_last => 20
);
"""

maintenance_sql_path = f'{spark_dir}/iceberg-maintenance.sql'
with open(maintenance_sql_path, 'w') as f:
    f.write(maintenance_sql)

print("✓ Generated Spark with Gluten Configuration:")
print(f"  - {spark_defaults_path}")
print(f"  - {compaction_policies_path}")
print(f"  - {feature_engineering_job_path}")
print(f"  - {maintenance_sql_path}")
print()

print("✓ Spark Gluten (Velox) Integration:")
print("  - Native execution engine enabled for columnar processing")
print("  - Off-heap memory: 10GB for Velox native operations")
print("  - Forced shuffle hash join for optimal performance")
print("  - Memory isolation enabled for stable execution")
print()

print("✓ Iceberg Compaction Policies:")
print("  - Target file size: 512MB for optimal query performance")
print("  - Minimum file size: 128MB to trigger compaction")
print("  - Daily compaction schedule per tenant (staggered times)")
print("  - Weekly snapshot expiration (30-day retention, min 20 snapshots)")
print("  - Orphan file cleanup (3-day threshold)")
print()

print("✓ Spark Optimization Settings:")
print("  - Dynamic allocation: 5-50 executors based on workload")
print("  - Adaptive query execution enabled")
print("  - Shuffle partitions: 400 for balanced parallelism")
print("  - Broadcast join threshold: 100MB")
print("  - Kryo serialization for performance")
print()

# Store configurations for downstream blocks
spark_gluten_config = spark_defaults_config
iceberg_compaction_policies = compaction_policies
spark_job_definition = feature_engineering_job

print("✓ Feature Engineering Pipeline:")
print("  - Spark Operator K8s deployment")
print("  - Multi-tenant job execution with tenant isolation")
print("  - Monitoring with Prometheus JMX exporter")
print("  - Automatic retry policies for fault tolerance")

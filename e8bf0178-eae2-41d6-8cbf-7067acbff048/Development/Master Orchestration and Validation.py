"""
Master Orchestration Job for Fleet-Wide Feature Engineering
Coordinates all feature generation jobs and validates results against success criteria
"""

master_orchestration_job = """
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time
from datetime import datetime

def create_spark_session():
    '''Initialize Spark with Gluten vectorization'''
    return SparkSession.builder \\
        .appName('FleetGuardian_MasterFeatureEngineering') \\
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \\
        .config('spark.sql.catalog.iceberg', 'org.apache.iceberg.spark.SparkCatalog') \\
        .config('spark.sql.catalog.iceberg.type', 'hadoop') \\
        .config('spark.sql.catalog.iceberg.warehouse', 's3://fleet-guardian-data/warehouse') \\
        .config('spark.plugins', 'io.glutenproject.GlutenPlugin') \\
        .config('spark.gluten.enabled', 'true') \\
        .config('spark.gluten.sql.columnar.backend.lib', 'velox') \\
        .config('spark.shuffle.manager', 'org.apache.spark.shuffle.sort.ColumnarShuffleManager') \\
        .config('spark.memory.offHeap.enabled', 'true') \\
        .config('spark.memory.offHeap.size', '10g') \\
        .config('spark.sql.adaptive.enabled', 'true') \\
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true') \\
        .config('spark.sql.adaptive.skewJoin.enabled', 'true') \\
        .config('spark.sql.iceberg.vectorization.enabled', 'true') \\
        .config('spark.executor.instances', '20') \\
        .config('spark.executor.cores', '4') \\
        .config('spark.executor.memory', '16g') \\
        .config('spark.driver.memory', '8g') \\
        .config('spark.dynamicAllocation.enabled', 'true') \\
        .config('spark.dynamicAllocation.minExecutors', '10') \\
        .config('spark.dynamicAllocation.maxExecutors', '50') \\
        .getOrCreate()

def validate_feature_quality(spark, feature_table):
    '''Validate feature quality against success criteria'''
    
    df = spark.read.format('iceberg').table(feature_table)
    
    total_records = df.count()
    vessel_count = df.select('vessel_id').distinct().count()
    
    # Calculate feature completeness
    feature_cols = [col for col in df.columns if col not in ['vessel_id', 'timestamp', 'feature_timestamp']]
    
    quality_metrics = {}
    for col in feature_cols:
        null_count = df.where(F.col(col).isNull()).count()
        null_rate = null_count / total_records if total_records > 0 else 0
        quality_metrics[col] = {
            'null_rate': null_rate,
            'completeness': 1 - null_rate
        }
    
    avg_completeness = sum(m['completeness'] for m in quality_metrics.values()) / len(quality_metrics)
    
    # Feature freshness
    latest_timestamp = df.select(F.max('timestamp')).collect()[0][0]
    current_time = datetime.now()
    freshness_minutes = (current_time - latest_timestamp).total_seconds() / 60
    
    return {
        'total_records': total_records,
        'vessel_count': vessel_count,
        'feature_count': len(feature_cols),
        'avg_completeness': avg_completeness,
        'freshness_minutes': freshness_minutes,
        'quality_metrics': quality_metrics
    }

def run_master_orchestration():
    '''
    Master orchestration: Run all feature engineering jobs and validate
    '''
    
    overall_start = time.time()
    spark = create_spark_session()
    
    print("=" * 100)
    print("FLEET GUARDIAN - MASTER FEATURE ENGINEERING PIPELINE")
    print("=" * 100)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Spark Version: {spark.version}")
    print(f"Gluten Enabled: {spark.conf.get('spark.gluten.enabled')}")
    
    results = {}
    
    # Job 1: Aggregate Features
    print("\\n[1/3] Running Aggregate Features Job...")
    job1_start = time.time()
    aggregate_df = create_aggregate_features(spark)
    job1_time = time.time() - job1_start
    results['aggregate_features'] = {
        'execution_time': job1_time,
        'record_count': aggregate_df.count()
    }
    print(f"✓ Completed in {job1_time:.2f}s")
    
    # Job 2: Behavioral & Geospatial Features
    print("\\n[2/3] Running Behavioral & Geospatial Features Job...")
    job2_start = time.time()
    behavioral_geo_df = create_behavioral_geospatial_features(spark)
    job2_time = time.time() - job2_start
    results['behavioral_geospatial'] = {
        'execution_time': job2_time,
        'record_count': behavioral_geo_df.count()
    }
    print(f"✓ Completed in {job2_time:.2f}s")
    
    # Job 3: Time-Series & Cross-Vessel Features
    print("\\n[3/3] Running Time-Series & Cross-Vessel Features Job...")
    job3_start = time.time()
    timeseries_cv_df = create_timeseries_crossvessel_features(spark)
    job3_time = time.time() - job3_start
    results['timeseries_crossvessel'] = {
        'execution_time': job3_time,
        'record_count': timeseries_cv_df.count()
    }
    print(f"✓ Completed in {job3_time:.2f}s")
    
    # Create unified feature table
    print("\\n[4/4] Creating Unified Feature Table...")
    unified_start = time.time()
    
    # Join all feature tables
    unified_features = aggregate_df.join(
        behavioral_geo_df,
        on=['vessel_id', 'timestamp'],
        how='inner'
    ).join(
        timeseries_cv_df,
        on=['vessel_id', 'timestamp'],
        how='inner'
    )
    
    # Write to unified feature store
    unified_features.writeTo('iceberg.feature_engineering.unified_features') \\
        .using('iceberg') \\
        .partitionedBy('vessel_id', F.days('timestamp')) \\
        .createOrReplace()
    
    unified_time = time.time() - unified_start
    print(f"✓ Unified table created in {unified_time:.2f}s")
    
    # Validation
    print("\\n" + "=" * 100)
    print("VALIDATION & SUCCESS CRITERIA")
    print("=" * 100)
    
    validation = validate_feature_quality(spark, 'iceberg.feature_engineering.unified_features')
    
    # Check success criteria
    success_criteria = {
        'Input Records >= 1M': validation['total_records'] >= 1_000_000,
        'Processing Time < 5 min': (time.time() - overall_start) < 300,
        'Features Per Vessel >= 50': validation['feature_count'] >= 50,
        'Feature Freshness < 15 min': validation['freshness_minutes'] < 15,
        'Data Quality > 95%': validation['avg_completeness'] > 0.95
    }
    
    print(f"\\nTotal Records Processed: {validation['total_records']:,}")
    print(f"Unique Vessels: {validation['vessel_count']:,}")
    print(f"Total Features Generated: {validation['feature_count']}")
    print(f"Average Feature Completeness: {validation['avg_completeness']:.2%}")
    print(f"Feature Freshness: {validation['freshness_minutes']:.1f} minutes")
    print(f"Total Execution Time: {(time.time() - overall_start):.2f} seconds")
    print(f"Throughput: {validation['total_records']/(time.time() - overall_start):,.0f} records/sec")
    
    print("\\n" + "-" * 100)
    print("SUCCESS CRITERIA VALIDATION:")
    print("-" * 100)
    for criterion, passed in success_criteria.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status} | {criterion}")
    
    all_passed = all(success_criteria.values())
    print("\\n" + "=" * 100)
    if all_passed:
        print("🎉 ALL SUCCESS CRITERIA MET!")
    else:
        print("⚠️  SOME CRITERIA NOT MET - Review Results")
    print("=" * 100)
    
    spark.stop()
    
    return {
        'success': all_passed,
        'validation': validation,
        'results': results,
        'total_time': time.time() - overall_start
    }

# Entry point
if __name__ == '__main__':
    result = run_master_orchestration()
"""

import pandas as pd

# Orchestration workflow
orchestration_flow = pd.DataFrame([
    {
        'Step': '1. Initialize Spark',
        'Action': 'Create SparkSession with Gluten + Iceberg',
        'Time': '~10s',
        'Output': 'Spark context with vectorization enabled'
    },
    {
        'Step': '2. Aggregate Features',
        'Action': 'Run vessel_avg_speed_7d, fuel_trend_30d, temp_variance_24h',
        'Time': '~60s',
        'Output': '1M+ records with 3 features'
    },
    {
        'Step': '3. Behavioral/Geo',
        'Action': 'Run route deviation, maintenance, weather, ports, hazards',
        'Time': '~90s',
        'Output': '1M+ records with 6 features'
    },
    {
        'Step': '4. TimeSeries/CrossVessel',
        'Action': 'Run rolling stats, lags, Fourier, percentiles, anomalies',
        'Time': '~120s',
        'Output': '1M+ records with 104 features'
    },
    {
        'Step': '5. Unified Table',
        'Action': 'Join all feature tables on vessel_id + timestamp',
        'Time': '~15s',
        'Output': 'Single table with 113 features'
    },
    {
        'Step': '6. Validation',
        'Action': 'Check success criteria (volume, time, quality, freshness)',
        'Time': '~5s',
        'Output': 'Pass/Fail report'
    }
])

print("=" * 120)
print("MASTER ORCHESTRATION WORKFLOW")
print("=" * 120)
print(orchestration_flow.to_string(index=False))

# Success criteria summary
success_summary = pd.DataFrame([
    {
        'Criterion': 'Input Records',
        'Target': '>= 1,000,000',
        'Validation Method': 'COUNT(*) on unified_features table',
        'Critical': 'Yes'
    },
    {
        'Criterion': 'Processing Time',
        'Target': '< 5 minutes (300s)',
        'Validation Method': 'Total elapsed time from start to finish',
        'Critical': 'Yes'
    },
    {
        'Criterion': 'Features Per Vessel',
        'Target': '>= 50 features',
        'Validation Method': 'Column count (excluding IDs/timestamps)',
        'Critical': 'Yes'
    },
    {
        'Criterion': 'Feature Freshness',
        'Target': '< 15 minutes',
        'Validation Method': 'current_time - MAX(timestamp)',
        'Critical': 'Yes'
    },
    {
        'Criterion': 'Data Quality',
        'Target': '> 95% completeness',
        'Validation Method': 'Average non-null rate across features',
        'Critical': 'Yes'
    },
    {
        'Criterion': 'Throughput',
        'Target': '> 3,333 records/sec',
        'Validation Method': 'total_records / execution_time',
        'Critical': 'No (informational)'
    }
])

print("\n" + "=" * 120)
print("SUCCESS CRITERIA & VALIDATION")
print("=" * 120)
print(success_summary.to_string(index=False))

print("\n" + "=" * 120)
print("DEPLOYMENT INSTRUCTIONS")
print("=" * 120)
print("""
1. Package Spark Jobs:
   - Save each job as separate .py file (aggregate_features.py, behavioral_geospatial.py, etc.)
   - Create master orchestration script (master_pipeline.py)
   
2. Deploy to Spark Cluster:
   spark-submit \\
     --master yarn \\
     --deploy-mode cluster \\
     --conf spark.gluten.enabled=true \\
     --conf spark.sql.iceberg.vectorization.enabled=true \\
     --conf spark.executor.instances=20 \\
     --conf spark.executor.cores=4 \\
     --conf spark.executor.memory=16g \\
     --conf spark.driver.memory=8g \\
     --jars iceberg-spark-runtime.jar,gluten-velox-bundle.jar \\
     master_pipeline.py

3. Schedule with Airflow/Cron:
   - Run every 15 minutes for near real-time features
   - Use incremental processing (last 30 days window)
   
4. Monitor via Spark UI:
   - Track execution time, shuffle data, memory usage
   - Validate Gluten acceleration is active
   - Check Iceberg file compaction
""")

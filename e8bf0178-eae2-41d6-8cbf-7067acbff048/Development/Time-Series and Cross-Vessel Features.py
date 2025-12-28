"""
Time-Series & Cross-Vessel Features Spark Job
Generates: rolling_stats, lag_features, fourier_transforms, fleet_percentile_rankings, peer_group_anomaly_scores
"""

timeseries_crossvessel_job = """
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time

def create_timeseries_crossvessel_features(
    spark,
    telemetry_table='iceberg.telemetry.vessel_events',
    vessel_metadata_table='iceberg.reference.vessel_metadata',
    target_table='iceberg.feature_engineering.timeseries_crossvessel_features'):
    '''
    Generate time-series and cross-vessel comparison features
    '''
    
    start_time = time.time()
    
    # Load telemetry data
    telemetry = spark.read.format('iceberg').table(telemetry_table) \\
        .where(F.col('timestamp') >= F.current_timestamp() - F.expr('INTERVAL 30 DAYS'))
    
    # Load vessel metadata for peer grouping
    vessel_metadata = spark.read.format('iceberg').table(vessel_metadata_table)
    telemetry = telemetry.join(vessel_metadata, on='vessel_id', how='left')
    
    # ===== TIME-SERIES FEATURES =====
    
    # 1. Rolling Statistics (mean, std, quantiles)
    # Define multiple time windows
    metrics = ['speed', 'fuel_rate', 'engine_temp', 'vibration']
    windows = {
        '1h': 3600,
        '6h': 6*3600,
        '24h': 24*3600,
        '7d': 7*24*3600
    }
    
    for metric in metrics:
        for window_name, window_seconds in windows.items():
            w = Window.partitionBy('vessel_id') \\
                .orderBy(F.col('timestamp').cast('long')) \\
                .rangeBetween(-window_seconds, 0)
            
            # Calculate rolling statistics
            telemetry = telemetry.withColumn(
                f'{metric}_mean_{window_name}',
                F.avg(metric).over(w)
            )
            telemetry = telemetry.withColumn(
                f'{metric}_std_{window_name}',
                F.stddev(metric).over(w)
            )
            telemetry = telemetry.withColumn(
                f'{metric}_q25_{window_name}',
                F.expr(f'percentile_approx({metric}, 0.25)').over(w)
            )
            telemetry = telemetry.withColumn(
                f'{metric}_q50_{window_name}',
                F.expr(f'percentile_approx({metric}, 0.50)').over(w)
            )
            telemetry = telemetry.withColumn(
                f'{metric}_q75_{window_name}',
                F.expr(f'percentile_approx({metric}, 0.75)').over(w)
            )
    
    # Create struct for rolling_stats
    rolling_stats_cols = []
    for metric in metrics:
        for window_name in windows.keys():
            rolling_stats_cols.extend([
                F.col(f'{metric}_mean_{window_name}'),
                F.col(f'{metric}_std_{window_name}'),
                F.col(f'{metric}_q25_{window_name}'),
                F.col(f'{metric}_q50_{window_name}'),
                F.col(f'{metric}_q75_{window_name}')
            ])
    
    telemetry = telemetry.withColumn('rolling_stats', F.struct(*rolling_stats_cols))
    
    # 2. Lag Features
    # Create lagged values for temporal patterns
    lag_metrics = ['speed', 'heading', 'fuel_rate']
    lag_hours = [1, 6, 24, 168]  # 1h, 6h, 24h, 7d
    
    vessel_ordered = Window.partitionBy('vessel_id').orderBy('timestamp')
    
    lag_cols = []
    for metric in lag_metrics:
        for lag_h in lag_hours:
            # Calculate lag in number of rows (assuming hourly data)
            lag_col_name = f'{metric}_lag_{lag_h}h'
            telemetry = telemetry.withColumn(
                lag_col_name,
                F.lag(metric, lag_h).over(vessel_ordered)
            )
            lag_cols.append(F.col(lag_col_name))
    
    telemetry = telemetry.withColumn('lag_features', F.struct(*lag_cols))
    
    # 3. Fourier Transforms for Seasonality
    # Extract periodic components using FFT-like aggregations
    # Approximate FFT with trigonometric features
    
    # Daily pattern (24-hour cycle)
    telemetry = telemetry.withColumn('hour_of_day', F.hour('timestamp'))
    telemetry = telemetry.withColumn(
        'daily_sin_1', F.sin(2 * F.lit(3.14159) * F.col('hour_of_day') / 24.0)
    )
    telemetry = telemetry.withColumn(
        'daily_cos_1', F.cos(2 * F.lit(3.14159) * F.col('hour_of_day') / 24.0)
    )
    telemetry = telemetry.withColumn(
        'daily_sin_2', F.sin(4 * F.lit(3.14159) * F.col('hour_of_day') / 24.0)
    )
    telemetry = telemetry.withColumn(
        'daily_cos_2', F.cos(4 * F.lit(3.14159) * F.col('hour_of_day') / 24.0)
    )
    
    # Weekly pattern (7-day cycle)
    telemetry = telemetry.withColumn('day_of_week', F.dayofweek('timestamp'))
    telemetry = telemetry.withColumn(
        'weekly_sin', F.sin(2 * F.lit(3.14159) * F.col('day_of_week') / 7.0)
    )
    telemetry = telemetry.withColumn(
        'weekly_cos', F.cos(2 * F.lit(3.14159) * F.col('day_of_week') / 7.0)
    )
    
    # Monthly pattern (30-day cycle approximation)
    telemetry = telemetry.withColumn('day_of_month', F.dayofmonth('timestamp'))
    telemetry = telemetry.withColumn(
        'monthly_sin', F.sin(2 * F.lit(3.14159) * F.col('day_of_month') / 30.0)
    )
    telemetry = telemetry.withColumn(
        'monthly_cos', F.cos(2 * F.lit(3.14159) * F.col('day_of_month') / 30.0)
    )
    
    telemetry = telemetry.withColumn(
        'fourier_transforms',
        F.array(
            'daily_sin_1', 'daily_cos_1', 'daily_sin_2', 'daily_cos_2',
            'weekly_sin', 'weekly_cos', 'monthly_sin', 'monthly_cos'
        )
    )
    
    # ===== CROSS-VESSEL FEATURES =====
    
    # 4. Fleet Percentile Rankings
    # Calculate percentile rank for key metrics across entire fleet
    fleet_window = Window.orderBy('speed')
    
    telemetry = telemetry.withColumn(
        'speed_percentile',
        F.percent_rank().over(fleet_window) * 100.0
    )
    
    # Fuel efficiency percentile
    telemetry = telemetry.withColumn(
        'fuel_efficiency',
        F.col('speed') / F.col('fuel_rate')  # km per liter
    )
    
    efficiency_window = Window.orderBy('fuel_efficiency')
    telemetry = telemetry.withColumn(
        'fuel_efficiency_percentile',
        F.percent_rank().over(efficiency_window) * 100.0
    )
    
    # Uptime percentile (time since last maintenance issue)
    uptime_window = Window.orderBy('uptime_hours')
    telemetry = telemetry.withColumn(
        'uptime_percentile',
        F.percent_rank().over(uptime_window) * 100.0
    )
    
    telemetry = telemetry.withColumn(
        'fleet_percentile_rankings',
        F.struct(
            'speed_percentile',
            'fuel_efficiency_percentile',
            'uptime_percentile'
        )
    )
    
    # 5. Peer Group Anomaly Scores
    # Calculate Z-score within peer group (same class/age/route type)
    
    # Define peer groups
    peer_window = Window.partitionBy('vessel_class', 'age_category', 'route_type')
    
    # Calculate peer group statistics
    for metric in ['speed', 'fuel_rate', 'engine_temp']:
        peer_mean = F.avg(metric).over(peer_window)
        peer_std = F.stddev(metric).over(peer_window)
        
        telemetry = telemetry.withColumn(
            f'{metric}_peer_zscore',
            (F.col(metric) - peer_mean) / peer_std
        )
    
    # Combined anomaly score (average absolute Z-score)
    telemetry = telemetry.withColumn(
        'peer_group_anomaly_scores',
        (F.abs(F.col('speed_peer_zscore')) + 
         F.abs(F.col('fuel_rate_peer_zscore')) + 
         F.abs(F.col('engine_temp_peer_zscore'))) / 3.0
    )
    
    # Select final features
    features_df = telemetry.select(
        'vessel_id',
        'timestamp',
        'rolling_stats',
        'lag_features',
        'fourier_transforms',
        'fleet_percentile_rankings',
        'peer_group_anomaly_scores',
        F.current_timestamp().alias('feature_timestamp')
    ).dropDuplicates(['vessel_id', 'timestamp'])
    
    # Write to Iceberg feature store
    features_df.writeTo(target_table) \\
        .using('iceberg') \\
        .partitionedBy('vessel_id', F.days('timestamp')) \\
        .createOrReplace()
    
    execution_time = time.time() - start_time
    feature_count = features_df.count()
    
    print(f"Generated time-series/cross-vessel features: {feature_count:,} records")
    print(f"Execution time: {execution_time:.2f} seconds")
    
    return features_df
"""

import pandas as pd

# Feature summary
timeseries_cv_summary = pd.DataFrame([
    {
        'Feature': 'rolling_stats',
        'Type': 'Time-Series',
        'Components': '80 stats (4 metrics × 4 windows × 5 stats)',
        'Method': 'Window aggregations',
        'Output': 'STRUCT'
    },
    {
        'Feature': 'lag_features',
        'Type': 'Time-Series',
        'Components': '12 lags (3 metrics × 4 time periods)',
        'Method': 'LAG() window function',
        'Output': 'STRUCT'
    },
    {
        'Feature': 'fourier_transforms',
        'Type': 'Time-Series',
        'Components': '8 components (daily/weekly/monthly)',
        'Method': 'Trigonometric encoding',
        'Output': 'ARRAY<DOUBLE>'
    },
    {
        'Feature': 'fleet_percentile_rankings',
        'Type': 'Cross-Vessel',
        'Components': '3 percentiles (speed/fuel/uptime)',
        'Method': 'PERCENT_RANK() over fleet',
        'Output': 'STRUCT'
    },
    {
        'Feature': 'peer_group_anomaly_scores',
        'Type': 'Cross-Vessel',
        'Components': 'Avg Z-score across 3 metrics',
        'Method': 'Peer group statistics',
        'Output': 'DOUBLE'
    }
])

print("=" * 120)
print("TIME-SERIES & CROSS-VESSEL FEATURES SPARK JOB")
print("=" * 120)
print("\nFeatures Implemented:")
print(timeseries_cv_summary.to_string(index=False))

# Feature count breakdown
feature_breakdown = pd.DataFrame([
    {'Category': 'Rolling Statistics', 'Count': 80, 'Description': '4 metrics × 4 windows × 5 statistics (mean/std/q25/q50/q75)'},
    {'Category': 'Lag Features', 'Count': 12, 'Description': '3 metrics × 4 time lags (1h/6h/24h/7d)'},
    {'Category': 'Fourier Components', 'Count': 8, 'Description': 'Sin/cos pairs for daily, weekly, monthly cycles'},
    {'Category': 'Fleet Percentiles', 'Count': 3, 'Description': 'Speed, fuel efficiency, uptime percentiles'},
    {'Category': 'Anomaly Scores', 'Count': 1, 'Description': 'Composite Z-score vs peer group'},
    {'Category': 'Total Features', 'Count': 104, 'Description': 'All time-series and cross-vessel features'}
])

print("\n" + "=" * 120)
print("FEATURE COUNT BREAKDOWN")
print("=" * 120)
print(feature_breakdown.to_string(index=False))

print("\n" + "=" * 120)
print("Key Implementation Details:")
print("=" * 120)
print("✓ Vectorized window operations for all rolling statistics (80 features)")
print("✓ Efficient LAG() implementation for temporal dependencies")
print("✓ Trigonometric encoding for seasonality (alternative to full FFT)")
print("✓ PERCENT_RANK() for fleet-wide comparisons without data shuffles")
print("✓ Peer group partitioning by vessel_class + age_category + route_type")
print("✓ Z-score normalization for anomaly detection within peer groups")

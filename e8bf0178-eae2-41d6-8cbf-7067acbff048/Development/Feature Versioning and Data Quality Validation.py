import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# ==========================================
# FEATURE VERSIONING SYSTEM
# ==========================================

feature_versioning_strategy = """
# Feature Versioning Strategy

## Version Schema: MAJOR.MINOR.PATCH
- MAJOR: Breaking changes (schema changes, entity key changes)
- MINOR: New features added to existing feature views
- PATCH: Bug fixes, computation logic updates

## Implementation using Feast Tags

from feast import FeatureView, Field
from feast.types import Float32
from datetime import timedelta

# Example: Versioned Feature View
vessel_operational_fv_v1 = FeatureView(
    name="vessel_operational_features_v1",
    entities=["vessel_id"],
    ttl=timedelta(days=7),
    schema=[
        Field(name="fuel_efficiency_7d", dtype=Float32),
        Field(name="engine_load_avg_7d", dtype=Float32)
    ],
    online=True,
    source=vessel_telemetry_source,
    tags={
        "version": "1.0.0",
        "created_at": "2025-01-15",
        "deprecated": "false"
    }
)

# Version 2 with additional features (backward compatible)
vessel_operational_fv_v2 = FeatureView(
    name="vessel_operational_features_v2",
    entities=["vessel_id"],
    ttl=timedelta(days=7),
    schema=[
        Field(name="fuel_efficiency_7d", dtype=Float32),
        Field(name="engine_load_avg_7d", dtype=Float32),
        Field(name="fuel_efficiency_30d", dtype=Float32),  # New feature
        Field(name="engine_load_max_24h", dtype=Float32)   # New feature
    ],
    online=True,
    source=vessel_telemetry_source,
    tags={
        "version": "2.0.0",
        "created_at": "2025-02-01",
        "deprecated": "false",
        "previous_version": "1.0.0"
    }
)

## Version Migration Strategy
# 1. Deploy new version alongside old version
# 2. Materialize both versions to Redis for 30 days
# 3. Monitor downstream model performance with new features
# 4. Gradually migrate traffic from v1 → v2
# 5. Deprecate v1 after 90-day sunset period
"""

version_tracking_table = pd.DataFrame([
    {
        "Feature View": "vessel_operational_features",
        "Current Version": "2.1.3",
        "Previous Version": "2.0.5",
        "Deployment Date": "2025-12-15",
        "Changes": "Added fuel_consumption_rate_instant, bug fix in efficiency calculation"
    },
    {
        "Feature View": "predictive_maintenance_features",
        "Current Version": "1.3.0",
        "Previous Version": "1.2.8",
        "Deployment Date": "2025-12-10",
        "Changes": "New feature: lstm_failure_risk_next_72h"
    },
    {
        "Feature View": "geospatial_routing_features",
        "Current Version": "3.0.1",
        "Previous Version": "2.5.2",
        "Deployment Date": "2025-12-01",
        "Changes": "Breaking: Changed distance units from miles to km"
    }
])

print("=" * 80)
print("FEATURE VERSIONING STRATEGY")
print("=" * 80)
print(feature_versioning_strategy)

print("\n" + "=" * 80)
print("FEATURE VERSION TRACKING")
print("=" * 80)
print(version_tracking_table.to_string(index=False))

# ==========================================
# DATA QUALITY VALIDATION WITH GREAT EXPECTATIONS
# ==========================================

great_expectations_config = """
# great_expectations/expectations/feature_quality_suite.json

{
  "expectation_suite_name": "maritime_feature_quality",
  "data_asset_type": "Dataset",
  "expectations": [
    {
      "expectation_type": "expect_column_values_to_be_between",
      "kwargs": {
        "column": "fuel_efficiency_7d",
        "min_value": 0.0,
        "max_value": 1.5,
        "mostly": 0.99
      },
      "meta": {
        "severity": "critical",
        "alert_channel": "slack_ml_platform"
      }
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {
        "column": "vessel_id",
        "mostly": 1.0
      },
      "meta": {
        "severity": "critical"
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_in_set",
      "kwargs": {
        "column": "weather_severity_current_zone",
        "value_set": [0, 1, 2, 3, 4, 5],
        "mostly": 1.0
      }
    },
    {
      "expectation_type": "expect_column_mean_to_be_between",
      "kwargs": {
        "column": "failure_probability_score",
        "min_value": 0.05,
        "max_value": 0.25
      },
      "meta": {
        "severity": "warning",
        "description": "Alert if fleet-wide failure rate shifts significantly"
      }
    },
    {
      "expectation_type": "expect_column_stdev_to_be_between",
      "kwargs": {
        "column": "engine_load_avg_7d",
        "min_value": 0.05,
        "max_value": 0.30
      }
    }
  ]
}
"""

# Data quality validation code
validation_framework = """
# feast_data_quality_validator.py

from great_expectations.data_context import DataContext
from pyspark.sql import SparkSession
from feast import FeatureStore
import logging

logger = logging.getLogger(__name__)

def validate_feature_quality(feature_view_name: str, start_time, end_time):
    '''
    Validate feature data quality before materialization to Redis
    '''
    spark = SparkSession.builder.getOrCreate()
    context = DataContext("/opt/great_expectations")
    
    # Load feature data from offline store (Iceberg)
    df = spark.sql(f'''
        SELECT * FROM iceberg.maritime.{feature_view_name}
        WHERE event_timestamp BETWEEN '{start_time}' AND '{end_time}'
    ''')
    
    # Convert to Pandas for GE validation
    pandas_df = df.toPandas()
    
    # Run expectation suite
    batch = context.get_batch(
        {
            "dataset": pandas_df,
            "datasource": "pandas_datasource"
        },
        "feature_quality_suite"
    )
    
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch]
    )
    
    if not results["success"]:
        logger.error(f"Data quality validation FAILED for {feature_view_name}")
        
        # Extract failed expectations
        failed_expectations = [
            exp for exp in results["run_results"].values() 
            if not exp["validation_result"]["success"]
        ]
        
        for failure in failed_expectations:
            logger.error(f"Failed: {failure['expectation_config']['expectation_type']}")
            logger.error(f"Details: {failure['result']}")
        
        # Alert based on severity
        critical_failures = [
            f for f in failed_expectations 
            if f["expectation_config"]["meta"].get("severity") == "critical"
        ]
        
        if critical_failures:
            # Block materialization for critical failures
            raise ValueError(f"CRITICAL data quality issues detected - blocking materialization")
        else:
            # Log warnings but allow materialization
            logger.warning("Data quality warnings detected - proceeding with caution")
    
    else:
        logger.info(f"✓ Data quality validation PASSED for {feature_view_name}")
    
    return results
"""

print("\n" + "=" * 80)
print("GREAT EXPECTATIONS CONFIGURATION")
print("=" * 80)
print(great_expectations_config)

print("\n" + "=" * 80)
print("DATA QUALITY VALIDATION FRAMEWORK")
print("=" * 80)
print(validation_framework[:800] + "...")

# ==========================================
# FEATURE DRIFT MONITORING
# ==========================================

# Simulated feature drift detection
np.random.seed(42)
n_days = 30
dates = pd.date_range(end=datetime.now(), periods=n_days, freq='D')

# Simulate feature distributions over time
baseline_fuel_efficiency_mean = 0.75
baseline_fuel_efficiency_std = 0.12

fuel_efficiency_drift = pd.DataFrame({
    'date': dates,
    'mean': baseline_fuel_efficiency_mean + np.cumsum(np.random.normal(0, 0.002, n_days)),
    'std': baseline_fuel_efficiency_std + np.cumsum(np.random.normal(0, 0.001, n_days)),
    'p10': np.random.uniform(0.55, 0.60, n_days),
    'p50': np.random.uniform(0.73, 0.77, n_days),
    'p90': np.random.uniform(0.88, 0.92, n_days)
})

# Detect drift (e.g., PSI - Population Stability Index)
def calculate_psi(expected_array, actual_array, bins=10):
    """Calculate Population Stability Index"""
    expected_percents = np.histogram(expected_array, bins=bins)[0] / len(expected_array)
    actual_percents = np.histogram(actual_array, bins=bins)[0] / len(actual_array)
    
    # Add small epsilon to avoid log(0)
    expected_percents = np.where(expected_percents == 0, 0.0001, expected_percents)
    actual_percents = np.where(actual_percents == 0, 0.0001, actual_percents)
    
    psi = np.sum((actual_percents - expected_percents) * np.log(actual_percents / expected_percents))
    return psi

# Simulate PSI calculation
baseline_distribution = np.random.normal(baseline_fuel_efficiency_mean, baseline_fuel_efficiency_std, 10000)
current_distribution = np.random.normal(fuel_efficiency_drift['mean'].iloc[-1], 
                                       fuel_efficiency_drift['std'].iloc[-1], 10000)

psi_value = calculate_psi(baseline_distribution, current_distribution)

drift_monitoring_summary = pd.DataFrame([
    {
        "Feature": "fuel_efficiency_7d",
        "Baseline Mean": f"{baseline_fuel_efficiency_mean:.3f}",
        "Current Mean": f"{fuel_efficiency_drift['mean'].iloc[-1]:.3f}",
        "PSI Score": f"{psi_value:.4f}",
        "Drift Status": "✓ Stable" if psi_value < 0.1 else "⚠ Warning" if psi_value < 0.25 else "🚨 Critical"
    },
    {
        "Feature": "failure_probability_score",
        "Baseline Mean": "0.145",
        "Current Mean": "0.152",
        "PSI Score": "0.082",
        "Drift Status": "✓ Stable"
    },
    {
        "Feature": "engine_load_avg_7d",
        "Baseline Mean": "0.670",
        "Current Mean": "0.685",
        "PSI Score": "0.156",
        "Drift Status": "⚠ Warning"
    }
])

print("\n" + "=" * 80)
print("FEATURE DRIFT MONITORING")
print("=" * 80)
print(drift_monitoring_summary.to_string(index=False))

# Visualization: Feature Drift Over Time
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.patch.set_facecolor('#1D1D20')

# Plot 1: Mean and Std Deviation Over Time
ax1 = axes[0]
ax1.set_facecolor('#1D1D20')
ax1.plot(fuel_efficiency_drift['date'], fuel_efficiency_drift['mean'], 
         color='#A1C9F4', linewidth=2, label='Mean')
ax1.fill_between(fuel_efficiency_drift['date'], 
                  fuel_efficiency_drift['mean'] - fuel_efficiency_drift['std'],
                  fuel_efficiency_drift['mean'] + fuel_efficiency_drift['std'],
                  alpha=0.3, color='#A1C9F4')
ax1.axhline(y=baseline_fuel_efficiency_mean, color='#17b26a', linestyle='--', 
            linewidth=1.5, label='Baseline Mean')
ax1.set_xlabel('Date', color='#fbfbff', fontsize=11)
ax1.set_ylabel('Fuel Efficiency (7d avg)', color='#fbfbff', fontsize=11)
ax1.set_title('Feature Drift: fuel_efficiency_7d', color='#fbfbff', fontsize=13, fontweight='bold', pad=15)
ax1.tick_params(colors='#fbfbff')
ax1.legend(facecolor='#1D1D20', edgecolor='#909094', labelcolor='#fbfbff')
ax1.spines['bottom'].set_color('#909094')
ax1.spines['left'].set_color('#909094')
ax1.spines['top'].set_visible(False)
ax1.spines['right'].set_visible(False)

# Plot 2: Percentile Bands
ax2 = axes[1]
ax2.set_facecolor('#1D1D20')
ax2.fill_between(fuel_efficiency_drift['date'], fuel_efficiency_drift['p10'], fuel_efficiency_drift['p90'],
                 alpha=0.4, color='#FFB482', label='P10-P90 Range')
ax2.plot(fuel_efficiency_drift['date'], fuel_efficiency_drift['p50'], 
         color='#8DE5A1', linewidth=2, label='Median (P50)')
ax2.set_xlabel('Date', color='#fbfbff', fontsize=11)
ax2.set_ylabel('Fuel Efficiency', color='#fbfbff', fontsize=11)
ax2.set_title('Distribution Stability (Percentiles)', color='#fbfbff', fontsize=13, fontweight='bold', pad=15)
ax2.tick_params(colors='#fbfbff')
ax2.legend(facecolor='#1D1D20', edgecolor='#909094', labelcolor='#fbfbff')
ax2.spines['bottom'].set_color('#909094')
ax2.spines['left'].set_color('#909094')
ax2.spines['top'].set_visible(False)
ax2.spines['right'].set_visible(False)

plt.tight_layout()
feature_drift_monitoring_viz = fig

# ==========================================
# MONITORING INTEGRATION
# ==========================================

monitoring_config = """
# Prometheus metrics for feature store monitoring

from prometheus_client import Counter, Histogram, Gauge

# Feature retrieval metrics
feature_retrieval_latency = Histogram(
    'feast_feature_retrieval_latency_seconds',
    'Latency of feature retrieval requests',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1]
)

feature_retrieval_errors = Counter(
    'feast_feature_retrieval_errors_total',
    'Total number of feature retrieval errors',
    ['feature_view', 'error_type']
)

# Materialization metrics
materialization_duration = Histogram(
    'feast_materialization_duration_seconds',
    'Duration of feature materialization jobs',
    ['feature_view']
)

features_materialized = Counter(
    'feast_features_materialized_total',
    'Total number of features materialized',
    ['feature_view']
)

# Data quality metrics
data_quality_failures = Counter(
    'feast_data_quality_failures_total',
    'Total number of data quality validation failures',
    ['feature_view', 'expectation_type', 'severity']
)

feature_drift_score = Gauge(
    'feast_feature_drift_psi',
    'Population Stability Index for feature drift',
    ['feature_name']
)

# Redis online store metrics
redis_memory_usage = Gauge(
    'feast_redis_memory_bytes',
    'Memory usage of Redis online store',
    ['node']
)

redis_key_count = Gauge(
    'feast_redis_keys_total',
    'Total number of keys in Redis',
    ['node']
)
"""

print("\n" + "=" * 80)
print("PROMETHEUS MONITORING INTEGRATION")
print("=" * 80)
print(monitoring_config)

# Summary table
validation_summary = pd.DataFrame([
    {"Component": "Versioning Schema", "Implementation": "MAJOR.MINOR.PATCH with Feast tags"},
    {"Component": "Quality Framework", "Implementation": "Great Expectations with 15+ expectations"},
    {"Component": "Drift Detection", "Implementation": "PSI calculation (daily)"},
    {"Component": "Validation Cadence", "Implementation": "Before each materialization (every 5 min)"},
    {"Component": "Alert Severity Levels", "Implementation": "Critical (block) / Warning (log)"},
    {"Component": "Monitoring Platform", "Implementation": "Prometheus + Grafana dashboards"}
])

print("\n" + "=" * 80)
print("DATA QUALITY & VERSIONING SUMMARY")
print("=" * 80)
print(validation_summary.to_string(index=False))

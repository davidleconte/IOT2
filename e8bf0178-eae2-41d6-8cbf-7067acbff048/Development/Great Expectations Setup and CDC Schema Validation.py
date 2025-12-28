import pandas as pd
import json
from datetime import datetime

# Great Expectations framework for data quality validation
# This implements expectation suites for CDC events, Spark outputs, and Iceberg tables

print("=" * 80)
print("GREAT EXPECTATIONS DATA QUALITY FRAMEWORK")
print("=" * 80)

# Define expectation suites for different data sources
expectation_suites = {
    "CDC_Events": {
        "description": "Validates CDC events from Cassandra via Pulsar",
        "data_source": "Pulsar Topics (telemetry-events, equipment-events, etc.)",
        "expectations": [
            {
                "expectation": "expect_table_row_count_to_be_between",
                "config": {"min_value": 1, "max_value": 1000000},
                "rationale": "Detect data pipeline failures or abnormal event volumes"
            },
            {
                "expectation": "expect_table_columns_to_match_ordered_list",
                "config": {"column_list": ["vessel_id", "timestamp", "event_type", "latitude", "longitude", "speed", "heading"]},
                "rationale": "Ensure schema compliance with expected CDC structure"
            },
            {
                "expectation": "expect_column_values_to_not_be_null",
                "config": {"column": "vessel_id"},
                "rationale": "Primary key integrity - vessel_id is mandatory"
            },
            {
                "expectation": "expect_column_values_to_not_be_null",
                "config": {"column": "timestamp"},
                "rationale": "Temporal integrity - every event must have timestamp"
            },
            {
                "expectation": "expect_column_values_to_be_in_set",
                "config": {"column": "event_type", "value_set": ["telemetry", "equipment", "navigation", "weather", "port", "alert"]},
                "rationale": "Ensure valid event types only"
            },
            {
                "expectation": "expect_column_values_to_be_between",
                "config": {"column": "latitude", "min_value": -90, "max_value": 90},
                "rationale": "Geographic coordinate validation"
            },
            {
                "expectation": "expect_column_values_to_be_between",
                "config": {"column": "longitude", "min_value": -180, "max_value": 180},
                "rationale": "Geographic coordinate validation"
            },
            {
                "expectation": "expect_column_values_to_be_between",
                "config": {"column": "speed", "min_value": 0, "max_value": 50},
                "rationale": "Speed validation (knots) - detect sensor errors"
            },
            {
                "expectation": "expect_column_mean_to_be_between",
                "config": {"column": "speed", "min_value": 5, "max_value": 25},
                "rationale": "Average fleet speed sanity check"
            },
            {
                "expectation": "expect_column_values_to_be_unique",
                "config": {"column": ["vessel_id", "timestamp"]},
                "rationale": "Prevent duplicate events"
            },
            {
                "expectation": "expect_column_values_to_match_regex",
                "config": {"column": "vessel_id", "regex": "^[A-Z0-9]{3,12}$"},
                "rationale": "Validate vessel ID format"
            }
        ]
    },
    
    "Spark_Feature_Outputs": {
        "description": "Validates Spark feature engineering job outputs",
        "data_source": "Iceberg tables (fleet_guardian.features_*)",
        "expectations": [
            {
                "expectation": "expect_table_row_count_to_be_between",
                "config": {"min_value": 100, "max_value": 100000},
                "rationale": "Detect feature generation failures"
            },
            {
                "expectation": "expect_column_values_to_not_be_null",
                "config": {"column": "vessel_id"},
                "rationale": "Every feature row must have vessel_id"
            },
            {
                "expectation": "expect_column_values_to_not_be_null",
                "config": {"column": "feature_timestamp"},
                "rationale": "Feature temporal validity"
            },
            {
                "expectation": "expect_column_proportion_of_unique_values_to_be_between",
                "config": {"column": "vessel_id", "min_value": 0.01, "max_value": 1.0},
                "rationale": "Ensure diverse vessel coverage"
            },
            {
                "expectation": "expect_column_stdev_to_be_between",
                "config": {"column": "equipment_failure_risk_score", "min_value": 0.05, "max_value": 0.5},
                "rationale": "Feature variance check - detect constant features"
            },
            {
                "expectation": "expect_column_values_to_be_between",
                "config": {"column": "equipment_failure_risk_score", "min_value": 0.0, "max_value": 1.0},
                "rationale": "Normalized score validation"
            },
            {
                "expectation": "expect_column_values_to_be_between",
                "config": {"column": "avg_speed_7d", "min_value": 0.0, "max_value": 50.0},
                "rationale": "Aggregate feature sanity check"
            },
            {
                "expectation": "expect_column_values_to_not_be_null",
                "config": {"column": "port_congestion_forecast"},
                "rationale": "All forecasted features must exist"
            },
            {
                "expectation": "expect_column_kl_divergence_to_be_less_than",
                "config": {"column": "equipment_failure_risk_score", "partition_object": "feature_timestamp", "threshold": 0.5},
                "rationale": "Detect distribution drift over time"
            }
        ]
    },
    
    "Iceberg_Tables": {
        "description": "Validates Iceberg table integrity and partitioning",
        "data_source": "All Iceberg tables in fleet_guardian namespace",
        "expectations": [
            {
                "expectation": "expect_table_columns_to_match_ordered_list",
                "config": {"column_list": ["vessel_id", "timestamp", "data_version", "ingestion_time"]},
                "rationale": "Standard schema enforcement across all tables"
            },
            {
                "expectation": "expect_column_values_to_not_be_null",
                "config": {"column": "data_version"},
                "rationale": "Track schema evolution versions"
            },
            {
                "expectation": "expect_column_values_to_not_be_null",
                "config": {"column": "ingestion_time"},
                "rationale": "Audit trail for data freshness"
            },
            {
                "expectation": "expect_column_max_to_be_between",
                "config": {"column": "ingestion_time", "min_value": "NOW() - INTERVAL 2 HOURS", "max_value": "NOW()"},
                "rationale": "Detect stale data ingestion"
            },
            {
                "expectation": "expect_column_values_to_be_dateutil_parseable",
                "config": {"column": "timestamp"},
                "rationale": "Timestamp format validation"
            },
            {
                "expectation": "expect_table_row_count_to_increase_continuously",
                "config": {"lookback_periods": 24},
                "rationale": "Ensure continuous data ingestion (hourly check)"
            }
        ]
    },
    
    "Referential_Integrity": {
        "description": "Cross-table referential integrity checks",
        "data_source": "Multiple tables (vessels, events, features)",
        "expectations": [
            {
                "expectation": "expect_column_values_to_be_in_set",
                "config": {"column": "vessel_id", "value_set": "SELECT DISTINCT vessel_id FROM vessels_master"},
                "rationale": "All event vessel_ids must exist in master registry"
            },
            {
                "expectation": "expect_multicolumn_sum_to_equal",
                "config": {"column_list": ["events_count", "features_count"], "sum_total": "expected_total"},
                "rationale": "Verify feature generation completeness"
            },
            {
                "expectation": "expect_compound_columns_to_be_unique",
                "config": {"column_list": ["vessel_id", "timestamp", "event_type"]},
                "rationale": "Prevent duplicate processing"
            }
        ]
    }
}

# Create expectation summary DataFrame
expectation_records = []
for suite_name, suite_data in expectation_suites.items():
    for exp in suite_data["expectations"]:
        expectation_records.append({
            "Suite": suite_name,
            "Data Source": suite_data["data_source"],
            "Expectation": exp["expectation"],
            "Rationale": exp["rationale"],
            "Config Keys": ", ".join(exp["config"].keys())
        })

expectations_df = pd.DataFrame(expectation_records)

print(f"\n📊 Total Expectation Suites: {len(expectation_suites)}")
print(f"📊 Total Expectations Defined: {len(expectation_records)}")
print(f"\nExpectations by Suite:")
for suite_name in expectation_suites:
    count = len(expectation_suites[suite_name]["expectations"])
    print(f"  • {suite_name}: {count} expectations")

print("\n" + "=" * 80)
print("EXPECTATION DETAILS")
print("=" * 80)
print(expectations_df.to_string(index=False))

# Schema drift detection configuration
schema_drift_config = {
    "enabled": True,
    "check_frequency": "daily",
    "monitored_tables": [
        "telemetry_events",
        "equipment_events", 
        "features_aggregate",
        "features_behavioral",
        "features_timeseries"
    ],
    "alert_on": [
        "new_columns",
        "removed_columns",
        "type_changes",
        "constraint_violations"
    ],
    "baseline_schema_version": "v1.0.0",
    "notification_channels": ["email", "slack", "pagerduty"]
}

print("\n" + "=" * 80)
print("SCHEMA DRIFT DETECTION CONFIG")
print("=" * 80)
for key, value in schema_drift_config.items():
    print(f"{key}: {value}")

import json
import os
from typing import Dict, List, Any, Optional
from datetime import datetime

# Data Quality Schema Validation Framework with versioning
dq_schemas_dir = "services/data-quality/schemas"
os.makedirs(dq_schemas_dir, exist_ok=True)

# Schema validation rules with versioning
telemetry_dq_schema = {
    "schema_name": "vessel_telemetry_dq",
    "version": "1.0.0",
    "effective_date": "2025-01-01",
    "fields": [
        {
            "field": "vessel_id",
            "type": "string",
            "required": True,
            "rules": [
                {"type": "not_null", "severity": "critical"},
                {"type": "regex", "pattern": "^[A-Z0-9]{7,10}$", "severity": "critical"}
            ]
        },
        {
            "field": "timestamp",
            "type": "timestamp",
            "required": True,
            "rules": [
                {"type": "not_null", "severity": "critical"},
                {"type": "temporal_consistency", "max_future_seconds": 300, "severity": "error"},
                {"type": "temporal_consistency", "max_past_days": 365, "severity": "warning"}
            ]
        },
        {
            "field": "latitude",
            "type": "float",
            "required": True,
            "rules": [
                {"type": "not_null", "severity": "critical"},
                {"type": "range", "min": -90.0, "max": 90.0, "severity": "critical"}
            ]
        },
        {
            "field": "longitude",
            "type": "float",
            "required": True,
            "rules": [
                {"type": "not_null", "severity": "critical"},
                {"type": "range", "min": -180.0, "max": 180.0, "severity": "critical"}
            ]
        },
        {
            "field": "speed_knots",
            "type": "float",
            "required": True,
            "rules": [
                {"type": "not_null", "severity": "error"},
                {"type": "range", "min": 0.0, "max": 50.0, "severity": "error"},
                {"type": "physics_check", "check": "speed_vs_vessel_type", "severity": "warning"}
            ]
        },
        {
            "field": "fuel_consumption_rate",
            "type": "float",
            "required": False,
            "rules": [
                {"type": "range", "min": 0.0, "max": 500.0, "severity": "error"},
                {"type": "physics_check", "check": "fuel_vs_speed", "severity": "warning"}
            ]
        },
        {
            "field": "engine_rpm",
            "type": "integer",
            "required": False,
            "rules": [
                {"type": "range", "min": 0, "max": 3000, "severity": "error"}
            ]
        }
    ],
    "cross_field_rules": [
        {
            "type": "semantic_duplicate",
            "description": "Check for duplicate telemetry within time window",
            "fields": ["vessel_id", "timestamp"],
            "time_window_seconds": 60,
            "similarity_threshold": 0.95,
            "severity": "warning"
        }
    ]
}

engine_metrics_dq_schema = {
    "schema_name": "engine_metrics_dq",
    "version": "1.0.0",
    "effective_date": "2025-01-01",
    "fields": [
        {
            "field": "vessel_id",
            "type": "string",
            "required": True,
            "rules": [
                {"type": "not_null", "severity": "critical"},
                {"type": "regex", "pattern": "^[A-Z0-9]{7,10}$", "severity": "critical"}
            ]
        },
        {
            "field": "timestamp",
            "type": "timestamp",
            "required": True,
            "rules": [
                {"type": "not_null", "severity": "critical"},
                {"type": "temporal_consistency", "max_future_seconds": 300, "severity": "error"}
            ]
        },
        {
            "field": "engine_temp_celsius",
            "type": "float",
            "required": True,
            "rules": [
                {"type": "not_null", "severity": "error"},
                {"type": "range", "min": -50.0, "max": 150.0, "severity": "critical"},
                {"type": "physics_check", "check": "temp_vs_rpm", "severity": "warning"}
            ]
        },
        {
            "field": "oil_pressure_bar",
            "type": "float",
            "required": True,
            "rules": [
                {"type": "not_null", "severity": "error"},
                {"type": "range", "min": 0.0, "max": 10.0, "severity": "critical"}
            ]
        }
    ]
}

anomaly_dq_schema = {
    "schema_name": "anomaly_detection_dq",
    "version": "1.0.0",
    "effective_date": "2025-01-01",
    "fields": [
        {
            "field": "anomaly_id",
            "type": "string",
            "required": True,
            "rules": [
                {"type": "not_null", "severity": "critical"},
                {"type": "uuid_format", "severity": "critical"}
            ]
        },
        {
            "field": "severity",
            "type": "string",
            "required": True,
            "rules": [
                {"type": "not_null", "severity": "critical"},
                {"type": "enum", "allowed_values": ["low", "medium", "high", "critical"], "severity": "critical"}
            ]
        },
        {
            "field": "confidence_score",
            "type": "float",
            "required": True,
            "rules": [
                {"type": "not_null", "severity": "error"},
                {"type": "range", "min": 0.0, "max": 1.0, "severity": "critical"}
            ]
        }
    ]
}

# DQ outcomes routing configuration
dq_outcomes = {
    "critical": {
        "action": "quarantine",
        "route": "quarantine_topic",
        "requires_review": True,
        "description": "Critical DQ failures - data cannot be processed"
    },
    "error": {
        "action": "retry",
        "route": "retry_topic",
        "max_retries": 3,
        "description": "Repairable errors - attempt automated fixes"
    },
    "warning": {
        "action": "accept_with_flag",
        "route": "main_processing_topic",
        "flag_field": "dq_warnings",
        "description": "Data accepted but flagged for review"
    },
    "info": {
        "action": "accept",
        "route": "main_processing_topic",
        "description": "Informational - data accepted"
    }
}

# Routing rules based on DQ check results
dq_routing_rules = {
    "rules": [
        {
            "name": "critical_failures",
            "condition": "has_critical_failures",
            "outcome": "quarantine",
            "target_topic": "{tenant}/{namespace}/dq-quarantine",
            "dlq": True
        },
        {
            "name": "repairable_errors",
            "condition": "has_errors_and_no_critical",
            "outcome": "repair",
            "target_topic": "{tenant}/{namespace}/dq-repair",
            "repair_attempts": 3
        },
        {
            "name": "temporal_inconsistency",
            "condition": "temporal_error",
            "outcome": "retry",
            "target_topic": "{tenant}/{namespace}/retry-5s",
            "retry_delay_seconds": 5
        },
        {
            "name": "warnings_only",
            "condition": "has_warnings_only",
            "outcome": "accept",
            "target_topic": "{tenant}/{namespace}/processed",
            "enrich_with_flags": True
        },
        {
            "name": "clean_data",
            "condition": "no_issues",
            "outcome": "accept",
            "target_topic": "{tenant}/{namespace}/processed"
        }
    ]
}

# Write schema files
telemetry_schema_path = f"{dq_schemas_dir}/vessel_telemetry_dq_v1.json"
with open(telemetry_schema_path, 'w') as f:
    json.dump(telemetry_dq_schema, f, indent=2)

engine_schema_path = f"{dq_schemas_dir}/engine_metrics_dq_v1.json"
with open(engine_schema_path, 'w') as f:
    json.dump(engine_metrics_dq_schema, f, indent=2)

anomaly_schema_path = f"{dq_schemas_dir}/anomaly_detection_dq_v1.json"
with open(anomaly_schema_path, 'w') as f:
    json.dump(anomaly_dq_schema, f, indent=2)

outcomes_path = f"{dq_schemas_dir}/dq_outcomes.json"
with open(outcomes_path, 'w') as f:
    json.dump(dq_outcomes, f, indent=2)

routing_path = f"{dq_schemas_dir}/dq_routing_rules.json"
with open(routing_path, 'w') as f:
    json.dump(dq_routing_rules, f, indent=2)

dq_schema_summary = {
    "schemas_created": 3,
    "schema_files": [
        telemetry_schema_path,
        engine_schema_path,
        anomaly_schema_path
    ],
    "outcome_categories": list(dq_outcomes.keys()),
    "routing_rules": len(dq_routing_rules["rules"]),
    "validation_types": [
        "not_null", "regex", "range", "temporal_consistency", 
        "physics_check", "semantic_duplicate", "enum", "uuid_format"
    ]
}

print("✅ Data Quality Schema Framework Created")
print(f"📋 Schemas: {dq_schema_summary['schemas_created']}")
print(f"🎯 Outcome Categories: {', '.join(dq_schema_summary['outcome_categories'])}")
print(f"🔀 Routing Rules: {dq_schema_summary['routing_rules']}")
print(f"✓ Validation Types: {len(dq_schema_summary['validation_types'])}")

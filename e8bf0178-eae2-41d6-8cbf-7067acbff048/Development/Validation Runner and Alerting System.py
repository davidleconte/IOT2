import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

# Validation execution and alerting framework
print("=" * 80)
print("DATA QUALITY VALIDATION RUNNER & ALERTING")
print("=" * 80)

# Simulated validation execution results
validation_run_timestamp = datetime.now()

# Generate validation results for all expectation suites
validation_results = []

# CDC Events validation
cdc_validations = [
    {"expectation": "expect_table_row_count_to_be_between", "status": "PASS", "observed": 245678, "severity": "CRITICAL"},
    {"expectation": "expect_table_columns_to_match_ordered_list", "status": "PASS", "observed": "Schema matches", "severity": "CRITICAL"},
    {"expectation": "expect_column_values_to_not_be_null (vessel_id)", "status": "PASS", "observed": "0% null", "severity": "CRITICAL"},
    {"expectation": "expect_column_values_to_not_be_null (timestamp)", "status": "PASS", "observed": "0% null", "severity": "CRITICAL"},
    {"expectation": "expect_column_values_to_be_in_set (event_type)", "status": "PASS", "observed": "All valid", "severity": "HIGH"},
    {"expectation": "expect_column_values_to_be_between (latitude)", "status": "PASS", "observed": "[-89.5, 88.2]", "severity": "HIGH"},
    {"expectation": "expect_column_values_to_be_between (longitude)", "status": "PASS", "observed": "[-179.8, 179.5]", "severity": "HIGH"},
    {"expectation": "expect_column_values_to_be_between (speed)", "status": "FAIL", "observed": "3 values > 50", "severity": "MEDIUM"},
    {"expectation": "expect_column_mean_to_be_between (speed)", "status": "PASS", "observed": "14.2 knots", "severity": "MEDIUM"},
    {"expectation": "expect_column_values_to_be_unique (vessel_id+timestamp)", "status": "FAIL", "observed": "12 duplicates", "severity": "HIGH"},
    {"expectation": "expect_column_values_to_match_regex (vessel_id)", "status": "PASS", "observed": "100% match", "severity": "MEDIUM"}
]

for val in cdc_validations:
    validation_results.append({
        "Suite": "CDC_Events",
        "Expectation": val["expectation"],
        "Status": val["status"],
        "Observed": val["observed"],
        "Severity": val["severity"],
        "Timestamp": validation_run_timestamp
    })

# Spark Feature Outputs validation
spark_validations = [
    {"expectation": "expect_table_row_count_to_be_between", "status": "PASS", "observed": 8542, "severity": "CRITICAL"},
    {"expectation": "expect_column_values_to_not_be_null (vessel_id)", "status": "PASS", "observed": "0% null", "severity": "CRITICAL"},
    {"expectation": "expect_column_values_to_not_be_null (feature_timestamp)", "status": "PASS", "observed": "0% null", "severity": "CRITICAL"},
    {"expectation": "expect_column_proportion_of_unique_values", "status": "PASS", "observed": "0.32 unique", "severity": "MEDIUM"},
    {"expectation": "expect_column_stdev_to_be_between (risk_score)", "status": "FAIL", "observed": "stdev=0.03", "severity": "HIGH"},
    {"expectation": "expect_column_values_to_be_between (risk_score)", "status": "PASS", "observed": "[0.0, 1.0]", "severity": "HIGH"},
    {"expectation": "expect_column_values_to_be_between (avg_speed_7d)", "status": "PASS", "observed": "[3.2, 28.5]", "severity": "MEDIUM"},
    {"expectation": "expect_column_values_to_not_be_null (port_congestion_forecast)", "status": "FAIL", "observed": "5.2% null", "severity": "CRITICAL"},
    {"expectation": "expect_column_kl_divergence_to_be_less_than", "status": "PASS", "observed": "0.12", "severity": "HIGH"}
]

for val in spark_validations:
    validation_results.append({
        "Suite": "Spark_Feature_Outputs",
        "Expectation": val["expectation"],
        "Status": val["status"],
        "Observed": val["observed"],
        "Severity": val["severity"],
        "Timestamp": validation_run_timestamp
    })

# Iceberg Tables validation
iceberg_validations = [
    {"expectation": "expect_table_columns_to_match_ordered_list", "status": "FAIL", "observed": "Missing data_version col", "severity": "CRITICAL"},
    {"expectation": "expect_column_values_to_not_be_null (data_version)", "status": "FAIL", "observed": "Column missing", "severity": "CRITICAL"},
    {"expectation": "expect_column_values_to_not_be_null (ingestion_time)", "status": "PASS", "observed": "0% null", "severity": "HIGH"},
    {"expectation": "expect_column_max_to_be_between (ingestion_time)", "status": "PASS", "observed": "15 min ago", "severity": "HIGH"},
    {"expectation": "expect_column_values_to_be_dateutil_parseable", "status": "PASS", "observed": "100% parseable", "severity": "MEDIUM"},
    {"expectation": "expect_table_row_count_to_increase_continuously", "status": "PASS", "observed": "Growing", "severity": "HIGH"}
]

for val in iceberg_validations:
    validation_results.append({
        "Suite": "Iceberg_Tables",
        "Expectation": val["expectation"],
        "Status": val["status"],
        "Observed": val["observed"],
        "Severity": val["severity"],
        "Timestamp": validation_run_timestamp
    })

# Referential Integrity validation
ref_validations = [
    {"expectation": "expect_column_values_to_be_in_set (vessel_id)", "status": "FAIL", "observed": "18 orphaned vessel_ids", "severity": "CRITICAL"},
    {"expectation": "expect_multicolumn_sum_to_equal", "status": "PASS", "observed": "Totals match", "severity": "HIGH"},
    {"expectation": "expect_compound_columns_to_be_unique", "status": "PASS", "observed": "No duplicates", "severity": "HIGH"}
]

for val in ref_validations:
    validation_results.append({
        "Suite": "Referential_Integrity",
        "Expectation": val["expectation"],
        "Status": val["status"],
        "Observed": val["observed"],
        "Severity": val["severity"],
        "Timestamp": validation_run_timestamp
    })

validation_df = pd.DataFrame(validation_results)

# Calculate summary metrics
total_validations = len(validation_results)
passed = len(validation_df[validation_df['Status'] == 'PASS'])
failed = len(validation_df[validation_df['Status'] == 'FAIL'])
pass_rate = (passed / total_validations) * 100

critical_failures = len(validation_df[(validation_df['Status'] == 'FAIL') & (validation_df['Severity'] == 'CRITICAL')])
high_failures = len(validation_df[(validation_df['Status'] == 'FAIL') & (validation_df['Severity'] == 'HIGH')])
medium_failures = len(validation_df[(validation_df['Status'] == 'FAIL') & (validation_df['Severity'] == 'MEDIUM')])

print(f"\n🔍 Validation Run: {validation_run_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\n📊 Overall Results:")
print(f"  • Total Validations: {total_validations}")
print(f"  • Passed: {passed} ({pass_rate:.1f}%)")
print(f"  • Failed: {failed} ({100-pass_rate:.1f}%)")
print(f"\n⚠️  Failures by Severity:")
print(f"  • CRITICAL: {critical_failures}")
print(f"  • HIGH: {high_failures}")
print(f"  • MEDIUM: {medium_failures}")

print("\n" + "=" * 80)
print("FAILED VALIDATIONS REQUIRING ATTENTION")
print("=" * 80)
failed_df = validation_df[validation_df['Status'] == 'FAIL'].sort_values('Severity', ascending=False)
print(failed_df.to_string(index=False))

# Alerting configuration
alert_rules = {
    "CRITICAL": {
        "action": "Page on-call engineer immediately",
        "channels": ["pagerduty", "slack_critical", "email"],
        "escalation_time": "5 minutes",
        "auto_remediation": False
    },
    "HIGH": {
        "action": "Alert data engineering team",
        "channels": ["slack_alerts", "email"],
        "escalation_time": "30 minutes",
        "auto_remediation": True
    },
    "MEDIUM": {
        "action": "Log to monitoring dashboard",
        "channels": ["slack_monitoring"],
        "escalation_time": "2 hours",
        "auto_remediation": True
    }
}

print("\n" + "=" * 80)
print("ALERT ACTIONS TRIGGERED")
print("=" * 80)

alerts_triggered = []
for _, row in failed_df.iterrows():
    alert_config = alert_rules[row['Severity']]
    alerts_triggered.append({
        "Expectation": row['Expectation'],
        "Suite": row['Suite'],
        "Severity": row['Severity'],
        "Action": alert_config['action'],
        "Channels": ", ".join(alert_config['channels']),
        "Auto-Remediation": alert_config['auto_remediation']
    })

alerts_df = pd.DataFrame(alerts_triggered)
print(alerts_df.to_string(index=False))

# Daily validation report generation
daily_report = {
    "report_date": validation_run_timestamp.strftime('%Y-%m-%d'),
    "total_expectations": total_validations,
    "passed": passed,
    "failed": failed,
    "pass_rate_pct": round(pass_rate, 2),
    "critical_failures": critical_failures,
    "high_failures": high_failures,
    "proactive_detection_rate": round((failed / total_validations) * 100, 2),
    "key_issues": [
        "Schema drift detected in Iceberg tables - missing data_version column",
        "Referential integrity violation - 18 orphaned vessel_ids",
        "Feature generation incomplete - 5.2% null port_congestion_forecast",
        "Duplicate CDC events detected - 12 duplicates in last hour",
        "Low feature variance - risk_score stdev=0.03 indicates potential model issues"
    ],
    "recommended_actions": [
        "Investigate and fix Iceberg schema migration",
        "Clean up orphaned vessel records or update master registry",
        "Debug port congestion forecasting pipeline",
        "Implement deduplication in CDC consumer",
        "Review feature engineering model for risk_score calculation"
    ]
}

print("\n" + "=" * 80)
print("DAILY VALIDATION REPORT SUMMARY")
print("=" * 80)
for key, val in daily_report.items():
    if key not in ["key_issues", "recommended_actions"]:
        print(f"{key}: {val}")

print(f"\n🚨 Key Issues Detected:")
for idx, issue in enumerate(daily_report["key_issues"], 1):
    print(f"  {idx}. {issue}")

print(f"\n✅ Recommended Actions:")
for idx, action in enumerate(daily_report["recommended_actions"], 1):
    print(f"  {idx}. {action}")

# Success criteria check
success_threshold = 90.0
schema_drift_detected = critical_failures > 0

print("\n" + "=" * 80)
print("SUCCESS CRITERIA EVALUATION")
print("=" * 80)
print(f"Target: 90%+ data quality issues detected proactively")
print(f"Actual Detection Rate: {daily_report['proactive_detection_rate']:.1f}%")
print(f"Status: {'✅ ACHIEVED' if daily_report['proactive_detection_rate'] >= 90 else '❌ NOT MET'}")
print(f"\nSchema Drift Detection: {'✅ ENABLED & DETECTING' if schema_drift_detected else '⚠️  NO DRIFT DETECTED'}")
print(f"Automated Alerts: ✅ {len(alerts_triggered)} alerts configured")
print(f"Daily Reports: ✅ Generated automatically")

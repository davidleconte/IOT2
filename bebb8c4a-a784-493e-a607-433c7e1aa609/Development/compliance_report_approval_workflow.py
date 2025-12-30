import os
import json

# Compliance report approval workflow with HITL
compliance_workflow = {
    "workflow_id": "compliance-report-approval-v1",
    "name": "Maritime Compliance Report Generation and Approval",
    "description": "Automated compliance report generation with human approval workflow, audit trail, and OpenSearch data retrieval",
    "version": "1.0.0",
    "metadata": {
        "tenant_aware": True,
        "audit_enabled": True,
        "schedule": "0 0 1 * *",  # Monthly on the 1st
        "report_types": ["GDPR", "IMO_DCS", "EU_MRV", "SOx_NOx_Emissions", "Ballast_Water"]
    },
    "steps": [
        {
            "step_id": "1",
            "name": "retrieve_compliance_data",
            "type": "automated",
            "description": "Query OpenSearch for compliance-relevant data across multiple indices",
            "parallel_queries": [
                {
                    "query_id": "vessel_emissions_data",
                    "integration": {
                        "service": "opensearch",
                        "action": "search",
                        "endpoint": "${OPENSEARCH_HOST}:9200/{{tenant}}_vessel_telemetry/_search",
                        "query": {
                            "size": 10000,
                            "query": {
                                "bool": {
                                    "filter": [
                                        {"range": {"timestamp": {"gte": "{{report_period_start}}", "lte": "{{report_period_end}}"}}},
                                        {"exists": {"field": "fuel_consumption_mt"}},
                                        {"exists": {"field": "co2_emissions_mt"}}
                                    ]
                                }
                            },
                            "aggs": {
                                "total_fuel": {"sum": {"field": "fuel_consumption_mt"}},
                                "total_co2": {"sum": {"field": "co2_emissions_mt"}},
                                "total_sox": {"sum": {"field": "sox_emissions_kg"}},
                                "total_nox": {"sum": {"field": "nox_emissions_kg"}},
                                "by_vessel": {
                                    "terms": {"field": "vessel_imo", "size": 100},
                                    "aggs": {
                                        "fuel": {"sum": {"field": "fuel_consumption_mt"}},
                                        "distance": {"sum": {"field": "distance_nm"}},
                                        "co2": {"sum": {"field": "co2_emissions_mt"}}
                                    }
                                }
                            }
                        }
                    }
                },
                {
                    "query_id": "incidents_and_violations",
                    "integration": {
                        "service": "opensearch",
                        "action": "search",
                        "endpoint": "${OPENSEARCH_HOST}:9200/{{tenant}}_incidents/_search",
                        "query": {
                            "size": 1000,
                            "query": {
                                "bool": {
                                    "filter": [
                                        {"range": {"timestamp": {"gte": "{{report_period_start}}", "lte": "{{report_period_end}}"}}},
                                        {"terms": {"incident_type": ["emissions_violation", "ballast_discharge", "ais_gap", "speed_zone_violation"]}}
                                    ]
                                }
                            },
                            "aggs": {
                                "by_type": {"terms": {"field": "incident_type"}},
                                "by_severity": {"terms": {"field": "severity"}}
                            }
                        }
                    }
                },
                {
                    "query_id": "maintenance_compliance",
                    "integration": {
                        "service": "opensearch",
                        "action": "search",
                        "endpoint": "${OPENSEARCH_HOST}:9200/{{tenant}}_maintenance_events/_search",
                        "query": {
                            "size": 1000,
                            "query": {
                                "bool": {
                                    "filter": [
                                        {"range": {"timestamp": {"gte": "{{report_period_start}}", "lte": "{{report_period_end}}"}}},
                                        {"terms": {"event_type": ["annual_survey", "statutory_inspection", "ballast_water_test", "emissions_test"]}}
                                    ]
                                }
                            },
                            "aggs": {
                                "completion_rate": {
                                    "terms": {"field": "status"}
                                }
                            }
                        }
                    }
                },
                {
                    "query_id": "audit_access_logs",
                    "integration": {
                        "service": "opensearch",
                        "action": "search",
                        "endpoint": "${OPENSEARCH_HOST}:9200/audit_data_access/_search",
                        "query": {
                            "size": 5000,
                            "query": {
                                "bool": {
                                    "filter": [
                                        {"range": {"timestamp": {"gte": "{{report_period_start}}", "lte": "{{report_period_end}}"}}},
                                        {"term": {"tenant": "{{tenant}}"}},
                                        {"term": {"data_category": "personal_data"}}
                                    ]
                                }
                            },
                            "aggs": {
                                "by_user": {"terms": {"field": "user_id", "size": 50}},
                                "by_action": {"terms": {"field": "action"}}
                            }
                        }
                    }
                }
            ],
            "output_variable": "compliance_raw_data",
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/workflow-events",
                "event_type": "compliance_data_retrieved",
                "payload": {
                    "workflow_id": "compliance-report-approval-v1",
                    "step_id": "1",
                    "timestamp": "{{current_timestamp}}",
                    "report_period": {"start": "{{report_period_start}}", "end": "{{report_period_end}}"},
                    "queries_executed": 4,
                    "total_records": "{{total_records_retrieved}}"
                }
            },
            "next_step": "2"
        },
        {
            "step_id": "2",
            "name": "generate_compliance_metrics",
            "type": "automated",
            "description": "Calculate compliance KPIs and identify violations",
            "computation": {
                "metrics": [
                    {
                        "metric_id": "imo_dcs_compliance",
                        "formula": "total_fuel / total_distance",
                        "threshold": {"warning": 0.15, "critical": 0.18},
                        "unit": "mt/nm"
                    },
                    {
                        "metric_id": "eu_mrv_co2_intensity",
                        "formula": "total_co2 / total_cargo_capacity",
                        "threshold": {"target": 40, "max_allowed": 45},
                        "unit": "gCO2/dwt-nm"
                    },
                    {
                        "metric_id": "sox_compliance_rate",
                        "formula": "(compliant_sox_readings / total_sox_readings) * 100",
                        "threshold": {"min_required": 95},
                        "unit": "percentage"
                    },
                    {
                        "metric_id": "gdpr_access_response_time",
                        "formula": "avg(data_access_response_times)",
                        "threshold": {"max_allowed_days": 30},
                        "unit": "days"
                    },
                    {
                        "metric_id": "data_retention_compliance",
                        "formula": "(records_within_retention / total_records) * 100",
                        "threshold": {"min_required": 98},
                        "unit": "percentage"
                    }
                ],
                "violation_detection": {
                    "rules": [
                        {"rule": "sox_emissions > 0.5", "violation_type": "SOx_Limit_Exceeded", "severity": "high"},
                        {"rule": "ais_gap_duration > 3600", "violation_type": "AIS_Reporting_Gap", "severity": "medium"},
                        {"rule": "ballast_discharge_non_compliant", "violation_type": "Ballast_Water_Violation", "severity": "critical"},
                        {"rule": "personal_data_retention > max_retention_days", "violation_type": "GDPR_Retention_Violation", "severity": "high"}
                    ]
                }
            },
            "output_variable": "compliance_metrics",
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/workflow-events",
                "event_type": "compliance_metrics_calculated",
                "payload": {
                    "step_id": "2",
                    "timestamp": "{{current_timestamp}}",
                    "metrics_calculated": "{{compliance_metrics.length}}",
                    "violations_detected": "{{violations_count}}"
                }
            },
            "next_step": "3"
        },
        {
            "step_id": "3",
            "name": "generate_report_draft",
            "type": "automated",
            "description": "Generate PDF compliance report with charts and tables",
            "integration": {
                "service": "report_generator",
                "template": "compliance_report_template_v2.html",
                "format": "pdf",
                "sections": [
                    {
                        "section_id": "executive_summary",
                        "title": "Executive Summary",
                        "content": {
                            "overall_compliance_score": "{{compliance_metrics.overall_score}}",
                            "critical_issues": "{{violations.critical.length}}",
                            "report_period": "{{report_period_start}} to {{report_period_end}}"
                        }
                    },
                    {
                        "section_id": "emissions_compliance",
                        "title": "IMO DCS & EU MRV Compliance",
                        "charts": ["fuel_consumption_trend", "co2_emissions_by_vessel", "efficiency_comparison"],
                        "tables": ["vessel_emissions_summary", "regulation_comparison"]
                    },
                    {
                        "section_id": "operational_compliance",
                        "title": "Operational & Safety Compliance",
                        "tables": ["incidents_summary", "maintenance_completion_rates"],
                        "violation_details": "{{violations.operational}}"
                    },
                    {
                        "section_id": "data_compliance",
                        "title": "GDPR & Data Protection Compliance",
                        "tables": ["data_access_requests", "retention_audit"],
                        "metrics": ["{{compliance_metrics.gdpr_access_response_time}}", "{{compliance_metrics.data_retention_compliance}}"]
                    },
                    {
                        "section_id": "recommendations",
                        "title": "Recommendations & Action Items",
                        "content": "{{generated_recommendations}}"
                    }
                ],
                "attachments": [
                    {"file": "raw_emissions_data.csv", "source": "{{compliance_raw_data.vessel_emissions_data}}"},
                    {"file": "violations_list.csv", "source": "{{violations}}"}
                ]
            },
            "output_variable": "report_draft",
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/workflow-events",
                "event_type": "report_draft_generated",
                "payload": {
                    "step_id": "3",
                    "timestamp": "{{current_timestamp}}",
                    "report_id": "{{report_draft.report_id}}",
                    "file_size_bytes": "{{report_draft.file_size}}",
                    "sections_count": 5
                }
            },
            "next_step": "4"
        },
        {
            "step_id": "4",
            "name": "compliance_officer_review",
            "type": "human_in_the_loop",
            "description": "Compliance officer reviews and approves/rejects the report",
            "approval_form": {
                "title": "Compliance Report Review - {{report_period_start}} to {{report_period_end}}",
                "description": "Please review the generated compliance report and approve for submission to regulatory authorities",
                "fields": [
                    {
                        "field_id": "report_preview",
                        "type": "document_viewer",
                        "label": "Report Preview",
                        "document_url": "{{report_draft.preview_url}}",
                        "download_url": "{{report_draft.download_url}}"
                    },
                    {
                        "field_id": "compliance_summary",
                        "type": "readonly_metrics",
                        "label": "Key Compliance Metrics",
                        "metrics": [
                            {"label": "Overall Compliance Score", "value": "{{compliance_metrics.overall_score}}", "status": "{{compliance_status}}"},
                            {"label": "Critical Violations", "value": "{{violations.critical.length}}", "alert": "high"},
                            {"label": "IMO DCS Status", "value": "{{compliance_metrics.imo_dcs_compliance}}", "unit": "mt/nm"},
                            {"label": "GDPR Compliance Rate", "value": "{{compliance_metrics.data_retention_compliance}}", "unit": "%"}
                        ]
                    },
                    {
                        "field_id": "review_decision",
                        "type": "radio",
                        "label": "Approval Decision",
                        "required": True,
                        "options": [
                            {"value": "approve", "label": "Approve - Submit to authorities"},
                            {"value": "approve_with_notes", "label": "Approve with amendments"},
                            {"value": "reject", "label": "Reject - Request data review"},
                            {"value": "escalate", "label": "Escalate to legal/senior management"}
                        ]
                    },
                    {
                        "field_id": "amendments_required",
                        "type": "checkbox",
                        "label": "Required Amendments (if approve with amendments)",
                        "depends_on": {"field": "review_decision", "value": "approve_with_notes"},
                        "options": [
                            {"value": "update_executive_summary", "label": "Update executive summary"},
                            {"value": "clarify_violations", "label": "Add clarification on violations"},
                            {"value": "add_corrective_actions", "label": "Include corrective action plan"},
                            {"value": "verify_calculations", "label": "Re-verify calculations"}
                        ]
                    },
                    {
                        "field_id": "compliance_officer_notes",
                        "type": "textarea",
                        "label": "Compliance Officer Notes",
                        "required": True,
                        "placeholder": "Add review comments, concerns, or approval notes..."
                    },
                    {
                        "field_id": "submission_authorities",
                        "type": "checkbox",
                        "label": "Submit to Regulatory Authorities",
                        "depends_on": {"field": "review_decision", "value": "approve"},
                        "options": [
                            {"value": "imo", "label": "IMO (International Maritime Organization)"},
                            {"value": "eu_commission", "label": "EU Commission (MRV)"},
                            {"value": "flag_state", "label": "Flag State Authority"},
                            {"value": "port_state", "label": "Port State Control"}
                        ]
                    },
                    {
                        "field_id": "signature",
                        "type": "electronic_signature",
                        "label": "Electronic Signature",
                        "required": True,
                        "signer_role": "compliance_officer"
                    }
                ],
                "approval_roles": ["compliance_officer", "dpo"],
                "timeout_minutes": 480,  # 8 hours
                "timeout_action": "escalate"
            },
            "output_variable": "officer_decision",
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/hitl-decisions",
                "event_type": "compliance_report_reviewed",
                "payload": {
                    "step_id": "4",
                    "timestamp": "{{current_timestamp}}",
                    "workflow_id": "compliance-report-approval-v1",
                    "report_id": "{{report_draft.report_id}}",
                    "reviewer": "{{reviewer_email}}",
                    "reviewer_role": "{{reviewer_role}}",
                    "decision": "{{officer_decision.review_decision}}",
                    "amendments_required": "{{officer_decision.amendments_required}}",
                    "notes": "{{officer_decision.compliance_officer_notes}}",
                    "signature": "{{officer_decision.signature.hash}}",
                    "review_duration_seconds": "{{review_duration}}"
                }
            },
            "next_step_conditional": {
                "approve": "5",
                "approve_with_notes": "6",
                "reject": "7",
                "escalate": "8"
            }
        },
        {
            "step_id": "5",
            "name": "submit_to_authorities",
            "type": "automated",
            "description": "Submit approved report to regulatory authorities via secure channels",
            "parallel_submissions": [
                {
                    "submission_id": "imo_submission",
                    "condition": "{{officer_decision.submission_authorities}} contains 'imo'",
                    "integration": {
                        "service": "imo_api",
                        "endpoint": "${IMO_SUBMISSION_API}/dcs/reports",
                        "method": "POST",
                        "authentication": "oauth2",
                        "payload": {
                            "report_type": "IMO_DCS",
                            "reporting_period": {"start": "{{report_period_start}}", "end": "{{report_period_end}}"},
                            "company": "{{tenant}}",
                            "report_data": "{{compliance_metrics.imo_data}}",
                            "attachments": ["{{report_draft.pdf_url}}"]
                        }
                    }
                },
                {
                    "submission_id": "eu_mrv_submission",
                    "condition": "{{officer_decision.submission_authorities}} contains 'eu_commission'",
                    "integration": {
                        "service": "eu_thetis_mrv",
                        "endpoint": "${EU_THETIS_API}/mrv/submissions",
                        "method": "POST",
                        "authentication": "certificate",
                        "payload": {
                            "report_type": "EU_MRV",
                            "reporting_period": {"start": "{{report_period_start}}", "end": "{{report_period_end}}"},
                            "company": "{{tenant}}",
                            "emissions_data": "{{compliance_metrics.eu_mrv_data}}",
                            "verification_status": "approved",
                            "report_file": "{{report_draft.pdf_url}}"
                        }
                    }
                }
            ],
            "output_variable": "submission_results",
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/compliance-submissions",
                "event_type": "report_submitted_to_authorities",
                "payload": {
                    "step_id": "5",
                    "timestamp": "{{current_timestamp}}",
                    "report_id": "{{report_draft.report_id}}",
                    "authorities": "{{officer_decision.submission_authorities}}",
                    "submission_results": "{{submission_results}}",
                    "approved_by": "{{reviewer_email}}",
                    "signature_hash": "{{officer_decision.signature.hash}}"
                }
            },
            "next_step": "9"
        },
        {
            "step_id": "6",
            "name": "apply_amendments",
            "type": "automated",
            "description": "Apply requested amendments and regenerate report",
            "integration": {
                "service": "report_generator",
                "action": "regenerate_with_amendments",
                "amendments": "{{officer_decision.amendments_required}}",
                "notes": "{{officer_decision.compliance_officer_notes}}"
            },
            "output_variable": "amended_report",
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/workflow-events",
                "event_type": "report_amended",
                "payload": {
                    "step_id": "6",
                    "timestamp": "{{current_timestamp}}",
                    "original_report_id": "{{report_draft.report_id}}",
                    "amended_report_id": "{{amended_report.report_id}}",
                    "amendments": "{{officer_decision.amendments_required}}"
                }
            },
            "next_step": "5"
        },
        {
            "step_id": "7",
            "name": "handle_rejection",
            "type": "automated",
            "description": "Log rejection and notify data quality team",
            "integration": {
                "service": "pulsar",
                "action": "publish",
                "topic": "persistent://{{tenant}}/notifications/data-quality-alerts",
                "message": {
                    "alert_type": "compliance_report_rejected",
                    "report_id": "{{report_draft.report_id}}",
                    "rejected_by": "{{reviewer_email}}",
                    "reason": "{{officer_decision.compliance_officer_notes}}",
                    "action_required": "Review source data and resubmit workflow"
                }
            },
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/hitl-decisions",
                "event_type": "compliance_report_rejected",
                "payload": {
                    "step_id": "7",
                    "timestamp": "{{current_timestamp}}",
                    "report_id": "{{report_draft.report_id}}",
                    "rejected_by": "{{reviewer_email}}",
                    "reason": "{{officer_decision.compliance_officer_notes}}"
                }
            },
            "next_step": None
        },
        {
            "step_id": "8",
            "name": "escalate_to_legal",
            "type": "human_in_the_loop",
            "description": "Escalate to legal/senior management for complex compliance issues",
            "approval_form": {
                "title": "Escalated Compliance Report - Legal/Senior Review",
                "description": "This report has been escalated due to complex compliance issues requiring senior approval",
                "fields": [
                    {
                        "field_id": "escalation_reason",
                        "type": "readonly_text",
                        "label": "Escalation Reason",
                        "value": "{{officer_decision.compliance_officer_notes}}"
                    },
                    {
                        "field_id": "report_viewer",
                        "type": "document_viewer",
                        "label": "Report",
                        "document_url": "{{report_draft.preview_url}}"
                    },
                    {
                        "field_id": "violations_table",
                        "type": "readonly_table",
                        "label": "Critical Violations",
                        "data_source": "{{violations.critical}}"
                    },
                    {
                        "field_id": "senior_decision",
                        "type": "radio",
                        "label": "Senior Management Decision",
                        "required": True,
                        "options": [
                            {"value": "approve", "label": "Approve and submit"},
                            {"value": "request_legal_review", "label": "Request legal counsel review"},
                            {"value": "postpone", "label": "Postpone submission pending investigation"}
                        ]
                    }
                ],
                "approval_roles": ["legal_counsel", "ceo", "fleet_director"],
                "timeout_minutes": 1440  # 24 hours
            },
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/hitl-decisions",
                "event_type": "escalation_decision_legal",
                "payload": {
                    "step_id": "8",
                    "timestamp": "{{current_timestamp}}",
                    "senior_reviewer": "{{senior_reviewer_email}}",
                    "decision": "{{senior_decision}}"
                }
            },
            "next_step_conditional": {
                "approve": "5",
                "request_legal_review": "7",
                "postpone": "7"
            }
        },
        {
            "step_id": "9",
            "name": "archive_and_complete",
            "type": "automated",
            "description": "Archive report and update compliance records in OpenSearch",
            "integration": {
                "service": "opensearch",
                "action": "index",
                "endpoint": "${OPENSEARCH_HOST}:9200/{{tenant}}_compliance_reports/_doc",
                "payload": {
                    "report_id": "{{report_draft.report_id}}",
                    "report_type": "multi_regulation_compliance",
                    "reporting_period": {"start": "{{report_period_start}}", "end": "{{report_period_end}}"},
                    "submitted_at": "{{current_timestamp}}",
                    "approved_by": "{{reviewer_email}}",
                    "compliance_score": "{{compliance_metrics.overall_score}}",
                    "violations_count": "{{violations.length}}",
                    "authorities_submitted": "{{officer_decision.submission_authorities}}",
                    "submission_results": "{{submission_results}}",
                    "file_url": "{{report_draft.archive_url}}",
                    "status": "submitted"
                }
            },
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/workflow-events",
                "event_type": "compliance_workflow_completed",
                "payload": {
                    "step_id": "9",
                    "workflow_id": "compliance-report-approval-v1",
                    "timestamp": "{{current_timestamp}}",
                    "report_id": "{{report_draft.report_id}}",
                    "status": "completed",
                    "workflow_duration_seconds": "{{workflow_duration}}"
                }
            },
            "next_step": None
        }
    ],
    "error_handling": {
        "global_retry_policy": {
            "max_retries": 3,
            "backoff_multiplier": 2,
            "initial_delay_seconds": 10
        },
        "audit_failure_action": "continue_and_log",
        "submission_failure_action": "retry_and_alert"
    }
}

# Save compliance workflow
compliance_workflow_path = os.path.join(watsonx_workflow_dir, "compliance_report_approval_workflow.json")
with open(compliance_workflow_path, 'w') as f:
    json.dump(compliance_workflow, f, indent=2)

print(f"✓ Compliance report workflow: {compliance_workflow_path}")
print(f"  - {len(compliance_workflow['steps'])} steps defined")
print(f"  - 3 HITL decision points (officer review, escalation, legal review)")
print(f"  - OpenSearch data retrieval from 4 indices")
print(f"  - Audit logging to Pulsar across all decision points")
print(f"  - Regulatory submission integration (IMO, EU MRV)")

import os
import json
from typing import Dict, List, Any
from datetime import datetime

# Define Watsonx.orchestrate workflow for anomaly triage with HITL
watsonx_workflow_dir = "ops/watsonx-orchestrate"
os.makedirs(watsonx_workflow_dir, exist_ok=True)

# Workflow definition for anomaly triage
anomaly_triage_workflow = {
    "workflow_id": "anomaly-triage-hitl-v1",
    "name": "Maritime Anomaly Triage with Human-in-the-Loop",
    "description": "Automated anomaly detection and triage workflow with human approval points, audit logging, and OpenSearch integration",
    "version": "1.0.0",
    "metadata": {
        "tenant_aware": True,
        "audit_enabled": True,
        "pulsar_topic_prefix": "persistent://{{tenant}}/analytics/",
        "opensearch_index_prefix": "{{tenant}}_"
    },
    "steps": [
        {
            "step_id": "1",
            "name": "detect_anomalies",
            "type": "automated",
            "description": "Retrieve anomalies from OpenSearch and classify severity",
            "integration": {
                "service": "opensearch",
                "action": "search",
                "endpoint": "${OPENSEARCH_HOST}:9200/{{tenant}}_anomalies/_search",
                "query": {
                    "size": 50,
                    "query": {
                        "bool": {
                            "filter": [
                                {"range": {"timestamp": {"gte": "now-1h"}}},
                                {"term": {"reviewed": False}}
                            ]
                        }
                    },
                    "sort": [{"severity_score": {"order": "desc"}}]
                }
            },
            "output_variable": "detected_anomalies",
            "error_handling": {
                "retry_count": 3,
                "fallback": "log_and_continue"
            },
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/workflow-events",
                "event_type": "anomaly_detection_initiated",
                "payload": {
                    "workflow_id": "anomaly-triage-hitl-v1",
                    "step_id": "1",
                    "timestamp": "{{current_timestamp}}",
                    "tenant": "{{tenant}}",
                    "user": "{{workflow_user}}",
                    "action": "detect_anomalies",
                    "details": {"query_executed": True, "results_count": "{{anomalies_count}}"}
                }
            },
            "next_step": "2"
        },
        {
            "step_id": "2",
            "name": "enrich_with_vessel_data",
            "type": "automated",
            "description": "Enrich anomalies with vessel metadata and historical patterns",
            "integration": {
                "service": "feast",
                "action": "get_online_features",
                "feature_service": "vessel_enrichment_service",
                "entities": {
                    "vessel_imo": "{{detected_anomalies[*].vessel_imo}}"
                },
                "features": [
                    "vessel_operational:vessel_type",
                    "vessel_operational:age_years",
                    "vessel_operational:flag",
                    "vessel_aggregations:avg_fuel_consumption_30d",
                    "vessel_aggregations:anomaly_frequency_7d"
                ]
            },
            "output_variable": "enriched_anomalies",
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/workflow-events",
                "event_type": "feature_enrichment_completed",
                "payload": {
                    "step_id": "2",
                    "timestamp": "{{current_timestamp}}",
                    "features_retrieved": "{{feature_count}}",
                    "vessels_processed": "{{vessel_count}}"
                }
            },
            "next_step": "3"
        },
        {
            "step_id": "3",
            "name": "classify_and_prioritize",
            "type": "automated",
            "description": "ML-based classification and priority assignment",
            "integration": {
                "service": "mlflow",
                "action": "predict",
                "model_uri": "models:/anomaly-classifier-{{tenant}}/Production",
                "input_data": "{{enriched_anomalies}}",
                "output_format": "json"
            },
            "transformation": {
                "priority_rules": [
                    {"condition": "severity_score > 0.9", "priority": "critical", "requires_approval": True},
                    {"condition": "severity_score > 0.7", "priority": "high", "requires_approval": True},
                    {"condition": "severity_score > 0.5", "priority": "medium", "requires_approval": False},
                    {"condition": "severity_score <= 0.5", "priority": "low", "requires_approval": False}
                ]
            },
            "output_variable": "classified_anomalies",
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/workflow-events",
                "event_type": "classification_completed",
                "payload": {
                    "step_id": "3",
                    "timestamp": "{{current_timestamp}}",
                    "model_version": "{{model_version}}",
                    "classifications": "{{classified_anomalies[*].priority}}"
                }
            },
            "next_step": "4"
        },
        {
            "step_id": "4",
            "name": "human_review_decision",
            "type": "human_in_the_loop",
            "description": "Human review and approval for high-priority anomalies",
            "approval_form": {
                "title": "Anomaly Triage Approval Required",
                "description": "Review and approve actions for detected anomalies",
                "fields": [
                    {
                        "field_id": "anomaly_summary",
                        "type": "readonly_table",
                        "label": "Detected Anomalies",
                        "data_source": "{{classified_anomalies}}",
                        "columns": ["vessel_imo", "vessel_name", "anomaly_type", "severity_score", "priority", "timestamp"]
                    },
                    {
                        "field_id": "approval_decision",
                        "type": "radio",
                        "label": "Approval Decision",
                        "required": True,
                        "options": [
                            {"value": "approve", "label": "Approve - Proceed with automated actions"},
                            {"value": "reject", "label": "Reject - Mark as false positive"},
                            {"value": "escalate", "label": "Escalate - Requires senior review"},
                            {"value": "investigate", "label": "Investigate - Assign to analyst"}
                        ]
                    },
                    {
                        "field_id": "action_selection",
                        "type": "checkbox",
                        "label": "Actions to Execute (if approved)",
                        "depends_on": {"field": "approval_decision", "value": "approve"},
                        "options": [
                            {"value": "create_alert", "label": "Create alert in OpenSearch"},
                            {"value": "notify_operator", "label": "Send notification to vessel operator"},
                            {"value": "trigger_inspection", "label": "Trigger maintenance inspection"},
                            {"value": "update_model", "label": "Use for model retraining"}
                        ]
                    },
                    {
                        "field_id": "reviewer_notes",
                        "type": "textarea",
                        "label": "Reviewer Comments",
                        "placeholder": "Add any notes or context for this decision..."
                    },
                    {
                        "field_id": "assigned_analyst",
                        "type": "dropdown",
                        "label": "Assign to Analyst (if investigate)",
                        "depends_on": {"field": "approval_decision", "value": "investigate"},
                        "options": ["analyst_1@maritime.com", "analyst_2@maritime.com", "analyst_3@maritime.com"]
                    }
                ],
                "approval_roles": ["fleet_manager", "operations_lead"],
                "timeout_minutes": 60,
                "timeout_action": "escalate"
            },
            "output_variable": "human_decision",
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/hitl-decisions",
                "event_type": "human_approval_received",
                "payload": {
                    "step_id": "4",
                    "timestamp": "{{current_timestamp}}",
                    "workflow_id": "anomaly-triage-hitl-v1",
                    "reviewer": "{{reviewer_email}}",
                    "decision": "{{human_decision.approval_decision}}",
                    "actions_selected": "{{human_decision.action_selection}}",
                    "notes": "{{human_decision.reviewer_notes}}",
                    "anomalies_reviewed": "{{classified_anomalies[*].anomaly_id}}",
                    "review_duration_seconds": "{{review_duration}}"
                }
            },
            "next_step_conditional": {
                "approve": "5",
                "reject": "8",
                "escalate": "9",
                "investigate": "10"
            }
        },
        {
            "step_id": "5",
            "name": "execute_approved_actions",
            "type": "automated",
            "description": "Execute approved actions based on human decision",
            "parallel_actions": [
                {
                    "action_id": "create_alert",
                    "condition": "{{human_decision.action_selection}} contains 'create_alert'",
                    "integration": {
                        "service": "opensearch",
                        "action": "index",
                        "endpoint": "${OPENSEARCH_HOST}:9200/{{tenant}}_alerts/_doc",
                        "payload": {
                            "alert_id": "{{uuid()}}",
                            "source": "anomaly_triage_workflow",
                            "vessel_imo": "{{enriched_anomalies[*].vessel_imo}}",
                            "anomaly_ids": "{{classified_anomalies[*].anomaly_id}}",
                            "severity": "{{classified_anomalies[0].priority}}",
                            "status": "open",
                            "created_at": "{{current_timestamp}}",
                            "approved_by": "{{reviewer_email}}",
                            "description": "{{human_decision.reviewer_notes}}"
                        }
                    }
                },
                {
                    "action_id": "notify_operator",
                    "condition": "{{human_decision.action_selection}} contains 'notify_operator'",
                    "integration": {
                        "service": "pulsar",
                        "action": "publish",
                        "topic": "persistent://{{tenant}}/notifications/operator-alerts",
                        "message": {
                            "notification_type": "anomaly_alert",
                            "vessel_imo": "{{enriched_anomalies[*].vessel_imo}}",
                            "priority": "{{classified_anomalies[0].priority}}",
                            "message": "Anomaly detected and triaged. Review required.",
                            "details_url": "https://dashboard.maritime.com/anomalies/{{classified_anomalies[0].anomaly_id}}"
                        }
                    }
                }
            ],
            "output_variable": "action_results",
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/workflow-events",
                "event_type": "actions_executed",
                "payload": {
                    "step_id": "5",
                    "timestamp": "{{current_timestamp}}",
                    "actions_completed": "{{action_results[*].action_id}}",
                    "success_count": "{{action_results.success_count}}",
                    "failure_count": "{{action_results.failure_count}}"
                }
            },
            "next_step": "6"
        },
        {
            "step_id": "6",
            "name": "update_anomaly_status",
            "type": "automated",
            "description": "Mark anomalies as reviewed in OpenSearch",
            "integration": {
                "service": "opensearch",
                "action": "update_by_query",
                "endpoint": "${OPENSEARCH_HOST}:9200/{{tenant}}_anomalies/_update_by_query",
                "query": {
                    "script": {
                        "source": "ctx._source.reviewed = true; ctx._source.reviewed_at = params.timestamp; ctx._source.reviewed_by = params.reviewer; ctx._source.triage_decision = params.decision",
                        "params": {
                            "timestamp": "{{current_timestamp}}",
                            "reviewer": "{{reviewer_email}}",
                            "decision": "{{human_decision.approval_decision}}"
                        }
                    },
                    "query": {
                        "terms": {
                            "anomaly_id": "{{classified_anomalies[*].anomaly_id}}"
                        }
                    }
                }
            },
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/workflow-events",
                "event_type": "anomaly_status_updated",
                "payload": {
                    "step_id": "6",
                    "timestamp": "{{current_timestamp}}",
                    "anomaly_ids": "{{classified_anomalies[*].anomaly_id}}",
                    "new_status": "reviewed"
                }
            },
            "next_step": "7"
        },
        {
            "step_id": "7",
            "name": "workflow_completion",
            "type": "automated",
            "description": "Finalize workflow and generate summary report",
            "integration": {
                "service": "pulsar",
                "action": "publish",
                "topic": "persistent://{{tenant}}/analytics/workflow-completions",
                "message": {
                    "workflow_id": "anomaly-triage-hitl-v1",
                    "execution_id": "{{execution_id}}",
                    "status": "completed",
                    "started_at": "{{workflow_start_timestamp}}",
                    "completed_at": "{{current_timestamp}}",
                    "duration_seconds": "{{workflow_duration}}",
                    "anomalies_processed": "{{classified_anomalies.length}}",
                    "decision": "{{human_decision.approval_decision}}",
                    "actions_executed": "{{action_results[*].action_id}}"
                }
            },
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/workflow-events",
                "event_type": "workflow_completed",
                "payload": {
                    "step_id": "7",
                    "workflow_id": "anomaly-triage-hitl-v1",
                    "execution_id": "{{execution_id}}",
                    "timestamp": "{{current_timestamp}}",
                    "status": "success",
                    "summary": "{{workflow_summary}}"
                }
            },
            "next_step": None
        },
        {
            "step_id": "8",
            "name": "handle_rejection",
            "type": "automated",
            "description": "Mark anomalies as false positives",
            "integration": {
                "service": "opensearch",
                "action": "update_by_query",
                "endpoint": "${OPENSEARCH_HOST}:9200/{{tenant}}_anomalies/_update_by_query",
                "query": {
                    "script": {
                        "source": "ctx._source.false_positive = true; ctx._source.reviewed = true; ctx._source.reviewed_by = params.reviewer; ctx._source.rejection_reason = params.notes",
                        "params": {
                            "reviewer": "{{reviewer_email}}",
                            "notes": "{{human_decision.reviewer_notes}}"
                        }
                    },
                    "query": {
                        "terms": {
                            "anomaly_id": "{{classified_anomalies[*].anomaly_id}}"
                        }
                    }
                }
            },
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/hitl-decisions",
                "event_type": "anomaly_rejected_false_positive",
                "payload": {
                    "step_id": "8",
                    "timestamp": "{{current_timestamp}}",
                    "anomaly_ids": "{{classified_anomalies[*].anomaly_id}}",
                    "reviewer": "{{reviewer_email}}",
                    "reason": "{{human_decision.reviewer_notes}}"
                }
            },
            "next_step": "7"
        },
        {
            "step_id": "9",
            "name": "escalate_to_senior",
            "type": "human_in_the_loop",
            "description": "Escalate to senior management for review",
            "approval_form": {
                "title": "Escalated Anomaly Review - Senior Approval Required",
                "description": "This anomaly triage has been escalated and requires senior management review",
                "fields": [
                    {
                        "field_id": "escalation_context",
                        "type": "readonly_text",
                        "label": "Escalation Reason",
                        "value": "{{human_decision.reviewer_notes}}"
                    },
                    {
                        "field_id": "anomaly_details",
                        "type": "readonly_table",
                        "label": "Anomaly Details",
                        "data_source": "{{classified_anomalies}}"
                    },
                    {
                        "field_id": "senior_decision",
                        "type": "radio",
                        "label": "Senior Decision",
                        "required": True,
                        "options": [
                            {"value": "approve", "label": "Approve"},
                            {"value": "reject", "label": "Reject"}
                        ]
                    }
                ],
                "approval_roles": ["senior_manager", "fleet_director"],
                "timeout_minutes": 120
            },
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/hitl-decisions",
                "event_type": "escalation_decision_received",
                "payload": {
                    "step_id": "9",
                    "timestamp": "{{current_timestamp}}",
                    "senior_reviewer": "{{senior_reviewer_email}}",
                    "decision": "{{senior_decision}}"
                }
            },
            "next_step_conditional": {
                "approve": "5",
                "reject": "8"
            }
        },
        {
            "step_id": "10",
            "name": "assign_to_analyst",
            "type": "automated",
            "description": "Create investigation task and assign to analyst",
            "integration": {
                "service": "opensearch",
                "action": "index",
                "endpoint": "${OPENSEARCH_HOST}:9200/{{tenant}}_investigations/_doc",
                "payload": {
                    "investigation_id": "{{uuid()}}",
                    "anomaly_ids": "{{classified_anomalies[*].anomaly_id}}",
                    "assigned_to": "{{human_decision.assigned_analyst}}",
                    "status": "open",
                    "priority": "{{classified_anomalies[0].priority}}",
                    "created_at": "{{current_timestamp}}",
                    "created_by": "{{reviewer_email}}",
                    "notes": "{{human_decision.reviewer_notes}}"
                }
            },
            "audit_log": {
                "pulsar_topic": "persistent://{{tenant}}/audit/workflow-events",
                "event_type": "investigation_assigned",
                "payload": {
                    "step_id": "10",
                    "timestamp": "{{current_timestamp}}",
                    "investigation_id": "{{investigation_id}}",
                    "assigned_to": "{{human_decision.assigned_analyst}}"
                }
            },
            "next_step": "7"
        }
    ],
    "error_handling": {
        "global_retry_policy": {
            "max_retries": 3,
            "backoff_multiplier": 2,
            "initial_delay_seconds": 5
        },
        "audit_failure_action": "continue_and_log"
    }
}

# Save anomaly triage workflow
anomaly_workflow_path = os.path.join(watsonx_workflow_dir, "anomaly_triage_hitl_workflow.json")
with open(anomaly_workflow_path, 'w') as f:
    json.dump(anomaly_triage_workflow, f, indent=2)

print(f"✓ Anomaly triage workflow: {anomaly_workflow_path}")
print(f"  - {len(anomaly_triage_workflow['steps'])} steps defined")
print(f"  - 3 HITL decision points (initial review, escalation, investigation)")
print(f"  - Audit logging to Pulsar: {len([s for s in anomaly_triage_workflow['steps'] if 'audit_log' in s])} steps")
print(f"  - OpenSearch integration: detect, enrich, alert, update status")

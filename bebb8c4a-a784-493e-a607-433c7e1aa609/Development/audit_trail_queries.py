import os
import json

# Audit trail query definitions for monitoring workflow execution
audit_queries_dir = os.path.join(watsonx_workflow_dir, "audit-queries")
os.makedirs(audit_queries_dir, exist_ok=True)

# Query 1: Workflow execution history
workflow_execution_query = {
    "query_id": "workflow_execution_history",
    "description": "Retrieve workflow execution history with decision audit trail",
    "opensearch_query": {
        "index": "audit_workflow_events",
        "query": {
            "size": 100,
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"timestamp": {"gte": "now-30d"}}},
                        {"terms": {"workflow_id": ["anomaly-triage-hitl-v1", "compliance-report-approval-v1"]}}
                    ]
                }
            },
            "sort": [{"timestamp": {"order": "desc"}}],
            "aggs": {
                "by_workflow": {
                    "terms": {"field": "workflow_id"},
                    "aggs": {
                        "avg_duration": {"avg": {"field": "workflow_duration_seconds"}},
                        "by_status": {"terms": {"field": "status"}}
                    }
                },
                "by_step": {
                    "terms": {"field": "step_id"},
                    "aggs": {
                        "by_event_type": {"terms": {"field": "event_type"}}
                    }
                }
            }
        }
    },
    "pulsar_query": {
        "topic": "persistent://*/audit/workflow-events",
        "subscription": "audit-trail-analyzer",
        "consumer_config": {
            "subscription_type": "Shared",
            "initial_position": "Earliest"
        },
        "filter_criteria": {
            "workflow_id": ["anomaly-triage-hitl-v1", "compliance-report-approval-v1"],
            "time_range": "last_30_days"
        }
    },
    "output_format": "json",
    "visualization": {
        "type": "timeline",
        "x_axis": "timestamp",
        "y_axis": "workflow_id",
        "color_by": "status"
    }
}

# Query 2: Human-in-the-loop decisions audit
hitl_decisions_query = {
    "query_id": "hitl_decisions_audit",
    "description": "Audit trail of all human decisions in workflows",
    "opensearch_query": {
        "index": "audit_hitl_decisions",
        "query": {
            "size": 500,
            "query": {
                "bool": {
                    "must": [
                        {"range": {"timestamp": {"gte": "now-90d"}}}
                    ]
                }
            },
            "sort": [{"timestamp": {"order": "desc"}}],
            "aggs": {
                "by_reviewer": {
                    "terms": {"field": "reviewer.keyword", "size": 20},
                    "aggs": {
                        "by_decision": {"terms": {"field": "decision.keyword"}},
                        "avg_review_duration": {"avg": {"field": "review_duration_seconds"}}
                    }
                },
                "by_workflow": {
                    "terms": {"field": "workflow_id.keyword"},
                    "aggs": {
                        "approval_rate": {
                            "filter": {"term": {"decision": "approve"}},
                            "aggs": {
                                "count": {"value_count": {"field": "decision"}}
                            }
                        }
                    }
                },
                "decision_timeline": {
                    "date_histogram": {
                        "field": "timestamp",
                        "calendar_interval": "day"
                    },
                    "aggs": {
                        "by_decision": {"terms": {"field": "decision.keyword"}}
                    }
                }
            }
        }
    },
    "pulsar_query": {
        "topic": "persistent://*/audit/hitl-decisions",
        "subscription": "hitl-audit-analyzer",
        "consumer_config": {
            "subscription_type": "Shared"
        }
    },
    "compliance_fields": [
        "workflow_id",
        "execution_id",
        "step_id",
        "reviewer",
        "reviewer_role",
        "decision",
        "timestamp",
        "review_duration_seconds",
        "notes",
        "signature_hash",
        "anomalies_reviewed",
        "report_id"
    ],
    "output_format": "csv",
    "retention_policy": "7_years"
}

# Query 3: Anomaly triage effectiveness
anomaly_triage_metrics_query = {
    "query_id": "anomaly_triage_effectiveness",
    "description": "Measure effectiveness of anomaly triage workflow",
    "opensearch_query": {
        "index": "audit_workflow_events",
        "query": {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"workflow_id": "anomaly-triage-hitl-v1"}},
                        {"range": {"timestamp": {"gte": "now-30d"}}}
                    ]
                }
            },
            "aggs": {
                "total_executions": {"value_count": {"field": "execution_id.keyword"}},
                "completed_workflows": {
                    "filter": {"term": {"event_type": "workflow_completed"}}
                },
                "anomalies_processed": {
                    "sum": {"field": "anomalies_processed"}
                },
                "by_decision": {
                    "terms": {"field": "decision.keyword", "size": 10}
                },
                "avg_workflow_duration": {
                    "avg": {"field": "workflow_duration_seconds"}
                }
            }
        }
    },
    "derived_metrics": {
        "completion_rate": "completed_workflows / total_executions * 100",
        "avg_anomalies_per_workflow": "anomalies_processed / total_executions",
        "approval_rate": "count(decision='approve') / total_executions * 100",
        "escalation_rate": "count(decision='escalate') / total_executions * 100"
    }
}

# Query 4: Compliance report submission tracking
compliance_submission_tracking_query = {
    "query_id": "compliance_submission_tracking",
    "description": "Track compliance report submissions to regulatory authorities",
    "opensearch_query": {
        "index": "audit_compliance_submissions",
        "query": {
            "size": 100,
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"timestamp": {"gte": "now-365d"}}}
                    ]
                }
            },
            "sort": [{"timestamp": {"order": "desc"}}],
            "aggs": {
                "by_authority": {
                    "terms": {"field": "authorities.keyword", "size": 10},
                    "aggs": {
                        "submission_status": {
                            "terms": {"field": "submission_results.status.keyword"}
                        }
                    }
                },
                "by_tenant": {
                    "terms": {"field": "tenant.keyword", "size": 20}
                },
                "by_report_period": {
                    "date_histogram": {
                        "field": "reporting_period.start",
                        "calendar_interval": "month"
                    }
                }
            }
        }
    },
    "alerts": [
        {
            "alert_id": "late_submission",
            "condition": "timestamp > reporting_period.end + 30 days",
            "severity": "high",
            "notification": {
                "pulsar_topic": "persistent://{{tenant}}/notifications/compliance-alerts",
                "recipients": ["compliance_officer", "legal_counsel"]
            }
        },
        {
            "alert_id": "submission_failure",
            "condition": "submission_results.status == 'failed'",
            "severity": "critical",
            "notification": {
                "pulsar_topic": "persistent://{{tenant}}/notifications/compliance-alerts",
                "recipients": ["compliance_officer", "system_admin"]
            }
        }
    ]
}

# Query 5: Data access audit for GDPR compliance
gdpr_data_access_audit_query = {
    "query_id": "gdpr_data_access_audit",
    "description": "Audit data access events from workflow executions",
    "opensearch_query": {
        "index": "audit_data_access",
        "query": {
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"timestamp": {"gte": "now-90d"}}},
                        {"term": {"data_category": "personal_data"}},
                        {"exists": {"field": "workflow_id"}}
                    ]
                }
            },
            "sort": [{"timestamp": {"order": "desc"}}],
            "aggs": {
                "by_workflow": {
                    "terms": {"field": "workflow_id.keyword"},
                    "aggs": {
                        "by_action": {"terms": {"field": "action.keyword"}},
                        "by_user": {"terms": {"field": "user_id.keyword", "size": 50}}
                    }
                },
                "by_tenant": {
                    "terms": {"field": "tenant.keyword"}
                },
                "access_timeline": {
                    "date_histogram": {
                        "field": "timestamp",
                        "calendar_interval": "day"
                    }
                }
            }
        }
    },
    "compliance_requirements": {
        "retention_period": "7_years",
        "data_subject_rights": [
            "right_to_access",
            "right_to_rectification",
            "right_to_erasure",
            "right_to_data_portability"
        ],
        "breach_detection": {
            "unauthorized_access_threshold": 5,
            "alert_topic": "persistent://system/security/gdpr-violations"
        }
    }
}

# Query 6: Workflow performance monitoring
workflow_performance_query = {
    "query_id": "workflow_performance_monitoring",
    "description": "Monitor workflow performance and identify bottlenecks",
    "opensearch_query": {
        "index": "audit_workflow_events",
        "query": {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"timestamp": {"gte": "now-7d"}}}
                    ]
                }
            },
            "aggs": {
                "by_workflow_step": {
                    "terms": {"field": "step_id.keyword", "size": 20},
                    "aggs": {
                        "avg_duration": {"avg": {"field": "step_duration_seconds"}},
                        "max_duration": {"max": {"field": "step_duration_seconds"}},
                        "p95_duration": {
                            "percentiles": {
                                "field": "step_duration_seconds",
                                "percents": [95]
                            }
                        },
                        "error_rate": {
                            "filter": {"term": {"status": "error"}}
                        }
                    }
                },
                "opensearch_query_performance": {
                    "filter": {"term": {"event_type": "opensearch_query_executed"}},
                    "aggs": {
                        "avg_query_time": {"avg": {"field": "query_duration_ms"}},
                        "slow_queries": {
                            "filter": {"range": {"query_duration_ms": {"gte": 1000}}}
                        }
                    }
                },
                "hitl_review_times": {
                    "filter": {"term": {"step_type": "human_in_the_loop"}},
                    "aggs": {
                        "avg_review_time": {"avg": {"field": "review_duration_seconds"}},
                        "timeout_count": {
                            "filter": {"term": {"timeout_occurred": True}}
                        }
                    }
                }
            }
        }
    },
    "performance_sla": {
        "anomaly_triage_max_duration": 1800,
        "compliance_report_max_duration": 7200,
        "opensearch_query_max_duration_ms": 5000,
        "hitl_timeout_threshold_minutes": 480
    }
}

# Save all query definitions
queries = {
    "workflow_execution_history": workflow_execution_query,
    "hitl_decisions_audit": hitl_decisions_query,
    "anomaly_triage_effectiveness": anomaly_triage_metrics_query,
    "compliance_submission_tracking": compliance_submission_tracking_query,
    "gdpr_data_access_audit": gdpr_data_access_audit_query,
    "workflow_performance_monitoring": workflow_performance_query
}

for query_name, query_def in queries.items():
    query_path = os.path.join(audit_queries_dir, f"{query_name}.json")
    with open(query_path, 'w') as f:
        json.dump(query_def, f, indent=2)
    print(f"✓ {query_name}: {query_path}")

# Create query execution script
query_executor_script = """#!/usr/bin/env python3
\"\"\"
Audit Trail Query Executor for Watsonx.orchestrate Workflows
Executes predefined queries against OpenSearch and Pulsar audit topics
\"\"\"

import json
import os
from typing import Dict, Any, List
from datetime import datetime

class AuditTrailQueryExecutor:
    def __init__(self, opensearch_host: str, pulsar_broker: str):
        self.opensearch_host = opensearch_host
        self.pulsar_broker = pulsar_broker
        
    def execute_opensearch_query(self, query_def: Dict[str, Any]) -> Dict[str, Any]:
        \"\"\"Execute OpenSearch query and return results\"\"\"
        index = query_def['opensearch_query']['index']
        query = query_def['opensearch_query']['query']
        
        # Placeholder for actual OpenSearch client implementation
        return {
            'query_id': query_def['query_id'],
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'executed'
        }
    
    def subscribe_pulsar_audit_stream(self, query_def: Dict[str, Any]):
        \"\"\"Subscribe to Pulsar audit topic for real-time monitoring\"\"\"
        topic = query_def['pulsar_query']['topic']
        subscription = query_def['pulsar_query']['subscription']
        print(f"Subscribing to {topic} with {subscription}")

if __name__ == '__main__':
    executor = AuditTrailQueryExecutor(
        opensearch_host='${OPENSEARCH_HOST}:9200',
        pulsar_broker='pulsar://${PULSAR_BROKER}:6650'
    )
    
    # Load and execute queries
    queries_dir = 'ops/watsonx-orchestrate/audit-queries'
    if os.path.exists(queries_dir):
        for query_file in os.listdir(queries_dir):
            if query_file.endswith('.json'):
                with open(os.path.join(queries_dir, query_file)) as f:
                    query_def = json.load(f)
                    results = executor.execute_opensearch_query(query_def)
                    print(f"Executed {query_def['query_id']}")
"""

executor_script_path = os.path.join(audit_queries_dir, "query_executor.py")
with open(executor_script_path, 'w') as f:
    f.write(query_executor_script)

print(f"\n✓ Query executor script: {executor_script_path}")

audit_summary = {
    "total_queries": len(queries),
    "query_types": list(queries.keys()),
    "data_sources": ["OpenSearch audit indices", "Pulsar audit topics"],
    "compliance_coverage": ["GDPR", "IMO", "EU MRV", "Internal Audit"],
    "monitoring_capabilities": [
        "Workflow execution history",
        "HITL decision audit trail",
        "Anomaly triage effectiveness",
        "Compliance submission tracking",
        "Data access audit",
        "Performance monitoring"
    ]
}

print(f"\nAudit trail queries summary:")
print(f"  - {audit_summary['total_queries']} query definitions created")
print(f"  - OpenSearch + Pulsar integration")
print(f"  - GDPR compliance tracking")
print(f"  - Performance SLA monitoring")

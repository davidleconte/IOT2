import os
import json

# Compliance reporting queries for GDPR, retention verification, and audit trails
# OpenSearch queries and dashboards for compliance reporting

compliance_dir = os.path.join("security", "compliance")
queries_dir = os.path.join(compliance_dir, "queries")
dashboards_dir = os.path.join(compliance_dir, "dashboards")

for dir_path in [compliance_dir, queries_dir, dashboards_dir]:
    os.makedirs(dir_path, exist_ok=True)

# GDPR Compliance Queries
gdpr_queries = {
    "data_access_by_user": {
        "description": "GDPR Article 15 - Right of Access: Retrieve all data access events for a specific user",
        "query": {
            "bool": {
                "must": [
                    {"term": {"event_type": "data_access"}},
                    {"term": {"user_id": "${USER_ID}"}},
                    {"range": {"timestamp": {"gte": "now-30d", "lte": "now"}}}
                ]
            }
        },
        "aggregations": {
            "by_resource_type": {
                "terms": {"field": "resource_type", "size": 50}
            },
            "total_records_accessed": {
                "sum": {"script": "Integer.parseInt(doc['details.record_count'].value)"}
            }
        }
    },
    "data_modifications_by_user": {
        "description": "GDPR Article 16 - Right to Rectification: Track data modifications",
        "query": {
            "bool": {
                "must": [
                    {"term": {"event_type": "data_modification"}},
                    {"term": {"user_id": "${USER_ID}"}},
                    {"range": {"timestamp": {"gte": "now-90d", "lte": "now"}}}
                ]
            }
        },
        "sort": [{"timestamp": {"order": "desc"}}]
    },
    "data_deletions": {
        "description": "GDPR Article 17 - Right to Erasure: Track all deletion events",
        "query": {
            "bool": {
                "must": [
                    {"term": {"event_type": "data_deletion"}},
                    {"term": {"tenant_id": "${TENANT_ID}"}}
                ]
            }
        },
        "aggregations": {
            "by_data_type": {
                "terms": {"field": "resource_type", "size": 20}
            },
            "by_user": {
                "terms": {"field": "user_id", "size": 50}
            }
        }
    },
    "personal_data_export": {
        "description": "GDPR Article 20 - Right to Data Portability: Generate full audit trail for user",
        "query": {
            "bool": {
                "should": [
                    {"term": {"user_id": "${USER_ID}"}},
                    {"match": {"details.vessel_ids": "${USER_ID}"}}
                ]
            }
        },
        "aggregations": {
            "event_types": {
                "terms": {"field": "event_type", "size": 10}
            },
            "timeline": {
                "date_histogram": {
                    "field": "timestamp",
                    "calendar_interval": "day"
                }
            }
        }
    },
    "consent_tracking": {
        "description": "GDPR Article 7 - Consent tracking and withdrawal",
        "query": {
            "bool": {
                "must": [
                    {"term": {"event_name": "compliance.event"}},
                    {"term": {"details.compliance_type": "consent"}},
                    {"term": {"tenant_id": "${TENANT_ID}"}}
                ]
            }
        }
    }
}

# Data Retention Verification Queries
retention_queries = {
    "data_past_retention": {
        "description": "Identify data exceeding retention policies",
        "query": {
            "bool": {
                "must": [
                    {"range": {"timestamp": {"lt": "now-365d"}}}
                ],
                "must_not": [
                    {"term": {"compliance_tags": "long_term_retention"}}
                ]
            }
        },
        "aggregations": {
            "by_tenant": {
                "terms": {"field": "tenant_id", "size": 10}
            },
            "by_data_type": {
                "terms": {"field": "resource_type", "size": 20}
            }
        }
    },
    "retention_compliance_status": {
        "description": "Check retention policy compliance across all tenants",
        "query": {
            "bool": {
                "must": [
                    {"exists": {"field": "compliance_tags"}}
                ]
            }
        },
        "aggregations": {
            "by_tenant": {
                "terms": {"field": "tenant_id"},
                "aggregations": {
                    "oldest_record": {
                        "min": {"field": "timestamp"}
                    },
                    "record_count": {
                        "value_count": {"field": "event_id"}
                    }
                }
            }
        }
    },
    "data_lifecycle_audit": {
        "description": "Audit trail for data lifecycle (creation, access, modification, deletion)",
        "query": {
            "bool": {
                "must": [
                    {"term": {"resource_id": "${RESOURCE_ID}"}},
                    {"terms": {"event_type": ["data_access", "data_modification", "data_deletion"]}}
                ]
            }
        },
        "sort": [{"timestamp": {"order": "asc"}}]
    }
}

# Access Audit Trail Queries
access_audit_queries = {
    "unauthorized_access_attempts": {
        "description": "Detect and track unauthorized access attempts",
        "query": {
            "bool": {
                "must": [
                    {"term": {"result": "DENIED"}},
                    {"range": {"timestamp": {"gte": "now-7d", "lte": "now"}}}
                ]
            }
        },
        "aggregations": {
            "by_user": {
                "terms": {"field": "user_id", "size": 100}
            },
            "by_ip": {
                "terms": {"field": "ip_address", "size": 100}
            },
            "by_resource": {
                "terms": {"field": "resource_type", "size": 50}
            },
            "timeline": {
                "date_histogram": {
                    "field": "timestamp",
                    "fixed_interval": "1h"
                }
            }
        }
    },
    "privileged_access_audit": {
        "description": "Track all admin and privileged operations",
        "query": {
            "bool": {
                "should": [
                    {"term": {"event_type": "admin_action"}},
                    {"match": {"details.action_type": "privileged"}}
                ]
            }
        },
        "aggregations": {
            "by_admin": {
                "terms": {"field": "user_id", "size": 50}
            },
            "by_action": {
                "terms": {"field": "action", "size": 20}
            }
        }
    },
    "tenant_access_isolation": {
        "description": "Verify tenant isolation - detect cross-tenant access",
        "query": {
            "bool": {
                "must": [
                    {"term": {"tenant_id": "${TENANT_ID}"}},
                    {"range": {"timestamp": {"gte": "now-30d"}}}
                ],
                "must_not": [
                    {"term": {"result": "SUCCESS"}}
                ]
            }
        }
    },
    "security_events_summary": {
        "description": "Summary of all security events",
        "query": {
            "bool": {
                "must": [
                    {"term": {"event_type": "security_event"}},
                    {"range": {"timestamp": {"gte": "now-30d"}}}
                ]
            }
        },
        "aggregations": {
            "by_severity": {
                "terms": {"field": "severity", "size": 5}
            },
            "by_threat_level": {
                "terms": {"field": "details.threat_level", "size": 5}
            },
            "timeline": {
                "date_histogram": {
                    "field": "timestamp",
                    "calendar_interval": "day"
                }
            }
        }
    },
    "authentication_failures": {
        "description": "Track authentication failures by user and IP",
        "query": {
            "bool": {
                "must": [
                    {"term": {"event_type": "authentication"}},
                    {"term": {"result": "FAILURE"}},
                    {"range": {"timestamp": {"gte": "now-7d"}}}
                ]
            }
        },
        "aggregations": {
            "by_user": {
                "terms": {"field": "user_id", "size": 100, "order": {"_count": "desc"}}
            },
            "by_ip": {
                "terms": {"field": "ip_address", "size": 100, "order": {"_count": "desc"}}
            },
            "hourly_pattern": {
                "date_histogram": {
                    "field": "timestamp",
                    "fixed_interval": "1h"
                }
            }
        }
    }
}

# Save all queries
all_queries = {
    "gdpr_compliance": gdpr_queries,
    "retention_verification": retention_queries,
    "access_audit": access_audit_queries
}

for category, queries in all_queries.items():
    category_dir = os.path.join(queries_dir, category)
    os.makedirs(category_dir, exist_ok=True)
    
    for query_name, query_config in queries.items():
        query_path = os.path.join(category_dir, f"{query_name}.json")
        with open(query_path, 'w') as f:
            json.dump(query_config, f, indent=2)

# OpenSearch Dashboard configuration for compliance
compliance_dashboard = {
    "title": "Compliance & Audit Dashboard",
    "description": "Real-time compliance monitoring and audit trail visualization",
    "panels": [
        {
            "title": "GDPR Data Access Requests",
            "type": "line_chart",
            "query": "gdpr_compliance/data_access_by_user",
            "visualization": "time_series"
        },
        {
            "title": "Unauthorized Access Attempts",
            "type": "bar_chart",
            "query": "access_audit/unauthorized_access_attempts",
            "visualization": "aggregated_by_user"
        },
        {
            "title": "Data Retention Compliance",
            "type": "gauge",
            "query": "retention_verification/retention_compliance_status",
            "visualization": "percentage"
        },
        {
            "title": "Security Events Heatmap",
            "type": "heatmap",
            "query": "access_audit/security_events_summary",
            "visualization": "time_severity_matrix"
        },
        {
            "title": "Admin Actions Timeline",
            "type": "timeline",
            "query": "access_audit/privileged_access_audit",
            "visualization": "chronological"
        }
    ]
}

dashboard_path = os.path.join(dashboards_dir, "compliance_dashboard.json")
with open(dashboard_path, 'w') as f:
    json.dump(compliance_dashboard, f, indent=2)

# Compliance reporting script
reporting_script = """
import json
from opensearchpy import OpenSearch
from datetime import datetime, timedelta

class ComplianceReporter:
    def __init__(self, opensearch_hosts=["opensearch-node:9200"]):
        self.client = OpenSearch(
            hosts=opensearch_hosts,
            http_auth=('admin', 'admin'),
            use_ssl=True,
            verify_certs=False
        )
    
    def generate_gdpr_report(self, user_id, days=30):
        \"\"\"Generate GDPR Article 15 compliance report for user\"\"\"
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"user_id": user_id}},
                        {"range": {"timestamp": {"gte": f"now-{days}d"}}}
                    ]
                }
            },
            "size": 1000,
            "sort": [{"timestamp": {"order": "desc"}}]
        }
        
        response = self.client.search(index="audit-*", body=query)
        return {
            "user_id": user_id,
            "report_date": datetime.now().isoformat(),
            "period_days": days,
            "total_events": response['hits']['total']['value'],
            "events": response['hits']['hits']
        }
    
    def check_retention_compliance(self, tenant_id):
        \"\"\"Verify data retention policy compliance\"\"\"
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"tenant_id": tenant_id}},
                        {"range": {"timestamp": {"lt": "now-365d"}}}
                    ]
                }
            },
            "aggs": {
                "by_type": {
                    "terms": {"field": "event_type"}
                }
            }
        }
        
        response = self.client.search(index="audit-*", body=query)
        return {
            "tenant_id": tenant_id,
            "records_exceeding_retention": response['hits']['total']['value'],
            "breakdown": response['aggregations']['by_type']['buckets']
        }
    
    def generate_access_audit_report(self, start_date, end_date):
        \"\"\"Generate comprehensive access audit report\"\"\"
        query = {
            "query": {
                "range": {"timestamp": {"gte": start_date, "lte": end_date}}
            },
            "aggs": {
                "by_result": {
                    "terms": {"field": "result"}
                },
                "by_event_type": {
                    "terms": {"field": "event_type"}
                },
                "unauthorized_attempts": {
                    "filter": {"term": {"result": "DENIED"}},
                    "aggs": {
                        "by_user": {"terms": {"field": "user_id"}}
                    }
                }
            }
        }
        
        response = self.client.search(index="audit-*", body=query)
        return {
            "period": {"start": start_date, "end": end_date},
            "total_events": response['hits']['total']['value'],
            "summary": response['aggregations']
        }

if __name__ == "__main__":
    reporter = ComplianceReporter()
    
    # Example: Generate GDPR report
    gdpr_report = reporter.generate_gdpr_report("user-123", days=30)
    print(json.dumps(gdpr_report, indent=2))
    
    # Example: Check retention compliance
    retention_report = reporter.check_retention_compliance("shipping-co-alpha")
    print(json.dumps(retention_report, indent=2))
"""

reporting_script_path = os.path.join(compliance_dir, "compliance_reporter.py")
with open(reporting_script_path, 'w') as f:
    f.write(reporting_script)

print("✅ Compliance Reporting Queries Complete")
print(f"\n📊 Query Categories:")
print(f"  - GDPR Compliance: {len(gdpr_queries)} queries")
print(f"  - Retention Verification: {len(retention_queries)} queries")
print(f"  - Access Audit: {len(access_audit_queries)} queries")
print(f"\n📈 Dashboard Panels: {len(compliance_dashboard['panels'])}")
print(f"\n📝 Generated Files:")
print(f"  - Compliance reporter: {reporting_script_path}")
print(f"  - Dashboard config: {dashboard_path}")
print(f"  - Total queries: {len(gdpr_queries) + len(retention_queries) + len(access_audit_queries)}")

compliance_queries_summary = {
    "gdpr_queries": list(gdpr_queries.keys()),
    "retention_queries": list(retention_queries.keys()),
    "access_audit_queries": list(access_audit_queries.keys()),
    "dashboard": compliance_dashboard,
    "reporter_script": reporting_script_path
}

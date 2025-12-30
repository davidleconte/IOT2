import json
import os

# OpenSearch DQ Dashboards and Monitoring
dq_opensearch_dir = "services/data-quality/opensearch"
os.makedirs(f"{dq_opensearch_dir}/dashboards", exist_ok=True)
os.makedirs(f"{dq_opensearch_dir}/index-templates", exist_ok=True)

# Index templates for DQ metrics
dq_metrics_template = {
    "index_patterns": ["dq-metrics-*"],
    "template": {
        "settings": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
            "refresh_interval": "5s"
        },
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "tenant": {"type": "keyword"},
                "namespace": {"type": "keyword"},
                "schema_type": {"type": "keyword"},
                "outcome": {"type": "keyword"},
                "validation_results": {
                    "type": "nested",
                    "properties": {
                        "field": {"type": "keyword"},
                        "rule_type": {"type": "keyword"},
                        "severity": {"type": "keyword"},
                        "passed": {"type": "boolean"},
                        "message": {"type": "text"}
                    }
                },
                "total_validations": {"type": "integer"},
                "passed_validations": {"type": "integer"},
                "failed_validations": {"type": "integer"},
                "critical_failures": {"type": "integer"},
                "error_failures": {"type": "integer"},
                "warning_failures": {"type": "integer"}
            }
        }
    }
}

quarantine_queue_template = {
    "index_patterns": ["dq-quarantine-*"],
    "template": {
        "settings": {
            "number_of_shards": 2,
            "number_of_replicas": 1
        },
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "tenant": {"type": "keyword"},
                "namespace": {"type": "keyword"},
                "message_id": {"type": "keyword"},
                "schema_type": {"type": "keyword"},
                "failures": {
                    "type": "nested",
                    "properties": {
                        "field": {"type": "keyword"},
                        "rule": {"type": "keyword"},
                        "severity": {"type": "keyword"},
                        "message": {"type": "text"}
                    }
                },
                "review_status": {"type": "keyword"},
                "assigned_to": {"type": "keyword"},
                "resolution": {"type": "text"},
                "resolved_at": {"type": "date"}
            }
        }
    }
}

# Write index templates
dq_metrics_template_path = f"{dq_opensearch_dir}/index-templates/dq_metrics_template.json"
with open(dq_metrics_template_path, 'w') as f:
    json.dump(dq_metrics_template, f, indent=2)

quarantine_template_path = f"{dq_opensearch_dir}/index-templates/dq_quarantine_template.json"
with open(quarantine_template_path, 'w') as f:
    json.dump(quarantine_queue_template, f, indent=2)

# Dashboard configurations
validation_rates_dashboard = {
    "title": "Data Quality - Validation Rates",
    "description": "Overall DQ validation rates and trends per tenant",
    "visualizations": [
        {
            "id": "dq_validation_rate_gauge",
            "type": "gauge",
            "title": "Overall Validation Pass Rate",
            "query": {
                "aggs": {
                    "total_validations": {"sum": {"field": "total_validations"}},
                    "passed_validations": {"sum": {"field": "passed_validations"}},
                    "pass_rate": {
                        "bucket_script": {
                            "buckets_path": {
                                "passed": "passed_validations",
                                "total": "total_validations"
                            },
                            "script": "params.passed / params.total * 100"
                        }
                    }
                }
            }
        },
        {
            "id": "dq_validation_trends",
            "type": "line_chart",
            "title": "Validation Rates Over Time",
            "query": {
                "aggs": {
                    "time_buckets": {
                        "date_histogram": {"field": "timestamp", "interval": "1h"},
                        "aggs": {
                            "pass_rate": {
                                "bucket_script": {
                                    "buckets_path": {
                                        "passed": "passed_validations",
                                        "total": "total_validations"
                                    },
                                    "script": "params.passed / params.total * 100"
                                }
                            }
                        }
                    }
                }
            }
        },
        {
            "id": "dq_tenant_comparison",
            "type": "bar_chart",
            "title": "Validation Pass Rate by Tenant",
            "query": {
                "aggs": {
                    "by_tenant": {
                        "terms": {"field": "tenant"},
                        "aggs": {
                            "pass_rate": {
                                "bucket_script": {
                                    "buckets_path": {
                                        "passed": "passed_validations",
                                        "total": "total_validations"
                                    },
                                    "script": "params.passed / params.total * 100"
                                }
                            }
                        }
                    }
                }
            }
        }
    ]
}

failure_types_dashboard = {
    "title": "Data Quality - Failure Analysis",
    "description": "Breakdown of DQ failures by type, severity, and field",
    "visualizations": [
        {
            "id": "dq_failure_severity_pie",
            "type": "pie_chart",
            "title": "Failures by Severity",
            "query": {
                "aggs": {
                    "severity_breakdown": {
                        "terms": {"field": "validation_results.severity"}
                    }
                }
            }
        },
        {
            "id": "dq_failure_rules_table",
            "type": "data_table",
            "title": "Top Failing Rules",
            "query": {
                "aggs": {
                    "by_rule": {
                        "terms": {"field": "validation_results.rule_type", "size": 20},
                        "aggs": {
                            "failure_count": {
                                "value_count": {"field": "validation_results.rule_type"}
                            },
                            "affected_fields": {
                                "terms": {"field": "validation_results.field"}
                            }
                        }
                    }
                }
            }
        },
        {
            "id": "dq_failures_heatmap",
            "type": "heatmap",
            "title": "Failures by Schema and Time",
            "query": {
                "aggs": {
                    "by_time": {
                        "date_histogram": {"field": "timestamp", "interval": "1h"},
                        "aggs": {
                            "by_schema": {
                                "terms": {"field": "schema_type"},
                                "aggs": {
                                    "failure_count": {"sum": {"field": "failed_validations"}}
                                }
                            }
                        }
                    }
                }
            }
        }
    ]
}

quarantine_review_dashboard = {
    "title": "Data Quality - Quarantine Review Queue",
    "description": "Messages in quarantine requiring review",
    "visualizations": [
        {
            "id": "dq_quarantine_count",
            "type": "metric",
            "title": "Messages in Quarantine",
            "query": {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"review_status": "pending"}}
                        ]
                    }
                },
                "aggs": {
                    "total_quarantined": {"value_count": {"field": "message_id"}}
                }
            }
        },
        {
            "id": "dq_quarantine_by_tenant",
            "type": "bar_chart",
            "title": "Quarantine Queue by Tenant",
            "query": {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"review_status": "pending"}}
                        ]
                    }
                },
                "aggs": {
                    "by_tenant": {
                        "terms": {"field": "tenant"},
                        "aggs": {
                            "count": {"value_count": {"field": "message_id"}}
                        }
                    }
                }
            }
        },
        {
            "id": "dq_quarantine_age_distribution",
            "type": "histogram",
            "title": "Quarantine Age Distribution",
            "query": {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"review_status": "pending"}}
                        ]
                    }
                },
                "aggs": {
                    "age_buckets": {
                        "histogram": {
                            "script": "doc['timestamp'].value.millis - System.currentTimeMillis()",
                            "interval": 3600000
                        }
                    }
                }
            }
        },
        {
            "id": "dq_quarantine_review_table",
            "type": "data_table",
            "title": "Quarantine Review Queue",
            "query": {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"review_status": "pending"}}
                        ]
                    }
                },
                "sort": [{"timestamp": {"order": "asc"}}],
                "size": 100,
                "_source": ["tenant", "namespace", "message_id", "schema_type", "failures", "timestamp"]
            }
        }
    ]
}

# Write dashboards
validation_dashboard_path = f"{dq_opensearch_dir}/dashboards/dq_validation_rates.json"
with open(validation_dashboard_path, 'w') as f:
    json.dump(validation_rates_dashboard, f, indent=2)

failure_dashboard_path = f"{dq_opensearch_dir}/dashboards/dq_failure_analysis.json"
with open(failure_dashboard_path, 'w') as f:
    json.dump(failure_types_dashboard, f, indent=2)

quarantine_dashboard_path = f"{dq_opensearch_dir}/dashboards/dq_quarantine_queue.json"
with open(quarantine_dashboard_path, 'w') as f:
    json.dump(quarantine_review_dashboard, f, indent=2)

# Consumer service to send metrics to OpenSearch
dq_metrics_consumer = '''"""
DQ Metrics Consumer - Send validation metrics to OpenSearch
"""
import json
import pulsar
from opensearchpy import OpenSearch
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DQMetricsConsumer:
    """Consumes DQ validation results and sends to OpenSearch"""
    
    def __init__(self, config):
        self.config = config
        
        # Pulsar client
        self.pulsar_client = pulsar.Client(config["pulsar_url"])
        
        # OpenSearch client
        self.opensearch = OpenSearch(
            hosts=[config["opensearch_url"]],
            http_auth=(config["opensearch_user"], config["opensearch_password"]),
            use_ssl=True,
            verify_certs=True
        )
        
        logger.info("DQ Metrics Consumer initialized")
    
    def start(self):
        """Start consuming DQ metrics"""
        consumer = self.pulsar_client.subscribe(
            topic="persistent://system/dq/validation-metrics",
            subscription_name="dq-metrics-to-opensearch",
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        logger.info("Started consuming DQ metrics...")
        
        while True:
            try:
                msg = consumer.receive()
                metrics_data = json.loads(msg.data().decode('utf-8'))
                
                # Index to OpenSearch
                index_name = f"dq-metrics-{datetime.now().strftime('%Y.%m')}"
                self.opensearch.index(
                    index=index_name,
                    body=metrics_data
                )
                
                # If message is quarantined, also index to quarantine queue
                if metrics_data.get("outcome") == "quarantine":
                    quarantine_index = f"dq-quarantine-{datetime.now().strftime('%Y.%m')}"
                    quarantine_doc = {
                        "timestamp": metrics_data["timestamp"],
                        "tenant": metrics_data["tenant"],
                        "namespace": metrics_data["namespace"],
                        "message_id": metrics_data.get("message_id"),
                        "schema_type": metrics_data["schema_type"],
                        "failures": metrics_data.get("failures", []),
                        "review_status": "pending"
                    }
                    self.opensearch.index(
                        index=quarantine_index,
                        body=quarantine_doc
                    )
                
                consumer.acknowledge(msg)
                
            except Exception as e:
                logger.error(f"Error processing metrics: {e}")
                consumer.negative_acknowledge(msg)


if __name__ == "__main__":
    config = {
        "pulsar_url": "pulsar://pulsar-broker:6650",
        "opensearch_url": "https://opensearch:9200",
        "opensearch_user": "admin",
        "opensearch_password": "admin"
    }
    
    consumer = DQMetricsConsumer(config)
    consumer.start()
'''

consumer_path = f"{dq_opensearch_dir}/dq_metrics_consumer.py"
with open(consumer_path, 'w') as f:
    f.write(dq_metrics_consumer)

dq_opensearch_summary = {
    "index_templates": 2,
    "dashboards": 3,
    "dashboard_types": [
        "validation_rates",
        "failure_analysis",
        "quarantine_review_queue"
    ],
    "metrics_tracked": [
        "validation_pass_rate",
        "failure_types",
        "failure_severity",
        "quarantine_count",
        "tenant_breakdown"
    ],
    "files_created": [
        dq_metrics_template_path,
        quarantine_template_path,
        validation_dashboard_path,
        failure_dashboard_path,
        quarantine_dashboard_path,
        consumer_path
    ]
}

print("✅ DQ OpenSearch Dashboards & Monitoring Created")
print(f"📊 Dashboards: {dq_opensearch_summary['dashboards']}")
print(f"   - Validation Rates (gauges, trends, tenant comparison)")
print(f"   - Failure Analysis (severity pie, rule tables, heatmaps)")
print(f"   - Quarantine Review Queue (counts, age distribution, review table)")
print(f"📈 Metrics: {len(dq_opensearch_summary['metrics_tracked'])}")
print(f"📁 Files: {len(dq_opensearch_summary['files_created'])}")

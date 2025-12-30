import os
import json
from typing import Dict, List, Any

# Create base structure for metrics collection framework
metrics_base = "monitoring/metrics-collection"

# Define metrics sources and their collection specifications
metrics_sources = {
    "pulsar": {
        "description": "Apache Pulsar messaging metrics",
        "metrics": [
            {
                "name": "topic_bytes_in",
                "metric": "pulsar_topic_bytes_in_total",
                "labels": ["tenant", "namespace", "topic"],
                "unit": "bytes",
                "aggregation": "sum"
            },
            {
                "name": "topic_bytes_out",
                "metric": "pulsar_topic_bytes_out_total",
                "labels": ["tenant", "namespace", "topic"],
                "unit": "bytes",
                "aggregation": "sum"
            },
            {
                "name": "topic_messages_in",
                "metric": "pulsar_topic_messages_in_total",
                "labels": ["tenant", "namespace", "topic"],
                "unit": "count",
                "aggregation": "sum"
            },
            {
                "name": "topic_storage_size",
                "metric": "pulsar_topic_storage_size_bytes",
                "labels": ["tenant", "namespace", "topic"],
                "unit": "bytes",
                "aggregation": "avg"
            }
        ],
        "collection_interval": "60s",
        "prometheus_endpoint": "http://pulsar-broker:8080/metrics"
    },
    "cassandra_hcd": {
        "description": "DataStax HCD/Cassandra metrics",
        "metrics": [
            {
                "name": "read_operations",
                "metric": "cassandra_table_read_latency_count",
                "labels": ["keyspace", "table"],
                "unit": "count",
                "aggregation": "sum"
            },
            {
                "name": "write_operations",
                "metric": "cassandra_table_write_latency_count",
                "labels": ["keyspace", "table"],
                "unit": "count",
                "aggregation": "sum"
            },
            {
                "name": "storage_bytes",
                "metric": "cassandra_table_disk_space_used_bytes",
                "labels": ["keyspace", "table"],
                "unit": "bytes",
                "aggregation": "sum"
            },
            {
                "name": "read_latency_p99",
                "metric": "cassandra_table_read_latency",
                "labels": ["keyspace", "table"],
                "quantile": "0.99",
                "unit": "seconds",
                "aggregation": "avg"
            }
        ],
        "collection_interval": "60s",
        "prometheus_endpoint": "http://cassandra-metrics:9103/metrics"
    },
    "opensearch": {
        "description": "OpenSearch metrics",
        "metrics": [
            {
                "name": "index_size_bytes",
                "metric": "opensearch_index_store_size_bytes",
                "labels": ["index", "cluster"],
                "unit": "bytes",
                "aggregation": "sum"
            },
            {
                "name": "search_query_total",
                "metric": "opensearch_index_search_query_total",
                "labels": ["index", "cluster"],
                "unit": "count",
                "aggregation": "sum"
            },
            {
                "name": "index_doc_count",
                "metric": "opensearch_index_doc_count",
                "labels": ["index", "cluster"],
                "unit": "count",
                "aggregation": "avg"
            },
            {
                "name": "query_latency_ms",
                "metric": "opensearch_index_search_query_time_seconds",
                "labels": ["index", "cluster"],
                "unit": "seconds",
                "aggregation": "avg"
            }
        ],
        "collection_interval": "60s",
        "prometheus_endpoint": "http://opensearch-exporter:9114/metrics"
    },
    "watsonx_data": {
        "description": "watsonx.data/Presto metrics",
        "metrics": [
            {
                "name": "presto_scan_bytes",
                "metric": "presto_query_physical_input_bytes_total",
                "labels": ["user", "catalog", "schema"],
                "unit": "bytes",
                "aggregation": "sum"
            },
            {
                "name": "presto_query_count",
                "metric": "presto_queries_completed_total",
                "labels": ["user", "catalog", "state"],
                "unit": "count",
                "aggregation": "sum"
            },
            {
                "name": "spark_job_duration",
                "metric": "spark_job_duration_seconds",
                "labels": ["tenant_id", "job_name", "application_id"],
                "unit": "seconds",
                "aggregation": "sum"
            },
            {
                "name": "spark_executor_memory",
                "metric": "spark_executor_memory_used_bytes",
                "labels": ["tenant_id", "application_id"],
                "unit": "bytes",
                "aggregation": "avg"
            }
        ],
        "collection_interval": "60s",
        "prometheus_endpoint": "http://presto-coordinator:8080/metrics"
    },
    "object_storage": {
        "description": "S3-compatible object storage metrics",
        "metrics": [
            {
                "name": "storage_gb_months",
                "metric": "s3_bucket_size_bytes",
                "labels": ["bucket", "tenant"],
                "unit": "bytes",
                "aggregation": "avg",
                "time_normalization": "monthly_average"
            },
            {
                "name": "api_requests",
                "metric": "s3_requests_total",
                "labels": ["bucket", "operation", "tenant"],
                "unit": "count",
                "aggregation": "sum"
            }
        ],
        "collection_interval": "300s",
        "cloudwatch_namespace": "AWS/S3"
    },
    "network": {
        "description": "Network egress metrics",
        "metrics": [
            {
                "name": "egress_bytes",
                "metric": "container_network_transmit_bytes_total",
                "labels": ["namespace", "pod", "tenant"],
                "unit": "bytes",
                "aggregation": "sum",
                "filter": "destination_external==true"
            }
        ],
        "collection_interval": "60s",
        "prometheus_endpoint": "http://prometheus:9090/metrics"
    }
}

# Create tenant mapping configuration
tenant_mapping = {
    "shipping-co-alpha": {
        "tenant_id": "shipping-co-alpha",
        "pulsar_tenant": "shipping-co-alpha",
        "cassandra_keyspaces": ["shipping_co_alpha"],
        "opensearch_indices": ["shipping-co-alpha-*"],
        "presto_users": ["shipping_alpha_user"],
        "s3_buckets": ["fleet-data-shipping-alpha"],
        "k8s_namespaces": ["shipping-alpha"]
    },
    "maritime-gamma": {
        "tenant_id": "maritime-gamma",
        "pulsar_tenant": "maritime-gamma",
        "cassandra_keyspaces": ["maritime_gamma"],
        "opensearch_indices": ["maritime-gamma-*"],
        "presto_users": ["maritime_gamma_user"],
        "s3_buckets": ["fleet-data-maritime-gamma"],
        "k8s_namespaces": ["maritime-gamma"]
    },
    "logistics-beta": {
        "tenant_id": "logistics-beta",
        "pulsar_tenant": "logistics-beta",
        "cassandra_keyspaces": ["logistics_beta"],
        "opensearch_indices": ["logistics-beta-*"],
        "presto_users": ["logistics_beta_user"],
        "s3_buckets": ["fleet-data-logistics-beta"],
        "k8s_namespaces": ["logistics-beta"]
    }
}

# Create metrics collector configuration
collector_config = {
    "sources": list(metrics_sources.keys()),
    "tenant_mapping": tenant_mapping,
    "aggregation_window": "1h",
    "storage": {
        "backend": "opensearch",
        "index_pattern": "tenant-metrics-{YYYY.MM}",
        "retention_days": 395
    },
    "collection_workers": 3,
    "batch_size": 1000
}

# Save all configurations
os.makedirs(metrics_base, exist_ok=True)

sources_file = os.path.join(metrics_base, "metrics_sources.json")
with open(sources_file, 'w') as f:
    json.dump(metrics_sources, f, indent=2)

tenant_mapping_file = os.path.join(metrics_base, "tenant_mapping.json")
with open(tenant_mapping_file, 'w') as f:
    json.dump(tenant_mapping, f, indent=2)

collector_config_file = os.path.join(metrics_base, "collector_config.json")
with open(collector_config_file, 'w') as f:
    json.dump(collector_config, f, indent=2)

metrics_framework_summary = {
    "total_services": len(metrics_sources),
    "total_metrics": sum(len(s["metrics"]) for s in metrics_sources.values()),
    "tenants": len(tenant_mapping),
    "files_created": [sources_file, tenant_mapping_file, collector_config_file]
}

print("Metrics Collection Framework Created")
print(f"Services configured: {metrics_framework_summary['total_services']}")
print(f"Total metrics: {metrics_framework_summary['total_metrics']}")
print(f"Tenants: {metrics_framework_summary['tenants']}")
print(f"Configuration files: {len(metrics_framework_summary['files_created'])}")
